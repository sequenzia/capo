"""DBOS ``monitor_delegation`` workflow (spec §5.6, §9.3, Task #30).

This module owns the **durable monitoring loop** for an in-flight Claude
Code / Codex delegation. The Phase-2 in-process monitor in
:mod:`capo.tools.claude_code` (``_spawn_monitor`` + ``_monitor_wrapper``) is
short-lived: when Capo restarts, those asyncio tasks vanish and the
``delegations`` row is left in ``running`` state forever. Phase 3 replaces
that with the DBOS workflow defined here so the monitor itself is durable
across process restarts (per spec §5.6 "DBOS Durable Monitoring + Restart
Resume").

What this Task #30 cut implements
---------------------------------

The §9.3 task table splits the §5.6 work across several tasks:

* **Task #30 (this module's first cut):** Defines the ``monitor_delegation``
  workflow shell with ``delegation_id`` as the deterministic input.
  Implements the continuous output-drain step (registered as a
  ``@DBOS.step``), the returncode polling loop, and the terminal-status
  transitions (``running`` → ``completed`` / ``failed`` / ``killed``).
* **Task #35 (later in Phase 3):** Wires ``delegate_to_claude_code`` to
  start the workflow instead of the in-process monitor + adds the
  ``claude --resume <session_id>`` spawn step that fires on workflow
  re-entry after a restart.
* **Tasks #32-#34:** Implement ``summarize_run``, ``notify_user``, and
  heartbeat steps.

Per the spec's "Workflow re-entered while subprocess is still running:
do NOT re-spawn" edge-case row (§5.6 Edge Cases): the workflow body in
THIS task assumes the subprocess was already spawned by the caller of
:func:`monitor_delegation` (i.e. ``delegate_to_claude_code``). It looks up
the live ``(process, reader_handle)`` pair in
:data:`_DELEGATION_REGISTRY` rather than spawning anything itself. Task
#35 will add the spawn-or-resume step that runs FIRST on workflow entry,
which sits in front of this monitor loop. That separation keeps Task #30
strictly scoped to "poll + drain + transition" so the resume path
(Task #35) can layer on top without re-coding the monitor.

Key invariants enforced here
----------------------------

1. **Workflow is deterministic on `delegation_id`.** DBOS replays the
   workflow on restart with the same input; the workflow body MUST be
   reproducible. We read the row's PID + session_id at the start of each
   wave (so Phase 4 restart-resume sees the same row state). No
   timestamps, no random UUIDs, no env reads.

2. **Output drain is a `@DBOS.step`.** The §5.6 acceptance criterion
   reads: "Reader is registered as a ``@DBOS.step`` so its progress is
   checkpointed." We register :func:`_drain_output_step` as a step (via
   ``@idempotent_step`` so the side effect — opening a fresh DB
   connection and writing rows — is at-most-once across retries). The
   step itself does NOT recreate the reader; it AWAITS the existing
   reader handle's ``wait()`` so the BatchedInserter buffer keeps draining
   until EOF or cancellation. The reader writes via
   ``asyncio.to_thread(inserter.add, ...)`` so the event loop is never
   pinned on the SQLite write path (§6.1, inherited from Task #21).

3. **Returncode polling has a configurable interval.** Default 1.0s
   matches the §5.6 acceptance criterion. Callers can override via the
   ``poll_interval_s`` keyword on :func:`monitor_delegation` for tests
   that want sub-millisecond polling.

4. **Status transitions are atomic + persisted before workflow exit.**
   - ``returncode == 0`` → ``completed``.
   - ``returncode != 0`` (and not a signal exit) → ``failed`` with
     ``summary="exit code N (stderr: <last 200 chars>)"``.
   - Negative ``returncode`` on POSIX (process was signaled — see
     ``signal.Signals(-rc)``) → ``killed`` when the signal is
     SIGTERM/SIGKILL/SIGINT (user-initiated kill); otherwise ``failed``.
   - Workflow exception (any branch raises) → ``failed`` with
     ``summary="monitor crashed: <repr>"``; Logfire span carries the
     traceback for operator debugging.

5. **All `@DBOS.step` calls with external side effects use the
   idempotency framework** (Task #29). The output drain step is wrapped
   in :func:`~capo.workflows._idempotency.idempotent_step` with
   ``deterministic_args=(delegation_id,)``. The terminal-status update
   step similarly. This satisfies the §5.6 acceptance criterion that "all
   side-effecting steps generate and persist an idempotency key".

6. **Workflow exceptions are visible.** Any uncaught exception in the
   workflow body opens a Logfire span (best-effort — falls back to
   stdlib logging if logfire isn't configured) and marks the row
   ``failed`` before re-raising. DBOS catches the re-raise and marks the
   workflow itself failed in ``dbos.db``, but the user-visible state is
   the ``delegations`` row.

In-process registry
-------------------

The §5.6 contract says "never re-attach by PID" — when the workflow
re-enters after a restart, the original subprocess is gone (or, worse,
the PID was recycled by the OS). The resume path (Task #35) handles that
by re-spawning via ``claude --resume <session_id>``. But on the
happy-path (single process lifetime), the workflow needs to find the
*current* ``Process`` + ``ReaderHandle`` for the ``delegation_id`` — the
DBOS workflow body can't pickle those objects through ``dbos.db``.

We solve this with :data:`_DELEGATION_REGISTRY`, a module-level dict
populated by :func:`register_delegation_subprocess` (called by
:func:`delegate_to_claude_code` after spawn). The workflow body reads
from this registry. On a restart, the registry is empty (process state
was lost), and the workflow body falls through to the failed-without-
subprocess branch — which Task #35 will replace with the resume step.

The registry is intentionally **process-local** and **never persisted**:
its only role is to bridge the in-asyncio ``Process`` object from the
spawn site to the monitor workflow. The DURABLE state is the
``delegations`` row (PID, session_id, status).

Spec references
---------------

* §5.6 Feature: DBOS Durable Monitoring + Restart Resume.
* §5.6 Edge Cases — restart mid-spawn, restart between completion and
  notify_user, session-resume fails, orphan reaped by OS.
* §6.1 Performance — zero loss + zero pipe blocking on ≥10 MB stdout
  (re-verified inside DBOS context per Task #30 perf criterion).
* §7.3 Data Models — ``delegations`` schema; PRAGMA + retry helper.
* §9.3 Phase 3 plan — task split (#30 monitor body, #31 boot recovery,
  #32-#34 summarize/notify/heartbeat, #35 spawn-or-resume handoff).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import sqlite3
import threading
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.observability import delegation_monitor_span
from capo.tools._subprocess import ReaderHandle, start_reader
from capo.workflows._idempotency import idempotent_step

logger = logging.getLogger("capo.workflows.delegation")

# Task #51: Logfire spans are opened through :mod:`capo.observability` —
# the named constructor :func:`delegation_monitor_span` already tolerates
# logfire being missing or unconfigured (returns a ``contextlib.nullcontext``
# in that case), so the prior soft-import of ``logfire`` here is no longer
# required.


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: Default returncode-polling interval. The §5.6 acceptance criterion says
#: "periodically"; 1 s matches the resolution at which the user perceives
#: status changes and avoids hammering the event loop. Tests pass a smaller
#: value (5–20 ms) so the integration suite stays fast.
DEFAULT_POLL_INTERVAL_S: float = 1.0

#: Default maximum runtime before we give up on the polling loop. Defends
#: against a runaway monitor in test environments; production workflows are
#: expected to run for hours. Set to ``None`` to disable. Tests override.
DEFAULT_MAX_RUNTIME_S: float | None = None

#: Signals we treat as "user-initiated kill" → status ``killed``.
#: Everything else (SIGABRT, SIGSEGV, ...) → ``failed``. Per §5.5
#: ``kill_delegation`` always escalates to SIGKILL after SIGTERM grace, so
#: both signals must map to ``killed``.
_KILLED_SIGNALS: frozenset[int] = frozenset(
    {int(signal.SIGTERM), int(signal.SIGKILL), int(signal.SIGINT)}
)

#: How many bytes of stderr we tail into the ``failed`` row's ``summary``
#: column. SQLite ``TEXT`` is unbounded but huge summaries are a UX hazard
#: in the agent's check_delegation_status output; 200 chars is enough for
#: a useful diagnosis without being a wall of text.
_STDERR_TAIL_BYTES: int = 200


# ---------------------------------------------------------------------------
# In-process subprocess + reader registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DelegationProcessHandle:
    """In-process pointer to the live subprocess + reader for one delegation.

    Stored in :data:`_DELEGATION_REGISTRY` by the spawn site (Task #35 wires
    ``delegate_to_claude_code`` to register here right after spawn) so the
    DBOS monitor workflow can find the live objects without trying to
    re-attach by PID.

    Attributes
    ----------
    delegation_id:
        Foreign key matching ``delegations.id``.
    process:
        The asyncio subprocess (anything with ``returncode`` /
        ``wait()`` / ``terminate()`` / ``stderr``). Typed as ``Any``
        because tests pass fakes.
    reader_handle:
        The :class:`~capo.tools._subprocess.ReaderHandle` returned by
        :func:`~capo.tools._subprocess.start_reader`. The monitor calls
        ``reader_handle.wait()`` inside the drain step so it stays alive
        until EOF (or cancellation).
    db_path:
        Absolute path to ``state.db``. Captured at registration time so
        the workflow body — which runs in DBOS-managed worker threads —
        can open its own connection without depending on the caller's
        ``RunContext``.
    """

    delegation_id: str
    process: Any
    reader_handle: ReaderHandle
    db_path: Path


# Lock guarding registry mutations. asyncio + DBOS step worker threads may
# both touch this dict; the lock keeps insert/remove atomic.
_registry_lock = threading.Lock()
_DELEGATION_REGISTRY: dict[str, DelegationProcessHandle] = {}


def register_delegation_subprocess(handle: DelegationProcessHandle) -> None:
    """Register a live subprocess + reader so :func:`monitor_delegation` can
    find them.

    Called by the spawn site (currently
    :func:`capo.tools.claude_code.delegate_to_claude_code` — wired in
    Task #35; for Task #30 this is called directly by integration tests).
    Safe to call multiple times for the same ``delegation_id``: the latest
    handle wins (Task #35's resume path will re-register after a re-spawn).
    """
    with _registry_lock:
        _DELEGATION_REGISTRY[handle.delegation_id] = handle


def unregister_delegation_subprocess(delegation_id: str) -> None:
    """Remove a delegation from the registry. Safe on missing keys."""
    with _registry_lock:
        _DELEGATION_REGISTRY.pop(delegation_id, None)


def _lookup_delegation(delegation_id: str) -> DelegationProcessHandle | None:
    """Return the registered handle for ``delegation_id`` or ``None``.

    Returning ``None`` is how the workflow detects "subprocess state was
    lost across a restart" — Task #35 will branch on that to trigger the
    ``claude --resume`` spawn step. In Task #30 we surface it as a
    ``failed`` terminal state (no subprocess to monitor).
    """
    with _registry_lock:
        return _DELEGATION_REGISTRY.get(delegation_id)


# ---------------------------------------------------------------------------
# Status enum (internal — see §7.3 CHECK constraint for the wire values)
# ---------------------------------------------------------------------------

STATUS_RUNNING: str = "running"
STATUS_COMPLETED: str = "completed"
STATUS_FAILED: str = "failed"
STATUS_KILLED: str = "killed"

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {STATUS_COMPLETED, STATUS_FAILED, STATUS_KILLED}
)


@dataclass(frozen=True)
class MonitorResult:
    """Return value of :func:`monitor_delegation`.

    Useful for integration tests; the workflow itself is fire-and-forget
    from the caller's perspective (the durable signal is the
    ``delegations`` row).
    """

    delegation_id: str
    status: str
    returncode: int | None
    summary: str | None


# ---------------------------------------------------------------------------
# DB helpers (sync — wrapped in ``asyncio.to_thread`` by callers)
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """ISO-8601 UTC timestamp matching the format used elsewhere in Capo."""
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _persist_terminal_status(
    db_path: Path,
    delegation_id: str,
    *,
    status: str,
    summary: str | None,
) -> None:
    """UPDATE ``delegations`` to a terminal ``status`` + ``ended_at`` + summary.

    Goes through :func:`begin_immediate_with_retry` so the §7.3 zero-
    ``SQLITE_BUSY``-to-caller invariant holds even when the killer
    (:func:`_persist_killed_row` in :mod:`capo.tools.delegations`) and the
    monitor race to write the row.

    **Idempotent on the row level**: if the row is already in a terminal
    state when we look at it, we leave it alone. This is the "killer
    already wrote, monitor finished a millisecond later" case — we MUST
    NOT overwrite ``killed`` with ``failed``.
    """
    if status not in _TERMINAL_STATUSES:
        raise ValueError(
            f"_persist_terminal_status: status must be terminal, got {status!r}"
        )
    conn = open_connection(db_path)
    try:
        # SELECT current status under the same connection so the read is
        # consistent with the UPDATE below.
        cur = conn.execute(
            "SELECT status FROM delegations WHERE id = ?", (delegation_id,)
        )
        row = cur.fetchone()
        if row is None:
            # No row to update — caller passed a stale delegation_id. Log
            # and bail; we don't want to fabricate a row.
            logger.warning(
                "monitor_delegation: no delegations row for id=%s — "
                "cannot persist terminal status %s",
                delegation_id,
                status,
                extra={
                    "event": "capo.workflows.delegation.row_missing",
                    "delegation_id": delegation_id,
                    "status": status,
                },
            )
            return
        current = row[0]
        if current in _TERMINAL_STATUSES:
            # Already terminal (killer beat us). Honor that state.
            logger.info(
                "monitor_delegation: row already terminal (%s); leaving as-is",
                current,
                extra={
                    "event": "capo.workflows.delegation.already_terminal",
                    "delegation_id": delegation_id,
                    "row_status": current,
                    "intended_status": status,
                },
            )
            return
        with begin_immediate_with_retry(
            conn, operation=f"monitor_terminal_status:{delegation_id}"
        ):
            conn.execute(
                "UPDATE delegations SET status = ?, ended_at = ?, summary = ? "
                "WHERE id = ?",
                (status, _utc_now_iso(), summary, delegation_id),
            )
    finally:
        conn.close()


def _read_status(db_path: Path, delegation_id: str) -> str | None:
    """SELECT the row's current ``status`` (or ``None`` if missing).

    Used by the monitor loop to detect "user killed the delegation while
    we were polling" — when the kill path UPDATEs the row to ``killed``,
    our next status read sees that and we exit the poll loop without
    trying to overwrite the column.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
    finally:
        conn.close()
    return row["status"] if row else None


# ---------------------------------------------------------------------------
# DBOS step: continuous output drain
# ---------------------------------------------------------------------------


@idempotent_step(
    step_name="monitor_delegation_drain_output",
    deterministic_args=lambda delegation_id, **_: (delegation_id,),
)
async def _drain_output_step(delegation_id: str) -> dict[str, Any]:
    """Drain the per-delegation reader's output buffer to ``delegation_output``.

    Registered as an ``@DBOS.step`` (via :func:`idempotent_step`) so its
    progress — chunks already persisted via the BatchedInserter — is
    checkpointed. The step body simply AWAITS the existing reader handle's
    ``wait()``, which blocks until both stdout and stderr hit EOF (or the
    reader is cancelled).

    The reader itself was started by the spawn site
    (:func:`capo.tools._subprocess.start_reader`) and uses
    ``asyncio.to_thread(inserter.add, ...)`` to flush chunks — so the
    event loop is never pinned on SQLite writes (§6.1, Task #21
    invariant). This step just keeps that drain alive until the reader
    finishes.

    Returns a small dict summarizing what the reader observed:

    * ``stdout_bytes`` / ``stderr_bytes`` — totals captured.
    * ``event_rows`` / ``stdout_rows`` / ``stderr_rows`` — persisted row counts.
    * ``eof_reached`` — True if both streams hit EOF cleanly; False if
      cancelled mid-drain.
    * ``fatal_error`` — repr of the reader's fatal error, if any
      (non-cancellation only; cancellation is recorded as
      ``eof_reached=False``).

    Why a dict rather than ``ReaderStats``? DBOS pickles step return
    values to ``dbos.db`` on success; a frozen dataclass works too, but
    a dict keeps the column-shape stable across Python class evolution.

    Raises
    ------
    RuntimeError
        If ``delegation_id`` isn't in the registry. The workflow body
        catches this and transitions the row to ``failed`` with a
        descriptive summary — Task #35 will replace this branch with the
        resume-spawn step.
    """
    handle = _lookup_delegation(delegation_id)
    if handle is None:
        raise RuntimeError(
            f"delegation {delegation_id!r} not found in process registry — "
            "subprocess state was lost across a restart. Task #35's resume "
            "step will recover from this path; for Task #30 we surface it "
            "as a failed delegation."
        )

    reader_handle = handle.reader_handle
    # ReaderHandle.wait() returns the final ReaderStats; non-cancellation
    # exceptions in the reader propagate out of .wait() (the reader wraps
    # them in :class:`ReaderError` per §5.6).
    stats = await reader_handle.wait()
    return {
        "stdout_bytes": stats.stdout_bytes,
        "stderr_bytes": stats.stderr_bytes,
        "event_rows": stats.event_rows,
        "stdout_rows": stats.stdout_rows,
        "stderr_rows": stats.stderr_rows,
        "flushes": stats.flushes,
        "split_lines": stats.split_lines,
        "parse_errors": stats.parse_errors,
        "eof_reached": stats.eof_reached,
        "fatal_error": (
            f"{type(stats.fatal_error).__name__}: {stats.fatal_error}"
            if stats.fatal_error is not None
            else None
        ),
    }


# ---------------------------------------------------------------------------
# DBOS step: terminal status update
# ---------------------------------------------------------------------------


@idempotent_step(
    step_name="monitor_delegation_persist_terminal",
    deterministic_args=lambda *args, **kwargs: (
        # First two positional args are (delegation_id, status). We accept
        # **all** positional + keyword args here so the lambda matches the
        # wrapped function's signature even when DBOS forwards everything
        # positionally (its internal step machinery does so).
        args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
        args[1] if len(args) >= 2 else kwargs.get("status"),
    ),
)
async def _persist_terminal_step(
    delegation_id: str,
    status: str,
    summary: str | None,
    db_path: str,
) -> dict[str, Any]:
    """Persist the workflow's terminal status conclusion on the row.

    Wrapped in :func:`idempotent_step` so re-entry after a crash between
    "decided the status" and "wrote the row" sees the same key and DBOS
    replays the cached return value (no double-write).

    The deterministic args are ``(delegation_id, status)`` — same logical
    operation idempotent on the *intended* terminal status. The
    ``summary`` is left out of the key on purpose: two different summaries
    for the same ``(delegation_id, status)`` would otherwise trip
    :class:`~capo.workflows._idempotency.IdempotencyMismatchError`, which
    would mask real bugs but here it's a known-fine race (e.g. the
    workflow recomputes the stderr tail across a retry — different bytes,
    same conclusion).

    ``db_path`` is taken as a ``str`` rather than ``Path`` because DBOS
    serializes step args to ``dbos.db`` on entry, and ``Path`` has no
    stable JSON serialization. The conversion back is a one-liner.
    """
    await asyncio.to_thread(
        _persist_terminal_status,
        Path(db_path),
        delegation_id,
        status=status,
        summary=summary,
    )
    # Task #59: release the caffeinate refcount on every terminal status
    # transition. We call it FROM the idempotent terminal-status step so
    # DBOS replays it as part of the same step on workflow re-entry —
    # repeated release for the same delegation_id is a no-op in the
    # manager (release of a not-tracked id just logs at debug).
    # Best-effort: caffeinate is a degradable convenience.
    try:
        from capo.caffeinate import (  # noqa: PLC0415
            get_caffeinate_manager,
        )

        with contextlib.suppress(Exception):
            await get_caffeinate_manager().release_delegation(delegation_id)
    except Exception:  # noqa: BLE001 — import path itself failing
        logger.debug(
            "caffeinate release skipped (import failure)",
            extra={"event": "capo.workflows.delegation.caffeinate.skip"},
        )
    return {"delegation_id": delegation_id, "status": status}


# ===========================================================================
# === summarize_run (Task #32) ==============================================
# ===========================================================================
#
# Spec references: §5.6 ("On completion, the workflow calls ``summarize_run``
# (a ``@DBOS.step``) and stores it on the delegations row") and §5.12 ("Check
# delegation status" — the user-facing ``summary_one_line`` is derived from
# this column).
#
# Original §5.6 wording framed ``summarize_run`` as an LLM call producing a
# one-paragraph summary. The Task #32 amendment downgrades that to a
# **deterministic** terminal one-liner so the §5.6 restart-resume
# reproducibility contract holds: DBOS at-least-once step semantics replay
# the cached step result on workflow re-entry; an LLM call would yield a
# different string on each replay → :class:`IdempotencyMismatchError`. An
# LLM-enriched summary can be layered later as a non-checkpointed background
# enrichment without changing the at-restart contract.
#
# Output contract — a single line with stable structure suitable for the
# row's ``summary`` column (SQLite TEXT, unbounded but kept short for UX):
#
#     "status=<S> runtime=<row-derived> bytes(out=<n> err=<n> evt=<n>) "
#     "rows(out=<n> err=<n> evt=<n>) | last[<kind>]=<excerpt>"
#
# Where ``<row-derived>`` is ``ended_at - started_at`` in whole seconds,
# computed from the DB row's own timestamps (NEVER ``datetime.now()``).
# When ``ended_at`` is still NULL (this step runs BEFORE
# ``_persist_terminal_step`` writes ``ended_at``), the field renders as
# ``runtime=?`` — a known, stable token rather than a wall-clock value.
# After ``_persist_terminal_step`` writes ``ended_at`` and the workflow
# replays this step on a subsequent restart, the runtime delta is
# computed from the DURABLE row state — same row → same string.
#
# Determinism contract (enforced by tests):
#   * No ``datetime.now()`` / ``time.time()`` reads.
#   * No randomness (``uuid``, ``random``).
#   * No external service calls.
#   * Same row + same delegation_output rows → byte-identical string.


#: Hard cap on the ``last[…]=`` excerpt — Task #32 edge case "Final assistant
#: message > 500 chars: truncated with explicit suffix". Char count rather
#: than byte count because the column is TEXT and operators read chars.
_SUMMARY_LAST_MAX_CHARS: int = 500

#: How many delegation_output rows we inspect (most-recent-first) when
#: hunting for the "last" line of interest. Bounds memory in pathological
#: runs (a chatty CC can produce 50k rows). 50 is enough — the last
#: assistant message + stderr lines are almost always in the final handful.
_SUMMARY_TAIL_ROWS: int = 50


def _summary_truncate_last(text: str) -> str:
    """Truncate ``text`` to :data:`_SUMMARY_LAST_MAX_CHARS` with explicit suffix.

    Returns ``text`` unchanged if already within the cap. Otherwise returns
    the first :data:`_SUMMARY_LAST_MAX_CHARS` characters followed by
    ``"…[truncated N chars]"`` where ``N`` is the number of dropped chars.
    The ellipsis + bracketed suffix is unambiguous in log output so an
    operator can tell at a glance that the field was cut.
    """
    if len(text) <= _SUMMARY_LAST_MAX_CHARS:
        return text
    dropped = len(text) - _SUMMARY_LAST_MAX_CHARS
    return text[:_SUMMARY_LAST_MAX_CHARS] + f"…[truncated {dropped} chars]"


def _summary_extract_assistant_text(event_chunk: str) -> str | None:
    """Best-effort extraction of an assistant-text payload from a stream-json event.

    Capo's CC subprocess emits stream-json events where assistant turns
    show up as ``{"type":"assistant","message":{"content":[{"type":"text",
    "text":"…"}, …]}}``. We surface only the text segments concatenated
    with newlines. Returns ``None`` if the chunk is not a JSON object,
    isn't an assistant event, or has no text content — the caller falls
    back to the next candidate row.

    Side-effect-free + deterministic.
    """
    chunk = event_chunk.strip()
    if not chunk:
        return None
    try:
        obj = json.loads(chunk)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    if obj.get("type") != "assistant":
        return None
    message = obj.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            text_val = item.get("text")
            if isinstance(text_val, str) and text_val:
                parts.append(text_val)
    if not parts:
        return None
    return "\n".join(parts)


def _summary_parse_db_timestamp(value: Any) -> datetime | None:
    """Parse a SQLite TIMESTAMP value into a UTC ``datetime``.

    SQLite stores timestamps as TEXT by default; ``CURRENT_TIMESTAMP``
    yields ``"YYYY-MM-DD HH:MM:SS"``, and our ``_utc_now_iso`` helper
    yields the same shape with fractional seconds. Both forms are
    accepted; a non-string value (already a ``datetime``) is returned
    as-is with UTC tzinfo added if missing.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    # Normalize space-separator → 'T' for fromisoformat compatibility.
    candidate = text.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _summary_format_runtime(started_at: Any, ended_at: Any) -> str:
    """Format ``ended_at - started_at`` as ``<int>s`` or ``?``.

    Both inputs come from the SQLite TIMESTAMP column. Returns ``"?"`` if
    either side is missing/unparseable, or if the delta is negative
    (clock skew).

    Deterministic: no ``datetime.now()`` call. Two replays of the same
    step over the same row produce the same string.
    """
    s = _summary_parse_db_timestamp(started_at)
    e = _summary_parse_db_timestamp(ended_at)
    if s is None or e is None:
        return "?"
    delta = (e - s).total_seconds()
    if delta < 0:
        return "?"
    return f"{int(delta)}s"


def _build_run_summary(
    db_path: Path,
    delegation_id: str,
    *,
    status_override: str | None = None,
    tail_rows: int = _SUMMARY_TAIL_ROWS,
) -> str:
    """Synchronous worker for :func:`summarize_run`.

    Runs the SELECTs in a single connection so the read is point-in-time
    consistent. Builds the fixed-shape one-liner described in the region
    docstring and returns it. Pure function (no side effects on the DB).

    ``status_override`` lets the caller pass the workflow's *intended*
    terminal status, which is written to the row by ``_persist_terminal_step``
    immediately after :func:`summarize_run` returns. Because summarize_run
    runs BEFORE that UPDATE, the row column still says ``running`` at
    this point; using the override keeps the summary aligned with what
    the column WILL say once persistence completes. The override is
    itself deterministic from the workflow body (derived from the
    subprocess's returncode classifier), so replays produce the same
    value.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, status, started_at, ended_at FROM delegations "
            "WHERE id = ?",
            (delegation_id,),
        ).fetchone()
        if row is None:
            return f"delegation {delegation_id} not found"

        # Aggregate per-stream byte + row counts across ALL output rows.
        # SUM(LENGTH) on a TEXT column counts UTF-8 bytes — fine for the
        # operator-facing summary (rough magnitude is what matters).
        agg_rows = conn.execute(
            """
            SELECT stream,
                   COUNT(*)                        AS rows,
                   COALESCE(SUM(LENGTH(chunk)), 0) AS bytes
              FROM delegation_output
             WHERE delegation_id = ?
             GROUP BY stream
            """,
            (delegation_id,),
        ).fetchall()
        # Materialize into a stable dict so the format string is
        # deterministic regardless of GROUP BY iteration order.
        per_stream: dict[str, tuple[int, int]] = {
            "stdout": (0, 0),
            "stderr": (0, 0),
            "event": (0, 0),
        }
        for r in agg_rows:
            stream = str(r["stream"])
            if stream in per_stream:
                per_stream[stream] = (int(r["rows"]), int(r["bytes"]))

        total_rows = sum(rows for rows, _ in per_stream.values())

        # Pull the tail (most-recent-first) to find the final assistant
        # message OR the last stderr line for an error. Bounded by
        # tail_rows so an enormous output history doesn't bloat memory.
        tail = conn.execute(
            """
            SELECT id, stream, chunk
              FROM delegation_output
             WHERE delegation_id = ?
             ORDER BY id DESC
             LIMIT ?
            """,
            (delegation_id, int(tail_rows)),
        ).fetchall()
    finally:
        conn.close()

    # Prefer the caller-provided override (the workflow's intended
    # terminal status, deterministic from returncode classifier) so the
    # summary aligns with what the row WILL contain after
    # ``_persist_terminal_step`` runs immediately after this call. Falls
    # back to the row's current value if no override.
    if status_override is not None:
        status = str(status_override)
    elif row["status"] is not None:
        status = str(row["status"])
    else:
        status = "?"
    runtime_str = _summary_format_runtime(row["started_at"], row["ended_at"])

    # Pick the "last" line of interest. Deterministic on output rows
    # (NOT on row.status): walk the tail in reverse-chronological order;
    # surface the first stderr OR assistant-event-text we find. If none,
    # fall back to the last non-empty stdout line. If still nothing →
    # "no output".
    last_segment: str = "no output"
    last_kind: str = "none"
    if total_rows > 0:
        for r in tail:
            stream = str(r["stream"])
            chunk = str(r["chunk"]) if r["chunk"] is not None else ""
            if not chunk.strip():
                continue
            if stream == "stderr":
                last_segment = chunk.strip()
                last_kind = "error"
                break
            if stream == "event":
                text = _summary_extract_assistant_text(chunk)
                if text is not None:
                    last_segment = text.strip()
                    last_kind = "assistant"
                    break
        else:
            for r in tail:
                stream = str(r["stream"])
                chunk = str(r["chunk"]) if r["chunk"] is not None else ""
                if stream == "stdout" and chunk.strip():
                    last_segment = chunk.strip()
                    last_kind = "stdout"
                    break

    last_segment = _summary_truncate_last(last_segment)

    out_rows, out_bytes = per_stream["stdout"]
    err_rows, err_bytes = per_stream["stderr"]
    evt_rows, evt_bytes = per_stream["event"]

    # Fixed-shape one-liner. Whitespace + key order are part of the
    # contract; dashboards / log parsers MAY depend on this layout.
    return (
        f"status={status} runtime={runtime_str} "
        f"bytes(out={out_bytes} err={err_bytes} evt={evt_bytes}) "
        f"rows(out={out_rows} err={err_rows} evt={evt_rows}) "
        f"| last[{last_kind}]={last_segment}"
    )


def _persist_run_summary(db_path: Path, delegation_id: str, summary: str) -> None:
    """UPDATE only the ``summary`` column for ``delegation_id``.

    Side effect of :func:`summarize_run`. Goes through
    :func:`begin_immediate_with_retry` so the §7.3 zero-``SQLITE_BUSY``-
    to-caller invariant holds even when the killer and the monitor race
    on the same row.

    No-op if the row is missing (consistent with
    :func:`_persist_terminal_status`).
    """
    conn = open_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT 1 FROM delegations WHERE id = ?", (delegation_id,)
        )
        if cur.fetchone() is None:
            logger.info(
                "summarize_run: no delegations row for id=%s — skipping",
                delegation_id,
                extra={
                    "event": "capo.workflows.delegation.summarize_row_missing",
                    "delegation_id": delegation_id,
                },
            )
            return
        with begin_immediate_with_retry(
            conn, operation=f"summarize_run:{delegation_id}"
        ):
            conn.execute(
                "UPDATE delegations SET summary = ? WHERE id = ?",
                (summary, delegation_id),
            )
    finally:
        conn.close()


@idempotent_step(
    step_name="summarize_run",
    deterministic_args=lambda *args, **kwargs: (
        # Spec §5.6: ``summarize_run`` is keyed by ``delegation_id`` ONLY.
        # The summary content is derived from durable state (the row +
        # delegation_output rows) which DBOS replays consistently across
        # restarts. We accept additional args (status, db_path) for the
        # Task #33 calling convention but don't include them in the
        # idempotency key.
        args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
    ),
)
async def summarize_run(
    delegation_id: str,
    status: str | None = None,
    db_path: str | None = None,
) -> str:
    """Produce a deterministic terminal summary and persist it on the row.

    Public entrypoint of the Task #32 step. Registered as a ``@DBOS.step``
    via :func:`~capo.workflows._idempotency.idempotent_step` keyed by
    ``delegation_id`` (spec §5.6). Called from :func:`_monitor_body` BEFORE
    :func:`_persist_terminal_step` so the row's ``summary`` column reflects
    the run.

    The function:

    1. Reads the ``delegations`` row + the last :data:`_SUMMARY_TAIL_ROWS`
       ``delegation_output`` rows for ``delegation_id``.
    2. Assembles a fixed-shape one-liner (see region docstring).
    3. Persists it to the row's ``summary`` column via
       :func:`_persist_run_summary`.
    4. Returns the same string for callers that want it.

    Determinism: no wall-clock reads, no randomness, no external service
    calls. Same input row + output rows → byte-identical output. This is
    the §5.6 restart-resume reproducibility contract — DBOS may replay
    this step on workflow re-entry, and the replayed result MUST match
    the original or :class:`IdempotencyMismatchError` fires.

    Edge cases:

    * **No output rows yet** (subprocess killed immediately after spawn):
      byte / row counts are zero and the ``last[...]=`` segment renders
      as ``last[none]=no output``.
    * **Final assistant message > 500 chars**: truncated to 500 chars
      with the explicit suffix ``…[truncated N chars]``.
    * **Row missing**: returns ``"delegation <id> not found"`` and skips
      the UPDATE (consistent with :func:`_persist_terminal_status`).

    Parameters
    ----------
    delegation_id:
        Foreign key on ``delegations.id``. The deterministic input.
    status:
        Optional. The workflow's intended terminal status (forwarded by
        the Task #33 hook in :func:`_monitor_body`). Used as the
        ``status=…`` field of the summary so it aligns with what
        ``_persist_terminal_step`` writes to the row *immediately after*
        this call. Falls back to the row's current ``status`` column if
        not supplied. The value is deterministic from the subprocess's
        returncode classifier, so replays produce the same summary
        regardless. NOT included in the idempotency key.
    db_path:
        Optional. When omitted, the function attempts to resolve the
        state.db path from the in-process registry
        (:data:`_DELEGATION_REGISTRY`). Required when called from
        contexts that bypass the registry (Task #33 passes it
        explicitly).

    Returns
    -------
    str
        The summary string that was persisted to the row.
    """
    # Resolve db_path either from the caller or from the registry handle.
    resolved_db_path: Path
    if db_path is not None:
        resolved_db_path = Path(db_path)
    else:
        handle = _lookup_delegation(delegation_id)
        if handle is None:
            # Without a path we can't read or write the row. Return a
            # diagnostic string so a misuse from a future caller is
            # visible in logs.
            logger.warning(
                "summarize_run: no db_path supplied and delegation %s "
                "not in process registry — returning diagnostic summary",
                delegation_id,
                extra={
                    "event": "capo.workflows.delegation.summarize_no_db_path",
                    "delegation_id": delegation_id,
                },
            )
            return f"summarize_run: unable to resolve db_path for {delegation_id}"
        resolved_db_path = handle.db_path

    # ``status`` is forwarded as ``status_override`` so the summary
    # aligns with the workflow's intended terminal status (which
    # ``_persist_terminal_step`` writes to the row IMMEDIATELY after this
    # call). It is intentionally NOT part of the idempotency key — the
    # workflow's classifier is deterministic from the subprocess's
    # returncode (durable state), so two calls with the same
    # ``delegation_id`` will always pass the same ``status`` value on a
    # given replay branch.

    summary = await asyncio.to_thread(
        _build_run_summary,
        resolved_db_path,
        delegation_id,
        status_override=status,
    )
    await asyncio.to_thread(
        _persist_run_summary,
        resolved_db_path,
        delegation_id,
        summary,
    )
    return summary


# === end summarize_run (Task #32) =========================================


# ===========================================================================
# === Restart resume (Task #31) ============================================
# ===========================================================================
#
# Spec references: §5.6 "Restart contract" + §7.5 "Integration: Claude Code
# CLI" + spike S-3 §4.2.
#
# When :func:`monitor_delegation` re-enters a workflow whose original
# subprocess is gone (process state lost across a Capo restart), the in-
# process registry has no live handle. Task #30 surfaced this as a hard
# ``failed`` terminal state; Task #31 replaces that with the documented
# resume contract:
#
#   1. Read the row's ``session_id_subagent`` and ``workspace``.
#   2. If session_id is NULL → mark the row ``failed`` with reason
#      "lost session_id across restart". The caller (operator) sees the
#      dropped delegation in ``state.db``.
#   3. Else → re-spawn the subprocess with
#      ``claude --resume <session_id_subagent>`` (per §7.5 / S-3 §4.2),
#      start a fresh reader on it, and register the new handle so the
#      monitor body's existing drain + poll path can take over.
#
# The spawn step is wrapped in :func:`idempotent_step` keyed only by
# ``delegation_id``. DBOS replays the cached return value on workflow
# re-entry inside a single process lifetime, so we never spawn twice for
# the same workflow attempt. We layer an in-process guard (registry check)
# on top so unit tests that exercise the step outside a real DBOS workflow
# context also see "at most one spawn per process lifetime".
#
# **Inherited contract — refuse to overwrite session_id on is_error=true.**
# Per S-3 §4.2: ``claude --resume <unknown>`` emits a plain-text error line
# then a single ``result`` event with ``is_error: true`` and a *new*
# synthetic session_id. The Phase 2 reader already enforces "first event
# must carry a non-empty top-level ``session_id``", but it does NOT inspect
# ``is_error``. So after we observe the reader's first event, we explicitly
# check ``is_error`` here and:
#   * REFUSE to UPDATE the row's ``session_id_subagent`` (the original
#     captured value stays). This is the "parser refuses to overwrite"
#     edge case row.
#   * Mark the row ``failed`` with reason "session resume failed".
#   * Kill the spawned subprocess so it doesn't keep running.

#: Subprocess pipe buffer for the resume spawn. Matches the original spawn
#: site (:data:`capo.tools.claude_code._SUBPROCESS_STREAM_LIMIT`) so a single
#: chatty CC event doesn't trip :class:`asyncio.LimitOverrunError`.
_RESUME_SUBPROCESS_STREAM_LIMIT: int = 2 * 1024 * 1024

#: How long the resume step waits for the resumed CC to emit its first JSON
#: event. v2.1.138 first-event latency is < 200 ms locally (S-3 §4.3); 30 s
#: matches the Phase 2 :data:`SESSION_ID_CAPTURE_TIMEOUT_S` ceiling.
RESUME_FIRST_EVENT_TIMEOUT_S: float = 30.0

#: Default Claude Code binary used by the resume step when no explicit
#: ``claude_binary`` is threaded through. Production callers (Task #35 and
#: friends) will pass the configured binary path via :func:`monitor_delegation`
#: kwargs; tests override either by passing the kwarg or by patching the
#: argv builder.
DEFAULT_CLAUDE_BINARY: str = "claude"

#: Sentinel ``summary`` strings used by the resume step. Exposed at module
#: scope so callers (and tests) can assert on them without hard-coding.
RESUME_REASON_LOST_SESSION_ID: str = "lost session_id across restart"
RESUME_REASON_FAILED: str = "session resume failed"

#: Sentinel ``summary`` string when the row's ``agent`` column is NULL or
#: holds an unrecognized value. Task #46 — the resume dispatcher refuses
#: to spawn for an unknown agent kind because the argv contract differs
#: per-agent and a wrong-binary spawn would corrupt the rollout.
RESUME_REASON_UNKNOWN_AGENT: str = "unknown agent type"

#: Agent-column values (mirror the §7.3 CHECK constraint).
AGENT_CLAUDE_CODE: str = "claude-code"
AGENT_CODEX: str = "codex"

#: Default Codex binary used by the resume step when no explicit
#: ``codex_binary`` is threaded through. Production callers (Task #35 +
#: cold-boot sweep in :mod:`capo.main`) pass the configured binary path
#: via :func:`monitor_delegation` kwargs.
DEFAULT_CODEX_BINARY: str = "codex"

#: Deterministic continuation prompt for ``codex exec resume``. Per
#: Task #46 acceptance criteria: the resume call passes a "deterministic
#: 'resume after restart' stub" rather than the original brief's
#: continuation. Rationale: the original prompt is INHERITED from the
#: rollout (Codex re-attaches to the prior turn); the trailing positional
#: argument on ``codex exec resume`` is the *next-turn* user message.
#: A short, deterministic stub keeps DBOS step-state replay reproducible
#: (no timestamps, no fresh UUIDs, no env-derived strings) while still
#: giving the subagent a clear "continue where you left off" signal.
CODEX_RESUME_CONTINUATION_PROMPT: str = (
    "Resume after Capo restart. Continue the previous task; "
    "the original brief is inherited from the rollout."
)


def _read_row_for_resume(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """SELECT the row fields needed by the resume step.

    Returns ``None`` when the row is missing. Otherwise a dict with keys
    ``status``, ``session_id_subagent``, ``workspace``, ``model``,
    ``agent``. Wrapped in :func:`asyncio.to_thread` by callers so the
    event loop isn't pinned on SQLite.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, session_id_subagent, workspace, model, agent "
            "FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _update_row_pid(db_path: Path, delegation_id: str, pid: int | None) -> None:
    """UPDATE the row's ``pid`` column after a successful resume-spawn.

    Goes through :func:`begin_immediate_with_retry` for the same §7.3
    zero-``SQLITE_BUSY``-to-caller contract the rest of the module honors.
    """
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation=f"resume_update_pid:{delegation_id}"
        ):
            conn.execute(
                "UPDATE delegations SET pid = ? WHERE id = ?",
                (pid, delegation_id),
            )
    finally:
        conn.close()


def _build_resume_argv(
    *,
    binary: str,
    session_id: str,
    model: str | None,
) -> list[str]:
    """Build the ``claude --resume <id>`` argv per spec §7.5.

    Per spec §7.5 / spike S-3 §4.2 the canonical resume invocation is::

        claude --resume <session_id> --output-format stream-json --verbose

    ``-p`` is NOT passed on resume — the prompt is inherited from the
    original rollout. Per-delegation ``--model`` is preserved when the row
    captured one, since CC honors per-resume model overrides.
    """
    argv: list[str] = [
        binary,
        "--resume",
        session_id,
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    if model:
        argv.extend(["--model", model])
    return argv


def _build_codex_resume_argv(
    *,
    binary: str,
    thread_id: str,
    model: str | None,
    continuation_prompt: str = CODEX_RESUME_CONTINUATION_PROMPT,
) -> list[str]:
    """Build the ``codex exec resume <thread_id> "<continuation>"`` argv.

    Per spec §5.4 / §7.5 (re-amended by spike S-1 on 2026-05-11) the
    canonical resume invocation is::

        codex exec resume --json --skip-git-repo-check [--model <m>] \
            <thread_id> "<continuation_prompt>"

    Per S-1 §4.2, ``codex exec resume`` does **NOT** accept ``--sandbox``,
    ``--ask-for-approval``, ``-C`` (``--cd``), or ``--add-dir`` — those
    flags inherit from the original rollout and passing them errors with
    ``error: unexpected argument '--sandbox' found``. The argv builder
    below therefore omits them by construction.

    Per-delegation ``--model`` is preserved when the row captured one;
    Codex honors per-resume model overrides the same way CC does.

    The trailing positional argument is the *next-turn* user message, not
    a re-statement of the brief (the brief is inherited from the
    rollout). Capo uses a deterministic
    :data:`CODEX_RESUME_CONTINUATION_PROMPT` so workflow replay produces
    identical argv (§5.6 restart-resume reproducibility).
    """
    argv: list[str] = [
        binary,
        "exec",
        "resume",
        "--json",
        "--skip-git-repo-check",
    ]
    if model:
        argv.extend(["--model", model])
    argv.append(thread_id)
    argv.append(continuation_prompt)
    return argv


def _first_event_is_error(event: dict[str, Any] | None) -> bool:
    """Return True when the resumed CC's first event signals a fatal error.

    Per S-3 §4.2: a ``--resume`` with an unknown session id emits a single
    ``result`` event with ``is_error: true``. Capo MUST treat that as a
    fatal spawn failure and refuse to overwrite ``session_id_subagent``.

    The check tolerates the field being missing (older CC versions or
    non-result first events) — in that case we treat it as "not an error"
    and continue, matching the §5.6 fast-path.
    """
    if not isinstance(event, dict):
        return False
    is_error = event.get("is_error")
    return bool(is_error)


async def _terminate_resume_process(process: Any) -> None:
    """Best-effort kill of a resume-spawned subprocess.

    Mirrors :func:`capo.tools.claude_code._terminate_process` but kept
    local so this module's resume path doesn't need to import from
    ``capo.tools.claude_code`` (avoids a circular dependency once
    Task #35 wires the spawn site back into this workflow).
    """
    terminate = getattr(process, "terminate", None)
    kill = getattr(process, "kill", None)
    wait = getattr(process, "wait", None)
    if callable(terminate):
        with contextlib.suppress(Exception):
            terminate()
    if callable(wait):
        with contextlib.suppress(Exception):
            await asyncio.wait_for(wait(), timeout=0.5)
        if getattr(process, "returncode", None) is None and callable(kill):
            with contextlib.suppress(Exception):
                kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(wait(), timeout=0.5)


async def _execute_resume_spawn(
    *,
    delegation_id: str,
    db_path_obj: Path,
    argv: list[str],
    workspace: str | None,
    session_id: str,
    agent_kind: str,
    binary_label: str,
    check_first_event_is_error: bool,
) -> dict[str, Any]:
    """Shared spawn-driver for both CC and Codex resume paths.

    The two flows are byte-identical past the argv builder modulo one
    branch: CC inspects the first event for ``is_error=true`` (per
    S-3 §4.2 — ``claude --resume <unknown>`` emits a synthetic
    ``result`` event with ``is_error: true``) and refuses to overwrite
    the session id. Codex does NOT have that branch — an unknown
    ``thread_id`` causes the subprocess to exit non-zero with no JSON
    event at all (handled by the timeout path). Codex's ``thread.started``
    on a successful resume re-emits the SAME ``thread_id``; the reader's
    first-event hook would normally UPDATE ``session_id_subagent`` to
    that value, which is a no-op write of the original. We rely on the
    Phase-2 ``session_id_subagent`` overwrite contract being "first
    write wins" so an already-populated value stays — see Task #45 +
    spike S-1.

    Parameters
    ----------
    delegation_id, db_path_obj, argv, workspace, session_id:
        Spawn context.
    agent_kind:
        ``"claude-code"`` or ``"codex"`` — used only for log events so
        operators can disambiguate the two paths.
    binary_label:
        Human-readable name of the binary (``"claude"`` / ``"codex"``)
        — used only for the ``binary_missing`` failure-reason string.
    check_first_event_is_error:
        ``True`` for CC (S-3 §4.2 contract). ``False`` for Codex — the
        equivalent failure is "subprocess exits before first event" and
        is covered by the timeout branch.
    """
    process: Any | None = None
    reader_conn: sqlite3.Connection | None = None
    reader_handle: ReaderHandle | None = None
    try:
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=workspace,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_RESUME_SUBPROCESS_STREAM_LIMIT,
            )
        except FileNotFoundError as exc:
            reason = f"{RESUME_REASON_FAILED}: {binary_label} binary not found"
            await asyncio.to_thread(
                _persist_terminal_status,
                db_path_obj,
                delegation_id,
                status=STATUS_FAILED,
                summary=reason,
            )
            logger.error(
                "resume spawn: %s binary missing for %s: %r",
                binary_label,
                delegation_id,
                exc,
                extra={
                    "event": "capo.workflows.delegation.resume.binary_missing",
                    "delegation_id": delegation_id,
                    "agent": agent_kind,
                },
            )
            return {
                "action": "resume_failed_binary_missing",
                "delegation_id": delegation_id,
                "reason": reason,
                "agent": agent_kind,
            }

        reader_conn = open_connection(db_path_obj)
        reader_handle = start_reader(delegation_id, process, store=reader_conn)

        # Wait for the first event so we can inspect ``is_error`` (CC) or
        # confirm the thread re-attached (Codex). Either way, the timeout
        # branch covers the "subprocess died before emitting any JSON".
        try:
            await asyncio.wait_for(
                reader_handle.first_event_received.wait(),
                timeout=RESUME_FIRST_EVENT_TIMEOUT_S,
            )
        except TimeoutError:
            reason = (
                f"{RESUME_REASON_FAILED}: no first event within "
                f"{RESUME_FIRST_EVENT_TIMEOUT_S}s"
            )
            await _terminate_resume_process(process)
            with contextlib.suppress(Exception):
                await reader_handle.wait()
            with contextlib.suppress(Exception):
                reader_conn.close()
            await asyncio.to_thread(
                _persist_terminal_status,
                db_path_obj,
                delegation_id,
                status=STATUS_FAILED,
                summary=reason,
            )
            return {
                "action": "resume_failed_no_first_event",
                "delegation_id": delegation_id,
                "reason": reason,
                "agent": agent_kind,
            }

        first_event = reader_handle.first_event
        if check_first_event_is_error and _first_event_is_error(first_event):
            # S-3 §4.2 (CC only): resume-failure first event. Refuse to
            # overwrite session_id_subagent. Mark failed, kill the child.
            await _terminate_resume_process(process)
            with contextlib.suppress(Exception):
                await reader_handle.wait()
            with contextlib.suppress(Exception):
                reader_conn.close()
            await asyncio.to_thread(
                _persist_terminal_status,
                db_path_obj,
                delegation_id,
                status=STATUS_FAILED,
                summary=RESUME_REASON_FAILED,
            )
            logger.error(
                "resume spawn: is_error=true on first event for %s — "
                "refusing to overwrite session_id_subagent",
                delegation_id,
                extra={
                    "event": "capo.workflows.delegation.resume.is_error",
                    "delegation_id": delegation_id,
                    "agent": agent_kind,
                    "first_event_keys": (
                        sorted(first_event.keys())
                        if isinstance(first_event, dict)
                        else None
                    ),
                },
            )
            return {
                "action": "resume_failed_is_error",
                "delegation_id": delegation_id,
                "reason": RESUME_REASON_FAILED,
                "agent": agent_kind,
            }

        # Success — register the new live handle and refresh PID.
        new_pid = getattr(process, "pid", None)
        await asyncio.to_thread(
            _update_row_pid, db_path_obj, delegation_id, new_pid
        )
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader_handle,
                db_path=db_path_obj,
            )
        )
        logger.info(
            "resume spawn: re-attached delegation %s via %s resume "
            "(session_id=%s, new_pid=%s)",
            delegation_id,
            binary_label,
            session_id,
            new_pid,
            extra={
                "event": "capo.workflows.delegation.resume.spawned",
                "delegation_id": delegation_id,
                "agent": agent_kind,
                "session_id": session_id,
                "new_pid": new_pid,
            },
        )
        return {
            "action": "spawned",
            "delegation_id": delegation_id,
            "pid": new_pid,
            "agent": agent_kind,
        }
    except BaseException:
        # Unwind partial state on any failure: kill subprocess, close
        # reader conn. Re-raise so DBOS marks the step failed.
        if reader_handle is not None:
            with contextlib.suppress(Exception):
                reader_handle.task.cancel()
        if process is not None:
            with contextlib.suppress(Exception):
                await _terminate_resume_process(process)
        if reader_conn is not None:
            with contextlib.suppress(Exception):
                reader_conn.close()
        raise


@idempotent_step(
    step_name="monitor_delegation_resume_spawn",
    deterministic_args=lambda *args, **kwargs: (
        args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
    ),
)
async def _resume_spawn_step(
    delegation_id: str,
    db_path: str,
    claude_binary: str,
    codex_binary: str = DEFAULT_CODEX_BINARY,
) -> dict[str, Any]:
    """Spawn-or-resume a subagent subprocess for ``delegation_id``.

    Implements the §5.6 restart contract for **both** Claude Code (CC)
    and Codex agents. Task #46 extended the CC-only Task #31 step to
    dispatch on the row's ``agent`` column.

    The step body:

    1. Honors an existing registered handle (single-process replay): if the
       in-process registry already has a live handle for ``delegation_id``,
       returns ``{"action": "skipped_registered"}`` immediately. This is the
       idempotency guard for the "re-entered twice within one process"
       case — DBOS step caching covers the same property at the workflow
       level; the registry check covers callers running outside a real
       workflow runtime (unit tests).
    2. Reads the row. Missing row → return action ``"row_missing"``; the
       caller transitions to ``failed``.
    3. Already-terminal row → return action ``"row_terminal"`` with the
       observed status; caller skips spawning entirely (Edge case row:
       "Subprocess already terminated before workflow re-entry").
    4. ``agent`` column is NULL or unrecognized → persist
       :data:`RESUME_REASON_UNKNOWN_AGENT` on the row and return action
       ``"unknown_agent"``. The argv contract differs per-agent and a
       wrong-binary spawn would corrupt the rollout.
    5. Missing ``session_id_subagent`` → persist
       :data:`RESUME_REASON_LOST_SESSION_ID` on the row and return action
       ``"missing_session_id"``.
    6. Dispatch on ``agent``:

       * ``claude-code`` → spawn ``claude --resume <session_id> …``
         (no ``-p``). On first event with ``is_error=true`` (per S-3 §4.2)
         → kill subprocess, mark failed, refuse to overwrite session id,
         return action ``"resume_failed_is_error"``.
       * ``codex`` → spawn ``codex exec resume --json
         --skip-git-repo-check [--model <m>] <thread_id>
         "<continuation>"`` (per spec §5.4 / §7.5 + spike S-1 §4.2).
         NEVER passes ``--sandbox``, ``--ask-for-approval``, ``-C``, or
         ``--add-dir`` — those flags inherit from the original rollout
         and would error on resume. An unknown thread_id causes the
         subprocess to exit non-zero with NO JSON event, handled by the
         shared timeout branch.

    7. On success → register the new
       :class:`DelegationProcessHandle`, UPDATE the row's PID, and return
       action ``"spawned"`` along with the new PID.

    **Single-threaded resume per thread_id**: ``codex exec resume`` is
    single-threaded per ``thread_id`` (spec §7.5 "Concurrency"). The
    per-delegation ``status='running'`` mutex on the ``delegations`` row
    already enforces this — the resume step never fires for a row in a
    terminal state (branch #3) and cannot fire twice for the same row
    within a process (branch #1: registry guard) or across replays
    within the same workflow attempt (DBOS step-state cache).

    Parameters
    ----------
    delegation_id:
        Deterministic input (the idempotency key).
    db_path:
        Absolute path to ``state.db`` (string — DBOS serializes step args).
    claude_binary:
        Path or basename of the Claude Code binary.
    codex_binary:
        Path or basename of the Codex binary. Defaults to
        :data:`DEFAULT_CODEX_BINARY`.

    Returns
    -------
    dict
        Action discriminator plus context. Always JSON-serializable so DBOS
        can pickle it to ``dbos.db``.
    """
    db_path_obj = Path(db_path)

    # 1. Registry guard — at-most-once spawn within a process lifetime.
    if _lookup_delegation(delegation_id) is not None:
        return {"action": "skipped_registered", "delegation_id": delegation_id}

    # 2. Row lookup.
    row = await asyncio.to_thread(
        _read_row_for_resume, db_path_obj, delegation_id
    )
    if row is None:
        return {"action": "row_missing", "delegation_id": delegation_id}

    # 3. Already-terminal row — derive status from the row; do NOT respawn.
    if row.get("status") in _TERMINAL_STATUSES:
        return {
            "action": "row_terminal",
            "delegation_id": delegation_id,
            "status": row["status"],
        }

    # 4. Agent-column dispatch — must come BEFORE the session-id check so
    #    a NULL agent on an otherwise-resumable row surfaces the right
    #    reason. Treat NULL, empty string, and any value not in the
    #    recognized set as "unknown agent type".
    agent_kind = row.get("agent")
    if not isinstance(agent_kind, str) or agent_kind not in {
        AGENT_CLAUDE_CODE,
        AGENT_CODEX,
    }:
        await asyncio.to_thread(
            _persist_terminal_status,
            db_path_obj,
            delegation_id,
            status=STATUS_FAILED,
            summary=RESUME_REASON_UNKNOWN_AGENT,
        )
        logger.error(
            "resume spawn: unknown agent type for %s: %r — refusing to spawn",
            delegation_id,
            agent_kind,
            extra={
                "event": "capo.workflows.delegation.resume.unknown_agent",
                "delegation_id": delegation_id,
                "agent": agent_kind,
            },
        )
        return {
            "action": "unknown_agent",
            "delegation_id": delegation_id,
            "agent": agent_kind,
            "reason": RESUME_REASON_UNKNOWN_AGENT,
        }

    # 5. Missing session id — mark failed.
    session_id = row.get("session_id_subagent")
    if not session_id or not isinstance(session_id, str):
        await asyncio.to_thread(
            _persist_terminal_status,
            db_path_obj,
            delegation_id,
            status=STATUS_FAILED,
            summary=RESUME_REASON_LOST_SESSION_ID,
        )
        return {
            "action": "missing_session_id",
            "delegation_id": delegation_id,
            "agent": agent_kind,
            "reason": RESUME_REASON_LOST_SESSION_ID,
        }

    # 6. Build argv per-agent and dispatch.
    workspace = row.get("workspace")
    model = row.get("model")
    if agent_kind == AGENT_CLAUDE_CODE:
        argv = _build_resume_argv(
            binary=claude_binary, session_id=session_id, model=model
        )
        return await _execute_resume_spawn(
            delegation_id=delegation_id,
            db_path_obj=db_path_obj,
            argv=argv,
            workspace=workspace,
            session_id=session_id,
            agent_kind=AGENT_CLAUDE_CODE,
            binary_label="claude",
            check_first_event_is_error=True,
        )
    # agent_kind == AGENT_CODEX (validated above).
    argv = _build_codex_resume_argv(
        binary=codex_binary, thread_id=session_id, model=model
    )
    return await _execute_resume_spawn(
        delegation_id=delegation_id,
        db_path_obj=db_path_obj,
        argv=argv,
        workspace=workspace,
        session_id=session_id,
        agent_kind=AGENT_CODEX,
        binary_label="codex",
        # Codex doesn't emit a synthetic is_error first event on resume
        # failure — it exits non-zero with no JSON output (covered by the
        # shared timeout branch).
        check_first_event_is_error=False,
    )


# === End Restart resume (Task #31) =========================================


# ---------------------------------------------------------------------------
# Workflow body
# ---------------------------------------------------------------------------


def _classify_returncode(
    returncode: int, stderr_tail: str | None
) -> tuple[str, str | None]:
    """Map a process ``returncode`` to ``(status, summary)``.

    * ``rc == 0`` → ``("completed", None)``.
    * ``rc < 0`` and ``-rc`` in :data:`_KILLED_SIGNALS` → ``("killed",
      "killed by signal SIG…")``.
    * Otherwise → ``("failed", "exit code N[: <stderr_tail>]")``.
    """
    if returncode == 0:
        return STATUS_COMPLETED, None
    # POSIX: negative returncode means "terminated by signal -rc". macOS
    # subprocess module preserves this convention.
    if returncode < 0:
        sig = -returncode
        if sig in _KILLED_SIGNALS:
            try:
                sig_name = signal.Signals(sig).name
            except ValueError:
                sig_name = f"signal({sig})"
            return STATUS_KILLED, f"killed by {sig_name}"
        # Other fatal signals (SEGV, ABRT, FPE...) are crashes — failed.
        try:
            sig_name = signal.Signals(sig).name
        except ValueError:
            sig_name = f"signal({sig})"
        return STATUS_FAILED, f"crashed: {sig_name}"
    # Positive nonzero — subprocess returned a nonzero exit code.
    if stderr_tail:
        return STATUS_FAILED, f"exit code {returncode}: {stderr_tail}"
    return STATUS_FAILED, f"exit code {returncode}"


async def _capture_stderr_tail(handle: DelegationProcessHandle) -> str | None:
    """Best-effort tail of stderr for the failed-row ``summary``.

    Reads up to :data:`_STDERR_TAIL_BYTES` from the subprocess's
    ``stderr`` pipe non-blockingly. Returns ``None`` if stderr was already
    fully drained by the reader (the common case — the reader owns the
    pipe). In that case the caller surfaces just the exit code.

    Why not query ``delegation_output`` for the last stderr row? The
    monitor runs concurrently with the reader's batched flush — the
    last few hundred bytes might still be in the inserter's buffer
    when we check. Reading from the subprocess pipe directly would
    interfere with the reader. So we leave this as ``None`` for now and
    let the operator scroll the delegation_output rows via
    ``get_delegation_output`` — Phase 3 task #32 (``summarize_run``) will
    enrich the summary with an LLM-generated one-liner anyway.
    """
    del handle  # placeholder API — see docstring.
    return None


async def _poll_returncode(
    handle: DelegationProcessHandle,
    *,
    poll_interval_s: float,
    max_runtime_s: float | None,
) -> int | None:
    """Poll ``handle.process.returncode`` until exit or timeout.

    Returns the integer exit code, or ``None`` if ``max_runtime_s``
    elapsed first. The polling loop also checks the row's status each
    cycle so a user-initiated ``kill_delegation`` is observed quickly
    (the killer updates the row to ``killed``; our caller then exits the
    workflow without overwriting).

    We don't ``await process.wait()`` directly because that would block
    forever on a hung subprocess and prevent us from observing
    out-of-band kill signals via the row. Polling at 1 s resolution is
    coarse enough to be cheap and fine enough to be UX-responsive.
    """
    deadline: float | None = None
    if max_runtime_s is not None:
        deadline = asyncio.get_event_loop().time() + max_runtime_s

    while True:
        rc = getattr(handle.process, "returncode", None)
        if rc is not None:
            return int(rc)

        if deadline is not None and asyncio.get_event_loop().time() >= deadline:
            return None

        # Each polling cycle: a small await so the cancellation path can
        # cut in. Cancellation here means the workflow was stopped (e.g.
        # DBOS.destroy on shutdown) — DBOS persists step state, the
        # next process incarnation resumes from here.
        await asyncio.sleep(poll_interval_s)


async def _resolve_agent_kind_for_span(
    delegation_id: str, db_path_str: str | None
) -> str:
    """Best-effort agent-kind lookup for the §6.5 monitor span name.

    The DBOS workflow body cannot grow new parameters without breaking
    in-flight pickled invocations, so the agent kind (``claude-code`` or
    ``codex``) is fetched lazily here:

    * If a registry handle exists and the row is cached, use that.
    * Otherwise read the ``agent`` column from ``delegations``.
    * On any failure or unknown value, fall back to ``"unknown"`` —
      the span name (``capo.delegation.unknown.monitor``) is still a
      valid §6.5 pattern instance for Logfire prefix queries.
    """
    if not db_path_str:
        return "unknown"
    try:
        row = await asyncio.to_thread(
            _read_row_for_resume, Path(db_path_str), delegation_id
        )
    except Exception:  # noqa: BLE001 - never break observability
        return "unknown"
    if not isinstance(row, dict):
        return "unknown"
    agent = row.get("agent")
    if isinstance(agent, str) and agent in {AGENT_CLAUDE_CODE, AGENT_CODEX}:
        return agent
    return "unknown"


# Note: Task #51 migrated the workflow body to use
# :func:`capo.observability.delegation_monitor_span` directly. The legacy
# ``_logfire_span_or_logger`` / ``_stdlib_span`` helpers have been removed
# — the §6.5 audit test (``tests/test_span_taxonomy.py``) requires every
# production span to come from a named constructor.


async def _monitor_body(
    delegation_id: str,
    *,
    poll_interval_s: float,
    max_runtime_s: float | None,
    db_path: Path | None = None,
    claude_binary: str = DEFAULT_CLAUDE_BINARY,
    codex_binary: str = DEFAULT_CODEX_BINARY,
) -> MonitorResult:
    """Inner workflow body — separated out so the ``@DBOS.workflow``
    decorator just wraps a thin adapter (registration happens lazily in
    :func:`_register_workflow`).

    The body's contract:

    1. Look up the live subprocess + reader handle for ``delegation_id``.
    2. Start the drain step (DBOS-checkpointed) concurrently with the
       returncode poll.
    3. When the subprocess exits, classify the returncode → status.
    4. If the row was killed out-of-band during the poll loop, honor
       that and exit cleanly without overwriting.
    5. Persist the terminal status via the idempotent persistence step.
    6. Return a :class:`MonitorResult` for callers that want it.

    All branches mark the row terminal or surface a clear exception that
    the workflow wrapper catches and turns into a ``failed`` row + Logfire
    span.
    """
    handle = _lookup_delegation(delegation_id)
    if handle is None:
        # ==== Restart-resume entry point (Task #31) =========================
        # The in-process registry has no live handle — workflow re-entered
        # after a Capo restart (or the spawn site never registered, e.g. an
        # operator-driven dry-run). Per §5.6 the workflow's first step on
        # re-entry is to re-spawn via ``claude --resume <session_id>``.
        # ====================================================================
        if db_path is None:
            # Without a db_path, we can't read the row or persist the
            # failure. Caller MUST thread db_path through monitor_delegation
            # kwargs once Task #35 owns the spawn site. Surface as failed.
            msg = (
                f"resume requested for delegation {delegation_id!r} but "
                "no db_path was threaded through monitor_delegation; "
                "cannot read session_id from row."
            )
            logger.error(
                msg,
                extra={
                    "event": "capo.workflows.delegation.resume.no_db_path",
                    "delegation_id": delegation_id,
                },
            )
            return MonitorResult(
                delegation_id=delegation_id,
                status=STATUS_FAILED,
                returncode=None,
                summary=msg,
            )

        resume_result = await _resume_spawn_step(
            delegation_id, str(db_path), claude_binary, codex_binary
        )
        action = (
            resume_result.get("action")
            if isinstance(resume_result, dict)
            else None
        )

        # Branches that terminate without further monitoring.
        if action in (
            "missing_session_id",
            "unknown_agent",
            "resume_failed_is_error",
            "resume_failed_no_first_event",
            "resume_failed_binary_missing",
        ):
            return MonitorResult(
                delegation_id=delegation_id,
                status=STATUS_FAILED,
                returncode=None,
                summary=resume_result.get("reason"),
            )
        if action == "row_missing":
            return MonitorResult(
                delegation_id=delegation_id,
                status=STATUS_FAILED,
                returncode=None,
                summary=(
                    f"resume requested for missing row {delegation_id!r}"
                ),
            )
        if action == "row_terminal":
            # Already-terminal row (subprocess finished before re-entry).
            # Honor the persisted status without re-spawning.
            return MonitorResult(
                delegation_id=delegation_id,
                status=resume_result.get("status", STATUS_FAILED),
                returncode=None,
                summary=None,
            )

        # action in ("spawned", "skipped_registered") → the registry now
        # has a live handle. Look it up and continue with normal monitoring.
        handle = _lookup_delegation(delegation_id)
        if handle is None:
            # Defense-in-depth: should never happen since both
            # "spawned" and "skipped_registered" guarantee a registered
            # handle. If we get here, mark failed and bail.
            msg = (
                f"resume step returned {action!r} but registry still has "
                f"no handle for {delegation_id!r}"
            )
            logger.error(
                msg,
                extra={
                    "event": (
                        "capo.workflows.delegation.resume.no_handle_post_step"
                    ),
                    "delegation_id": delegation_id,
                },
            )
            return MonitorResult(
                delegation_id=delegation_id,
                status=STATUS_FAILED,
                returncode=None,
                summary=msg,
            )

    db_path = handle.db_path

    # Start the drain step. The drain is itself a @DBOS.step so its
    # progress (bytes drained, rows persisted) is checkpointed — on
    # workflow re-entry within the same process, DBOS replays its cached
    # return. We kick it off as a task and the poll loop watches the
    # subprocess concurrently.
    drain_task: asyncio.Task[dict[str, Any]] = asyncio.create_task(
        _drain_output_step(delegation_id),
        name=f"monitor-drain-{delegation_id}",
    )

    # === Task #34 hook: heartbeat poller =================================
    # Run a concurrent poller that emits a @DBOS.step heartbeat to the
    # user at each frozen wall-clock threshold (default 5m/15m/60m). Each
    # ``(delegation_id, threshold_seconds)`` pair has its OWN idempotency
    # key so workflow restart cannot duplicate or skip a heartbeat at the
    # same threshold. The poller is cancelled as soon as the subprocess
    # exits (terminal status path skips remaining thresholds — spec §5.13
    # "Delegation completes before any heartbeat threshold: no heartbeats
    # sent"). Defensive try-import keeps this hook tolerant of symbol
    # arrival order during parallel wave execution (Task #33 pattern).
    heartbeat_task: asyncio.Task[Any] | None = None
    try:
        from capo.workflows.delegation import (  # noqa: PLC0415
            _run_heartbeat_poller as _hb_poller,
        )
    except ImportError:
        _hb_poller = None  # type: ignore[assignment]
    if _hb_poller is not None:
        heartbeat_task = asyncio.create_task(
            _hb_poller(delegation_id, db_path),
            name=f"monitor-heartbeat-{delegation_id}",
        )

    try:
        returncode = await _poll_returncode(
            handle,
            poll_interval_s=poll_interval_s,
            max_runtime_s=max_runtime_s,
        )
    except BaseException:
        # Polling itself crashed (cancellation, internal bug) — make sure
        # the drain task + heartbeat task are reaped before we re-raise
        # so they don't leak.
        drain_task.cancel()
        if heartbeat_task is not None:
            heartbeat_task.cancel()
        with contextlib.suppress(BaseException):
            await drain_task
        if heartbeat_task is not None:
            with contextlib.suppress(BaseException):
                await heartbeat_task
        raise

    # Subprocess exited — cancel the heartbeat poller. Any threshold not
    # yet crossed will NOT fire (§5.13 "Delegation completes before any
    # heartbeat threshold: no heartbeats sent").
    if heartbeat_task is not None:
        heartbeat_task.cancel()
        with contextlib.suppress(BaseException):
            await heartbeat_task

    # Subprocess exited (or max_runtime_s elapsed). Wait for the drain to
    # finish so all output is in ``delegation_output`` before we publish
    # the terminal row. The reader naturally exits on EOF after the
    # subprocess closes its pipes.
    try:
        drain_summary = await asyncio.wait_for(drain_task, timeout=30.0)
    except TimeoutError:
        # Reader stuck after subprocess exit — cancel it and continue.
        # We still publish the terminal row; output is best-effort here.
        logger.warning(
            "monitor_delegation: drain step timed out after subprocess "
            "exit for delegation %s; cancelling reader",
            delegation_id,
            extra={
                "event": "capo.workflows.delegation.drain_timeout",
                "delegation_id": delegation_id,
            },
        )
        drain_task.cancel()
        with contextlib.suppress(BaseException):
            await drain_task
        drain_summary = {}

    # Honor an out-of-band kill: if the row is already ``killed``, the user
    # (or Phase 4 approval flow) wrote that during our poll loop. Leave
    # it alone and return.
    current_status = await asyncio.to_thread(_read_status, db_path, delegation_id)
    if current_status in _TERMINAL_STATUSES and current_status != STATUS_RUNNING:
        return MonitorResult(
            delegation_id=delegation_id,
            status=current_status,
            returncode=returncode,
            summary=None,
        )

    # Translate the returncode into our terminal status.
    if returncode is None:
        # max_runtime_s elapsed without a returncode. Treat as failed —
        # the spec's perf criterion has the workflow run for hours, so
        # this only fires when the test harness sets a tight timeout or
        # the subprocess legitimately hung.
        status = STATUS_FAILED
        summary = (
            f"monitor timed out after {max_runtime_s}s without subprocess exit"
        )
    else:
        stderr_tail = await _capture_stderr_tail(handle)
        status, summary = _classify_returncode(returncode, stderr_tail)

    # Enrich the summary with drain-side info on failure paths so the
    # operator sees what was captured.
    if status == STATUS_FAILED and drain_summary:
        evt_rows = drain_summary.get("event_rows", 0)
        out_rows = drain_summary.get("stdout_rows", 0)
        err_rows = drain_summary.get("stderr_rows", 0)
        if summary:
            summary = (
                f"{summary} (drained event={evt_rows} stdout={out_rows} "
                f"stderr={err_rows})"
            )

    # === Task #32 hook: summarize_run BEFORE terminal persist ============
    # ``summarize_run`` (a ``@DBOS.step`` keyed by ``delegation_id``)
    # writes a deterministic terminal one-liner into the row's ``summary``
    # column. We capture its return value and feed it into
    # ``_persist_terminal_step`` so the subsequent UPDATE writes the SAME
    # summary value (rather than overwriting with the classifier-derived
    # heuristic above). If ``summarize_run`` raises unexpectedly, fall
    # back to the heuristic — a missing summary is a degraded UX, not a
    # workflow failure. The deterministic_args lambda is ``delegation_id``
    # only, so DBOS replays the same string on re-entry regardless of
    # which terminal status path got us here.
    try:
        run_summary = await summarize_run(
            delegation_id, status, str(db_path)
        )
        if run_summary:
            summary = run_summary
    except Exception:  # noqa: BLE001 — degraded UX, not a workflow failure
        logger.exception(
            "summarize_run failed for %s; falling back to heuristic summary",
            delegation_id,
            extra={
                "event": "capo.workflows.delegation.summarize_failed",
                "delegation_id": delegation_id,
            },
        )

    await _persist_terminal_step(
        delegation_id, status, summary, str(db_path)
    )

    # === Task #33 hook: notify_user AFTER terminal persist ===============
    # Direct AMC send, no agent re-entry. Failures here do NOT mutate the
    # row's terminal status — the delegation is still terminal; the user
    # just didn't get the courtesy notification. Permanent AMC errors
    # propagate so DBOS marks the workflow failed (and an operator can
    # inspect the cause); transient errors are retried by DBOS itself.
    #
    # Defensive import — mirrors the Task #32 hook above so this branch is
    # tolerant of Task #33's symbol arrival order (parallel-task safety
    # during Phase 3 wave execution; once Task #33 lands the symbols, the
    # import simply succeeds).
    try:
        from capo.workflows.delegation import (  # noqa: PLC0415
            notify_user as _notify_user,
        )
    except ImportError:
        _notify_user = None  # type: ignore[assignment]
    try:
        from capo.workflows.delegation import (  # noqa: PLC0415
            NotifyError as _NotifyError,
        )
    except ImportError:
        _NotifyError = None  # type: ignore[assignment]
    if _notify_user is not None:
        try:
            await _notify_user(delegation_id, str(db_path))
        except Exception as _notify_exc:  # noqa: BLE001
            if _NotifyError is not None and isinstance(
                _notify_exc, _NotifyError
            ):
                # Permanent AMC error (PLATFORM_AUTH, CHANNEL_NOT_FOUND, ...)
                # — log + re-raise so DBOS marks the workflow failed, but
                # the row stays terminal (already persisted above).
                logger.exception(
                    "notify_user permanent failure for %s; row remains "
                    "terminal but user was not notified",
                    delegation_id,
                    extra={
                        "event": "capo.workflows.delegation.notify_failed",
                        "delegation_id": delegation_id,
                    },
                )
                raise
            # Other exceptions are transient — let DBOS retry.
            raise

    return MonitorResult(
        delegation_id=delegation_id,
        status=status,
        returncode=returncode,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Public workflow entrypoint
# ---------------------------------------------------------------------------


_workflow_lock = threading.Lock()
_workflow_registered = False
_monitor_workflow: Any | None = None


def _register_workflow() -> Any:
    """Register :func:`_monitor_workflow_inner` with DBOS lazily.

    DBOS requires :meth:`DBOS.launch` to have run before
    ``@DBOS.workflow()`` can register a workflow. We delay the registration
    until the first call to :func:`monitor_delegation` (or to
    :func:`get_monitor_workflow`) so this module can be imported during
    boot — before DBOS is launched — without crashing.

    Returns the registered workflow callable. Idempotent: repeated calls
    return the same wrapped function.
    """
    global _workflow_registered, _monitor_workflow
    with _workflow_lock:
        if _workflow_registered and _monitor_workflow is not None:
            return _monitor_workflow

        try:
            from dbos import DBOS  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "dbos package not importable — capo.workflows.delegation "
                "requires DBOS to be installed and launched"
            ) from exc

        @DBOS.workflow()
        async def monitor_delegation_workflow(
            delegation_id: str,
            poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
            max_runtime_s: float | None = DEFAULT_MAX_RUNTIME_S,
            db_path_str: str | None = None,
            claude_binary: str = DEFAULT_CLAUDE_BINARY,
            codex_binary: str = DEFAULT_CODEX_BINARY,
        ) -> MonitorResult:
            """DBOS-registered ``monitor_delegation`` workflow body.

            Acceptance criterion: ``@DBOS.workflow()`` with
            ``delegation_id`` as the deterministic input. The polling
            interval and max-runtime ceiling are tunables; both default
            to the §5.6 values (1s / unlimited).

            ``db_path_str`` + ``claude_binary`` are threaded through so
            the restart-resume contract (Task #31) can re-spawn the
            subprocess on workflow re-entry when the in-process registry
            is empty. They are taken as plain strings rather than
            :class:`Path` because DBOS pickles workflow args to ``dbos.db``
            and ``Path`` has no stable JSON encoding.
            """
            # Spec §6.5: ``capo.delegation.<agent>.monitor``. The
            # workflow body doesn't receive the agent kind as a
            # parameter (would break the DBOS argument-pickling
            # signature for already-pickled in-flight workflows), so
            # we resolve it from the row best-effort. On a fresh row
            # or any failure to read, fall back to the generic
            # ``unknown`` slug — the span name STILL satisfies the
            # spec pattern, and Logfire queries on the family prefix
            # (``capo.delegation.``) still match.
            _agent_kind = await _resolve_agent_kind_for_span(
                delegation_id, db_path_str
            )
            with delegation_monitor_span(
                delegation_id=delegation_id,
                agent=_agent_kind,
                step_name="workflow",
            ):
                try:
                    return await _monitor_body(
                        delegation_id,
                        poll_interval_s=poll_interval_s,
                        max_runtime_s=max_runtime_s,
                        db_path=(
                            Path(db_path_str) if db_path_str else None
                        ),
                        claude_binary=claude_binary,
                        codex_binary=codex_binary,
                    )
                except BaseException as exc:
                    # §5.6 acceptance criterion: "Workflow exception
                    # surfaced via Logfire span and reflected on the row
                    # as ``failed``." The span context manager records
                    # the exception; here we ALSO best-effort UPDATE the
                    # row so the user-visible state reflects the crash.
                    logger.exception(
                        "monitor_delegation crashed for %s",
                        delegation_id,
                        extra={
                            "event": "capo.workflows.delegation.crash",
                            "delegation_id": delegation_id,
                        },
                    )
                    handle = _lookup_delegation(delegation_id)
                    if handle is not None:
                        with contextlib.suppress(Exception):
                            await asyncio.to_thread(
                                _persist_terminal_status,
                                handle.db_path,
                                delegation_id,
                                status=STATUS_FAILED,
                                summary=f"monitor crashed: {exc!r}",
                            )
                    raise

        _monitor_workflow = monitor_delegation_workflow
        _workflow_registered = True
        return _monitor_workflow


def get_monitor_workflow() -> Any:
    """Return the DBOS-registered workflow callable, registering on first call.

    Tests and Task #35 (handoff in :mod:`capo.tools.claude_code`) use
    this to obtain the workflow without depending on import-time DBOS
    state.
    """
    return _register_workflow()


async def monitor_delegation(
    delegation_id: str,
    *,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    max_runtime_s: float | None = DEFAULT_MAX_RUNTIME_S,
    db_path: Path | None = None,
    claude_binary: str = DEFAULT_CLAUDE_BINARY,
    codex_binary: str = DEFAULT_CODEX_BINARY,
) -> MonitorResult:
    """Run the DBOS-managed monitor for ``delegation_id`` and return its result.

    This is the public entrypoint. Callers (Task #35:
    ``delegate_to_claude_code``) invoke this AFTER the subprocess is
    spawned and registered via :func:`register_delegation_subprocess`.

    Parameters
    ----------
    delegation_id:
        Foreign key on ``delegations.id``. The deterministic input to the
        DBOS workflow per spec §5.6.
    poll_interval_s:
        Returncode polling interval in seconds. Default
        :data:`DEFAULT_POLL_INTERVAL_S` (1.0). Tests override to keep the
        integration suite fast.
    max_runtime_s:
        Optional safety ceiling — when set, the workflow returns
        ``status='failed'`` if the subprocess hasn't exited by then.
        Default :data:`DEFAULT_MAX_RUNTIME_S` (``None`` — no ceiling).

    Returns
    -------
    MonitorResult
        Captures the final ``(status, returncode, summary)``. Note that
        the DURABLE signal is the ``delegations`` row — callers should
        treat this return value as a convenience, not the source of
        truth.

    Raises
    ------
    Any exception the workflow body raises. DBOS marks the workflow
    failed in ``dbos.db`` in addition to our row-level ``failed`` update.
    """
    workflow = _register_workflow()
    # DBOS-decorated workflows are async — await directly.
    return await workflow(
        delegation_id,
        poll_interval_s,
        max_runtime_s,
        str(db_path) if db_path is not None else None,
        claude_binary,
        codex_binary,
    )


# ===========================================================================
# === notify_user (Task #33) ================================================
# ===========================================================================
#
# Spec §5.6 "terminal completion message" + §5.12 (delegation completion UX)
# + §7.5 (AMC ``Idempotency-Key`` header). Sends ONE AMC message directly to
# the user when a delegation reaches a terminal status (completed / failed /
# killed). The message body is composed deterministically from the
# ``delegations`` row so DBOS step replay produces the SAME bytes — no
# timestamps, no fresh UUIDs, no random formatting. The HTTP-level
# idempotency key is derived from the step's own
# ``.idempotency_key_for(delegation_id)`` so AMC server-side dedupe and
# DBOS at-most-once replay align on the same token.
#
# Critical invariant: NO agent re-entry. This is a terminal courtesy
# notification — we do NOT spawn another delegation, do NOT call the LLM,
# do NOT read SOUL files. The message body is a short, factual completion
# string derived only from ``(status, summary, delegation_id)`` columns.
#
# Lifecycle wiring:
#   * The application bootstrap (``capo.main.amain`` or equivalent) calls
#     :func:`register_amc_sender` with an awaitable that takes
#     ``(channel_id, text, idempotency_key)`` and returns the AMC send
#     result. The wired implementation in production is a thin wrapper
#     around :meth:`capo.transport.amc_client.AMCClient.send`.
#   * Inside the workflow, :func:`notify_user` looks up that sender,
#     reads the row's ``(status, summary, parent_thread_id)``, composes
#     the body, computes the idempotency key, and invokes the sender.
#   * If no sender is registered (e.g. in a test that doesn't exercise
#     AMC), :func:`notify_user` raises :class:`NotifyError` with code
#     ``NO_SENDER_REGISTERED``. The orchestrator branch in
#     :func:`_monitor_body` treats permanent NotifyErrors as workflow-
#     failure-but-row-already-terminal (see comment there).
# ---------------------------------------------------------------------------


class NotifyError(Exception):
    """Permanent failure delivering the terminal notification to the user.

    Raised when the AMC send hits a non-retryable error (e.g.
    ``PLATFORM_AUTH``, ``CHANNEL_NOT_FOUND``, ``VALIDATION_ERROR``) OR
    when no AMC sender is registered. Carries the AMC error code so
    operators can grep logs for a specific failure mode.

    Transient errors (5xx, network) do NOT raise ``NotifyError`` — they
    propagate the original exception so DBOS's at-least-once step
    semantics drive the retry. The AMC ``Idempotency-Key`` header keeps
    the receiver deduped across those retries.
    """

    def __init__(self, code: str, message: str, *, delegation_id: str) -> None:
        self.code = code
        self.message = message
        self.delegation_id = delegation_id
        super().__init__(f"{code}: {message} (delegation_id={delegation_id})")


#: AMC error codes that are PERMANENT — surfaced to operators as
#: :class:`NotifyError`. Mirrors
#: :data:`capo.transport.amc_client._NON_RETRYABLE_CODES` but inlined here
#: to avoid importing a private name from the transport package.
_NOTIFY_PERMANENT_CODES: frozenset[str] = frozenset(
    {"PLATFORM_AUTH", "CHANNEL_NOT_FOUND", "ATTACHMENT_TOO_LARGE", "VALIDATION_ERROR"}
)


# A coroutine signature for the registered AMC sender. Keeping this typed
# as a plain ``Callable`` (rather than importing the AMCClient type) keeps
# this module independent of the transport package at import time — useful
# because the AMCClient pulls httpx into the import graph.
#
# Contract:
#   * ``await sender(channel_id, text, idempotency_key)`` MUST return a
#     value with a ``.message_id`` attribute (matches
#     :class:`capo.transport.amc_client.SendResult`).
#   * Transient transport errors propagate (DBOS retries the step).
#   * Permanent errors raise something with a ``.code`` attribute whose
#     value is in :data:`_NOTIFY_PERMANENT_CODES`.
_AmcSenderFn = Callable[[str, str, str], Awaitable[Any]]

_amc_sender_lock = threading.Lock()
_AMC_SENDER: _AmcSenderFn | None = None


def register_amc_sender(sender: _AmcSenderFn | None) -> None:
    """Register (or clear) the AMC sender used by :func:`notify_user`.

    Called by :func:`capo.main.amain` after the :class:`AMCClient` is
    constructed. Pass ``None`` to clear the registration (e.g. on
    teardown). Idempotent: the latest registration wins.

    The sender is a coroutine
    ``(channel_id, text, idempotency_key) -> SendResult``. The
    ``idempotency_key`` flows verbatim into the AMC ``Idempotency-Key``
    HTTP header so AMC dedupe aligns with DBOS dedupe.
    """
    global _AMC_SENDER
    with _amc_sender_lock:
        _AMC_SENDER = sender


def _lookup_amc_sender() -> _AmcSenderFn | None:
    """Return the registered sender, or ``None`` if no AMC client is wired."""
    with _amc_sender_lock:
        return _AMC_SENDER


def _channel_id_from_parent_thread(parent_thread_id: str | None) -> str | None:
    """Recover the AMC ``channel_id`` from a Capo ``parent_thread_id``.

    ``capo.memory.conversation.thread_id_for_amc`` composes the thread id
    as ``f"amc:{channel_id}"`` (§5.7). We invert that here. Anything that
    doesn't carry the ``amc:`` prefix means the delegation was issued
    out-of-band (e.g. a Phase-4 batch job) and we have no user channel to
    notify — return ``None`` and let the caller log + skip the send.
    """
    if not parent_thread_id:
        return None
    prefix = "amc:"
    if not parent_thread_id.startswith(prefix):
        return None
    channel_id = parent_thread_id[len(prefix) :]
    return channel_id or None


def _read_row_for_notify(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """Read the row columns we need to compose the notify body.

    Returns ``None`` if the row is missing. Run under an ``open_connection``
    to keep the read consistent with the persistence path (same PRAGMA
    settings as the rest of the monitor).
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, summary, parent_thread_id "
            "FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _compose_notify_body(
    *, delegation_id: str, status: str, summary: str | None
) -> str:
    """Deterministic message body for the terminal completion notification.

    Format (newline-joined, factual, short)::

        Delegation <id>: <STATUS>
        <summary>

    The same ``(delegation_id, status, summary)`` MUST produce the same
    bytes on every call — DBOS step replay relies on this so the AMC
    receiver sees the same payload across at-most-once retries. NO
    timestamps, NO formatted dates, NO env reads, NO randomness.

    If ``summary`` is ``None`` or empty (or only whitespace), the second
    line is omitted (one line of body is still a complete notification).
    """
    # ``status`` is a CHECK-constrained column value; uppercase it for
    # human readability but never reformat the underlying string.
    status_label = status.upper() if status else "UNKNOWN"
    head = f"Delegation {delegation_id}: {status_label}"
    if summary:
        # Collapse any embedded newlines so the AMC body is single-block.
        # Use a literal space — no formatting hooks for locale or timestamps.
        cleaned = summary.replace("\r\n", " ").replace("\n", " ").strip()
        if cleaned:
            return f"{head}\n{cleaned}"
    return head


@idempotent_step(
    step_name="notify_user",
    deterministic_args=lambda *args, **kwargs: (
        args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
    ),
)
async def notify_user(
    delegation_id: str, db_path: str
) -> dict[str, Any]:
    """Send ONE terminal completion message to the user via AMC.

    Acceptance criteria (Task #33, spec §5.6 / §5.12 / §7.5):

    * ``@DBOS.step`` + ``@idempotent_step`` keyed on ``delegation_id``.
    * Direct AMC ``send`` only — no agent re-entry, no LLM call, no
      SOUL file reads. The message body is composed deterministically
      from ``(status, summary, delegation_id)``.
    * ``Idempotency-Key`` HTTP header is the same string returned by
      ``notify_user.idempotency_key_for(delegation_id, db_path)`` so
      AMC's server-side dedupe and DBOS's step replay align on one
      token.
    * On transient AMC errors (5xx, network) the wrapped exception
      propagates and DBOS retries the step — the at-most-once contract
      is preserved by AMC's dedupe on the idempotency key.
    * On permanent AMC errors (``PLATFORM_AUTH``, ``CHANNEL_NOT_FOUND``,
      ``VALIDATION_ERROR``, ``ATTACHMENT_TOO_LARGE``) we wrap as
      :class:`NotifyError` exposing ``.code`` and re-raise. The row is
      already terminal (persisted by ``_persist_terminal_step``); only
      the notification was lost.

    Parameters
    ----------
    delegation_id:
        The row's PK; also the deterministic input to the idempotency key.
    db_path:
        Absolute path to ``state.db`` (as a string for DBOS-serializable
        step args; see :func:`_persist_terminal_step` for the same
        convention).

    Returns
    -------
    dict
        ``{"delegation_id", "channel_id", "message_id", "idempotency_key",
        "notified"}`` — a small audit trail. ``notified`` is ``True`` on
        a successful AMC ack; ``False`` when the row is missing, has no
        channel, or has a non-terminal status (no-op cases).

    Raises
    ------
    NotifyError
        On permanent AMC errors or when no AMC sender is registered.
    Exception
        Transient AMC / network errors propagate unchanged so DBOS
        retries the step. The ``Idempotency-Key`` keeps the receiver
        deduped across attempts.
    """
    row = await asyncio.to_thread(_read_row_for_notify, Path(db_path), delegation_id)
    if row is None:
        logger.warning(
            "notify_user: no delegations row for id=%s — skipping send",
            delegation_id,
            extra={
                "event": "capo.workflows.delegation.notify_no_row",
                "delegation_id": delegation_id,
            },
        )
        return {
            "delegation_id": delegation_id,
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    status = str(row.get("status") or "")
    if status not in _TERMINAL_STATUSES:
        # Defensive: caller wired notify_user before _persist_terminal_step.
        # Refuse to notify on a non-terminal row — we'd send a misleading
        # "still running" line. The Phase 3 wiring in _monitor_body
        # invariably calls notify_user AFTER persist, so this branch is a
        # safety net rather than a hot path.
        logger.warning(
            "notify_user: row %s is not terminal (status=%r) — skipping",
            delegation_id,
            status,
            extra={
                "event": "capo.workflows.delegation.notify_non_terminal",
                "delegation_id": delegation_id,
                "row_status": status,
            },
        )
        return {
            "delegation_id": delegation_id,
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    parent_thread_id = row.get("parent_thread_id")
    channel_id = _channel_id_from_parent_thread(parent_thread_id)
    if channel_id is None:
        # Delegation came in without an AMC channel — nothing to notify.
        logger.info(
            "notify_user: delegation %s has no AMC channel "
            "(parent_thread_id=%r) — skipping send",
            delegation_id,
            parent_thread_id,
            extra={
                "event": "capo.workflows.delegation.notify_no_channel",
                "delegation_id": delegation_id,
                "parent_thread_id": parent_thread_id,
            },
        )
        return {
            "delegation_id": delegation_id,
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    summary = row.get("summary")
    body = _compose_notify_body(
        delegation_id=delegation_id,
        status=status,
        summary=summary if isinstance(summary, str) else None,
    )

    # Derive the idempotency key from THIS step's own
    # ``.idempotency_key_for(delegation_id, db_path)``. Only the FIRST
    # positional arg (delegation_id) flows into the key per the
    # decorator's ``deterministic_args`` lambda, so passing ``db_path``
    # here is fine — it's ignored by the key derivation but matches the
    # call's positional signature. The framework uses the current DBOS
    # workflow id when available; under replay the same id is reused so
    # the key is stable across crashes.
    idem_key = notify_user.idempotency_key_for(  # type: ignore[attr-defined]
        delegation_id, db_path
    )

    sender = _lookup_amc_sender()
    if sender is None:
        raise NotifyError(
            code="NO_SENDER_REGISTERED",
            message=(
                "no AMC sender registered with capo.workflows.delegation — "
                "call register_amc_sender() during boot before launching "
                "monitor workflows"
            ),
            delegation_id=delegation_id,
        )

    try:
        result = await sender(channel_id, body, idem_key)
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code in _NOTIFY_PERMANENT_CODES:
            # Permanent error — wrap so the caller can branch on it.
            message = getattr(exc, "message", None) or str(exc)
            raise NotifyError(
                code=code,
                message=str(message),
                delegation_id=delegation_id,
            ) from exc
        # Transient — propagate so DBOS retries. The AMC idempotency key
        # keeps the receiver deduped across attempts.
        logger.warning(
            "notify_user: transient AMC error for %s; DBOS will retry "
            "(idempotency_key=%s, err=%r)",
            delegation_id,
            idem_key,
            exc,
            extra={
                "event": "capo.workflows.delegation.notify_transient",
                "delegation_id": delegation_id,
                "idempotency_key": idem_key,
            },
        )
        raise

    message_id = getattr(result, "message_id", None)
    logger.info(
        "notify_user: delivered terminal notification for %s "
        "(message_id=%s, idempotency_key=%s)",
        delegation_id,
        message_id,
        idem_key,
        extra={
            "event": "capo.workflows.delegation.notify_sent",
            "delegation_id": delegation_id,
            "message_id": message_id,
            "idempotency_key": idem_key,
        },
    )
    return {
        "delegation_id": delegation_id,
        "channel_id": channel_id,
        "message_id": message_id,
        "idempotency_key": idem_key,
        "notified": True,
    }


# ===========================================================================
# === end notify_user (Task #33) ============================================
# ===========================================================================


# ===========================================================================
# === heartbeat (Task #34) ==================================================
# ===========================================================================
#
# Spec §5.13: emit a periodic progress notification to the user at frozen
# wall-clock thresholds (default 5m / 15m / 60m). Each
# ``(delegation_id, threshold_seconds)`` pair has its OWN idempotency key so
# workflow restart cannot duplicate or skip a heartbeat at the same
# threshold — restart-resume reproducibility (§5.6) is preserved by:
#
#   * Reading the row's ``started_at`` (durable wall-clock anchor) rather
#     than ``asyncio.get_event_loop().time()`` (process-local). On
#     workflow restart, the same ``started_at`` yields the same elapsed,
#     and the same threshold-crossings → the same idempotency keys.
#   * Wrapping the step body with :func:`idempotent_step` keyed by
#     ``(delegation_id, str(threshold_seconds))``. DBOS step-state +
#     in-process cache then make the side effect at-most-once across
#     restarts.
#   * Composing the message body deterministically from
#     ``delegations.summary`` + a frozen threshold label — NO timestamps,
#     NO ``datetime.now()``, NO env reads.
#
# Lifecycle wiring (mirrors notify_user/Task #33):
#   * The application bootstrap registers the AMC sender via
#     :func:`register_amc_sender` (Task #33). The heartbeat step reuses
#     that same sender — the step-name prefix (``heartbeat`` vs
#     ``notify_user``) keeps idempotency-key namespaces disjoint.
#   * The poller :func:`_run_heartbeat_poller` runs as an asyncio task
#     alongside the drain task inside :func:`_monitor_body`. It is
#     cancelled the moment the subprocess exits, so a fast-completing
#     delegation emits zero heartbeats (§5.13 acceptance criterion).
#   * On workflow re-entry after a restart, the poller re-reads
#     ``started_at`` and re-fires each threshold-step that's already
#     crossed; DBOS step-state replays the cached return without re-
#     sending, satisfying "exactly-once delivery per threshold".
# ---------------------------------------------------------------------------


#: Frozen default thresholds in seconds: 5 minutes, 15 minutes, 1 hour.
#: Spec §5.13. The configured override is read via
#: :func:`set_heartbeat_thresholds` (called by ``capo.main.amain`` after
#: settings are loaded); falling back to this constant keeps unit tests
#: importable without a full settings rig.
DEFAULT_HEARTBEAT_THRESHOLDS_S: tuple[int, ...] = (300, 900, 3600)

#: Maximum interval the poller sleeps between threshold-crossing checks.
#: We MUST poll at least this often so we don't miss a threshold by more
#: than this much. Tests override via :func:`set_heartbeat_poll_interval`
#: to keep integration suites fast (sub-second polling).
_DEFAULT_HEARTBEAT_POLL_INTERVAL_S: float = 5.0

# Module-level configuration (mutable via the setters below). The pattern
# mirrors the Task #33 ``_AMC_SENDER`` registry: a process-singleton
# updated by ``capo.main.amain`` during boot, with sensible defaults for
# import-time and tests.
_heartbeat_config_lock = threading.Lock()
_HEARTBEAT_THRESHOLDS_S: tuple[int, ...] = DEFAULT_HEARTBEAT_THRESHOLDS_S
_HEARTBEAT_POLL_INTERVAL_S: float = _DEFAULT_HEARTBEAT_POLL_INTERVAL_S


def set_heartbeat_thresholds(
    thresholds: Sequence[int] | tuple[int, ...] | list[int] | None,
) -> None:
    """Configure the heartbeat thresholds. ``None`` resets to spec defaults.

    Called by :func:`capo.main.amain` after settings load (passes
    ``settings.heartbeat.intervals_seconds`` from :mod:`capo.config`).
    Any positive ascending sequence is accepted; non-positive entries
    raise ``ValueError`` (mirrors the validator on
    :class:`~capo.config.HeartbeatConfig`).

    Each call replaces the prior configuration atomically. A workflow
    that's already running picks up the change on its next poll cycle —
    but per-threshold idempotency keys are unaffected, so a threshold
    that already fired stays fired.
    """
    global _HEARTBEAT_THRESHOLDS_S
    if thresholds is None:
        with _heartbeat_config_lock:
            _HEARTBEAT_THRESHOLDS_S = DEFAULT_HEARTBEAT_THRESHOLDS_S
        return
    frozen = tuple(int(t) for t in thresholds)
    if not frozen:
        raise ValueError("heartbeat thresholds must be non-empty")
    if any(t <= 0 for t in frozen):
        raise ValueError("heartbeat thresholds must be positive")
    with _heartbeat_config_lock:
        _HEARTBEAT_THRESHOLDS_S = frozen


def get_heartbeat_thresholds() -> tuple[int, ...]:
    """Return the current frozen heartbeat thresholds (read-only snapshot)."""
    with _heartbeat_config_lock:
        return _HEARTBEAT_THRESHOLDS_S


def set_heartbeat_poll_interval(seconds: float | None) -> None:
    """Configure the heartbeat poll interval (seconds between elapsed checks).

    Tests override this to keep integration suites fast (e.g. 0.01s so
    crossing a 0.05s "threshold" fires within a single test). Production
    boot leaves the default (5s) which is plenty given the smallest
    threshold is 300s.
    """
    global _HEARTBEAT_POLL_INTERVAL_S
    if seconds is None:
        with _heartbeat_config_lock:
            _HEARTBEAT_POLL_INTERVAL_S = _DEFAULT_HEARTBEAT_POLL_INTERVAL_S
        return
    s = float(seconds)
    if s <= 0:
        raise ValueError("heartbeat poll interval must be positive")
    with _heartbeat_config_lock:
        _HEARTBEAT_POLL_INTERVAL_S = s


def get_heartbeat_poll_interval() -> float:
    """Return the current heartbeat poll interval (seconds)."""
    with _heartbeat_config_lock:
        return _HEARTBEAT_POLL_INTERVAL_S


def _format_threshold_label(threshold_seconds: int) -> str:
    """Render a threshold as a short human-readable label.

    The label is part of the deterministic message body — so the format
    is intentionally minimal and stable. Examples::

        300   -> "5m"
        900   -> "15m"
        3600  -> "1h"
        7200  -> "2h"
        45    -> "45s"
        7245  -> "2h45s"

    Pure function. No locale, no env, no timestamp.
    """
    s = int(threshold_seconds)
    if s <= 0:
        return "0s"
    hours, rem = divmod(s, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    return "".join(parts) if parts else "0s"


def _compose_heartbeat_body(
    *,
    delegation_id: str,
    threshold_seconds: int,
    summary: str | None,
) -> str:
    """Deterministic heartbeat message body.

    Format (newline-joined, factual, short)::

        Delegation <id> still running (<threshold-label>)
        <summary>

    The same ``(delegation_id, threshold_seconds, summary)`` MUST produce
    the same bytes on every call — DBOS step replay relies on this so
    the AMC receiver sees the same payload across at-most-once retries.
    NO timestamps, NO formatted dates, NO env reads, NO randomness.

    If ``summary`` is ``None`` or empty (or only whitespace), the second
    line is omitted (one line of body is still a complete heartbeat).
    """
    label = _format_threshold_label(threshold_seconds)
    head = f"Delegation {delegation_id} still running ({label})"
    if summary:
        cleaned = summary.replace("\r\n", " ").replace("\n", " ").strip()
        if cleaned:
            return f"{head}\n{cleaned}"
    return head


def _read_row_for_heartbeat(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """Read the row columns the heartbeat step needs.

    Returns ``None`` if the row is missing. Symmetric with
    :func:`_read_row_for_notify` so the no-op branches mirror exactly.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, summary, parent_thread_id, started_at "
            "FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


@idempotent_step(
    step_name="heartbeat",
    deterministic_args=lambda *args, **kwargs: (
        # Spec §5.13: ``(delegation_id, str(threshold_seconds))`` is the
        # idempotency key. Two positional args expected; tolerate kwargs
        # for DBOS step-args forwarding flexibility.
        args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
        str(args[1]) if len(args) >= 2 else str(kwargs.get("threshold_seconds")),
    ),
)
async def heartbeat(
    delegation_id: str,
    threshold_seconds: int,
    db_path: str,
) -> dict[str, Any]:
    """Send ONE heartbeat message to the user via AMC at a threshold crossing.

    Acceptance criteria (Task #34, spec §5.13):

    * ``@DBOS.step`` + ``@idempotent_step`` keyed by
      ``(delegation_id, str(threshold_seconds))``.
    * Reuses the Task #33 :data:`_AMC_SENDER` registry — the step-name
      prefix (``heartbeat`` vs ``notify_user``) keeps idempotency-key
      namespaces disjoint.
    * ``Idempotency-Key`` HTTP header equals
      ``heartbeat.idempotency_key_for(delegation_id, threshold_seconds, db_path)``.
    * Message body is deterministic: derived from ``delegations.summary``
      + the threshold label. No timestamps in payload.
    * No-op branches (return ``notified=False``, no exception):
        - row missing,
        - row already terminal (delegation finished),
        - parent_thread_id without ``amc:`` prefix (no AMC channel).
      This matches the §5.13 spec edge case "No AMC channel on row:
      heartbeat is a no-op (same pattern as notify_user)".

    Parameters
    ----------
    delegation_id:
        Foreign key on ``delegations.id``; first deterministic arg.
    threshold_seconds:
        The wall-clock threshold (in seconds) being signalled.
        Stringified for the idempotency-key per spec §5.13.
    db_path:
        Absolute path to ``state.db`` (as a string for DBOS-serializable
        step args).

    Returns
    -------
    dict
        ``{"delegation_id", "threshold_seconds", "channel_id",
        "message_id", "idempotency_key", "notified"}`` audit trail.
        ``notified`` is ``True`` on a successful AMC ack; ``False`` for
        no-op branches.

    Raises
    ------
    NotifyError
        On permanent AMC errors or when no AMC sender is registered
        (reuses the Task #33 typed error).
    Exception
        Transient AMC / network errors propagate unchanged so DBOS
        retries the step. The ``Idempotency-Key`` keeps the receiver
        deduped across attempts.
    """
    row = await asyncio.to_thread(
        _read_row_for_heartbeat, Path(db_path), delegation_id
    )
    if row is None:
        logger.warning(
            "heartbeat: no delegations row for id=%s — skipping send",
            delegation_id,
            extra={
                "event": "capo.workflows.delegation.heartbeat_no_row",
                "delegation_id": delegation_id,
                "threshold_seconds": int(threshold_seconds),
            },
        )
        return {
            "delegation_id": delegation_id,
            "threshold_seconds": int(threshold_seconds),
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    status = str(row.get("status") or "")
    if status in _TERMINAL_STATUSES:
        # Delegation already finished — a heartbeat would be misleading.
        # The poller normally cancels before we ever reach a terminal-
        # row state, but on a tight race (subprocess exit + threshold
        # crossing in the same tick) we defensively skip here.
        logger.info(
            "heartbeat: row %s is terminal (status=%r) — skipping send",
            delegation_id,
            status,
            extra={
                "event": "capo.workflows.delegation.heartbeat_terminal_row",
                "delegation_id": delegation_id,
                "row_status": status,
                "threshold_seconds": int(threshold_seconds),
            },
        )
        return {
            "delegation_id": delegation_id,
            "threshold_seconds": int(threshold_seconds),
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    parent_thread_id = row.get("parent_thread_id")
    channel_id = _channel_id_from_parent_thread(parent_thread_id)
    if channel_id is None:
        logger.info(
            "heartbeat: delegation %s has no AMC channel "
            "(parent_thread_id=%r) — skipping send",
            delegation_id,
            parent_thread_id,
            extra={
                "event": "capo.workflows.delegation.heartbeat_no_channel",
                "delegation_id": delegation_id,
                "parent_thread_id": parent_thread_id,
                "threshold_seconds": int(threshold_seconds),
            },
        )
        return {
            "delegation_id": delegation_id,
            "threshold_seconds": int(threshold_seconds),
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    summary = row.get("summary")
    body = _compose_heartbeat_body(
        delegation_id=delegation_id,
        threshold_seconds=int(threshold_seconds),
        summary=summary if isinstance(summary, str) else None,
    )

    # Derive the idempotency key from THIS step's own
    # ``.idempotency_key_for(delegation_id, threshold_seconds, db_path)``.
    # Per §5.13 only ``(delegation_id, str(threshold_seconds))`` drives
    # the key — the lambda above ignores ``db_path``.
    idem_key = heartbeat.idempotency_key_for(  # type: ignore[attr-defined]
        delegation_id, int(threshold_seconds), db_path
    )

    sender = _lookup_amc_sender()
    if sender is None:
        raise NotifyError(
            code="NO_SENDER_REGISTERED",
            message=(
                "no AMC sender registered with capo.workflows.delegation — "
                "call register_amc_sender() during boot before launching "
                "monitor workflows"
            ),
            delegation_id=delegation_id,
        )

    try:
        result = await sender(channel_id, body, idem_key)
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code in _NOTIFY_PERMANENT_CODES:
            message = getattr(exc, "message", None) or str(exc)
            raise NotifyError(
                code=code,
                message=str(message),
                delegation_id=delegation_id,
            ) from exc
        logger.warning(
            "heartbeat: transient AMC error for %s thr=%ds; DBOS will retry "
            "(idempotency_key=%s, err=%r)",
            delegation_id,
            int(threshold_seconds),
            idem_key,
            exc,
            extra={
                "event": "capo.workflows.delegation.heartbeat_transient",
                "delegation_id": delegation_id,
                "idempotency_key": idem_key,
                "threshold_seconds": int(threshold_seconds),
            },
        )
        raise

    message_id = getattr(result, "message_id", None)
    logger.info(
        "heartbeat: delivered for %s thr=%ds (message_id=%s, "
        "idempotency_key=%s)",
        delegation_id,
        int(threshold_seconds),
        message_id,
        idem_key,
        extra={
            "event": "capo.workflows.delegation.heartbeat_sent",
            "delegation_id": delegation_id,
            "message_id": message_id,
            "idempotency_key": idem_key,
            "threshold_seconds": int(threshold_seconds),
        },
    )
    return {
        "delegation_id": delegation_id,
        "threshold_seconds": int(threshold_seconds),
        "channel_id": channel_id,
        "message_id": message_id,
        "idempotency_key": idem_key,
        "notified": True,
    }


def _elapsed_since_started(
    started_at: Any, *, now: datetime | None = None
) -> float | None:
    """Return wall-clock seconds elapsed since ``started_at``.

    ``started_at`` is a SQLite ``TIMESTAMP`` value (TEXT). We reuse
    :func:`_summary_parse_db_timestamp` for parsing so the behavior
    matches the Task #32 runtime field exactly. Returns ``None`` if
    ``started_at`` is unparseable, missing, or in the future.

    The optional ``now`` parameter is for unit tests (deterministic
    elapsed without monkeypatching :func:`datetime.now`). When omitted
    we read ``datetime.now(UTC)`` — this read is the ONE wall-clock
    side effect of the heartbeat poller; it's confined to the poller
    layer, NEVER the step body or the message payload (which stay
    deterministic per the spec).
    """
    start = _summary_parse_db_timestamp(started_at)
    if start is None:
        return None
    current = now if now is not None else datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    elapsed = (current - start).total_seconds()
    if elapsed < 0:
        return None
    return elapsed


async def _run_heartbeat_poller(
    delegation_id: str,
    db_path: Path,
) -> dict[str, Any]:
    """Poll wall-clock elapsed and fire :func:`heartbeat` at each crossing.

    Runs as an asyncio task alongside the drain task in
    :func:`_monitor_body`. The poller:

    1. Resolves the configured thresholds (frozen ascending order).
    2. Reads ``started_at`` from the row ONCE; if missing/unparseable the
       poller exits silently (heartbeat is best-effort UX, not a workflow
       failure).
    3. Loops: sleeps ``get_heartbeat_poll_interval()`` seconds, computes
       elapsed, and invokes the :func:`heartbeat` step for each threshold
       whose elapsed has now crossed it. Already-fired thresholds are
       tracked in-process; cross-restart idempotency is the step's job
       (DBOS step-state + ``@idempotent_step`` keyed per threshold).
    4. Exits cleanly on :class:`asyncio.CancelledError` (caller's signal
       that the subprocess has exited or the workflow is shutting down).

    Returns a small audit dict ``{"fired", "elapsed_at_exit"}``.
    """
    thresholds = get_heartbeat_thresholds()
    if not thresholds:
        return {"fired": [], "elapsed_at_exit": 0.0}

    # Snapshot the row's started_at — we anchor elapsed off the durable
    # column rather than a process-local clock so workflow restart re-
    # computes the same elapsed and re-fires the same threshold-set
    # (whose step idempotency cache catches the replay).
    row = await asyncio.to_thread(
        _read_row_for_heartbeat, db_path, delegation_id
    )
    started_at = row.get("started_at") if row else None
    if started_at is None:
        logger.info(
            "heartbeat poller: no started_at on row %s — exiting",
            delegation_id,
            extra={
                "event": "capo.workflows.delegation.heartbeat_no_started_at",
                "delegation_id": delegation_id,
            },
        )
        return {"fired": [], "elapsed_at_exit": 0.0}

    fired: list[int] = []
    elapsed: float = 0.0
    try:
        while True:
            elapsed_value = await asyncio.to_thread(
                _elapsed_since_started, started_at
            )
            if elapsed_value is not None:
                elapsed = float(elapsed_value)
                for threshold in thresholds:
                    if int(threshold) in fired:
                        continue
                    if elapsed >= float(threshold):
                        # Fire the step. Errors (NotifyError, transient)
                        # propagate out of the poller — but since the
                        # poller is cancelled on subprocess exit AND the
                        # step's idempotency key keeps retries from
                        # duplicating, we just log and continue so a
                        # single misbehaving threshold doesn't suppress
                        # later ones.
                        try:
                            await heartbeat(
                                delegation_id, int(threshold), str(db_path)
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "heartbeat poller: step raised for %s "
                                "thr=%ds — continuing",
                                delegation_id,
                                int(threshold),
                                extra={
                                    "event": (
                                        "capo.workflows.delegation"
                                        ".heartbeat_step_error"
                                    ),
                                    "delegation_id": delegation_id,
                                    "threshold_seconds": int(threshold),
                                },
                            )
                        finally:
                            fired.append(int(threshold))
            await asyncio.sleep(get_heartbeat_poll_interval())
    except asyncio.CancelledError:
        # Normal exit path: caller cancelled us because the subprocess
        # exited (or the workflow is shutting down).
        return {"fired": list(fired), "elapsed_at_exit": float(elapsed)}


# ===========================================================================
# === end heartbeat (Task #34) ==============================================
# ===========================================================================


__all__ = [
    "AGENT_CLAUDE_CODE",
    "AGENT_CODEX",
    "CODEX_RESUME_CONTINUATION_PROMPT",
    "DEFAULT_CLAUDE_BINARY",
    "DEFAULT_CODEX_BINARY",
    "DEFAULT_HEARTBEAT_THRESHOLDS_S",
    "DEFAULT_MAX_RUNTIME_S",
    "DEFAULT_POLL_INTERVAL_S",
    "RESUME_FIRST_EVENT_TIMEOUT_S",
    "RESUME_REASON_FAILED",
    "RESUME_REASON_LOST_SESSION_ID",
    "RESUME_REASON_UNKNOWN_AGENT",
    "STATUS_COMPLETED",
    "STATUS_FAILED",
    "STATUS_KILLED",
    "STATUS_RUNNING",
    "DelegationProcessHandle",
    "MonitorResult",
    "NotifyError",
    "get_heartbeat_poll_interval",
    "get_heartbeat_thresholds",
    "get_monitor_workflow",
    "heartbeat",
    "monitor_delegation",
    "notify_user",
    "register_amc_sender",
    "register_delegation_subprocess",
    "set_heartbeat_poll_interval",
    "set_heartbeat_thresholds",
    "summarize_run",
    "unregister_delegation_subprocess",
]

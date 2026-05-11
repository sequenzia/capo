"""Agent-facing tools for delegation status / output / kill / list (spec §5.5).

This module implements the four read/control tools the agent invokes to
inspect or terminate Claude Code (and later Codex) delegations:

* :func:`check_delegation_status` — side-effect-free status snapshot.
* :func:`get_delegation_output` — tail recent ``stdout`` / ``event`` chunks.
* :func:`kill_delegation` — request SIGTERM (always goes through approval
  in Phase 4; Phase 2 just raises :class:`ApprovalRequired`).
* :func:`list_delegations` — recent delegations for the current ``user_id``.

Phase 2 vs. Phase 4
-------------------

Per spec §5.8, every ``kill_delegation`` invocation **requires user
approval** — there is no "force-kill from the agent" path in the V1 user
contract. Phase 2 raises :class:`ApprovalRequired` so the agent loop has a
stable failure signal; Phase 4 (task #41) wires the actual approval
prompt and routes back into :func:`_kill_delegation_forced` once the user
says yes. The integration test in task #27 exercises the forced path
directly to prove the kill code path works end-to-end.

Schema notes (spec §7.3 / migrations/versions/001_init.py)
---------------------------------------------------------

* The PK column on ``delegations`` is ``id`` (this module accepts a
  ``delegation_id: str`` argument that maps to it). The §5.5 acceptance
  criteria's return-key name is ``delegation_id`` — that's what the agent
  sees.
* ``delegations`` has no dedicated ``last_activity_ts`` column. We derive
  it from ``MAX(delegation_output.ts)`` per delegation. For delegations
  that haven't emitted any output yet, ``last_activity_ts`` falls back to
  ``started_at`` so the field is always populated.
* ``delegations`` has no dedicated ``error_reason`` column. The kill
  ``reason`` is persisted to ``summary`` (the existing free-form field
  the §5.6 monitor uses for terminal annotations).
* "Summary one line" in §5.5 maps to the ``summary`` column directly —
  it's expected to be one paragraph max (§5.6) and §5.5 callers
  treat ``None`` as "not yet summarized".
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic_ai import RunContext

from capo.deps import CapoDeps
from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.tools.basic import ApprovalRequired

logger = logging.getLogger("capo.tools.delegations")


# ---------------------------------------------------------------------------
# Tunables.
# ---------------------------------------------------------------------------

#: Hard cap on ``get_delegation_output(tail_lines=...)``. Values above this
#: are silently clamped — per §5.5 acceptance criteria.
MAX_TAIL_LINES: int = 1000

#: Default tail size when the agent doesn't pass one.
DEFAULT_TAIL_LINES: int = 200

#: Streams the agent is allowed to see via :func:`get_delegation_output`.
#: ``stderr`` is intentionally **excluded**: per §5.5 the agent reads the
#: user-relevant child output; stderr is operator-debug only and lives in
#: Logfire / the row's ``summary`` on failure.
_VISIBLE_STREAMS: tuple[str, ...] = ("stdout", "event")

#: SQLite ``status`` values considered terminal (§7.3 CHECK constraint).
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "killed"}
)

#: How long to wait after SIGTERM before escalating to SIGKILL. The spec
#: doesn't pin a value; 5s mirrors the §6.1 "fail fast" expectation while
#: still giving Claude Code time to flush its final event.
_SIGTERM_GRACE_SECONDS: float = 5.0


# ---------------------------------------------------------------------------
# Errors.
# ---------------------------------------------------------------------------


class DelegationNotFound(Exception):
    """Raised when an operation references a ``delegation_id`` not in the DB.

    Used by :func:`check_delegation_status` and :func:`_kill_delegation_forced`.
    :func:`get_delegation_output` returns an empty list instead (consistent
    with "no output yet") so the agent can poll cheaply without exception
    handling for the common race where it asks for output the same instant
    the row was inserted.
    """

    def __init__(self, delegation_id: str) -> None:
        self.delegation_id = delegation_id
        super().__init__(f"delegation not found: {delegation_id!r}")


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """ISO-8601 UTC timestamp matching the format used by ``claude_code.py``.

    Kept locally rather than imported so this module can stand alone; the
    format is stable (§7.3) and both writers parse it back to ``datetime``
    via :func:`_parse_iso`.
    """
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _parse_iso(value: str | None) -> datetime | None:
    """Parse a timestamp written by ``_utc_now_iso`` (or SQLite's default).

    Handles the three shapes we may see in the DB:
    * ``"YYYY-MM-DD HH:MM:SS.ffffff"`` — ``_utc_now_iso`` output.
    * ``"YYYY-MM-DD HH:MM:SS"`` — SQLite ``CURRENT_TIMESTAMP`` default.
    * ``None`` — column is NULL (e.g. ``ended_at`` while still running).

    Returns ``None`` for NULL / unparseable inputs so callers can treat
    "no timestamp" uniformly without try/except.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    # ``fromisoformat`` accepts both "YYYY-MM-DD HH:MM:SS.ffffff" and
    # the no-microseconds form on Python 3.11+.
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    # Assume naive timestamps are UTC (matches `_utc_now_iso`).
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _runtime_seconds(
    started_at: str | None,
    ended_at: str | None,
    status: str,
) -> float | None:
    """Return wall-clock runtime in seconds.

    * ``status='running'``: ``now() - started_at``.
    * Terminal status: ``ended_at - started_at`` (falls back to ``now`` if
      ``ended_at`` is somehow NULL — shouldn't happen but the §6.1
      "agent loop must not crash" invariant beats strict consistency).
    * Unparseable ``started_at``: ``None`` (rare; row was hand-edited).
    """
    start = _parse_iso(started_at)
    if start is None:
        return None
    if status in _TERMINAL_STATUSES:
        end = _parse_iso(ended_at) or datetime.now(UTC)
    else:
        end = datetime.now(UTC)
    delta = (end - start).total_seconds()
    # Negative deltas (clock skew, manual row edits) clamp to 0 so the
    # agent never reports a nonsense "runtime: -3s".
    return max(delta, 0.0)


def _db_path(ctx: RunContext[CapoDeps]) -> Path:
    """Resolve ``state.db`` path from the run context's settings."""
    return ctx.deps.settings.paths.db_path


def _require_user_id(ctx: RunContext[CapoDeps]) -> str:
    """Return ``ctx.deps.user_id`` or raise a clear error.

    ``list_delegations`` is scoped to the current user (§5.5 / §5.11) so
    we never want to silently return rows for someone else if the
    dispatcher forgot to populate ``user_id``.
    """
    user_id = ctx.deps.user_id
    if not user_id:
        raise RuntimeError(
            "list_delegations requires CapoDeps.user_id to be set; "
            "the dispatcher must populate it from the AMC envelope "
            "(see capo.transport.user_resolver)."
        )
    return user_id


# ---------------------------------------------------------------------------
# 1. check_delegation_status
# ---------------------------------------------------------------------------


async def check_delegation_status(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
) -> dict[str, Any]:
    """Return a side-effect-free status snapshot for ``delegation_id``.

    Per spec §5.5: returns ``{delegation_id, status, started_at,
    runtime_seconds, last_activity_ts, summary_one_line}``. ``status`` is
    one of ``running|completed|failed|killed|pending_approval``.
    ``last_activity_ts`` is the most recent ``delegation_output.ts`` for
    this row (falling back to ``started_at`` when no output exists).
    ``summary_one_line`` is the ``summary`` column — ``None`` while
    in-flight, populated on terminal status by the §5.6 monitor.

    Raises:
        DelegationNotFound: When the row does not exist. The agent should
            treat this as "the id you provided is invalid" rather than
            "still spawning" — :func:`delegate_to_claude_code` persists
            before yielding the handle (§5.3 acceptance criterion).
    """
    db_path = _db_path(ctx)
    row = await asyncio.to_thread(_read_status_row, db_path, delegation_id)
    if row is None:
        raise DelegationNotFound(delegation_id)

    started_at = row["started_at"]
    ended_at = row["ended_at"]
    status = row["status"]
    last_activity = row["last_activity_ts"] or started_at

    return {
        "delegation_id": delegation_id,
        "status": status,
        "started_at": started_at,
        "runtime_seconds": _runtime_seconds(started_at, ended_at, status),
        "last_activity_ts": last_activity,
        "summary_one_line": row["summary"],
    }


def _read_status_row(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """SELECT the row + the MAX(ts) over its output. Returns ``None`` if
    the row does not exist. Synchronous; callers use ``asyncio.to_thread``.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                d.id                AS id,
                d.status            AS status,
                d.started_at        AS started_at,
                d.ended_at          AS ended_at,
                d.summary           AS summary,
                (SELECT MAX(ts) FROM delegation_output
                    WHERE delegation_id = d.id) AS last_activity_ts
            FROM delegations AS d
            WHERE d.id = ?
            """,
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return dict(row)


# ---------------------------------------------------------------------------
# 2. get_delegation_output
# ---------------------------------------------------------------------------


async def get_delegation_output(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
    tail_lines: int = DEFAULT_TAIL_LINES,
) -> list[dict[str, Any]]:
    """Return the most recent ``tail_lines`` rows from ``delegation_output``.

    Per spec §5.5: filtered to ``stream IN ('stdout','event')``,
    ``tail_lines`` clamped to ``MAX_TAIL_LINES`` (1000), returned in
    chronological order — i.e. the **last** ``tail_lines`` chunks the
    child emitted, with the oldest of those first.

    For unknown ``delegation_id`` this returns ``[]`` rather than raising:
    the agent often polls output right after spawning, and an empty list
    is the natural "no output yet" answer. Use
    :func:`check_delegation_status` to confirm a row exists.

    Args:
        delegation_id: PK of the ``delegations`` row.
        tail_lines: Number of chunks to return. Values < 1 are clamped
            to 1; values > 1000 are clamped to 1000.

    Returns:
        A list of dicts with keys ``{"ts", "stream", "chunk"}``. Empty
        when the delegation has emitted nothing (or doesn't exist).
    """
    # Silent clamp per §5.5 acceptance criterion ("Cap tail_lines at 1000").
    capped = max(1, min(int(tail_lines), MAX_TAIL_LINES))
    db_path = _db_path(ctx)
    return await asyncio.to_thread(
        _read_output_rows, db_path, delegation_id, capped
    )


def _read_output_rows(
    db_path: Path, delegation_id: str, limit: int
) -> list[dict[str, Any]]:
    """SELECT the latest ``limit`` rows for ``delegation_id``, reversed.

    We grab the latest ``limit`` chunks via ``ORDER BY ts DESC`` then
    reverse the result so callers see them in chronological order. Doing
    the reverse server-side via a subquery would be cleaner but SQLite
    can't avoid the second sort, so we do it in Python for free.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT ts, stream, chunk
            FROM delegation_output
            WHERE delegation_id = ?
              AND stream IN ('stdout', 'event')
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (delegation_id, limit),
        ).fetchall()
    finally:
        conn.close()
    # Reverse to chronological (oldest of the tail first).
    return [
        {"ts": r["ts"], "stream": r["stream"], "chunk": r["chunk"]}
        for r in reversed(rows)
    ]


# ---------------------------------------------------------------------------
# 3. kill_delegation
# ---------------------------------------------------------------------------


async def kill_delegation(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
    reason: str,
) -> dict[str, Any]:
    """Request termination of a running delegation.

    Per spec §5.8, **every** ``kill_delegation`` call requires user
    approval — there is no "force-kill from the agent" path in V1. Phase
    2 raises :class:`ApprovalRequired` so the agent loop has a stable
    failure signal; Phase 4 wires the actual approval prompt and routes
    the approved call into :func:`_kill_delegation_forced`.

    Args:
        delegation_id: PK of the row to kill.
        reason: User-supplied (or agent-supplied) one-line reason. Stored
            on the row's ``summary`` column after a successful kill so
            the user sees it on the completion notification.

    Raises:
        ApprovalRequired: Always, in Phase 2. ``command`` =
            ``f"kill_delegation({delegation_id})"``; ``reason`` is the
            caller's reason (the approval UI will show this).
    """
    # ``ctx`` is currently unused — Phase 4 will resolve the approver
    # via ``ctx.deps.user_id`` and the (future) approvals subsystem.
    # Touch it explicitly so static checkers don't complain.
    _ = ctx
    raise ApprovalRequired(
        command=f"kill_delegation({delegation_id})", reason=reason
    )


async def _kill_delegation_forced(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
    reason: str,
) -> dict[str, Any]:
    """Force-kill path used by integration tests and (Phase 4) the post-
    approval continuation.

    **Not** an agent-facing tool. Phase 4 (task #41) is the only sanctioned
    caller in production. The task #27 checkpoint test invokes this
    directly to prove the kill code path actually terminates the child
    and persists the terminal row.

    Behavior:
    1. SELECT the row's ``pid`` and current ``status``.
    2. If unknown ``delegation_id``: raise :class:`DelegationNotFound`.
    3. If already terminal: return ``{"status": "already_terminal", ...}``
       (idempotent — re-running the kill is a no-op).
    4. Send ``SIGTERM`` to ``pid``; if still alive after
       :data:`_SIGTERM_GRACE_SECONDS`, send ``SIGKILL``.
    5. UPDATE the row to ``status='killed'``, ``ended_at=now``,
       ``summary=reason`` via the ``BEGIN IMMEDIATE`` retry helper.

    Returns a result dict with ``status`` either ``"killed"`` or
    ``"already_terminal"``, ``delegation_id``, and the caller's ``reason``.
    """
    db_path = _db_path(ctx)
    row = await asyncio.to_thread(_read_pid_row, db_path, delegation_id)
    if row is None:
        raise DelegationNotFound(delegation_id)

    current_status: str = row["status"]
    pid: int | None = row["pid"]

    if current_status in _TERMINAL_STATUSES:
        # Idempotent fast-path: a second kill after the row already
        # transitioned is harmless and shouldn't error.
        return {
            "status": "already_terminal",
            "delegation_id": delegation_id,
            "reason": reason,
            "prior_status": current_status,
        }

    # Best-effort signal. Failure modes we tolerate:
    # * pid is None (row was inserted by Phase 4 approval-pending path
    #   before the subprocess spawned). Nothing to signal; we still
    #   write the terminal row.
    # * `ProcessLookupError`: the child already died (race with monitor).
    # * `PermissionError`: extremely unlikely (Capo owns its children);
    #   surface as the kill outcome anyway — the row will reflect
    #   "killed" and the monitor will reap on its own.
    await asyncio.to_thread(_signal_with_escalation, pid)

    await asyncio.to_thread(
        _persist_killed_row, db_path, delegation_id, reason
    )
    return {
        "status": "killed",
        "delegation_id": delegation_id,
        "reason": reason,
    }


def _read_pid_row(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """SELECT ``pid`` + ``status`` for the kill path. ``None`` if missing."""
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT pid, status FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _signal_with_escalation(pid: int | None) -> None:
    """Send SIGTERM, wait up to the grace period, then SIGKILL if needed.

    Synchronous — runs in a worker thread so the asyncio loop isn't
    blocked by the grace-period polling. A blocking ``time.sleep`` in
    the worker is fine; an ``await asyncio.sleep`` would pin the loop.
    """
    import time

    if pid is None:
        return

    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.kill(pid, signal.SIGTERM)

    # Poll for exit. The kernel's `kill(pid, 0)` returns ESRCH once the
    # process is gone, which raises `ProcessLookupError` from
    # `os.kill(pid, 0)`. We use that as our "is alive" probe.
    deadline = time.monotonic() + _SIGTERM_GRACE_SECONDS
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return  # Child exited cleanly under SIGTERM.
        except (PermissionError, OSError):
            # Can't probe — assume alive and escalate.
            break
        time.sleep(0.1)

    # Escalate to SIGKILL. Ignore ESRCH / EPERM — best-effort.
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.kill(pid, signal.SIGKILL)


def _persist_killed_row(
    db_path: Path, delegation_id: str, reason: str
) -> None:
    """UPDATE the row to terminal-killed, with ``reason`` in ``summary``.

    Uses the §7.3 retry helper so concurrent writers (e.g. the §5.6
    monitor racing us to mark the same row ``failed``) get serialized
    without ``SQLITE_BUSY`` ever surfacing.
    """
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation=f"kill_delegation:{delegation_id}"
        ):
            conn.execute(
                """
                UPDATE delegations
                SET status = 'killed',
                    ended_at = ?,
                    summary = ?
                WHERE id = ?
                """,
                (_utc_now_iso(), reason, delegation_id),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4. list_delegations
# ---------------------------------------------------------------------------


async def list_delegations(
    ctx: RunContext[CapoDeps],
    status: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return recent delegations for the current ``user_id``.

    Per spec §5.5 / §5.11: scoped to ``ctx.deps.user_id`` — never crosses
    user boundaries. Ordered by ``started_at DESC``; ``status``, when
    provided, filters by exact match against the §7.3 CHECK constraint
    (``running|completed|failed|killed|pending_approval``).

    Args:
        status: Optional status filter. Unrecognized values produce an
            empty list (no error) — the agent might pass arbitrary
            user-typed strings and we don't want to crash the loop.
        limit: Cap on rows returned. Defaults to 20, clamped to >= 1.

    Returns:
        A list of dicts with keys ``{"delegation_id", "agent", "status",
        "started_at", "summary_one_line"}``. Empty list when the user
        has no matching delegations.
    """
    user_id = _require_user_id(ctx)
    safe_limit = max(1, int(limit))
    db_path = _db_path(ctx)
    return await asyncio.to_thread(
        _read_list_rows, db_path, user_id, status, safe_limit
    )


def _read_list_rows(
    db_path: Path,
    user_id: str,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """SELECT recent delegations for ``user_id`` with optional ``status``."""
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        if status is None:
            rows = conn.execute(
                """
                SELECT id, agent, status, started_at, summary
                FROM delegations
                WHERE user_id = ?
                ORDER BY started_at DESC, id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, agent, status, started_at, summary
                FROM delegations
                WHERE user_id = ? AND status = ?
                ORDER BY started_at DESC, id DESC
                LIMIT ?
                """,
                (user_id, status, limit),
            ).fetchall()
    finally:
        conn.close()

    return [
        {
            "delegation_id": r["id"],
            "agent": r["agent"],
            "status": r["status"],
            "started_at": r["started_at"],
            "summary_one_line": r["summary"],
        }
        for r in rows
    ]


__all__ = [
    "DEFAULT_TAIL_LINES",
    "DelegationNotFound",
    "MAX_TAIL_LINES",
    "check_delegation_status",
    "get_delegation_output",
    "kill_delegation",
    "list_delegations",
]

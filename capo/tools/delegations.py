"""Agent-facing tools for delegation status / output / kill / list (spec §5.5).

This module implements the four read/control tools the agent invokes to
inspect or terminate Claude Code (and later Codex) delegations:

* :func:`check_delegation_status` — side-effect-free status snapshot.
* :func:`get_delegation_output` — tail recent ``stdout`` / ``event`` chunks.
* :func:`kill_delegation` — kill a delegation. Owner kills proceed directly;
  non-owner kills route through the §5.8 / §5.10 approval workflow.
* :func:`list_delegations` — recent delegations for the current ``user_id``.

Phase 2 vs. Phase 4 (Task #44)
------------------------------

Phase 2 implemented :func:`kill_delegation` as an unconditional
:class:`ApprovalRequired` raise — there was no approval workflow to
route through yet. Task #44 (§5.8 + §5.10 + §7.5) wires the actual
approval gate:

* If the requester **is** the delegation owner
  (``delegations.user_id == requester_user_id``): kill proceeds directly
  with no approval round-trip (the user has authority over their own
  delegations per §5.10).
* If the requester is NOT the owner: route through
  :func:`capo.workflows.approval.request_approval` with
  ``request_type='kill_delegation'``. On ``approved`` the kill proceeds;
  on ``denied`` / ``expired`` / ``cancelled`` we raise
  :class:`~capo.tools.basic.ApprovalRejected`.
* When the kill proceeds (either branch), any **other** pending approval
  rows tied to the same ``delegation_id`` (e.g. the killed delegation
  was itself waiting on an out-of-root approval) are force-resolved via
  :func:`capo.workflows.approval.force_resolve_approval` so the
  awaiting workflows fall out of ``DBOS.recv``.

The Phase 2 :func:`_kill_delegation_forced` helper remains and is the
shared post-approval implementation: both the owner direct-kill path and
the non-owner approved path call it.

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
    """Kill a running delegation, gating non-owner kills behind approval.

    Behavior (spec §5.8, §5.10, §7.5; Task #44):

    * If the row is already in a terminal state
      (``completed``/``failed``/``killed``): no-op fast-path. Returns
      ``{"status": "already_terminal", ...}`` without invoking the
      approval workflow.
    * If the requesting user (``ctx.deps.user_id``) **is** the owner
      (``delegations.user_id == requester_user_id``): kill proceeds
      directly — the user has authority over their own delegations.
    * Otherwise (non-owner kill): route through the §5.8 approval
      workflow with ``request_type='kill_delegation'``. On ``approved``
      the kill proceeds; on ``denied``/``expired``/``cancelled`` raise
      :class:`~capo.tools.basic.ApprovalRejected`.
    * When the kill proceeds (either branch), **other** pending approval
      rows whose ``request_payload.delegation_id`` matches this
      ``delegation_id`` are force-resolved with ``status='cancelled'``
      via :func:`capo.workflows.approval.force_resolve_approval` so any
      workflows awaiting on them fall out of ``recv``.

    If the approval infrastructure is genuinely unavailable (DBOS not
    launched, approval module not importable) for a non-owner kill, we
    fall back to :class:`~capo.tools.basic.ApprovalRequired` for
    backwards compatibility with unit tests and degraded environments —
    mirroring the §5.8 ``shell_exec`` gating contract.

    Args:
        delegation_id: PK of the row to kill.
        reason: User-supplied (or agent-supplied) one-line reason. Stored
            on the row's ``summary`` column after a successful kill so
            the user sees it on the completion notification.

    Returns:
        Dict with ``status`` either ``"killed"`` (kill executed) or
        ``"already_terminal"`` (row was already terminal — idempotent
        no-op), ``delegation_id``, and ``reason``. ``cascaded_approvals``
        (int) records how many tied-pending approvals were
        force-resolved on the kill path.

    Raises:
        DelegationNotFound: ``delegation_id`` does not exist in the DB.
        ApprovalRequired: Non-owner kill and the approval workflow
            infrastructure is unavailable.
        ApprovalRejected: Non-owner kill, approval workflow ran and
            returned a non-``approved`` terminal status.
    """
    db_path = _db_path(ctx)

    # 1. Look up the row so we can (a) short-circuit on terminal status
    #    and (b) resolve ownership.
    row = await asyncio.to_thread(
        _read_owner_row, db_path, delegation_id
    )
    if row is None:
        raise DelegationNotFound(delegation_id)

    current_status: str = row["status"]
    if current_status in _TERMINAL_STATUSES:
        # Edge case: kill on terminal row — idempotent no-op, no approval
        # round-trip. Mirrors ``_kill_delegation_forced``.
        return {
            "status": "already_terminal",
            "delegation_id": delegation_id,
            "reason": reason,
            "prior_status": current_status,
        }

    owner_user_id: str | None = row["user_id"]
    requester_user_id: str | None = ctx.deps.user_id

    # 2. Owner check — if requester owns the row, no approval required.
    if (
        requester_user_id is not None
        and owner_user_id == requester_user_id
    ):
        result = await _kill_delegation_forced(ctx, delegation_id, reason)
        cascaded = await _cascade_cancel_tied_approvals(
            db_path,
            delegation_id=delegation_id,
            requester_user_id=requester_user_id,
        )
        if isinstance(result, dict):
            result["cascaded_approvals"] = cascaded
        return result

    # 3. Non-owner kill — route through the approval workflow.
    try:
        decision = await _request_kill_approval(
            ctx, delegation_id=delegation_id, reason=reason
        )
    except _KillApprovalUnavailableError as exc:
        # Degraded environment (DBOS not launched, etc.). Mirror the
        # shell_exec / §5.8 fallback contract.
        raise ApprovalRequired(
            command=f"kill_delegation({delegation_id})",
            reason=(
                f"non-owner kill requires approval and the approval "
                f"workflow is unavailable ({exc.reason})"
            ),
        ) from exc

    if decision.status == "approved":
        logger.info(
            "kill_delegation: approval %s approved by %s — killing %s",
            decision.approval_id,
            decision.resolved_by,
            delegation_id,
            extra={
                "event": "capo.tools.delegations.kill_approved",
                "approval_id": decision.approval_id,
                "delegation_id": delegation_id,
                "resolved_by": decision.resolved_by,
            },
        )
        result = await _kill_delegation_forced(ctx, delegation_id, reason)
        # Cascade-cancel any OTHER pending approvals tied to this
        # delegation. The approval we just resolved (this kill_delegation
        # one) is already terminal, so it won't show up in the scan.
        cascaded = await _cascade_cancel_tied_approvals(
            db_path,
            delegation_id=delegation_id,
            requester_user_id=requester_user_id,
        )
        if isinstance(result, dict):
            result["cascaded_approvals"] = cascaded
        return result

    logger.info(
        "kill_delegation: approval %s resolved %s by %s for %s",
        decision.approval_id,
        decision.status,
        decision.resolved_by,
        delegation_id,
        extra={
            "event": "capo.tools.delegations.kill_rejected",
            "approval_id": decision.approval_id,
            "delegation_id": delegation_id,
            "status": decision.status,
            "resolved_by": decision.resolved_by,
        },
    )
    # Imported lazily so this module doesn't unconditionally pull
    # ``capo.tools.basic.ApprovalRejected`` (which transitively imports
    # the approval workflow types) into Phase-1 callers.
    from capo.tools.basic import ApprovalRejected

    raise ApprovalRejected(decision)


class _KillApprovalUnavailableError(Exception):
    """Raised internally when the approval workflow can't be reached.

    Caught by :func:`kill_delegation` to fall back to
    :class:`~capo.tools.basic.ApprovalRequired` so unit tests + non-DBOS
    contexts keep their existing behavior. Mirrors the
    :class:`capo.tools.basic.ApprovalUnavailableError` pattern from
    Task #42.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _utc_iso_now() -> str:
    """Caller-stable UTC ISO 8601 timestamp for the approvals row.

    Mirrors :func:`capo.tools.basic._utc_iso_now`. Format
    ``%Y-%m-%dT%H:%M:%SZ`` per §5.8.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _request_kill_approval(
    ctx: RunContext[CapoDeps],
    *,
    delegation_id: str,
    reason: str,
) -> Any:
    """Invoke the §5.8 approval workflow with ``request_type='kill_delegation'``.

    Raises :class:`_KillApprovalUnavailableError` (caught by
    :func:`kill_delegation`) when the approval workflow can't be reached.
    """
    try:
        from capo.workflows import is_launched
        from capo.workflows.approval import request_approval
    except ImportError as exc:  # pragma: no cover - DBOS is a hard dep
        raise _KillApprovalUnavailableError(
            f"approval workflow module not importable: {exc}"
        ) from exc

    try:
        launched = is_launched()
    except Exception as exc:  # noqa: BLE001 - defensive
        raise _KillApprovalUnavailableError(
            f"could not check DBOS launch state: {exc!r}"
        ) from exc
    if not launched:
        raise _KillApprovalUnavailableError("DBOS not launched")

    settings = ctx.deps.settings
    db_path = getattr(getattr(settings, "paths", None), "db_path", None)
    if db_path is None:
        raise _KillApprovalUnavailableError(
            "settings.paths.db_path is not configured"
        )

    # Caller-supplied deterministic inputs — generated BEFORE the call so
    # a workflow replay observes the same approval_id + requested_at.
    import uuid

    approval_id = uuid.uuid4().hex
    requested_at = _utc_iso_now()
    request_payload: dict[str, Any] = {
        "delegation_id": delegation_id,
        "reason": reason,
    }

    timeout_s = float(
        getattr(getattr(settings, "approval", None), "timeout_seconds", 0)
        or 0
    )

    kwargs: dict[str, Any] = dict(
        request_type="kill_delegation",
        request_payload=request_payload,
        requester_user_id=ctx.deps.user_id,
        parent_thread_id=ctx.deps.thread_id,
        requested_at=requested_at,
        db_path=db_path,
    )
    if timeout_s > 0:
        kwargs["timeout_seconds"] = timeout_s

    logger.info(
        "kill_delegation: requesting approval for delegation %s "
        "(approval_id=%s)",
        delegation_id,
        approval_id,
        extra={
            "event": "capo.tools.delegations.kill_approval_requested",
            "approval_id": approval_id,
            "delegation_id": delegation_id,
        },
    )

    return await request_approval(approval_id, **kwargs)


async def _cascade_cancel_tied_approvals(
    db_path: Path,
    *,
    delegation_id: str,
    requester_user_id: str | None,
) -> int:
    """Force-cancel all pending approvals whose payload references ``delegation_id``.

    Called after a kill proceeds (either owner-direct or post-approval).
    For each pending approval row whose
    ``request_payload.delegation_id == delegation_id``, send
    ``force_resolve_approval(workflow_id, status='cancelled', ...)`` so
    the awaiting workflow falls out of ``DBOS.recv``.

    Returns the number of approvals that were force-resolved.

    Notes:
        * The scan runs against the durable ``approvals`` table — uses
          the ``idx_approvals_pending_sweep`` index (filters
          ``status='pending'`` first).
        * If the approvals table or the workflow module is unavailable
          (Phase 2 tests, no DBOS), the scan returns 0 silently — the
          kill itself has already succeeded; the cascade is best-effort.
        * The kill_delegation approval row itself (request_type=
          ``kill_delegation`` keyed by the same ``delegation_id``) is
          handled by the inbound dispatcher (Task #40) on the
          ``approved`` path — it's already terminal by the time this
          scan runs, so it's filtered out by the ``status='pending'``
          predicate.
    """
    try:
        from capo.workflows.approval import force_resolve_approval
    except ImportError:  # pragma: no cover - DBOS is a hard dep
        return 0

    rows = await asyncio.to_thread(
        _find_pending_approvals_for_delegation, db_path, delegation_id
    )
    if not rows:
        return 0

    cancelled = 0
    resolver = f"system:killer:{requester_user_id or 'unknown'}"
    cancel_reason = f"delegation {delegation_id} killed"
    for row in rows:
        workflow_id = row.get("workflow_id")
        approval_id = row.get("approval_id")
        if not workflow_id:
            logger.warning(
                "kill_delegation: cascade-cancel skipped — approval %s "
                "has no workflow_id",
                approval_id,
                extra={
                    "event": (
                        "capo.tools.delegations."
                        "kill_cascade_no_workflow_id"
                    ),
                    "approval_id": approval_id,
                    "delegation_id": delegation_id,
                },
            )
            continue
        try:
            await force_resolve_approval(
                str(workflow_id),
                status="cancelled",
                resolved_by=resolver,
                reason=cancel_reason,
            )
        except Exception as exc:  # noqa: BLE001 - best-effort
            logger.warning(
                "kill_delegation: force_resolve_approval failed for "
                "approval %s (workflow=%s): %r",
                approval_id,
                workflow_id,
                exc,
                extra={
                    "event": (
                        "capo.tools.delegations.kill_cascade_failed"
                    ),
                    "approval_id": approval_id,
                    "workflow_id": workflow_id,
                    "delegation_id": delegation_id,
                },
            )
            continue
        cancelled += 1
        logger.info(
            "kill_delegation: cascade-cancelled approval %s "
            "(workflow=%s) tied to delegation %s",
            approval_id,
            workflow_id,
            delegation_id,
            extra={
                "event": "capo.tools.delegations.kill_cascade_cancelled",
                "approval_id": approval_id,
                "workflow_id": workflow_id,
                "delegation_id": delegation_id,
            },
        )
    return cancelled


def _find_pending_approvals_for_delegation(
    db_path: Path, delegation_id: str
) -> list[dict[str, Any]]:
    """SELECT pending approvals whose payload references ``delegation_id``.

    The ``approvals.request_payload`` column is JSON text. We use
    SQLite's ``json_extract`` so the filter happens in-DB without
    materializing every pending row in Python.

    If the approvals table is missing (Phase 2 tests that haven't run
    the migration) this returns ``[]`` so the cascade is a no-op. Same
    for any other unexpected SQL error — the kill itself is the
    user-visible operation; the cascade is best-effort cleanup.
    """
    try:
        conn = open_connection(db_path)
    except Exception:  # noqa: BLE001 - best-effort
        return []
    try:
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                """
                SELECT approval_id, workflow_id, request_payload
                FROM approvals
                WHERE status = 'pending'
                  AND json_extract(request_payload, '$.delegation_id') = ?
                """,
                (delegation_id,),
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            # Table missing or json1 extension unavailable — best-effort.
            return []
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _read_owner_row(
    db_path: Path, delegation_id: str
) -> dict[str, Any] | None:
    """SELECT ``user_id`` + ``status`` for the kill-gating check.

    Returns ``None`` when the row does not exist so the caller can raise
    :class:`DelegationNotFound`.
    """
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT user_id, status FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


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

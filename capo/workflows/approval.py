"""DBOS ``request_approval`` workflow (spec §5.8, §5.9, §7.5, Task #39).

The §5.8 approval workflow gates non-allowlisted ``shell_exec`` invocations,
out-of-``projects_root`` delegations, and ``kill_delegation`` calls. The tool
INSERTs an ``approvals`` row (status=``pending``) and then suspends until the
user replies with ``/approve <id>`` or ``/deny <id>`` — or the operator-
configured timeout elapses, or an external path (e.g. delegation killer)
force-cancels the request. The user reply is routed by the inbound webhook
dispatcher (Task #40) which calls :func:`dbos.DBOS.send` against the awaiting
workflow's id with topic ``approval_decision``.

DBOS at-least-once guarantees the workflow body is replay-safe across crashes:

* The INSERT step is wrapped with :func:`~capo.workflows._idempotency.idempotent_step`
  keyed by ``approval_id`` so a workflow crash + replay does NOT insert a
  duplicate row.
* The AMC notify step is similarly wrapped; the AMC ``Idempotency-Key`` header
  matches the step's stable key so the receiver dedupes too.
* The deterministic input is ``(approval_id, requested_at)`` — both are caller-
  supplied so two replays produce the same ``requested_at`` column value
  (§5.8 calls this out explicitly: no ``CURRENT_TIMESTAMP`` / no
  ``datetime.now()`` inside the workflow body).

Decision routing
----------------

The workflow waits via :meth:`dbos.DBOS.recv_async` (topic
``approval_decision``). The dispatcher (Task #40) parses ``/approve <id>`` or
``/deny <id>`` from the inbound AMC message, looks up the pending row's
``workflow_id``, and calls ``DBOS.send(workflow_id, payload, "approval_decision")``
with a payload like ``{"status": "approve", "resolved_by": "u-42", "reason": None}``.
The workflow then UPDATEs the row to the resolved state and returns the typed
:class:`ApprovalDecision`.

Status transitions:

* ``pending`` → ``approved`` (user sent ``/approve``).
* ``pending`` → ``denied`` (user sent ``/deny``).
* ``pending`` → ``expired`` (timeout elapsed before any decision).
* ``pending`` → ``cancelled`` (external force-resolution, e.g. delegation
  killed before the approval landed).

External cancellation
---------------------

A separate code path (e.g. ``kill_delegation`` tearing down a delegation that
was waiting on an approval) can resolve a pending approval by calling
``DBOS.send(workflow_id, {"status": "cancelled", "resolved_by": "...",
"reason": "..."}, "approval_decision")``. The workflow body treats ``cancelled``
identically to ``approve``/``deny`` for the purposes of UPDATE + return.

Spec references
---------------

* §5.8 Feature: Approval Workflow (state machine, INSERT, recv, UPDATE,
  return shape).
* §5.9 Feature: Inbound routing (``/approve <id>`` → ``DBOS.send`` matched by
  ``approval_id``).
* §7.5 Data Models — ``approvals`` schema; ``Idempotency-Key`` header.
* §9.4 Phase 4 plan — task #39 acceptance criteria.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.observability import approval_request_span
from capo.workflows._idempotency import idempotent_step

# Reuse the Task #33 AMC sender registry from the delegation module. The
# step-name prefix on the idempotency key (``notify_approval:<hex>`` vs
# ``notify_user:<hex>``) keeps the dedupe namespaces disjoint at the AMC
# receiver. Importing lazily-via-module avoids cyclic imports in tests that
# only exercise the approval helpers.
from capo.workflows.delegation import (
    NotifyError,
    _channel_id_from_parent_thread,
    _lookup_amc_sender,
)

logger = logging.getLogger("capo.workflows.approval")

# Task #51: Logfire spans are opened through
# :func:`capo.observability.approval_request_span`; no soft-import of
# ``logfire`` required at this layer.


# ---------------------------------------------------------------------------
# Tunables / constants
# ---------------------------------------------------------------------------

#: Default recv timeout (24 h, per §5.8 "operator-configured timeout").
#: Tests override via the ``timeout_seconds`` keyword on
#: :func:`request_approval`. A very generous default is fine because the
#: workflow is durable — the process can crash and re-enter and the
#: remaining timeout is re-applied on each ``recv`` call.
DEFAULT_APPROVAL_TIMEOUT_S: float = 24 * 60 * 60.0

#: DBOS message topic used for the approve/deny/cancel dispatch.
APPROVAL_TOPIC: str = "approval_decision"

#: Statuses recognized in the workflow + persisted to the ``approvals`` row.
STATUS_PENDING: str = "pending"
STATUS_APPROVED: str = "approved"
STATUS_DENIED: str = "denied"
STATUS_EXPIRED: str = "expired"
STATUS_CANCELLED: str = "cancelled"

#: Resolutions a caller-on-the-send-side may pass. We accept the verb form
#: (``approve``/``deny``) used by the dispatcher AND the noun form
#: (``approved``/``denied``) for caller convenience.
_DECISION_VERB_TO_STATUS: dict[str, str] = {
    "approve": STATUS_APPROVED,
    "approved": STATUS_APPROVED,
    "deny": STATUS_DENIED,
    "denied": STATUS_DENIED,
    "cancel": STATUS_CANCELLED,
    "cancelled": STATUS_CANCELLED,
    "expire": STATUS_EXPIRED,
    "expired": STATUS_EXPIRED,
}

#: Terminal statuses for an approvals row.
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {STATUS_APPROVED, STATUS_DENIED, STATUS_EXPIRED, STATUS_CANCELLED}
)


# ---------------------------------------------------------------------------
# Typed errors + return value
# ---------------------------------------------------------------------------


class ApprovalError(RuntimeError):
    """Raised when the workflow cannot complete the approval round-trip.

    Examples:

    * AMC notify hit a PERMANENT error (``PLATFORM_AUTH``,
      ``CHANNEL_NOT_FOUND``, ...) — propagated as :class:`ApprovalError` so
      the tool caller (``shell_exec``, ``delegate_to_codex``, etc.) can
      surface it instead of suspending forever.
    * Inbound decision had an unrecognized ``status``.

    The row state on raise: per Task #39 ack — we choose "revert to
    pending and re-raise" so a transient AMC issue does NOT poison the
    durable row. The operator can re-fire the workflow later with the
    same ``approval_id`` and the row is still in ``pending``. (If you prefer
    the "set to failed" alternative, swap the branch in
    :func:`_handle_notify_permanent_error`; both modes are explicit in
    code below.)
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        approval_id: str,
    ) -> None:
        self.code = code
        self.message = message
        self.approval_id = approval_id
        super().__init__(f"{code}: {message} (approval_id={approval_id})")


@dataclass(frozen=True)
class ApprovalDecision:
    """Typed return value of :func:`request_approval`.

    Attributes
    ----------
    approval_id:
        The same id passed in by the caller — the deterministic workflow
        input.
    status:
        One of ``approved``, ``denied``, ``expired``, ``cancelled``.
    resolved_by:
        User / system identifier of whoever resolved the request. May be
        ``None`` for ``expired`` (the system itself resolved it).
    reason:
        Optional free-text reason — denial reason, cancel reason, timeout
        annotation. ``None`` on ``approved`` by default.
    """

    approval_id: str
    status: str
    resolved_by: str | None
    reason: str | None


# ---------------------------------------------------------------------------
# DB helpers (sync — wrapped in ``asyncio.to_thread`` by callers)
# ---------------------------------------------------------------------------


def _insert_pending_row(
    db_path: Path,
    *,
    approval_id: str,
    requested_at: str,
    request_type: str,
    request_payload_json: str,
    requester_user_id: str | None,
    parent_thread_id: str | None,
    workflow_id: str | None,
) -> None:
    """INSERT one ``approvals`` row in status ``pending``.

    Idempotent at the SQL level via ``INSERT OR IGNORE`` so a workflow
    replay (the @idempotent_step wrapper handles step-state replay first;
    this clause is belt-and-braces for the case where the wrapper is
    bypassed in tests).
    """
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation=f"approval_insert:{approval_id}"
        ):
            conn.execute(
                """
                INSERT OR IGNORE INTO approvals (
                    approval_id, requested_at, request_type, request_payload,
                    status, requester_user_id, parent_thread_id, workflow_id
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    approval_id,
                    requested_at,
                    request_type,
                    request_payload_json,
                    requester_user_id,
                    parent_thread_id,
                    workflow_id,
                ),
            )
    finally:
        conn.close()


def _persist_resolution(
    db_path: Path,
    *,
    approval_id: str,
    status: str,
    decided_at: str,
    resolved_by: str | None,
    reason: str | None,
) -> None:
    """UPDATE the row to a terminal status.

    Goes through :func:`begin_immediate_with_retry` for the §7.3 zero-
    ``SQLITE_BUSY`` contract. Refuses to overwrite a row already in a
    terminal state (an external cancel that lost the race to the user's
    approve, or vice versa, MUST not flip the column twice).
    """
    if status not in _TERMINAL_STATUSES:
        raise ValueError(
            f"_persist_resolution: status must be terminal, got {status!r}"
        )
    conn = open_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT status FROM approvals WHERE approval_id = ?",
            (approval_id,),
        )
        row = cur.fetchone()
        if row is None:
            logger.warning(
                "request_approval: no approvals row for id=%s — cannot "
                "persist resolution %s",
                approval_id,
                status,
                extra={
                    "event": "capo.workflows.approval.row_missing",
                    "approval_id": approval_id,
                    "status": status,
                },
            )
            return
        current = row[0]
        if current in _TERMINAL_STATUSES:
            logger.info(
                "request_approval: row already terminal (%s); leaving as-is",
                current,
                extra={
                    "event": "capo.workflows.approval.already_terminal",
                    "approval_id": approval_id,
                    "row_status": current,
                    "intended_status": status,
                },
            )
            return
        with begin_immediate_with_retry(
            conn, operation=f"approval_resolve:{approval_id}"
        ):
            conn.execute(
                "UPDATE approvals SET status = ?, decided_at = ?, "
                "resolved_by = ?, reason = ? WHERE approval_id = ?",
                (status, decided_at, resolved_by, reason, approval_id),
            )
    finally:
        conn.close()


def _read_row_for_notify(
    db_path: Path, approval_id: str
) -> dict[str, Any] | None:
    """SELECT the columns we need to compose the notify body."""
    conn = open_connection(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT request_type, request_payload, parent_thread_id, "
            "requester_user_id FROM approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _read_status(db_path: Path, approval_id: str) -> str | None:
    """SELECT the row's current ``status`` (or ``None`` if missing).

    Used by the workflow body to detect "external cancel already wrote
    the row" so a redundant UPDATE doesn't fight the cancel path.
    """
    conn = open_connection(db_path)
    try:
        row = conn.execute(
            "SELECT status FROM approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
    finally:
        conn.close()
    return str(row[0]) if row else None


# ---------------------------------------------------------------------------
# Body composition (deterministic, no timestamps / env / randomness)
# ---------------------------------------------------------------------------


def _compose_approval_request_body(
    *,
    approval_id: str,
    request_type: str,
    request_summary: str,
) -> str:
    """Deterministic AMC notification body for an approval request.

    Format::

        Approval required (<type>): <approval_id>
        <summary>

        Reply with /approve <approval_id> or /deny <approval_id>.

    The ``request_summary`` is provided by the caller (the tool gating
    behind approval). It is collapsed to a single line + stripped so the
    AMC body is deterministic for stable DBOS replay.
    """
    head = f"Approval required ({request_type}): {approval_id}"
    cleaned = (request_summary or "").replace("\r\n", " ").replace("\n", " ").strip()
    footer = f"Reply with /approve {approval_id} or /deny {approval_id}."
    if cleaned:
        return f"{head}\n{cleaned}\n\n{footer}"
    return f"{head}\n\n{footer}"


def _request_summary_from_payload(request_type: str, payload: dict[str, Any]) -> str:
    """Best-effort deterministic one-line summary from the request payload.

    The tool gating behind approval (``shell_exec`` etc.) is responsible
    for supplying useful payload fields; this helper picks the most
    user-readable bits without crashing on unexpected shapes.
    """
    if not isinstance(payload, dict):
        return ""
    if request_type == "shell_exec":
        cmd = payload.get("command") or payload.get("cmd") or ""
        if isinstance(cmd, list | tuple):
            cmd = " ".join(str(part) for part in cmd)
        return f"shell_exec: {cmd}".strip()
    if request_type == "delegate_out_of_root":
        agent = payload.get("agent") or "?"
        path = (
            payload.get("repo_path")
            or payload.get("path")
            or payload.get("workspace")
            or "?"
        )
        return f"out-of-root delegation: agent={agent} repo_path={path}"
    if request_type == "delegate_dangerous_sandbox":
        agent = payload.get("agent") or "codex"
        path = payload.get("repo_path") or payload.get("path") or "?"
        sandbox = payload.get("sandbox") or "danger-full-access"
        return (
            f"dangerous-sandbox delegation: agent={agent} "
            f"sandbox={sandbox} repo_path={path}"
        )
    if request_type == "kill_delegation":
        delegation_id = payload.get("delegation_id") or "?"
        return f"kill_delegation: {delegation_id}"
    # Unknown type — surface the JSON keys as a hint.
    keys = ",".join(sorted(str(k) for k in payload))
    return f"{request_type}: ({keys})"


# ---------------------------------------------------------------------------
# Idempotent step: INSERT the pending row
# ---------------------------------------------------------------------------


@idempotent_step(
    step_name="approval_insert",
    deterministic_args=lambda *args, **kwargs: (
        args[0] if len(args) >= 1 else kwargs.get("approval_id"),
    ),
)
async def _insert_approval_step(
    approval_id: str,
    *,
    requested_at: str,
    request_type: str,
    request_payload_json: str,
    requester_user_id: str | None,
    parent_thread_id: str | None,
    workflow_id: str | None,
    db_path: str,
) -> dict[str, Any]:
    """Idempotent step wrapping the INSERT.

    Keyed by ``approval_id`` so workflow replay does not insert twice. We
    also use ``INSERT OR IGNORE`` at the SQL layer so the table CHECK
    constraints don't fire spuriously on a replay that bypassed the
    step-state cache (e.g. tests).
    """
    await asyncio.to_thread(
        _insert_pending_row,
        Path(db_path),
        approval_id=approval_id,
        requested_at=requested_at,
        request_type=request_type,
        request_payload_json=request_payload_json,
        requester_user_id=requester_user_id,
        parent_thread_id=parent_thread_id,
        workflow_id=workflow_id,
    )
    return {"approval_id": approval_id, "status": "pending"}


# ---------------------------------------------------------------------------
# Idempotent step: AMC notify
# ---------------------------------------------------------------------------


@idempotent_step(
    step_name="notify_approval",
    deterministic_args=lambda *args, **kwargs: (
        args[0] if len(args) >= 1 else kwargs.get("approval_id"),
    ),
)
async def notify_approval(approval_id: str, db_path: str) -> dict[str, Any]:
    """Send the approval-request notification to the user via AMC.

    Acceptance criteria (Task #39, spec §5.8 / §5.9 / §7.5):

    * ``@DBOS.step`` + ``@idempotent_step`` keyed by ``approval_id`` so
      workflow restart does NOT re-send.
    * Body is composed deterministically from the row's
      ``(request_type, request_payload, approval_id)``.
    * The HTTP ``Idempotency-Key`` header equals
      ``notify_approval.idempotency_key_for(approval_id, db_path)``.
    * Permanent AMC errors raise :class:`ApprovalError` (the caller maps
      to either "revert row to pending" or "mark row failed" — see
      :func:`request_approval`).
    * Transient errors propagate so DBOS retries the step; the AMC
      idempotency key keeps the receiver deduped.

    Returns a small audit dict with the message_id (or ``None`` for the
    no-op branches where no channel is available).
    """
    row = await asyncio.to_thread(_read_row_for_notify, Path(db_path), approval_id)
    if row is None:
        logger.warning(
            "notify_approval: no approvals row for id=%s — skipping send",
            approval_id,
            extra={
                "event": "capo.workflows.approval.notify_no_row",
                "approval_id": approval_id,
            },
        )
        return {
            "approval_id": approval_id,
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    parent_thread_id = row.get("parent_thread_id")
    channel_id = _channel_id_from_parent_thread(parent_thread_id)
    if channel_id is None:
        logger.info(
            "notify_approval: approval %s has no AMC channel "
            "(parent_thread_id=%r) — skipping send",
            approval_id,
            parent_thread_id,
            extra={
                "event": "capo.workflows.approval.notify_no_channel",
                "approval_id": approval_id,
                "parent_thread_id": parent_thread_id,
            },
        )
        return {
            "approval_id": approval_id,
            "channel_id": None,
            "message_id": None,
            "idempotency_key": None,
            "notified": False,
        }

    request_type = str(row.get("request_type") or "")
    payload_raw = row.get("request_payload") or "{}"
    try:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else {}
    except (TypeError, ValueError):
        payload = {}
    summary = _request_summary_from_payload(request_type, payload)
    body = _compose_approval_request_body(
        approval_id=approval_id,
        request_type=request_type,
        request_summary=summary,
    )

    idem_key = notify_approval.idempotency_key_for(  # type: ignore[attr-defined]
        approval_id, db_path
    )

    sender = _lookup_amc_sender()
    if sender is None:
        raise ApprovalError(
            code="NO_SENDER_REGISTERED",
            message=(
                "no AMC sender registered with capo.workflows — call "
                "register_amc_sender() during boot before launching the "
                "approval workflow"
            ),
            approval_id=approval_id,
        )

    # Imported here lazily so the union doesn't pull notify_user's permanent-
    # code table into the approval module's import graph by name.
    from capo.workflows.delegation import _NOTIFY_PERMANENT_CODES

    try:
        result = await sender(channel_id, body, idem_key)
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", None)
        if isinstance(code, str) and code in _NOTIFY_PERMANENT_CODES:
            message = getattr(exc, "message", None) or str(exc)
            raise ApprovalError(
                code=code,
                message=str(message),
                approval_id=approval_id,
            ) from exc
        # Transient — propagate.
        logger.warning(
            "notify_approval: transient AMC error for %s; DBOS will retry "
            "(idempotency_key=%s, err=%r)",
            approval_id,
            idem_key,
            exc,
            extra={
                "event": "capo.workflows.approval.notify_transient",
                "approval_id": approval_id,
                "idempotency_key": idem_key,
            },
        )
        raise

    message_id = getattr(result, "message_id", None)
    logger.info(
        "notify_approval: delivered approval-request notification for %s "
        "(message_id=%s, idempotency_key=%s)",
        approval_id,
        message_id,
        idem_key,
        extra={
            "event": "capo.workflows.approval.notify_sent",
            "approval_id": approval_id,
            "message_id": message_id,
            "idempotency_key": idem_key,
        },
    )
    return {
        "approval_id": approval_id,
        "channel_id": channel_id,
        "message_id": message_id,
        "idempotency_key": idem_key,
        "notified": True,
    }


# ---------------------------------------------------------------------------
# Decision normalization
# ---------------------------------------------------------------------------


def _normalize_decision_status(raw: Any) -> str | None:
    """Map a caller-supplied status verb/noun to a canonical terminal status.

    Returns ``None`` for unrecognized inputs so the workflow body can raise
    a typed :class:`ApprovalError` rather than silently persisting garbage.
    """
    if not isinstance(raw, str):
        return None
    return _DECISION_VERB_TO_STATUS.get(raw.strip().lower())


def _coerce_payload(message: Any) -> dict[str, Any]:
    """Normalize an inbound ``DBOS.send`` payload to a dict.

    Acceptable shapes:

    * ``{"status": "approve", "resolved_by": "u-42", "reason": "..."}``
    * ``"approve"`` — bare verb (treated as ``{"status": "approve"}``).
    * Any other shape is returned as ``{"status": <repr>}`` so the
      normalize step downstream emits ``ApprovalError``.
    """
    if isinstance(message, dict):
        return dict(message)
    if isinstance(message, str):
        return {"status": message}
    return {"status": repr(message)}


# ---------------------------------------------------------------------------
# Lazy DBOS workflow registration (mirrors delegation.py Task #30 pattern)
# ---------------------------------------------------------------------------

_workflow_lock = threading.Lock()
_workflow_registered: bool = False
_approval_workflow: Any | None = None


def _utc_now_iso() -> str:
    """Best-effort wall-clock UTC ISO timestamp (used for decided_at only).

    DBOS replay determinism for ``decided_at`` is preserved by the
    ``_persist_resolution`` step's wrapping in a ``@DBOS.step`` (the
    step-state cache replays the persisted value on resume, so the
    timestamp is captured once and reused). The workflow body NEVER reads
    ``datetime.now()`` directly into any deterministic-input path.
    """
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


@idempotent_step(
    step_name="approval_resolve",
    # The deterministic key for the resolve step is just ``approval_id``;
    # the (status, resolved_by, reason) tuple is part of the payload
    # fingerprint, so a same-key-different-payload call (e.g. external
    # cancel racing approve) raises IdempotencyMismatchError loudly.
    deterministic_args=lambda *args, **kwargs: (
        args[0] if len(args) >= 1 else kwargs.get("approval_id"),
    ),
)
async def _resolve_approval_step(
    approval_id: str,
    *,
    status: str,
    decided_at: str,
    resolved_by: str | None,
    reason: str | None,
    db_path: str,
) -> dict[str, Any]:
    """Idempotent step wrapping the resolution UPDATE."""
    await asyncio.to_thread(
        _persist_resolution,
        Path(db_path),
        approval_id=approval_id,
        status=status,
        decided_at=decided_at,
        resolved_by=resolved_by,
        reason=reason,
    )
    return {
        "approval_id": approval_id,
        "status": status,
        "resolved_by": resolved_by,
        "reason": reason,
    }


async def _approval_body(
    approval_id: str,
    *,
    requested_at: str,
    request_type: str,
    request_payload_json: str,
    requester_user_id: str | None,
    parent_thread_id: str | None,
    timeout_seconds: float,
    db_path: str,
) -> ApprovalDecision:
    """Inner workflow body — extracted so the DBOS decorator wraps a single
    callable but the implementation can be unit-tested without DBOS launched.

    Order of operations:

    1. INSERT the pending row (idempotent step).
    2. Notify the user via AMC (idempotent step).
    3. ``DBOS.recv_async`` with the configured timeout.
    4. On any decision (approve/deny/cancel) or timeout: UPDATE the row +
       return :class:`ApprovalDecision`.
    """
    try:
        from dbos import DBOS  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - dbos is a hard dep
        raise RuntimeError("dbos package not importable") from None

    # Best-effort workflow id capture for the INSERT. The wrapping
    # @DBOS.workflow() decorator sets DBOS.workflow_id inside the body; we
    # persist it on the row so the inbound dispatcher (Task #40) can route
    # `DBOS.send(workflow_id, ...)` without a separate lookup table.
    try:
        wf_id = DBOS.workflow_id
        workflow_id: str | None = wf_id if isinstance(wf_id, str) and wf_id else None
    except Exception:
        workflow_id = None

    # --- 1. INSERT pending row (idempotent) ---------------------------------
    await _insert_approval_step(
        approval_id,
        requested_at=requested_at,
        request_type=request_type,
        request_payload_json=request_payload_json,
        requester_user_id=requester_user_id,
        parent_thread_id=parent_thread_id,
        workflow_id=workflow_id,
        db_path=db_path,
    )

    # --- 2. AMC notify (idempotent — at most one send across replays) -------
    try:
        await notify_approval(approval_id, db_path)
    except ApprovalError as notify_exc:
        # Per Task #39 acceptance: permanent notify failure -> revert row to
        # pending (no-op, it's already pending) and re-raise. The tool caller
        # surfaces the error to the user instead of silently waiting forever.
        # We intentionally DO NOT mark the row terminal here — the operator
        # can re-fire the workflow with the same approval_id later.
        logger.error(
            "request_approval: permanent notify failure for %s (%s) — "
            "leaving row pending and re-raising ApprovalError",
            approval_id,
            notify_exc.code,
            extra={
                "event": "capo.workflows.approval.notify_permanent_failure",
                "approval_id": approval_id,
                "code": notify_exc.code,
            },
        )
        raise

    # --- 3. recv-await with timeout ----------------------------------------
    raw_message: Any = await DBOS.recv_async(
        APPROVAL_TOPIC,
        timeout_seconds=float(timeout_seconds),
    )

    # --- 4. Normalize + resolve --------------------------------------------
    decided_at = _utc_now_iso()
    if raw_message is None:
        # recv timed out — DBOS returns None per its contract.
        status_final = STATUS_EXPIRED
        resolved_by: str | None = None
        reason: str | None = f"timeout after {int(timeout_seconds)}s"
    else:
        payload = _coerce_payload(raw_message)
        status_final = _normalize_decision_status(payload.get("status")) or ""
        if not status_final:
            raise ApprovalError(
                code="UNKNOWN_DECISION",
                message=(
                    f"approval decision payload {payload!r} did not carry a "
                    f"recognized status field"
                ),
                approval_id=approval_id,
            )
        resolved_by = payload.get("resolved_by")
        if resolved_by is not None and not isinstance(resolved_by, str):
            resolved_by = str(resolved_by)
        reason_raw = payload.get("reason")
        reason = None if reason_raw is None else str(reason_raw)

    # Before persisting, check the durable row state — an external cancel
    # path (e.g. delegation killer) may have already UPDATEd the row to a
    # terminal status. In that case we honor the persisted value and
    # return it instead of overwriting.
    current_status = await asyncio.to_thread(
        _read_status, Path(db_path), approval_id
    )
    if current_status in _TERMINAL_STATUSES and current_status != status_final:
        logger.info(
            "request_approval: external resolution won race for %s "
            "(persisted=%s, incoming=%s)",
            approval_id,
            current_status,
            status_final,
            extra={
                "event": "capo.workflows.approval.race_external_resolve",
                "approval_id": approval_id,
                "persisted_status": current_status,
                "incoming_status": status_final,
            },
        )
        # Read back resolved_by/reason from the row so the decision returned
        # to the tool caller reflects the persisted state.
        return await _materialize_decision_from_row(
            db_path, approval_id, fallback_status=current_status
        )

    await _resolve_approval_step(
        approval_id,
        status=status_final,
        decided_at=decided_at,
        resolved_by=resolved_by,
        reason=reason,
        db_path=db_path,
    )

    return ApprovalDecision(
        approval_id=approval_id,
        status=status_final,
        resolved_by=resolved_by,
        reason=reason,
    )


async def _materialize_decision_from_row(
    db_path: str, approval_id: str, *, fallback_status: str
) -> ApprovalDecision:
    """Build :class:`ApprovalDecision` from the persisted row.

    Used when an external resolver beat the workflow to the UPDATE — we
    return the durable state rather than overwriting it.
    """
    def _read() -> dict[str, Any] | None:
        conn = open_connection(Path(db_path))
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT status, resolved_by, reason "
                "FROM approvals WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    row = await asyncio.to_thread(_read)
    if row is None:
        return ApprovalDecision(
            approval_id=approval_id,
            status=fallback_status,
            resolved_by=None,
            reason=None,
        )
    return ApprovalDecision(
        approval_id=approval_id,
        status=str(row.get("status") or fallback_status),
        resolved_by=row.get("resolved_by"),
        reason=row.get("reason"),
    )


def _register_workflow() -> Any:
    """Register :func:`_approval_body` with DBOS lazily.

    Mirrors the Task #30 pattern in delegation.py: DBOS requires
    :meth:`DBOS.launch` to have run before ``@DBOS.workflow()`` can
    register a workflow. Defer the registration to first call so this
    module is importable during boot (before DBOS is launched).
    """
    global _workflow_registered, _approval_workflow
    with _workflow_lock:
        if _workflow_registered and _approval_workflow is not None:
            return _approval_workflow

        try:
            from dbos import DBOS  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "dbos package not importable — capo.workflows.approval "
                "requires DBOS to be installed and launched"
            ) from exc

        @DBOS.workflow()
        async def request_approval_workflow(
            approval_id: str,
            requested_at: str,
            request_type: str,
            request_payload_json: str,
            requester_user_id: str | None,
            parent_thread_id: str | None,
            timeout_seconds: float,
            db_path: str,
        ) -> ApprovalDecision:
            """DBOS-registered approval workflow body.

            Note that all parameters are positional + JSON-serializable to
            satisfy DBOS's argument-pickling contract.
            """
            # Spec §6.5: ``capo.approval.request``. ``action_kind`` is
            # the §5.8 vocabulary slug carried on the request row (e.g.
            # ``shell_exec``, ``delegate_claude_code``); we reuse
            # ``request_type`` here since the two are identical at this
            # layer (the dispatcher carries the same value through).
            with approval_request_span(
                approval_id=approval_id,
                action_kind=request_type,
            ):
                return await _approval_body(
                    approval_id,
                    requested_at=requested_at,
                    request_type=request_type,
                    request_payload_json=request_payload_json,
                    requester_user_id=requester_user_id,
                    parent_thread_id=parent_thread_id,
                    timeout_seconds=timeout_seconds,
                    db_path=db_path,
                )

        _approval_workflow = request_approval_workflow
        _workflow_registered = True
        return _approval_workflow


def get_approval_workflow() -> Any:
    """Return the DBOS-registered workflow callable, registering on first call.

    Test fixtures and the dispatcher use this to obtain the workflow
    without depending on import-time DBOS state.
    """
    return _register_workflow()


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


async def request_approval(
    approval_id: str,
    *,
    request_type: str,
    request_payload: dict[str, Any] | None,
    requester_user_id: str | None,
    parent_thread_id: str | None,
    requested_at: str,
    timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT_S,
    db_path: Path | str,
) -> ApprovalDecision:
    """Run the DBOS-managed approval workflow for ``approval_id``.

    This is the public entrypoint invoked by tools that gate behind
    approval (``shell_exec``, out-of-projects_root delegations,
    ``kill_delegation``). The workflow:

    1. INSERTs an ``approvals`` row (status=``pending``) with the
       caller-supplied ``requested_at``.
    2. Sends one AMC notification to ``parent_thread_id`` describing the
       request and the ``/approve <id>`` / ``/deny <id>`` reply contract.
    3. Suspends via :meth:`dbos.DBOS.recv_async` (topic ``approval_decision``)
       until the dispatcher (Task #40) routes the user reply or the
       ``timeout_seconds`` elapses.
    4. UPDATEs the row to the resolved status and returns
       :class:`ApprovalDecision`.

    Parameters
    ----------
    approval_id:
        Deterministic input. The caller (e.g. ``shell_exec``) MUST
        generate this ahead of time (UUIDv4 hex is conventional) and reuse
        it across retries so DBOS replay sees the same workflow id.
    request_type:
        One of ``shell_exec``, ``delegate_out_of_root``, ``kill_delegation``
        (matches the §5.8 CHECK constraint on ``approvals.request_type``).
    request_payload:
        Action-specific JSON payload describing what is being requested.
        Stored as JSON text in the row's ``request_payload`` column. May
        be ``None`` for callers with no extra context (rare).
    requester_user_id:
        Optional userspace identifier of the user who triggered the gated
        action. May be ``None`` for system-initiated approvals.
    parent_thread_id:
        AMC thread id (``amc:<channel_id>`` per §5.7) used both as the
        notification destination AND as the dispatcher routing key for
        the inbound ``/approve``/``/deny`` reply. ``None`` means no AMC
        channel is available — the notify step is a no-op and the
        workflow can only be resolved via external ``DBOS.send`` (e.g.
        an operator-issued cancel).
    requested_at:
        Caller-supplied UTC ISO 8601 timestamp. MUST be stable across
        retries (DBOS replay determinism) — do NOT call
        ``datetime.utcnow()`` inside the caller's retry loop.
    timeout_seconds:
        ``DBOS.recv_async`` timeout in seconds. Defaults to 24 h. When the
        timeout elapses without a decision, the row transitions to
        ``expired`` and the return value is
        ``ApprovalDecision(status='expired', resolved_by=None, reason='timeout after Ns')``.
    db_path:
        Filesystem path to ``state.db``. Accepts :class:`Path` or string;
        normalized to string for DBOS workflow args.

    Returns
    -------
    ApprovalDecision
        Typed decision capturing ``(approval_id, status, resolved_by, reason)``.

    Raises
    ------
    ApprovalError
        On permanent AMC notify error (``PLATFORM_AUTH``, ...) or when the
        inbound decision payload is malformed. On permanent notify
        failure the row is left ``pending`` so the workflow can be re-
        fired later with the same ``approval_id``.
    """
    workflow = _register_workflow()
    payload_json = json.dumps(
        request_payload or {}, sort_keys=True, separators=(",", ":")
    )
    db_path_str = str(db_path) if isinstance(db_path, Path) else db_path
    return await workflow(
        approval_id,
        requested_at,
        request_type,
        payload_json,
        requester_user_id,
        parent_thread_id,
        float(timeout_seconds),
        db_path_str,
    )


# ---------------------------------------------------------------------------
# Helper: external resolve (for kill_delegation force-cancel, etc.)
# ---------------------------------------------------------------------------


async def force_resolve_approval(
    workflow_id: str,
    *,
    status: str,
    resolved_by: str | None = None,
    reason: str | None = None,
) -> None:
    """Force-resolve a pending approval workflow from outside the body.

    Used by the delegation kill path: when an in-flight delegation is
    killed and its pending approval needs to be torn down, the killer
    calls this with ``status="cancelled"``. We send a
    ``DBOS.send(workflow_id, payload, "approval_decision")`` so the
    awaiting workflow body falls out of ``recv`` and runs through the
    same resolve-step path as a user decision.

    Parameters
    ----------
    workflow_id:
        The DBOS workflow id of the awaiting ``request_approval`` call.
        Conventionally obtained from the ``approvals.workflow_id`` column.
    status:
        Verb form (``cancel``/``cancelled``) or any value accepted by
        :func:`_normalize_decision_status`.
    resolved_by, reason:
        Optional metadata for the audit trail. ``resolved_by`` defaults
        to ``None`` (the system / killer path).

    Notes
    -----
    Calling this when no workflow is waiting on ``workflow_id`` is a no-op
    on the DBOS side (the message lands in DBOS's notification table and
    is collected the next time a matching ``recv`` runs — practically
    that's never, for a force-cancel against a now-defunct id). Callers
    SHOULD check the persisted ``approvals.status`` before sending to
    avoid hot-looping on already-resolved rows.
    """
    try:
        from dbos import DBOS  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("dbos package not importable") from exc

    payload = {
        "status": status,
        "resolved_by": resolved_by,
        "reason": reason,
    }
    try:
        await DBOS.send_async(workflow_id, payload, APPROVAL_TOPIC)
    except Exception:
        # Synchronous fallback for older DBOS versions: send is async-safe
        # but some versions expose it as sync-only.
        await asyncio.to_thread(DBOS.send, workflow_id, payload, APPROVAL_TOPIC)


# ---------------------------------------------------------------------------
# Re-exports
# ---------------------------------------------------------------------------

# A callable that takes (channel_id, text, idempotency_key) and returns an
# awaitable yielding a value with ``.message_id``. Imported for type
# annotations in the approval notify step.
_AmcSenderFn = Callable[[str, str, str], Awaitable[Any]]


__all__ = [
    "APPROVAL_TOPIC",
    "DEFAULT_APPROVAL_TIMEOUT_S",
    "STATUS_APPROVED",
    "STATUS_CANCELLED",
    "STATUS_DENIED",
    "STATUS_EXPIRED",
    "STATUS_PENDING",
    "ApprovalDecision",
    "ApprovalError",
    "NotifyError",  # re-exported convenience
    "_compose_approval_request_body",
    "_normalize_decision_status",
    "_request_summary_from_payload",
    "force_resolve_approval",
    "get_approval_workflow",
    "notify_approval",
    "request_approval",
]

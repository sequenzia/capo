"""Shared approval-gating helpers for agent tools (spec §5.8).

This module centralizes the typed exceptions and the
:func:`request_tool_approval` helper used by tools that gate behind the
DBOS-managed approval workflow:

* :func:`capo.tools.basic.shell_exec` — non-allowlisted commands
  (Task #42).
* :func:`capo.tools.claude_code.delegate_to_claude_code` — out-of-
  ``projects_root`` delegations (Task #43).
* :func:`capo.tools.codex.delegate_to_codex` — out-of-
  ``projects_root`` delegations AND ``sandbox=danger-full-access``
  (Task #43).
* :func:`capo.tools.delegations.kill_delegation` — operator-issued kills
  (Task #44).

Centralizing here avoids each tool re-implementing the same wiring
(check ``is_launched()`` → generate ``approval_id``/``requested_at`` →
build payload → ``await request_approval(...)`` → branch on
``decision.status``) and ensures the exception types are importable from
exactly one place.

Backwards compatibility
-----------------------

``ApprovalRequired`` and ``ApprovalRejected`` are re-exported from
:mod:`capo.tools.basic` for callers / tests that imported them from
their original home — those imports continue to work.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - imported only for type hints
    from capo.deps import CapoDeps
    from capo.workflows.approval import ApprovalDecision

logger = logging.getLogger("capo.tools.approval")


# ---------------------------------------------------------------------------
# Canonical request_type constants (matches the §5.8 / migration 002 CHECK
# constraint plus the Task #43 dangerous-sandbox extension).
# ---------------------------------------------------------------------------

#: Non-allowlisted shell command requiring human approval. Owned by
#: :func:`capo.tools.basic.shell_exec` (Task #42).
REQUEST_TYPE_SHELL_EXEC: str = "shell_exec"

#: Delegation whose ``repo_path`` is outside ``projects_root``. Owned by
#: :func:`delegate_to_claude_code` and :func:`delegate_to_codex` (Task #43).
REQUEST_TYPE_DELEGATE_OUT_OF_ROOT: str = "delegate_out_of_root"

#: Codex delegation requesting ``sandbox=danger-full-access`` per spec
#: §5.4 / §7.5 "Concurrency" — operators must explicitly approve before
#: Codex runs without sandboxing (Task #43). NB: the approvals table
#: CHECK constraint was extended in migration ``003_approvals_request_types``
#: to admit this value.
REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX: str = "delegate_dangerous_sandbox"

#: Operator-issued ``kill_delegation`` requiring approval (Task #44).
REQUEST_TYPE_KILL_DELEGATION: str = "kill_delegation"


# ---------------------------------------------------------------------------
# Typed exceptions (shared across all approval-gated tools).
# ---------------------------------------------------------------------------


class ApprovalRequired(Exception):
    """Raised by approval-gated tools when the approval workflow is
    unavailable AND the action would otherwise require approval.

    Per spec §5.8 the runtime path now routes gated actions through the
    DBOS-managed approval workflow (Tasks #42, #43, #44). This exception
    remains as the fallback signal for environments where that workflow
    cannot run — e.g. DBOS not launched (unit tests calling tools without
    booting DBOS) or when the approval module can't be imported. It is
    ALSO raised eagerly for hard rejections that the approval workflow
    itself would never accept (shell metacharacters, ``sudo``,
    tokenization errors, cwd outside scope) — those are structural
    argv-contract violations, not policy decisions a human should review.

    Attributes surface the *original* request string (command, repo
    path, etc.) and a one-line ``reason`` so the approval UI / log line
    can show the user exactly what was about to run.
    """

    def __init__(self, command: str, reason: str) -> None:
        self.command = command
        self.reason = reason
        super().__init__(f"approval required for {command!r}: {reason}")


class ApprovalRejected(Exception):
    """Raised by approval-gated tools when the approval workflow resolved
    to a non-``approved`` terminal status.

    The wrapped :class:`~capo.workflows.approval.ApprovalDecision` carries
    the canonical ``(approval_id, status, resolved_by, reason)`` tuple so
    the agent loop can surface a precise error to the user
    (``"denied by u-42: not safe to run"``, ``"timed out after 1800s"``,
    etc.).

    Status set on raise: ``denied``, ``expired``, or ``cancelled``. A
    workflow that returned ``approved`` does NOT raise — the caller
    proceeds with the gated action.
    """

    def __init__(self, decision: ApprovalDecision) -> None:
        self.decision = decision
        self.approval_id = decision.approval_id
        self.status = decision.status
        self.resolved_by = decision.resolved_by
        self.reason = decision.reason
        who = decision.resolved_by or "system"
        why = decision.reason or "no reason given"
        super().__init__(
            f"approval {decision.approval_id} {decision.status} "
            f"by {who}: {why}"
        )


class ApprovalUnavailableError(Exception):
    """Raised internally when the approval workflow cannot be reached.

    Caught by gated tools to fall back to :class:`ApprovalRequired` so
    unit tests + non-DBOS contexts keep their existing behavior.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def utc_iso_now() -> str:
    """Caller-stable UTC ISO 8601 timestamp for the ``approvals`` row.

    Format matches the §5.8 spec (``%Y-%m-%dT%H:%M:%SZ``). Generated
    ONCE per tool invocation by the caller and reused on retries so DBOS
    replay observes the same ``requested_at`` column value.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_approval_id() -> str:
    """Return a fresh uuid4 hex (32 chars) for use as ``approval_id``.

    Centralized so the caller, the persisted row, and any HTTP
    ``Idempotency-Key`` header all agree on the canonical shape.
    """
    return uuid.uuid4().hex


async def request_tool_approval(
    deps: CapoDeps,
    *,
    request_type: str,
    request_payload: dict[str, Any],
    approval_id: str | None = None,
    requested_at: str | None = None,
    log_subject: str = "",
) -> ApprovalDecision:
    """Invoke the §5.8 approval workflow for a tool-level gated action.

    Generates ``approval_id`` (uuid4 hex) and ``requested_at`` (ISO 8601
    UTC) BEFORE the call if the caller didn't supply them — this keeps
    determinism inside the caller's own retry loop while still letting
    tests / callers override for reproducibility.

    Raises :class:`ApprovalUnavailableError` (caught by callers to fall
    back to :class:`ApprovalRequired`) when the approval workflow can't
    be reached — missing ``state.db`` path, DBOS not launched, or
    import failure.

    Parameters
    ----------
    deps:
        :class:`CapoDeps` — carries ``settings`` (for ``approval.timeout_seconds``
        + ``paths.db_path``), ``user_id``, and ``thread_id``.
    request_type:
        One of the ``REQUEST_TYPE_*`` constants. The §5.8 CHECK
        constraint on ``approvals.request_type`` admits this set.
    request_payload:
        Action-specific JSON payload. Stored as JSON in the row's
        ``request_payload`` column. MUST NOT contain raw secrets — the
        delegation gating path uses a SHA-256 hash of the rendered
        prompt instead of the prompt itself.
    approval_id:
        Optional deterministic id (uuid4 hex). Generated fresh when
        omitted.
    requested_at:
        Optional caller-supplied UTC ISO 8601 timestamp. Generated
        fresh via :func:`utc_iso_now` when omitted.
    log_subject:
        Short, redaction-safe label for log lines (e.g. the command
        being run, the repo path being approved). Optional; defaults to
        ``request_type``.

    Returns
    -------
    ApprovalDecision
        Typed decision carrying ``(approval_id, status, resolved_by,
        reason)``. Callers branch on ``decision.status``: ``approved``
        → proceed; ``denied`` / ``expired`` / ``cancelled`` → raise
        :class:`ApprovalRejected`.
    """
    # Import lazily so this module is importable even when DBOS isn't
    # around (Phase 1 callers, tests).
    try:
        from capo.workflows import is_launched  # noqa: PLC0415
        from capo.workflows.approval import request_approval  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - DBOS is a hard dep
        raise ApprovalUnavailableError(
            f"approval workflow module not importable: {exc}"
        ) from exc

    try:
        launched = is_launched()
    except Exception as exc:  # noqa: BLE001 - defensive
        raise ApprovalUnavailableError(
            f"could not check DBOS launch state: {exc!r}"
        ) from exc
    if not launched:
        raise ApprovalUnavailableError("DBOS not launched")

    settings = deps.settings
    db_path = getattr(getattr(settings, "paths", None), "db_path", None)
    if db_path is None:
        raise ApprovalUnavailableError(
            "settings.paths.db_path is not configured"
        )

    aid = approval_id or new_approval_id()
    rat = requested_at or utc_iso_now()
    subject = log_subject or request_type

    timeout_s = float(
        getattr(getattr(settings, "approval", None), "timeout_seconds", 0)
        or 0
    )

    logger.info(
        "approval requested for %s (approval_id=%s request_type=%s)",
        subject,
        aid,
        request_type,
        extra={
            "event": "capo.tools.approval.requested",
            "approval_id": aid,
            "request_type": request_type,
        },
    )

    kwargs: dict[str, Any] = dict(
        request_type=request_type,
        request_payload=request_payload,
        requester_user_id=deps.user_id,
        parent_thread_id=deps.thread_id,
        requested_at=rat,
        db_path=db_path,
    )
    if timeout_s > 0:
        kwargs["timeout_seconds"] = timeout_s

    return await request_approval(aid, **kwargs)


def ensure_path(db_path: Any) -> Path:
    """Coerce ``db_path`` to :class:`Path`. Pragmatic helper for callers
    that store it as a string in DBOS workflow args.
    """
    return db_path if isinstance(db_path, Path) else Path(str(db_path))


__all__ = [
    "REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX",
    "REQUEST_TYPE_DELEGATE_OUT_OF_ROOT",
    "REQUEST_TYPE_KILL_DELEGATION",
    "REQUEST_TYPE_SHELL_EXEC",
    "ApprovalRejected",
    "ApprovalRequired",
    "ApprovalUnavailableError",
    "ensure_path",
    "new_approval_id",
    "request_tool_approval",
    "utc_iso_now",
]

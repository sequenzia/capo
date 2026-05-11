"""Integration tests for :mod:`capo.workflows.approval` (Task #39).

Covers the spec §5.8 / §5.9 / §7.5 acceptance criteria for the DBOS-managed
``request_approval`` workflow.

Functional / Edge Cases:

* approve path — ``DBOS.send(workflow_id, {"status": "approve", ...}, "approval_decision")``
  resolves the workflow to ``ApprovalDecision(status='approved')``.
* deny path — symmetric.
* timeout path — recv timeout elapses → status ``expired``.
* idempotent notify on workflow restart (re-entry).
* cancelled path — external ``force_resolve_approval`` resolves to ``cancelled``.
* permanent AMC error on notify -> ``ApprovalError`` raised, row left
  ``pending``.
* malformed decision payload -> ``ApprovalError``.
* deterministic notify body bytes (replay determinism).
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from capo.config import PathsConfig
from capo.memory.store import open_connection
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.approval import (
    APPROVAL_TOPIC,
    STATUS_APPROVED,
    STATUS_CANCELLED,
    STATUS_DENIED,
    STATUS_EXPIRED,
    STATUS_PENDING,
    ApprovalDecision,
    ApprovalError,
    _compose_approval_request_body,
    _normalize_decision_status,
    _request_summary_from_payload,
    force_resolve_approval,
    get_approval_workflow,
    request_approval,
)
from capo.workflows.delegation import register_amc_sender

# ---------------------------------------------------------------------------
# Fixtures / schema helpers
# ---------------------------------------------------------------------------


class _StubSettings:
    def __init__(self, paths: PathsConfig) -> None:
        self.paths = paths


def _make_settings(tmp_path: Path) -> _StubSettings:
    dotcapo = tmp_path / ".capo"
    return _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=dotcapo / "state.db",
            dbos_db_path=dotcapo / "dbos.db",
        )
    )


# The approvals table DDL mirrors migrations/versions/002_approvals.py
# verbatim. We don't run Alembic here to keep test startup fast — Task #38
# tests already prove the migration is correct.
_APPROVALS_DDL = """
CREATE TABLE IF NOT EXISTS approvals (
    approval_id       TEXT PRIMARY KEY,
    requested_at      TEXT NOT NULL,
    decided_at        TEXT,
    request_type      TEXT NOT NULL CHECK (
        request_type IN ('shell_exec','delegate_out_of_root','kill_delegation')
    ),
    request_payload   TEXT NOT NULL,
    requester_user_id TEXT,
    parent_thread_id  TEXT,
    status            TEXT NOT NULL CHECK (
        status IN ('pending','approved','denied','expired','cancelled')
    ),
    resolved_by       TEXT,
    reason            TEXT,
    workflow_id       TEXT
)
"""


def _ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = open_connection(db_path)
    try:
        conn.execute(_APPROVALS_DDL)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _clear_amc_sender() -> Iterator[None]:
    """Ensure no AMC sender or DBOS instance leaks across tests."""
    register_amc_sender(None)
    # Reset the lazy-registration cache so each test re-registers cleanly
    # against the fresh DBOS instance.
    import capo.workflows.approval as approval_mod

    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    yield
    register_amc_sender(None)
    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def dbos_settings(tmp_path: Path) -> _StubSettings:
    settings = _make_settings(tmp_path)
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    return settings


# ---------------------------------------------------------------------------
# AMC stub
# ---------------------------------------------------------------------------


class _StubSendResult:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class _StubAmcError(Exception):
    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AmcStub:
    """Records every AMC send + deduplicates on idempotency_key for assertions."""

    def __init__(
        self,
        *,
        message_id: str = "msg-1",
        permanent_code: str | None = None,
    ) -> None:
        self.calls: list[dict[str, str]] = []
        self.delivered_keys: set[str] = set()
        self._message_id = message_id
        self._permanent_code = permanent_code

    async def send(
        self, channel_id: str, text: str, idempotency_key: str
    ) -> _StubSendResult:
        self.calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        if self._permanent_code is not None:
            raise _StubAmcError(self._permanent_code, "permanent failure")
        self.delivered_keys.add(idempotency_key)
        return _StubSendResult(self._message_id)


def _utc_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_row(db_path: Path, approval_id: str) -> dict[str, object] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


# ===========================================================================
# Unit tests: helpers (no DBOS launch needed)
# ===========================================================================


def test_normalize_decision_status_accepts_verb_and_noun() -> None:
    assert _normalize_decision_status("approve") == STATUS_APPROVED
    assert _normalize_decision_status("approved") == STATUS_APPROVED
    assert _normalize_decision_status("DENY") == STATUS_DENIED
    assert _normalize_decision_status("denied") == STATUS_DENIED
    assert _normalize_decision_status("cancel") == STATUS_CANCELLED
    assert _normalize_decision_status("cancelled") == STATUS_CANCELLED
    assert _normalize_decision_status("expire") == STATUS_EXPIRED
    assert _normalize_decision_status("expired") == STATUS_EXPIRED


def test_normalize_decision_status_rejects_garbage() -> None:
    assert _normalize_decision_status("yes") is None
    assert _normalize_decision_status(None) is None
    assert _normalize_decision_status(42) is None


def test_compose_approval_request_body_deterministic() -> None:
    body1 = _compose_approval_request_body(
        approval_id="ap-1", request_type="shell_exec", request_summary="rm -rf /tmp/foo"
    )
    body2 = _compose_approval_request_body(
        approval_id="ap-1", request_type="shell_exec", request_summary="rm -rf /tmp/foo"
    )
    assert body1 == body2
    assert "ap-1" in body1
    assert "shell_exec" in body1
    assert "rm -rf /tmp/foo" in body1
    assert "/approve ap-1" in body1
    assert "/deny ap-1" in body1


def test_compose_approval_request_body_collapses_newlines() -> None:
    body = _compose_approval_request_body(
        approval_id="ap-1",
        request_type="shell_exec",
        request_summary="line one\nline two\r\nline three",
    )
    assert "\nline one line two line three\n" in body


def test_request_summary_from_payload_shell_exec() -> None:
    summary = _request_summary_from_payload(
        "shell_exec", {"command": ["git", "push"]}
    )
    assert "git push" in summary
    summary2 = _request_summary_from_payload("shell_exec", {"cmd": "ls -la"})
    assert "ls -la" in summary2


def test_request_summary_from_payload_kill_delegation() -> None:
    summary = _request_summary_from_payload(
        "kill_delegation", {"delegation_id": "d-abc"}
    )
    assert "d-abc" in summary


def test_request_summary_from_payload_delegate_out_of_root() -> None:
    summary = _request_summary_from_payload(
        "delegate_out_of_root",
        {"agent": "claude-code", "path": "/var/log"},
    )
    assert "claude-code" in summary
    assert "/var/log" in summary


# ===========================================================================
# Integration: full workflow round-trip
# ===========================================================================


async def test_approve_path_resolves_to_approved(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: approve path — DBOS.send approve_decision → ApprovalDecision(approved)."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-approve")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"

    workflow = get_approval_workflow()

    # Pre-allocate workflow id so we can DBOS.send to it after we start.
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "git push"}',
            "user-42",
            "amc:ch-A",
            10.0,  # short timeout — but we send before it elapses
            str(dbos_settings.paths.db_path),
        )

    # Give the workflow a tick to INSERT + notify + arm recv.
    await asyncio.sleep(0.5)

    # Verify pending row landed + notify fired.
    row = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row is not None, "INSERT step did not land pending row"
    assert row["status"] == STATUS_PENDING
    assert row["workflow_id"] == workflow_id
    assert len(stub.calls) == 1, "notify_approval should fire exactly once"
    assert stub.calls[0]["channel_id"] == "ch-A"

    # Send the approve decision.
    await DBOS.send_async(
        workflow_id,
        {"status": "approve", "resolved_by": "user-42", "reason": None},
        APPROVAL_TOPIC,
    )

    # Await the workflow.
    decision: ApprovalDecision = await handle.get_result()
    assert decision.approval_id == approval_id
    assert decision.status == STATUS_APPROVED
    assert decision.resolved_by == "user-42"

    # Row is terminal.
    row_final = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row_final is not None
    assert row_final["status"] == STATUS_APPROVED
    assert row_final["resolved_by"] == "user-42"
    assert row_final["decided_at"] is not None


async def test_deny_path_resolves_to_denied(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: deny path."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-deny")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "rm -rf /"}',
            "user-7",
            "amc:ch-D",
            10.0,
            str(dbos_settings.paths.db_path),
        )

    await asyncio.sleep(0.5)
    await DBOS.send_async(
        workflow_id,
        {"status": "deny", "resolved_by": "user-7", "reason": "looks dangerous"},
        APPROVAL_TOPIC,
    )

    decision: ApprovalDecision = await handle.get_result()
    assert decision.status == STATUS_DENIED
    assert decision.resolved_by == "user-7"
    assert decision.reason == "looks dangerous"

    row_final = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row_final is not None
    assert row_final["status"] == STATUS_DENIED
    assert row_final["reason"] == "looks dangerous"


async def test_timeout_path_resolves_to_expired(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: timeout — recv_async returns None → row=expired."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-timeout")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    # Short timeout — DBOS recv polling floor on SQLite is ~1s; use 2s.
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "echo hi"}',
            None,
            "amc:ch-T",
            2.0,
            str(dbos_settings.paths.db_path),
        )

    decision: ApprovalDecision = await handle.get_result()
    assert decision.status == STATUS_EXPIRED
    assert decision.resolved_by is None
    assert decision.reason is not None
    assert "timeout" in decision.reason.lower()

    row_final = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row_final is not None
    assert row_final["status"] == STATUS_EXPIRED
    assert row_final["decided_at"] is not None


async def test_cancelled_path_via_force_resolve(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: external cancel — force_resolve_approval → row=cancelled."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-cancel")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "kill_delegation",
            '{"delegation_id": "d-victim"}',
            "user-3",
            "amc:ch-C",
            10.0,
            str(dbos_settings.paths.db_path),
        )

    await asyncio.sleep(0.5)
    # External path force-cancels the approval (e.g. delegation was killed
    # while waiting for the approval).
    await force_resolve_approval(
        workflow_id,
        status="cancelled",
        resolved_by="system:killer",
        reason="delegation killed",
    )

    decision: ApprovalDecision = await handle.get_result()
    assert decision.status == STATUS_CANCELLED
    assert decision.resolved_by == "system:killer"
    assert decision.reason == "delegation killed"

    row_final = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row_final is not None
    assert row_final["status"] == STATUS_CANCELLED


async def test_idempotent_notify_on_replay(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: workflow re-entry with same workflow id → notify fires once.

    Models the §5.8 "Workflow restart between notify-sent and recv-await:
    notify_approval not re-sent" criterion. DBOS replays the cached step
    return on a same-id resume, so the AMC stub records exactly one
    unique idempotency-key.
    """
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-idem")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()
    requested_at = _utc_iso()
    db_path = str(dbos_settings.paths.db_path)

    # First invocation: complete the workflow (approve path).
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            requested_at,
            "shell_exec",
            '{"command": "ls"}',
            "user-1",
            "amc:ch-I",
            10.0,
            db_path,
        )
    await asyncio.sleep(0.5)
    await DBOS.send_async(
        workflow_id,
        {"status": "approve", "resolved_by": "user-1", "reason": None},
        APPROVAL_TOPIC,
    )
    decision1 = await handle.get_result()
    assert decision1.status == STATUS_APPROVED

    # Capture initial call count.
    calls_after_first = len(stub.calls)
    keys_after_first = len(stub.delivered_keys)
    assert calls_after_first == 1
    assert keys_after_first == 1

    # Second invocation under the SAME workflow id — DBOS treats this as a
    # resume of an already-completed workflow and returns the cached
    # result without re-running the workflow body.
    with SetWorkflowID(workflow_id):
        replayed = await workflow(
            approval_id,
            requested_at,
            "shell_exec",
            '{"command": "ls"}',
            "user-1",
            "amc:ch-I",
            10.0,
            db_path,
        )
    assert replayed.status == STATUS_APPROVED

    # Idempotency-key sent to AMC didn't fire a SECOND distinct delivery
    # (DBOS step-state replay short-circuits the notify step body).
    assert len(stub.delivered_keys) == keys_after_first, (
        f"notify_approval re-fired on workflow replay: "
        f"keys={stub.delivered_keys}"
    )


async def test_permanent_amc_error_raises_approval_error(
    dbos_settings: _StubSettings,
) -> None:
    """Error handling: AMC permanent error -> ApprovalError, row stays pending."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(permanent_code="PLATFORM_AUTH")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "echo bad"}',
            "user-9",
            "amc:ch-E",
            10.0,
            str(dbos_settings.paths.db_path),
        )

    with pytest.raises(Exception) as excinfo:
        await handle.get_result()
    # DBOS may wrap the exception — check by name on the chain.
    chained = excinfo.value
    found = False
    while chained is not None:
        if isinstance(chained, ApprovalError):
            assert chained.code == "PLATFORM_AUTH"
            found = True
            break
        chained = chained.__cause__ or chained.__context__
    assert found, f"expected ApprovalError in cause chain, got {excinfo.value!r}"

    # Row should still be pending (acceptance criterion: revert/keep pending
    # on permanent notify failure, no terminal write).
    row = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row is not None
    assert row["status"] == STATUS_PENDING


async def test_request_approval_public_entrypoint(
    dbos_settings: _StubSettings,
) -> None:
    """Public ``request_approval`` API: invoke + send → returns ApprovalDecision."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-pub")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"

    async def _runner() -> ApprovalDecision:
        with SetWorkflowID(workflow_id):
            return await request_approval(
                approval_id,
                request_type="shell_exec",
                request_payload={"command": "pwd"},
                requester_user_id="user-pub",
                parent_thread_id="amc:ch-P",
                requested_at=_utc_iso(),
                timeout_seconds=10.0,
                db_path=dbos_settings.paths.db_path,
            )

    task = asyncio.create_task(_runner())
    # Wait until the row appears (workflow has reached recv).
    for _ in range(40):
        await asyncio.sleep(0.1)
        row = _read_row(dbos_settings.paths.db_path, approval_id)
        if row is not None:
            break
    else:
        pytest.fail("workflow did not INSERT pending row in time")

    await DBOS.send_async(
        workflow_id,
        {"status": "approve", "resolved_by": "user-pub", "reason": None},
        APPROVAL_TOPIC,
    )

    decision = await task
    assert decision.approval_id == approval_id
    assert decision.status == STATUS_APPROVED


async def test_workflow_id_persisted_on_row(
    dbos_settings: _StubSettings,
) -> None:
    """The pending row carries the DBOS workflow id (for inbound dispatcher)."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-wf")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "pwd"}',
            "user-w",
            "amc:ch-W",
            10.0,
            str(dbos_settings.paths.db_path),
        )

    await asyncio.sleep(0.5)
    row = _read_row(dbos_settings.paths.db_path, approval_id)
    assert row is not None
    assert row["workflow_id"] == workflow_id

    # Clean up: approve so the workflow exits.
    await DBOS.send_async(
        workflow_id,
        {"status": "approve", "resolved_by": "user-w", "reason": None},
        APPROVAL_TOPIC,
    )
    await handle.get_result()


async def test_notify_approval_no_amc_channel_skips_send(
    dbos_settings: _StubSettings,
) -> None:
    """Edge: parent_thread_id without 'amc:' prefix → notify is a no-op."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub()
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "ls"}',
            None,
            "cli:local",  # no amc: prefix
            10.0,
            str(dbos_settings.paths.db_path),
        )

    await asyncio.sleep(0.5)
    assert len(stub.calls) == 0  # notify skipped

    # Workflow still requires a decision to exit — send one.
    await DBOS.send_async(
        workflow_id,
        {"status": "approve", "resolved_by": "system", "reason": None},
        APPROVAL_TOPIC,
    )
    decision = await handle.get_result()
    assert decision.status == STATUS_APPROVED


async def test_unknown_decision_payload_raises_approval_error(
    dbos_settings: _StubSettings,
) -> None:
    """Error: unrecognized decision status -> ApprovalError(UNKNOWN_DECISION)."""
    from dbos import DBOS, SetWorkflowID

    stub = AmcStub(message_id="msg-bad")
    register_amc_sender(stub.send)

    approval_id = f"ap-{uuid.uuid4().hex[:8]}"
    workflow_id = f"wf-approval-{uuid.uuid4().hex[:8]}"
    workflow = get_approval_workflow()

    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(
            workflow,
            approval_id,
            _utc_iso(),
            "shell_exec",
            '{"command": "ls"}',
            "user-u",
            "amc:ch-U",
            10.0,
            str(dbos_settings.paths.db_path),
        )

    await asyncio.sleep(0.5)
    await DBOS.send_async(
        workflow_id,
        {"status": "maybe", "resolved_by": "user-u"},
        APPROVAL_TOPIC,
    )

    with pytest.raises(Exception) as excinfo:
        await handle.get_result()
    chained = excinfo.value
    found = False
    while chained is not None:
        if isinstance(chained, ApprovalError):
            assert chained.code == "UNKNOWN_DECISION"
            found = True
            break
        chained = chained.__cause__ or chained.__context__
    assert found

"""Integration tests for Task #44 — kill_delegation approval gating.

Covers the spec §5.8 / §5.10 / §7.5 acceptance criteria for routing
non-owner ``kill_delegation`` calls through the DBOS-managed
:func:`capo.workflows.approval.request_approval` workflow, plus the
cascade-cancel behavior on pending approvals tied to the killed
delegation.

Functional / Edge Cases:

* Owner kill (delegations.user_id == requester_user_id) → no approval
  round-trip; row flips to ``killed`` directly.
* Non-owner kill + ``/approve`` → kill proceeds, returns
  ``status='killed'``; approvals row lands in ``approved``.
* Non-owner kill + ``/deny`` → :class:`ApprovalRejected` is raised; row
  stays ``running``.
* Pending approval whose ``request_payload.delegation_id`` matches the
  killed delegation → force_resolve_approval fires ``cancelled``.
* Already-terminal delegation → idempotent no-op, no approval call.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools.basic import ApprovalRejected
from capo.tools.delegations import (
    _utc_now_iso,
    kill_delegation,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.approval import (
    APPROVAL_TOPIC,
    STATUS_CANCELLED,
    STATUS_DENIED,
    request_approval,
)
from capo.workflows.delegation import register_amc_sender

# ---------------------------------------------------------------------------
# Settings scaffolding (mirrors tests/test_shell_exec_approval.py)
# ---------------------------------------------------------------------------

_VALID_TOML_TMPL = """\
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "{workspaces_root}"
projects_root   = "{projects_root}"
db_path         = "{db_path}"
dbos_db_path    = "{dbos_db_path}"

[soul]
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8080"
agent_id              = "capo"
listen_host           = "127.0.0.1"
listen_port           = 8090
max_boot_wait_seconds = 60

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[retention]
delegation_output_days = 7

[compaction]
threshold_tokens = 100000
preserve_delegation_handles = true

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

[users.owner]
amc_senders = ["+15551234567"]
display_name = "Owner"

[observability]
logfire_enabled = true
"""

_VALID_ENV = {
    "AMC_WEBHOOK_SECRET": "shhh-webhook",
    "AMC_BEARER_TOKEN": "shhh-bearer",
}


_DELEGATIONS_DDL = """
CREATE TABLE IF NOT EXISTS delegations (
    id                       TEXT PRIMARY KEY,
    user_id                  TEXT NOT NULL,
    agent                    TEXT NOT NULL CHECK (agent IN ('claude-code','codex')),
    workspace                TEXT NOT NULL,
    pid                      INTEGER,
    session_id_subagent      TEXT,
    brief_json               TEXT NOT NULL,
    status                   TEXT NOT NULL CHECK (
        status IN ('running','completed','failed','killed','pending_approval')
    ),
    started_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at                 TIMESTAMP,
    parent_thread_id         TEXT,
    summary                  TEXT,
    cost_usd                 REAL NOT NULL DEFAULT 0,
    model                    TEXT,
    heartbeat_intervals_json TEXT NOT NULL DEFAULT '[]'
)
"""

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
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_APPROVALS_DDL)
        conn.commit()
    finally:
        conn.close()


def _insert_delegation_row(
    db_path: Path,
    *,
    delegation_id: str,
    user_id: str,
    status: str = "running",
    pid: int | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, brief_json,
                status, started_at, parent_thread_id
            ) VALUES (?, ?, 'claude-code', ?, ?, '{}', ?, ?, 'amc:ch-A')
            """,
            (
                delegation_id,
                user_id,
                f"/tmp/ws/{delegation_id}",
                pid,
                status,
                _utc_now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _read_delegation_row(db_path: Path, delegation_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return dict(row)


def _read_approvals(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM approvals ORDER BY requested_at"
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# AMC stub
# ---------------------------------------------------------------------------


class _StubSendResult:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class AmcStub:
    def __init__(self, *, message_id: str = "msg-1") -> None:
        self.calls: list[dict[str, str]] = []
        self._message_id = message_id

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
        return _StubSendResult(self._message_id)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path]:
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()
    (tmp_path / ".capo").mkdir()
    db_path = tmp_path / ".capo" / "state.db"
    dbos_path = tmp_path / ".capo" / "dbos.db"

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(db_path),
            dbos_db_path=str(dbos_path),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    _ensure_schema(db_path)
    return settings, db_path


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def make_deps(
    scaffold: tuple[Settings, Path],
    http_client: httpx.AsyncClient,
):
    settings, _ = scaffold

    def _make(
        *, user_id: str = "owner", thread_id: str = "amc:ch-A"
    ) -> CapoDeps:
        return CapoDeps.from_settings(
            settings=settings,
            http_client=http_client,
            user_id=user_id,
            thread_id=thread_id,
        )

    return _make


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


@pytest.fixture(autouse=True)
def _reset_workflows() -> Iterator[None]:
    """Reset module-level state between tests."""
    register_amc_sender(None)
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
def dbos(scaffold: tuple[Settings, Path]) -> Iterator[Settings]:
    settings, _ = scaffold
    init_dbos(settings)
    launch_dbos()
    yield settings


# ---------------------------------------------------------------------------
# Integration: owner kills own delegation — no approval
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_kills_own_delegation_no_approval(
    scaffold: tuple[Settings, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """delegations.user_id == requester → direct kill, no AMC traffic."""
    _, db_path = scaffold
    stub = AmcStub()
    register_amc_sender(stub.send)

    # Spawn a real long-lived child so we can prove SIGTERM works.
    proc = subprocess.Popen(  # noqa: S603 - fixed argv
        [sys.executable, "-c", "import time; time.sleep(60)"],
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert proc.poll() is None
        _insert_delegation_row(
            db_path,
            delegation_id="own-real",
            user_id="user-42",
            pid=proc.pid,
        )

        result = await kill_delegation(
            _ctx(make_deps(user_id="user-42")),
            "own-real",
            "owner kill",
        )

        assert result["status"] == "killed"
        # No approval round-trip — no AMC notify.
        assert stub.calls == [], "owner kill must NOT invoke AMC"
        # No approvals row was created.
        assert _read_approvals(db_path) == []

        # Child reaped under SIGTERM.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            await asyncio.sleep(0.05)
        assert proc.poll() is not None, "child still alive"

        row = _read_delegation_row(db_path, "own-real")
        assert row["status"] == "killed"
        assert row["summary"] == "owner kill"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)


# ---------------------------------------------------------------------------
# Integration: non-owner kill — approval required
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_owner_kill_requires_approval_and_proceeds_on_approved(
    scaffold: tuple[Settings, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """Non-owner kill → approval requested → approved → kill proceeds."""
    _, db_path = scaffold
    from dbos import DBOS

    stub = AmcStub(message_id="msg-kill-approve")
    register_amc_sender(stub.send)

    _insert_delegation_row(
        db_path,
        delegation_id="not-mine",
        user_id="user-owner",
        pid=None,  # No subprocess — kill just flips the row.
    )

    async def _approver() -> None:
        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not stub.calls:
                continue
            conn = sqlite3.connect(str(db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT approval_id, workflow_id FROM approvals "
                    "WHERE status='pending' "
                    "ORDER BY requested_at DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            if row is None or row["workflow_id"] is None:
                continue
            await DBOS.send_async(
                row["workflow_id"],
                {
                    "status": "approve",
                    "resolved_by": "user-other",
                    "reason": None,
                },
                APPROVAL_TOPIC,
            )
            return
        raise AssertionError("approver did not see pending row")

    approver_task = asyncio.create_task(_approver())
    try:
        result = await kill_delegation(
            _ctx(make_deps(user_id="user-other")),
            "not-mine",
            "killing for testing",
        )
    finally:
        with contextlib.suppress(Exception):
            await approver_task

    assert result["status"] == "killed"
    # AMC notify fired once for the kill approval.
    assert len(stub.calls) == 1

    # Persisted approvals row: approved, payload references the delegation.
    rows = _read_approvals(db_path)
    assert len(rows) == 1
    appr = rows[0]
    assert appr["status"] == "approved"
    assert appr["request_type"] == "kill_delegation"
    assert appr["resolved_by"] == "user-other"
    payload = json.loads(appr["request_payload"])
    assert payload["delegation_id"] == "not-mine"

    # Delegation row flipped to killed.
    drow = _read_delegation_row(db_path, "not-mine")
    assert drow["status"] == "killed"
    assert drow["summary"] == "killing for testing"


# ---------------------------------------------------------------------------
# Integration: deny → ApprovalRejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_owner_kill_denied_raises_rejected(
    scaffold: tuple[Settings, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """Non-owner kill + /deny → ApprovalRejected(status='denied').

    Delegation row must NOT transition to killed.
    """
    _, db_path = scaffold
    from dbos import DBOS

    stub = AmcStub(message_id="msg-kill-deny")
    register_amc_sender(stub.send)

    _insert_delegation_row(
        db_path,
        delegation_id="deny-target",
        user_id="user-owner",
    )

    async def _denier() -> None:
        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not stub.calls:
                continue
            conn = sqlite3.connect(str(db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT approval_id, workflow_id FROM approvals "
                    "WHERE status='pending' "
                    "ORDER BY requested_at DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            if row is None or row["workflow_id"] is None:
                continue
            await DBOS.send_async(
                row["workflow_id"],
                {
                    "status": "deny",
                    "resolved_by": "user-other",
                    "reason": "not allowed",
                },
                APPROVAL_TOPIC,
            )
            return
        raise AssertionError("denier did not see pending row")

    denier_task = asyncio.create_task(_denier())
    try:
        with pytest.raises(ApprovalRejected) as exc_info:
            await kill_delegation(
                _ctx(make_deps(user_id="user-other")),
                "deny-target",
                "trying to kill",
            )
    finally:
        with contextlib.suppress(Exception):
            await denier_task

    err = exc_info.value
    assert err.status == STATUS_DENIED
    assert err.resolved_by == "user-other"

    # Delegation row untouched.
    drow = _read_delegation_row(db_path, "deny-target")
    assert drow["status"] == "running"
    assert drow["ended_at"] is None


# ---------------------------------------------------------------------------
# Integration: kill + cascade-cancel pending approval tied to the delegation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_kill_cascade_cancels_tied_pending_approval(
    scaffold: tuple[Settings, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """A pending approval tied to a killed delegation is force-cancelled.

    Simulates the §5.10 scenario: a delegation has a *separate* pending
    approval (e.g. a ``delegate_out_of_root`` approval keyed by the
    delegation's id). When the owner kills the delegation, the pending
    approval workflow falls out of recv with ``status='cancelled'``.
    """
    _, db_path = scaffold

    stub = AmcStub(message_id="msg-tied")
    register_amc_sender(stub.send)

    delegation_id = "del-with-tied"
    _insert_delegation_row(
        db_path,
        delegation_id=delegation_id,
        user_id="user-42",
    )

    # Fire a separate approval whose payload references the delegation.
    # We pin it to ``delegate_out_of_root`` so it's a meaningfully different
    # request type from the kill-delegation approval (and it stays pending
    # waiting for either user action OR our force-cancel).
    tied_approval_id = uuid.uuid4().hex
    tied_requested_at = "2026-05-11T00:00:00Z"

    async def _wait_for_pending() -> None:
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
            conn = sqlite3.connect(str(db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT workflow_id FROM approvals "
                    "WHERE approval_id = ? AND status = 'pending'",
                    (tied_approval_id,),
                ).fetchone()
            finally:
                conn.close()
            if row is not None and row["workflow_id"] is not None:
                return
        raise AssertionError("tied approval never went pending")

    # Run the tied approval workflow as a background task; it will block
    # in recv until our kill_delegation cascade fires the cancel.
    tied_task = asyncio.create_task(
        request_approval(
            tied_approval_id,
            request_type="delegate_out_of_root",
            request_payload={
                "delegation_id": delegation_id,
                "path": "/tmp/elsewhere",
            },
            requester_user_id="user-42",
            parent_thread_id="amc:ch-A",
            requested_at=tied_requested_at,
            timeout_seconds=30,
            db_path=str(db_path),
        )
    )

    try:
        await _wait_for_pending()

        # Now owner kills the delegation. Cascade should force-cancel
        # the tied approval.
        result = await kill_delegation(
            _ctx(make_deps(user_id="user-42")),
            delegation_id,
            "owner kill with tied approval",
        )
        assert result["status"] == "killed"
        # Exactly one tied approval was cascade-cancelled.
        assert result["cascaded_approvals"] == 1

        # The tied approval workflow falls out of recv with `cancelled`.
        decision = await asyncio.wait_for(tied_task, timeout=10.0)
        assert decision.status == STATUS_CANCELLED
        assert decision.resolved_by == "system:killer:user-42"
        assert decision.reason == f"delegation {delegation_id} killed"

        # Persisted row matches.
        rows = _read_approvals(db_path)
        tied_row = next(r for r in rows if r["approval_id"] == tied_approval_id)
        assert tied_row["status"] == "cancelled"
        assert tied_row["resolved_by"] == "system:killer:user-42"
    finally:
        if not tied_task.done():
            tied_task.cancel()
            with contextlib.suppress(BaseException):
                await tied_task

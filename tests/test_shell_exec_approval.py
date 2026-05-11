"""Integration tests for Task #42 — shell_exec approval-workflow gating.

Covers the spec §5.8 / §5.9 / §7.5 acceptance criteria for routing
non-allowlisted ``shell_exec`` invocations through the DBOS-managed
:func:`capo.workflows.approval.request_approval` workflow.

Functional / Edge Cases:

* Non-allowlisted command → approval workflow invoked; user ``/approve``
  resolves the workflow and the command executes; ``ShellResult`` returned.
* Non-allowlisted command → ``/deny`` raises :class:`ApprovalRejected`.
* Allowlisted command → NO approval workflow invoked (no DBOS / AMC
  traffic); ``ShellResult`` returned directly.
* Approval timeout → :class:`ApprovalRejected` with
  ``decision.status == 'expired'``.
* Permanent AMC notify error → :class:`ApprovalError` surfaces.
* DBOS-not-launched → falls back to :class:`ApprovalRequired`
  (backwards-compatibility contract documented in the task spec).
* Approval-id + requested_at deterministic-input contract:
  ``approval_id`` is a fresh uuid4 hex (32 chars), ``requested_at`` is
  caller-supplied ISO 8601 UTC (``%Y-%m-%dT%H:%M:%SZ``).
* Request payload shape: ``{"command", "args", "cwd"}`` JSON-encoded.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import sqlite3
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools.basic import (
    ApprovalRejected,
    ApprovalRequired,
    ShellResult,
    shell_exec,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.approval import (
    APPROVAL_TOPIC,
    STATUS_DENIED,
    STATUS_EXPIRED,
    ApprovalError,
)
from capo.workflows.delegation import register_amc_sender

# ---------------------------------------------------------------------------
# Settings scaffolding (mirrors tests/test_shell_exec.py)
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


# Approvals table DDL — mirrors migrations/versions/002_approvals.py so we
# don't have to run Alembic in tests. Task #38 tests prove the migration's
# correctness.
_APPROVALS_DDL = """
CREATE TABLE IF NOT EXISTS approvals (
    approval_id       TEXT PRIMARY KEY,
    requested_at      TEXT NOT NULL,
    decided_at        TEXT,
    request_type      TEXT NOT NULL CHECK (
        request_type IN (
            'shell_exec',
            'delegate_out_of_root',
            'delegate_dangerous_sandbox',
            'kill_delegation'
        )
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


def _read_row(db_path: Path, approval_id: str) -> dict[str, object] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# AMC stub (mirrors test_workflows_approval.py)
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()
    (tmp_path / ".capo").mkdir()
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(tmp_path / ".capo" / "state.db"),
            dbos_db_path=str(tmp_path / ".capo" / "dbos.db"),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    return settings, projects_root, workspaces_root


@pytest.fixture
def deps(scaffold: tuple[Settings, Path, Path]) -> CapoDeps:
    settings, _, _ = scaffold
    return CapoDeps.from_settings(
        settings=settings,
        http_client=httpx.AsyncClient(),
        user_id="user-42",
        thread_id="amc:ch-A",
    )


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


@pytest.fixture(autouse=True)
def _reset_workflows() -> Iterator[None]:
    """Reset module-level state between tests (matches approval suite)."""
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
def dbos(scaffold: tuple[Settings, Path, Path]) -> Iterator[Settings]:
    """Launch DBOS against the scaffold's state.db + ensure approvals schema."""
    settings, _, _ = scaffold
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)
    launch_dbos()
    yield settings


# ---------------------------------------------------------------------------
# Allowlisted: NO approval workflow invoked
# ---------------------------------------------------------------------------


async def test_allowlisted_command_no_approval(
    deps: CapoDeps,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """Allowlisted commands run directly — never touch DBOS or AMC.

    No ``dbos`` fixture is requested — if shell_exec accidentally tried to
    invoke the approval workflow, importing/launching DBOS would fail and
    the test would error. This proves the allowlisted path is unchanged.
    """
    _, projects_root, _ = scaffold
    stub = AmcStub()
    register_amc_sender(stub.send)

    result = await shell_exec(_ctx(deps), "pwd", cwd=str(projects_root))
    assert isinstance(result, ShellResult)
    assert result.exit_code == 0
    assert stub.calls == [], "allowlisted path must NOT invoke AMC"


# ---------------------------------------------------------------------------
# Approve path → command executes
# ---------------------------------------------------------------------------


async def test_non_allowlisted_approved_executes(
    deps: CapoDeps,
    scaffold: tuple[Settings, Path, Path],
    dbos: Settings,
) -> None:
    """Non-allowlisted command → approval requested → approved → executes.

    We pick ``echo`` (NOT in the test allowlist) and verify:

    * The approval workflow is invoked once (one AMC notify).
    * Sending ``{"status": "approve", ...}`` via DBOS resolves the
      workflow.
    * ``shell_exec`` returns a :class:`ShellResult` from the actual
      ``echo`` run.
    * The approvals row lands in ``approved``.
    """
    _, projects_root, _ = scaffold
    from dbos import DBOS

    stub = AmcStub(message_id="msg-shell-approve")
    register_amc_sender(stub.send)

    # Echo prints its arg to stdout — gives us a deterministic ShellResult
    # to assert against. NOT in the test allowlist.
    cmd = "echo hello-from-approval"

    async def _approver() -> None:
        # Poll for the pending row, then send the approve decision.
        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not stub.calls:
                continue
            # We need the workflow_id to send. Look it up from the row.
            conn = sqlite3.connect(str(dbos.paths.db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT approval_id, workflow_id, request_payload "
                    "FROM approvals WHERE status='pending' "
                    "ORDER BY requested_at DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            if row is None or row["workflow_id"] is None:
                continue
            await DBOS.send_async(
                row["workflow_id"],
                {"status": "approve", "resolved_by": "user-42", "reason": None},
                APPROVAL_TOPIC,
            )
            return
        raise AssertionError("approver loop did not see a pending row")

    approver_task = asyncio.create_task(_approver())
    try:
        result = await shell_exec(_ctx(deps), cmd, cwd=str(projects_root))
    finally:
        with contextlib.suppress(Exception):
            await approver_task

    assert isinstance(result, ShellResult)
    assert result.exit_code == 0
    assert "hello-from-approval" in result.stdout

    # AMC notify fired exactly once.
    assert len(stub.calls) == 1
    assert stub.calls[0]["channel_id"] == "ch-A"  # from thread_id amc:ch-A

    # Inspect persisted row — request_payload contains command/args/cwd.
    conn = sqlite3.connect(str(dbos.paths.db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE status='approved' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None, "approved row should be persisted"
    assert row["request_type"] == "shell_exec"
    assert row["resolved_by"] == "user-42"
    assert row["requester_user_id"] == "user-42"
    assert row["parent_thread_id"] == "amc:ch-A"

    # requested_at MUST be in the caller-supplied ISO 8601 UTC format.
    requested_at = row["requested_at"]
    assert isinstance(requested_at, str)
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", requested_at
    ), f"requested_at not ISO8601: {requested_at!r}"

    # approval_id is a 32-char uuid4 hex.
    assert isinstance(row["approval_id"], str)
    assert len(row["approval_id"]) == 32
    # Round-trip through uuid.UUID to confirm hex shape.
    uuid.UUID(hex=row["approval_id"])

    # request_payload JSON has the expected shape.
    import json

    payload = json.loads(row["request_payload"])
    assert payload["command"] == cmd
    assert payload["args"] == ["echo", "hello-from-approval"]
    assert payload["cwd"] == str(projects_root)


# ---------------------------------------------------------------------------
# Deny path → ApprovalRejected
# ---------------------------------------------------------------------------


async def test_non_allowlisted_denied_raises_rejected(
    deps: CapoDeps,
    scaffold: tuple[Settings, Path, Path],
    dbos: Settings,
) -> None:
    """Non-allowlisted command → /deny → ApprovalRejected(status='denied')."""
    _, projects_root, _ = scaffold
    from dbos import DBOS

    stub = AmcStub(message_id="msg-shell-deny")
    register_amc_sender(stub.send)

    async def _denier() -> None:
        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not stub.calls:
                continue
            conn = sqlite3.connect(str(dbos.paths.db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT approval_id, workflow_id FROM approvals "
                    "WHERE status='pending' ORDER BY requested_at DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            if row is None or row["workflow_id"] is None:
                continue
            await DBOS.send_async(
                row["workflow_id"],
                {
                    "status": "deny",
                    "resolved_by": "user-42",
                    "reason": "not safe to run",
                },
                APPROVAL_TOPIC,
            )
            return
        raise AssertionError("denier loop did not see a pending row")

    denier_task = asyncio.create_task(_denier())
    try:
        with pytest.raises(ApprovalRejected) as exc_info:
            await shell_exec(
                _ctx(deps), "echo nope", cwd=str(projects_root)
            )
    finally:
        with contextlib.suppress(Exception):
            await denier_task

    err = exc_info.value
    assert err.status == STATUS_DENIED
    assert err.resolved_by == "user-42"
    assert err.reason == "not safe to run"
    assert err.decision.status == STATUS_DENIED
    # Helpful str() for log lines.
    assert "denied" in str(err)
    assert err.approval_id in str(err)


# ---------------------------------------------------------------------------
# Timeout → ApprovalRejected with status=expired
# ---------------------------------------------------------------------------


async def test_non_allowlisted_timeout_raises_rejected(
    scaffold: tuple[Settings, Path, Path],
    dbos: Settings,
) -> None:
    """recv timeout elapses → ApprovalRejected(status='expired').

    We override ``[approval].timeout_seconds`` to a value just above the
    DBOS SQLite recv polling floor (~1s).
    """
    settings, projects_root, _ = scaffold
    # Override the approval timeout on the loaded settings instance to a
    # short value. Settings is a Pydantic frozen model so we re-create the
    # approval section via the underlying assignment helper.
    object.__setattr__(settings.approval, "timeout_seconds", 2)

    deps_local = CapoDeps.from_settings(
        settings=settings,
        http_client=httpx.AsyncClient(),
        user_id="user-42",
        thread_id="amc:ch-A",
    )

    stub = AmcStub(message_id="msg-shell-timeout")
    register_amc_sender(stub.send)

    with pytest.raises(ApprovalRejected) as exc_info:
        await shell_exec(
            _ctx(deps_local), "echo timeout-test", cwd=str(projects_root)
        )

    err = exc_info.value
    assert err.status == STATUS_EXPIRED
    assert err.resolved_by is None  # system-resolved
    assert err.reason is not None
    assert "timeout" in err.reason


# ---------------------------------------------------------------------------
# Permanent AMC notify error → ApprovalError surfaces
# ---------------------------------------------------------------------------


async def test_permanent_amc_error_surfaces(
    deps: CapoDeps,
    scaffold: tuple[Settings, Path, Path],
    dbos: Settings,
) -> None:
    """Permanent AMC error (PLATFORM_AUTH) on notify → ApprovalError to caller."""
    _, projects_root, _ = scaffold
    stub = AmcStub(permanent_code="PLATFORM_AUTH")
    register_amc_sender(stub.send)

    with pytest.raises(ApprovalError) as exc_info:
        await shell_exec(
            _ctx(deps), "echo permerror", cwd=str(projects_root)
        )

    assert exc_info.value.code == "PLATFORM_AUTH"


# ---------------------------------------------------------------------------
# DBOS-not-launched fallback → ApprovalRequired
# ---------------------------------------------------------------------------


async def test_dbos_not_launched_falls_back_to_approval_required(
    deps: CapoDeps,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """When DBOS is not launched, non-allowlisted commands raise ApprovalRequired.

    The task spec leaves us a choice between "raise typed error" and "fall
    through to prior ApprovalRequired behavior" — we picked the latter for
    backwards compatibility with unit tests and degraded environments.
    """
    _, projects_root, _ = scaffold
    # Crucially, no ``dbos`` fixture — is_launched() returns False.
    assert not is_launched()

    with pytest.raises(ApprovalRequired) as exc_info:
        await shell_exec(
            _ctx(deps), "echo fallback", cwd=str(projects_root)
        )
    # Reason explicitly mentions the unavailable workflow.
    assert "approval workflow is unavailable" in exc_info.value.reason
    assert "DBOS not launched" in exc_info.value.reason

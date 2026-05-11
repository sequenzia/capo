"""Integration tests for Task #43 — delegation approval-workflow gating.

Covers the spec §5.3 / §5.4 / §5.8 / §7.5 acceptance criteria for
routing the two delegation approval gates through the DBOS-managed
:func:`capo.workflows.approval.request_approval` workflow:

* :func:`delegate_to_claude_code` — out-of-``projects_root`` →
  ``request_type='delegate_out_of_root'``.
* :func:`delegate_to_codex` — out-of-``projects_root`` →
  ``request_type='delegate_out_of_root'``.
* :func:`delegate_to_codex` — ``sandbox=danger-full-access`` →
  ``request_type='delegate_dangerous_sandbox'``.

Functional / Edge Cases:

* Out-of-root CC delegation → approval flow → ``/approve`` → delegation
  proceeds normally (worktree create → spawn → handoff).
* Out-of-root Codex delegation → approval flow → ``/approve`` →
  delegation proceeds.
* Dangerous-sandbox Codex delegation (inside projects_root) → approval
  required (``delegate_dangerous_sandbox`` request_type).
* Deny path → :class:`ApprovalRejected`.
* Approval timeout → :class:`ApprovalRejected(status='expired')`.
* External cancel (mid-approval) → :class:`ApprovalRejected(status='cancelled')`.
* DBOS-not-launched fallback → :class:`ApprovalRequired`
  (backwards-compat contract documented in Task #42).
* ``approval_id`` and ``requested_at`` generated BEFORE the call
  (DBOS-determinism); ``request_payload`` carries the brief summary
  (repo_path, model, mode, prompt_hash) but NOT the raw prompt.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sqlite3
import subprocess
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools._approval import (
    REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX,
    REQUEST_TYPE_DELEGATE_OUT_OF_ROOT,
    ApprovalRejected,
    ApprovalRequired,
)
from capo.tools.claude_code import (
    ClaudeCodeBrief,
    DelegationHandle,
    delegate_to_claude_code,
)
from capo.tools.codex import (
    CodexBrief,
    CodexDelegationHandle,
    delegate_to_codex,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.approval import (
    APPROVAL_TOPIC,
    STATUS_CANCELLED,
    STATUS_DENIED,
    STATUS_EXPIRED,
    force_resolve_approval,
)
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    register_amc_sender,
)

# ---------------------------------------------------------------------------
# Settings scaffolding — mirrors tests/test_delegate_to_claude_code.py
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
binary = "{codex_binary}"
default_sandbox = "workspace-write"

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


# Schema for delegations + approvals tables — same shape as the other
# delegate tests, plus migration-002+003 approvals layout.
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

_DELEGATION_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stream        TEXT NOT NULL CHECK (stream IN ('stdout','stderr','event')),
    chunk         TEXT NOT NULL
)
"""

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


# ---------------------------------------------------------------------------
# Fake codex --version binary (returns 0.130.0 so the pre-flight passes).
# ---------------------------------------------------------------------------

_FAKE_CODEX_VERSION_BIN = """\
#!/bin/sh
echo "codex-cli 0.130.0"
exit 0
"""


@pytest.fixture
def codex_version_bin(tmp_path: Path) -> Path:
    p = tmp_path / "codex_fake_version"
    p.write_text(_FAKE_CODEX_VERSION_BIN)
    p.chmod(0o755)
    return p


# ---------------------------------------------------------------------------
# AMC stub — same shape as test_shell_exec_approval.AmcStub
# ---------------------------------------------------------------------------


class _StubSendResult:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class AmcStub:
    def __init__(self, *, message_id: str = "msg-1") -> None:
        self.calls: list[dict[str, str]] = []
        self.delivered_keys: set[str] = set()
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
        self.delivered_keys.add(idempotency_key)
        return _StubSendResult(self._message_id)


# ---------------------------------------------------------------------------
# Fake subprocess for spawn-path tests (so the approved-path runs the
# whole spawn → register flow without needing a real `claude` binary).
# ---------------------------------------------------------------------------


class _FakeStream:
    """Yields one CC-shaped JSON event with a session_id, then EOF."""

    def __init__(self, events: list[str]) -> None:
        self._events = list(events)

    async def readline(self) -> bytes:
        if not self._events:
            return b""
        return self._events.pop(0).encode("utf-8") + b"\n"

    async def read(self, n: int) -> bytes:
        return b""


class _FakeProcess:
    def __init__(
        self,
        *,
        pid: int = 24680,
        stdout_events: list[str] | None = None,
    ) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self.stdout = _FakeStream(stdout_events or [])
        self.stderr = _FakeStream([])
        self._exit = asyncio.Event()
        self.terminate_called = False
        self.kill_called = False

    def terminate(self) -> None:
        self.terminate_called = True
        if self.returncode is None:
            self.returncode = -15
        self._exit.set()

    def kill(self) -> None:
        self.kill_called = True
        self.returncode = -9
        self._exit.set()

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = 0
        self._exit.set()
        return self.returncode


def _cc_session_event(session_id: str = "sess-0001") -> str:
    return json.dumps(
        {
            "type": "system",
            "subtype": "init",
            "session_id": session_id,
            "model": "claude-sonnet-4-6",
        }
    )


def _codex_thread_event(thread_id: str = "thr-0001") -> str:
    return json.dumps(
        {
            "type": "thread.started",
            "thread_id": thread_id,
        }
    )


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state() -> Iterator[None]:
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    register_amc_sender(None)
    import capo.workflows.approval as approval_mod

    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    yield
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    register_amc_sender(None)
    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def scaffold(
    tmp_path: Path, codex_version_bin: Path
) -> tuple[Settings, Path, Path]:
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()

    db_path = tmp_path / "state.db"
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(db_path),
            dbos_db_path=str(tmp_path / "dbos.db"),
            codex_binary=str(codex_version_bin),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))

    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
        conn.execute(_APPROVALS_DDL)
    finally:
        conn.close()

    init_dbos(settings)
    launch_dbos()

    return settings, projects_root, workspaces_root


@pytest.fixture
def scaffold_no_dbos(
    tmp_path: Path, codex_version_bin: Path
) -> tuple[Settings, Path, Path]:
    """Same as ``scaffold`` but with DBOS NOT launched.

    Used to exercise the ``ApprovalRequired`` fallback path (the same
    contract Task #42 documented for shell_exec).
    """
    projects_root = tmp_path / "projects_nb"
    workspaces_root = tmp_path / "workspaces_nb"
    projects_root.mkdir()
    workspaces_root.mkdir()

    db_path = tmp_path / "state_nb.db"
    cfg = tmp_path / "config_nb.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(db_path),
            dbos_db_path=str(tmp_path / "dbos_nb.db"),
            codex_binary=str(codex_version_bin),
        )
    )
    (tmp_path / "souls_nb").mkdir(exist_ok=True)
    (tmp_path / "souls" / "default.md").parent.mkdir(exist_ok=True)
    if not (tmp_path / "souls" / "default.md").exists():
        (tmp_path / "souls" / "default.md").write_text("# default soul\n")
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))

    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
        conn.execute(_APPROVALS_DDL)
    finally:
        conn.close()

    return settings, projects_root, workspaces_root


@pytest.fixture
def git_repo_in_projects(scaffold: tuple[Settings, Path, Path]) -> Path:
    _, projects_root, _ = scaffold
    repo = projects_root / "repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)
    return repo


@pytest.fixture
def git_repo_outside(tmp_path: Path) -> Path:
    """Real git repo OUTSIDE projects_root — used by the out-of-root tests."""
    repo = tmp_path / "elsewhere" / "repo"
    repo.mkdir(parents=True)
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)
    return repo


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _make_deps(
    settings: Settings,
    http_client: httpx.AsyncClient,
    *,
    user_id: str = "user-42",
    thread_id: str | None = "amc:ch-A",
) -> CapoDeps:
    return CapoDeps.from_settings(
        settings=settings,
        http_client=http_client,
        user_id=user_id,
        thread_id=thread_id,
    )


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _delegations_rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM delegations")]
    finally:
        conn.close()


def _approval_row(db_path: Path, status: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE status=? "
            "ORDER BY requested_at DESC LIMIT 1",
            (status,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _pending_approval(db_path: Path) -> dict[str, Any] | None:
    return _approval_row(db_path, "pending")


# ---------------------------------------------------------------------------
# Helpers — drive approval decisions through DBOS.
# ---------------------------------------------------------------------------


async def _drive_decision(
    db_path: Path,
    amc_stub: AmcStub,
    *,
    decision: dict[str, str],
    timeout: float = 10.0,
) -> str:
    """Poll for a pending approval row and send the decision via DBOS.

    Returns the approval_id that was resolved.
    """
    from dbos import DBOS

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.1)
        if not amc_stub.calls:
            continue
        row = _pending_approval(db_path)
        if row is None or row["workflow_id"] is None:
            continue
        await DBOS.send_async(row["workflow_id"], decision, APPROVAL_TOPIC)
        return row["approval_id"]
    raise AssertionError("approval driver did not observe a pending row")


# ===========================================================================
# 1. delegate_to_claude_code — out-of-root → approve → proceeds normally.
# ===========================================================================


@pytest.mark.asyncio
async def test_cc_out_of_root_approved_proceeds_with_delegation(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """Out-of-root CC delegation → /approve → spawn proceeds → DelegationHandle."""
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub(message_id="msg-cc-approve")
    register_amc_sender(stub.send)

    brief = ClaudeCodeBrief(
        goal="add docstring",
        repo_path=str(git_repo_outside),
        create_worktree=True,
    )

    spawn_captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        spawn_captured["argv"] = list(args)
        spawn_captured["cwd"] = kwargs.get("cwd")
        return _FakeProcess(stdout_events=[_cc_session_event("sess-cc-1")])

    approver_task = asyncio.create_task(
        _drive_decision(
            settings.paths.db_path,
            stub,
            decision={
                "status": "approve",
                "resolved_by": "user-42",
                "reason": None,
            },
        )
    )
    try:
        with patch(
            "capo.tools.claude_code.asyncio.create_subprocess_exec",
            side_effect=fake_spawn,
        ):
            handle = await delegate_to_claude_code(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await approver_task

    # Approval was requested and approved.
    assert len(stub.calls) == 1
    approved = _approval_row(settings.paths.db_path, "approved")
    assert approved is not None
    assert approved["request_type"] == REQUEST_TYPE_DELEGATE_OUT_OF_ROOT
    payload = json.loads(approved["request_payload"])
    assert payload["agent"] == "claude-code"
    assert payload["repo_path"] == str(git_repo_outside)
    assert payload["mode"] == "acceptEdits"
    assert "prompt_hash" in payload and len(payload["prompt_hash"]) == 16
    assert "prompt" not in payload, "raw prompt must not be in payload"

    # Delegation proceeded: handle returned, row persisted.
    assert isinstance(handle, DelegationHandle)
    assert handle.status == "running"
    rows = _delegations_rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["agent"] == "claude-code"

    # Spawn was actually invoked.
    assert "argv" in spawn_captured

    # approval_id and requested_at deterministic-input contract.
    assert isinstance(approved["approval_id"], str)
    assert len(approved["approval_id"]) == 32
    uuid.UUID(hex=approved["approval_id"])
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", approved["requested_at"]
    )


# ===========================================================================
# 2. delegate_to_codex — out-of-root → approve → proceeds normally.
# ===========================================================================


@pytest.mark.asyncio
async def test_codex_out_of_root_approved_proceeds_with_delegation(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """Out-of-root Codex delegation → /approve → spawn proceeds → handle."""
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub(message_id="msg-codex-approve")
    register_amc_sender(stub.send)

    brief = CodexBrief(
        goal="add docstring",
        repo_path=str(git_repo_outside),
        create_worktree=True,
    )

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess(stdout_events=[_codex_thread_event("thr-codex-1")])

    approver_task = asyncio.create_task(
        _drive_decision(
            settings.paths.db_path,
            stub,
            decision={
                "status": "approve",
                "resolved_by": "user-42",
                "reason": None,
            },
        )
    )
    try:
        with patch(
            "capo.tools.codex.asyncio.create_subprocess_exec",
            side_effect=fake_spawn,
        ):
            handle = await delegate_to_codex(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await approver_task

    assert len(stub.calls) == 1
    approved = _approval_row(settings.paths.db_path, "approved")
    assert approved is not None
    assert approved["request_type"] == REQUEST_TYPE_DELEGATE_OUT_OF_ROOT
    payload = json.loads(approved["request_payload"])
    assert payload["agent"] == "codex"
    assert payload["sandbox"] == "workspace-write"

    assert isinstance(handle, CodexDelegationHandle)
    assert handle.status == "running"
    rows = _delegations_rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["agent"] == "codex"


# ===========================================================================
# 3. delegate_to_codex — danger-full-access → approval required.
# ===========================================================================


@pytest.mark.asyncio
async def test_codex_dangerous_sandbox_requires_approval(
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """sandbox=danger-full-access → delegate_dangerous_sandbox request_type."""
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub(message_id="msg-codex-danger")
    register_amc_sender(stub.send)

    brief = CodexBrief(
        goal="full access task",
        repo_path=str(git_repo_in_projects),
        sandbox="danger-full-access",
        create_worktree=True,
    )

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess(
            stdout_events=[_codex_thread_event("thr-codex-danger")]
        )

    approver_task = asyncio.create_task(
        _drive_decision(
            settings.paths.db_path,
            stub,
            decision={
                "status": "approve",
                "resolved_by": "user-42",
                "reason": "operator OK",
            },
        )
    )
    try:
        with patch(
            "capo.tools.codex.asyncio.create_subprocess_exec",
            side_effect=fake_spawn,
        ):
            handle = await delegate_to_codex(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await approver_task

    # Exactly one approval row, with the dangerous-sandbox request_type.
    approved = _approval_row(settings.paths.db_path, "approved")
    assert approved is not None
    assert (
        approved["request_type"] == REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX
    )
    payload = json.loads(approved["request_payload"])
    assert payload["sandbox"] == "danger-full-access"
    assert payload["agent"] == "codex"
    assert "prompt_hash" in payload

    assert isinstance(handle, CodexDelegationHandle)
    rows = _delegations_rows(settings.paths.db_path)
    assert len(rows) == 1


# ===========================================================================
# 4. Deny path → ApprovalRejected for CC.
# ===========================================================================


@pytest.mark.asyncio
async def test_cc_out_of_root_denied_raises_rejected(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """Out-of-root CC delegation → /deny → ApprovalRejected(denied)."""
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub()
    register_amc_sender(stub.send)

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_outside), create_worktree=False
    )

    denier_task = asyncio.create_task(
        _drive_decision(
            settings.paths.db_path,
            stub,
            decision={
                "status": "deny",
                "resolved_by": "user-42",
                "reason": "not safe",
            },
        )
    )
    try:
        with pytest.raises(ApprovalRejected) as excinfo:
            await delegate_to_claude_code(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await denier_task

    assert excinfo.value.status == STATUS_DENIED
    assert excinfo.value.resolved_by == "user-42"
    assert excinfo.value.reason == "not safe"
    # No delegation row written.
    assert _delegations_rows(settings.paths.db_path) == []


# ===========================================================================
# 4b. Deny path → ApprovalRejected for Codex.
# ===========================================================================


@pytest.mark.asyncio
async def test_codex_out_of_root_denied_raises_rejected(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """Out-of-root Codex delegation → /deny → ApprovalRejected(denied)."""
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub()
    register_amc_sender(stub.send)

    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_outside), create_worktree=False
    )

    denier_task = asyncio.create_task(
        _drive_decision(
            settings.paths.db_path,
            stub,
            decision={
                "status": "deny",
                "resolved_by": "user-42",
                "reason": "no",
            },
        )
    )
    try:
        with pytest.raises(ApprovalRejected) as excinfo:
            await delegate_to_codex(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await denier_task

    assert excinfo.value.status == STATUS_DENIED
    assert _delegations_rows(settings.paths.db_path) == []


# ===========================================================================
# 5. Approval timeout → ApprovalRejected(status='expired').
# ===========================================================================


@pytest.mark.asyncio
async def test_cc_out_of_root_timeout_raises_rejected_expired(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """recv_async timeout elapses → ApprovalRejected(status='expired')."""
    settings, _, _ = scaffold
    # Short timeout — just above the SQLite polling floor (~1s).
    object.__setattr__(settings.approval, "timeout_seconds", 2)
    deps = _make_deps(settings, http_client)

    stub = AmcStub()
    register_amc_sender(stub.send)

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_outside), create_worktree=False
    )

    with pytest.raises(ApprovalRejected) as excinfo:
        await delegate_to_claude_code(_ctx(deps), brief)

    assert excinfo.value.status == STATUS_EXPIRED
    assert excinfo.value.resolved_by is None
    assert excinfo.value.reason is not None
    assert "timeout" in excinfo.value.reason
    assert _delegations_rows(settings.paths.db_path) == []


# ===========================================================================
# 6. External force-cancel mid-approval → ApprovalRejected(status='cancelled').
# ===========================================================================


@pytest.mark.asyncio
async def test_cc_out_of_root_cancelled_raises_rejected(
    scaffold: tuple[Settings, Path, Path],
    git_repo_outside: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """Mid-approval external force_resolve_approval → ApprovalRejected(cancelled).

    Simulates a "another delegation killed mid-approval → killer cancels"
    race per the §5.8 edge case in Task #43's brief.
    """
    settings, _, _ = scaffold
    deps = _make_deps(settings, http_client)

    stub = AmcStub()
    register_amc_sender(stub.send)

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_outside), create_worktree=False
    )

    async def _canceller() -> None:
        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not stub.calls:
                continue
            row = _pending_approval(settings.paths.db_path)
            if row is None or row["workflow_id"] is None:
                continue
            await force_resolve_approval(
                row["workflow_id"],
                status="cancelled",
                resolved_by="system:killer",
                reason="parent delegation killed",
            )
            return
        raise AssertionError("canceller did not observe a pending row")

    canceller_task = asyncio.create_task(_canceller())
    try:
        with pytest.raises(ApprovalRejected) as excinfo:
            await delegate_to_claude_code(_ctx(deps), brief)
    finally:
        with contextlib.suppress(Exception):
            await canceller_task

    assert excinfo.value.status == STATUS_CANCELLED
    assert excinfo.value.resolved_by == "system:killer"
    assert _delegations_rows(settings.paths.db_path) == []


# ===========================================================================
# 7. DBOS-not-launched → falls back to ApprovalRequired (legacy contract).
# ===========================================================================


@pytest.mark.asyncio
async def test_cc_out_of_root_dbos_not_launched_falls_back(
    scaffold_no_dbos: tuple[Settings, Path, Path],
    tmp_path: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """DBOS not launched → ApprovalRequired (same contract as Task #42)."""
    settings, _, _ = scaffold_no_dbos
    assert not is_launched()
    deps = _make_deps(settings, http_client)

    # Build a real git repo OUTSIDE projects_root for this scaffold.
    outside = tmp_path / "elsewhere_nodbos" / "repo"
    outside.mkdir(parents=True)
    _git("init", "-q", "-b", "main", cwd=outside)
    _git("config", "user.email", "test@capo.local", cwd=outside)
    _git("config", "user.name", "Capo Test", cwd=outside)
    (outside / "README.md").write_text("hi\n")
    _git("add", "README.md", cwd=outside)
    _git("commit", "-q", "-m", "i", cwd=outside)

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(outside), create_worktree=False
    )

    with pytest.raises(ApprovalRequired) as excinfo:
        await delegate_to_claude_code(_ctx(deps), brief)
    assert "projects_root" in excinfo.value.reason
    assert "approval workflow is unavailable" in excinfo.value.reason
    assert _delegations_rows(settings.paths.db_path) == []
    assert list(settings.paths.workspaces_root.iterdir()) == []


@pytest.mark.asyncio
async def test_codex_dangerous_sandbox_dbos_not_launched_falls_back(
    scaffold_no_dbos: tuple[Settings, Path, Path],
    http_client: httpx.AsyncClient,
) -> None:
    """Codex dangerous-sandbox + DBOS not launched → ApprovalRequired."""
    settings, projects_root, _ = scaffold_no_dbos
    assert not is_launched()
    deps = _make_deps(settings, http_client)

    # In-projects path so out-of-root gate doesn't apply; only the
    # dangerous-sandbox gate triggers.
    repo = projects_root / "repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hi\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "i", cwd=repo)

    brief = CodexBrief(
        goal="g",
        repo_path=str(repo),
        sandbox="danger-full-access",
        create_worktree=False,
    )

    with pytest.raises(ApprovalRequired) as excinfo:
        await delegate_to_codex(_ctx(deps), brief)
    assert "approval workflow is unavailable" in excinfo.value.reason
    assert _delegations_rows(settings.paths.db_path) == []

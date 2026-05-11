"""Tests for Task #35 — DBOS workflow handoff in :func:`delegate_to_claude_code`.

Covers spec §5.6 / §5.13 / §9.3 acceptance criteria specific to the
Phase-2 → Phase-3 monitor handoff:

Functional:
  * ``delegate_to_claude_code`` no longer spawns an in-process monitor —
    it registers the live process via ``register_delegation_subprocess``
    and invokes ``monitor_delegation`` as a DBOS workflow.
  * End-to-end: spawn → DBOS workflow runs → terminal row persisted →
    ``notify_user`` is invoked through the registered AMC sender.
  * ``capo.main._cold_boot_resume_sweep`` SELECTs rows in
    ``status='running'`` and re-invokes ``monitor_delegation`` for each.

Edge Cases:
  * Cold-boot sweep with no running delegations: no-op, no errors.
  * Cold-boot sweep with a running delegation whose subprocess is gone:
    monitor_delegation's resume contract (Task #31) handles it.

Error Handling:
  * DBOS not launched when ``delegate_to_claude_code`` runs: clear typed
    error :class:`DBOSNotLaunchedError`, row marked ``failed`` with
    summary ``"DBOS not launched"``, no orphan workspace.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import subprocess
import sys
from collections.abc import AsyncIterator
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
from capo.tools.claude_code import (
    ClaudeCodeBrief,
    DBOSNotLaunchedError,
    delegate_to_claude_code,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    register_amc_sender,
)

# ---------------------------------------------------------------------------
# Scaffolding — mirrors tests/test_delegate_to_claude_code.py.
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

_DELEGATION_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stream        TEXT NOT NULL CHECK (stream IN ('stdout','stderr','event')),
    chunk         TEXT NOT NULL
)
"""


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


def _make_settings_no_dbos(tmp_path: Path) -> Settings:
    """Build settings + schema but DO NOT launch DBOS.

    Used by the "DBOS not launched" error-handling test where we want to
    invoke ``delegate_to_claude_code`` before DBOS is up.
    """
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
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
    finally:
        conn.close()
    return settings


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state():
    """DBOS is a process-level singleton; clean teardown between tests."""
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    with contextlib.suppress(Exception):
        register_amc_sender(None)
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    """Settings + state.db schema + DBOS launched."""
    settings = _make_settings_no_dbos(tmp_path)
    init_dbos(settings)
    launch_dbos()
    return settings, tmp_path / "projects", tmp_path / "workspaces"


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
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _row(db_path: Path, delegation_id: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
    finally:
        conn.close()
    return dict(r) if r else None


def _rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM delegations")]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fake CC script — emits one system event + one terminal result event.
# ---------------------------------------------------------------------------

_FAKE_CLAUDE_SCRIPT = """\
#!/usr/bin/env python3
import json, sys, time
event = {
    "type": "system",
    "subtype": "init",
    "session_id": "task35-session-00000000-0000-0000-0000-000000000001",
    "claude_code_version": "0.0.0",
}
print(json.dumps(event), flush=True)
time.sleep(0.05)
final = {
    "type": "result",
    "session_id": "task35-session-00000000-0000-0000-0000-000000000001",
    "is_error": False,
    "subtype": "success",
}
print(json.dumps(final), flush=True)
sys.exit(0)
"""


# ===========================================================================
# Functional: spawn → DBOS workflow → terminal status → notify_user
# ===========================================================================


@pytest.mark.asyncio
async def test_handoff_invokes_dbos_workflow_and_notifies_user(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """End-to-end: delegate → DBOS monitor_delegation → notify_user invoked.

    This is the Task #35 functional acceptance criterion. We register a
    fake AMC sender to capture the notify_user invocation, drive the
    spawn site with a fake CC script that exits cleanly, wait for the
    row to reach terminal status, and assert that the registered
    sender was called exactly once with the expected (channel_id, key)
    pair.
    """
    settings, _, _ = scaffold
    fake_claude = tmp_path / "fake_claude"
    fake_claude.write_text(_FAKE_CLAUDE_SCRIPT)
    fake_claude.chmod(0o755)

    sender_calls: list[dict[str, str]] = []

    class _StubSendResult:
        message_id = "msg-task35-1"

    async def _stub_sender(
        channel_id: str, text: str, idempotency_key: str
    ) -> _StubSendResult:
        sender_calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return _StubSendResult()

    register_amc_sender(_stub_sender)

    deps = CapoDeps.from_settings(
        settings=settings,
        http_client=http_client,
        user_id="owner",
        thread_id="amc:task35-channel",
    )
    object.__setattr__(deps.settings.agents.claude_code, "binary", sys.executable)

    brief = ClaudeCodeBrief(
        goal="emit one event",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="anthropic:claude-sonnet-4-6",
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake_claude),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    assert handle.status == "running"

    # Wait up to ~10s for terminal status + notify_user call.
    deadline = asyncio.get_event_loop().time() + 10.0
    while asyncio.get_event_loop().time() < deadline:
        row = _row(settings.paths.db_path, handle.delegation_id)
        if row and row["status"] != "running" and sender_calls:
            break
        await asyncio.sleep(0.05)

    row = _row(settings.paths.db_path, handle.delegation_id)
    assert row is not None
    assert row["status"] == "completed", f"row never reached terminal: {row}"
    assert row["ended_at"] is not None

    # AMC sender was invoked once for the terminal completion message.
    assert len(sender_calls) == 1, (
        f"expected exactly one notify_user send; got {len(sender_calls)}"
    )
    call = sender_calls[0]
    assert call["channel_id"] == "task35-channel"
    assert "COMPLETED" in call["text"]
    assert call["idempotency_key"].startswith("notify_user:")


# ===========================================================================
# Error handling: DBOS not launched → typed error + failed row
# ===========================================================================


@pytest.mark.asyncio
async def test_delegate_without_dbos_launched_marks_row_failed_and_raises(
    tmp_path: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """DBOS not launched when spawn site runs → typed error + failed row.

    Task #35 error-handling criterion: the spawn site checks
    ``is_launched()`` before invoking the workflow. If DBOS is down,
    cleanup must be the same as Phase 2 (no orphan workspace, no
    orphan process, row UPDATEd to failed with a clear reason).
    """
    # Build settings WITHOUT launching DBOS.
    settings = _make_settings_no_dbos(tmp_path)
    repo = tmp_path / "projects" / "repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)

    deps = CapoDeps.from_settings(
        settings=settings,
        http_client=http_client,
        user_id="owner",
        thread_id="amc:dbos-not-launched",
    )

    class _FakeStream:
        async def readline(self) -> bytes:
            return b""

        async def read(self, n: int) -> bytes:
            return b""

    class _FakeProcess:
        def __init__(self) -> None:
            self.pid = 9999
            self.returncode = None
            self.stdout = _FakeStream()
            self.stderr = _FakeStream()
            self.terminate_called = False
            self.kill_called = False

        def terminate(self) -> None:
            self.terminate_called = True
            self.returncode = -15

        def kill(self) -> None:
            self.kill_called = True
            self.returncode = -9

        async def wait(self) -> int:
            if self.returncode is None:
                self.returncode = -15
            return self.returncode

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess()

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(repo), create_worktree=True
    )

    assert not is_launched()
    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ), pytest.raises(DBOSNotLaunchedError):
        await delegate_to_claude_code(_ctx(deps), brief)

    # Row exists and is marked failed with the canonical reason.
    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["summary"] == "DBOS not launched"


# ===========================================================================
# Cold-boot sweep
# ===========================================================================


@pytest.mark.asyncio
async def test_cold_boot_sweep_no_running_delegations_is_noop(
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """Cold-boot sweep over an empty delegations table: no errors, no work."""
    settings, _, _ = scaffold
    from capo.main import _cold_boot_resume_sweep

    # No rows inserted → no monitor_delegation invocations. Just assert
    # the sweep returns cleanly.
    await _cold_boot_resume_sweep(settings)


@pytest.mark.asyncio
async def test_cold_boot_sweep_re_invokes_monitor_delegation_for_running_rows(
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """Cold-boot sweep finds one ``status='running'`` row and re-invokes
    monitor_delegation for it.

    We patch ``monitor_delegation`` in :mod:`capo.workflows.delegation` so
    we can observe the call without actually running the workflow (the
    real workflow's resume contract is exercised in
    test_workflows_delegation_resume.py).
    """
    settings, _, _ = scaffold
    db_path = settings.paths.db_path

    delegation_id = "running-row-task35"
    conn = open_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delegation_id,
                "owner",
                "claude-code",
                "/tmp/ws/running",
                12345,
                "old-session-id",
                "{}",
                "running",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    observed_calls: list[dict[str, Any]] = []

    async def _stub_monitor_delegation(
        did: str,
        *,
        db_path: Path | None = None,
        claude_binary: str = "claude",
        **_kwargs: Any,
    ) -> None:
        observed_calls.append(
            {
                "delegation_id": did,
                "db_path": db_path,
                "claude_binary": claude_binary,
            }
        )

    with patch(
        "capo.workflows.delegation.monitor_delegation",
        side_effect=_stub_monitor_delegation,
    ):
        from capo.main import _cold_boot_resume_sweep

        await _cold_boot_resume_sweep(settings)
        # The sweep schedules fire-and-forget tasks; give them a chance
        # to run before asserting.
        for _ in range(20):
            if observed_calls:
                break
            await asyncio.sleep(0.05)

    assert len(observed_calls) == 1
    call = observed_calls[0]
    assert call["delegation_id"] == delegation_id
    assert call["db_path"] == db_path
    assert call["claude_binary"] == "claude"

"""Tests for :func:`capo.tools.claude_code.delegate_to_claude_code` (Task #22).

Covers spec §5.3 / §9.2 acceptance criteria:

Functional
* CC argv shape matches §7.5 / spike S-3 (``-p``, ``--output-format stream-json
  --verbose``, ``--permission-mode``, optional ``--model``).
* **Persistence-before-yield**: the ``delegations`` INSERT happens BEFORE
  the function returns the :class:`DelegationHandle`. Verified by mocking
  the spawn step to capture call ordering.
* Returns a :class:`DelegationHandle` with ``status='running'`` and the
  generated ``delegation_id`` after the row is durable.

Edge / error
* ``repo_path`` outside ``projects_root`` → :class:`ApprovalRequired`
  (no row, no workspace).
* Missing ``claude`` binary → :class:`FileNotFoundError` with a
  ``status='failed'`` row left behind (reason cites the missing binary).
* Worktree creation failure leaves DB consistent: one ``failed`` row, no
  orphan workspace dir, no orphan process.
* Two consecutive calls produce distinct ``delegation_id`` values.

Integration
* End-to-end against a fake "claude" subprocess (a tiny python script that
  emits one CC-shaped JSON event then exits): row exists with the right
  pid/model/parent_thread_id, ``session_id_subagent=NULL`` (Task #23 sets
  it), and the handle is returned.
"""

from __future__ import annotations

import asyncio
import json
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
from capo.tools.basic import ApprovalRequired
from capo.tools.claude_code import (
    ClaudeCodeBrief,
    DelegationHandle,
    delegate_to_claude_code,
)

# ---------------------------------------------------------------------------
# Scaffolding — config + on-disk roots + state.db with §7.3 schema.
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


# Same §7.3 DDL as production (sans the FK to users so tests can stay
# self-contained without seeding parents). The shape matches migrations/001.
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


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    """Build a Settings rooted at ``tmp_path`` + the on-disk roots.

    Initializes ``state.db`` with the §7.3 ``delegations`` and
    ``delegation_output`` tables so tests can INSERT/SELECT without
    bringing up Alembic.
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

    # Initialize schema.
    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
    finally:
        conn.close()

    return settings, projects_root, workspaces_root


@pytest.fixture
def git_repo_in_projects(scaffold: tuple[Settings, Path, Path]) -> Path:
    """Create a real git repo *inside* projects_root with a ``main`` branch."""
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


@pytest.fixture
def make_deps(
    scaffold: tuple[Settings, Path, Path],
    http_client: httpx.AsyncClient,
):
    """Factory: ``make_deps(thread_id=..., user_id=...) -> CapoDeps``."""
    settings, _, _ = scaffold

    def _make(
        *, thread_id: str | None = "amc:test-channel", user_id: str = "owner"
    ) -> CapoDeps:
        return CapoDeps.from_settings(
            settings=settings,
            http_client=http_client,
            user_id=user_id,
            thread_id=thread_id,
        )

    return _make


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    """Minimal :class:`RunContext` for direct delegate_to_claude_code calls."""
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM delegations")]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fake subprocess + spawn for argv / ordering tests.
# ---------------------------------------------------------------------------


class _FakeStream:
    """Minimal async stream that immediately reports EOF.

    The reader treats EOF==``b""`` as "this pipe is closed" — perfect for
    tests that don't care about content.
    """

    async def readline(self) -> bytes:
        return b""

    async def read(self, n: int) -> bytes:
        return b""


class _FakeProcess:
    """asyncio.subprocess.Process duck — controllable returncode + wait()."""

    def __init__(self, *, pid: int = 12345, returncode: int = 0) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._target_rc = returncode
        self.stdout = _FakeStream()
        self.stderr = _FakeStream()
        self._exit_event = asyncio.Event()
        self.terminate_called = False
        self.kill_called = False

    def terminate(self) -> None:
        self.terminate_called = True
        if self.returncode is None:
            self.returncode = -15
        self._exit_event.set()

    def kill(self) -> None:
        self.kill_called = True
        self.returncode = -9
        self._exit_event.set()

    async def wait(self) -> int:
        # Simulate "exit immediately" so tests don't hang on monitor.
        self.returncode = self._target_rc
        self._exit_event.set()
        return self.returncode


# ---------------------------------------------------------------------------
# Unit: argv shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_argv_matches_s3_spec(
    git_repo_in_projects: Path, make_deps: Any
) -> None:
    """The CC spawn argv matches §7.5 / spike S-3.

    Default config: ``claude -p <prompt> --output-format stream-json
    --verbose --permission-mode acceptEdits --model <subagents.claude_code>``.
    """
    deps = make_deps()
    captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["argv"] = list(args)
        captured["cwd"] = kwargs.get("cwd")
        captured["limit"] = kwargs.get("limit")
        return _FakeProcess()

    brief = ClaudeCodeBrief(
        goal="Add a docstring",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    assert isinstance(handle, DelegationHandle)
    argv = captured["argv"]
    # First token is the binary; rest is the spec invocation.
    assert argv[0] == "claude"
    assert "-p" in argv
    p_idx = argv.index("-p")
    assert isinstance(argv[p_idx + 1], str) and argv[p_idx + 1]
    # §7.5: --output-format stream-json --verbose
    assert "--output-format" in argv
    of_idx = argv.index("--output-format")
    assert argv[of_idx + 1] == "stream-json"
    assert "--verbose" in argv
    # permission-mode pulled from agents.claude_code.default_permission_mode
    pm_idx = argv.index("--permission-mode")
    assert argv[pm_idx + 1] == "acceptEdits"
    # --model is the configured subagents.claude_code value when brief omits it
    m_idx = argv.index("--model")
    assert argv[m_idx + 1] == "anthropic:claude-sonnet-4-6"
    # spawn options: 2 MiB limit per Wave 1 guidance.
    assert captured["limit"] == 2 * 1024 * 1024
    # cwd is the worktree directory.
    assert handle.workspace == captured["cwd"]


@pytest.mark.asyncio
async def test_argv_uses_brief_model_override(
    git_repo_in_projects: Path, make_deps: Any
) -> None:
    """``brief.model`` overrides the config default in the ``--model`` slot."""
    deps = make_deps()
    captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["argv"] = list(args)
        return _FakeProcess()

    brief = ClaudeCodeBrief(
        goal="g",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="anthropic:claude-opus-4-7",
    )
    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        await delegate_to_claude_code(_ctx(deps), brief)

    argv = captured["argv"]
    m_idx = argv.index("--model")
    assert argv[m_idx + 1] == "anthropic:claude-opus-4-7"


# ---------------------------------------------------------------------------
# Unit: persistence-before-yield ordering.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persistence_happens_before_handle_returns(
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """The INSERT must complete BEFORE delegate_to_claude_code returns.

    We verify by patching :func:`open_connection` so the persist path records
    a "before-return" marker, and we record the "returned" marker on the
    awaiter side. Order of markers in the list reveals whether persistence
    truly happens before the handle is yielded.
    """
    settings, _, _ = scaffold
    db_path = settings.paths.db_path
    deps = make_deps()
    timeline: list[str] = []

    real_open = open_connection

    class _TrackingConn:
        """Proxy around sqlite3.Connection that tags INSERT INTO delegations."""

        def __init__(self, inner: sqlite3.Connection) -> None:
            self._inner = inner

        def execute(self, sql: str, *params: Any) -> Any:
            if isinstance(sql, str) and sql.strip().upper().startswith(
                "INSERT INTO DELEGATIONS"
            ):
                timeline.append("persisted")
            return self._inner.execute(sql, *params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    def tracking_open(path: Any, **kwargs: Any) -> Any:
        return _TrackingConn(real_open(path, **kwargs))

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess()

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with patch(
        "capo.tools.claude_code.open_connection", side_effect=tracking_open
    ), patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)
        timeline.append("returned")

    assert timeline[0] == "persisted", (
        f"persistence-before-yield violated: timeline={timeline}"
    )
    assert "returned" in timeline
    # And the row truly is durable.
    rows = _rows(db_path)
    assert len(rows) == 1
    assert rows[0]["id"] == handle.delegation_id
    assert rows[0]["status"] == "running"
    assert rows[0]["session_id_subagent"] is None
    assert rows[0]["pid"] == 12345
    assert rows[0]["model"] == "anthropic:claude-sonnet-4-6"
    assert rows[0]["parent_thread_id"] == "amc:test-channel"


# ---------------------------------------------------------------------------
# Unit: repo_path outside projects_root raises ApprovalRequired.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repo_outside_projects_root_raises_approval(
    tmp_path: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """A repo path outside ``projects_root`` triggers :class:`ApprovalRequired`."""
    deps = make_deps()
    # Different tmp directory; not under projects_root.
    outside = tmp_path / "elsewhere" / "repo"
    outside.mkdir(parents=True)
    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(outside), create_worktree=False
    )

    with pytest.raises(ApprovalRequired) as excinfo:
        await delegate_to_claude_code(_ctx(deps), brief)

    assert "projects_root" in excinfo.value.reason
    # And no row was written.
    rows = _rows(scaffold[0].paths.db_path)
    assert rows == []
    # And no workspace dir was created.
    assert list((scaffold[0].paths.workspaces_root).iterdir()) == []


# ---------------------------------------------------------------------------
# Unit: missing `claude` binary leaves a failed row.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_claude_binary_leaves_failed_row(
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """If ``claude`` is not on PATH, spawn raises FileNotFoundError and a
    ``status='failed'`` row is left behind with the reason."""
    deps = make_deps()
    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    async def boom(*args: Any, **kwargs: Any) -> Any:
        raise FileNotFoundError(
            2, "No such file or directory", "claude"
        )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=boom,
    ), pytest.raises(FileNotFoundError):
        await delegate_to_claude_code(_ctx(deps), brief)

    rows = _rows(scaffold[0].paths.db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["summary"] is not None
    assert "claude binary not found on PATH" in rows[0]["summary"]
    # Workspace dir was cleaned up.
    assert list((scaffold[0].paths.workspaces_root).iterdir()) == []


# ---------------------------------------------------------------------------
# Unit: worktree creation failure leaves DB consistent.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worktree_failure_leaves_consistent_state(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
) -> None:
    """A non-git repo with ``create_worktree=True`` raises and leaves one
    failed row, no orphan workspace, no orphan process."""
    settings, projects_root, _ = scaffold
    not_a_repo = projects_root / "not_a_repo"
    not_a_repo.mkdir()
    deps = make_deps()
    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(not_a_repo), create_worktree=True
    )

    spawn_called: list[bool] = []

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        spawn_called.append(True)
        return _FakeProcess()

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        with pytest.raises(Exception) as excinfo:
            await delegate_to_claude_code(_ctx(deps), brief)
        # Must be a WorktreeError subclass.
        from capo.tools._worktree import NotAGitRepo, WorktreeError

        assert isinstance(excinfo.value, WorktreeError)
        assert isinstance(excinfo.value, NotAGitRepo)

    # No process was spawned — we failed at the worktree step.
    assert spawn_called == []

    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert "worktree creation failed" in (rows[0]["summary"] or "")
    # No orphan workspace.
    assert list(settings.paths.workspaces_root.iterdir()) == []


# ---------------------------------------------------------------------------
# Edge: distinct delegation_ids across two calls.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_delegations_have_distinct_ids(
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """Two consecutive delegate calls produce different ``delegation_id``s."""
    deps = make_deps()

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess()

    brief = ClaudeCodeBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        h1 = await delegate_to_claude_code(_ctx(deps), brief)
        h2 = await delegate_to_claude_code(_ctx(deps), brief)

    assert h1.delegation_id != h2.delegation_id

    ids = {r["id"] for r in _rows(scaffold[0].paths.db_path)}
    assert ids == {h1.delegation_id, h2.delegation_id}


# ---------------------------------------------------------------------------
# Integration: end-to-end with a fake "claude" subprocess.
# ---------------------------------------------------------------------------


# A self-contained python script that emits one CC-shaped JSON event then
# exits cleanly. Written to a tmp path and used as the ``claude`` binary by
# patching settings.agents.claude_code.binary.
_FAKE_CLAUDE_SCRIPT = """\
#!/usr/bin/env python3
import json, sys, time
event = {
    "type": "system",
    "subtype": "init",
    "session_id": "deadbeef-0000-0000-0000-000000000001",
    "claude_code_version": "0.0.0",
}
print(json.dumps(event), flush=True)
time.sleep(0.05)
final = {
    "type": "result",
    "session_id": "deadbeef-0000-0000-0000-000000000001",
    "is_error": False,
    "subtype": "success",
}
print(json.dumps(final), flush=True)
sys.exit(0)
"""


@pytest.mark.asyncio
async def test_integration_end_to_end_with_fake_claude(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """End-to-end: a real subprocess spawn, real reader, real DB row.

    Uses a tiny python "claude" stand-in so the test never touches the real
    CC binary. Verifies:
    - the DelegationHandle is returned with status='running',
    - the row exists with pid, model, parent_thread_id set, and
      session_id_subagent NULL (Task #23 fills it in),
    - the row eventually transitions to 'completed' once the child exits.
    """
    settings, _, _ = scaffold
    fake_claude = tmp_path / "fake_claude"
    fake_claude.write_text(_FAKE_CLAUDE_SCRIPT)
    fake_claude.chmod(0o755)

    deps = make_deps(thread_id="amc:integration-channel")
    # Swap the binary by editing the agents.claude_code config in place.
    # AgentBinaryConfig is frozen=False by default (extra="allow"); we can
    # set ``binary`` via object.__setattr__ for the test only.
    object.__setattr__(deps.settings.agents.claude_code, "binary", sys.executable)

    brief = ClaudeCodeBrief(
        goal="emit one event",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="anthropic:claude-sonnet-4-6",
    )

    # Patch create_subprocess_exec to inject the script path as an extra
    # arg in front of the rendered prompt — the simplest way to drive the
    # python interpreter at our fake script without altering production argv.
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        # args = (binary, "-p", prompt, ...). Replace argv with
        # [python, fake_claude] so the fake script runs.
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
    assert handle.agent == "claude-code"
    # Row exists immediately (persistence-before-yield).
    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == handle.delegation_id
    assert row["status"] == "running"
    assert row["pid"] is not None
    assert row["model"] == "anthropic:claude-sonnet-4-6"
    assert row["parent_thread_id"] == "amc:integration-channel"
    assert row["session_id_subagent"] is None  # Task #23 fills this.
    assert row["agent"] == "claude-code"

    # Drive the monitor: wait up to 5s for the row to transition to
    # 'completed'. The fake script exits in ~50ms; the reader+monitor
    # should observe that promptly.
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        rows = _rows(settings.paths.db_path)
        if rows and rows[0]["status"] != "running":
            break
        await asyncio.sleep(0.05)

    rows = _rows(settings.paths.db_path)
    assert rows[0]["status"] == "completed", (
        f"row never transitioned to completed; row={rows[0]}"
    )
    assert rows[0]["ended_at"] is not None

    # And delegation_output captured both JSON events.
    conn = sqlite3.connect(str(settings.paths.db_path))
    try:
        out = conn.execute(
            "SELECT stream, chunk FROM delegation_output "
            "WHERE delegation_id = ? ORDER BY id",
            (handle.delegation_id,),
        ).fetchall()
    finally:
        conn.close()
    streams = [r[0] for r in out]
    assert streams.count("event") >= 2, (
        f"expected >=2 event rows; got streams={streams}"
    )
    payloads = [json.loads(r[1]) for r in out if r[0] == "event"]
    types = [p.get("type") for p in payloads]
    assert "system" in types and "result" in types

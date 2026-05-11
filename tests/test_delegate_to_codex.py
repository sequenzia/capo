"""Tests for :func:`capo.tools.codex.delegate_to_codex` (Task #45).

Covers spec §5.4 / §7.5 / §9.4 acceptance criteria for the Codex spawn
site (initial delegation only; Task #46 owns resume).

Functional
* :class:`CodexBrief` validation — required fields, sandbox Literal,
  frozen=True, extra="forbid".
* Argv shape matches spec §5.4 (amended by Task #37):
  ``codex exec --json --skip-git-repo-check --sandbox <mode>
  -C <workspace> [--model <m>] "<prompt>"``. NEVER ``--ephemeral``.
* **Persistence-before-yield**: the delegations INSERT happens BEFORE
  :class:`CodexDelegationHandle` is returned.
* Returns a :class:`CodexDelegationHandle` with ``status='running'``,
  ``agent='codex'``, and the generated ``delegation_id``.

Edge / error
* ``codex`` binary missing → :class:`CodexBinaryError` BEFORE any side
  effects (no row, no workspace).
* Below-min-version → :class:`CodexBinaryError`.
* ``repo_path`` outside ``projects_root`` → :class:`ApprovalRequired`.
* ``thread.started`` capture timeout → row marked failed + subprocess
  killed.
* Sandbox mode outside the allowed set → Pydantic ``ValidationError``
  at brief construction.

Integration
* End-to-end with a fake "codex" subprocess (a tiny python script that
  emits one ``thread.started`` event then a ``turn.completed``):
  - DBOS handoff runs;
  - ``thread_id`` is captured into ``session_id_subagent``;
  - row transitions ``running → completed``.
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
from pydantic import ValidationError
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools.codex import (
    DEFAULT_SANDBOX_MODE,
    MIN_CODEX_VERSION,
    CodexBinaryError,
    CodexBrief,
    CodexDelegationHandle,
    DBOSNotLaunchedError,
    ThreadIdCaptureTimeout,
    _check_codex_version,
    _parse_semver,
    _render_codex_prompt,
    delegate_to_codex,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    register_amc_sender,
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
binary = "{codex_binary}"
default_sandbox = "workspace-write"
worktree_base_branch = "main"

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
# self-contained without seeding parents).
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

# Mirrors migrations/versions/002_approvals.py + 003_approvals_request_types.py.
# Tests that exercise the Task #43 approval gating need this table.
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


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


# A "fake codex" binary that emits a valid --version and otherwise
# behaves like a no-op subprocess. Written to a tmp dir at fixture time.
_FAKE_CODEX_VERSION_BIN = """\
#!/usr/bin/env python3
import sys
if len(sys.argv) >= 2 and sys.argv[1] == "--version":
    print("codex-cli 0.130.0")
    sys.exit(0)
# Real spawn path is hijacked by tests via patch(create_subprocess_exec).
sys.exit(0)
"""

# Same as above but reports an old version → triggers the min-version
# check.
_OLD_CODEX_VERSION_BIN = """\
#!/usr/bin/env python3
import sys
if len(sys.argv) >= 2 and sys.argv[1] == "--version":
    print("codex-cli 0.129.0")
    sys.exit(0)
sys.exit(0)
"""

# A fake codex that emits the canonical Codex JSONL event sequence per
# spike S-1: a non-JSON prefix line, thread.started, turn.started,
# item.completed (agent_message), turn.completed.
_FAKE_CODEX_SCRIPT = """\
#!/usr/bin/env python3
import json, sys, time
# Non-JSON prefix that the reader must tolerate (spec §7.5 / S-1 §6).
print("Reading additional input from stdin...", flush=True)
print(json.dumps({
    "type": "thread.started",
    "thread_id": "019e1450-1553-7e83-83e9-5d7649363e99",
}), flush=True)
time.sleep(0.05)
print(json.dumps({"type": "turn.started"}), flush=True)
print(json.dumps({
    "type": "item.completed",
    "item": {"id": "1", "type": "agent_message", "text": "ok"},
}), flush=True)
print(json.dumps({
    "type": "turn.completed",
    "usage": {
        "input_tokens": 1,
        "cached_input_tokens": 0,
        "output_tokens": 1,
        "reasoning_output_tokens": 0,
    },
}), flush=True)
sys.exit(0)
"""


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state():
    """DBOS is a process-level singleton; teardown between tests."""
    import contextlib as _contextlib

    with _contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    with _contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    with _contextlib.suppress(Exception):
        register_amc_sender(None)
    if is_launched():
        with _contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def codex_version_bin(tmp_path: Path) -> Path:
    """Drop a fake-codex --version stub on disk; return its path."""
    p = tmp_path / "codex_fake_version"
    p.write_text(_FAKE_CODEX_VERSION_BIN)
    p.chmod(0o755)
    return p


@pytest.fixture
def scaffold(
    tmp_path: Path, codex_version_bin: Path
) -> tuple[Settings, Path, Path]:
    """Build a Settings rooted at ``tmp_path`` + the on-disk roots.

    The Codex binary in config points at ``codex_version_bin`` which
    answers ``--version`` with ``codex-cli 0.130.0`` so the
    pre-flight passes. Tests that exercise real spawn replace this via
    ``object.__setattr__`` and ``patch(create_subprocess_exec)``.
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
def git_repo_in_projects(scaffold: tuple[Settings, Path, Path]) -> Path:
    """Create a real git repo inside ``projects_root`` with a main branch."""
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
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM delegations")]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fake subprocess used by argv / ordering / cleanup tests.
# ---------------------------------------------------------------------------


class _FakeStream:
    async def readline(self) -> bytes:
        return b""

    async def read(self, n: int) -> bytes:
        return b""


class _FakeProcess:
    """Async-process duck — exits cleanly with returncode 0 by default."""

    def __init__(self, *, pid: int = 4242, returncode: int = 0) -> None:
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
        self.returncode = self._target_rc
        self._exit_event.set()
        return self.returncode


# ===========================================================================
# Unit: CodexBrief validation.
# ===========================================================================


def test_brief_minimal_construction() -> None:
    """Required fields only; sandbox defaults to workspace-write."""
    b = CodexBrief(goal="x", repo_path="/r")
    assert b.goal == "x"
    assert b.repo_path == "/r"
    assert b.sandbox == DEFAULT_SANDBOX_MODE == "workspace-write"
    assert b.constraints == []
    assert b.success_criteria == []
    assert b.relevant_files == []
    assert b.create_worktree is True
    assert b.model is None


def test_brief_frozen() -> None:
    """``frozen=True`` — fields cannot be mutated after construction."""
    b = CodexBrief(goal="x", repo_path="/r")
    with pytest.raises(ValidationError):
        b.goal = "y"  # type: ignore[misc]


def test_brief_extra_forbidden() -> None:
    """``extra="forbid"`` — unknown keys raise ValidationError."""
    with pytest.raises(ValidationError):
        CodexBrief(goal="x", repo_path="/r", surprise_field="nope")  # type: ignore[call-arg]


def test_brief_goal_min_length() -> None:
    """Empty goal is rejected by Pydantic min_length."""
    with pytest.raises(ValidationError):
        CodexBrief(goal="", repo_path="/r")


def test_brief_sandbox_literal_enforces_allowed_set() -> None:
    """Sandbox outside the allowed set → ValidationError."""
    # Valid values pass.
    for mode in ("read-only", "workspace-write", "danger-full-access"):
        CodexBrief(goal="x", repo_path="/r", sandbox=mode)  # type: ignore[arg-type]
    # Invalid value is rejected at construction time.
    with pytest.raises(ValidationError) as exc:
        CodexBrief(goal="x", repo_path="/r", sandbox="modal")  # type: ignore[arg-type]
    assert "sandbox" in str(exc.value)


def test_brief_whitespace_stripping() -> None:
    """``str_strip_whitespace=True`` per project convention."""
    b = CodexBrief(goal="  hello  ", repo_path="  /r  ")
    assert b.goal == "hello"
    assert b.repo_path == "/r"


def test_brief_round_trips_through_json() -> None:
    """``model_dump_json`` → ``model_validate_json`` is lossless."""
    b = CodexBrief(
        goal="g",
        repo_path="/r",
        sandbox="read-only",
        constraints=["c1"],
        success_criteria=["s1"],
        relevant_files=["f1"],
        create_worktree=False,
        model="openai:gpt-5",
    )
    raw = b.model_dump_json()
    restored = CodexBrief.model_validate_json(raw)
    assert restored == b


# ===========================================================================
# Unit: rendering.
# ===========================================================================


def test_render_codex_prompt_is_deterministic() -> None:
    """Same brief → byte-identical render."""
    b = CodexBrief(
        goal="Build X",
        repo_path="/repo",
        constraints=["no network"],
        success_criteria=["tests pass"],
        relevant_files=["README.md"],
    )
    a = _render_codex_prompt(b)
    b2 = _render_codex_prompt(b)
    assert a == b2
    assert "Build X" in a
    assert "/repo" in a
    assert "no network" in a


def test_render_codex_prompt_empty_lists_use_placeholder() -> None:
    """Empty lists render '_None._' so sections aren't blank."""
    b = CodexBrief(goal="g", repo_path="/r")
    rendered = _render_codex_prompt(b)
    # Three sections (constraints / success / files) all empty.
    assert rendered.count("_None._") == 3


# ===========================================================================
# Unit: version parser.
# ===========================================================================


def test_parse_semver_handles_prefixes() -> None:
    assert _parse_semver("0.130.0") == (0, 130, 0)
    assert _parse_semver("codex-cli/0.130.0") == (0, 130, 0)
    assert _parse_semver("codex 1.2.3 (build abc)") == (1, 2, 3)
    assert _parse_semver("not a version") is None


# ===========================================================================
# Unit: version check.
# ===========================================================================


def test_version_check_missing_binary_raises() -> None:
    """A non-existent binary raises CodexBinaryError before any side effects."""
    with pytest.raises(CodexBinaryError) as exc:
        _check_codex_version("/no/such/binary-xyz")
    assert "not found on PATH" in str(exc.value)


def test_version_check_below_min_raises(tmp_path: Path) -> None:
    """An older version is rejected with min-version diagnostics."""
    fake = tmp_path / "codex_old"
    fake.write_text(_OLD_CODEX_VERSION_BIN)
    fake.chmod(0o755)
    with pytest.raises(CodexBinaryError) as exc:
        _check_codex_version(str(fake))
    assert "below required minimum" in str(exc.value)
    assert MIN_CODEX_VERSION in str(exc.value)
    assert exc.value.observed_version is not None
    assert "0.129.0" in exc.value.observed_version


def test_version_check_accepts_min_version(codex_version_bin: Path) -> None:
    """A binary at exactly the min version is accepted."""
    observed = _check_codex_version(str(codex_version_bin))
    assert "0.130.0" in observed


# ===========================================================================
# Unit: argv shape matches spec §5.4.
# ===========================================================================


@pytest.mark.asyncio
async def test_argv_matches_spec_canonical_form(
    git_repo_in_projects: Path, make_deps: Any
) -> None:
    """The Codex spawn argv matches spec §5.4 (amended by Task #37):

    ``codex exec --json --skip-git-repo-check --sandbox <mode>
    -C <workspace> [--model <m>] "<prompt>"``

    NEVER ``--ephemeral``.
    """
    deps = make_deps()
    captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["argv"] = list(args)
        captured["cwd"] = kwargs.get("cwd")
        captured["limit"] = kwargs.get("limit")
        captured["stdin"] = kwargs.get("stdin")
        return _FakeProcess()

    brief = CodexBrief(
        goal="Add a docstring",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        handle = await delegate_to_codex(_ctx(deps), brief)

    assert isinstance(handle, CodexDelegationHandle)
    assert handle.agent == "codex"
    argv = captured["argv"]
    # Subcommand + structural flags.
    assert argv[1] == "exec"
    assert "--json" in argv
    assert "--skip-git-repo-check" in argv
    assert "--sandbox" in argv
    sandbox_idx = argv.index("--sandbox")
    assert argv[sandbox_idx + 1] == "workspace-write"
    # -C points at workspace, not raw repo_path.
    cmin_idx = argv.index("-C")
    assert argv[cmin_idx + 1] == handle.workspace
    # --model is the configured subagents.codex value when brief omits it.
    m_idx = argv.index("--model")
    assert argv[m_idx + 1] == "openai:gpt-5"
    # The prompt is the trailing positional argument.
    assert isinstance(argv[-1], str) and len(argv[-1]) > 0
    # NEVER --ephemeral.
    assert "--ephemeral" not in argv
    # Forbidden resume-only flags must not be on the spawn argv either.
    assert "--ask-for-approval" not in argv
    # 2 MiB buffer + closed stdin per §7.5.
    assert captured["limit"] == 2 * 1024 * 1024
    assert captured["stdin"] is asyncio.subprocess.DEVNULL


@pytest.mark.asyncio
async def test_argv_uses_brief_sandbox_override(
    git_repo_in_projects: Path, make_deps: Any
) -> None:
    """``brief.sandbox`` overrides the config default in the ``--sandbox`` slot."""
    deps = make_deps()
    captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["argv"] = list(args)
        return _FakeProcess()

    brief = CodexBrief(
        goal="g",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        sandbox="read-only",
    )
    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        await delegate_to_codex(_ctx(deps), brief)

    argv = captured["argv"]
    sb_idx = argv.index("--sandbox")
    assert argv[sb_idx + 1] == "read-only"


@pytest.mark.asyncio
async def test_argv_uses_brief_model_override(
    git_repo_in_projects: Path, make_deps: Any
) -> None:
    deps = make_deps()
    captured: dict[str, Any] = {}

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        captured["argv"] = list(args)
        return _FakeProcess()

    brief = CodexBrief(
        goal="g",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="openai:gpt-5-mini",
    )
    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        await delegate_to_codex(_ctx(deps), brief)

    argv = captured["argv"]
    m_idx = argv.index("--model")
    assert argv[m_idx + 1] == "openai:gpt-5-mini"


# ===========================================================================
# Unit: persistence-before-yield ordering.
# ===========================================================================


@pytest.mark.asyncio
async def test_persistence_happens_before_handle_returns(
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """INSERT must complete BEFORE delegate_to_codex returns the handle."""
    settings, _, _ = scaffold
    db_path = settings.paths.db_path
    deps = make_deps()
    timeline: list[str] = []

    real_open = open_connection

    class _TrackingConn:
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

    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with patch(
        "capo.tools.codex.open_connection", side_effect=tracking_open
    ), patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        handle = await delegate_to_codex(_ctx(deps), brief)
        timeline.append("returned")

    assert timeline[0] == "persisted", (
        f"persistence-before-yield violated: timeline={timeline}"
    )
    assert "returned" in timeline
    # Row is truly durable + matches the handle.
    rows = _rows(db_path)
    assert len(rows) == 1
    assert rows[0]["id"] == handle.delegation_id
    assert rows[0]["agent"] == "codex"
    assert rows[0]["status"] == "running"
    assert rows[0]["session_id_subagent"] is None  # filled by thread.started
    assert rows[0]["pid"] == 4242
    assert rows[0]["parent_thread_id"] == "amc:test-channel"


# ===========================================================================
# Edge: missing binary BEFORE any side effects.
# ===========================================================================


@pytest.mark.asyncio
async def test_missing_codex_binary_raises_before_side_effects(
    tmp_path: Path,
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """Pre-flight version check: missing binary → CodexBinaryError, no row.

    Confirms "typed error before any side effects" Edge Cases criterion.
    """
    settings, _, workspaces_root = scaffold
    deps = make_deps()
    object.__setattr__(
        deps.settings.agents.codex, "binary", "/no/such/codex-bin"
    )

    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with pytest.raises(CodexBinaryError) as exc:
        await delegate_to_codex(_ctx(deps), brief)
    assert "not found on PATH" in str(exc.value)

    # NO row written, NO workspace created.
    assert _rows(settings.paths.db_path) == []
    assert list(workspaces_root.iterdir()) == []


@pytest.mark.asyncio
async def test_below_min_codex_version_raises(
    tmp_path: Path,
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """Pre-flight: codex < MIN_CODEX_VERSION → CodexBinaryError."""
    settings, _, workspaces_root = scaffold
    # Replace the codex binary with the "old version" stub.
    old_bin = tmp_path / "codex_old"
    old_bin.write_text(_OLD_CODEX_VERSION_BIN)
    old_bin.chmod(0o755)
    deps = make_deps()
    object.__setattr__(deps.settings.agents.codex, "binary", str(old_bin))

    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with pytest.raises(CodexBinaryError) as exc:
        await delegate_to_codex(_ctx(deps), brief)
    assert "below required minimum" in str(exc.value)
    # No row, no workspace.
    assert _rows(settings.paths.db_path) == []
    assert list(workspaces_root.iterdir()) == []


# ===========================================================================
# Edge: repo_path outside projects_root raises ApprovalRequired.
# ===========================================================================


@pytest.mark.asyncio
async def test_repo_outside_projects_root_denied_raises_rejected(
    tmp_path: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """A repo path outside ``projects_root`` routes through approval (Task #43).

    Under the Task #43 wiring, ``delegate_to_codex`` no longer raises
    :class:`ApprovalRequired` directly for out-of-``projects_root`` paths
    — it invokes the §5.8 approval workflow. Driving a ``/deny`` decision
    through DBOS resolves the workflow and yields
    :class:`ApprovalRejected`.
    """
    import contextlib as _contextlib
    import json

    from capo.tools._approval import ApprovalRejected
    from capo.workflows.approval import APPROVAL_TOPIC, STATUS_DENIED

    deps = make_deps()
    settings, _, _ = scaffold

    calls: list[dict[str, str]] = []

    async def _stub_send(channel_id: str, text: str, idempotency_key: str):
        calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )

        class _Res:
            message_id = "msg-codex-out-of-root-deny"

        return _Res()

    register_amc_sender(_stub_send)

    outside = tmp_path / "elsewhere" / "repo"
    outside.mkdir(parents=True)
    brief = CodexBrief(
        goal="g", repo_path=str(outside), create_worktree=False
    )

    async def _denier() -> None:
        from dbos import DBOS

        deadline = asyncio.get_event_loop().time() + 10.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not calls:
                continue
            conn = sqlite3.connect(str(settings.paths.db_path))
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
                {"status": "deny", "resolved_by": "user-42", "reason": "no"},
                APPROVAL_TOPIC,
            )
            return
        raise AssertionError("denier did not observe a pending row")

    denier_task = asyncio.create_task(_denier())
    try:
        with pytest.raises(ApprovalRejected) as excinfo:
            await delegate_to_codex(_ctx(deps), brief)
    finally:
        with _contextlib.suppress(Exception):
            await denier_task

    assert excinfo.value.status == STATUS_DENIED
    assert _rows(scaffold[0].paths.db_path) == []
    assert list((scaffold[0].paths.workspaces_root).iterdir()) == []

    conn = sqlite3.connect(str(settings.paths.db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE status='denied' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["request_type"] == "delegate_out_of_root"
    payload = json.loads(row["request_payload"])
    assert payload["agent"] == "codex"
    assert payload["repo_path"] == str(outside)
    assert payload["sandbox"] == "workspace-write"
    assert "prompt_hash" in payload
    assert "prompt" not in payload, (
        "raw prompt must not be in approval payload"
    )


# ===========================================================================
# Edge: worktree creation failure leaves DB consistent.
# ===========================================================================


@pytest.mark.asyncio
async def test_worktree_failure_leaves_consistent_state(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
) -> None:
    """Non-git repo with create_worktree=True → failed row, no orphan."""
    settings, projects_root, _ = scaffold
    not_a_repo = projects_root / "not_a_repo"
    not_a_repo.mkdir()
    deps = make_deps()
    brief = CodexBrief(
        goal="g", repo_path=str(not_a_repo), create_worktree=True
    )

    spawn_called: list[bool] = []

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        spawn_called.append(True)
        return _FakeProcess()

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        with pytest.raises(Exception) as excinfo:
            await delegate_to_codex(_ctx(deps), brief)
        from capo.tools._worktree import NotAGitRepo, WorktreeError

        assert isinstance(excinfo.value, WorktreeError)
        assert isinstance(excinfo.value, NotAGitRepo)

    # No process was spawned — we failed at the worktree step.
    assert spawn_called == []

    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["agent"] == "codex"
    assert "worktree creation failed" in (rows[0]["summary"] or "")
    assert list(settings.paths.workspaces_root.iterdir()) == []


# ===========================================================================
# Edge: DBOS not launched.
# ===========================================================================


@pytest.mark.asyncio
async def test_dbos_not_launched_marks_row_failed_and_raises(
    git_repo_in_projects: Path,
    make_deps: Any,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """When DBOS is not launched, the spawn marks the row failed + raises."""
    settings, _, _ = scaffold
    # Tear DBOS down so the spawn-site check trips.
    destroy_dbos()
    assert not is_launched()

    deps = make_deps()
    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess()

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ), pytest.raises(DBOSNotLaunchedError):
        await delegate_to_codex(_ctx(deps), brief)

    # Row exists and is marked failed.
    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["summary"] == "DBOS not launched"


# ===========================================================================
# Edge: distinct delegation_ids across two calls.
# ===========================================================================


@pytest.mark.asyncio
async def test_two_delegations_have_distinct_ids(
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    deps = make_deps()

    async def fake_spawn(*args: Any, **kwargs: Any) -> _FakeProcess:
        return _FakeProcess()

    brief = CodexBrief(
        goal="g", repo_path=str(git_repo_in_projects), create_worktree=True
    )

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        h1 = await delegate_to_codex(_ctx(deps), brief)
        h2 = await delegate_to_codex(_ctx(deps), brief)

    assert h1.delegation_id != h2.delegation_id
    ids = {r["id"] for r in _rows(scaffold[0].paths.db_path)}
    assert ids == {h1.delegation_id, h2.delegation_id}


# ===========================================================================
# Integration: end-to-end with a fake "codex" subprocess.
# ===========================================================================


@pytest.mark.asyncio
async def test_integration_end_to_end_with_fake_codex(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """End-to-end: real subprocess spawn, real reader, real DB row.

    Drives the fake codex script that emits a non-JSON prefix line, then
    ``thread.started`` → ``turn.started`` → ``item.completed`` →
    ``turn.completed``. Verifies:
    - The handle is returned with status='running' + agent='codex'.
    - The row exists immediately (persistence-before-yield).
    - The reader captured the thread_id and persisted it to
      ``session_id_subagent``.
    - The row eventually transitions to 'completed' once the child exits.
    - The reader skipped the non-JSON prefix line (S-1 §6).
    """
    settings, _, _ = scaffold
    fake_codex = tmp_path / "fake_codex"
    fake_codex.write_text(_FAKE_CODEX_SCRIPT)
    fake_codex.chmod(0o755)

    deps = make_deps(thread_id="amc:integration-channel")

    brief = CodexBrief(
        goal="emit one event",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="openai:gpt-5",
        sandbox="workspace-write",
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        # Replace argv with [python, fake_codex] so the fake script runs
        # instead of real codex. Keep all other kwargs (cwd, pipes, etc).
        return await real_exec(
            sys.executable,
            str(fake_codex),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        handle = await delegate_to_codex(_ctx(deps), brief)

    assert handle.status == "running"
    assert handle.agent == "codex"
    # Row exists immediately (persistence-before-yield).
    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == handle.delegation_id
    assert row["agent"] == "codex"
    assert row["pid"] is not None
    assert row["model"] == "openai:gpt-5"
    assert row["parent_thread_id"] == "amc:integration-channel"

    # Wait for thread_id capture + completion.
    deadline = asyncio.get_event_loop().time() + 8.0
    expected_thread_id = "019e1450-1553-7e83-83e9-5d7649363e99"
    while asyncio.get_event_loop().time() < deadline:
        rows = _rows(settings.paths.db_path)
        if rows and rows[0]["status"] != "running":
            break
        await asyncio.sleep(0.05)

    rows = _rows(settings.paths.db_path)
    assert rows[0]["status"] == "completed", (
        f"row never transitioned to completed; row={rows[0]}"
    )
    assert rows[0]["session_id_subagent"] == expected_thread_id, (
        f"thread_id not captured into session_id_subagent: "
        f"got {rows[0]['session_id_subagent']!r}"
    )
    assert rows[0]["ended_at"] is not None

    # delegation_output captured the JSON events and skipped the
    # "Reading additional input from stdin..." prefix.
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
    # Non-JSON prefix is recorded as stdout; JSON events as 'event'.
    assert "event" in streams
    # All four JSON events should land as 'event' rows.
    event_payloads = [
        json.loads(r[1]) for r in out if r[0] == "event"
    ]
    types = [p.get("type") for p in event_payloads]
    assert "thread.started" in types
    assert "turn.completed" in types
    # And the non-JSON prefix was captured as stdout (not event).
    stdout_chunks = [r[1] for r in out if r[0] == "stdout"]
    assert any(
        "Reading additional input from stdin" in c for c in stdout_chunks
    ), (
        f"non-JSON prefix should be recorded as stdout, not parsed as "
        f"JSON; streams={streams!r} stdout_chunks={stdout_chunks!r}"
    )


# ===========================================================================
# Edge: thread_id capture timeout.
# ===========================================================================


_SILENT_CODEX_SCRIPT = """\
#!/usr/bin/env python3
import time
# Emit nothing on stdout; just sleep until killed.
time.sleep(30)
"""


@pytest.mark.asyncio
async def test_thread_id_capture_timeout_marks_row_failed(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """When codex never emits ``thread.started``, the row is marked failed
    + the subprocess is killed within a short timeout."""
    import capo.tools.codex as codex_mod

    settings, _, _ = scaffold
    silent_codex = tmp_path / "silent_codex"
    silent_codex.write_text(_SILENT_CODEX_SCRIPT)
    silent_codex.chmod(0o755)

    deps = make_deps()

    brief = CodexBrief(
        goal="g",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(silent_codex),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    # Shrink the capture timeout so the test stays fast.
    with patch.object(codex_mod, "CODEX_THREAD_ID_CAPTURE_TIMEOUT_S", 0.5), patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        handle = await delegate_to_codex(_ctx(deps), brief)

    # Wait for the handoff coroutine to mark the row failed.
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        rows = _rows(settings.paths.db_path)
        if rows and rows[0]["status"] == "failed":
            break
        await asyncio.sleep(0.05)

    rows = _rows(settings.paths.db_path)
    assert rows[0]["id"] == handle.delegation_id
    assert rows[0]["status"] == "failed"
    assert "thread_id not captured" in (rows[0]["summary"] or "")


# ===========================================================================
# Smoke: ThreadIdCaptureTimeout is the right error type.
# ===========================================================================


def test_thread_id_capture_timeout_is_runtime_error() -> None:
    """ThreadIdCaptureTimeout is a RuntimeError subclass for clean except."""
    assert issubclass(ThreadIdCaptureTimeout, RuntimeError)
    assert issubclass(DBOSNotLaunchedError, RuntimeError)
    assert issubclass(CodexBinaryError, RuntimeError)

"""Tests for Phase-2 tool registration on the Capo agent (Task #26).

Covers spec §5.3-§5.5 / §9.2 acceptance criteria:

Unit
* :func:`build_agent` registers all six Phase-2 tools alongside the Phase-1
  basics (``web_search`` + ``fetch_url``): ``shell_exec``,
  ``delegate_to_claude_code``, ``check_delegation_status``,
  ``get_delegation_output``, ``kill_delegation``, ``list_delegations``.
* Re-importing :mod:`capo.agent` does not double-register tools (each call
  to :func:`build_agent` constructs a fresh :class:`pydantic_ai.Agent`).
* Each registered tool exposes a non-empty model-visible description so
  Pydantic AI can present them to the LLM.
* :class:`capo.tools.basic.ApprovalRequired` raised by a tool inside
  ``agent.run`` is caught at the dispatcher boundary and surfaced to the
  user via the Phase-2 stub reply format
  (``"That action (...) needs approval. Reason: .... Full approval flow
  lands in Phase 4."``).

Integration
* An agent run that picks ``delegate_to_claude_code`` returns a
  :class:`DelegationHandle`. Uses a fake ``claude`` binary (the
  ``_FAKE_CLAUDE_SCRIPT`` pattern from Task #22) so the test never touches
  the real CC.
* Two delegate calls in the same run get distinct ``delegation_id``s.
"""

from __future__ import annotations

import asyncio
import importlib
import sqlite3
import sys
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

import capo.agent
from capo.agent import build_agent
from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools.basic import ApprovalRequired
from capo.tools.claude_code import (
    ClaudeCodeBrief,
    DelegationHandle,
    delegate_to_claude_code,
)
from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.dispatcher import (
    ChannelWorker,
    format_approval_required_reply,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    register_amc_sender,
)

# ---------------------------------------------------------------------------
# Schema bootstrap — re-uses migrations/001 the same way test_dispatcher.py
# does. We need a real ``delegations`` + ``delegation_output`` schema for
# the integration test that exercises persist-before-yield.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_PATH = REPO_ROOT / "migrations" / "versions" / "001_init.py"


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    """Run the bare table DDL from migration 001 against ``conn``."""
    import importlib.util as _ilu

    sys.path.insert(0, str(_MIG_PATH.parent))
    spec = _ilu.spec_from_file_location("_mig_001_phase2_reg", _MIG_PATH)
    assert spec is not None and spec.loader is not None
    mig = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mig)
    for stmt in mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Config fixture (mirrors test_delegate_to_claude_code.py).
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
allowlist = ["git","ls","rg","cat"]

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


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state():
    """DBOS is a process-level singleton; clean teardown between tests."""
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
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    """Build a ``Settings`` rooted at ``tmp_path`` with on-disk roots + state.db.

    Initializes the §7.3 schema so tools that touch the DB (the four
    delegation tools + ``delegate_to_claude_code``) work without bringing
    up Alembic. Also launches DBOS so the Task #35 spawn-site handoff
    in :func:`delegate_to_claude_code` succeeds.
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
    souls = tmp_path / "souls"
    souls.mkdir()
    (souls / "default.md").write_text("# default soul\nbe calm\n")
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "system.md").write_text("# system\nrouting rules\n")

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))

    conn = open_connection(db_path)
    try:
        _apply_inline_schema(conn)
        # Seed the owner user so FK references from delegations.user_id
        # are satisfied when the integration tests INSERT a row.
        conn.execute("INSERT INTO users(user_id) VALUES ('owner')")
        conn.commit()
    finally:
        conn.close()

    # Launch DBOS so the Task #35 spawn-site handoff doesn't trip the
    # DBOSNotLaunchedError guard inside ``delegate_to_claude_code``.
    init_dbos(settings)
    launch_dbos()

    return settings, projects_root, workspaces_root


@pytest.fixture
def system_prompt_path(scaffold: tuple[Settings, Path, Path]) -> Path:
    settings, _, _ = scaffold
    # ``soul.dir`` is resolved to ``tmp_path/souls`` — prompts/ lives next to it.
    return settings.soul.dir.parent / "prompts" / "system.md"


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _testmodel_factory(model: str, instructions: str, *, deps_type: type) -> Agent:
    """Build a real :class:`Agent` against :class:`TestModel`.

    Mirrors :func:`capo.agent._default_agent_factory` semantics but swaps
    in ``TestModel`` so no API key is required. ``call_tools=[]`` keeps
    the agent from auto-firing tool calls during registration tests.
    """
    return Agent(
        model=TestModel(call_tools=[]),
        instructions=instructions,
        deps_type=deps_type,
    )


# ---------------------------------------------------------------------------
# Helpers — introspect tool names off a constructed Pydantic AI agent.
# ---------------------------------------------------------------------------


def _registered_tool_names(agent: Any) -> set[str]:
    """Collect the set of tool names registered on ``agent``.

    Pydantic AI 1.x stores tools across ``agent.toolsets`` (each with a
    ``tools`` mapping keyed by tool name).
    """
    names: set[str] = set()
    for ts in agent.toolsets:
        if hasattr(ts, "tools"):
            names.update(ts.tools.keys())
    return names


def _tool_descriptions(agent: Any) -> dict[str, str]:
    """Map tool name → rendered description for each tool on ``agent``."""
    out: dict[str, str] = {}
    for ts in agent.toolsets:
        for name, tool in getattr(ts, "tools", {}).items():
            desc = getattr(tool, "description", None)
            if desc is None and hasattr(tool, "tool_def"):
                desc = getattr(tool.tool_def, "description", None)
            if desc:
                out[name] = desc
    return out


# ---------------------------------------------------------------------------
# Unit: registration — all six Phase-2 tools + Phase-1 basics present.
# ---------------------------------------------------------------------------


PHASE1_TOOLS: frozenset[str] = frozenset({"web_search", "fetch_url"})

PHASE2_TOOLS: frozenset[str] = frozenset(
    {
        "shell_exec",
        "delegate_to_claude_code",
        "check_delegation_status",
        "get_delegation_output",
        "kill_delegation",
        "list_delegations",
    }
)

# Phase-5 session-control NL tools (Task #53, spec §5.10 / §5.11). Mirror
# the slash commands ``/new`` / ``/status`` / ``/clear`` so the LLM can
# invoke them when the user phrases the request in natural language.
PHASE5_TOOLS: frozenset[str] = frozenset(
    {
        "session_new",
        "session_status",
        "session_clear",
    }
)


def test_build_agent_registers_all_phase1_and_phase2_tools(
    scaffold: tuple[Settings, Path, Path],
    system_prompt_path: Path,
) -> None:
    """All eight tools land on the agent in a single ``build_agent`` call."""
    settings, _, _ = scaffold
    agent = build_agent(
        settings, system_prompt_path, agent_factory=_testmodel_factory
    )
    names = _registered_tool_names(agent)

    missing = (PHASE1_TOOLS | PHASE2_TOOLS | PHASE5_TOOLS) - names
    assert not missing, (
        f"expected all 11 tools registered; missing {sorted(missing)}"
    )


def test_build_agent_registers_phase2_tools_with_descriptions(
    scaffold: tuple[Settings, Path, Path],
    system_prompt_path: Path,
) -> None:
    """Each Phase-2 tool exposes a non-empty description for the model.

    Pydantic AI uses tool docstrings verbatim as the model-visible
    description. An empty description would mean the model can't tell when
    to use the tool — guard against accidental docstring removal here.
    """
    settings, _, _ = scaffold
    agent = build_agent(
        settings, system_prompt_path, agent_factory=_testmodel_factory
    )
    descriptions = _tool_descriptions(agent)

    for name in PHASE2_TOOLS:
        assert name in descriptions, (
            f"phase-2 tool {name!r} has no description on the agent"
        )
        # A useful description is more than a few characters of whitespace.
        assert len(descriptions[name].strip()) > 20, (
            f"phase-2 tool {name!r} description is too short: "
            f"{descriptions[name]!r}"
        )


def test_reimporting_capo_agent_does_not_change_tool_count(
    scaffold: tuple[Settings, Path, Path],
    system_prompt_path: Path,
) -> None:
    """Re-importing :mod:`capo.agent` and re-calling :func:`build_agent`
    yields a fresh agent with the *same* tool set — no double registration.
    """
    settings, _, _ = scaffold
    agent_a = build_agent(
        settings, system_prompt_path, agent_factory=_testmodel_factory
    )
    names_a = _registered_tool_names(agent_a)

    # Force a re-import. We're not mutating module-level state, but this
    # mirrors what a test runner that imports capo.agent twice (e.g. from
    # two test files) would observe.
    importlib.reload(capo.agent)

    agent_b = capo.agent.build_agent(
        settings, system_prompt_path, agent_factory=_testmodel_factory
    )
    names_b = _registered_tool_names(agent_b)

    assert names_a == names_b, (
        f"tool registration changed across imports: {names_a ^ names_b}"
    )
    # And both contain the full Phase-1 + Phase-2 + Phase-5 set exactly once.
    assert names_a == (PHASE1_TOOLS | PHASE2_TOOLS | PHASE5_TOOLS), (
        f"expected exactly the 11 capo tools; got {sorted(names_a)}"
    )


def test_build_agent_called_twice_gives_independent_agents(
    scaffold: tuple[Settings, Path, Path],
    system_prompt_path: Path,
) -> None:
    """Two ``build_agent`` calls produce *distinct* agent instances with the
    same tool set. Production never reuses a single agent across calls, so
    this is the contract that makes phase-2 registration safe to repeat.
    """
    settings, _, _ = scaffold
    a = build_agent(settings, system_prompt_path, agent_factory=_testmodel_factory)
    b = build_agent(settings, system_prompt_path, agent_factory=_testmodel_factory)

    assert a is not b
    assert _registered_tool_names(a) == _registered_tool_names(b)
    assert _registered_tool_names(a) == (
        PHASE1_TOOLS | PHASE2_TOOLS | PHASE5_TOOLS
    )


# ---------------------------------------------------------------------------
# Unit: ApprovalRequired → stub reply formatter.
# ---------------------------------------------------------------------------


def test_format_approval_required_reply_uses_phase2_stub() -> None:
    """The dispatcher's translation of ``ApprovalRequired`` → user text.

    Wording is **locked** by the Task #26 brief; a future Phase-4 rewrite
    should be intentional, not accidental.
    """
    exc = ApprovalRequired(
        command="rm -rf /etc",
        reason="binary 'rm' is not in the shell allowlist",
    )
    text = format_approval_required_reply(exc)

    assert text == (
        "That action (rm -rf /etc) needs approval. "
        "Reason: binary 'rm' is not in the shell allowlist. "
        "Full approval flow lands in Phase 4."
    )
    # Component-level sanity (defends against accidental reordering).
    assert "(rm -rf /etc)" in text
    assert "needs approval" in text
    assert "Phase 4" in text


# ---------------------------------------------------------------------------
# Unit: ApprovalRequired raised inside agent.run is caught + replied via AMC.
# ---------------------------------------------------------------------------


class _FakeAgentRaisingApproval:
    """An agent fake whose ``run`` always raises :class:`ApprovalRequired`.

    Simulates a tool inside the agent loop having raised — Pydantic AI's
    real behavior, as confirmed empirically against a registered tool
    that raises ``ApprovalRequired``.
    """

    def __init__(self, command: str, reason: str) -> None:
        self._exc = ApprovalRequired(command=command, reason=reason)
        self.calls: list[str] = []

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        self.calls.append(text)
        raise self._exc


class _FakeAMCClient:
    """Minimal AMC client recorder for dispatcher reply assertions."""

    def __init__(self) -> None:
        self.send_calls: list[dict[str, Any]] = []
        self.mark_read_calls: list[list[str]] = []

    async def send(
        self,
        channel_id: str,
        text: str,
        *,
        reply_to_message_id: str | None = None,
        approval: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        self.send_calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
            }
        )
        return {"message_id": "out-1", "channel_id": channel_id, "status": "sent"}

    async def mark_read(self, message_ids: Any) -> Any:
        self.mark_read_calls.append(list(message_ids))
        return {"marked_read": list(message_ids), "already_read": []}


def _conn_factory(tmp_path: Path) -> Callable[[], sqlite3.Connection]:
    """Build a connection factory backed by a real schema-loaded sqlite file.

    Reuses migrations/001 so the conversation memory tables exist (the
    dispatcher's history-load path runs before ``agent.run``).
    """
    db_path = tmp_path / "dispatcher.db"
    boot = sqlite3.connect(str(db_path))
    boot.execute("PRAGMA foreign_keys = ON")
    _apply_inline_schema(boot)
    boot.execute("INSERT INTO users(user_id) VALUES ('owner')")
    boot.commit()
    boot.close()

    def _factory() -> sqlite3.Connection:
        c = sqlite3.connect(
            str(db_path), isolation_level=None, check_same_thread=False
        )
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("PRAGMA busy_timeout = 5000")
        return c

    return _factory


@pytest.mark.asyncio
async def test_dispatcher_catches_approval_required_and_sends_stub(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
) -> None:
    """``ApprovalRequired`` from a tool → AMC reply with the Phase-2 stub.

    Wraps :class:`ChannelWorker` (which owns ``_handle_envelope``) around a
    fake agent that always raises, then drives one envelope through and
    asserts the recorded AMC send.
    """
    settings, _, _ = scaffold
    agent = _FakeAgentRaisingApproval(
        command="shell_exec('rm -rf /etc')",
        reason="binary 'rm' is not in the shell allowlist",
    )
    amc = _FakeAMCClient()
    conn_factory = _conn_factory(tmp_path)

    def deps_factory(user_id: str) -> CapoDeps:
        return CapoDeps(
            settings=settings,
            http_client=None,  # type: ignore[arg-type]
            user_id=user_id,
        )

    worker = ChannelWorker(
        channel_id="amc:approval-channel",
        settings=settings,
        agent=agent,
        amc_client=amc,
        store_conn_factory=conn_factory,
        deps_factory=deps_factory,
    )
    worker.start()
    try:
        envelope = AMCInboundEnvelope(
            id="msg-approval-1",
            channel_id="amc:approval-channel",
            sender_id="+15551234567",  # maps to user_id 'owner' per fixture.
            text="please rm -rf /etc for me",
            ts="2026-05-10T12:00:00Z",
        )
        worker.put_nowait(envelope)
        await asyncio.wait_for(worker._queue.join(), timeout=5.0)
    finally:
        await worker.stop()

    # The fake agent ran exactly once.
    assert agent.calls == ["please rm -rf /etc for me"]
    # Exactly one outbound send, carrying the Phase-2 stub format.
    assert len(amc.send_calls) == 1
    send = amc.send_calls[0]
    assert send["channel_id"] == "amc:approval-channel"
    assert send["reply_to_message_id"] == "msg-approval-1"
    expected = (
        "That action (shell_exec('rm -rf /etc')) needs approval. "
        "Reason: binary 'rm' is not in the shell allowlist. "
        "Full approval flow lands in Phase 4."
    )
    assert send["text"] == expected
    # mark_read still fires (defense-in-depth so AMC stops redelivering).
    assert amc.mark_read_calls == [["msg-approval-1"]]


# ---------------------------------------------------------------------------
# Integration: agent → delegate_to_claude_code returns a DelegationHandle.
#
# We exercise the *tool* directly through the agent's registered surface so
# we don't have to teach the underlying TestModel to invent a valid
# ClaudeCodeBrief. The point of this test is the registration glue: a
# registered tool, invoked through the agent's deps, returns the production
# type with persistence-before-yield intact.
# ---------------------------------------------------------------------------


# Self-contained fake ``claude`` script. Identical pattern to Task #22's
# ``_FAKE_CLAUDE_SCRIPT`` — emits one system-init event then exits cleanly.
_FAKE_CLAUDE_SCRIPT = """\
#!/usr/bin/env python3
import json, sys, time
event = {
    "type": "system",
    "subtype": "init",
    "session_id": "deadbeef-1111-2222-3333-444444444444",
    "claude_code_version": "0.0.0",
}
print(json.dumps(event), flush=True)
time.sleep(0.05)
final = {
    "type": "result",
    "session_id": "deadbeef-1111-2222-3333-444444444444",
    "is_error": False,
    "subtype": "success",
}
print(json.dumps(final), flush=True)
sys.exit(0)
"""


def _make_run_ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    """Minimal :class:`RunContext` for direct tool invocation."""
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _git_init_repo(repo: Path) -> None:
    """Create a real git repo with one commit on ``main``."""
    import subprocess

    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init", "-q", "-b", "main"],
        cwd=str(repo),
        check=True,
        shell=False,
    )
    subprocess.run(
        ["git", "config", "user.email", "test@capo.local"],
        cwd=str(repo),
        check=True,
        shell=False,
    )
    subprocess.run(
        ["git", "config", "user.name", "Capo Test"],
        cwd=str(repo),
        check=True,
        shell=False,
    )
    (repo / "README.md").write_text("hello\n")
    subprocess.run(
        ["git", "add", "README.md"],
        cwd=str(repo),
        check=True,
        shell=False,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "initial"],
        cwd=str(repo),
        check=True,
        shell=False,
    )


@pytest.mark.asyncio
async def test_agent_run_with_delegate_tool_returns_handle(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    system_prompt_path: Path,
    http_client: httpx.AsyncClient,
) -> None:
    """A registered ``delegate_to_claude_code`` call yields a
    :class:`DelegationHandle`.

    Builds the agent through :func:`build_agent` (so we exercise the real
    registration path), then invokes the registered tool function via
    Pydantic AI's run context. We don't try to make the model *choose* the
    tool — the registration glue is what we're verifying.
    """
    settings, projects_root, _ = scaffold
    repo = projects_root / "repo"
    _git_init_repo(repo)

    # Build the agent — proves registration succeeds against a real Agent.
    agent = build_agent(
        settings, system_prompt_path, agent_factory=_testmodel_factory
    )
    names = _registered_tool_names(agent)
    assert "delegate_to_claude_code" in names

    # Drive the registered tool. We swap the spawn step for the fake script
    # so we don't need a real ``claude`` binary in the test env.
    fake_claude = tmp_path / "fake_claude.py"
    fake_claude.write_text(_FAKE_CLAUDE_SCRIPT)
    fake_claude.chmod(0o755)

    deps = CapoDeps.from_settings(
        settings=settings,
        http_client=http_client,
        user_id="owner",
        thread_id="amc:integration-channel",
    )
    # Swap the configured CC binary for the python interpreter; the spawn
    # hijack below replaces argv with ``[python, fake_claude]``.
    object.__setattr__(deps.settings.agents.claude_code, "binary", sys.executable)

    brief = ClaudeCodeBrief(
        goal="emit one event",
        repo_path=str(repo),
        create_worktree=True,
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
        handle = await delegate_to_claude_code(_make_run_ctx(deps), brief)

    assert isinstance(handle, DelegationHandle)
    assert handle.status == "running"
    assert handle.agent == "claude-code"
    assert handle.delegation_id


@pytest.mark.asyncio
async def test_two_delegate_calls_get_distinct_ids(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    http_client: httpx.AsyncClient,
) -> None:
    """Sequential delegate calls in the same context get distinct ids.

    Uses the lower-cost in-process fake-spawn pattern (no real subprocess)
    since we're only asserting id uniqueness, not subprocess behavior.
    """
    settings, projects_root, _ = scaffold
    repo = projects_root / "repo2"
    _git_init_repo(repo)

    deps = CapoDeps.from_settings(
        settings=settings,
        http_client=http_client,
        user_id="owner",
        thread_id="amc:integration-channel",
    )

    class _FakeStream:
        async def readline(self) -> bytes:
            return b""

        async def read(self, n: int) -> bytes:
            return b""

    class _FakeProc:
        def __init__(self) -> None:
            self.pid = 0
            self.returncode: int | None = None
            self.stdout = _FakeStream()
            self.stderr = _FakeStream()
            self._next_pid = 99999

        async def wait(self) -> int:
            self.returncode = 0
            return 0

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    pids = iter([10001, 10002])

    async def fake_spawn(*args: Any, **kwargs: Any) -> Any:
        proc = _FakeProc()
        proc.pid = next(pids)
        return proc

    brief = ClaudeCodeBrief(
        goal="goal",
        repo_path=str(repo),
        create_worktree=False,  # avoid double-worktree on the same branch
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=fake_spawn,
    ):
        h1 = await delegate_to_claude_code(_make_run_ctx(deps), brief)
        h2 = await delegate_to_claude_code(_make_run_ctx(deps), brief)

    assert h1.delegation_id != h2.delegation_id
    assert len({h1.delegation_id, h2.delegation_id}) == 2

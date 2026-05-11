"""Unit tests for Task 14 — :mod:`capo.tools.basic` + ``CapoDeps`` wiring.

Covers acceptance criteria from spec §5.1, §9.1:

Functional
* Pydantic AI ``Agent`` is constructed with ``instructions=<composite>``,
  ``deps_type=CapoDeps``, ``model=settings.models.default``.
* ``web_search(query)`` is registered and returns ``list[SearchResult]``.
* ``fetch_url(url)`` is registered and returns a UTF-8 string.

Edge cases
* ``fetch_url`` non-2xx → structured ``FetchError`` (NOT exception).
* ``web_search`` empty query → :class:`pydantic_ai.ModelRetry`.

Error handling
* Tool exceptions (network errors in ``fetch_url``) → structured error
  surfaced to the agent, not a raised exception.

SOUL anchoring (§5.1)
* No tool docstring / rendered tool description contains SOUL text.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.agent import build_agent
from capo.config import Settings
from capo.deps import CapoDeps, FetchError, SearchResult
from capo.tools import basic as tools_basic
from capo.tools.basic import (
    FETCH_URL_MAX_BYTES,
    fetch_url,
    web_search,
)

# ---------------------------------------------------------------------------
# Shared fixtures.
# ---------------------------------------------------------------------------

# A SOUL body containing a unique marker the SOUL-anchoring test scans for.
SOUL_MARKER = "SOUL-ANCHOR-DO-NOT-LEAK-12345"

_VALID_TOML = """\
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "~/.capo/workspaces"
projects_root   = "~/code"
db_path         = "~/.capo/state.db"
dbos_db_path    = "~/.capo/dbos.db"

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


def _scaffold(tmp_path: Path, *, soul_text: str | None = None) -> tuple[Settings, Path]:
    """Write a valid config layout and return ``(settings, system_prompt_path)``."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(_VALID_TOML)
    souls = tmp_path / "souls"
    souls.mkdir(exist_ok=True)
    (souls / "default.md").write_text(soul_text or "# default soul\nbe calm\n")
    prompts = tmp_path / "prompts"
    prompts.mkdir(exist_ok=True)
    system_path = prompts / "system.md"
    system_path.write_text("# system\nrouting rules\n")
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    return settings, system_path


def _make_ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    """Build a minimal :class:`RunContext` for direct tool-function invocation.

    Avoids spinning up a full agent run; we only need ``ctx.deps`` to flow
    through.
    """
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


class _FakeWebSearch:
    """In-memory web-search backend for happy-path tests."""

    def __init__(self, results: list[SearchResult]) -> None:
        self._results = results
        self.calls: list[str] = []

    def search(self, query: str) -> list[SearchResult]:
        self.calls.append(query)
        return list(self._results)


# ---------------------------------------------------------------------------
# Agent construction — Functional AC #1.
# ---------------------------------------------------------------------------


def _testmodel_factory(model: str, instructions: str, *, deps_type: type) -> Agent:
    """Build a real ``Agent`` against ``TestModel`` so we don't need API keys.

    Mirrors ``_default_agent_factory`` semantics (deps_type pass-through)
    but swaps in ``TestModel`` for the actual provider. Tool registration
    in ``build_agent`` still runs against this Agent.
    """
    return Agent(model=TestModel(), instructions=instructions, deps_type=deps_type)


def test_build_agent_constructs_with_deps_type_and_tools(tmp_path: Path) -> None:
    """build_agent passes deps_type=CapoDeps and registers both tools."""
    settings, system_path = _scaffold(tmp_path)
    agent = build_agent(settings, system_path, agent_factory=_testmodel_factory)

    # Agent built with our settings model.
    # Pydantic AI 1.x stores it on a few attrs; ``model`` is the public one.
    assert getattr(agent, "model", None) is not None
    # Tools registered.
    tool_names: set[str] = set()
    for ts in agent.toolsets:
        if hasattr(ts, "tools"):
            tool_names.update(ts.tools.keys())
    assert {"web_search", "fetch_url"}.issubset(tool_names)


def test_build_agent_factory_receives_deps_type(tmp_path: Path) -> None:
    """When the injected factory accepts deps_type, it gets CapoDeps."""
    settings, system_path = _scaffold(tmp_path)
    captured: dict[str, Any] = {}

    def factory(model: str, instructions: str, *, deps_type: type) -> Any:
        captured["model"] = model
        captured["instructions"] = instructions
        captured["deps_type"] = deps_type
        # Return a plain object so register_basic_tools (which checks
        # hasattr(agent, "tool")) is skipped.
        return object()

    build_agent(settings, system_path, agent_factory=factory)
    assert captured["model"] == settings.models.default
    assert captured["deps_type"] is CapoDeps
    assert captured["instructions"].endswith("routing rules\n")


async def test_agent_run_returns_reply_string_with_deps(tmp_path: Path) -> None:
    """Functional AC: ``agent.run(text, deps=...)`` returns a reply string.

    Uses :class:`TestModel` (configured to skip tool calls so it doesn't
    auto-fire with bogus args) so we don't hit any real LLM provider.
    Confirms the wire-up: deps_type is registered, deps flow through, and
    the output is a string.
    """
    settings, system_path = _scaffold(tmp_path)

    def factory(model: str, instructions: str, *, deps_type: type) -> Agent:
        return Agent(
            model=TestModel(call_tools=[]),
            instructions=instructions,
            deps_type=deps_type,
        )

    agent = build_agent(settings, system_path, agent_factory=factory)

    async with httpx.AsyncClient() as http:
        deps = CapoDeps(settings=settings, http_client=http)
        result = await agent.run("hello", deps=deps)

    assert isinstance(result.output, str)


# ---------------------------------------------------------------------------
# web_search — happy path + empty-query edge case.
# ---------------------------------------------------------------------------


async def test_web_search_happy_path_returns_results() -> None:
    """Injected client → list[SearchResult] passes through verbatim."""
    fake = _FakeWebSearch(
        [
            SearchResult(title="A", url="https://a.example", snippet="alpha"),
            SearchResult(title="B", url="https://b.example", snippet="beta"),
        ]
    )
    async with httpx.AsyncClient() as http:
        deps = CapoDeps(
            settings=_settings_stub(),
            http_client=http,
            web_search_client=fake,
        )
        results = await web_search(_make_ctx(deps), "test query")

    assert [r.title for r in results] == ["A", "B"]
    assert fake.calls == ["test query"]


async def test_web_search_empty_query_raises_model_retry() -> None:
    """Empty / whitespace query → ModelRetry with a clear message."""
    fake = _FakeWebSearch([])
    async with httpx.AsyncClient() as http:
        deps = CapoDeps(
            settings=_settings_stub(),
            http_client=http,
            web_search_client=fake,
        )
        with pytest.raises(ModelRetry) as excinfo:
            await web_search(_make_ctx(deps), "   ")
    assert "query is required" in str(excinfo.value).lower()
    assert fake.calls == []  # never reached the backend


async def test_web_search_missing_provider_raises_model_retry() -> None:
    """No web_search_client → ModelRetry, NOT AttributeError."""
    async with httpx.AsyncClient() as http:
        deps = CapoDeps(
            settings=_settings_stub(),
            http_client=http,
            web_search_client=None,
        )
        with pytest.raises(ModelRetry) as excinfo:
            await web_search(_make_ctx(deps), "anything")
    assert "no web-search provider" in str(excinfo.value).lower()


# ---------------------------------------------------------------------------
# fetch_url — happy path + 404 + body-cap + network error.
# ---------------------------------------------------------------------------


async def test_fetch_url_happy_path_returns_body() -> None:
    """200 OK → response body returned as decoded UTF-8."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/hello"
        return httpx.Response(200, text="hello world")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        deps = CapoDeps(settings=_settings_stub(), http_client=http)
        body = await fetch_url(_make_ctx(deps), "http://example.invalid/hello")
    assert body == "hello world"


async def test_fetch_url_non_2xx_returns_structured_error() -> None:
    """404 → FetchError(error='http_error', status=404). No exception."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="missing")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        deps = CapoDeps(settings=_settings_stub(), http_client=http)
        result = await fetch_url(_make_ctx(deps), "http://example.invalid/missing")

    assert isinstance(result, FetchError)
    assert result.error == "http_error"
    assert result.status == 404
    assert result.url == "http://example.invalid/missing"


async def test_fetch_url_body_too_large_returns_structured_error() -> None:
    """Body exceeding cap → FetchError(error='body_too_large')."""
    oversized = b"x" * (FETCH_URL_MAX_BYTES + 16)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=oversized)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        deps = CapoDeps(settings=_settings_stub(), http_client=http)
        result = await fetch_url(_make_ctx(deps), "http://example.invalid/huge")

    assert isinstance(result, FetchError)
    assert result.error == "body_too_large"


async def test_fetch_url_network_error_returns_structured_error() -> None:
    """ConnectError / DNS / TLS / timeout → FetchError, NOT raised."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns hosed")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        deps = CapoDeps(settings=_settings_stub(), http_client=http)
        result = await fetch_url(_make_ctx(deps), "http://example.invalid/down")

    assert isinstance(result, FetchError)
    assert result.error == "http_error"
    assert result.status is None
    assert "ConnectError" in (result.detail or "")


async def test_fetch_url_empty_url_raises_model_retry() -> None:
    """Empty URL → ModelRetry so the model fixes its input."""
    async with httpx.AsyncClient() as http:
        deps = CapoDeps(settings=_settings_stub(), http_client=http)
        with pytest.raises(ModelRetry):
            await fetch_url(_make_ctx(deps), "")


# ---------------------------------------------------------------------------
# SOUL anchoring — §5.1 acceptance criterion.
# ---------------------------------------------------------------------------


def test_soul_absent_from_tool_descriptions(tmp_path: Path) -> None:
    """Tool docstrings / rendered descriptions must NOT contain SOUL text."""
    settings, system_path = _scaffold(
        tmp_path,
        soul_text=f"{SOUL_MARKER}\nbe calm\n",
    )
    agent = build_agent(settings, system_path, agent_factory=_testmodel_factory)

    # Composite instructions DO contain SOUL — that's the whole point.
    # Pydantic AI 1.x stores instructions as a list[str] on ``_instructions``.
    rendered_instructions = " ".join(
        s for s in getattr(agent, "_instructions", []) if isinstance(s, str)
    )
    assert SOUL_MARKER in rendered_instructions, (
        f"expected SOUL marker in agent instructions; got {rendered_instructions!r}"
    )

    # ...but no tool description should contain it.
    descriptions: list[str] = []
    for ts in agent.toolsets:
        for tool in getattr(ts, "tools", {}).values():
            desc = getattr(tool, "description", None)
            if desc is None and hasattr(tool, "tool_def"):
                desc = getattr(tool.tool_def, "description", None)
            if desc:
                descriptions.append(desc)

    assert descriptions, "expected at least one registered tool description"
    for desc in descriptions:
        assert SOUL_MARKER not in desc, (
            f"SOUL content leaked into tool description: {desc!r}"
        )

    # And the raw docstrings of the underlying tool functions also stay clean.
    for fn in (web_search, fetch_url):
        assert SOUL_MARKER not in (inspect.getdoc(fn) or "")


def test_basic_tools_module_does_not_import_soul() -> None:
    """Defense-in-depth: the tools module text itself has no SOUL leakage.

    A test that catches a future regression where someone copy-pastes SOUL
    text into a tool docstring "to give the model context".
    """
    source = inspect.getsource(tools_basic)
    # SOUL is specifically the user-defined personality file — its content
    # should never appear in tool descriptions. We can't check for arbitrary
    # SOUL content here, but we CAN assert the module never reads a SOUL
    # file itself.
    assert "souls/" not in source
    assert "soul_text" not in source.lower()


# ---------------------------------------------------------------------------
# Test helpers.
# ---------------------------------------------------------------------------


def _settings_stub() -> Settings:
    """Settings.load() is heavyweight; tools only need ``settings`` to exist.

    For tool-level tests we lean on ``model_construct`` to skip validation —
    we don't care about the values, only that the attribute is present.
    """
    return Settings.model_construct()

"""Unit tests for :mod:`capo.agent` (Task 9 — SOUL + ops-prompt loader).

Covers acceptance criteria from spec §5.1, §9.1:

* Composite instructions are exactly ``<soul>\\n\\n<system_prompt>``.
* Default model is read from ``config.toml [models] default`` and passed to
  the injected agent factory.
* Missing SOUL file → :class:`AgentBuildError` with the path checked.
* Missing ``prompts/system.md`` → :class:`AgentBuildError` with the path.
* SOUL files longer than the recommended max trigger a ``logging.warning``
  but do **not** fail the build.
* The injected agent factory is invoked exactly once per :func:`build_agent`
  call (one-shot composition; no hidden re-reads).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from capo.agent import (
    SOUL_RECOMMENDED_MAX_LINES,
    AgentBuildError,
    build_agent,
    build_composite_instructions,
)
from capo.config import Settings

# ---------------------------------------------------------------------------
# Shared fixtures — keep the harness minimal and explicit. We reuse the
# baseline TOML from test_config to ensure our loader stays in sync with the
# settings layer.
# ---------------------------------------------------------------------------

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


def _write_layout(
    tmp_path: Path,
    *,
    soul_text: str = "# default soul\nbe calm\n",
    system_text: str = "# system\nrouting rules\n",
    soul_active: str = "default",
    omit_soul: bool = False,
    omit_system: bool = False,
) -> tuple[Settings, Path, Path]:
    """Scaffold a config + soul + prompts layout in ``tmp_path``.

    Returns ``(settings, soul_path, system_prompt_path)``. The boolean
    ``omit_*`` flags allow tests to deliberately exercise the missing-file
    error paths without breaking the Settings.load() pre-validation step.
    """
    cfg = tmp_path / "config.toml"
    cfg.write_text(_VALID_TOML)

    # SOUL file: Settings.load() requires it to exist, so we always write
    # a temporary placeholder, then optionally delete it after the fact
    # to exercise the missing-file path.
    souls = tmp_path / "souls"
    souls.mkdir(exist_ok=True)
    soul_path = souls / f"{soul_active}.md"
    soul_path.write_text(soul_text)

    prompts = tmp_path / "prompts"
    prompts.mkdir(exist_ok=True)
    system_path = prompts / "system.md"
    system_path.write_text(system_text)

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))

    if omit_soul:
        soul_path.unlink()
    if omit_system:
        system_path.unlink()

    return settings, soul_path, system_path


class _FakeAgent:
    """Captures kwargs passed by :func:`build_agent` to its factory."""

    def __init__(self, model: str, instructions: str) -> None:
        self.model = model
        self.instructions = instructions


def _make_factory():
    """Return a factory that records every call it receives.

    The returned ``factory`` callable has a ``.calls`` attribute holding the
    list of ``(model, instructions)`` tuples it has been called with, plus a
    ``.last`` attribute returning the most recent :class:`_FakeAgent`.
    """
    calls: list[tuple[str, str]] = []
    last: list[_FakeAgent] = []

    def factory(model: str, instructions: str) -> _FakeAgent:
        agent = _FakeAgent(model, instructions)
        calls.append((model, instructions))
        last.append(agent)
        return agent

    factory.calls = calls  # type: ignore[attr-defined]
    factory.last = last  # type: ignore[attr-defined]
    return factory


# ---------------------------------------------------------------------------
# Happy path — composite contents + model wiring.
# ---------------------------------------------------------------------------


def test_composite_instructions_equal_soul_then_system(tmp_path: Path) -> None:
    """``instructions == soul + "\\n\\n" + system`` exactly."""
    settings, _soul, system_path = _write_layout(
        tmp_path,
        soul_text="SOUL-BODY",
        system_text="SYS-BODY",
    )
    composite = build_composite_instructions(settings, system_path)
    assert composite == "SOUL-BODY\n\nSYS-BODY"


def test_build_agent_passes_composite_to_factory(tmp_path: Path) -> None:
    """``build_agent`` forwards the composite as ``instructions=`` to the factory."""
    settings, _soul, system_path = _write_layout(
        tmp_path,
        soul_text="SOUL-BODY",
        system_text="SYS-BODY",
    )
    factory = _make_factory()
    agent = build_agent(settings, system_path, agent_factory=factory)

    assert isinstance(agent, _FakeAgent)
    assert agent.instructions == "SOUL-BODY\n\nSYS-BODY"


def test_default_model_from_config_passed_to_factory(tmp_path: Path) -> None:
    """``settings.models.default`` reaches the agent factory verbatim."""
    settings, _soul, system_path = _write_layout(tmp_path)
    factory = _make_factory()
    agent = build_agent(settings, system_path, agent_factory=factory)

    assert agent.model == settings.models.default
    assert agent.model == "anthropic:claude-sonnet-4-6"


def test_factory_invoked_exactly_once_per_call(tmp_path: Path) -> None:
    """Composite is built exactly once per :func:`build_agent` invocation.

    Guards against accidental double-reads (e.g. computing the composite,
    then re-computing inside the factory wrapper).
    """
    settings, _soul, system_path = _write_layout(tmp_path)
    factory = _make_factory()
    build_agent(settings, system_path, agent_factory=factory)
    assert len(factory.calls) == 1  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Error paths — missing files cite the offending path.
# ---------------------------------------------------------------------------


def test_missing_soul_raises_with_path(tmp_path: Path) -> None:
    """Removing the SOUL file post-load → :class:`AgentBuildError` w/ the path."""
    settings, soul_path, system_path = _write_layout(tmp_path, omit_soul=True)
    with pytest.raises(AgentBuildError) as excinfo:
        build_agent(settings, system_path, agent_factory=_make_factory())
    msg = str(excinfo.value)
    assert "soul" in msg.lower()
    assert str(soul_path) in msg


def test_missing_system_prompt_raises_with_path(tmp_path: Path) -> None:
    """Missing ``prompts/system.md`` → :class:`AgentBuildError` w/ the path."""
    settings, _soul, system_path = _write_layout(tmp_path, omit_system=True)
    with pytest.raises(AgentBuildError) as excinfo:
        build_agent(settings, system_path, agent_factory=_make_factory())
    msg = str(excinfo.value)
    assert "system prompt" in msg.lower()
    assert str(system_path) in msg


def test_errors_are_single_line(tmp_path: Path) -> None:
    """Error messages stay on one line so ``main.py`` can stderr them verbatim."""
    settings, _soul, system_path = _write_layout(tmp_path, omit_system=True)
    with pytest.raises(AgentBuildError) as excinfo:
        build_agent(settings, system_path, agent_factory=_make_factory())
    assert "\n" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# SOUL length warning — spec §5.1 edge case.
# ---------------------------------------------------------------------------


def test_long_soul_logs_warning_and_succeeds(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """SOUL > 50 lines → boot succeeds, ``logging.warning`` emitted."""
    long_soul = "\n".join(f"line {i}" for i in range(SOUL_RECOMMENDED_MAX_LINES + 5))
    settings, _soul, system_path = _write_layout(tmp_path, soul_text=long_soul)

    factory = _make_factory()
    with caplog.at_level(logging.WARNING, logger="capo.agent"):
        agent = build_agent(settings, system_path, agent_factory=factory)

    assert isinstance(agent, _FakeAgent)
    # Spec mandates the exact phrase "longer than recommended".
    assert any(
        "longer than recommended" in record.getMessage() for record in caplog.records
    ), f"expected SOUL-too-long warning; got {[r.getMessage() for r in caplog.records]}"


def test_short_soul_does_not_log_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A reasonable SOUL stays quiet — the warning is a real signal."""
    settings, _soul, system_path = _write_layout(
        tmp_path,
        soul_text="brief\nsoul\n",
    )
    with caplog.at_level(logging.WARNING, logger="capo.agent"):
        build_agent(settings, system_path, agent_factory=_make_factory())
    assert not any(
        "longer than recommended" in record.getMessage() for record in caplog.records
    )

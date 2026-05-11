"""Capo agent factory — SOUL + ops-prompt loader (spec §5.1, §9.1).

The Pydantic AI ``Agent`` for capo is constructed exactly once per process on
boot via :func:`build_agent`. The composite ``instructions=`` string is built
by concatenating the active SOUL file with the operational system prompt:

::

    <soul_text>\n\n<system_text>

Design notes:

* **Lazy imports.** ``pydantic_ai`` is imported only inside the default agent
  factory, so tests (and the ``--no-config`` boot path) can exercise this
  module without an LLM provider being configured.
* **Dependency-inject the agent factory.** Tests pass a fake callable that
  captures ``(model, instructions)`` so we can assert the composite contents
  without spinning up a real ``Agent``.
* **Errors are single-line and cite the offending path.** Missing SOUL,
  missing ``system.md``, and unreadable files all raise :class:`AgentBuildError`
  with a message suitable for printing verbatim to stderr.
* **SOUL length warning.** SOUL files longer than
  :data:`SOUL_RECOMMENDED_MAX_LINES` log a ``logging.warning`` per the spec
  edge-case table; boot continues normally.

This task does **not** wire the resulting agent into the dispatcher / DBOS
workflows — that lives in later tasks. We only land the loader + factory so
later tasks have a stable shape to import.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from capo.config import Settings
from capo.deps import CapoDeps

logger = logging.getLogger("capo.agent")

# Per spec §5.1 edge cases: SOUL files >50 lines log a "longer than
# recommended" warning at boot but do NOT block startup.
SOUL_RECOMMENDED_MAX_LINES: int = 50


class AgentBuildError(Exception):
    """Raised when the agent cannot be constructed at boot.

    The message is a single line that names the offending path or condition;
    callers should print it verbatim to stderr and exit non-zero (the same
    pattern :class:`capo.config.ConfigError` uses).
    """


class _AgentLike(Protocol):
    """Structural type for whatever the agent factory returns.

    We avoid importing ``pydantic_ai.Agent`` at module top so this module is
    safe to import in test environments without an LLM provider configured.
    The real type is ``pydantic_ai.Agent`` — see :func:`_default_agent_factory`.
    """


# Signature: (model, instructions, *, deps_type) -> AgentLike. ``deps_type``
# is keyword-only so callers from Task #9 that supplied a 2-arg factory keep
# working — :func:`build_agent` adapts old-style factories transparently.
AgentFactory = Callable[..., _AgentLike]


def _default_agent_factory(
    model: str,
    instructions: str,
    *,
    deps_type: type[Any] = CapoDeps,
) -> _AgentLike:
    """Lazy-import ``pydantic_ai.Agent`` and construct it.

    Kept as a separate function so the import cost (and provider-config
    requirement) is paid only when an agent is actually needed. The default
    ``deps_type`` is :class:`capo.deps.CapoDeps` so every Phase-1+ tool sees
    the canonical aggregate via ``RunContext[CapoDeps]``.
    """
    from pydantic_ai import Agent  # local import — see module docstring

    return Agent(  # type: ignore[return-value]
        model=model,
        instructions=instructions,
        deps_type=deps_type,
    )


def _read_text(path: Path, *, kind: str) -> str:
    """Read ``path`` as UTF-8; wrap any failure in :class:`AgentBuildError`.

    ``kind`` ("soul" / "system prompt") is interpolated into the error so the
    caller can tell at a glance which file is missing.
    """
    if not path.is_file():
        raise AgentBuildError(f"{kind} not found at {path}")
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise AgentBuildError(f"failed to read {kind} at {path}: {exc}") from exc


def _resolve_soul_path(settings: Settings) -> Path:
    """Compute the absolute SOUL file path from settings.

    ``settings.soul.dir`` is already resolved to an absolute path by
    :meth:`Settings.load` (relative dirs are resolved against the config
    file's parent). We just join the active filename.
    """
    return settings.soul.dir / f"{settings.soul.active}.md"


def build_composite_instructions(
    settings: Settings,
    system_prompt_path: Path,
) -> str:
    """Read SOUL + system prompt and return the composite ``instructions`` string.

    Pure / side-effect-free except for the SOUL-length warning. Useful as a
    primitive in tests and tools that need to inspect the rendered prompt
    without constructing a full ``Agent``.

    Returns
    -------
    str
        ``<soul_text>\\n\\n<system_text>``. No trailing newline normalization
        is performed beyond the literal ``\\n\\n`` join.

    Raises
    ------
    AgentBuildError
        On missing or unreadable SOUL / system-prompt files. The message
        always cites the offending path.
    """
    soul_path = _resolve_soul_path(settings)
    soul_text = _read_text(soul_path, kind="soul")
    system_text = _read_text(system_prompt_path, kind="system prompt")

    line_count = soul_text.count("\n") + (0 if soul_text.endswith("\n") else 1)
    if line_count > SOUL_RECOMMENDED_MAX_LINES:
        logger.warning(
            "SOUL is longer than recommended: %s has %d lines (>%d)",
            soul_path,
            line_count,
            SOUL_RECOMMENDED_MAX_LINES,
        )

    return f"{soul_text}\n\n{system_text}"


def build_agent(
    settings: Settings,
    system_prompt_path: Path,
    *,
    agent_factory: AgentFactory | None = None,
    deps_type: type[Any] = CapoDeps,
    register_tools: bool = True,
) -> Any:
    """Construct the Pydantic AI agent for capo from validated settings.

    Parameters
    ----------
    settings:
        Validated :class:`capo.config.Settings`. ``settings.models.default``
        supplies the model id (per spec §5.1 — runtime override is explicitly
        out of scope for V1).
    system_prompt_path:
        Absolute path to ``prompts/system.md``. We require the caller to
        compute this (typically ``config_path.parent / "prompts" / "system.md"``)
        because :class:`Settings` does not currently expose the originating
        config file path. Caller-provided paths also make this function
        trivially testable.
    agent_factory:
        Optional callable used to construct the underlying agent. Accepted
        signatures:

        * ``(model, instructions)`` — Task-9 era 2-arg factory; deps_type
          is silently dropped (used by existing tests).
        * ``(model, instructions, *, deps_type=...)`` — Task-14+ factory;
          deps_type is forwarded. The default lazy-import factory uses
          this signature.
    deps_type:
        Class shipped to ``Agent(deps_type=...)``. Defaults to
        :class:`capo.deps.CapoDeps`. Override only in tests that need a
        narrower / wider deps surface.
    register_tools:
        If ``True`` (default), register :mod:`capo.tools.basic` onto the
        constructed agent. Set ``False`` in tests that want to inspect a
        bare agent without any tools attached, or that inject a fake
        non-Agent factory return value.

    Returns
    -------
    pydantic_ai.Agent
        The constructed agent (typed as ``Any`` here so the import stays
        lazy — see module docstring).

    Raises
    ------
    AgentBuildError
        On missing SOUL or system prompt, or any other boot-time failure
        that prevents the composite instructions from being assembled.
    """
    factory: AgentFactory = agent_factory or _default_agent_factory
    instructions = build_composite_instructions(settings, system_prompt_path)

    # Backward-compat: support 2-arg factories from Task #9 by sniffing the
    # signature. Factories that don't accept ``deps_type`` get called the
    # old way; everyone else (including the default) gets ``deps_type``.
    try:
        sig = inspect.signature(factory)
        accepts_deps_type = "deps_type" in sig.parameters or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
    except (TypeError, ValueError):
        # C-extension factories or builtins — assume modern signature.
        accepts_deps_type = True

    if accepts_deps_type:
        agent = factory(settings.models.default, instructions, deps_type=deps_type)
    else:
        agent = factory(settings.models.default, instructions)

    if register_tools and hasattr(agent, "tool"):
        # Local import keeps capo.tools out of the import graph for the
        # Settings-only "is the config valid" boot path.
        from capo.tools.basic import register_basic_tools

        register_basic_tools(agent)

    return agent

"""Capo agent tools (spec §5.1, §9.1, §5.3-§5.5).

Each tool is a function annotated with ``RunContext[CapoDeps]`` as its
first argument so Pydantic AI injects shared deps automatically. Tools are
registered onto the agent inside :func:`capo.agent.build_agent` via the
``register_basic_tools`` + ``register_phase2_tools`` helpers exported
here so the registration order stays explicit and testable.

Tools intentionally do **not** import anything from
:mod:`capo.agent` — that would create a circular import. Registration is
top-down (agent.py imports tools/*.py), never the other way.

Phase split
-----------
* :func:`register_basic_tools` (spec §5.1): ``web_search``, ``fetch_url``.
  Always registered. Defined in :mod:`capo.tools.basic`.
* :func:`register_phase2_tools` (spec §5.3-§5.5, §9.2): ``shell_exec``,
  ``delegate_to_claude_code``, ``check_delegation_status``,
  ``get_delegation_output``, ``kill_delegation``, ``list_delegations``.
  Registered after the basic set so the model sees the full Phase-2
  toolbox. Each call to :func:`capo.agent.build_agent` constructs a
  fresh ``Agent`` instance, so re-importing this module never doubles
  up registrations on a long-lived agent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from capo.tools.basic import fetch_url, register_basic_tools, web_search

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pydantic_ai import Agent

    from capo.deps import CapoDeps


def register_phase2_tools(agent: Agent[CapoDeps, Any]) -> None:
    """Register the six Phase-2 tools onto ``agent`` (spec §5.3-§5.5, §9.2).

    Tools registered (in declaration order):

    1. :func:`capo.tools.basic.shell_exec` — allowlisted subprocess
       runner (§6.2). Raises :class:`~capo.tools.basic.ApprovalRequired`
       for any non-allowlisted invocation.
    2. :func:`capo.tools.claude_code.delegate_to_claude_code` — spawn a
       Claude Code subagent (§5.3).
    3. :func:`capo.tools.delegations.check_delegation_status` — status
       snapshot for a delegation (§5.5).
    4. :func:`capo.tools.delegations.get_delegation_output` — tail recent
       output chunks (§5.5).
    5. :func:`capo.tools.delegations.kill_delegation` — request kill;
       always raises :class:`ApprovalRequired` in Phase 2 (§5.8).
    6. :func:`capo.tools.delegations.list_delegations` — user-scoped
       recent delegations (§5.5, §5.11).

    The docstrings on each function double as the model-visible tool
    description (Pydantic AI exposes them verbatim). Keep them
    descriptive — they're the only signal the model gets about *when* to
    reach for each tool.

    Idempotency
    -----------
    This function decorates the supplied ``agent``; calling it twice on
    the *same* agent would register every tool twice (Pydantic AI raises
    on duplicate names). Production code never does that:
    :func:`capo.agent.build_agent` constructs a fresh ``Agent`` per call.
    Tests that want to exercise "re-import does not double-register"
    semantics rely on that factory invariant — see
    ``tests/test_agent_phase2_registration.py``.
    """
    # Local imports so importing `capo.tools` doesn't drag in subprocess,
    # sqlite3, and the worktree helpers when only the basic tools are
    # needed (e.g. the Settings-only boot path in `capo.main`).
    from capo.tools.basic import shell_exec
    from capo.tools.claude_code import delegate_to_claude_code
    from capo.tools.delegations import (
        check_delegation_status,
        get_delegation_output,
        kill_delegation,
        list_delegations,
    )

    agent.tool(shell_exec)
    agent.tool(delegate_to_claude_code)
    agent.tool(check_delegation_status)
    agent.tool(get_delegation_output)
    agent.tool(kill_delegation)
    agent.tool(list_delegations)


__all__ = [
    "fetch_url",
    "register_basic_tools",
    "register_phase2_tools",
    "web_search",
]

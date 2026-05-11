"""Capo agent tools (spec §5.1, §9.1).

Each tool is a function annotated with ``RunContext[CapoDeps]`` as its
first argument so Pydantic AI injects shared deps automatically. Tools are
registered onto the agent inside :func:`capo.agent.build_agent` via the
``register_basic_tools`` helper exported here so the registration order
stays explicit and testable.

Tools intentionally do **not** import anything from
:mod:`capo.agent` — that would create a circular import. Registration is
top-down (agent.py imports tools/basic.py), never the other way.
"""

from capo.tools.basic import fetch_url, register_basic_tools, web_search

__all__ = [
    "fetch_url",
    "register_basic_tools",
    "web_search",
]

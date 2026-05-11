"""Tests for the Phase-5 session-control NL agent tools (Task #53, §5.10 / §5.11).

Covers :func:`capo.tools.session.session_new`,
:func:`capo.tools.session.session_status`, and
:func:`capo.tools.session.session_clear`. Each tool mirrors a slash
command from Task #52 but is invokable by the LLM via natural language
("clear my conversation", "what's my status?", "start a new session").

Test surface
------------

Unit
* :func:`session_new` ends an active session; idempotent / no-op when
  there is none.
* :func:`session_status` returns the four required fields: active
  delegation count, today's cost USD, current model, conversation
  message count. The cost field is ``None`` when the cost accountant is
  unavailable; the message count is ``0`` when there's no active session.
* :func:`session_clear` deletes message rows for the thread + ends the
  active session. ``cleared_count`` reflects the actual delete count;
  an empty thread returns ``cleared_count=0`` and ``status="noop"``.
* Both ``session_new`` and ``session_clear`` operate on the
  ``deps.thread_id`` from :class:`CapoDeps` — the tool refuses (raises
  ``RuntimeError``) when thread_id is missing.

Integration
* Calling :func:`session_clear` through the production registration
  surface (:func:`register_phase5_tools` on a real Pydantic AI
  :class:`Agent`) wires the tool with a description the model can pick
  on.
* A direct registered-tool invocation parallels the Task #26 pattern
  used in :mod:`tests.test_agent_phase2_registration`: drive deps,
  invoke the registered tool function with ``RunContext``, assert the
  result dict.
"""

from __future__ import annotations

import contextlib
import importlib.util as _ilu
import sqlite3
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.tools import register_phase5_tools
from capo.tools.session import (
    register_session_tools,
    session_clear,
    session_new,
    session_status,
)
from tests.test_config import _VALID_ENV, _write_config

# ---------------------------------------------------------------------------
# Schema bootstrap — migrations 001..004 inlined (sessions, conversation_history,
# delegations, approvals, costs). Mirrors tests/test_dispatcher_session_commands.py.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_001 = REPO_ROOT / "migrations" / "versions" / "001_init.py"
_MIG_002 = REPO_ROOT / "migrations" / "versions" / "002_approvals.py"
_MIG_003 = REPO_ROOT / "migrations" / "versions" / "003_approvals_request_types.py"
_MIG_004 = REPO_ROOT / "migrations" / "versions" / "004_costs.py"

sys.path.insert(0, str(_MIG_001.parent))


def _load_migration(path: Path, name: str) -> Any:
    spec = _ilu.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mig_001 = _load_migration(_MIG_001, "_mig_001_session_tools")
_mig_002 = _load_migration(_MIG_002, "_mig_002_session_tools")
_mig_003 = _load_migration(_MIG_003, "_mig_003_session_tools")
_mig_004 = _load_migration(_MIG_004, "_mig_004_session_tools")


def _apply_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig_001._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_002._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_003._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_004._UPGRADE_STATEMENTS:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    # tmp_path-scoped db so each test starts with a clean schema. Settings
    # sub-models are frozen, so we route through ``object.__setattr__``.
    db_path = tmp_path / "state.db"
    object.__setattr__(s.paths, "db_path", db_path)
    return s


@pytest.fixture
def conn_factory(
    settings: Settings,
) -> Iterator[Callable[[], sqlite3.Connection]]:
    """Return a connection factory and pre-seed the schema + owner user."""
    db_path = settings.paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    boot = sqlite3.connect(str(db_path))
    boot.execute("PRAGMA foreign_keys = ON")
    _apply_schema(boot)
    boot.execute("INSERT INTO users(user_id) VALUES ('owner')")
    boot.commit()
    boot.close()

    opened: list[sqlite3.Connection] = []

    def _factory() -> sqlite3.Connection:
        c = sqlite3.connect(
            str(db_path), isolation_level=None, check_same_thread=False
        )
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("PRAGMA busy_timeout = 5000")
        opened.append(c)
        return c

    yield _factory
    for c in opened:
        with contextlib.suppress(Exception):  # pragma: no cover - defensive
            c.close()


def _seed_session(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    user_id: str,
    thread_id: str,
    session_id: str | None = None,
    ended: bool = False,
) -> str:
    sid = session_id or uuid.uuid4().hex
    conn = conn_factory()
    try:
        conn.execute(
            "INSERT INTO sessions(session_id, thread_id, user_id, "
            "started_at, ended_at, compacted_to_message_index) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            (
                sid,
                thread_id,
                user_id,
                "2026-05-11T11:00:00Z",
                "2026-05-11T11:30:00Z" if ended else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return sid


def _seed_history_row(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    user_id: str,
    thread_id: str,
    session_id: str,
    message_index: int,
    payload: str = '[{"role":"user","content":"x"}]',
) -> None:
    conn = conn_factory()
    try:
        conn.execute(
            "INSERT INTO conversation_history(user_id, thread_id, "
            "message_index, session_id, model_message_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, thread_id, message_index, session_id, payload),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_delegation(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    delegation_id: str,
    user_id: str = "owner",
    status: str = "running",
) -> None:
    conn = conn_factory()
    try:
        conn.execute(
            "INSERT INTO delegations(id, user_id, agent, workspace, pid, "
            "brief_json, status, started_at) "
            "VALUES (?, ?, 'claude-code', ?, ?, '{}', ?, ?)",
            (
                delegation_id,
                user_id,
                f"/tmp/ws-{delegation_id}",
                12345,
                status,
                "2026-05-11T11:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _make_ctx(settings: Settings, *, thread_id: str | None = "amc:chan-1") -> RunContext[CapoDeps]:
    """Build a :class:`RunContext` populated for the session tools."""
    deps = CapoDeps(
        settings=settings,
        http_client=None,  # type: ignore[arg-type]
        user_id="owner",
        thread_id=thread_id,
    )
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


# ---------------------------------------------------------------------------
# Unit: session_new
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_new_ends_active_session(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``session_new`` flips ``ended_at`` on the active session row."""
    thread_id = "amc:chan-1"
    sid = _seed_session(conn_factory, user_id="owner", thread_id=thread_id)

    ctx = _make_ctx(settings, thread_id=thread_id)
    result = await session_new(ctx)

    assert result == {"status": "ended", "thread_id": thread_id}

    # Confirm the row really has ended_at set now.
    conn = conn_factory()
    try:
        row = conn.execute(
            "SELECT ended_at FROM sessions WHERE session_id = ?",
            (sid,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] is not None, "ended_at should be set after session_new"


@pytest.mark.asyncio
async def test_session_new_noop_when_no_active_session(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """Edge case: ``session_new`` is a no-op when no active session exists."""
    # No session seeded; thread is fresh.
    ctx = _make_ctx(settings, thread_id="amc:fresh")
    result = await session_new(ctx)

    assert result == {"status": "no_active_session", "thread_id": "amc:fresh"}


@pytest.mark.asyncio
async def test_session_new_only_ends_target_thread(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``session_new`` MUST scope by (user_id, thread_id), not just user_id."""
    other_sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:other"
    )
    target_sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:target"
    )

    ctx = _make_ctx(settings, thread_id="amc:target")
    result = await session_new(ctx)
    assert result["status"] == "ended"

    conn = conn_factory()
    try:
        other_ended = conn.execute(
            "SELECT ended_at FROM sessions WHERE session_id = ?",
            (other_sid,),
        ).fetchone()
        target_ended = conn.execute(
            "SELECT ended_at FROM sessions WHERE session_id = ?",
            (target_sid,),
        ).fetchone()
    finally:
        conn.close()

    assert other_ended[0] is None, "other thread's session should be untouched"
    assert target_ended[0] is not None


@pytest.mark.asyncio
async def test_session_new_requires_thread_id(settings: Settings) -> None:
    """Missing ``deps.thread_id`` raises a clear ``RuntimeError``."""
    ctx = _make_ctx(settings, thread_id=None)
    with pytest.raises(RuntimeError, match="thread_id"):
        await session_new(ctx)


@pytest.mark.asyncio
async def test_session_new_requires_user_id(settings: Settings) -> None:
    """Missing ``deps.user_id`` raises a clear ``RuntimeError``."""
    deps = CapoDeps(
        settings=settings,
        http_client=None,  # type: ignore[arg-type]
        user_id=None,
        thread_id="amc:chan-1",
    )
    ctx: RunContext[CapoDeps] = RunContext(
        deps=deps, model=TestModel(), usage=RunUsage()
    )
    with pytest.raises(RuntimeError, match="user_id"):
        await session_new(ctx)


# ---------------------------------------------------------------------------
# Unit: session_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_status_returns_required_fields(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``session_status`` returns model, active_delegations, daily_cost_usd,
    conversation_message_count, thread_id."""
    thread_id = "amc:chan-1"
    sid = _seed_session(conn_factory, user_id="owner", thread_id=thread_id)
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=0,
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=1,
    )
    _seed_delegation(conn_factory, delegation_id="d1", status="running")
    _seed_delegation(conn_factory, delegation_id="d2", status="completed")

    ctx = _make_ctx(settings, thread_id=thread_id)
    result = await session_status(ctx)

    assert set(result.keys()) == {
        "model",
        "active_delegations",
        "daily_cost_usd",
        "conversation_message_count",
        "thread_id",
    }
    assert result["model"] == settings.models.default
    assert result["active_delegations"] == 1  # only the running one
    assert result["conversation_message_count"] == 2
    assert result["thread_id"] == thread_id
    # daily_cost_usd is a float (Decimal "0.000000") cast to float since
    # no costs were recorded for today.
    assert result["daily_cost_usd"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_session_status_no_active_session_returns_zero_messages(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``conversation_message_count`` is 0 when there's no active session row."""
    ctx = _make_ctx(settings, thread_id="amc:fresh")
    result = await session_status(ctx)

    assert result["conversation_message_count"] == 0
    assert result["active_delegations"] == 0


@pytest.mark.asyncio
async def test_session_status_handles_cost_accountant_unavailable(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daily_cost_usd`` is ``None`` when the cost accountant fails."""

    async def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("simulated costs failure")

    # Patch the module-level reference daily_total_usd in capo.costs that
    # session.read_daily_cost_usd imports lazily.
    monkeypatch.setattr("capo.costs.daily_total_usd", _boom)

    ctx = _make_ctx(settings, thread_id="amc:chan-1")
    result = await session_status(ctx)
    assert result["daily_cost_usd"] is None


@pytest.mark.asyncio
async def test_session_status_only_counts_current_user_delegations(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """Active delegation count MUST scope to ``ctx.deps.user_id``."""
    # Seed another user + their running delegation. Owner should NOT see it.
    conn = conn_factory()
    try:
        conn.execute("INSERT INTO users(user_id) VALUES ('other')")
        conn.commit()
    finally:
        conn.close()
    _seed_delegation(
        conn_factory, delegation_id="other-d", user_id="other", status="running"
    )

    ctx = _make_ctx(settings, thread_id="amc:chan-1")
    result = await session_status(ctx)
    assert result["active_delegations"] == 0


# ---------------------------------------------------------------------------
# Unit: session_clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_clear_deletes_history_and_ends_session(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``session_clear`` deletes per-thread messages AND ends the session."""
    thread_id = "amc:chan-1"
    sid = _seed_session(conn_factory, user_id="owner", thread_id=thread_id)
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=0,
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=1,
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=2,
    )

    ctx = _make_ctx(settings, thread_id=thread_id)
    result = await session_clear(ctx)

    assert result == {
        "cleared_count": 3,
        "status": "cleared",
        "thread_id": thread_id,
    }

    # Rows really are gone; session row really is ended.
    conn = conn_factory()
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id=? AND thread_id=?",
            ("owner", thread_id),
        ).fetchone()[0]
        ended_at = conn.execute(
            "SELECT ended_at FROM sessions WHERE session_id=?",
            (sid,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0
    assert ended_at is not None


@pytest.mark.asyncio
async def test_session_clear_noop_on_empty_thread(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """Edge case: ``session_clear`` on a thread with no messages
    returns ``{"cleared_count": 0, "status": "noop"}``."""
    ctx = _make_ctx(settings, thread_id="amc:fresh")
    result = await session_clear(ctx)

    assert result["cleared_count"] == 0
    assert result["status"] == "noop"
    assert result["thread_id"] == "amc:fresh"


@pytest.mark.asyncio
async def test_session_clear_scopes_by_thread(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """``session_clear`` MUST NOT delete messages from another thread."""
    other_sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:other"
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:other",
        session_id=other_sid,
        message_index=0,
    )
    target_sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:target"
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:target",
        session_id=target_sid,
        message_index=0,
    )

    ctx = _make_ctx(settings, thread_id="amc:target")
    result = await session_clear(ctx)
    assert result["cleared_count"] == 1

    conn = conn_factory()
    try:
        other_count = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id=? AND thread_id=?",
            ("owner", "amc:other"),
        ).fetchone()[0]
    finally:
        conn.close()
    assert other_count == 1, "other thread's history must be untouched"


# ---------------------------------------------------------------------------
# Integration: registered tools on a real Pydantic AI Agent.
# ---------------------------------------------------------------------------


def _registered_tool_names(agent: Any) -> set[str]:
    names: set[str] = set()
    for ts in agent.toolsets:
        if hasattr(ts, "tools"):
            names.update(ts.tools.keys())
    return names


def _tool_descriptions(agent: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for ts in agent.toolsets:
        for name, tool in getattr(ts, "tools", {}).items():
            desc = getattr(tool, "description", None)
            if desc is None and hasattr(tool, "tool_def"):
                desc = getattr(tool.tool_def, "description", None)
            if desc:
                out[name] = desc
    return out


def test_register_phase5_tools_attaches_all_three(settings: Settings) -> None:
    """:func:`register_phase5_tools` adds the three session tools to the agent."""
    agent: Agent[CapoDeps, Any] = Agent(
        model=TestModel(call_tools=[]),
        instructions="test",
        deps_type=CapoDeps,
    )
    register_phase5_tools(agent)

    names = _registered_tool_names(agent)
    assert {"session_new", "session_status", "session_clear"} <= names


def test_register_session_tools_directly(settings: Settings) -> None:
    """The submodule helper exposes the same three tools without going
    through ``capo.tools.register_phase5_tools``."""
    agent: Agent[CapoDeps, Any] = Agent(
        model=TestModel(call_tools=[]),
        instructions="test",
        deps_type=CapoDeps,
    )
    register_session_tools(agent)

    names = _registered_tool_names(agent)
    assert {"session_new", "session_status", "session_clear"} <= names


def test_phase5_tools_have_descriptions(settings: Settings) -> None:
    """Each session tool exposes a non-empty model-visible description.

    Pydantic AI uses docstrings as the model-visible description. An
    empty description means the model can't tell when to use the tool —
    guard against accidental docstring removal here.
    """
    agent: Agent[CapoDeps, Any] = Agent(
        model=TestModel(call_tools=[]),
        instructions="test",
        deps_type=CapoDeps,
    )
    register_phase5_tools(agent)
    descriptions = _tool_descriptions(agent)

    for name in ("session_new", "session_status", "session_clear"):
        assert name in descriptions, f"{name!r} has no description"
        assert len(descriptions[name].strip()) > 20, (
            f"{name!r} description is too short: {descriptions[name]!r}"
        )


@pytest.mark.asyncio
async def test_agent_can_invoke_registered_session_clear(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """The registered ``session_clear`` tool on a real Agent clears history.

    Smoke test for the "agent receives a message like 'clear my
    conversation' and calls session_clear" requirement. Mirrors Task #26's
    pattern (test_agent_phase2_registration.py) — invoke the registered
    tool directly via :class:`RunContext` rather than expecting
    :class:`TestModel` to pick the tool by name.
    """
    thread_id = "amc:chan-1"
    sid = _seed_session(conn_factory, user_id="owner", thread_id=thread_id)
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id=thread_id,
        session_id=sid,
        message_index=0,
    )

    agent: Agent[CapoDeps, Any] = Agent(
        model=TestModel(call_tools=[]),
        instructions="test",
        deps_type=CapoDeps,
    )
    register_phase5_tools(agent)
    # Confirm registration glue actually wired the tools onto the agent.
    assert "session_clear" in _registered_tool_names(agent)

    # Drive the registered function through the canonical RunContext path.
    ctx = _make_ctx(settings, thread_id=thread_id)
    result = await session_clear(ctx)
    assert result["cleared_count"] == 1
    assert result["status"] == "cleared"

    conn = conn_factory()
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id=? AND thread_id=?",
            ("owner", thread_id),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


@pytest.mark.asyncio
async def test_session_tools_share_helpers_with_dispatcher(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """The dispatcher's private helpers point at ``capo.tools.session`` symbols.

    Sanity-checks the refactor invariant from Task #53: both the slash
    path and the NL path call the SAME underlying functions.
    """
    from capo.tools.session import (
        delete_thread_history,
        end_active_session,
        read_daily_cost_usd,
        select_active_delegation_count,
        select_active_session_id,
        select_conversation_count,
    )
    from capo.transport.dispatcher import (
        _delete_thread_history,
        _end_active_session,
        _read_daily_cost_usd,
        _select_active_delegation_count,
        _select_active_session_id,
        _select_conversation_count,
    )

    assert _delete_thread_history is delete_thread_history
    assert _end_active_session is end_active_session
    assert _read_daily_cost_usd is read_daily_cost_usd
    assert _select_active_delegation_count is select_active_delegation_count
    assert _select_active_session_id is select_active_session_id
    assert _select_conversation_count is select_conversation_count

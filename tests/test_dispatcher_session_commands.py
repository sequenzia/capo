"""Tests for dispatcher session-control slash commands (Task #52, §5.10).

Covers ``/new``, ``/status``, ``/clear``, ``/kill <delegation_id>``,
``/override`` plus the unknown-slash-verb fallback. Mirrors the fakes and
schema-bootstrap patterns from
:mod:`tests.test_dispatcher_approval_routing` so the two test files share
operational shape.

Key invariants the tests pin:

* Each command consumes ZERO LLM tokens (the fake agent is never invoked).
* Each command's side effect is observable in ``state.db`` and/or the
  worker's in-memory sentinel.
* Replies are emitted via AMC + mark_read fires.
* Edge cases: missing/bad ``delegation_id`` on ``/kill``, empty thread on
  ``/clear``, unknown slash verb.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util as _ilu
import sqlite3
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from capo.config import Settings
from capo.deps import CapoDeps
from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.dispatcher import (
    SESSION_REPLY_CLEARED,
    SESSION_REPLY_CLEARED_EMPTY,
    SESSION_REPLY_KILL_MISSING_ID,
    SESSION_REPLY_KILL_UNKNOWN,
    SESSION_REPLY_NEW,
    SESSION_REPLY_OVERRIDE_ARMED,
    ChannelWorker,
    Dispatcher,
    format_kill_reply,
    format_status_reply,
    format_unknown_command_reply,
)
from tests.test_config import _VALID_ENV, _write_config

# ---------------------------------------------------------------------------
# Schema bootstrap — migrations 001 + 002 + 003 + 004 inlined.
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


_mig_001 = _load_migration(_MIG_001, "_mig_001_session_cmds")
_mig_002 = _load_migration(_MIG_002, "_mig_002_session_cmds")
_mig_003 = _load_migration(_MIG_003, "_mig_003_session_cmds")
_mig_004 = _load_migration(_MIG_004, "_mig_004_session_cmds")


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
# Fakes — minimal copies of the patterns in tests/test_dispatcher.py.
# ---------------------------------------------------------------------------


class _FakeAgent:
    """Fake agent: records every invocation so tests assert "agent NOT called"."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Any], CapoDeps]] = []

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        self.calls.append((text, list(message_history or []), deps))
        return _FakeRunResult(f"agent:{text}", [])


class _FakeRunResult:
    def __init__(self, output: str, new_messages: list[Any]) -> None:
        self.output = output
        self._new_messages = new_messages

    def new_messages(self) -> list[Any]:
        return list(self._new_messages)


class _FakeAMCClient:
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
        return {
            "message_id": f"out-{len(self.send_calls)}",
            "channel_id": channel_id,
        }

    async def mark_read(self, message_ids: Any) -> Any:
        ids = list(message_ids)
        self.mark_read_calls.append(ids)
        return {"marked_read": ids, "already_read": []}


def _envelope(
    *,
    message_id: str,
    channel_id: str,
    text: str,
    sender_id: str = "+15551234567",
) -> AMCInboundEnvelope:
    return AMCInboundEnvelope(
        id=message_id,
        channel_id=channel_id,
        sender_id=sender_id,
        text=text,
        ts="2026-05-11T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    # Redirect ``paths.db_path`` to a tmp_path-scoped file so concurrent
    # tests don't share the real ``~/.capo/state.db`` (and so each test
    # starts with a clean schema). ``Settings`` sub-models are frozen, so
    # we go through ``object.__setattr__``.
    db_path = tmp_path / "state.db"
    object.__setattr__(s.paths, "db_path", db_path)
    return s


@pytest.fixture
def conn_factory(
    tmp_path: Path, settings: Settings
) -> Iterator[Callable[[], sqlite3.Connection]]:
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
        with contextlib.suppress(Exception):
            c.close()


def _fake_deps_factory(settings: Settings) -> Callable[[str], CapoDeps]:
    def _f(user_id: str) -> CapoDeps:
        return CapoDeps(
            settings=settings,
            http_client=None,  # type: ignore[arg-type]
            user_id=user_id,
        )

    return _f


async def _drain(worker: ChannelWorker) -> None:
    await asyncio.wait_for(worker._queue.join(), timeout=5.0)


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
    pid: int | None = None,
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
                pid,
                status,
                "2026-05-11T11:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# /new
# ---------------------------------------------------------------------------


async def test_new_ends_active_session_and_replies(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:c1"
    )
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-new-1", channel_id="c1", text="/new")
        )
        worker = dispatcher._workers["c1"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_NEW
    assert amc.mark_read_calls == [["m-new-1"]]

    # The active session row should now have ended_at populated.
    conn = conn_factory()
    try:
        row = conn.execute(
            "SELECT ended_at FROM sessions WHERE session_id = ?", (sid,)
        ).fetchone()
    finally:
        conn.close()
    assert row is not None and row[0] is not None


async def test_new_with_no_active_session_still_replies(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    # No active session — /new is a no-op on the DB side but still acks.
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-new-2", channel_id="c2", text="/new")
        )
        worker = dispatcher._workers["c2"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_NEW


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------


async def test_status_reports_active_delegations_and_messages(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:cS"
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:cS",
        session_id=sid,
        message_index=0,
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:cS",
        session_id=sid,
        message_index=1,
    )
    _seed_delegation(
        conn_factory, delegation_id=uuid.uuid4().hex, status="running"
    )
    _seed_delegation(
        conn_factory, delegation_id=uuid.uuid4().hex, status="running"
    )
    # A non-running delegation that MUST be excluded from the count.
    _seed_delegation(
        conn_factory, delegation_id=uuid.uuid4().hex, status="completed"
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-stat-1", channel_id="cS", text="/status")
        )
        worker = dispatcher._workers["cS"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    reply = amc.send_calls[0]["text"]
    assert "active delegations: 2" in reply
    assert "conversation messages: 2" in reply
    assert f"model: {settings.models.default}" in reply
    # Cost is read from the cost accountant: with no costs rows recorded
    # the accountant returns 0.000000 (NOT ``None``).
    assert "today's cost: $0.0000" in reply
    assert amc.mark_read_calls == [["m-stat-1"]]


async def test_status_reports_zero_when_thread_empty(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-stat-2", channel_id="cT", text="/status")
        )
        worker = dispatcher._workers["cT"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    reply = amc.send_calls[0]["text"]
    assert "active delegations: 0" in reply
    # No active session yet → conversation messages renders as "0".
    assert "conversation messages: 0" in reply


def test_format_status_reply_renders_unavailable_when_cost_none() -> None:
    out = format_status_reply(
        model="claude-sonnet-4-5",
        active_delegations=0,
        daily_cost_usd=None,
        conversation_message_count=0,
    )
    assert "today's cost: unavailable" in out


# ---------------------------------------------------------------------------
# /clear
# ---------------------------------------------------------------------------


async def test_clear_deletes_rows_and_replies_cleared(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    sid = _seed_session(
        conn_factory, user_id="owner", thread_id="amc:cC"
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:cC",
        session_id=sid,
        message_index=0,
    )
    _seed_history_row(
        conn_factory,
        user_id="owner",
        thread_id="amc:cC",
        session_id=sid,
        message_index=1,
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-clr-1", channel_id="cC", text="/clear")
        )
        worker = dispatcher._workers["cC"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_CLEARED
    assert amc.mark_read_calls == [["m-clr-1"]]

    conn = conn_factory()
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id='owner' AND thread_id='amc:cC'"
        ).fetchone()
    finally:
        conn.close()
    assert rows is not None and rows[0] == 0


async def test_clear_empty_thread_replies_empty_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-clr-2", channel_id="cE", text="/clear")
        )
        worker = dispatcher._workers["cE"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_CLEARED_EMPTY


# ---------------------------------------------------------------------------
# /kill
# ---------------------------------------------------------------------------


async def test_kill_missing_id_replies_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-kill-1", channel_id="cK", text="/kill")
        )
        worker = dispatcher._workers["cK"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_KILL_MISSING_ID


async def test_kill_bad_id_replies_unknown(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-kill-2",
                channel_id="cK2",
                text="/kill not-hex",
            )
        )
        worker = dispatcher._workers["cK2"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_KILL_UNKNOWN


async def test_kill_unknown_id_replies_unknown(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    # A valid hex shape but no row in the DB.
    bogus = uuid.uuid4().hex
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-kill-3",
                channel_id="cK3",
                text=f"/kill {bogus}",
            )
        )
        worker = dispatcher._workers["cK3"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_KILL_UNKNOWN


async def test_kill_already_terminal_replies_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    delegation_id = uuid.uuid4().hex
    _seed_delegation(
        conn_factory,
        delegation_id=delegation_id,
        user_id="owner",
        status="completed",
    )
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-kill-4",
                channel_id="cK4",
                text=f"/kill {delegation_id}",
            )
        )
        worker = dispatcher._workers["cK4"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    reply = amc.send_calls[0]["text"]
    assert "already terminal" in reply
    assert delegation_id in reply


async def test_kill_owner_kills_running_delegation(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegation_id = uuid.uuid4().hex
    # Owner deps come back from _fake_deps_factory with ``user_id='owner'``
    # because resolve_user_id maps the canonical sender to that id (see
    # tests.test_config._VALID_ENV). The delegation row is owned by
    # ``owner`` so kill proceeds directly without approval.
    _seed_delegation(
        conn_factory,
        delegation_id=delegation_id,
        user_id="owner",
        status="running",
        pid=None,  # nothing to actually signal
    )

    # Skip the SIGTERM/SIGKILL escalation (no real pid).
    import capo.tools.delegations as delegations_mod

    monkeypatch.setattr(
        delegations_mod, "_signal_with_escalation", lambda _pid: None
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-kill-5",
                channel_id="cK5",
                text=f"/kill {delegation_id}",
            )
        )
        worker = dispatcher._workers["cK5"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    reply = amc.send_calls[0]["text"]
    assert "killed" in reply
    assert delegation_id in reply

    # Row is now terminal.
    conn = conn_factory()
    try:
        row = conn.execute(
            "SELECT status, summary FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "killed"
    assert row[1] == "user requested via /kill"


def test_format_kill_reply_killed_with_cascade() -> None:
    out = format_kill_reply(
        delegation_id="abc123",
        result={"status": "killed", "cascaded_approvals": 2},
    )
    assert "killed delegation abc123" in out
    assert "2 tied approvals" in out


def test_format_kill_reply_killed_no_cascade() -> None:
    out = format_kill_reply(
        delegation_id="abc123",
        result={"status": "killed", "cascaded_approvals": 0},
    )
    assert out == "killed delegation abc123"


# ---------------------------------------------------------------------------
# /override
# ---------------------------------------------------------------------------


async def test_override_arms_cost_override_sentinel(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-ov-1", channel_id="cOV", text="/override"
            )
        )
        worker = dispatcher._workers["cOV"]
        await _drain(worker)

        # Sentinel should be armed on the per-channel worker.
        assert worker.is_cost_override_armed("amc:cOV") is True

        # Consume the sentinel (Task #49 will call this from its cap check).
        assert worker.consume_cost_override("amc:cOV") is True
        # Auto-clears on consume.
        assert worker.is_cost_override_armed("amc:cOV") is False
        assert worker.consume_cost_override("amc:cOV") is False
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == SESSION_REPLY_OVERRIDE_ARMED


# ---------------------------------------------------------------------------
# Unknown slash verb fallback.
# ---------------------------------------------------------------------------


async def test_unknown_slash_verb_replies_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-unk-1", channel_id="cU", text="/frobnicate"
            )
        )
        worker = dispatcher._workers["cU"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    reply = amc.send_calls[0]["text"]
    assert reply == format_unknown_command_reply("frobnicate")
    assert "/new" in reply
    assert "/status" in reply
    assert "/clear" in reply
    assert "/kill" in reply
    assert "/override" in reply
    assert "/approve" in reply
    assert "/deny" in reply


async def test_non_slash_message_flows_to_agent(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """Regression: natural-language input still flows through to the agent."""
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-nl-1",
                channel_id="cN",
                text="hello there, please help",
            )
        )
        worker = dispatcher._workers["cN"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert len(agent.calls) == 1
    assert agent.calls[0][0] == "hello there, please help"


# ---------------------------------------------------------------------------
# Case insensitivity (acceptance criterion).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected_reply",
    [
        ("/NEW", SESSION_REPLY_NEW),
        ("/Clear", SESSION_REPLY_CLEARED_EMPTY),
        ("/Override", SESSION_REPLY_OVERRIDE_ARMED),
    ],
)
async def test_case_insensitive_verbs(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    text: str,
    expected_reply: str,
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id=f"m-ci-{hash(text)}",
                channel_id=f"cCI{abs(hash(text)) % 10000}",
                text=text,
            )
        )
        # We don't know the channel until we look it up; the worker map
        # has at most one entry per test.
        worker = next(iter(dispatcher._workers.values()))
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert amc.send_calls[0]["text"] == expected_reply

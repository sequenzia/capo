"""Dispatcher integration tests for the pre-agent budget hook (Task #49).

Verifies the §5.9 acceptance criteria end-to-end through the dispatcher:

* ``check_budget`` is called BEFORE ``agent.run`` for every envelope.
* ``hard_block`` path → friendly refusal reply, agent NEVER invoked.
* ``soft_warn`` path → agent invoked, warning prepended to reply.
* ``overridden`` path → ``/override`` sentinel consumed, agent invoked,
  override-note prepended to reply.
* ``ok`` path → agent invoked, no prefix added (unchanged reply text).

Mirrors the fakes / schema-bootstrap patterns from
:mod:`tests.test_dispatcher_session_commands`.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util as _ilu
import sqlite3
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from capo.config import Settings
from capo.deps import CapoDeps
from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.dispatcher import (
    ChannelWorker,
    Dispatcher,
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


_mig_001 = _load_migration(_MIG_001, "_mig_001_budget_hook")
_mig_002 = _load_migration(_MIG_002, "_mig_002_budget_hook")
_mig_003 = _load_migration(_MIG_003, "_mig_003_budget_hook")
_mig_004 = _load_migration(_MIG_004, "_mig_004_budget_hook")


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
# Fakes
# ---------------------------------------------------------------------------


class _FakeAgent:
    """Records every invocation so we can assert "agent NOT called"."""

    def __init__(self, reply: str = "hello from agent") -> None:
        self.calls: list[tuple[str, list[Any], CapoDeps]] = []
        self._reply = reply

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        self.calls.append((text, list(message_history or []), deps))
        return _FakeRunResult(self._reply, [])


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
        return {"message_id": f"out-{len(self.send_calls)}", "channel_id": channel_id}

    async def mark_read(self, message_ids: Any) -> Any:
        ids = list(message_ids)
        self.mark_read_calls.append(ids)
        return {"marked_read": ids, "already_read": []}


def _envelope(
    *,
    message_id: str,
    channel_id: str,
    text: str = "what time is it?",
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
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Real Settings with budget soft=25, hard=75 (from test_config TOML)."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
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


def _seed_spend(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    user_id: str,
    total_usd: float,
    day_iso: str = "2026-05-11",
) -> None:
    """Insert a single cost row so ``daily_total_usd`` returns ``total_usd``."""
    conn = conn_factory()
    try:
        conn.execute(
            "INSERT INTO costs(recorded_at, user_id, parent_thread_id, model, "
            "input_tokens, output_tokens, cached_tokens, cost_usd, span_id) "
            "VALUES (?, ?, ?, 'claude-opus-4-7', 0, 0, 0, ?, NULL)",
            (f"{day_iso}T12:00:00Z", user_id, "amc:test", total_usd),
        )
        conn.commit()
    finally:
        conn.close()


def _set_today(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin ``datetime.now(UTC)`` to 2026-05-11 inside capo.budget.

    The TOML test caps are soft=25 / hard=75 and we seed costs at the
    matching ``recorded_at`` date so the dispatcher's hook sees them.
    """
    from datetime import UTC, datetime

    import capo.budget as budget_mod

    fixed = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

    def _fake_today() -> str:
        return fixed.strftime("%Y-%m-%d")

    monkeypatch.setattr(budget_mod, "_utc_today_iso", _fake_today)


# ---------------------------------------------------------------------------
# Test: ok path — agent invoked normally, no prefix
# ---------------------------------------------------------------------------


async def test_under_soft_cap_agent_runs_normally(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_today(settings, monkeypatch)
    # Below soft cap ($25): seed $10.
    _seed_spend(conn_factory, user_id="owner", total_usd=10.0)

    agent = _FakeAgent(reply="agent answer")
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
            _envelope(message_id="m-ok-1", channel_id="cOK", text="hi")
        )
        worker = dispatcher._workers["cOK"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    # Agent WAS called.
    assert len(agent.calls) == 1
    # Reply is the bare agent output, no budget prefix.
    assert amc.send_calls[0]["text"] == "agent answer"


# ---------------------------------------------------------------------------
# Test: soft_warn path — agent invoked, warning prepended
# ---------------------------------------------------------------------------


async def test_soft_cap_crossed_prepends_warning(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_today(settings, monkeypatch)
    # At or above soft cap ($25) but below hard ($75): seed $30.
    _seed_spend(conn_factory, user_id="owner", total_usd=30.0)

    agent = _FakeAgent(reply="agent answer")
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
            _envelope(message_id="m-sw-1", channel_id="cSW", text="hi")
        )
        worker = dispatcher._workers["cSW"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    # Agent WAS called.
    assert len(agent.calls) == 1
    # Reply has the soft-warn prefix prepended, then the agent reply.
    reply = amc.send_calls[0]["text"]
    assert "$30.00" in reply
    assert "$25.00" in reply
    assert "agent answer" in reply
    # Prefix appears BEFORE agent text.
    assert reply.index("$30.00") < reply.index("agent answer")


# ---------------------------------------------------------------------------
# Test: hard_block path — agent NOT invoked, friendly refusal reply
# ---------------------------------------------------------------------------


async def test_hard_cap_reached_refuses_turn(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_today(settings, monkeypatch)
    # At or above hard cap ($75): seed $100.
    _seed_spend(conn_factory, user_id="owner", total_usd=100.0)

    agent = _FakeAgent(reply="agent answer")
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
            _envelope(message_id="m-hb-1", channel_id="cHB", text="hi")
        )
        worker = dispatcher._workers["cHB"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    # Agent was NOT called.
    assert agent.calls == []

    # Friendly refusal reply was sent.
    assert len(amc.send_calls) == 1
    reply = amc.send_calls[0]["text"]
    assert "$100.00" in reply  # daily total named
    assert "$75.00" in reply  # cap named
    assert "/override" in reply  # suggestion present

    # The envelope was still marked-read so AMC stops redelivering.
    assert amc.mark_read_calls == [["m-hb-1"]]


# ---------------------------------------------------------------------------
# Test: overridden path — /override consumes sentinel, agent invoked
# ---------------------------------------------------------------------------


async def test_override_armed_consumes_sentinel_and_lets_turn_through(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_today(settings, monkeypatch)
    _seed_spend(conn_factory, user_id="owner", total_usd=100.0)  # past hard

    agent = _FakeAgent(reply="agent answer")
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
        # First envelope: arm /override via slash command on this thread.
        await dispatcher.enqueue(
            _envelope(message_id="m-arm-1", channel_id="cOR", text="/override")
        )
        worker = dispatcher._workers["cOR"]
        await _drain(worker)
        assert worker.is_cost_override_armed("amc:cOR") is True

        # Now send a real message — hard cap WOULD block, but override
        # is armed → agent invoked, sentinel consumed.
        await dispatcher.enqueue(
            _envelope(message_id="m-or-1", channel_id="cOR", text="hi")
        )
        await _drain(worker)

        # Snapshot the sentinel state BEFORE dispatcher.stop() clears
        # the workers map.
        override_after_turn = worker.is_cost_override_armed("amc:cOR")
    finally:
        await dispatcher.stop()

    # Agent WAS called (exactly once — /override itself is a slash
    # command and skips the agent path).
    assert len(agent.calls) == 1

    # Sentinel has been consumed by the budget hook.
    assert override_after_turn is False

    # Both replies sent: the /override ack, then the override-prefixed
    # agent reply.
    assert len(amc.send_calls) == 2
    ack_reply = amc.send_calls[0]["text"]
    turn_reply = amc.send_calls[1]["text"]
    assert "override" in ack_reply.lower()
    assert "agent answer" in turn_reply
    # Prefix carries the override note.
    assert "$100.00" in turn_reply
    assert "$75.00" in turn_reply


# ---------------------------------------------------------------------------
# Test: override only consumed on hard-block — soft warn leaves it armed
# ---------------------------------------------------------------------------


async def test_override_not_consumed_on_soft_warn_only(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_today(settings, monkeypatch)
    # Soft-only crossing: $30 ≥ $25 soft, < $75 hard.
    _seed_spend(conn_factory, user_id="owner", total_usd=30.0)

    agent = _FakeAgent(reply="agent answer")
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
        # Arm the override pre-emptively.
        await dispatcher.enqueue(
            _envelope(message_id="m-arm-2", channel_id="cSWO", text="/override")
        )
        worker = dispatcher._workers["cSWO"]
        await _drain(worker)
        assert worker.is_cost_override_armed("amc:cSWO") is True

        # Send a soft-warn-triggering envelope.
        await dispatcher.enqueue(
            _envelope(message_id="m-sw-2", channel_id="cSWO", text="hi")
        )
        await _drain(worker)

        # Snapshot before dispatcher.stop() clears the worker map.
        override_after_turn = worker.is_cost_override_armed("amc:cSWO")
    finally:
        await dispatcher.stop()

    # Override sentinel should STILL be armed (not consumed on soft warn).
    assert override_after_turn is True

    # The reply should carry the soft-warn prefix (not the override one).
    turn_reply = amc.send_calls[-1]["text"]
    assert "agent answer" in turn_reply
    assert "$30.00" in turn_reply
    # Soft warn names the soft cap.
    assert "$25.00" in turn_reply


# ---------------------------------------------------------------------------
# Test: budget check runs BEFORE agent.run (verify ordering)
# ---------------------------------------------------------------------------


async def test_hard_block_does_not_touch_agent_or_conversation_memory(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stronger assertion: on hard-block, NO conversation_history row
    is written either (no message_history materialization)."""
    _set_today(settings, monkeypatch)
    _seed_spend(conn_factory, user_id="owner", total_usd=200.0)

    agent = _FakeAgent(reply="agent answer")
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
            _envelope(message_id="m-block", channel_id="cBL", text="hi")
        )
        worker = dispatcher._workers["cBL"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []

    # No conversation_history row written.
    conn = conn_factory()
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id = 'owner' AND thread_id = 'amc:cBL'"
        ).fetchone()
    finally:
        conn.close()
    assert rows is not None and rows[0] == 0


# ---------------------------------------------------------------------------
# Test: fail-open path — DB missing costs table doesn't wedge dispatcher
# ---------------------------------------------------------------------------


async def test_costs_table_missing_fails_open(
    settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the costs table is missing (legacy DB), the budget hook
    fails-open and the agent runs as usual."""
    _set_today(settings, monkeypatch)

    db_path = settings.paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    boot = sqlite3.connect(str(db_path))
    boot.execute("PRAGMA foreign_keys = ON")
    # Apply migrations 001, 002, 003 ONLY — skip 004 so ``costs`` is missing.
    for stmt in _mig_001._UPGRADE_STATEMENTS:
        boot.execute(stmt)
    for stmt in _mig_002._UPGRADE_STATEMENTS:
        boot.execute(stmt)
    for stmt in _mig_003._UPGRADE_STATEMENTS:
        boot.execute(stmt)
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

    agent = _FakeAgent(reply="agent answer")
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        _factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m-fo-1", channel_id="cFO", text="hi")
        )
        worker = dispatcher._workers["cFO"]
        await _drain(worker)
    finally:
        await dispatcher.stop()
        for c in opened:
            with contextlib.suppress(Exception):
                c.close()

    # Agent ran despite the costs-table miss → fail-open.
    assert len(agent.calls) == 1
    # No prefix on the reply (the hook returned ``ok``).
    assert amc.send_calls[0]["text"] == "agent answer"

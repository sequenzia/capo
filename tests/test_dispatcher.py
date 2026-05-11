"""Tests for :mod:`capo.transport.dispatcher` (Task #11).

Covers spec §5.2 / §5.11 / §9.1 / §15.3 acceptance criteria:

* **Per-channel ordering.** Envelopes for the same ``channel_id`` are
  processed in arrival order.
* **Parallel channels.** Different channels do not block each other — a
  slow turn on channel A does not stall channel B.
* **Unknown sender rejection.** Senders not in ``[users]`` are dropped
  with a structured warning log; the message is still marked-read; the
  agent is NOT invoked.
* **Worker crash recovery.** An exception inside ``agent.run`` results in
  an error reply being sent + the inbound message being marked-read; the
  worker stays alive for the next envelope.
* **Queue-depth bound.** Once ``concurrency.queue_depth_max`` is reached,
  additional envelopes are dropped with a logged warning.
* **AMC ``RATE_LIMITED``** is handled inside :class:`AMCClient`; the
  dispatcher does not retry on top. The dispatcher gets called once per
  send and the AMCClient retry policy stays sole owner of retry semantics.
* **AMC ``PLATFORM_AUTH``** → log + no retry + best-effort fallback reply.
* **AMC ``CHANNEL_NOT_FOUND``** → log + no retry + no fallback reply
  (channel is gone).

The agent and AMC client are both faked so tests run without an LLM
provider or a live AMC. The store connection is a real in-memory SQLite
seeded with the §7.3 DDL via the same migration-import pattern used by
``tests/test_conversation.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util as _ilu
import logging
import sqlite3
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from capo.config import Settings
from capo.deps import CapoDeps
from capo.transport.amc_client import (
    AMCInboundEnvelope,
    ChannelNotFound,
    PlatformAuth,
    RateLimited,
)
from capo.transport.dispatcher import (
    CHANNEL_NOT_FOUND_REPLY,
    INTERNAL_ERROR_REPLY,
    PLATFORM_AUTH_REPLY,
    ChannelWorker,
    Dispatcher,
)
from tests.test_config import _VALID_ENV, _VALID_TOML, _write_config

# ---------------------------------------------------------------------------
# Schema bootstrap (same pattern as test_conversation.py).
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_PATH = REPO_ROOT / "migrations" / "versions" / "001_init.py"
sys.path.insert(0, str(_MIG_PATH.parent))
_spec = _ilu.spec_from_file_location("_mig_001_init_dispatcher", _MIG_PATH)
assert _spec is not None and _spec.loader is not None
_mig = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mig)


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Fakes.
# ---------------------------------------------------------------------------


class _FakeAgent:
    """Records every ``run`` invocation; returns a configurable output.

    By default returns ``"reply-to:<text>"`` and an empty ``new_messages``
    list (so the conversation_history write path is a no-op). Tests that
    want history persistence inject real :class:`ModelMessage` instances
    via ``new_messages_factory``.
    """

    def __init__(
        self,
        *,
        reply_factory: Callable[[str], str] | None = None,
        new_messages_factory: Callable[[str], list[Any]] | None = None,
        slow_event: asyncio.Event | None = None,
        raise_on_call: BaseException | None = None,
    ) -> None:
        self._reply_factory = reply_factory or (lambda t: f"reply-to:{t}")
        self._new_messages_factory = new_messages_factory or (lambda _: [])
        self._slow_event = slow_event
        self._raise = raise_on_call
        self.calls: list[tuple[str, list[Any], CapoDeps]] = []

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        if self._slow_event is not None:
            await self._slow_event.wait()
        self.calls.append((text, list(message_history or []), deps))
        if self._raise is not None:
            raise self._raise
        reply = self._reply_factory(text)
        new_msgs = self._new_messages_factory(text)
        return _FakeRunResult(reply, new_msgs)


class _FakeRunResult:
    """Mimics :class:`pydantic_ai.RunResult` minimally — ``output`` +
    ``new_messages()``."""

    def __init__(self, output: str, new_messages: list[Any]) -> None:
        self.output = output
        self._new_messages = new_messages

    def new_messages(self) -> list[Any]:
        return list(self._new_messages)


class _FakeAMCClient:
    """Records every ``send`` / ``mark_read`` call; supports scripted errors.

    ``send_side_effects`` is a list of either ``None`` (success — produce a
    canned :class:`SendResult`) or exceptions to raise. Consumed in order;
    once exhausted, falls back to success.
    """

    def __init__(
        self,
        *,
        send_side_effects: list[BaseException | None] | None = None,
        mark_read_side_effects: list[BaseException | None] | None = None,
    ) -> None:
        self.send_calls: list[dict[str, Any]] = []
        self.mark_read_calls: list[list[str]] = []
        self._send_se = list(send_side_effects or [])
        self._mark_se = list(mark_read_side_effects or [])

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
        if self._send_se:
            effect = self._send_se.pop(0)
            if effect is not None:
                raise effect
        return {"message_id": "out-1", "channel_id": channel_id, "status": "sent"}

    async def mark_read(self, message_ids: Any) -> Any:
        ids = list(message_ids)
        self.mark_read_calls.append(ids)
        if self._mark_se:
            effect = self._mark_se.pop(0)
            if effect is not None:
                raise effect
        return {"marked_read": ids, "already_read": []}


def _envelope(
    *,
    message_id: str,
    channel_id: str,
    sender_id: str = "+15551234567",
    text: str = "hello",
) -> AMCInboundEnvelope:
    return AMCInboundEnvelope(
        id=message_id,
        channel_id=channel_id,
        sender_id=sender_id,
        text=text,
        ts="2026-05-10T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# Shared fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """A real :class:`Settings` instance with the canonical fixture TOML.

    Maps ``+15551234567`` and ``discord:user:123`` → user_id ``owner``.
    The default :class:`ConcurrencyConfig.queue_depth_max` is 100 (we
    override per-test where a smaller cap is wanted).
    """
    cfg = _write_config(tmp_path)
    return Settings.load(cfg, env_override=dict(_VALID_ENV))


@pytest.fixture
def settings_small_queue(tmp_path: Path) -> Settings:
    """A :class:`Settings` with ``queue_depth_max = 3`` for overflow tests."""
    toml = _VALID_TOML.replace(
        "[concurrency]\nmax_delegations = 3",
        "[concurrency]\nmax_delegations = 3\nqueue_depth_max = 3",
    )
    cfg = _write_config(tmp_path, toml)
    return Settings.load(cfg, env_override=dict(_VALID_ENV))


@pytest.fixture
def conn_factory(tmp_path: Path) -> Iterator[Callable[[], sqlite3.Connection]]:
    """A factory that hands out fresh sqlite connections to a shared in-memory DB.

    We use a file-backed temp DB (rather than ``:memory:``) so connections
    opened in different tasks/threads see the same schema and rows.
    """
    db_path = tmp_path / "state.db"

    # Seed the schema + the ``owner`` user row once with a bootstrap conn.
    boot = sqlite3.connect(str(db_path))
    boot.execute("PRAGMA foreign_keys = ON")
    _apply_inline_schema(boot)
    boot.execute("INSERT INTO users(user_id) VALUES ('owner')")
    boot.commit()
    boot.close()

    opened: list[sqlite3.Connection] = []

    def _factory() -> sqlite3.Connection:
        c = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("PRAGMA busy_timeout = 5000")
        opened.append(c)
        return c

    yield _factory
    for c in opened:
        with contextlib.suppress(Exception):
            c.close()


def _fake_deps_factory(settings: Settings) -> Callable[[str], CapoDeps]:
    """Build a deps factory that does NOT touch httpx.

    Tests don't exercise the tools layer; we just need ``CapoDeps`` to be a
    valid dataclass instance. The agent fake never reads its fields.
    """

    def _f(user_id: str) -> CapoDeps:
        return CapoDeps(
            settings=settings,
            http_client=None,  # type: ignore[arg-type]
            user_id=user_id,
        )

    return _f


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


async def _drain_worker(worker: ChannelWorker, *, expected: int) -> None:
    """Wait until the worker has processed ``expected`` envelopes.

    Uses :meth:`asyncio.Queue.join` plus a small grace period to let any
    final ``mark_read`` complete.
    """
    # We watch the queue depth + the number of mark_read calls would be the
    # cleanest signal but tests pass that signal in via fakes. Here we rely
    # on Queue.join which awaits ``task_done`` for every put_nowait.
    await asyncio.wait_for(worker._queue.join(), timeout=5.0)


# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------


async def test_per_channel_ordering_preserved(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """Envelopes for the same channel are processed strictly in arrival order."""
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
        msgs = [
            _envelope(message_id=f"m{i}", channel_id="amc:c1", text=f"t{i}")
            for i in range(5)
        ]
        for m in msgs:
            assert await dispatcher.enqueue(m) is True

        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=5)

        # agent.run was called once per envelope, in arrival order.
        assert [c[0] for c in agent.calls] == ["t0", "t1", "t2", "t3", "t4"]
        # amc.send was called with the correct reply_to_message_id sequence.
        assert [c["reply_to_message_id"] for c in amc.send_calls] == [
            "m0",
            "m1",
            "m2",
            "m3",
            "m4",
        ]
        # mark_read is called once per envelope, in order.
        assert amc.mark_read_calls == [["m0"], ["m1"], ["m2"], ["m3"], ["m4"]]
    finally:
        await dispatcher.stop()


async def test_parallel_channels_do_not_block(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """Slow turn on channel A does not block channel B.

    Channel A's fake agent waits on an :class:`asyncio.Event` we never set
    until B has fully completed. If the dispatcher serialized across
    channels we would deadlock.
    """
    slow_event = asyncio.Event()
    slow_agent = _FakeAgent(slow_event=slow_event)

    # We need two agents (one per channel) — simplest is to install a
    # router-agent that dispatches based on env text prefix.
    fast_agent = _FakeAgent()

    class _RouterAgent:
        calls: list[str] = []

        async def run(self, text: str, *, message_history: Any, deps: CapoDeps) -> Any:
            if text.startswith("slow:"):
                return await slow_agent.run(
                    text, message_history=message_history, deps=deps
                )
            return await fast_agent.run(
                text, message_history=message_history, deps=deps
            )

    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        _RouterAgent(),
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(message_id="A1", channel_id="amc:A", text="slow:A1")
        )
        await dispatcher.enqueue(
            _envelope(message_id="B1", channel_id="amc:B", text="fast:B1")
        )

        # B should finish quickly even though A is blocked.
        worker_b = dispatcher._workers["amc:B"]
        await asyncio.wait_for(worker_b._queue.join(), timeout=5.0)
        assert any(c["reply_to_message_id"] == "B1" for c in amc.send_calls)
        # A should still be in-flight (not yet sent).
        assert not any(c["reply_to_message_id"] == "A1" for c in amc.send_calls)

        # Unblock A.
        slow_event.set()
        worker_a = dispatcher._workers["amc:A"]
        await asyncio.wait_for(worker_a._queue.join(), timeout=5.0)
        assert any(c["reply_to_message_id"] == "A1" for c in amc.send_calls)
    finally:
        await dispatcher.stop()


async def test_unknown_sender_rejected_and_marked_read(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown sender → no agent call, no send, but mark_read still fires."""
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
    caplog.set_level(logging.WARNING, logger="capo.transport.dispatcher")
    try:
        env = _envelope(
            message_id="u1",
            channel_id="amc:c1",
            sender_id="+19998887777",  # not in [users.owner].amc_senders
        )
        assert await dispatcher.enqueue(env) is True
        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=1)

        # Agent is NOT invoked for unknown senders.
        assert agent.calls == []
        # No outbound reply.
        assert amc.send_calls == []
        # But the message is still mark_read'd so AMC stops redelivering.
        assert amc.mark_read_calls == [["u1"]]
        # Structured warning captured with the canonical event name.
        events = [r for r in caplog.records if r.name == "capo.transport.dispatcher"]
        assert any(
            getattr(r, "event", None) == "amc.envelope.rejected.unknown_sender"
            for r in events
        )
        # And the sender_id is preserved in the structured record.
        assert any(
            getattr(r, "sender_id", None) == "+19998887777" for r in events
        )
    finally:
        await dispatcher.stop()


async def test_worker_crash_recovery(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Agent exception → error reply + mark_read + worker stays alive."""
    # First call raises; subsequent calls succeed. Use a small wrapper agent
    # to script the failure.
    call_count = {"n": 0}

    class _CrashingAgent:
        async def run(
            self,
            text: str,
            *,
            message_history: Any,
            deps: CapoDeps,
        ) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("kaboom")
            return _FakeRunResult(f"ok:{text}", [])

    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        _CrashingAgent(),
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    caplog.set_level(logging.ERROR, logger="capo.transport.dispatcher")
    try:
        await dispatcher.enqueue(
            _envelope(message_id="m1", channel_id="amc:c1", text="bad")
        )
        await dispatcher.enqueue(
            _envelope(message_id="m2", channel_id="amc:c1", text="good")
        )
        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=2)

        # (a) error reply sent for the crashed turn (with the canonical text)
        assert any(
            c["text"] == INTERNAL_ERROR_REPLY and c["reply_to_message_id"] == "m1"
            for c in amc.send_calls
        )
        # (a') and a normal reply sent for the next envelope
        assert any(c["reply_to_message_id"] == "m2" for c in amc.send_calls)
        # (b) mark_read called for BOTH envelopes
        assert ["m1"] in amc.mark_read_calls
        assert ["m2"] in amc.mark_read_calls
        # (c) worker task is still alive
        assert worker._task is not None and not worker._task.done()
        # And the failure was logged with a traceback.
        assert any(
            getattr(r, "event", None) == "capo.dispatcher.turn.failed"
            for r in caplog.records
        )
    finally:
        await dispatcher.stop()


async def test_queue_depth_bound_drops_overflow(
    settings_small_queue: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``queue_depth_max + 1`` envelopes → 1 dropped with logged warning.

    We use a "park before any I/O" agent (it awaits an event BEFORE returning
    a value) so the worker is parked entirely outside of the DAO write path
    when stop() cancels — keeping the test segfault-free under concurrent
    teardown.
    """
    # Worker parks here BEFORE any DB or AMC interaction completes; this
    # keeps the sqlite conn that was opened in _handle_envelope idle while
    # we measure queue depth, and lets stop() cleanly cancel without
    # racing the teardown of the conn_factory fixture.
    park_event = asyncio.Event()

    class _ParkingAgent:
        async def run(
            self, text: str, *, message_history: Any, deps: CapoDeps
        ) -> Any:
            # NOTE: agent.run() is called AFTER get_or_create_active_session
            # + load_history complete, so the conn has already finished its
            # work for m0 by the time we park here.
            await park_event.wait()
            return _FakeRunResult(f"ok:{text}", [])

    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings_small_queue,
        _ParkingAgent(),
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings_small_queue),
    )
    await dispatcher.start()
    caplog.set_level(logging.WARNING, logger="capo.transport.dispatcher")
    try:
        # First envelope: worker pulls it out and parks inside agent.run.
        first = _envelope(message_id="m0", channel_id="amc:c1", text="t0")
        assert await dispatcher.enqueue(first) is True
        # Give the worker enough ticks to: pull m0, run the (sync) DAO
        # to_thread calls to completion, then enter agent.run park.
        for _ in range(20):
            await asyncio.sleep(0.01)

        # Now fill the queue: 3 more should fit (cap=3), the 4th must drop.
        accepted = []
        for i in range(1, 5):
            accepted.append(
                await dispatcher.enqueue(
                    _envelope(message_id=f"m{i}", channel_id="amc:c1", text=f"t{i}")
                )
            )
        # 3 accepted, 1 dropped.
        assert accepted == [True, True, True, False]

        worker = dispatcher._workers["amc:c1"]
        # Queue cap respected.
        assert worker._queue.maxsize == 3
        assert worker.queue_depth() == 3

        # The drop produced a structured warning.
        assert any(
            getattr(r, "event", None)
            == "capo.dispatcher.envelope.dropped.queue_full"
            for r in caplog.records
        )
    finally:
        # Unblock the parked agent so stop()'s cancel propagates cleanly
        # rather than racing a worker that's stuck in event.wait() with an
        # open sqlite conn on the stack.
        park_event.set()
        await dispatcher.stop()


async def test_rate_limited_handled_by_amc_client_not_dispatcher(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """If RateLimited surfaces past the AMCClient (deadline busted), the
    dispatcher logs and moves on — it does NOT retry on top.

    Documents the project decision: retry semantics live in the AMCClient
    layer (§7.5). The dispatcher gets exactly one ``send`` call per turn.
    """
    amc = _FakeAMCClient(
        send_side_effects=[
            RateLimited(
                code="RATE_LIMITED",
                message="too fast",
                retry_after_seconds=1,
                status_code=429,
            ),
        ]
    )
    agent = _FakeAgent()
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
            _envelope(message_id="r1", channel_id="amc:c1", text="hello")
        )
        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=1)

        # Exactly ONE send attempt (the dispatcher does not retry).
        assert len(amc.send_calls) == 1
        # And mark_read still fires (idempotent contract).
        assert amc.mark_read_calls == [["r1"]]
    finally:
        await dispatcher.stop()


async def test_platform_auth_logs_alert_and_sends_fallback(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """PLATFORM_AUTH → log alert + fallback reply attempt; no retry of the
    original send."""
    # First send: raise PLATFORM_AUTH. Second send (the fallback attempt):
    # let it succeed so we can assert it was made.
    amc = _FakeAMCClient(
        send_side_effects=[
            PlatformAuth(
                code="PLATFORM_AUTH",
                message="bearer rejected",
                retry_after_seconds=None,
                status_code=401,
            ),
            None,  # fallback attempt succeeds
        ]
    )
    agent = _FakeAgent()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    caplog.set_level(logging.ERROR, logger="capo.transport.dispatcher")
    try:
        await dispatcher.enqueue(
            _envelope(message_id="a1", channel_id="amc:c1", text="hi")
        )
        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=1)

        # Two send attempts: original reply + the user-facing fallback text.
        assert len(amc.send_calls) == 2
        assert amc.send_calls[1]["text"] == PLATFORM_AUTH_REPLY
        # The alert log was emitted with the canonical event name.
        assert any(
            getattr(r, "event", None) == "capo.dispatcher.amc.platform_auth"
            for r in caplog.records
        )
        # mark_read still attempted (idempotent + safe).
        assert amc.mark_read_calls == [["a1"]]
    finally:
        await dispatcher.stop()


async def test_channel_not_found_no_retry_no_fallback(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """CHANNEL_NOT_FOUND → no retry + no fallback send (channel is gone)."""
    amc = _FakeAMCClient(
        send_side_effects=[
            ChannelNotFound(
                code="CHANNEL_NOT_FOUND",
                message="channel gone",
                retry_after_seconds=None,
                status_code=404,
            ),
            # Second slot intentionally non-None — if the dispatcher
            # mistakenly retried, this would be consumed.
            RuntimeError("should not be consumed"),
        ]
    )
    agent = _FakeAgent()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    caplog.set_level(logging.WARNING, logger="capo.transport.dispatcher")
    try:
        await dispatcher.enqueue(
            _envelope(message_id="g1", channel_id="amc:c1", text="hi")
        )
        worker = dispatcher._workers["amc:c1"]
        await _drain_worker(worker, expected=1)

        # Exactly ONE send attempt — no fallback reply was sent.
        assert len(amc.send_calls) == 1
        # mark_read still attempted (idempotent).
        assert amc.mark_read_calls == [["g1"]]
        # CHANNEL_NOT_FOUND log emitted.
        assert any(
            getattr(r, "event", None) == "capo.dispatcher.amc.channel_not_found"
            for r in caplog.records
        )
        # The CHANNEL_NOT_FOUND_REPLY constant is exposed so listeners
        # (Task #10) or follow-up tasks can surface it via another path.
        assert "channel" in CHANNEL_NOT_FOUND_REPLY.lower()
    finally:
        await dispatcher.stop()


async def test_lazy_worker_creation_and_stop(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """Dispatcher lazily spawns workers, ``stop()`` cancels them all."""
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
    assert dispatcher.worker_count() == 0

    await dispatcher.enqueue(_envelope(message_id="m1", channel_id="amc:x"))
    await dispatcher.enqueue(_envelope(message_id="m2", channel_id="amc:y"))
    # Two channels => two workers.
    assert dispatcher.worker_count() == 2

    await dispatcher.stop()
    # After stop(), the map is cleared.
    assert dispatcher.worker_count() == 0
    # Post-stop enqueue is refused.
    assert (
        await dispatcher.enqueue(_envelope(message_id="m3", channel_id="amc:z"))
        is False
    )


async def test_history_round_trip_persists_new_messages(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """``result.new_messages()`` is appended to conversation_history.

    Belt-and-braces test that the dispatcher integrates correctly with the
    memory DAO from Tasks #15 / #16.
    """
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    def _msgs(text: str) -> list[Any]:
        return [
            ModelRequest(parts=[UserPromptPart(content=text)]),
            ModelResponse(parts=[TextPart(content=f"reply:{text}")]),
        ]

    agent = _FakeAgent(new_messages_factory=_msgs)
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
        # Use a raw channel_id — the dispatcher derives the thread_id via
        # :func:`thread_id_for_amc` which prepends ``amc:`` exactly once.
        await dispatcher.enqueue(
            _envelope(message_id="h1", channel_id="histtest", text="hello")
        )
        worker = dispatcher._workers["histtest"]
        await _drain_worker(worker, expected=1)

        # Inspect the DB directly: 2 rows for the (owner, amc:histtest) thread.
        c = conn_factory()
        try:
            rows = c.execute(
                "SELECT user_id, thread_id, message_index FROM conversation_history "
                "WHERE user_id = ? AND thread_id = ? ORDER BY message_index",
                ("owner", "amc:histtest"),
            ).fetchall()
            assert len(rows) == 2, f"rows={rows}"
            assert [r[2] for r in rows] == [0, 1]

            # A session row was also created.
            sess = c.execute(
                "SELECT session_id FROM sessions "
                "WHERE user_id = ? AND thread_id = ? AND ended_at IS NULL",
                ("owner", "amc:histtest"),
            ).fetchone()
            assert sess is not None
        finally:
            c.close()
    finally:
        await dispatcher.stop()


def test_dispatcher_requires_deps_factory_or_http_client(
    settings: Settings, conn_factory: Callable[[], sqlite3.Connection]
) -> None:
    """Constructing without either ``deps_factory`` or ``http_client`` errors fast."""
    with pytest.raises(ValueError, match="deps_factory or http_client"):
        Dispatcher(settings, object(), object(), conn_factory)  # type: ignore[arg-type]

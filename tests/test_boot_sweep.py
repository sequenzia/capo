"""Tests for :mod:`capo.transport.boot_sweep` (Task #13).

Covers spec §5.2 / §6.4 / §7.5 / §9.1 acceptance criteria:

* Empty unread list → returns 0, no enqueues, completion event logged.
* N envelopes → all N enqueued in the order AMC returned them, return value
  equals N, completion event logged.
* AMC unreachable for the entire ``max_boot_wait_seconds`` window → deadline
  expires (using an injected clock), warning event logged, function returns
  0 and does NOT raise. Boot can proceed.
* AMC unreachable then recovers → backoff between attempts, eventually
  succeeds and enqueues the recovered envelopes.
* Deadline=0 → exactly one attempt is made (degenerate "no retries" mode).
* Dispatcher queue full → envelope is logged as dropped, return value
  counts only accepted envelopes.

All tests inject fake ``sleep`` / ``monotonic`` so they finish in
milliseconds without burning real wall-clock time. They also use a fake
:class:`AMCClient`-shaped stub and a fake :class:`Dispatcher`-shaped stub so
no httpx / sqlite3 setup is required.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import pytest

from capo.transport.amc_client import (
    AMCError,
    AMCInboundEnvelope,
    PlatformAuth,
    RateLimited,
)
from capo.transport.boot_sweep import (
    EVENT_ATTEMPT_FAILED,
    EVENT_COMPLETE,
    EVENT_TIMEOUT,
    run_boot_sweep,
)

# ---------------------------------------------------------------------------
# Fakes.
# ---------------------------------------------------------------------------


class _FakeAMCClient:
    """Records every ``get_unread`` call; supports scripted responses.

    ``responses`` is a list of either ``list[AMCInboundEnvelope]`` (a
    successful response) or :class:`BaseException` instances (raised in
    order). Consumed left-to-right; once exhausted the last entry is
    re-used so a single ``[]`` keeps returning ``[]``.
    """

    def __init__(
        self, responses: list[list[AMCInboundEnvelope] | BaseException]
    ) -> None:
        if not responses:
            raise ValueError("responses must not be empty")
        self._responses = list(responses)
        self.call_count: int = 0

    async def get_unread(self) -> list[AMCInboundEnvelope]:
        self.call_count += 1
        if self._responses:
            response = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        else:  # pragma: no cover - defensive (init guards against empty)
            raise RuntimeError("no scripted responses left")
        if isinstance(response, BaseException):
            raise response
        return list(response)


class _FakeDispatcher:
    """Records every ``enqueue`` call. ``accept`` controls success/drop."""

    def __init__(self, *, accept: Callable[[AMCInboundEnvelope], bool] | None = None) -> None:
        self.enqueued: list[AMCInboundEnvelope] = []
        self._accept = accept or (lambda _e: True)

    async def enqueue(self, envelope: AMCInboundEnvelope) -> bool:
        accepted = self._accept(envelope)
        if accepted:
            self.enqueued.append(envelope)
        return accepted


class _FakeClock:
    """Monotonic clock backed by an explicit counter.

    ``sleep`` advances the clock by the requested delay instantly (no real
    wall-clock wait) so deadline-driven tests finish in milliseconds.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self._now += delay


def _make_settings(max_boot_wait_seconds: int) -> Any:
    """Minimal settings stub: only ``settings.amc.max_boot_wait_seconds`` is read."""

    class _Amc:
        pass

    class _Settings:
        pass

    s = _Settings()
    s.amc = _Amc()
    s.amc.max_boot_wait_seconds = max_boot_wait_seconds
    return s


def _envelope(message_id: str, channel_id: str = "+15550000001") -> AMCInboundEnvelope:
    return AMCInboundEnvelope(
        id=message_id,
        channel_id=channel_id,
        sender_id="+15550000099",
        text=f"hello from {message_id}",
        ts="2026-05-10T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# Functional: empty list.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_unread_list_returns_zero_logs_complete(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Empty unread → returns 0, dispatcher.enqueue NOT called, EVENT_COMPLETE logged."""
    amc = _FakeAMCClient(responses=[[]])
    dispatcher = _FakeDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.INFO, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
        )

    assert result == 0
    assert amc.call_count == 1
    assert dispatcher.enqueued == []
    assert clock.sleeps == []  # No backoff needed on first-attempt success.

    events = [r.__dict__.get("event") for r in caplog.records]
    assert EVENT_COMPLETE in events


# ---------------------------------------------------------------------------
# Functional: N envelopes are enqueued in order.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n_envelopes_enqueued_in_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """get_unread returns N envelopes → all N enqueued in order, return=N, EVENT_COMPLETE."""
    envs = [_envelope(f"msg-{i}") for i in range(5)]
    amc = _FakeAMCClient(responses=[envs])
    dispatcher = _FakeDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.INFO, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
        )

    assert result == 5
    assert [e.id for e in dispatcher.enqueued] == [f"msg-{i}" for i in range(5)]
    assert amc.call_count == 1

    complete_records = [r for r in caplog.records if r.__dict__.get("event") == EVENT_COMPLETE]
    assert len(complete_records) == 1
    rec = complete_records[0]
    assert rec.__dict__["envelope_count"] == 5
    assert rec.__dict__["envelope_total"] == 5


# ---------------------------------------------------------------------------
# Error handling: AMC unreachable forever → deadline expires, warning, returns 0.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_amc_unreachable_until_deadline_logs_timeout_returns_zero(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AMCError on every attempt → deadline expires, warning, returns 0, no raise."""
    # 100+ scripted PlatformAuth failures so the deadline (not the response
    # list) is what stops us. ``_FakeAMCClient`` re-uses the last response
    # once the list is exhausted, but to be explicit we make sure each
    # attempt raises.
    failures: list[list[AMCInboundEnvelope] | BaseException] = [
        PlatformAuth(
            code="PLATFORM_AUTH",
            message="bearer token rejected",
            retry_after_seconds=None,
            status_code=401,
        )
        for _ in range(100)
    ]
    amc = _FakeAMCClient(responses=failures)
    dispatcher = _FakeDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.INFO, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
        )

    assert result == 0
    assert dispatcher.enqueued == []
    # We made at least 2 attempts (single attempt would be the empty-deadline
    # case). With backoff 1s, 2s, 4s, 8s, 10s, 10s, ... the 60s budget should
    # allow ~7-8 attempts.
    assert amc.call_count >= 2

    # Backoff series should be non-decreasing up to the cap of 10s, capped to
    # remaining deadline. The clock advanced to >= 60s.
    assert clock.monotonic() >= 60.0
    # First two backoffs should be 1s and 2s (exponential start).
    assert clock.sleeps[0] == pytest.approx(1.0)
    assert clock.sleeps[1] == pytest.approx(2.0)

    # Timeout event logged with metadata.
    timeout_records = [
        r for r in caplog.records if r.__dict__.get("event") == EVENT_TIMEOUT
    ]
    assert len(timeout_records) == 1
    rec = timeout_records[0]
    assert rec.__dict__["attempts"] == amc.call_count
    assert rec.__dict__["max_boot_wait_seconds"] == 60.0
    assert rec.levelname == "WARNING"

    # At least one attempt_failed event per failure attempt.
    attempt_failed = [
        r for r in caplog.records if r.__dict__.get("event") == EVENT_ATTEMPT_FAILED
    ]
    assert len(attempt_failed) >= 2


# ---------------------------------------------------------------------------
# Functional: AMC unreachable then recovers.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_amc_unreachable_then_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two transient failures, then success → backoff between attempts, sweep succeeds."""
    envs = [_envelope("recovered-1"), _envelope("recovered-2")]
    responses: list[list[AMCInboundEnvelope] | BaseException] = [
        RateLimited(
            code="RATE_LIMITED",
            message="slow down",
            retry_after_seconds=None,
            status_code=429,
        ),
        AMCError(
            code="INTERNAL_ERROR",
            message="transient",
            retry_after_seconds=None,
            status_code=503,
        ),
        envs,
    ]
    amc = _FakeAMCClient(responses=responses)
    dispatcher = _FakeDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.INFO, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
        )

    assert result == 2
    assert [e.id for e in dispatcher.enqueued] == ["recovered-1", "recovered-2"]
    assert amc.call_count == 3
    # Two backoffs between three attempts: 1s, 2s.
    assert clock.sleeps == [pytest.approx(1.0), pytest.approx(2.0)]

    # Completion event logged after recovery.
    complete = [r for r in caplog.records if r.__dict__.get("event") == EVENT_COMPLETE]
    assert len(complete) == 1
    assert complete[0].__dict__["envelope_count"] == 2
    assert complete[0].__dict__["attempts"] == 3


# ---------------------------------------------------------------------------
# Edge case: max_boot_wait_seconds=0 → single-shot attempt.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zero_deadline_makes_single_attempt() -> None:
    """max_boot_wait_seconds=0 → exactly one attempt; failure → deadline timeout."""
    amc = _FakeAMCClient(
        responses=[
            PlatformAuth(
                code="PLATFORM_AUTH",
                message="nope",
                retry_after_seconds=None,
                status_code=401,
            )
        ]
    )
    dispatcher = _FakeDispatcher()
    settings = _make_settings(0)
    clock = _FakeClock()

    result = await run_boot_sweep(
        amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
    )

    assert result == 0
    assert amc.call_count == 1
    assert clock.sleeps == []  # No backoff possible past a zero deadline.


@pytest.mark.asyncio
async def test_zero_deadline_success_still_returns_envelope_count() -> None:
    """max_boot_wait_seconds=0 + first attempt succeeds → envelopes enqueued."""
    envs = [_envelope("ok")]
    amc = _FakeAMCClient(responses=[envs])
    dispatcher = _FakeDispatcher()
    settings = _make_settings(0)
    clock = _FakeClock()

    result = await run_boot_sweep(
        amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
    )

    assert result == 1
    assert [e.id for e in dispatcher.enqueued] == ["ok"]


# ---------------------------------------------------------------------------
# Edge case: dispatcher queue full → drop counted, not returned.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_queue_full_drops_are_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dispatcher refusal is logged as dropped; return counts only accepted envelopes."""
    envs = [_envelope(f"e-{i}") for i in range(3)]

    # Accept the first envelope, drop the next two.
    accept_count = {"n": 0}

    def _accept(_env: AMCInboundEnvelope) -> bool:
        accept_count["n"] += 1
        return accept_count["n"] == 1

    amc = _FakeAMCClient(responses=[envs])
    dispatcher = _FakeDispatcher(accept=_accept)
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.WARNING, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc, dispatcher, settings, sleep=clock.sleep, monotonic=clock.monotonic
        )

    assert result == 1
    dropped = [
        r
        for r in caplog.records
        if r.__dict__.get("event") == "capo.boot.sweep.enqueue_dropped"
    ]
    assert len(dropped) == 2


# ---------------------------------------------------------------------------
# Robustness: dispatcher.enqueue raising does not crash boot.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_exception_does_not_crash_boot(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A buggy dispatcher.enqueue must not stop the rest of the drain."""
    envs = [_envelope("a"), _envelope("b"), _envelope("c")]

    class _PartialBrokenDispatcher:
        def __init__(self) -> None:
            self.enqueued: list[AMCInboundEnvelope] = []
            self._n = 0

        async def enqueue(self, envelope: AMCInboundEnvelope) -> bool:
            self._n += 1
            if self._n == 2:  # second envelope explodes
                raise RuntimeError("boom")
            self.enqueued.append(envelope)
            return True

    amc = _FakeAMCClient(responses=[envs])
    dispatcher = _PartialBrokenDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with caplog.at_level(logging.ERROR, logger="capo.transport.boot_sweep"):
        result = await run_boot_sweep(
            amc,
            dispatcher,  # type: ignore[arg-type]
            settings,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )

    # Two envelopes succeed (a and c); the middle one raised.
    assert result == 2
    assert [e.id for e in dispatcher.enqueued] == ["a", "c"]


# ---------------------------------------------------------------------------
# Robustness: CancelledError during get_unread propagates.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_error_propagates() -> None:
    """Cancellation during boot sweep must propagate so shutdown still works."""

    class _CancellingAMC:
        async def get_unread(self) -> list[AMCInboundEnvelope]:
            raise asyncio.CancelledError()

    dispatcher = _FakeDispatcher()
    settings = _make_settings(60)
    clock = _FakeClock()

    with pytest.raises(asyncio.CancelledError):
        await run_boot_sweep(
            _CancellingAMC(),  # type: ignore[arg-type]
            dispatcher,
            settings,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )

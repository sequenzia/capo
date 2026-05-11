"""Boot-time AMC unread sweep (spec §5.2, §6.4, §7.5, §9.1).

On boot, before the FastAPI webhook listener starts serving, Capo MUST drain
any envelopes AMC has been buffering by calling ``GET /messages/unread``
through :class:`capo.transport.amc_client.AMCClient` and feeding each returned
envelope into the same per-channel :class:`capo.transport.dispatcher.Dispatcher`
that handles webhook envelopes. This guarantees:

* "Exactly once" agent delivery for unacked messages (combined with the
  AMC-side dedupe and the dispatcher's per-channel ordering).
* Recovery from Capo downtime up to AMC's ~13-minute retry window (§6.4).
  Anything older has been dead-lettered AMC-side; this sweep recovers the
  rest.

The sweep is resilient to a temporarily-unreachable AMC: it retries with
exponential backoff (1s → 2s → 4s → … cap 10s) until either the call
succeeds OR ``settings.amc.max_boot_wait_seconds`` (default 60s) elapses,
at which point it logs a structured warning and returns ``0`` so the boot
can proceed. The decision to proceed without a successful sweep matches
spec §5.2:

    | AMC offline at boot | retry with backoff up to max_boot_wait_seconds
    |                     | then proceed and log a warning.

Both :func:`asyncio.sleep` and :func:`time.monotonic` are injection points so
tests can fast-forward through the backoff schedule without burning real
wall-clock time.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from capo.transport.amc_client import AMCError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings
    from capo.transport.amc_client import AMCClient, AMCInboundEnvelope
    from capo.transport.dispatcher import Dispatcher

logger = logging.getLogger("capo.transport.boot_sweep")

#: Initial inter-attempt backoff in seconds. Doubles per attempt (capped).
_INITIAL_BACKOFF_S: float = 1.0

#: Upper bound on per-attempt backoff. Keeps total attempt-count reasonable
#: even when ``max_boot_wait_seconds`` is large.
_MAX_BACKOFF_S: float = 10.0


#: Sentinel structured-log event names. Match the dispatcher's
#: ``capo.<subsystem>.<category>`` convention (see Task #11 notes).
EVENT_COMPLETE: str = "capo.boot.sweep.complete"
EVENT_TIMEOUT: str = "capo.boot.sweep.timeout"
EVENT_ATTEMPT_FAILED: str = "capo.boot.sweep.attempt_failed"


async def run_boot_sweep(
    amc_client: AMCClient,
    dispatcher: Dispatcher,
    settings: Settings,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Drain AMC's unread queue into ``dispatcher`` before the listener starts.

    Loops ``amc_client.get_unread()`` with exponential backoff until either
    one call succeeds or ``settings.amc.max_boot_wait_seconds`` is reached.
    On success, every returned envelope is enqueued through
    :meth:`Dispatcher.enqueue` in the order AMC returned them — the same code
    path Task #10's webhook will use, so the rest of the pipeline (per-channel
    ordering, conversation memory, mark_read) is shared with normal traffic.

    Parameters
    ----------
    amc_client:
        Connected :class:`AMCClient`. Must be open; the caller owns the
        lifecycle. The sweep only calls :meth:`AMCClient.get_unread`.
    dispatcher:
        Started :class:`Dispatcher`. Workers may be lazy-created on first
        envelope; the sweep does NOT need the dispatcher to be otherwise
        primed.
    settings:
        Validated :class:`Settings`. Read for
        :attr:`AmcConfig.max_boot_wait_seconds` (default 60s).
    sleep:
        Async sleep injection point. Defaults to :func:`asyncio.sleep`;
        tests pass a fake to fast-forward through backoff.
    monotonic:
        Time source. Defaults to :func:`time.monotonic`; tests pass a
        controllable clock to make the deadline guard deterministic.

    Returns
    -------
    int
        The number of envelopes successfully enqueued. Returns ``0`` both
        when AMC returns an empty list AND when the deadline expires before
        any successful call (the structured warning log distinguishes the
        two — :attr:`EVENT_COMPLETE` vs. :attr:`EVENT_TIMEOUT`).

    Notes
    -----
    This function never raises. AMC errors are caught and retried within
    the deadline; any other exception during the enqueue loop is also
    swallowed so a single bad envelope cannot stop the boot. The boot is
    a best-effort drain — the regular webhook + AMC's own retry window
    (~13 min, ~5 attempts) carry the remaining recovery semantics.
    """
    max_wait_s = float(settings.amc.max_boot_wait_seconds)

    # max_boot_wait_seconds=0 still gets ONE shot at get_unread — the spec
    # says "proceed after max_boot_wait_seconds with warning", and zero is
    # the degenerate "no retries, single attempt" mode.
    start = monotonic()
    deadline = start + max_wait_s

    last_error: BaseException | None = None
    attempt = 0
    envelopes: list[AMCInboundEnvelope] | None = None

    while True:
        attempt += 1
        try:
            envelopes = await amc_client.get_unread()
            break
        except AMCError as exc:
            last_error = exc
            now = monotonic()
            remaining = deadline - now

            # Log every failed attempt so the operator can see the retry
            # cadence in the boot log.
            logger.info(
                "boot sweep attempt %d failed: %s (remaining=%.1fs)",
                attempt,
                exc,
                max(0.0, remaining),
                extra={
                    "event": EVENT_ATTEMPT_FAILED,
                    "attempt": attempt,
                    "amc_code": exc.code,
                    "amc_message": exc.message,
                    "remaining_seconds": max(0.0, remaining),
                },
            )

            if remaining <= 0:
                # Deadline already busted by this attempt's elapsed time.
                break

            # Backoff: 1s, 2s, 4s, ..., cap _MAX_BACKOFF_S. Never sleep
            # past the deadline.
            backoff = min(
                _INITIAL_BACKOFF_S * (2 ** (attempt - 1)),
                _MAX_BACKOFF_S,
            )
            delay = min(backoff, remaining)
            if delay > 0:
                await sleep(delay)

            # After sleeping, re-check the deadline before trying again so
            # we don't burn an attempt when the deadline has just expired.
            if monotonic() >= deadline:
                break
            continue
        except BaseException as exc:  # noqa: BLE001 - boot must not crash
            # Non-AMCError exceptions (e.g. asyncio.CancelledError, unexpected
            # bug) — CancelledError must propagate so a stop signal during
            # boot still works; everything else is logged and treated as a
            # deadline expiry so the boot proceeds.
            if isinstance(exc, asyncio.CancelledError):
                raise
            last_error = exc
            logger.exception(
                "boot sweep attempt %d raised unexpected error: %r",
                attempt,
                exc,
                extra={
                    "event": EVENT_ATTEMPT_FAILED,
                    "attempt": attempt,
                    "error": repr(exc),
                },
            )
            break

    if envelopes is None:
        # Deadline expired or unexpected error — log warning and return 0.
        elapsed = monotonic() - start
        logger.warning(
            "boot sweep timed out after %.1fs (attempts=%d, last_error=%r); "
            "proceeding without sweep — AMC retry window (~13 min) will "
            "redeliver any unacked envelopes",
            elapsed,
            attempt,
            last_error,
            extra={
                "event": EVENT_TIMEOUT,
                "attempts": attempt,
                "elapsed_seconds": elapsed,
                "max_boot_wait_seconds": max_wait_s,
                "last_error": repr(last_error) if last_error else None,
            },
        )
        return 0

    # Successful sweep — enqueue every envelope through the dispatcher.
    enqueued = 0
    for envelope in envelopes:
        try:
            accepted = await dispatcher.enqueue(envelope)
        except BaseException as exc:  # noqa: BLE001 - boot must not crash
            if isinstance(exc, asyncio.CancelledError):
                raise
            # Per-envelope failure is logged but doesn't stop the drain.
            logger.exception(
                "boot sweep failed to enqueue envelope %s: %r",
                envelope.id,
                exc,
                extra={
                    "event": "capo.boot.sweep.enqueue_failed",
                    "message_id": envelope.id,
                    "channel_id": envelope.channel_id,
                    "error": repr(exc),
                },
            )
            continue
        if accepted:
            enqueued += 1
        else:
            # Queue full at boot is a configuration issue — log and continue.
            logger.warning(
                "boot sweep envelope dropped: dispatcher queue full "
                "(channel_id=%s, message_id=%s)",
                envelope.channel_id,
                envelope.id,
                extra={
                    "event": "capo.boot.sweep.enqueue_dropped",
                    "channel_id": envelope.channel_id,
                    "message_id": envelope.id,
                },
            )

    elapsed = monotonic() - start
    logger.info(
        "boot sweep complete: %d envelope(s) enqueued in %.2fs (attempts=%d)",
        enqueued,
        elapsed,
        attempt,
        extra={
            "event": EVENT_COMPLETE,
            "envelope_count": enqueued,
            "envelope_total": len(envelopes),
            "elapsed_seconds": elapsed,
            "attempts": attempt,
        },
    )
    return enqueued


__all__ = [
    "EVENT_ATTEMPT_FAILED",
    "EVENT_COMPLETE",
    "EVENT_TIMEOUT",
    "run_boot_sweep",
]

"""AMC webhook listener (spec §5.2, §7.4, §9.1).

This module owns the front half of Capo's inbound transport: the FastAPI
``POST /amc/webhook`` endpoint that AMC calls to deliver every inbound
message. Every concern that comes BEFORE the per-channel dispatcher lives
here:

1. **HMAC verification** — compute ``sha256=<hex>`` over the raw request body
   using ``settings.amc.webhook_secret`` and compare with
   ``hmac.compare_digest`` against the ``X-AMC-Signature`` header. Performed
   BEFORE parsing or logging the body. Mismatch → ``401`` with the exact
   §7.4 error envelope, body NEVER parsed.
2. **Dedupe** — a 15-minute TTL LRU keyed on the ``X-AMC-Delivery-Id`` header
   (§5.2 retry-window invariant; AMC retries up to ~13 min / 5 attempts per
   §7.6). Duplicates within the window return ``204`` without enqueuing.
3. **Fast-ACK** — once signature + dedupe pass, parse the body as
   :class:`capo.transport.amc_client.AMCInboundEnvelope`, hand it to the
   dispatcher's :meth:`Dispatcher.enqueue`, and respond ``204``. The
   dispatcher worker owns the agent run; the request handler returns
   inside the §6.1 P99 < 1s SLA regardless of how long the turn takes.

Missing ``X-AMC-Delivery-Id`` is rejected with ``400 VALIDATION_ERROR``
following the spike S-4 finding (§6 of ``S-4-amc-webhook-e2e.md``) — the
spec §7.4 endpoint contract names the header as required but does not
itself name a response code. The conservative choice is to refuse the
request rather than dedupe on something else (e.g. body hash) and risk
silent double-enqueue. Captured in Known Decisions in the execution
context.

Logging conventions
-------------------

Every structured log uses ``extra={"event": "capo.listener.<...>"}`` so
Logfire / OTel exporters get the same tag taxonomy used by the dispatcher
module (``capo.dispatcher.<...>``). NEVER log the request body before
signature verification (spec §5.2 explicitly forbids it — that's a
defense-in-depth against signature-forgery probes).

The factory :func:`build_app` returns a configured :class:`fastapi.FastAPI`
so tests can drive it via ``httpx.AsyncClient(transport=ASGITransport(app))``
in-process with a fake dispatcher.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError as PydanticValidationError

from capo.transport.amc_client import AMCInboundEnvelope

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings
    from capo.transport.dispatcher import Dispatcher

logger = logging.getLogger("capo.transport.amc_listener")

#: §5.2 retry-window invariant. AMC retries cover ~13 min / 5 attempts (§7.6);
#: the dedupe LRU MUST cover at least 15 min so the last retry is still
#: recognised as a duplicate. Configurable via the ``dedupe_ttl_seconds``
#: kwarg on :func:`build_app` for tests; production wires the spec default.
DEFAULT_DEDUPE_TTL_SECONDS: int = 15 * 60

#: Cap on the number of delivery-ids retained — protects against a flood of
#: distinct ids exhausting memory. 10k entries at ~40 bytes each ≈ 0.4 MB,
#: ample headroom for the AMC retry-window invariant.
DEFAULT_DEDUPE_MAX_ENTRIES: int = 10_000

#: §7.4 ``401`` body. Exact text is part of the contract — clients (including
#: the S-4 harness) match on the literal ``code`` value.
BAD_SIGNATURE_BODY: dict[str, dict[str, str]] = {
    "error": {
        "code": "BAD_SIGNATURE",
        "message": "X-AMC-Signature does not match body HMAC",
    }
}

#: ``400`` body for the missing-delivery-id case (S-4 finding §6 recommendation).
MISSING_DELIVERY_ID_BODY: dict[str, dict[str, str]] = {
    "error": {
        "code": "VALIDATION_ERROR",
        "message": "X-AMC-Delivery-Id header is required",
    }
}


# Type alias for the injectable monotonic clock — tests pass a fake to drive
# the TTL boundary without sleeping.
MonotonicClock = Callable[[], float]


class DedupeLRU:
    """Bounded TTL LRU keyed by delivery-id (§5.2).

    The webhook handler calls :meth:`seen_and_record` exactly once per
    request after signature verification. On a hit, return ``True`` and do
    NOT enqueue; on a miss, record the id and return ``False``.

    Implemented as an ordered dict with lazy expiry: every call expires
    entries older than ``ttl_seconds`` from the front of the dict. Entries
    are inserted at the back; on capacity overflow the oldest entry is
    evicted (LRU eviction policy — duplicates touching ``move_to_end``
    refresh recency).
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_DEDUPE_TTL_SECONDS,
        max_entries: int = DEFAULT_DEDUPE_MAX_ENTRIES,
        monotonic: MonotonicClock = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self._ttl = ttl_seconds
        self._max = max_entries
        self._items: OrderedDict[str, float] = OrderedDict()
        self._monotonic = monotonic

    def seen_and_record(self, delivery_id: str) -> bool:
        """Return ``True`` if ``delivery_id`` is already known (dedupe hit);
        otherwise record it and return ``False``.

        Hits move the id to the end of the LRU so an actively-retried id
        stays warm even past the nominal TTL relative to first-seen — the
        TTL is "time since most recent observation," which is the AMC retry
        invariant we care about.
        """
        now = self._monotonic()
        self._expire(now)
        if delivery_id in self._items:
            self._items.move_to_end(delivery_id)
            self._items[delivery_id] = now
            return True
        self._items[delivery_id] = now
        if len(self._items) > self._max:
            # Evict oldest by insertion order.
            self._items.popitem(last=False)
        return False

    def _expire(self, now: float) -> None:
        cutoff = now - self._ttl
        # Walk from the front; entries are roughly ordered by insertion / last
        # touch, so as soon as we see a fresh entry we can stop.
        while self._items:
            oldest_key = next(iter(self._items))
            if self._items[oldest_key] < cutoff:
                self._items.popitem(last=False)
            else:
                break

    def __len__(self) -> int:  # pragma: no cover - trivial wrapper
        return len(self._items)


def _verify_signature(secret: bytes, body: bytes, header_value: str | None) -> bool:
    """Constant-time HMAC-SHA256 verification per §7.4.

    Returns ``True`` iff ``header_value`` matches ``sha256=<hex>`` of
    ``HMAC-SHA256(secret, body)``. All comparisons use
    :func:`hmac.compare_digest` to avoid timing side channels.
    """
    if not header_value:
        return False
    if not header_value.startswith("sha256="):
        return False
    provided = header_value.removeprefix("sha256=").strip()
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    # Compare ASCII-encoded values so compare_digest sees equal-length inputs.
    try:
        return hmac.compare_digest(provided.encode("ascii"), expected.encode("ascii"))
    except (UnicodeEncodeError, ValueError):
        return False


def build_app(
    settings: Settings,
    dispatcher: Dispatcher,
    *,
    dedupe_ttl_seconds: int = DEFAULT_DEDUPE_TTL_SECONDS,
    dedupe_max_entries: int = DEFAULT_DEDUPE_MAX_ENTRIES,
    monotonic: MonotonicClock = time.monotonic,
) -> FastAPI:
    """Construct the FastAPI app exposing ``POST /amc/webhook``.

    Parameters
    ----------
    settings:
        Validated :class:`capo.config.Settings`. The webhook secret is read
        from ``settings.amc_webhook_secret`` (a :class:`pydantic.SecretStr`).
    dispatcher:
        Constructed :class:`capo.transport.dispatcher.Dispatcher`. The
        handler calls ``await dispatcher.enqueue(envelope)`` after
        signature + dedupe pass.
    dedupe_ttl_seconds:
        TTL for the dedupe LRU. Defaults to §5.2's 15-minute invariant.
        Tests override to drive the post-window-replay path deterministically.
    dedupe_max_entries:
        Cap on the dedupe LRU. Defaults to 10k (~0.4 MB).
    monotonic:
        Injectable clock for the dedupe LRU. Tests pass a fake so the
        post-window-replay path doesn't require ``time.sleep``.
    """
    secret_bytes = settings.amc_webhook_secret.get_secret_value().encode("utf-8")
    dedupe = DedupeLRU(
        ttl_seconds=dedupe_ttl_seconds,
        max_entries=dedupe_max_entries,
        monotonic=monotonic,
    )
    app = FastAPI(
        title="capo-amc-listener",
        version="1",
        # Disable the docs surfaces — this is a loopback-only contract endpoint,
        # not a developer API. Less surface area for surprises.
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    # Stash for tests / introspection.
    app.state.dedupe = dedupe
    app.state.dispatcher = dispatcher
    app.state.settings = settings

    @app.post("/amc/webhook")
    async def amc_webhook(request: Request) -> Response:
        # (a) Read the RAW body BEFORE any other work. The HMAC contract is
        # over the exact bytes AMC sent; we must not let FastAPI's body
        # parser run first (which is why we don't bind a Pydantic model
        # parameter — that would consume the stream and re-encode it).
        raw_body = await request.body()

        # (b) Verify signature BEFORE parsing or logging the body. This is the
        # §5.2 invariant: signature mismatch must produce 401 with the body
        # never touched and the contents NEVER logged.
        signature_header = request.headers.get("X-AMC-Signature")
        if not _verify_signature(secret_bytes, raw_body, signature_header):
            logger.warning(
                "amc_listener signature mismatch from %s",
                request.client.host if request.client else "<unknown>",
                extra={
                    "event": "capo.listener.signature.fail",
                    "remote_addr": request.client.host if request.client else None,
                    # Body length only — never the body itself.
                    "body_bytes": len(raw_body),
                    "has_signature_header": signature_header is not None,
                },
            )
            return JSONResponse(status_code=401, content=BAD_SIGNATURE_BODY)

        # (c) Delivery-id required. Decision: 400 VALIDATION_ERROR (S-4
        # finding §6; spec gap noted). Without a delivery-id we cannot
        # dedupe → safer to refuse the request than risk silent double-enqueue.
        delivery_id = request.headers.get("X-AMC-Delivery-Id")
        if not delivery_id:
            logger.warning(
                "amc_listener missing X-AMC-Delivery-Id header",
                extra={
                    "event": "capo.listener.delivery_id.missing",
                    "remote_addr": request.client.host if request.client else None,
                },
            )
            return JSONResponse(status_code=400, content=MISSING_DELIVERY_ID_BODY)

        # (d) Dedupe. Same delivery-id within the TTL window → 204, NO enqueue.
        if dedupe.seen_and_record(delivery_id):
            logger.info(
                "amc_listener dedupe hit for delivery_id=%s",
                delivery_id,
                extra={
                    "event": "capo.listener.dedupe.hit",
                    "delivery_id": delivery_id,
                },
            )
            return Response(status_code=204)

        # (e) Parse the envelope. Malformed JSON or missing required fields:
        # 400 VALIDATION_ERROR (consistent with the missing-header path).
        try:
            envelope = AMCInboundEnvelope.model_validate_json(raw_body)
        except PydanticValidationError as exc:
            logger.warning(
                "amc_listener envelope validation failed for delivery_id=%s: %s",
                delivery_id,
                exc.errors(),
                extra={
                    "event": "capo.listener.envelope.invalid",
                    "delivery_id": delivery_id,
                    # ``error_count`` is safe to log — the bad body still
                    # passed HMAC, so it's authentic, but we err on the side
                    # of caution and skip the actual content.
                    "error_count": exc.error_count(),
                },
            )
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "request body does not match AMC envelope schema",
                    }
                },
            )
        except ValueError as exc:
            # Catches plain JSON parse errors (not all paths route through
            # PydanticValidationError on bad JSON in pydantic v2).
            logger.warning(
                "amc_listener envelope JSON parse failed for delivery_id=%s: %r",
                delivery_id,
                exc,
                extra={
                    "event": "capo.listener.envelope.bad_json",
                    "delivery_id": delivery_id,
                },
            )
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "request body is not valid JSON",
                    }
                },
            )

        # (f) Hand off to the dispatcher. Failures here MUST NOT block the
        # handler — AMC retries on non-2xx so a transient dispatcher error
        # would just snowball. We log structured detail and still 204; the
        # retry-window invariant means a true outage will surface via the
        # downstream worker, not via the handler latency budget.
        try:
            accepted = await dispatcher.enqueue(envelope)
        except Exception as exc:  # noqa: BLE001 - intentional broad catch
            logger.error(
                "amc_listener dispatcher.enqueue raised for delivery_id=%s "
                "channel_id=%s message_id=%s: %r",
                delivery_id,
                envelope.channel_id,
                envelope.id,
                exc,
                extra={
                    "event": "capo.listener.enqueue.raised",
                    "delivery_id": delivery_id,
                    "channel_id": envelope.channel_id,
                    "message_id": envelope.id,
                    "error": repr(exc),
                },
            )
            return Response(status_code=204)

        if not accepted:
            # Queue full. AMC's retries are idempotent against our dedupe
            # window — re-delivery of the same delivery-id will hit the
            # dedupe LRU and 204 without re-enqueuing. Tasks #12 / #34 may
            # later promote this to a 429 surface; Phase 1 documents the
            # warning and ACKs.
            logger.warning(
                "amc_listener dispatcher.enqueue returned False (queue full) "
                "for delivery_id=%s channel_id=%s message_id=%s",
                delivery_id,
                envelope.channel_id,
                envelope.id,
                extra={
                    "event": "capo.listener.enqueue.full",
                    "delivery_id": delivery_id,
                    "channel_id": envelope.channel_id,
                    "message_id": envelope.id,
                },
            )
            return Response(status_code=204)

        logger.info(
            "amc_listener accepted delivery_id=%s channel_id=%s message_id=%s",
            delivery_id,
            envelope.channel_id,
            envelope.id,
            extra={
                "event": "capo.listener.accepted",
                "delivery_id": delivery_id,
                "channel_id": envelope.channel_id,
                "message_id": envelope.id,
            },
        )
        return Response(status_code=204)

    return app


__all__ = [
    "BAD_SIGNATURE_BODY",
    "DEFAULT_DEDUPE_MAX_ENTRIES",
    "DEFAULT_DEDUPE_TTL_SECONDS",
    "DedupeLRU",
    "MISSING_DELIVERY_ID_BODY",
    "build_app",
]

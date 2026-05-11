"""Tests for :mod:`capo.transport.amc_listener` (Task #10).

Covers spec §5.2 / §6.1 / §7.4 acceptance criteria:

* **HMAC happy path** — correctly-signed envelope → 204; dispatcher.enqueue
  called with the parsed :class:`AMCInboundEnvelope`.
* **HMAC mismatch** — tampered signature → 401 with the EXACT §7.4 body;
  dispatcher.enqueue NEVER called; body never parsed.
* **Missing X-AMC-Signature** — 401 (no header == no valid signature).
* **Missing X-AMC-Delivery-Id** — 400 ``VALIDATION_ERROR`` (S-4 finding §6).
* **Dedupe** — same delivery-id within the TTL window → second call returns
  204 without enqueuing.
* **Dedupe TTL** — after the window expires (fake-clock advance), the same
  delivery-id is enqueued again.
* **enqueue returns False (queue full)** — handler still 204s; warning
  logged.
* **enqueue raises** — handler still 204s; error logged with traceback.
* **Performance** — 1000 sequential signed webhooks via
  ``httpx.AsyncClient(transport=ASGITransport(...))`` complete with P99 ACK
  latency under the §6.1 1s SLA.

HMAC signing uses the same :mod:`signer` reference used by the S-4 harness,
imported via :class:`sys.path` insertion so production code does NOT
depend on harness code. (The harness is a test fixture, not a runtime
artifact.)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.amc_listener import (
    BAD_SIGNATURE_BODY,
    DEFAULT_DEDUPE_TTL_SECONDS,
    DedupeLRU,
    build_app,
)
from tests.test_config import _VALID_ENV, _write_config

# Import the S-4 reference signer for parity with the harness scenarios. Adding
# the harness directory to sys.path is the deliberate path of least surprise —
# the signer is a small, well-tested module and copy/pasting it into tests
# would silently drift from the harness contract.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_HARNESS_DIR = _REPO_ROOT / "internal" / "specs" / "spikes" / "S-4-harness"
if str(_HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(_HARNESS_DIR))

from signer import sign_body, sign_request  # noqa: E402  - sys.path tweak above

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


class FakeDispatcher:
    """Stand-in dispatcher: records every enqueue, returns ``True`` by default.

    Behavior knobs:

    * ``return_value`` — what :meth:`enqueue` returns. Set to ``False`` to
      drive the queue-full path.
    * ``raise_exc`` — if set, :meth:`enqueue` raises this exception instead.
    """

    def __init__(
        self,
        *,
        return_value: bool = True,
        raise_exc: BaseException | None = None,
    ) -> None:
        self.calls: list[AMCInboundEnvelope] = []
        self.return_value = return_value
        self.raise_exc = raise_exc

    async def enqueue(self, envelope: AMCInboundEnvelope) -> bool:
        self.calls.append(envelope)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.return_value


def _make_settings(tmp_path: Path) -> Any:
    """Build a real :class:`capo.config.Settings` for the listener factory."""
    from capo.config import Settings

    cfg = _write_config(tmp_path)
    return Settings.load(cfg, env_override=dict(_VALID_ENV))


@pytest.fixture
def settings(tmp_path: Path) -> Any:
    return _make_settings(tmp_path)


@pytest.fixture
def webhook_secret() -> str:
    return _VALID_ENV["AMC_WEBHOOK_SECRET"]


def _make_envelope(**overrides: Any) -> dict[str, Any]:
    env: dict[str, Any] = {
        "id": f"msg-{uuid.uuid4()}",
        "channel_id": "+15551234567",
        "sender_id": "+15557654321",
        "text": "hello capo",
        "ts": "2026-05-10T12:34:56Z",
        "approval_id": None,
    }
    env.update(overrides)
    return env


def _make_client(app: Any) -> httpx.AsyncClient:
    """Build an in-process httpx client bound to the FastAPI ASGI app."""
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


# ---------------------------------------------------------------------------
# DedupeLRU unit tests.
# ---------------------------------------------------------------------------


def test_dedupe_lru_first_seen_returns_false() -> None:
    lru = DedupeLRU(ttl_seconds=60, max_entries=10)
    assert lru.seen_and_record("d-1") is False


def test_dedupe_lru_repeat_returns_true_within_ttl() -> None:
    lru = DedupeLRU(ttl_seconds=60, max_entries=10)
    assert lru.seen_and_record("d-1") is False
    assert lru.seen_and_record("d-1") is True
    assert lru.seen_and_record("d-1") is True


def test_dedupe_lru_expiry_after_ttl() -> None:
    """After the TTL elapses, the same id is treated as fresh again."""
    clock = {"t": 1000.0}

    def fake_now() -> float:
        return clock["t"]

    lru = DedupeLRU(ttl_seconds=60, max_entries=10, monotonic=fake_now)
    assert lru.seen_and_record("d-1") is False
    clock["t"] += 30
    assert lru.seen_and_record("d-1") is True
    clock["t"] += 60.1  # past TTL since most-recent touch
    assert lru.seen_and_record("d-1") is False


def test_dedupe_lru_max_entries_evicts_oldest() -> None:
    lru = DedupeLRU(ttl_seconds=3600, max_entries=3)
    for i in range(5):
        assert lru.seen_and_record(f"d-{i}") is False
    # After 5 inserts with cap=3, the OLDEST two ids (d-0, d-1) should have
    # been evicted; the most recent three (d-2, d-3, d-4) are retained.
    assert lru.seen_and_record("d-0") is False  # evicted → fresh
    assert lru.seen_and_record("d-1") is False  # evicted → fresh
    assert lru.seen_and_record("d-4") is True  # still present


def test_dedupe_lru_rejects_invalid_args() -> None:
    with pytest.raises(ValueError):
        DedupeLRU(ttl_seconds=-1)
    with pytest.raises(ValueError):
        DedupeLRU(max_entries=0)


# ---------------------------------------------------------------------------
# HMAC verification tests.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_204_and_enqueue_called(
    settings: Any, webhook_secret: str
) -> None:
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope(text="hi")
    signed = sign_request(webhook_secret, envelope)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 204
    assert resp.content == b""
    assert len(dispatcher.calls) == 1
    enqueued = dispatcher.calls[0]
    assert isinstance(enqueued, AMCInboundEnvelope)
    assert enqueued.text == "hi"
    assert enqueued.channel_id == envelope["channel_id"]
    assert enqueued.id == envelope["id"]


@pytest.mark.asyncio
async def test_extra_envelope_fields_passthrough(
    settings: Any, webhook_secret: str
) -> None:
    """``extra='allow'`` per §7.4 — unknown fields survive parsing."""
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope(platform_native={"thread_role": "owner"})
    signed = sign_request(webhook_secret, envelope)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 204
    assert len(dispatcher.calls) == 1
    parsed = dispatcher.calls[0]
    # The extra field is exposed via __pydantic_extra__ when extra=allow.
    extras = parsed.__pydantic_extra__ or {}
    assert extras.get("platform_native") == {"thread_role": "owner"}


@pytest.mark.asyncio
async def test_bad_signature_returns_401_exact_body(
    settings: Any, webhook_secret: str
) -> None:
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    signed = sign_request(webhook_secret, envelope, tamper_signature=True)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 401
    assert resp.json() == BAD_SIGNATURE_BODY
    # Critical: body NEVER parsed → dispatcher NEVER called.
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_missing_signature_returns_401(
    settings: Any, webhook_secret: str
) -> None:
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    body = json.dumps(envelope).encode("utf-8")

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook",
            content=body,
            headers={"Content-Type": "application/json"},
        )

    assert resp.status_code == 401
    assert resp.json() == BAD_SIGNATURE_BODY
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_malformed_signature_prefix_returns_401(
    settings: Any, webhook_secret: str
) -> None:
    """A signature header that doesn't start with ``sha256=`` is invalid."""
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    body = json.dumps(envelope).encode("utf-8")
    bogus_sig = sign_body(webhook_secret, body).removeprefix("sha256=")

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-AMC-Signature": bogus_sig,  # missing "sha256=" prefix
                "X-AMC-Delivery-Id": str(uuid.uuid4()),
            },
        )

    assert resp.status_code == 401
    assert resp.json() == BAD_SIGNATURE_BODY
    assert dispatcher.calls == []


# ---------------------------------------------------------------------------
# Missing delivery-id.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_delivery_id_returns_400_validation_error(
    settings: Any, webhook_secret: str
) -> None:
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    signed = sign_request(webhook_secret, envelope, omit_delivery_id=True)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert dispatcher.calls == []


# ---------------------------------------------------------------------------
# Dedupe.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_delivery_id_within_window_dedupes(
    settings: Any, webhook_secret: str
) -> None:
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    delivery_id = str(uuid.uuid4())
    # Sign each request fresh — same body, same delivery-id, two POSTs.
    signed1 = sign_request(webhook_secret, envelope, delivery_id=delivery_id)
    signed2 = sign_request(webhook_secret, envelope, delivery_id=delivery_id)

    async with _make_client(app) as client:
        r1 = await client.post(
            "/amc/webhook", content=signed1.body, headers=signed1.headers
        )
        r2 = await client.post(
            "/amc/webhook", content=signed2.body, headers=signed2.headers
        )

    assert r1.status_code == 204
    assert r2.status_code == 204
    # Only the first delivery is enqueued.
    assert len(dispatcher.calls) == 1


@pytest.mark.asyncio
async def test_duplicate_delivery_id_after_window_re_enqueues(
    settings: Any, webhook_secret: str
) -> None:
    """A fake monotonic clock advances past the TTL between calls."""
    clock = {"t": 1000.0}

    def fake_now() -> float:
        return clock["t"]

    dispatcher = FakeDispatcher()
    app = build_app(
        settings,
        dispatcher,
        dedupe_ttl_seconds=60,
        monotonic=fake_now,
    )
    envelope = _make_envelope()
    delivery_id = str(uuid.uuid4())
    signed1 = sign_request(webhook_secret, envelope, delivery_id=delivery_id)
    signed2 = sign_request(webhook_secret, envelope, delivery_id=delivery_id)

    async with _make_client(app) as client:
        r1 = await client.post(
            "/amc/webhook", content=signed1.body, headers=signed1.headers
        )
        assert r1.status_code == 204
        # Advance well past the 60s TTL.
        clock["t"] += 120
        r2 = await client.post(
            "/amc/webhook", content=signed2.body, headers=signed2.headers
        )

    assert r2.status_code == 204
    # Both deliveries enqueued because the dedupe window expired.
    assert len(dispatcher.calls) == 2


# ---------------------------------------------------------------------------
# Dispatcher failure modes.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_returns_false_handler_still_204(
    settings: Any, webhook_secret: str, caplog: pytest.LogCaptureFixture
) -> None:
    dispatcher = FakeDispatcher(return_value=False)
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    signed = sign_request(webhook_secret, envelope)

    caplog.set_level(logging.WARNING, logger="capo.transport.amc_listener")
    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 204
    assert len(dispatcher.calls) == 1
    # A queue-full warning is captured.
    assert any(
        getattr(r, "event", None) == "capo.listener.enqueue.full"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_enqueue_raises_handler_still_204(
    settings: Any, webhook_secret: str, caplog: pytest.LogCaptureFixture
) -> None:
    dispatcher = FakeDispatcher(raise_exc=RuntimeError("boom"))
    app = build_app(settings, dispatcher)
    envelope = _make_envelope()
    signed = sign_request(webhook_secret, envelope)

    caplog.set_level(logging.ERROR, logger="capo.transport.amc_listener")
    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook", content=signed.body, headers=signed.headers
        )

    assert resp.status_code == 204
    # The dispatcher was still called (it raised), and the error was logged.
    assert len(dispatcher.calls) == 1
    assert any(
        getattr(r, "event", None) == "capo.listener.enqueue.raised"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_malformed_json_body_400(
    settings: Any, webhook_secret: str
) -> None:
    """Authentic body (HMAC passes) but not JSON → 400 VALIDATION_ERROR."""
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    body = b"not-json-at-all"
    sig = sign_body(webhook_secret, body)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-AMC-Signature": sig,
                "X-AMC-Delivery-Id": str(uuid.uuid4()),
            },
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_envelope_missing_required_field_400(
    settings: Any, webhook_secret: str
) -> None:
    """Authentic body but schema-invalid (missing ``channel_id``) → 400."""
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)
    bad_envelope = {
        "id": "msg-1",
        # channel_id deliberately missing
        "sender_id": "+15557654321",
        "text": "hi",
        "ts": "2026-05-10T12:34:56Z",
    }
    body = json.dumps(bad_envelope, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    sig = sign_body(webhook_secret, body)

    async with _make_client(app) as client:
        resp = await client.post(
            "/amc/webhook",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-AMC-Signature": sig,
                "X-AMC-Delivery-Id": str(uuid.uuid4()),
            },
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert dispatcher.calls == []


# ---------------------------------------------------------------------------
# Performance: 1000 signed webhooks, P99 < 1s ACK.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ack_p99_under_one_second_for_1000_signed_webhooks(
    settings: Any, webhook_secret: str
) -> None:
    """§6.1: P99 < 1s for the webhook ACK path under load.

    The §5.2 sender rate floor is 5 rps, but the latency budget is per-call;
    we measure 1000 sequential in-process calls (so we drive the worst-case
    same-loop concurrency) and assert P99 < 1s. In-process numbers are far
    tighter than over-the-wire — this is a regression bar, not a load test.
    """
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)

    n = 1000
    latencies_ms: list[float] = []
    async with _make_client(app) as client:
        for _ in range(n):
            envelope = _make_envelope()
            signed = sign_request(webhook_secret, envelope)
            t0 = time.perf_counter()
            resp = await client.post(
                "/amc/webhook", content=signed.body, headers=signed.headers
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            assert resp.status_code == 204
            latencies_ms.append(elapsed_ms)

    assert len(dispatcher.calls) == n
    latencies_ms.sort()
    p50 = latencies_ms[int(0.50 * n) - 1]
    p99 = latencies_ms[int(0.99 * n) - 1]
    # §6.1 budget is 1s; in-process should be orders of magnitude under that.
    # We assert a generous bound to keep the test stable on slow CI.
    assert p99 < 1000.0, (
        f"P99 webhook ACK latency exceeded 1s budget: "
        f"p50={p50:.2f}ms p99={p99:.2f}ms (n={n})"
    )


@pytest.mark.asyncio
async def test_signature_mismatch_never_reaches_dispatcher(
    settings: Any, webhook_secret: str
) -> None:
    """Property-style: 50 tampered signatures, 0 dispatcher invocations."""
    dispatcher = FakeDispatcher()
    app = build_app(settings, dispatcher)

    async with _make_client(app) as client:
        for _ in range(50):
            envelope = _make_envelope()
            signed = sign_request(
                webhook_secret, envelope, tamper_signature=True
            )
            resp = await client.post(
                "/amc/webhook", content=signed.body, headers=signed.headers
            )
            assert resp.status_code == 401
            assert resp.json() == BAD_SIGNATURE_BODY

    assert dispatcher.calls == []


# ---------------------------------------------------------------------------
# Default TTL is the §5.2 invariant.
# ---------------------------------------------------------------------------


def test_default_dedupe_ttl_is_15_minutes() -> None:
    """Sanity check the spec invariant exposed by the module constant."""
    assert DEFAULT_DEDUPE_TTL_SECONDS == 15 * 60


# Silence the unused import warning on asyncio (imported for the async fixtures
# autoload contract under pytest-asyncio).
_ = asyncio

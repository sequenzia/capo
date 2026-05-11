"""Tests for :mod:`capo.transport.amc_client` (Task 12).

Covers the §5.2 + §7.5 acceptance criteria:

* Every outbound request sets ``Authorization: Bearer ...``, ``X-Agent-ID:
  capo``, and ``Content-Type: application/json``.
* ``POST /messages/send`` always carries an ``Idempotency-Key`` header — a
  fresh UUIDv4 by default; the caller-provided value is forwarded verbatim
  on every retry so AMC dedupes correctly.
* Each AMC error code from §7.5 maps to its typed exception.
* ``RATE_LIMITED`` honors ``Retry-After`` and retries within the window.
* ``PLATFORM_AUTH`` / ``CHANNEL_NOT_FOUND`` / ``ATTACHMENT_TOO_LARGE`` /
  ``VALIDATION_ERROR`` are NOT auto-retried.
* ``mark_read`` returns both ``marked_read`` and ``already_read`` lists
  (idempotency contract).
* The ``AMCInboundEnvelope`` Pydantic model parses the §7.4 fixture.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from capo.transport.amc_client import (
    AGENT_ID,
    IDEMPOTENCY_HEADER,
    AMCClient,
    AMCError,
    AMCInboundEnvelope,
    AMCInternalError,
    AttachmentTooLarge,
    ChannelNotFound,
    PlatformAuth,
    RateLimited,
    ValidationError,
)

# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

BASE_URL = "http://amc.test"
BEARER = "test-token-abc123"


def _bearer() -> SecretStr:
    return SecretStr(BEARER)


def _capture_handler(
    responder: list[httpx.Response],
    recorded: list[httpx.Request],
) -> Any:
    """Return an httpx.MockTransport handler that pops a response per call.

    Each incoming request is appended to ``recorded`` so tests can assert
    header / body / URL contents post-hoc.
    """
    iterator = iter(responder)

    def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        return next(iterator)

    return handler


def _mock_client(
    responses: list[httpx.Response],
    recorded: list[httpx.Request],
    **kwargs: Any,
) -> AMCClient:
    """Build an :class:`AMCClient` backed by :class:`httpx.MockTransport`.

    Defaults to a no-op async sleep so retry-honoring tests do not actually
    wait, and a deterministic ``rng`` so backoff jitter is reproducible.
    """
    transport = httpx.MockTransport(_capture_handler(responses, recorded))

    async def fast_sleep(_seconds: float) -> None:
        return None

    return AMCClient(
        base_url=BASE_URL,
        bearer_token=_bearer(),
        transport=transport,
        sleep=kwargs.pop("sleep", fast_sleep),
        max_attempts=kwargs.pop("max_attempts", 3),
        total_deadline_s=kwargs.pop("total_deadline_s", 30.0),
        initial_backoff_s=kwargs.pop("initial_backoff_s", 0.5),
        **kwargs,
    )


def _err_body(code: str, message: str = "x", retry_after: int | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"error": {"code": code, "message": message}}
    if retry_after is not None:
        body["error"]["retry_after_seconds"] = retry_after
    return body


# ---------------------------------------------------------------------------
# Header contract — applies to every outbound call.
# ---------------------------------------------------------------------------


class TestCommonHeaders:
    """Every outbound request carries the §7.5 contract headers."""

    async def test_get_unread_headers(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [httpx.Response(200, json={"messages": []})],
            recorded,
        )
        await client.get_unread()
        await client.aclose()

        assert len(recorded) == 1
        req = recorded[0]
        assert req.method == "GET"
        assert req.url.path == "/messages/unread"
        assert req.headers["Authorization"] == f"Bearer {BEARER}"
        assert req.headers["X-Agent-ID"] == AGENT_ID == "capo"
        assert req.headers["Content-Type"] == "application/json"

    async def test_send_headers_include_idempotency_key(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [
                httpx.Response(
                    200,
                    json={"message_id": "m-1", "channel_id": "c", "status": "sent"},
                )
            ],
            recorded,
        )
        await client.send(channel_id="c", text="hi")
        await client.aclose()

        req = recorded[0]
        assert req.headers["Authorization"] == f"Bearer {BEARER}"
        assert req.headers["X-Agent-ID"] == "capo"
        assert req.headers["Content-Type"] == "application/json"
        key = req.headers[IDEMPOTENCY_HEADER]
        # UUIDv4 by default.
        parsed = uuid.UUID(key)
        assert parsed.version == 4

    async def test_send_uses_explicit_idempotency_key(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [
                httpx.Response(
                    200,
                    json={"message_id": "m-1", "channel_id": "c", "status": "sent"},
                )
            ],
            recorded,
        )
        explicit = "11111111-1111-4111-8111-111111111111"
        await client.send(channel_id="c", text="hi", idempotency_key=explicit)
        await client.aclose()

        assert recorded[0].headers[IDEMPOTENCY_HEADER] == explicit

    async def test_send_generates_new_uuid_per_call(self) -> None:
        recorded: list[httpx.Request] = []
        ok = httpx.Response(
            200, json={"message_id": "m", "channel_id": "c", "status": "sent"}
        )
        client = _mock_client([ok, ok], recorded)
        await client.send(channel_id="c", text="one")
        await client.send(channel_id="c", text="two")
        await client.aclose()

        k1 = recorded[0].headers[IDEMPOTENCY_HEADER]
        k2 = recorded[1].headers[IDEMPOTENCY_HEADER]
        assert k1 != k2
        assert uuid.UUID(k1).version == 4
        assert uuid.UUID(k2).version == 4

    async def test_mark_read_headers(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [httpx.Response(200, json={"marked_read": ["m-1"], "already_read": []})],
            recorded,
        )
        await client.mark_read(["m-1"])
        await client.aclose()

        req = recorded[0]
        assert req.method == "POST"
        assert req.url.path == "/messages/mark_read"
        assert req.headers["X-Agent-ID"] == "capo"
        assert req.headers["Authorization"] == f"Bearer {BEARER}"
        assert req.headers["Content-Type"] == "application/json"
        # mark_read MUST NOT add an Idempotency-Key — dedupe is keyed off
        # message_id (§7.5 ``Idempotent by message_id``).
        assert IDEMPOTENCY_HEADER not in req.headers


# ---------------------------------------------------------------------------
# Happy-path response shapes.
# ---------------------------------------------------------------------------


class TestResponseShapes:
    async def test_get_unread_returns_envelopes(self) -> None:
        fixture_path = (
            Path(__file__).resolve().parents[1]
            / "internal"
            / "specs"
            / "spikes"
            / "S-4-harness"
            / "fixtures"
            / "envelope.json"
        )
        envelope_dict = json.loads(fixture_path.read_text())

        recorded: list[httpx.Request] = []
        client = _mock_client(
            [httpx.Response(200, json={"messages": [envelope_dict]})],
            recorded,
        )
        out = await client.get_unread()
        await client.aclose()

        assert len(out) == 1
        env = out[0]
        assert isinstance(env, AMCInboundEnvelope)
        assert env.id == envelope_dict["id"]
        assert env.channel_id == envelope_dict["channel_id"]
        assert env.sender_id == envelope_dict["sender_id"]
        assert env.text == envelope_dict["text"]
        assert env.ts == envelope_dict["ts"]
        assert env.approval_id is None

    async def test_get_unread_passthrough_fields(self) -> None:
        recorded: list[httpx.Request] = []
        msg = {
            "id": "m-1",
            "channel_id": "c",
            "sender_id": "s",
            "text": "hi",
            "ts": "2026-05-10T12:00:00Z",
            "approval_id": None,
            "platform_metadata": {"discord_guild_id": "123"},
        }
        client = _mock_client(
            [httpx.Response(200, json={"messages": [msg]})],
            recorded,
        )
        out = await client.get_unread()
        await client.aclose()

        # extra="allow" keeps the AMC passthrough fields.
        assert out[0].model_dump()["platform_metadata"] == {"discord_guild_id": "123"}

    async def test_send_returns_send_result(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [
                httpx.Response(
                    200,
                    json={"message_id": "m-1", "channel_id": "c", "status": "sent"},
                )
            ],
            recorded,
        )
        result = await client.send(channel_id="c", text="hi")
        await client.aclose()

        assert result.message_id == "m-1"
        assert result.channel_id == "c"
        assert result.status == "sent"

    async def test_send_payload_shape(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [
                httpx.Response(
                    200,
                    json={"message_id": "m-1", "channel_id": "c", "status": "sent"},
                )
            ],
            recorded,
        )
        approval = {
            "approval_id": "ap-1",
            "prompt": "approve?",
            "options": ["approve", "deny"],
        }
        await client.send(
            channel_id="c",
            text="t",
            reply_to_message_id="m-0",
            approval=approval,
        )
        await client.aclose()

        body = json.loads(recorded[0].content)
        # ``reply_to_message_id`` maps to the AMC wire field ``reply_to``;
        # ``approval`` is reserved for Phase 4 and silently dropped from
        # the body until AMC's send schema accepts it.
        assert body == {
            "channel_id": "c",
            "text": "t",
            "reply_to": "m-0",
        }

    async def test_mark_read_returns_both_lists(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [
                httpx.Response(
                    200,
                    json={"marked_read": ["m-1"], "already_read": ["m-2"]},
                )
            ],
            recorded,
        )
        result = await client.mark_read(["m-1", "m-2"])
        await client.aclose()

        assert result.marked_read == ["m-1"]
        assert result.already_read == ["m-2"]

    async def test_mark_read_idempotent_no_op_on_repeat(self) -> None:
        """Repeated mark_read on already-read id is a success no-op (§7.5)."""
        recorded: list[httpx.Request] = []
        first = httpx.Response(200, json={"marked_read": ["m-1"], "already_read": []})
        second = httpx.Response(200, json={"marked_read": [], "already_read": ["m-1"]})
        client = _mock_client([first, second], recorded)

        r1 = await client.mark_read(["m-1"])
        r2 = await client.mark_read(["m-1"])
        await client.aclose()

        assert r1.marked_read == ["m-1"]
        assert r2.already_read == ["m-1"]
        assert r2.marked_read == []


# ---------------------------------------------------------------------------
# Error-code → typed exception mapping (§7.5).
# ---------------------------------------------------------------------------


class TestErrorCodeMapping:
    @pytest.mark.parametrize(
        "code,exc_cls,status",
        [
            ("RATE_LIMITED", RateLimited, 429),
            ("PLATFORM_AUTH", PlatformAuth, 401),
            ("CHANNEL_NOT_FOUND", ChannelNotFound, 404),
            ("ATTACHMENT_TOO_LARGE", AttachmentTooLarge, 413),
            ("VALIDATION_ERROR", ValidationError, 400),
            ("INTERNAL_ERROR", AMCInternalError, 500),
        ],
    )
    async def test_each_code_raises_typed_exception(
        self,
        code: str,
        exc_cls: type[AMCError],
        status: int,
    ) -> None:
        recorded: list[httpx.Request] = []
        # Pre-populate enough responses so the retryable codes can exhaust
        # their attempts; non-retryable will only consume one.
        responses = [httpx.Response(status, json=_err_body(code))] * 5
        client = _mock_client(responses, recorded, max_attempts=3)

        with pytest.raises(exc_cls) as exc_info:
            await client.send(channel_id="c", text="t")
        await client.aclose()

        assert exc_info.value.code == code
        assert exc_info.value.status_code == status

    async def test_unknown_code_falls_back_to_base_amcerror(self) -> None:
        recorded: list[httpx.Request] = []
        client = _mock_client(
            [httpx.Response(418, json=_err_body("WEIRD_CODE"))],
            recorded,
        )
        with pytest.raises(AMCError) as exc_info:
            await client.send(channel_id="c", text="t")
        await client.aclose()

        # Type is the base class, not any subclass.
        assert type(exc_info.value) is AMCError
        assert exc_info.value.code == "WEIRD_CODE"


# ---------------------------------------------------------------------------
# Retry policy.
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    async def test_rate_limited_honors_retry_after_header(self) -> None:
        """RATE_LIMITED with Retry-After → sleep then retry, second attempt
        passes. We capture every sleep delay via an injected fake clock.
        """
        recorded: list[httpx.Request] = []
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        responses = [
            httpx.Response(
                429,
                headers={"Retry-After": "2"},
                json=_err_body("RATE_LIMITED", retry_after=2),
            ),
            httpx.Response(
                200,
                json={"message_id": "m-1", "channel_id": "c", "status": "sent"},
            ),
        ]
        client = _mock_client(responses, recorded, sleep=fake_sleep)
        result = await client.send(channel_id="c", text="t")
        await client.aclose()

        assert result.message_id == "m-1"
        assert len(recorded) == 2
        # Sleep equals Retry-After value (no jitter when explicit).
        assert sleeps == [2.0]
        # Idempotency key is reused on the retry → AMC dedupe stays in effect.
        assert (
            recorded[0].headers[IDEMPOTENCY_HEADER]
            == recorded[1].headers[IDEMPOTENCY_HEADER]
        )

    async def test_rate_limited_falls_back_to_body_retry_after_seconds(self) -> None:
        """When no Retry-After header is set, body ``retry_after_seconds``
        wins over exponential backoff."""
        recorded: list[httpx.Request] = []
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        responses = [
            httpx.Response(429, json=_err_body("RATE_LIMITED", retry_after=5)),
            httpx.Response(
                200,
                json={"message_id": "m", "channel_id": "c", "status": "sent"},
            ),
        ]
        client = _mock_client(responses, recorded, sleep=fake_sleep)
        await client.send(channel_id="c", text="t")
        await client.aclose()

        assert sleeps == [5.0]

    async def test_internal_error_is_retried(self) -> None:
        """INTERNAL_ERROR is treated as transient — retried with backoff."""
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(500, json=_err_body("INTERNAL_ERROR")),
            httpx.Response(
                200,
                json={"message_id": "m", "channel_id": "c", "status": "sent"},
            ),
        ]
        client = _mock_client(responses, recorded)
        result = await client.send(channel_id="c", text="t")
        await client.aclose()

        assert result.message_id == "m"
        assert len(recorded) == 2

    async def test_transient_5xx_without_code_is_retried(self) -> None:
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(503, text="upstream down"),
            httpx.Response(200, json={"messages": []}),
        ]
        client = _mock_client(responses, recorded)
        await client.get_unread()
        await client.aclose()
        assert len(recorded) == 2

    @pytest.mark.parametrize(
        "code,exc_cls,status",
        [
            ("PLATFORM_AUTH", PlatformAuth, 401),
            ("CHANNEL_NOT_FOUND", ChannelNotFound, 404),
            ("ATTACHMENT_TOO_LARGE", AttachmentTooLarge, 413),
            ("VALIDATION_ERROR", ValidationError, 400),
        ],
    )
    async def test_non_retryable_codes_make_exactly_one_request(
        self,
        code: str,
        exc_cls: type[AMCError],
        status: int,
    ) -> None:
        recorded: list[httpx.Request] = []
        # Provide many responses so a (buggy) retry would silently pass —
        # the assertion is on the request count.
        responses = [httpx.Response(status, json=_err_body(code))] * 5
        client = _mock_client(responses, recorded, max_attempts=3)

        with pytest.raises(exc_cls):
            await client.send(channel_id="c", text="t")
        await client.aclose()

        assert len(recorded) == 1

    async def test_unread_retries_rate_limited(self) -> None:
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(
                429,
                headers={"Retry-After": "1"},
                json=_err_body("RATE_LIMITED", retry_after=1),
            ),
            httpx.Response(200, json={"messages": []}),
        ]
        client = _mock_client(responses, recorded)
        out = await client.get_unread()
        await client.aclose()
        assert out == []
        assert len(recorded) == 2

    async def test_mark_read_retries_transient_5xx(self) -> None:
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(503, json=_err_body("INTERNAL_ERROR")),
            httpx.Response(
                200, json={"marked_read": ["m-1"], "already_read": []}
            ),
        ]
        client = _mock_client(responses, recorded)
        result = await client.mark_read(["m-1"])
        await client.aclose()
        assert result.marked_read == ["m-1"]
        assert len(recorded) == 2

    async def test_max_attempts_bounded(self) -> None:
        """Retryable failures exhaust max_attempts → last typed error raised."""
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(429, json=_err_body("RATE_LIMITED", retry_after=0))
        ] * 5
        client = _mock_client(responses, recorded, max_attempts=3)

        with pytest.raises(RateLimited):
            await client.send(channel_id="c", text="t")
        await client.aclose()
        assert len(recorded) == 3


# ---------------------------------------------------------------------------
# Idempotency edge case: same explicit key → no duplicate user-visible message.
# ---------------------------------------------------------------------------


class TestSendIdempotency:
    async def test_same_explicit_idempotency_key_sends_same_header(self) -> None:
        """Caller-controlled dedupe: two send calls with the same
        ``idempotency_key`` produce the same on-the-wire header. AMC's
        server-side dedupe then guarantees no duplicate user-visible
        message — Capo's contract is the header, not the response body.
        """
        recorded: list[httpx.Request] = []
        ok = httpx.Response(
            200, json={"message_id": "m", "channel_id": "c", "status": "sent"}
        )
        client = _mock_client([ok, ok], recorded)
        key = "22222222-2222-4222-8222-222222222222"

        await client.send(channel_id="c", text="hi", idempotency_key=key)
        await client.send(channel_id="c", text="hi", idempotency_key=key)
        await client.aclose()

        assert recorded[0].headers[IDEMPOTENCY_HEADER] == key
        assert recorded[1].headers[IDEMPOTENCY_HEADER] == key

    async def test_idempotency_key_reused_across_retries(self) -> None:
        """A single logical send retains the same Idempotency-Key across
        every retry so AMC dedupes correctly even on RATE_LIMITED retries."""
        recorded: list[httpx.Request] = []
        responses = [
            httpx.Response(429, json=_err_body("RATE_LIMITED", retry_after=0)),
            httpx.Response(500, json=_err_body("INTERNAL_ERROR")),
            httpx.Response(
                200,
                json={"message_id": "m", "channel_id": "c", "status": "sent"},
            ),
        ]
        client = _mock_client(responses, recorded, max_attempts=3)
        await client.send(channel_id="c", text="t")
        await client.aclose()

        keys = {req.headers[IDEMPOTENCY_HEADER] for req in recorded}
        assert len(recorded) == 3
        assert len(keys) == 1  # exactly one distinct key reused on every retry

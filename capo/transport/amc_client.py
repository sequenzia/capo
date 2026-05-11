"""AMC REST client (§7.5 Integration Points).

This module owns Capo's outbound REST traffic to AMC for the three Capo-facing
endpoints documented in the §7.5 AMC REST Contract table:

* ``GET /messages/unread`` — boot-time unread sweep (§5.2).
* ``POST /messages/send`` — outbound reply (§5.2 dispatcher worker).
* ``POST /messages/mark_read`` — idempotent read marker (§5.2 worker
  post-agent step).

Every request carries the canonical contract headers::

    Authorization: Bearer $AMC_BEARER_TOKEN
    X-Agent-ID: capo
    Content-Type: application/json

``send`` additionally generates a fresh UUIDv4 ``Idempotency-Key`` per call
unless the caller passes one explicitly — AMC's server-side dedupe keys off
this header (§7.5 contract table). The same key passed in two calls hits the
AMC dedupe and does NOT produce duplicate user-visible messages.

Error handling follows the §7.5 envelope ``{"error": {"code", "message",
"retry_after_seconds": int|null}}``. Each canonical code maps to its typed
exception in :class:`AMCError`'s subclass hierarchy. Retry policy:

* ``RATE_LIMITED`` and transient 5xx → retry with backoff, honoring
  ``Retry-After`` header or ``retry_after_seconds`` body field.
* ``INTERNAL_ERROR`` (5xx-equivalent code) → retry once with backoff.
* ``PLATFORM_AUTH``, ``CHANNEL_NOT_FOUND``, ``ATTACHMENT_TOO_LARGE``,
  ``VALIDATION_ERROR`` → NEVER retried; raised immediately.

A bounded retry count (default 3 attempts) and a total deadline (default 30s)
cap the worst-case latency so callers up the stack see a deterministic
failure mode.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

# ---------------------------------------------------------------------------
# Constants — keep verbatim per §7.5 contract.
# ---------------------------------------------------------------------------

#: Header name AMC keys server-side dedupe off for ``POST /messages/send``.
IDEMPOTENCY_HEADER: str = "Idempotency-Key"

#: Static ``X-Agent-ID`` value — every Capo→AMC request carries this exact
#: string per §5.2 + §7.5 contract.
AGENT_ID: str = "capo"

#: Default retry budget. AMC's webhook retry window is ~13 min / 5 attempts
#: (§7.6); the outbound budget is intentionally tighter — the per-channel
#: dispatcher caller will surface persistent failures to the user.
DEFAULT_MAX_ATTEMPTS: int = 3
DEFAULT_TOTAL_DEADLINE_S: float = 30.0
DEFAULT_INITIAL_BACKOFF_S: float = 0.5
DEFAULT_TIMEOUT_S: float = 10.0

# ---------------------------------------------------------------------------
# Inbound envelope (§7.4 body + §7.5 GET /messages/unread response).
# ---------------------------------------------------------------------------


class AMCInboundEnvelope(BaseModel):
    """One AMC inbound message envelope.

    Matches the §7.4 ``POST /amc/webhook`` body shape; also returned (as an
    element of the ``messages`` array) by ``GET /messages/unread`` per §7.5.

    ``extra="allow"`` matches the spec note "Pydantic ``extra='allow'``" so
    AMC passthrough fields (platform-native metadata) flow through untouched
    without forcing a schema bump on every AMC iteration.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    channel_id: str
    sender_id: str
    text: str
    ts: str  # ISO8601; left as ``str`` to preserve the wire format verbatim.
    approval_id: str | None = None


class SendResult(BaseModel):
    """Response body for ``POST /messages/send`` per §7.5."""

    model_config = ConfigDict(extra="allow")

    message_id: str
    channel_id: str
    status: str  # documented value: ``"sent"``


class MarkReadResult(BaseModel):
    """Response body for ``POST /messages/mark_read`` per §7.5.

    Both lists are always present (possibly empty); ``already_read`` carries
    the idempotency contract: repeated marks on the same ``message_id`` land
    in this list rather than failing.
    """

    model_config = ConfigDict(extra="allow")

    marked_read: list[str] = Field(default_factory=list)
    already_read: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Typed exception hierarchy (§7.5 known error codes).
# ---------------------------------------------------------------------------


@dataclass
class AMCError(Exception):
    """Base for every typed AMC REST error.

    Carries the wire-format ``code``, ``message``, and (when applicable)
    ``retry_after_seconds`` so callers can log a single structured event
    without re-parsing the response body. The corresponding HTTP status is
    also retained for diagnostics — it is NOT used for retry classification
    (that's keyed off ``code`` instead, per §7.5).
    """

    code: str
    message: str
    retry_after_seconds: int | None = None
    status_code: int | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        suffix = f" (retry_after={self.retry_after_seconds}s)" if self.retry_after_seconds else ""
        return f"{self.code}: {self.message}{suffix}"


@dataclass
class RateLimited(AMCError):
    """AMC ``RATE_LIMITED`` (§7.5). Retryable; honor ``Retry-After``."""


@dataclass
class PlatformAuth(AMCError):
    """AMC ``PLATFORM_AUTH`` (§7.5). NEVER retried — fix bearer + restart."""


@dataclass
class ChannelNotFound(AMCError):
    """AMC ``CHANNEL_NOT_FOUND`` (§7.5). NEVER retried — channel gone."""


@dataclass
class AttachmentTooLarge(AMCError):
    """AMC ``ATTACHMENT_TOO_LARGE`` (§7.5). NEVER retried — caller drops."""


@dataclass
class ValidationError(AMCError):
    """AMC ``VALIDATION_ERROR`` (§7.5). NEVER retried — caller fix."""


@dataclass
class AMCInternalError(AMCError):
    """AMC ``INTERNAL_ERROR`` (§7.5). Treated as transient; retried once."""


#: Map from wire ``error.code`` string → concrete exception class.
_CODE_MAP: dict[str, type[AMCError]] = {
    "RATE_LIMITED": RateLimited,
    "PLATFORM_AUTH": PlatformAuth,
    "CHANNEL_NOT_FOUND": ChannelNotFound,
    "ATTACHMENT_TOO_LARGE": AttachmentTooLarge,
    "VALIDATION_ERROR": ValidationError,
    "INTERNAL_ERROR": AMCInternalError,
}

#: Codes that MUST NOT be auto-retried (caller-fix or out-of-channel issues).
_NON_RETRYABLE_CODES: frozenset[str] = frozenset(
    {"PLATFORM_AUTH", "CHANNEL_NOT_FOUND", "ATTACHMENT_TOO_LARGE", "VALIDATION_ERROR"}
)

#: Codes that ARE auto-retried with backoff. ``RATE_LIMITED`` honors
#: ``Retry-After``; ``INTERNAL_ERROR`` uses exponential backoff. Other
#: transient surfaces (5xx, network errors) get the same treatment without a
#: code lookup.
_RETRYABLE_CODES: frozenset[str] = frozenset({"RATE_LIMITED", "INTERNAL_ERROR"})


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _build_headers(bearer_token: SecretStr) -> dict[str, str]:
    """Canonical Capo→AMC headers (§7.5, every request)."""
    return {
        "Authorization": f"Bearer {bearer_token.get_secret_value()}",
        "X-Agent-ID": AGENT_ID,
        "Content-Type": "application/json",
    }


def _parse_retry_after(response: httpx.Response, body: dict[str, Any] | None) -> int | None:
    """Resolve ``Retry-After`` precedence: header > body ``retry_after_seconds``.

    Returns ``None`` when no hint is present. Negative or unparseable values
    are treated as missing so the caller falls back to exponential backoff.
    """
    header = response.headers.get("Retry-After") or response.headers.get("retry-after")
    if header is not None:
        try:
            value = int(header)
            if value >= 0:
                return value
        except ValueError:
            pass
    if body is not None:
        err = body.get("error")
        if isinstance(err, dict):
            ra = err.get("retry_after_seconds")
            if isinstance(ra, int) and ra >= 0:
                return ra
    return None


def _raise_for_error(response: httpx.Response) -> None:
    """Translate an AMC error response into the right typed exception.

    On 2xx, returns. On 4xx/5xx, parses the §7.5 error envelope and raises
    the matching :class:`AMCError` subclass. Unknown codes fall back to
    :class:`AMCError` itself.
    """
    if 200 <= response.status_code < 300:
        return

    body: dict[str, Any] | None
    try:
        body = response.json()
    except ValueError:
        body = None

    code: str
    message: str
    retry_after: int | None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err = body["error"]
        code = str(err.get("code") or "UNKNOWN")
        message = str(err.get("message") or "")
        retry_after = _parse_retry_after(response, body)
    else:
        # 5xx with no body / non-JSON body is still treated as transient via
        # the synthetic ``INTERNAL_ERROR`` code so the retry layer kicks in.
        code = "INTERNAL_ERROR" if response.status_code >= 500 else "UNKNOWN"
        message = response.text or f"HTTP {response.status_code}"
        retry_after = _parse_retry_after(response, None)

    exc_cls = _CODE_MAP.get(code, AMCError)
    raise exc_cls(
        code=code,
        message=message,
        retry_after_seconds=retry_after,
        status_code=response.status_code,
    )


def _is_retryable_status(status: int) -> bool:
    """True for transient 5xx (§7.5 ``Retry transient 5xx``)."""
    return 500 <= status < 600


# ---------------------------------------------------------------------------
# Client.
# ---------------------------------------------------------------------------


class AMCClient:
    """Async REST client for Capo→AMC traffic (§7.5).

    Parameters
    ----------
    base_url:
        AMC REST base URL (e.g. ``http://127.0.0.1:8765``). Typically pulled
        from :attr:`capo.config.AmcConfig.base_url`.
    bearer_token:
        AMC bearer token wrapped in :class:`pydantic.SecretStr` so it stays
        redacted in logs / reprs. Typically ``Settings.amc_bearer_token``.
    timeout:
        Per-request HTTP timeout in seconds. Defaults to 10s.
    max_attempts:
        Maximum number of attempts per call (initial + retries). The bound
        applies *only* to retry-eligible failures; non-retryable codes raise
        on the first attempt regardless.
    total_deadline_s:
        Total wall-clock budget per call across all attempts. Wins over
        ``max_attempts`` if reached first.
    initial_backoff_s:
        Base exponential-backoff delay (seconds) used when no
        ``Retry-After`` hint is available. Doubles per attempt with ±25%
        jitter.
    transport:
        Optional :class:`httpx.AsyncBaseTransport`. Tests pass
        :class:`httpx.MockTransport` here to script responses without a
        network listener.
    sleep:
        Optional async sleep injection point; defaults to :func:`asyncio.sleep`.
        Tests pass a fake to keep ``Retry-After``-honoring scenarios fast.
    monotonic:
        Optional time source for the total-deadline guard; defaults to
        :func:`time.monotonic`.
    rng:
        Optional :class:`random.Random` for backoff jitter; defaults to a
        module-internal generator.
    """

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: SecretStr,
        timeout: float = DEFAULT_TIMEOUT_S,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        total_deadline_s: float = DEFAULT_TOTAL_DEADLINE_S,
        initial_backoff_s: float = DEFAULT_INITIAL_BACKOFF_S,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Any] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if total_deadline_s <= 0:
            raise ValueError("total_deadline_s must be > 0")

        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._max_attempts = max_attempts
        self._total_deadline_s = total_deadline_s
        self._initial_backoff_s = initial_backoff_s
        self._monotonic = monotonic
        self._rng = rng or random.Random()
        self._sleep = sleep if sleep is not None else asyncio.sleep

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            transport=transport,
        )

    @classmethod
    def from_settings(
        cls,
        amc: Any,
        bearer_token: SecretStr,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> AMCClient:
        """Construct an :class:`AMCClient` from a :class:`capo.config.AmcConfig`
        instance plus the bearer secret from :class:`capo.config.Settings`.

        This avoids a hard import cycle while keeping the canonical wiring
        path in one place.
        """
        return cls(
            base_url=amc.base_url,
            bearer_token=bearer_token,
            transport=transport,
        )

    # ---- lifecycle --------------------------------------------------------

    async def __aenter__(self) -> AMCClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying :class:`httpx.AsyncClient`."""
        await self._client.aclose()

    # ---- public endpoints ------------------------------------------------

    async def get_unread(self) -> list[AMCInboundEnvelope]:
        """``GET /messages/unread`` — return all currently-unread envelopes.

        AMC may return the same message in later sweeps until a matching
        :meth:`mark_read` succeeds (§7.5 ``Idempotency`` column). Retries
        transient 5xx and ``RATE_LIMITED`` per §7.5 contract.
        """
        body = await self._request(
            method="GET",
            path="/messages/unread",
            json_body=None,
            extra_headers=None,
        )
        messages = body.get("messages") if isinstance(body, dict) else None
        if not isinstance(messages, list):
            raise AMCError(
                code="VALIDATION_ERROR",
                message="GET /messages/unread response missing 'messages' array",
                retry_after_seconds=None,
                status_code=200,
            )
        return [AMCInboundEnvelope.model_validate(m) for m in messages]

    async def send(
        self,
        channel_id: str,
        text: str,
        *,
        reply_to_message_id: str | None = None,
        approval: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> SendResult:
        """``POST /messages/send`` — deliver one outbound message.

        ``idempotency_key`` defaults to a freshly-generated UUIDv4 — the
        same key passed twice hits AMC's server-side dedupe and does NOT
        produce duplicate user-visible messages (§7.5 ``Idempotency``
        column).

        Retries ``RATE_LIMITED`` honoring ``Retry-After`` and transient 5xx.
        Does NOT retry ``PLATFORM_AUTH``, ``CHANNEL_NOT_FOUND``,
        ``ATTACHMENT_TOO_LARGE``, or ``VALIDATION_ERROR``.
        """
        payload: dict[str, Any] = {
            "channel_id": channel_id,
            "text": text,
            "reply_to_message_id": reply_to_message_id,
            "approval": approval,
        }
        key = idempotency_key if idempotency_key is not None else str(uuid.uuid4())
        body = await self._request(
            method="POST",
            path="/messages/send",
            json_body=payload,
            extra_headers={IDEMPOTENCY_HEADER: key},
        )
        return SendResult.model_validate(body)

    async def mark_read(self, message_ids: Iterable[str]) -> MarkReadResult:
        """``POST /messages/mark_read`` — idempotent read-marker batch.

        Repeated marks on the same ``message_id`` succeed as no-ops, landing
        in the ``already_read`` list rather than failing (§7.5 ``Idempotent
        by message_id``). Retries transient 5xx and ``RATE_LIMITED``; safe
        to retry after worker crash.
        """
        payload = {"message_ids": list(message_ids)}
        body = await self._request(
            method="POST",
            path="/messages/mark_read",
            json_body=payload,
            extra_headers=None,
        )
        return MarkReadResult.model_validate(body)

    # ---- retrying request core -------------------------------------------

    async def _request(
        self,
        *,
        method: str,
        path: str,
        json_body: dict[str, Any] | None,
        extra_headers: dict[str, str] | None,
    ) -> Any:
        """Perform one logical AMC request with the retry policy in §7.5.

        The retry decision tree:

        1. Build headers + payload once. ``Idempotency-Key`` (when supplied)
           is reused across retries so AMC's dedupe stays in effect.
        2. Send via :class:`httpx.AsyncClient`. Catch
           :class:`httpx.HTTPError` as a transient network failure.
        3. On 2xx: parse JSON and return.
        4. On 4xx/5xx: parse the §7.5 error envelope.
           - Non-retryable code → raise immediately.
           - Retryable code (``RATE_LIMITED``, ``INTERNAL_ERROR``) → sleep
             ``retry_after_seconds`` (preferring header over body) or
             exponential backoff with jitter, then retry.
           - Transient 5xx without a recognized code → same as
             ``INTERNAL_ERROR`` (treated as transient).
        5. After ``max_attempts`` or ``total_deadline_s``: raise the last
           recorded :class:`AMCError`.
        """
        headers = _build_headers(self._bearer_token)
        if extra_headers:
            headers.update(extra_headers)

        deadline = self._monotonic() + self._total_deadline_s
        last_exc: AMCError | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.request(
                    method=method,
                    url=path,
                    json=json_body,
                    headers=headers,
                )
            except httpx.HTTPError as exc:
                last_exc = AMCError(
                    code="INTERNAL_ERROR",
                    message=f"transport error: {exc!r}",
                    retry_after_seconds=None,
                    status_code=None,
                )
                if attempt >= self._max_attempts:
                    raise last_exc from exc
                await self._sleep_for_retry(attempt, retry_after=None, deadline=deadline)
                continue

            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError as exc:
                    raise AMCError(
                        code="VALIDATION_ERROR",
                        message=f"AMC 2xx body is not JSON: {exc!r}",
                        retry_after_seconds=None,
                        status_code=response.status_code,
                    ) from exc

            # Non-2xx: classify.
            try:
                _raise_for_error(response)
            except AMCError as exc:
                last_exc = exc

                # Non-retryable codes: raise on first contact.
                if exc.code in _NON_RETRYABLE_CODES:
                    raise

                # Retryable codes + transient 5xx fall through to sleep+retry.
                is_retryable = (
                    exc.code in _RETRYABLE_CODES
                    or (exc.status_code is not None and _is_retryable_status(exc.status_code))
                )
                if not is_retryable:
                    raise

                if attempt >= self._max_attempts:
                    raise

                await self._sleep_for_retry(
                    attempt,
                    retry_after=exc.retry_after_seconds,
                    deadline=deadline,
                )
                continue

        # Defensive fallthrough — loop always ``return``s or ``raise``s above.
        assert last_exc is not None  # pragma: no cover
        raise last_exc  # pragma: no cover

    async def _sleep_for_retry(
        self,
        attempt: int,
        *,
        retry_after: int | None,
        deadline: float,
    ) -> None:
        """Sleep before the next retry, honoring deadline and Retry-After.

        Precedence: explicit ``retry_after`` (capped at remaining deadline)
        > exponential backoff with ±25% jitter starting at
        ``initial_backoff_s``. Sleeping past the deadline is converted into
        an immediate raise so the caller sees a deterministic failure.
        """
        now = self._monotonic()
        remaining = deadline - now
        if remaining <= 0:
            raise AMCError(
                code="INTERNAL_ERROR",
                message="AMC retry total deadline exceeded",
                retry_after_seconds=None,
                status_code=None,
            )

        if retry_after is not None and retry_after >= 0:
            delay = float(retry_after)
        else:
            base = self._initial_backoff_s * (2 ** (attempt - 1))
            jitter = base * 0.25 * (self._rng.random() * 2 - 1)
            delay = max(0.0, base + jitter)

        # Never sleep past the deadline.
        delay = min(delay, remaining)
        await self._sleep(delay)

"""Basic (non-coding) agent tools: ``web_search`` and ``fetch_url``.

These are the Phase-1 tools required by spec §5.1 / §9.1. The shell-exec
tool is intentionally deferred to Phase 2 (Task #19 in the plan).

Design contract
---------------

* **Both tools take ``ctx: RunContext[CapoDeps]`` first.** Pydantic AI
  injects ``ctx.deps`` automatically from the ``deps=`` kwarg of
  ``agent.run``.
* **Errors are returned, not raised.** Per spec §5.1 acceptance criteria
  for the agent loop — tool exceptions must not crash the loop. Recoverable
  conditions (non-2xx, body cap exceeded, missing provider) return a
  structured :class:`~capo.deps.FetchError` or raise :class:`ModelRetry`
  to give the LLM a chance to fix its input. Only programmer errors and
  totally unexpected exceptions bubble up — and even those are caught +
  logged by the registration wrapper so the agent loop survives.
* **SOUL anchoring.** Tool docstrings MUST NOT contain SOUL content; SOUL
  is anchored in :class:`pydantic_ai.Agent`'s ``instructions=`` only
  (spec §5.1). Tests assert this anchoring by scanning rendered tool
  descriptions.
"""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

from pydantic_ai import ModelRetry, RunContext

from capo.deps import CapoDeps, FetchError, SearchResult

if TYPE_CHECKING:  # pragma: no cover - imported only for type hints
    from pydantic_ai import Agent

logger = logging.getLogger("capo.tools.basic")

# ---------------------------------------------------------------------------
# Constants — keep behavior reviewable from this one place.
# ---------------------------------------------------------------------------

#: Hard cap on response body bytes ``fetch_url`` will buffer. 1 MiB keeps the
#: agent context window safe and prevents accidental DoS via huge HTML
#: pages. Larger fetches return a structured ``body_too_large`` error.
FETCH_URL_MAX_BYTES: int = 1024 * 1024  # 1 MiB

#: Per-request timeout for ``fetch_url`` in seconds. Independent from the
#: AMC client deadline — agent tool calls should fail fast.
FETCH_URL_TIMEOUT_S: float = 15.0


# ---------------------------------------------------------------------------
# Tool implementations.
#
# These are module-level functions (NOT methods) so they can be unit-tested
# in isolation without spinning up an Agent. ``register_basic_tools`` below
# wires them onto an Agent via ``@agent.tool``.
# ---------------------------------------------------------------------------


async def web_search(
    ctx: RunContext[CapoDeps], query: str
) -> list[SearchResult]:
    """Search the public web and return up to a handful of result snippets.

    Args:
        query: A non-empty natural-language search query.

    Returns:
        A list of ``SearchResult`` items (possibly empty if the provider
        returns no hits). The list is the canonical agent-visible result;
        backend-specific metadata is intentionally stripped.

    Raises:
        ModelRetry: If ``query`` is empty/whitespace, telling the LLM to
            retry with a real query. If no provider is configured, the
            same exception fires with a clear "no provider configured"
            message so the agent surfaces it to the user instead of
            silently returning ``[]``.
    """
    if not query or not query.strip():
        raise ModelRetry("web_search: query is required (non-empty string)")

    client = ctx.deps.web_search_client
    if client is None:
        raise ModelRetry(
            "web_search: no web-search provider configured "
            "(deps.web_search_client is None)"
        )

    result = client.search(query.strip())
    if inspect.isawaitable(result):
        result = await result
    return list(result)


async def fetch_url(
    ctx: RunContext[CapoDeps], url: str
) -> str | FetchError:
    """Fetch the body of ``url`` over HTTPS/HTTP and return it as text.

    Caps the response body at :data:`FETCH_URL_MAX_BYTES` and the request
    at :data:`FETCH_URL_TIMEOUT_S` seconds. Non-2xx responses and
    over-cap bodies return a structured :class:`FetchError` so the agent
    can reason about them in-band instead of treating them as exceptions.

    Args:
        url: An absolute http(s) URL to fetch.

    Returns:
        The response body as a UTF-8 string (decoded with ``replace``
        errors so binary noise can't crash the tool), OR a ``FetchError``
        describing why the fetch failed (non-2xx, body cap, etc.).

    Raises:
        ModelRetry: If ``url`` is empty / not a string. Real network
            failures (DNS, TLS, refused) are caught and returned as a
            ``FetchError`` with ``error="http_error"`` and ``status=None``
            so the agent loop survives transient outages.
    """
    if not url or not isinstance(url, str) or not url.strip():
        raise ModelRetry("fetch_url: url is required (absolute http(s) URL)")

    client = ctx.deps.http_client
    try:
        # ``stream=True`` lets us enforce the size cap without materializing
        # the entire body when the server is hostile / huge.
        async with client.stream(
            "GET",
            url,
            timeout=FETCH_URL_TIMEOUT_S,
            follow_redirects=True,
        ) as response:
            if not (200 <= response.status_code < 300):
                # Read body up to the cap for diagnostics, but don't return
                # it as success content.
                await response.aread()
                return FetchError(
                    error="http_error",
                    status=response.status_code,
                    url=url,
                    detail=f"non-2xx response: {response.status_code}",
                )

            buf = bytearray()
            async for chunk in response.aiter_bytes():
                buf.extend(chunk)
                if len(buf) > FETCH_URL_MAX_BYTES:
                    return FetchError(
                        error="body_too_large",
                        status=response.status_code,
                        url=url,
                        detail=(
                            f"response body exceeded {FETCH_URL_MAX_BYTES} bytes"
                        ),
                    )
        return bytes(buf).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - intentional broad catch
        # Network errors (DNS, TLS, timeouts, refused) all surface here.
        # We log them so operators can see them, and return a structured
        # error so the agent loop doesn't crash.
        logger.warning(
            "fetch_url failed: url=%s err=%s",
            url,
            exc.__class__.__name__,
        )
        return FetchError(
            error="http_error",
            status=None,
            url=url,
            detail=f"{exc.__class__.__name__}: {exc}",
        )


# ---------------------------------------------------------------------------
# Registration helper — called from build_agent.
# ---------------------------------------------------------------------------


def register_basic_tools(agent: Agent[CapoDeps, Any]) -> None:
    """Register :func:`web_search` and :func:`fetch_url` onto ``agent``.

    Kept as a free function (not a method on ``Agent``) so:

    * Phase-1 ``build_agent`` can call it unconditionally.
    * Tests can build a real :class:`pydantic_ai.Agent`, call this helper,
      and then introspect ``agent.toolsets`` / tool descriptions to verify
      registration and SOUL-anchoring.

    The decorators preserve the underlying functions, so callers can still
    invoke ``web_search`` / ``fetch_url`` directly in unit tests by
    constructing a ``RunContext`` manually.
    """
    agent.tool(web_search)
    agent.tool(fetch_url)


__all__ = [
    "FETCH_URL_MAX_BYTES",
    "FETCH_URL_TIMEOUT_S",
    "fetch_url",
    "register_basic_tools",
    "web_search",
]

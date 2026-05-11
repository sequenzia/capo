"""Pydantic AI ``deps_type`` for capo (spec §7.3 class diagram).

The spec's §7.3 class diagram defines a ``CapoDeps`` aggregate that every
agent tool receives via ``RunContext[CapoDeps]``. This module materializes
that contract for Phase 1 — only the fields that Phase-1 tools actually
need are required; later phases (Task #10 dispatcher, Task #11 worker, the
delegation tools in Phase 2/3) will populate the optional fields.

Design notes
------------

* **Dataclass, not Pydantic.** Pydantic AI only needs ``deps_type`` to be
  a type the user code can pattern on; it never serializes it. A frozen-ish
  dataclass keeps construction cheap and avoids re-running validation on
  every dispatcher invocation (the AMC envelope upstream is already
  validated).
* **Pluggable clients.** The two Phase-1 tools — ``web_search`` and
  ``fetch_url`` — depend on injectable client objects so tests can swap in
  fakes without monkey-patching modules. ``web_search_client`` is a small
  ``Protocol`` (no concrete provider in V1); ``http_client`` is just an
  ``httpx.AsyncClient`` (or anything quacking like one — see
  :class:`SupportsAsyncRequest`).
* **No SOUL exposure.** ``CapoDeps`` deliberately does **not** carry the
  composed SOUL/system instructions. SOUL is anchored in
  ``Agent(instructions=...)`` and must not leak into tool-side prompts
  (spec §5.1 acceptance criteria). Tools should reach for ``settings`` if
  they need configuration; never for instructions.
"""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:  # pragma: no cover - imported only for type hints
    import httpx

    from capo.config import Settings


# ---------------------------------------------------------------------------
# Public result/error models exchanged with the agent.
# ---------------------------------------------------------------------------


class SearchResult(BaseModel):
    """One web-search hit returned by the ``web_search`` tool.

    The shape is intentionally minimal — title, URL, snippet — so multiple
    backends (Brave, Tavily, Bing, etc.) can map onto it without leaking
    provider-specific noise into the agent's reasoning context.
    """

    model_config = ConfigDict(frozen=True)

    title: str
    url: str
    snippet: str


class FetchError(BaseModel):
    """Structured non-2xx / size-cap error returned by ``fetch_url``.

    Returning this (instead of raising) lets the agent reason about the
    failure in-band rather than aborting the tool-call loop. See spec §5.1
    edge-cases table and Task-14 acceptance criteria.
    """

    model_config = ConfigDict(frozen=True)

    error: str
    """Short machine code — currently ``"http_error"`` or ``"body_too_large"``."""

    status: int | None = None
    """HTTP status code when applicable; ``None`` for client-side caps."""

    url: str
    """The URL we attempted to fetch."""

    detail: str | None = None
    """Optional human-readable detail (e.g. truncated body size)."""


# ---------------------------------------------------------------------------
# Provider protocols — kept narrow so tests can supply lambdas / fakes.
# ---------------------------------------------------------------------------


@runtime_checkable
class WebSearchClient(Protocol):
    """Minimal web-search backend protocol consumed by ``capo.tools.basic``.

    Implementations may be sync or async; the tool awaits the return value
    via :func:`inspect.isawaitable` so a lambda returning a list works fine
    in tests. Returns ``list[SearchResult]`` in either case.
    """

    def search(
        self, query: str
    ) -> list[SearchResult] | Awaitable[list[SearchResult]]:  # pragma: no cover - protocol
        ...


# ---------------------------------------------------------------------------
# CapoDeps — the aggregate the agent loop ships down to every tool.
# ---------------------------------------------------------------------------


@dataclass
class CapoDeps:
    """Aggregate dependencies handed to every tool via ``RunContext``.

    Only the fields Phase-1 tools (``web_search``, ``fetch_url``) need are
    required. The remaining fields are optional placeholders for the
    Phase 2/3 delegation tools (DBOS handle, memory store, etc.) so callers
    can construct a ``CapoDeps`` incrementally as subsystems come online.

    Attributes
    ----------
    settings:
        Validated :class:`capo.config.Settings`. Tools that read config —
        e.g. ``shell_exec`` allowlist (Phase 2), ``fetch_url`` max-body
        size — pull from here.
    http_client:
        Shared :class:`httpx.AsyncClient` used by network tools. Sharing one
        client is the recommended httpx pattern (connection pool, HTTP/2,
        timeouts). Tests inject one wired to :class:`httpx.MockTransport`.
    web_search_client:
        Optional :class:`WebSearchClient`. If ``None``, the ``web_search``
        tool returns a structured error to the agent ("no provider
        configured") rather than crashing. V1 ships without a real provider;
        Phase 5 will land one.
    user_id:
        Resolved internal user id for the current AMC envelope
        (see :mod:`capo.transport.user_resolver`). Optional during boot;
        the dispatcher (Task #11) populates it per-invocation.
    workspaces_root, projects_root:
        Convenience pointers (mirror ``settings.paths.*``) used by
        delegation tools in later phases.
    """

    settings: Settings
    http_client: httpx.AsyncClient
    web_search_client: WebSearchClient | None = None
    user_id: str | None = None
    workspaces_root: Path | None = field(default=None)
    projects_root: Path | None = field(default=None)

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        http_client: httpx.AsyncClient,
        *,
        web_search_client: WebSearchClient | None = None,
        user_id: str | None = None,
    ) -> CapoDeps:
        """Convenience constructor that mirrors path fields from ``settings``.

        Saves the dispatcher from re-spelling ``settings.paths.*`` on every
        envelope.
        """
        return cls(
            settings=settings,
            http_client=http_client,
            web_search_client=web_search_client,
            user_id=user_id,
            workspaces_root=settings.paths.workspaces_root,
            projects_root=settings.paths.projects_root,
        )


__all__ = [
    "CapoDeps",
    "FetchError",
    "SearchResult",
    "WebSearchClient",
]

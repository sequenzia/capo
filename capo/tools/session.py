"""Shared helpers + NL agent tools for session control (spec §5.10, §5.11).

This module is the shared substrate behind the two surface area choices
the user has for session control:

* **Slash commands** routed by the dispatcher (Task #52): ``/new``,
  ``/status``, ``/clear``. These are pre-agent control messages — the
  dispatcher intercepts them BEFORE :class:`Agent.run` and replies via
  AMC. Implemented in :mod:`capo.transport.dispatcher`.
* **Natural-language tools** registered onto the agent (this Task #53):
  :func:`session_new`, :func:`session_status`, :func:`session_clear`.
  When the user says something like *"start a new session"* or *"what's
  my status?"* or *"clear my conversation"*, the LLM is free to pick the
  matching tool. The tools share the same DB helpers as the slash path
  so both surface areas produce the same side effect on ``state.db``.

Design notes
------------

* **Pure helpers vs. tools.** The synchronous DB helpers
  (:func:`select_active_session_id`, :func:`delete_thread_history`,
  :func:`end_active_session`, :func:`select_active_delegation_count`,
  :func:`select_conversation_count`) take a live :class:`sqlite3.Connection`
  and never touch :class:`pydantic_ai.RunContext`. The async helper
  :func:`read_daily_cost_usd` is also context-agnostic — it just wraps
  :func:`capo.costs.daily_total_usd`. These helpers are the canonical
  shared implementation; the dispatcher (Task #52) imports them via
  the private re-exports below for backwards compat.
* **Tools shape.** Each NL tool takes ``ctx: RunContext[CapoDeps]`` first
  (Pydantic AI injects deps automatically) and returns a ``dict[str, Any]``.
  Dicts are friendlier for the LLM than custom Pydantic models for
  these short status/confirmation payloads; they round-trip cleanly
  through the agent's tool-result channel.
* **No new tables / migrations.** Everything reads from / writes to
  the §7.3 ``sessions`` + ``conversation_history`` + ``delegations`` +
  ``costs`` tables that already exist (Task #5 / #6 / #7 / #48).
* **Edge cases.** ``session_new`` is a no-op when there's no active
  session (returns ``{"status": "no_active_session"}``); ``session_clear``
  on an empty thread returns ``{"cleared_count": 0, "status": "noop"}``.
  Both are surfaced verbatim so the LLM can phrase the user reply
  honestly.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic_ai import RunContext

from capo.deps import CapoDeps

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pydantic_ai import Agent

    from capo.config import Settings

logger = logging.getLogger("capo.tools.session")


# ---------------------------------------------------------------------------
# Synchronous DB helpers — shared by dispatcher slash handlers + NL tools.
# ---------------------------------------------------------------------------


def select_active_delegation_count(
    conn: sqlite3.Connection, user_id: str
) -> int:
    """Count ``delegations`` rows with ``status='running'`` for ``user_id``."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM delegations "
        "WHERE user_id = ? AND status = 'running'",
        (user_id,),
    ).fetchone()
    if row is None:
        return 0
    return int(row["n"])


def select_active_session_id(
    conn: sqlite3.Connection, user_id: str, thread_id: str
) -> str | None:
    """Return the active ``session_id`` for ``(user_id, thread_id)`` or ``None``."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT session_id FROM sessions "
        "WHERE user_id = ? AND thread_id = ? AND ended_at IS NULL",
        (user_id, thread_id),
    ).fetchone()
    if row is None:
        return None
    return str(row["session_id"])


def select_conversation_count(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> int:
    """Count messages in ``conversation_history`` for the active session."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM conversation_history "
        "WHERE user_id = ? AND thread_id = ? AND session_id = ?",
        (user_id, thread_id, session_id),
    ).fetchone()
    if row is None:
        return 0
    return int(row["n"])


def delete_thread_history(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
) -> int:
    """Delete all ``conversation_history`` rows for ``(user_id, thread_id)``.

    Returns the number of rows deleted (``0`` if the thread had no
    messages). Also ends the active session row for the thread (sets
    ``ended_at = NOW()``) so the next inbound message starts a fresh
    one. Mirrors the §5.10 slash-``/clear`` semantics.

    Wrapped in ``BEGIN IMMEDIATE`` so concurrent writers serialize
    cleanly (mirrors :func:`capo.memory.conversation.append_messages`).
    """
    # Local imports — hot path; keeps the import graph for the bare
    # tools module shallow.
    from capo.memory.conversation import _utcnow_iso
    from capo.memory.store import begin_immediate_with_retry

    operation = f"clear_thread:{thread_id}"
    with begin_immediate_with_retry(conn, operation=operation):
        cur = conn.execute(
            "DELETE FROM conversation_history "
            "WHERE user_id = ? AND thread_id = ?",
            (user_id, thread_id),
        )
        deleted = int(cur.rowcount)
        conn.execute(
            "UPDATE sessions SET ended_at = ? "
            "WHERE user_id = ? AND thread_id = ? AND ended_at IS NULL",
            (_utcnow_iso(), user_id, thread_id),
        )
    return deleted


def end_active_session(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
) -> bool:
    """End the active session row for ``(user_id, thread_id)``.

    Returns ``True`` when a row was flipped to ended, ``False`` when
    there was no active session. Mirrors the §5.10 slash-``/new``
    semantics — older conversation history is preserved on disk; only
    the active ``sessions`` row gets ``ended_at`` stamped.
    """
    from capo.memory.conversation import _utcnow_iso
    from capo.memory.store import begin_immediate_with_retry

    operation = f"end_session_for_new:{thread_id}"
    with begin_immediate_with_retry(conn, operation=operation):
        cur = conn.execute(
            "UPDATE sessions SET ended_at = ? "
            "WHERE user_id = ? AND thread_id = ? AND ended_at IS NULL",
            (_utcnow_iso(), user_id, thread_id),
        )
        return cur.rowcount > 0


async def read_daily_cost_usd(
    settings: Settings, user_id: str
) -> float | None:
    """Return today's USD cost total for ``user_id`` via Task #48 accountant.

    Returns ``None`` on any error so callers never crash because the cost
    accountant had a hiccup. The UTC calendar day is
    ``datetime.now(UTC).strftime("%Y-%m-%d")``.
    """
    try:
        from capo.costs import daily_total_usd

        db_path = settings.paths.db_path
        day_iso = datetime.now(UTC).strftime("%Y-%m-%d")
        total = await daily_total_usd(db_path, user_id, day_iso)
        return float(total)
    except Exception as exc:  # noqa: BLE001 - best-effort
        logger.debug(
            "session: cost accountant unavailable: %r",
            exc,
            extra={"event": "capo.tools.session.cost_unavailable"},
        )
        return None


# ---------------------------------------------------------------------------
# Internal helpers — resolve required deps from RunContext.
# ---------------------------------------------------------------------------


def _require_user_id(ctx: RunContext[CapoDeps]) -> str:
    """Return ``ctx.deps.user_id`` or raise a clear error.

    The session tools are scoped to the current user / thread (§5.11),
    so we never want to silently operate on the wrong record if the
    dispatcher forgot to populate the deps.
    """
    user_id = ctx.deps.user_id
    if not user_id:
        raise RuntimeError(
            "session tools require CapoDeps.user_id to be set; "
            "the dispatcher must populate it from the AMC envelope "
            "(see capo.transport.user_resolver)."
        )
    return user_id


def _require_thread_id(ctx: RunContext[CapoDeps]) -> str:
    """Return ``ctx.deps.thread_id`` or raise a clear error.

    Session tools always act on the active thread; without a thread_id
    the SQL would silently match no rows. Raising keeps the contract
    explicit.
    """
    thread_id = ctx.deps.thread_id
    if not thread_id:
        raise RuntimeError(
            "session tools require CapoDeps.thread_id to be set; "
            "the dispatcher must populate it from the AMC envelope "
            "(thread_id_for_amc(channel_id))."
        )
    return thread_id


def _open_conn(ctx: RunContext[CapoDeps]) -> sqlite3.Connection:
    """Open a fresh sqlite3 connection to ``state.db`` for one tool call."""
    from capo.memory.store import open_connection

    return open_connection(ctx.deps.settings.paths.db_path)


# ---------------------------------------------------------------------------
# Natural-language agent tools (spec §5.10 / §5.11).
# ---------------------------------------------------------------------------


async def session_new(ctx: RunContext[CapoDeps]) -> dict[str, Any]:
    """End the current conversation session so the next turn starts fresh.

    Use this when the user asks to "start a new session", "begin again",
    "reset the conversation context", or similar phrasing. Older
    conversation history is **retained on disk** — only the active
    ``sessions`` row is flipped to ended; the next agent turn will lazily
    open a fresh session id. This mirrors the ``/new`` slash command.

    Returns:
        A dict with keys:

        * ``status``: ``"ended"`` when an active session was flipped to
          ended, ``"no_active_session"`` when the thread had no active
          session row (no-op — still a valid outcome).
        * ``thread_id``: The thread id the action applied to.
    """
    user_id = _require_user_id(ctx)
    thread_id = _require_thread_id(ctx)

    def _do() -> bool:
        conn = _open_conn(ctx)
        try:
            return end_active_session(conn, user_id, thread_id)
        finally:
            conn.close()

    ended = await asyncio.to_thread(_do)
    return {
        "status": "ended" if ended else "no_active_session",
        "thread_id": thread_id,
    }


async def session_status(ctx: RunContext[CapoDeps]) -> dict[str, Any]:
    """Return a compact status summary for the current user + thread.

    Use this when the user asks about their current state — phrasings
    like "what's my status?", "how am I doing?", "show me my budget",
    "how many delegations are running?" — anything that wants the
    session-level snapshot. Mirrors the ``/status`` slash command.

    Returns:
        A dict with keys:

        * ``model``: Current orchestrator model id
          (``settings.models.default``).
        * ``active_delegations``: Count of ``status='running'`` rows in
          ``delegations`` for this user.
        * ``daily_cost_usd``: Today's UTC USD spend for this user
          (``None`` when the cost accountant is unavailable — e.g.
          legacy environments without the ``costs`` table).
        * ``conversation_message_count``: Rows in ``conversation_history``
          for the active session (``0`` when there is no active session
          yet).
        * ``thread_id``: The thread the snapshot applies to.
    """
    user_id = _require_user_id(ctx)
    thread_id = _require_thread_id(ctx)
    settings = ctx.deps.settings

    def _read() -> tuple[int, int]:
        conn = _open_conn(ctx)
        try:
            active = select_active_delegation_count(conn, user_id)
            sid = select_active_session_id(conn, user_id, thread_id)
            if sid is None:
                msg_count = 0
            else:
                msg_count = select_conversation_count(
                    conn, user_id, thread_id, sid
                )
            return active, msg_count
        finally:
            conn.close()

    active_delegations, conversation_message_count = await asyncio.to_thread(
        _read
    )
    daily_cost = await read_daily_cost_usd(settings, user_id)

    return {
        "model": settings.models.default,
        "active_delegations": active_delegations,
        "daily_cost_usd": daily_cost,
        "conversation_message_count": conversation_message_count,
        "thread_id": thread_id,
    }


async def session_clear(ctx: RunContext[CapoDeps]) -> dict[str, Any]:
    """Clear the conversation memory for this thread + end the active session.

    Use this when the user says "clear my conversation", "wipe my chat
    history", "start over with no memory", or similar — anything that
    asks for the thread to forget prior turns. Mirrors the ``/clear``
    slash command: deletes every row in ``conversation_history`` for
    ``(user_id, thread_id)`` AND ends the active session row.

    Returns:
        A dict with keys:

        * ``cleared_count``: Number of ``conversation_history`` rows
          deleted (``0`` when there was nothing to clear).
        * ``status``: ``"cleared"`` when rows were deleted,
          ``"noop"`` when the thread was already empty (still ends the
          session — same as the slash path).
        * ``thread_id``: The thread the action applied to.
    """
    user_id = _require_user_id(ctx)
    thread_id = _require_thread_id(ctx)

    def _do() -> int:
        conn = _open_conn(ctx)
        try:
            return delete_thread_history(conn, user_id, thread_id)
        finally:
            conn.close()

    cleared = await asyncio.to_thread(_do)
    return {
        "cleared_count": cleared,
        "status": "cleared" if cleared > 0 else "noop",
        "thread_id": thread_id,
    }


# ---------------------------------------------------------------------------
# Registration helper — called from build_agent via register_phase5_tools.
# ---------------------------------------------------------------------------


def register_session_tools(agent: Agent[CapoDeps, Any]) -> None:
    """Register :func:`session_new`, :func:`session_status`, :func:`session_clear`.

    Kept as a free function (not a method on ``Agent``) so:

    * :func:`capo.agent.build_agent` can call it unconditionally after
      the Phase-2 tools.
    * Tests can build a real :class:`pydantic_ai.Agent`, call this
      helper, and introspect ``agent.toolsets`` to verify registration.
    """
    agent.tool(session_new)
    agent.tool(session_status)
    agent.tool(session_clear)


__all__ = [
    "delete_thread_history",
    "end_active_session",
    "read_daily_cost_usd",
    "register_session_tools",
    "select_active_delegation_count",
    "select_active_session_id",
    "select_conversation_count",
    "session_clear",
    "session_new",
    "session_status",
]

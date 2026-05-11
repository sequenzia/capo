"""Per-(user_id, thread_id) ModelMessage history persistence.

Backed by the ``conversation_history`` table from spec §7.3. Messages are
serialized via Pydantic AI's :class:`ModelMessagesTypeAdapter` (the supported
public JSON adapter for the ``ModelMessage`` union) and stored one row per
message with a monotonically-increasing ``message_index`` scoped to
``(user_id, thread_id)``.

Spec references:
- §5.7 Conversation Memory (Per-Thread, Per-User).
- §7.3 Data Models → ``conversation_history`` table; PK ``(user_id, thread_id, message_index)``.
- §9.1 Phase 1 deliverable: "Conversation memory: per-thread, per-user".

Design notes:
- ``thread_id`` is opaque to this module; callers compose it via
  :func:`thread_id_for_amc` (``f"amc:{channel_id}"``).
- Reads (``load_history``) DO NOT take the retry helper — they are
  autocommit SELECTs. Writes (``append_messages``) go through
  :func:`begin_immediate_with_retry` so concurrent writers serialize cleanly
  (§7.3 retry contract).
- ``message_index`` is computed inside the write transaction as ``MAX+1`` over
  the existing rows for ``(user_id, thread_id)``. This is correct under
  BEGIN IMMEDIATE serialization (two concurrent writers can't both see the
  same MAX) and avoids needing a separate counter table.
- Each message is serialized individually (one row per message) by wrapping
  it in a singleton list before handing to the type adapter — the on-disk
  shape is therefore a JSON array of length one. This is intentional: it
  keeps row-level reads symmetric with the spec schema and lets compaction
  (#37) rewrite individual rows without touching neighbors.
"""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from capo.memory.store import begin_immediate_with_retry

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pydantic_ai.messages import ModelMessage


__all__ = [
    "append_messages",
    "append_messages_async",
    "end_session",
    "end_session_async",
    "get_or_create_active_session",
    "get_or_create_active_session_async",
    "load_history",
    "load_history_async",
    "thread_id_for_amc",
]


# ---------------------------------------------------------------------------
# Thread-id composition
# ---------------------------------------------------------------------------


def thread_id_for_amc(channel_id: str) -> str:
    """Compose the canonical AMC thread id (``amc:<channel_id>``) per §5.7."""
    if not channel_id:
        raise ValueError("channel_id must be non-empty")
    return f"amc:{channel_id}"


# ---------------------------------------------------------------------------
# Session lifecycle (§5.7 implicit creation, §7.3 sessions table)
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    """ISO-8601 UTC timestamp (microsecond precision) suitable for SQLite TIMESTAMP."""
    return datetime.now(UTC).isoformat()


def _select_active_session(
    conn: sqlite3.Connection, user_id: str, thread_id: str
) -> str | None:
    """Return the ``session_id`` of the active row for ``(user_id, thread_id)``, else ``None``.

    "Active" = ``ended_at IS NULL`` — the predicate enforced by the unique
    partial index ``idx_sessions_user_thread_active`` per §7.3.
    """
    row = conn.execute(
        "SELECT session_id FROM sessions "
        "WHERE user_id = ? AND thread_id = ? AND ended_at IS NULL",
        (user_id, thread_id),
    ).fetchone()
    return None if row is None else str(row[0])


def get_or_create_active_session(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
) -> str:
    """Return the active ``session_id`` for ``(user_id, thread_id)``, creating one if needed.

    On the first turn for a ``(user_id, thread_id)`` pair, inserts a fresh
    row in the ``sessions`` table with ``started_at = NOW()``,
    ``ended_at = NULL``, and ``compacted_to_message_index = 0`` per §5.7 and
    §7.3. Subsequent calls reuse the existing active session.

    Concurrency: the unique partial index ``idx_sessions_user_thread_active``
    guarantees at-most-one active session per ``(user_id, thread_id)``. If
    two callers race and one wins the INSERT, the loser catches
    :class:`sqlite3.IntegrityError` and re-SELECTs to discover the winner's
    ``session_id`` — both callers therefore return the same id.

    V1 imposes no implicit timeout: a session ends only via
    :func:`end_session` (wired in Phase 5 to ``/new`` and ``/clear``).
    """
    # Fast path: read outside the write transaction. If we hit it, no INSERT
    # is needed at all and we skip the retry helper.
    existing = _select_active_session(conn, user_id, thread_id)
    if existing is not None:
        return existing

    # Slow path: open a write transaction. Re-check inside the txn (another
    # caller may have raced ahead of us), then INSERT a fresh row. If the
    # unique partial index trips, a concurrent winner beat us — re-SELECT
    # under the same txn and return their ``session_id``.
    operation = f"create_session:{thread_id}"
    with begin_immediate_with_retry(conn, operation=operation):
        existing = _select_active_session(conn, user_id, thread_id)
        if existing is not None:
            return existing

        new_session_id = uuid.uuid4().hex
        try:
            conn.execute(
                "INSERT INTO sessions "
                "(session_id, thread_id, user_id, started_at, "
                "ended_at, compacted_to_message_index) "
                "VALUES (?, ?, ?, ?, NULL, 0)",
                (new_session_id, thread_id, user_id, _utcnow_iso()),
            )
        except sqlite3.IntegrityError:
            # Race: a concurrent writer inserted the active row between our
            # SELECT and INSERT. The partial unique index fired — re-SELECT
            # to discover their session_id.
            winner = _select_active_session(conn, user_id, thread_id)
            if winner is None:
                # Shouldn't happen — the IntegrityError implies a row exists
                # with ended_at IS NULL. Re-raise so the caller sees the bug.
                raise
            return winner
        return new_session_id


async def get_or_create_active_session_async(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
) -> str:
    """Async wrapper around :func:`get_or_create_active_session`."""
    return await asyncio.to_thread(
        get_or_create_active_session, conn, user_id, thread_id
    )


def end_session(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> bool:
    """End the named session by stamping ``ended_at = NOW()``. Returns whether a row updated.

    Idempotent: if the session is already ended (or never existed), this is
    a no-op and returns ``False``. Wired in Phase 5 by ``/new`` and ``/clear``
    (per task #16 description; spec §5.7).
    """
    operation = f"end_session:{thread_id}"
    with begin_immediate_with_retry(conn, operation=operation):
        cursor = conn.execute(
            "UPDATE sessions SET ended_at = ? "
            "WHERE session_id = ? AND user_id = ? AND thread_id = ? "
            "AND ended_at IS NULL",
            (_utcnow_iso(), session_id, user_id, thread_id),
        )
        return cursor.rowcount > 0


async def end_session_async(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> bool:
    """Async wrapper around :func:`end_session`."""
    return await asyncio.to_thread(
        end_session, conn, user_id, thread_id, session_id
    )


# ---------------------------------------------------------------------------
# Lazy import helper
# ---------------------------------------------------------------------------


def _type_adapter():  # type: ignore[no-untyped-def]
    """Lazy-import :class:`ModelMessagesTypeAdapter`.

    Keeps ``pydantic_ai`` off the import path for callers that only touch
    :func:`thread_id_for_amc`, mirroring the lazy-import pattern in
    ``capo.agent``.
    """
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    return ModelMessagesTypeAdapter


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------


def load_history(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> list[ModelMessage]:
    """Return all messages for the given session in ``message_index`` order.

    Selects from ``conversation_history`` scoped to
    ``(user_id, thread_id, session_id)``. Each row's ``model_message_json``
    is deserialized via :class:`ModelMessagesTypeAdapter` and the resulting
    single-element list is unwrapped.

    A session with no rows yet (e.g. the very first turn) returns ``[]``.
    """
    rows = conn.execute(
        "SELECT model_message_json FROM conversation_history "
        "WHERE user_id = ? AND thread_id = ? AND session_id = ? "
        "ORDER BY message_index ASC",
        (user_id, thread_id, session_id),
    ).fetchall()

    if not rows:
        return []

    adapter = _type_adapter()
    out: list[ModelMessage] = []
    for (payload,) in rows:
        # Each row stores a single-message JSON array — unwrap it.
        messages = adapter.validate_json(payload)
        out.extend(messages)
    return out


async def load_history_async(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> list[ModelMessage]:
    """Async wrapper around :func:`load_history` (runs in a worker thread)."""
    return await asyncio.to_thread(load_history, conn, user_id, thread_id, session_id)


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


def append_messages(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
    messages: Sequence[ModelMessage],
) -> int:
    """Append ``messages`` to the per-thread history. Returns rows written.

    Wraps the INSERT batch in :func:`begin_immediate_with_retry` so two
    concurrent writers for the same ``(user_id, thread_id)`` serialize via
    the §7.3 retry helper (no SQLITE_BUSY surfaces).

    The next ``message_index`` is computed inside the transaction as
    ``MAX(message_index) + 1`` for the ``(user_id, thread_id)`` pair, which
    is correct under BEGIN IMMEDIATE because only one writer holds the write
    lock at a time.

    Empty ``messages`` is a no-op and returns 0 (still opens a transaction —
    callers that want to skip the round-trip should check ``len()`` first).
    """
    if not messages:
        return 0

    adapter = _type_adapter()
    # Pre-serialize each message individually so the transaction is short.
    # Each payload is a JSON array of length one (the on-disk shape per the
    # module docstring).
    payloads: list[str] = []
    for msg in messages:
        raw = adapter.dump_json([msg])
        payloads.append(raw.decode("utf-8"))

    operation = f"append_messages:{thread_id}"
    written = 0
    with begin_immediate_with_retry(conn, operation=operation):
        row = conn.execute(
            "SELECT COALESCE(MAX(message_index), -1) "
            "FROM conversation_history WHERE user_id = ? AND thread_id = ?",
            (user_id, thread_id),
        ).fetchone()
        next_index = int(row[0]) + 1

        rows_to_insert = [
            (user_id, thread_id, next_index + offset, session_id, payload)
            for offset, payload in enumerate(payloads)
        ]
        conn.executemany(
            "INSERT INTO conversation_history "
            "(user_id, thread_id, message_index, session_id, model_message_json) "
            "VALUES (?, ?, ?, ?, ?)",
            rows_to_insert,
        )
        written = len(rows_to_insert)
    return written


async def append_messages_async(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
    messages: Sequence[ModelMessage],
) -> int:
    """Async wrapper around :func:`append_messages` (runs in a worker thread)."""
    return await asyncio.to_thread(
        append_messages, conn, user_id, thread_id, session_id, messages
    )

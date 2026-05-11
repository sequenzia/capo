"""Tests for the implicit session lifecycle (Task #16).

Covers spec §5.7 (per-thread sessions, no implicit timeout), §7.3 (sessions
table + ``idx_sessions_user_thread_active`` partial unique index), and §9.1
Phase 1 deliverable: "Session boundary ends only via ``/new`` or ``/clear``".

Schema is set up by re-executing the migration's verbatim ``_UPGRADE_STATEMENTS``
against a temp SQLite DB (the same fast-test pattern test_conversation.py uses
— see Task #15 note).
"""

from __future__ import annotations

import asyncio
import importlib.util as _ilu
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "migrations" / "versions"))

_MIG_PATH = (
    Path(__file__).resolve().parent.parent / "migrations" / "versions" / "001_init.py"
)
_spec = _ilu.spec_from_file_location("_mig_001_init_session", _MIG_PATH)
assert _spec is not None and _spec.loader is not None
_mig = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mig)

from capo.memory import store  # noqa: E402
from capo.memory.conversation import (  # noqa: E402
    end_session,
    end_session_async,
    get_or_create_active_session,
    get_or_create_active_session_async,
    thread_id_for_amc,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Hardened SQLite connection with the §7.3 schema applied + seed users."""
    db = tmp_path / "state.db"
    c = store.open_connection(db)
    _apply_inline_schema(c)
    c.execute("INSERT INTO users(user_id) VALUES ('alice')")
    c.execute("INSERT INTO users(user_id) VALUES ('bob')")
    yield c
    c.close()


def _count_sessions(
    conn: sqlite3.Connection, user_id: str, thread_id: str, *, active_only: bool = False
) -> int:
    sql = "SELECT COUNT(*) FROM sessions WHERE user_id = ? AND thread_id = ?"
    if active_only:
        sql += " AND ended_at IS NULL"
    return int(conn.execute(sql, (user_id, thread_id)).fetchone()[0])


# ---------------------------------------------------------------------------
# Functional: first-message creates a session
# ---------------------------------------------------------------------------


def test_first_call_creates_session_row(conn: sqlite3.Connection) -> None:
    """First call for (user_id, thread_id) inserts exactly one ``sessions`` row.

    Row invariants per §7.3: ``ended_at IS NULL``, ``compacted_to_message_index = 0``,
    ``started_at`` non-NULL.
    """
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")

    assert _count_sessions(conn, user_id, thread_id) == 0

    session_id = get_or_create_active_session(conn, user_id, thread_id)
    assert isinstance(session_id, str)
    assert session_id  # non-empty

    rows = conn.execute(
        "SELECT session_id, thread_id, user_id, started_at, ended_at, "
        "compacted_to_message_index FROM sessions "
        "WHERE user_id = ? AND thread_id = ?",
        (user_id, thread_id),
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == session_id
    assert row[1] == thread_id
    assert row[2] == user_id
    assert row[3] is not None  # started_at populated
    assert row[4] is None  # ended_at NULL
    assert row[5] == 0  # compacted_to_message_index


def test_subsequent_calls_reuse_active_session(conn: sqlite3.Connection) -> None:
    """Subsequent calls return the same ``session_id`` — no new row inserted."""
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")

    s1 = get_or_create_active_session(conn, user_id, thread_id)
    s2 = get_or_create_active_session(conn, user_id, thread_id)
    s3 = get_or_create_active_session(conn, user_id, thread_id)
    assert s1 == s2 == s3
    assert _count_sessions(conn, user_id, thread_id) == 1


def test_distinct_threads_get_distinct_sessions(conn: sqlite3.Connection) -> None:
    """Distinct ``thread_id`` for the same user → distinct sessions."""
    t1 = thread_id_for_amc("c1")
    t2 = thread_id_for_amc("c2")
    s1 = get_or_create_active_session(conn, "alice", t1)
    s2 = get_or_create_active_session(conn, "alice", t2)
    assert s1 != s2
    assert _count_sessions(conn, "alice", t1, active_only=True) == 1
    assert _count_sessions(conn, "alice", t2, active_only=True) == 1


def test_distinct_users_get_distinct_sessions(conn: sqlite3.Connection) -> None:
    """Same ``thread_id`` for distinct users → distinct sessions (FK on user_id)."""
    thread_id = thread_id_for_amc("shared")
    s_alice = get_or_create_active_session(conn, "alice", thread_id)
    s_bob = get_or_create_active_session(conn, "bob", thread_id)
    assert s_alice != s_bob


# ---------------------------------------------------------------------------
# Functional: V1 has no implicit timeout — session ends only via end_session
# ---------------------------------------------------------------------------


def test_end_session_then_get_creates_new_session(conn: sqlite3.Connection) -> None:
    """After :func:`end_session`, the next ``get_or_create`` creates a NEW row.

    Mirrors the Phase-5 ``/new`` and ``/clear`` flow (§5.7).
    """
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")

    first = get_or_create_active_session(conn, user_id, thread_id)
    updated = end_session(conn, user_id, thread_id, first)
    assert updated is True

    # The previous row is now historical (ended_at populated).
    ended_row = conn.execute(
        "SELECT ended_at FROM sessions WHERE session_id = ?", (first,)
    ).fetchone()
    assert ended_row[0] is not None

    # No active row remains.
    assert _count_sessions(conn, user_id, thread_id, active_only=True) == 0

    # A new call creates a fresh active session with a NEW id.
    second = get_or_create_active_session(conn, user_id, thread_id)
    assert second != first
    assert _count_sessions(conn, user_id, thread_id) == 2
    assert _count_sessions(conn, user_id, thread_id, active_only=True) == 1


def test_end_session_is_idempotent(conn: sqlite3.Connection) -> None:
    """Calling :func:`end_session` twice → second call is a no-op (returns False)."""
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")
    sid = get_or_create_active_session(conn, user_id, thread_id)
    assert end_session(conn, user_id, thread_id, sid) is True
    assert end_session(conn, user_id, thread_id, sid) is False


def test_end_session_unknown_returns_false(conn: sqlite3.Connection) -> None:
    """Ending a non-existent session is a no-op (returns False) — no error raised."""
    assert end_session(conn, "alice", thread_id_for_amc("c1"), "nope") is False


# ---------------------------------------------------------------------------
# Edge case: partial unique index rejects a second active row
# ---------------------------------------------------------------------------


def test_partial_unique_index_rejects_second_active(conn: sqlite3.Connection) -> None:
    """``idx_sessions_user_thread_active`` MUST reject a second ``ended_at IS NULL`` row.

    Integration-level invariant: the index is the serialization point that
    makes :func:`get_or_create_active_session` safe under concurrency.
    """
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")
    conn.execute(
        "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
        ("s-first", thread_id, user_id),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
            ("s-second", thread_id, user_id),
        )


def test_partial_unique_index_allows_after_ended(conn: sqlite3.Connection) -> None:
    """Once the first row is ``ended_at != NULL``, a second active row IS allowed."""
    user_id = "alice"
    thread_id = thread_id_for_amc("c1")
    conn.execute(
        "INSERT INTO sessions(session_id, thread_id, user_id, ended_at) "
        "VALUES (?, ?, ?, ?)",
        ("s-old", thread_id, user_id, "2026-01-01T00:00:00+00:00"),
    )
    # Allowed: only one ACTIVE row at a time, but historical rows can stack.
    conn.execute(
        "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
        ("s-new", thread_id, user_id),
    )
    assert _count_sessions(conn, user_id, thread_id) == 2
    assert _count_sessions(conn, user_id, thread_id, active_only=True) == 1


# ---------------------------------------------------------------------------
# Concurrency: racing first-message attempts converge on one active session
# ---------------------------------------------------------------------------


async def test_concurrent_first_calls_serialize(tmp_path: Path) -> None:
    """Four asyncio tasks racing on the same (user, thread) MUST end with exactly
    one active session, and all callers MUST observe the same ``session_id``.

    Each task owns its own connection (sqlite3 connections aren't thread-safe).
    The BEGIN IMMEDIATE retry helper + partial unique index together guarantee
    convergence: the winner's INSERT succeeds, every loser either sees the
    winner's row on the in-txn re-SELECT, or trips the index and falls back
    to a final SELECT.
    """
    db = tmp_path / "state.db"
    setup = store.open_connection(db)
    try:
        _apply_inline_schema(setup)
        setup.execute("INSERT INTO users(user_id) VALUES ('alice')")
    finally:
        setup.close()

    user_id = "alice"
    thread_id = thread_id_for_amc("c1")

    async def racer() -> str:
        c = store.open_connection(db)
        try:
            return await get_or_create_active_session_async(c, user_id, thread_id)
        finally:
            c.close()

    results = await asyncio.gather(*(racer() for _ in range(4)))

    # All callers see the same session_id.
    assert len(set(results)) == 1

    # And exactly one active row exists.
    reader = store.open_connection(db)
    try:
        assert _count_sessions(reader, user_id, thread_id) == 1
        assert _count_sessions(reader, user_id, thread_id, active_only=True) == 1
        row = reader.execute(
            "SELECT session_id FROM sessions WHERE user_id = ? AND thread_id = ?",
            (user_id, thread_id),
        ).fetchone()
        assert row[0] == results[0]
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# Async wrappers smoke test
# ---------------------------------------------------------------------------


async def test_async_wrappers_basic(tmp_path: Path) -> None:
    """``get_or_create_active_session_async`` + ``end_session_async`` work end-to-end."""
    db = tmp_path / "state.db"
    c = store.open_connection(db)
    try:
        _apply_inline_schema(c)
        c.execute("INSERT INTO users(user_id) VALUES ('alice')")

        thread_id = thread_id_for_amc("c1")
        sid1 = await get_or_create_active_session_async(c, "alice", thread_id)
        sid2 = await get_or_create_active_session_async(c, "alice", thread_id)
        assert sid1 == sid2

        ended = await end_session_async(c, "alice", thread_id, sid1)
        assert ended is True

        sid3 = await get_or_create_active_session_async(c, "alice", thread_id)
        assert sid3 != sid1
    finally:
        c.close()

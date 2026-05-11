"""Tests for the per-thread conversation memory DAO (Task #15).

Covers spec §5.7 acceptance criteria and the §13 risk note (S-4 spike):
ModelMessage JSON round-trip must be lossless across our persistence layer.

Schema is set up by importing the canonical CREATE TABLE statements from
``migrations/versions/001_init.py`` and executing them against a temp SQLite
DB — exercising the *exact* DDL the production migration ships, without
paying the ~3-5s ``alembic upgrade head`` subprocess cost on every test.

We also have a dedicated alembic-driven coverage test
(``test_conversation_with_alembic_migrated_db``) that proves the module is
compatible with the migration-applied schema end-to-end.
"""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

# We import the constants from the migration to keep test DDL in lock-step
# with what the alembic migration applies in production.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "migrations" / "versions"))
import importlib.util as _ilu

_MIG_PATH = (
    Path(__file__).resolve().parent.parent / "migrations" / "versions" / "001_init.py"
)
_spec = _ilu.spec_from_file_location("_mig_001_init", _MIG_PATH)
assert _spec is not None and _spec.loader is not None
_mig = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mig)

from capo.memory import store  # noqa: E402  (sys.path tweak above)
from capo.memory.conversation import (  # noqa: E402
    append_messages,
    append_messages_async,
    load_history,
    load_history_async,
    thread_id_for_amc,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    """Apply the §7.3 DDL by re-executing the migration's verbatim statements."""
    for stmt in _mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Hardened SQLite connection with the §7.3 schema applied."""
    db = tmp_path / "state.db"
    c = store.open_connection(db)
    _apply_inline_schema(c)
    # Seed two users referenced by tests.
    c.execute("INSERT INTO users(user_id) VALUES ('alice')")
    c.execute("INSERT INTO users(user_id) VALUES ('bob')")
    yield c
    c.close()


def _make_session(
    conn: sqlite3.Connection, *, user_id: str, thread_id: str, session_id: str
) -> None:
    """Insert a row in ``sessions`` so the FK on ``conversation_history`` holds."""
    conn.execute(
        "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
        (session_id, thread_id, user_id),
    )


def _msgs(user_text: str, assistant_text: str):
    """Build a realistic (request, response) pair using Pydantic AI primitives."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    return [
        ModelRequest(parts=[UserPromptPart(content=user_text)]),
        ModelResponse(parts=[TextPart(content=assistant_text)]),
    ]


# ---------------------------------------------------------------------------
# thread_id_for_amc
# ---------------------------------------------------------------------------


def test_thread_id_for_amc_returns_amc_prefixed_id() -> None:
    """``thread_id_for_amc(channel_id)`` MUST return ``amc:<channel_id>`` (§5.7)."""
    assert thread_id_for_amc("chan-42") == "amc:chan-42"
    assert thread_id_for_amc("+15551234567") == "amc:+15551234567"


def test_thread_id_for_amc_rejects_empty() -> None:
    with pytest.raises(ValueError):
        thread_id_for_amc("")


# ---------------------------------------------------------------------------
# JSON round-trip (anchors S-4 risk note in §13)
# ---------------------------------------------------------------------------


def test_modelmessage_json_roundtrip_via_type_adapter() -> None:
    """``ModelMessagesTypeAdapter`` round-trip MUST be lossless (===)."""
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    original = _msgs("hello", "hi there") + _msgs("what is 2+2?", "4")
    blob = ModelMessagesTypeAdapter.dump_json(original)
    decoded = ModelMessagesTypeAdapter.validate_json(blob)
    assert decoded == original
    # And dumping the decoded list again must produce identical bytes
    # (idempotent serialization — important for content-addressing later).
    assert ModelMessagesTypeAdapter.dump_json(decoded) == blob


def test_modelmessage_persistence_roundtrip(conn: sqlite3.Connection) -> None:
    """End-to-end: append → load returns equal ModelMessage objects (== equality)."""
    user_id, thread_id, session_id = "alice", thread_id_for_amc("c1"), "s1"
    _make_session(conn, user_id=user_id, thread_id=thread_id, session_id=session_id)

    original = _msgs("hello", "hi there")
    n = append_messages(conn, user_id, thread_id, session_id, original)
    assert n == 2

    loaded = load_history(conn, user_id, thread_id, session_id)
    assert loaded == original


# ---------------------------------------------------------------------------
# Schema invariants
# ---------------------------------------------------------------------------


def test_composite_primary_key_enforced(conn: sqlite3.Connection) -> None:
    """PK ``(user_id, thread_id, message_index)`` MUST reject duplicates."""
    user_id, thread_id, session_id = "alice", thread_id_for_amc("c1"), "s1"
    _make_session(conn, user_id=user_id, thread_id=thread_id, session_id=session_id)

    conn.execute(
        "INSERT INTO conversation_history "
        "(user_id, thread_id, message_index, session_id, model_message_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (user_id, thread_id, 0, session_id, "[]"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO conversation_history "
            "(user_id, thread_id, message_index, session_id, model_message_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, thread_id, 0, session_id, "[]"),
        )


# ---------------------------------------------------------------------------
# Append + load ordering across turns
# ---------------------------------------------------------------------------


def test_append_load_preserves_order_across_turns(conn: sqlite3.Connection) -> None:
    """Three append batches → load returns 6 messages in correct order."""
    user_id, thread_id, session_id = "alice", thread_id_for_amc("c1"), "s1"
    _make_session(conn, user_id=user_id, thread_id=thread_id, session_id=session_id)

    batch1 = _msgs("turn1-q", "turn1-a")
    batch2 = _msgs("turn2-q", "turn2-a")
    batch3 = _msgs("turn3-q", "turn3-a")
    assert append_messages(conn, user_id, thread_id, session_id, batch1) == 2
    assert append_messages(conn, user_id, thread_id, session_id, batch2) == 2
    assert append_messages(conn, user_id, thread_id, session_id, batch3) == 2

    loaded = load_history(conn, user_id, thread_id, session_id)
    assert loaded == batch1 + batch2 + batch3

    # Index column must be 0..5 contiguously.
    indices = [
        row[0]
        for row in conn.execute(
            "SELECT message_index FROM conversation_history "
            "WHERE user_id=? AND thread_id=? ORDER BY message_index",
            (user_id, thread_id),
        )
    ]
    assert indices == [0, 1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Isolation — threads / users
# ---------------------------------------------------------------------------


def test_two_threads_dont_bleed(conn: sqlite3.Connection) -> None:
    """Different channel_ids → different thread_ids → isolated histories."""
    user_id = "alice"
    t1, t2 = thread_id_for_amc("c1"), thread_id_for_amc("c2")
    _make_session(conn, user_id=user_id, thread_id=t1, session_id="s1")
    _make_session(conn, user_id=user_id, thread_id=t2, session_id="s2")

    b1 = _msgs("c1-q", "c1-a")
    b2 = _msgs("c2-q", "c2-a")
    append_messages(conn, user_id, t1, "s1", b1)
    append_messages(conn, user_id, t2, "s2", b2)

    assert load_history(conn, user_id, t1, "s1") == b1
    assert load_history(conn, user_id, t2, "s2") == b2
    # Cross-thread read MUST be empty (would catch a mistaken WHERE clause).
    assert load_history(conn, user_id, t1, "s2") == []
    assert load_history(conn, user_id, t2, "s1") == []


def test_two_users_dont_bleed(conn: sqlite3.Connection) -> None:
    """Same channel_id but different user_ids → isolated histories."""
    thread_id = thread_id_for_amc("shared-channel")
    _make_session(conn, user_id="alice", thread_id=thread_id, session_id="s-alice")
    _make_session(conn, user_id="bob", thread_id=thread_id, session_id="s-bob")

    alice_msgs = _msgs("alice-q", "alice-a")
    bob_msgs = _msgs("bob-q", "bob-a")
    append_messages(conn, "alice", thread_id, "s-alice", alice_msgs)
    append_messages(conn, "bob", thread_id, "s-bob", bob_msgs)

    assert load_history(conn, "alice", thread_id, "s-alice") == alice_msgs
    assert load_history(conn, "bob", thread_id, "s-bob") == bob_msgs
    # Cross-user reads MUST be empty.
    assert load_history(conn, "alice", thread_id, "s-bob") == []
    assert load_history(conn, "bob", thread_id, "s-alice") == []


# ---------------------------------------------------------------------------
# First message — empty load (§5.7 edge case)
# ---------------------------------------------------------------------------


def test_first_turn_returns_empty(conn: sqlite3.Connection) -> None:
    """A brand-new (user, thread, session) returns ``[]`` from ``load_history``."""
    user_id, thread_id, session_id = "alice", thread_id_for_amc("c1"), "s1"
    _make_session(conn, user_id=user_id, thread_id=thread_id, session_id=session_id)

    assert load_history(conn, user_id, thread_id, session_id) == []


def test_load_unknown_thread_returns_empty(conn: sqlite3.Connection) -> None:
    """Reading a (user, thread, session) with no inserts returns ``[]``."""
    assert load_history(conn, "alice", thread_id_for_amc("nope"), "nope-s") == []


def test_append_empty_messages_is_noop(conn: sqlite3.Connection) -> None:
    """``append_messages([])`` is a no-op and returns 0 — no rows written."""
    user_id, thread_id, session_id = "alice", thread_id_for_amc("c1"), "s1"
    _make_session(conn, user_id=user_id, thread_id=thread_id, session_id=session_id)
    assert append_messages(conn, user_id, thread_id, session_id, []) == 0
    assert load_history(conn, user_id, thread_id, session_id) == []


# ---------------------------------------------------------------------------
# Concurrent writers serialize via the retry helper (§7.3)
# ---------------------------------------------------------------------------


async def test_concurrent_writers_serialize(tmp_path: Path) -> None:
    """Two asyncio tasks each appending 10 batches → all 40 messages persisted.

    Each task owns its OWN connection (sqlite3 connections aren't thread-safe);
    the BEGIN IMMEDIATE retry helper in ``store.py`` ensures no SQLITE_BUSY
    leaks even when both tasks race.
    """
    db = tmp_path / "state.db"
    # Set up schema + users on one connection, then close — both tasks open fresh.
    setup = store.open_connection(db)
    try:
        _apply_inline_schema(setup)
        setup.execute("INSERT INTO users(user_id) VALUES ('alice')")
        setup.execute(
            "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
            ("s1", thread_id_for_amc("c1"), "alice"),
        )
    finally:
        setup.close()

    async def writer(label: str) -> None:
        c = store.open_connection(db)
        try:
            for i in range(10):
                batch = _msgs(f"{label}-q{i}", f"{label}-a{i}")
                await append_messages_async(
                    c, "alice", thread_id_for_amc("c1"), "s1", batch
                )
        finally:
            c.close()

    await asyncio.gather(writer("A"), writer("B"))

    # Verify on a fresh connection.
    reader = store.open_connection(db)
    try:
        history = await load_history_async(
            reader, "alice", thread_id_for_amc("c1"), "s1"
        )
        # 10 batches × 2 messages each × 2 writers = 40 messages.
        assert len(history) == 40

        # Indexes MUST be 0..39 contiguous (the retry helper guarantees this).
        indices = [
            row[0]
            for row in reader.execute(
                "SELECT message_index FROM conversation_history "
                "WHERE user_id='alice' AND thread_id=? ORDER BY message_index",
                (thread_id_for_amc("c1"),),
            )
        ]
        assert indices == list(range(40))

        # Within each writer's contributions, ordering MUST be monotonic.
        a_seen, b_seen = [], []
        for msg in history:
            # Inspect the request part if present.
            if hasattr(msg, "parts"):
                for part in msg.parts:
                    content = getattr(part, "content", None)
                    if isinstance(content, str):
                        if content.startswith("A-q"):
                            a_seen.append(int(content.removeprefix("A-q")))
                        elif content.startswith("B-q"):
                            b_seen.append(int(content.removeprefix("B-q")))
        assert a_seen == sorted(a_seen) == list(range(10))
        assert b_seen == sorted(b_seen) == list(range(10))
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# Alembic-migrated DB compatibility (slow; skipped if `uv` missing)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_conversation_with_alembic_migrated_db(tmp_path: Path) -> None:
    """Append+load works against a schema produced by ``alembic upgrade head``."""
    db = tmp_path / "state.db"
    import os

    keep = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "LC_ALL")
    env = {k: v for k, v in os.environ.items() if k in keep}
    env["CAPO_STATE_DB"] = str(db)
    result = subprocess.run(  # noqa: S603 — fixed argv
        ["uv", "run", "alembic", "upgrade", "head"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    c = store.open_connection(db)
    try:
        user_id = f"user-{uuid.uuid4().hex[:8]}"
        thread_id = thread_id_for_amc("c-alembic")
        session_id = f"s-{uuid.uuid4().hex[:8]}"
        c.execute("INSERT INTO users(user_id) VALUES (?)", (user_id,))
        c.execute(
            "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
            (session_id, thread_id, user_id),
        )

        batch = _msgs("hello", "hi")
        assert append_messages(c, user_id, thread_id, session_id, batch) == 2
        assert load_history(c, user_id, thread_id, session_id) == batch
    finally:
        c.close()

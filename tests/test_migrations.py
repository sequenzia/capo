"""Tests for the Alembic ``state.db`` migrations (Task 8).

Covers the Phase-1 acceptance criteria from §7.3 / §9.1:

- ``alembic upgrade head`` creates every Phase-1 table and index against an
  empty SQLite file.
- The migration is idempotent — running ``upgrade head`` a second time
  succeeds without error and without duplicating objects.
- Foreign-key constraints are enforced at runtime (insert into ``delegations``
  with a non-existent ``user_id`` raises ``IntegrityError``).
- Cross-table inserts that satisfy FKs succeed.
- The ``approvals`` table is added by migration ``002_approvals`` (Phase 4 /
  task #38); detailed introspection lives in ``tests/test_approvals_migration.py``.
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

# Repo root = three levels up from this file: tests/ -> repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Spec-mandated table set after ``alembic upgrade head``
# (001_init + 002_approvals + 004_costs; 003 only modifies an existing CHECK).
EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "users",
        "sessions",
        "conversation_history",
        "delegations",
        "delegation_output",
        "delegation_heartbeats",
        "daily_costs",
        "approvals",
        "costs",
    }
)

EXPECTED_INDEXES: frozenset[str] = frozenset(
    {
        "idx_sessions_user_thread_active",
        "idx_history_session",
        "idx_delegations_user_status",
        "idx_delegations_started",
        "idx_output_delegation_ts",
        "idx_approvals_pending_sweep",
        "idx_approvals_parent_thread",
        "idx_costs_parent_thread_recorded",
        "idx_costs_user_recorded",
    }
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_alembic_upgrade(db_path: Path) -> subprocess.CompletedProcess[str]:
    """Invoke ``alembic upgrade head`` against ``db_path``.

    We shell out to the real CLI (rather than the programmatic API) so the
    test exercises the exact pathway operators will use.
    """
    return subprocess.run(  # noqa: S603 — fixed argv, no shell expansion
        ["uv", "run", "alembic", "upgrade", "head"],
        cwd=str(REPO_ROOT),
        env={
            **_clean_env(),
            "CAPO_STATE_DB": str(db_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )


def _clean_env() -> dict[str, str]:
    """Build a minimal env preserving PATH and HOME for ``uv``."""
    import os

    keep = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "LC_ALL")
    return {k: v for k, v in os.environ.items() if k in keep}


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fresh SQLite file for each test."""
    return tmp_path / "state.db"


@pytest.fixture
def fk_conn(db_path: Path) -> sqlite3.Connection:
    """Connection with FKs enforced — matches runtime pragma list."""
    result = _run_alembic_upgrade(db_path)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_creates_all_tables_and_indexes(db_path: Path) -> None:
    """``alembic upgrade head`` produces every spec-mandated object."""
    result = _run_alembic_upgrade(db_path)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "AND name <> 'alembic_version'"
            )
        }
        assert tables == EXPECTED_TABLES, (
            f"table set mismatch: missing={EXPECTED_TABLES - tables}, "
            f"extra={tables - EXPECTED_TABLES}"
        )

        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert EXPECTED_INDEXES.issubset(indexes), (
            f"missing indexes: {EXPECTED_INDEXES - indexes}"
        )
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_is_idempotent(db_path: Path) -> None:
    """Running ``upgrade head`` twice succeeds without error."""
    first = _run_alembic_upgrade(db_path)
    assert first.returncode == 0, first.stderr

    second = _run_alembic_upgrade(db_path)
    assert second.returncode == 0, (
        f"second upgrade failed:\nSTDOUT:\n{second.stdout}\n"
        f"STDERR:\n{second.stderr}"
    )

    # Table set must remain identical after a re-apply.
    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "AND name <> 'alembic_version'"
            )
        }
        assert tables == EXPECTED_TABLES
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_fk_enforced_for_delegation_without_user(fk_conn: sqlite3.Connection) -> None:
    """Inserting a delegation whose ``user_id`` is unknown raises IntegrityError."""
    with pytest.raises(sqlite3.IntegrityError):
        fk_conn.execute(
            "INSERT INTO delegations "
            "(id, user_id, agent, workspace, brief_json, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "nobody", "claude-code", "/tmp/ws", "{}", "running"),
        )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_satisfied_fk_chain_inserts_succeed(fk_conn: sqlite3.Connection) -> None:
    """A full user → session → conversation_history → delegation chain commits."""
    fk_conn.execute("INSERT INTO users (user_id) VALUES (?)", ("alice",))
    fk_conn.execute(
        "INSERT INTO sessions (session_id, thread_id, user_id) "
        "VALUES (?, ?, ?)",
        ("s1", "t1", "alice"),
    )
    fk_conn.execute(
        "INSERT INTO conversation_history "
        "(user_id, thread_id, message_index, session_id, model_message_json) "
        "VALUES (?, ?, ?, ?, ?)",
        ("alice", "t1", 0, "s1", '{"role": "user"}'),
    )
    fk_conn.execute(
        "INSERT INTO delegations "
        "(id, user_id, agent, workspace, brief_json, status) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("d1", "alice", "claude-code", "/tmp/ws", "{}", "running"),
    )

    assert fk_conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 1
    assert fk_conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1
    assert (
        fk_conn.execute("SELECT COUNT(*) FROM conversation_history").fetchone()[0] == 1
    )
    assert fk_conn.execute("SELECT COUNT(*) FROM delegations").fetchone()[0] == 1


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_unique_active_session_per_user_thread(fk_conn: sqlite3.Connection) -> None:
    """Partial unique index forbids two open sessions on the same (user, thread)."""
    fk_conn.execute("INSERT INTO users (user_id) VALUES (?)", ("alice",))
    fk_conn.execute(
        "INSERT INTO sessions (session_id, thread_id, user_id) "
        "VALUES (?, ?, ?)",
        ("s1", "t1", "alice"),
    )

    with pytest.raises(sqlite3.IntegrityError):
        fk_conn.execute(
            "INSERT INTO sessions (session_id, thread_id, user_id) "
            "VALUES (?, ?, ?)",
            ("s2", "t1", "alice"),
        )

    # But once the first session is closed, a new one is allowed.
    fk_conn.execute(
        "UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE session_id = 's1'"
    )
    fk_conn.execute(
        "INSERT INTO sessions (session_id, thread_id, user_id) "
        "VALUES (?, ?, ?)",
        ("s2", "t1", "alice"),
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_delegation_check_constraints(fk_conn: sqlite3.Connection) -> None:
    """CHECK constraints on ``agent`` and ``status`` reject invalid values."""
    fk_conn.execute("INSERT INTO users (user_id) VALUES (?)", ("alice",))

    with pytest.raises(sqlite3.IntegrityError):
        fk_conn.execute(
            "INSERT INTO delegations "
            "(id, user_id, agent, workspace, brief_json, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d1", "alice", "bogus", "/tmp/ws", "{}", "running"),
        )

    with pytest.raises(sqlite3.IntegrityError):
        fk_conn.execute(
            "INSERT INTO delegations "
            "(id, user_id, agent, workspace, brief_json, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("d2", "alice", "claude-code", "/tmp/ws", "{}", "weird"),
        )

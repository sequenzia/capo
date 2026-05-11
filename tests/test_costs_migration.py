"""Tests for the ``costs`` table migration (Task #48, Phase 5).

Covers the Phase-5 acceptance criteria from §5.9 / §6.5 / §9.5 + task #48:

- ``alembic upgrade head`` creates the ``costs`` table + both indexes.
- The migration is idempotent — running ``upgrade head`` a second time
  is a no-op past the first apply.
- ``downgrade -1`` drops the costs artifacts and leaves the rest intact.
- A full ``downgrade base`` → ``upgrade head`` round-trip lands on
  identical schema.
- Schema introspection: columns + indexes match the spec verbatim.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

# Repo root = parent of ``tests/``.
REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Expected schema — kept in lockstep with ``migrations/versions/004_costs.py``
# ---------------------------------------------------------------------------

# (column_name, type, notnull, dflt_value, pk)
EXPECTED_COSTS_COLUMNS: tuple[tuple[str, str, int, str | None, int], ...] = (
    ("cost_id", "INTEGER", 0, None, 1),
    ("recorded_at", "TEXT", 1, None, 0),
    ("user_id", "TEXT", 0, None, 0),
    ("parent_thread_id", "TEXT", 0, None, 0),
    ("model", "TEXT", 1, None, 0),
    ("input_tokens", "INTEGER", 1, None, 0),
    ("output_tokens", "INTEGER", 1, None, 0),
    ("cached_tokens", "INTEGER", 0, "0", 0),
    ("cost_usd", "REAL", 1, None, 0),
    ("span_id", "TEXT", 0, None, 0),
)

EXPECTED_PARENT_THREAD_RECORDED: tuple[str, ...] = (
    "parent_thread_id",
    "recorded_at",
)
EXPECTED_USER_RECORDED: tuple[str, ...] = ("user_id", "recorded_at")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_env() -> dict[str, str]:
    keep = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "LC_ALL")
    return {k: v for k, v in os.environ.items() if k in keep}


def _run_alembic(
    db_path: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 — fixed argv
        ["uv", "run", "alembic", *args],
        cwd=str(REPO_ROOT),
        env={**_clean_env(), "CAPO_STATE_DB": str(db_path)},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


@pytest.fixture
def head_db(db_path: Path) -> Path:
    result = _run_alembic(db_path, "upgrade", "head")
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    return db_path


# ---------------------------------------------------------------------------
# Integration: upgrade / downgrade / idempotent / round-trip
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_creates_costs_table(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='costs'"
        ).fetchall()
        assert rows == [("costs",)]
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_creates_cost_indexes(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        rows = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND tbl_name='costs' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert rows == {
            "idx_costs_parent_thread_recorded",
            "idx_costs_user_recorded",
        }, f"unexpected index set: {rows}"
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_downgrade_drops_costs_table(head_db: Path) -> None:
    """``alembic downgrade -1`` drops costs and leaves rest intact."""
    result = _run_alembic(head_db, "downgrade", "-1")
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        costs = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='costs'"
        ).fetchall()
        assert costs == []

        cost_indexes = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name LIKE 'idx_costs_%'"
        ).fetchall()
        assert cost_indexes == []

        # Other tables still present.
        remaining = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "AND name <> 'alembic_version'"
            )
        }
        assert "approvals" in remaining
        assert "delegations" in remaining
        assert "users" in remaining
        assert "costs" not in remaining
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_round_trip_base_then_head_is_clean(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        before = sorted(
            conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
            ).fetchall()
        )
    finally:
        conn.close()

    down = _run_alembic(head_db, "downgrade", "base")
    assert down.returncode == 0, down.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        residual = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
        ).fetchall()
        assert residual == []
    finally:
        conn.close()

    up = _run_alembic(head_db, "upgrade", "head")
    assert up.returncode == 0, up.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        after = sorted(
            conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
            ).fetchall()
        )
    finally:
        conn.close()

    assert before == after, "schema mismatch after roundtrip"


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_is_idempotent(head_db: Path) -> None:
    second = _run_alembic(head_db, "upgrade", "head")
    assert second.returncode == 0, second.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='costs'"
        ).fetchall()
        assert rows == [("costs",)]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_costs_columns_match_spec(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute("PRAGMA table_info(costs)").fetchall()
    finally:
        conn.close()

    observed = tuple((r[1], r[2], r[3], r[4], r[5]) for r in rows)
    assert observed == EXPECTED_COSTS_COLUMNS, (
        f"costs columns mismatch:\nobserved: {observed}\n"
        f"expected: {EXPECTED_COSTS_COLUMNS}"
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_costs_parent_thread_index_columns(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "PRAGMA index_info('idx_costs_parent_thread_recorded')"
        ).fetchall()
    finally:
        conn.close()

    observed = tuple(r[2] for r in rows)
    assert observed == EXPECTED_PARENT_THREAD_RECORDED


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_costs_user_index_columns(head_db: Path) -> None:
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "PRAGMA index_info('idx_costs_user_recorded')"
        ).fetchall()
    finally:
        conn.close()

    observed = tuple(r[2] for r in rows)
    assert observed == EXPECTED_USER_RECORDED


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_costs_autoincrement_unique(head_db: Path) -> None:
    """``cost_id`` autoincrements and never duplicates within a session."""
    conn = sqlite3.connect(str(head_db), isolation_level=None)
    try:
        for idx in range(5):
            conn.execute(
                "INSERT INTO costs ("
                "recorded_at, model, input_tokens, output_tokens, cost_usd"
                ") VALUES (?, ?, ?, ?, ?)",
                ("2026-05-11T00:00:00Z", "claude-sonnet-4-6", idx, idx, 0.0),
            )
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT cost_id FROM costs ORDER BY cost_id ASC"
            )
        ]
        assert len(ids) == 5
        assert len(set(ids)) == 5  # all distinct
        # AUTOINCREMENT guarantees monotonic growth.
        assert ids == sorted(ids)
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_costs_nullable_fields(head_db: Path) -> None:
    """``user_id``, ``parent_thread_id``, ``span_id`` accept NULL."""
    conn = sqlite3.connect(str(head_db), isolation_level=None)
    try:
        conn.execute(
            "INSERT INTO costs ("
            "recorded_at, model, input_tokens, output_tokens, cost_usd"
            ") VALUES (?, ?, ?, ?, ?)",
            ("2026-05-11T00:00:00Z", "claude-sonnet-4-6", 100, 50, 0.001),
        )
        row = conn.execute(
            "SELECT user_id, parent_thread_id, span_id, cached_tokens "
            "FROM costs"
        ).fetchone()
        assert row[0] is None  # user_id
        assert row[1] is None  # parent_thread_id
        assert row[2] is None  # span_id
        assert row[3] == 0  # cached_tokens default
    finally:
        conn.close()

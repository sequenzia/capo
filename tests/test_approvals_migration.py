"""Tests for the ``approvals`` table migration (Task #38, Phase 4).

Covers the Phase-4 acceptance criteria from §5.8 / §5.9 / §9.4:

- ``alembic upgrade head`` creates the ``approvals`` table + both indexes
  against an empty SQLite file.
- The migration is idempotent — running ``upgrade head`` a second time
  succeeds without error and without duplicating objects.
- The migration is reversible — ``downgrade -1`` drops only the approvals
  artifacts and leaves the rest of the Phase-1 schema intact.
- A full round-trip (``downgrade base`` → ``upgrade head``) lands on an
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
# Expected schema — kept in lockstep with ``migrations/versions/002_approvals.py``
# ---------------------------------------------------------------------------

# (column_name, type, notnull, dflt_value, pk)
EXPECTED_APPROVALS_COLUMNS: tuple[tuple[str, str, int, str | None, int], ...] = (
    ("approval_id", "TEXT", 0, None, 1),  # PK columns report notnull=0 in pragma
    ("requested_at", "TEXT", 1, None, 0),
    ("decided_at", "TEXT", 0, None, 0),
    ("request_type", "TEXT", 1, None, 0),
    ("request_payload", "TEXT", 1, None, 0),
    ("requester_user_id", "TEXT", 0, None, 0),
    ("parent_thread_id", "TEXT", 0, None, 0),
    ("status", "TEXT", 1, None, 0),
    ("resolved_by", "TEXT", 0, None, 0),
    ("reason", "TEXT", 0, None, 0),
    ("workflow_id", "TEXT", 0, None, 0),
)

EXPECTED_PENDING_SWEEP_COLUMNS: tuple[str, ...] = ("status", "requested_at")
EXPECTED_PARENT_THREAD_COLUMNS: tuple[str, ...] = ("parent_thread_id", "status")

EXPECTED_REQUEST_TYPES: frozenset[str] = frozenset(
    {
        "shell_exec",
        "delegate_out_of_root",
        "delegate_dangerous_sandbox",
        "kill_delegation",
    }
)
EXPECTED_STATUSES: frozenset[str] = frozenset(
    {"pending", "approved", "denied", "expired", "cancelled"}
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_env() -> dict[str, str]:
    """Minimal env preserving PATH/HOME for ``uv``."""
    keep = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "LC_ALL")
    return {k: v for k, v in os.environ.items() if k in keep}


def _run_alembic(
    db_path: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    """Invoke ``alembic <args>`` against ``db_path`` via ``uv run``."""
    return subprocess.run(  # noqa: S603 — fixed argv, no shell expansion
        ["uv", "run", "alembic", *args],
        cwd=str(REPO_ROOT),
        env={**_clean_env(), "CAPO_STATE_DB": str(db_path)},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fresh SQLite file for each test."""
    return tmp_path / "state.db"


@pytest.fixture
def head_db(db_path: Path) -> Path:
    """Apply ``alembic upgrade head`` against a clean DB and return its path."""
    result = _run_alembic(db_path, "upgrade", "head")
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nSTDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    return db_path


# ---------------------------------------------------------------------------
# Integration: upgrade head + downgrade base round-trip
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_creates_approvals_table(head_db: Path) -> None:
    """``alembic upgrade head`` produces the ``approvals`` table."""
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='approvals'"
        ).fetchall()
        assert rows == [("approvals",)], (
            "approvals table missing after `alembic upgrade head`"
        )
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_creates_approval_indexes(head_db: Path) -> None:
    """Both spec-mandated indexes are created on the ``approvals`` table."""
    conn = sqlite3.connect(str(head_db))
    try:
        rows = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND tbl_name='approvals' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert rows == {
            "idx_approvals_pending_sweep",
            "idx_approvals_parent_thread",
        }, f"unexpected index set: {rows}"
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_downgrade_drops_approvals_table(head_db: Path) -> None:
    """``alembic downgrade 001_init`` drops the approvals table and indexes.

    With migration 003 (Task #43) stacked on top of 002, we need to
    downgrade past BOTH approvals migrations to land back on the
    Phase-1 schema. ``downgrade 001_init`` is the canonical "drop
    approvals" step (drops 003 then 002).
    """
    result = _run_alembic(head_db, "downgrade", "001_init")
    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        approvals = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='approvals'"
        ).fetchall()
        assert approvals == [], "approvals table not dropped on downgrade"

        approval_indexes = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name LIKE 'idx_approvals_%'"
        ).fetchall()
        assert approval_indexes == [], (
            f"approvals indexes survived downgrade: {approval_indexes}"
        )

        # Phase-1 tables still present.
        remaining = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "AND name <> 'alembic_version'"
            )
        }
        assert "delegations" in remaining
        assert "users" in remaining
        assert "approvals" not in remaining
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_round_trip_base_then_head_is_clean(head_db: Path) -> None:
    """``downgrade base`` then ``upgrade head`` lands on identical schema."""
    # Snapshot pre-roundtrip schema.
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

    # downgrade base
    down = _run_alembic(head_db, "downgrade", "base")
    assert down.returncode == 0, down.stderr

    # Everything (except alembic_version itself) should be gone.
    conn = sqlite3.connect(str(head_db))
    try:
        residual = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
        ).fetchall()
        assert residual == [], f"downgrade base left residue: {residual}"
    finally:
        conn.close()

    # upgrade head
    up = _run_alembic(head_db, "upgrade", "head")
    assert up.returncode == 0, up.stderr

    # Schema must match the pre-roundtrip snapshot.
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

    assert before == after, "schema mismatch after downgrade-base/upgrade-head"


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_upgrade_head_is_idempotent(head_db: Path) -> None:
    """Running ``upgrade head`` twice succeeds without error or duplication."""
    second = _run_alembic(head_db, "upgrade", "head")
    assert second.returncode == 0, second.stderr

    conn = sqlite3.connect(str(head_db))
    try:
        # Approvals table still present exactly once.
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='approvals'"
        ).fetchall()
        assert rows == [("approvals",)]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit: schema introspection — columns + indexes match spec verbatim
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_columns_match_spec(head_db: Path) -> None:
    """``PRAGMA table_info`` matches the spec'd column shape for approvals."""
    conn = sqlite3.connect(str(head_db))
    try:
        # PRAGMA returns: (cid, name, type, notnull, dflt_value, pk)
        rows = conn.execute("PRAGMA table_info(approvals)").fetchall()
    finally:
        conn.close()

    observed = tuple((r[1], r[2], r[3], r[4], r[5]) for r in rows)
    assert observed == EXPECTED_APPROVALS_COLUMNS, (
        f"approvals columns mismatch:\nobserved: {observed}\n"
        f"expected: {EXPECTED_APPROVALS_COLUMNS}"
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_pending_sweep_index_columns(head_db: Path) -> None:
    """``idx_approvals_pending_sweep`` is on ``(status, requested_at)`` in order."""
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "PRAGMA index_info('idx_approvals_pending_sweep')"
        ).fetchall()
    finally:
        conn.close()

    # PRAGMA returns: (seqno, cid, name)
    observed = tuple(r[2] for r in rows)
    assert observed == EXPECTED_PENDING_SWEEP_COLUMNS, (
        f"pending-sweep index columns mismatch: {observed}"
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_parent_thread_index_columns(head_db: Path) -> None:
    """``idx_approvals_parent_thread`` is on ``(parent_thread_id, status)`` in order."""
    conn = sqlite3.connect(str(head_db))
    try:
        rows = conn.execute(
            "PRAGMA index_info('idx_approvals_parent_thread')"
        ).fetchall()
    finally:
        conn.close()

    observed = tuple(r[2] for r in rows)
    assert observed == EXPECTED_PARENT_THREAD_COLUMNS, (
        f"parent-thread index columns mismatch: {observed}"
    )


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_status_check_constraint(head_db: Path) -> None:
    """The ``status`` CHECK constraint accepts every spec value and rejects others."""
    conn = sqlite3.connect(str(head_db), isolation_level=None)
    try:
        for idx, status in enumerate(sorted(EXPECTED_STATUSES)):
            conn.execute(
                "INSERT INTO approvals "
                "(approval_id, requested_at, request_type, request_payload, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    f"a-status-{idx}",
                    "2026-05-11T00:00:00Z",
                    "shell_exec",
                    "{}",
                    status,
                ),
            )

        # Bogus status must be rejected.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO approvals "
                "(approval_id, requested_at, request_type, request_payload, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "a-bogus",
                    "2026-05-11T00:00:00Z",
                    "shell_exec",
                    "{}",
                    "not-a-status",
                ),
            )
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_request_type_check_constraint(head_db: Path) -> None:
    """The ``request_type`` CHECK constraint accepts every spec value and rejects others."""
    conn = sqlite3.connect(str(head_db), isolation_level=None)
    try:
        for idx, rt in enumerate(sorted(EXPECTED_REQUEST_TYPES)):
            conn.execute(
                "INSERT INTO approvals "
                "(approval_id, requested_at, request_type, request_payload, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"a-rt-{idx}", "2026-05-11T00:00:00Z", rt, "{}", "pending"),
            )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO approvals "
                "(approval_id, requested_at, request_type, request_payload, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "a-rt-bad",
                    "2026-05-11T00:00:00Z",
                    "not-a-request-type",
                    "{}",
                    "pending",
                ),
            )
    finally:
        conn.close()


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv not on PATH")
def test_approvals_no_fk_to_delegations(head_db: Path) -> None:
    """Per §5.8: approvals MUST NOT have a FK to delegations."""
    conn = sqlite3.connect(str(head_db))
    try:
        # PRAGMA foreign_key_list returns FK definitions for the named table.
        rows = conn.execute("PRAGMA foreign_key_list(approvals)").fetchall()
    finally:
        conn.close()

    assert rows == [], (
        f"approvals must not declare FKs (any linkage is via "
        f"request_payload per §5.8); observed: {rows}"
    )

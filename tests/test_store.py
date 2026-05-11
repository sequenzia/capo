"""Tests for :mod:`capo.memory.store` (Task 7).

Covers the §7.3 acceptance criteria:

- Pragmas applied at every connection.
- ``BEGIN IMMEDIATE`` retry helper backs off, retries, and gives up at the
  5s wall-clock deadline with :class:`StoreUnavailable`.
- Non-lock ``OperationalError`` propagates immediately (no sleep).
- Batched insert uses parameterized queries + flush triggers.
- Connection cleanup on caller error (ROLLBACK fires).
- Concurrent writers from multiple asyncio tasks do not surface
  ``SQLITE_BUSY`` (matches §6.1).
- :class:`StoreUnavailable` carries operation + last_error context.
"""

from __future__ import annotations

import asyncio
import random
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from capo.memory import store
from capo.memory.store import (
    BatchedInserter,
    StoreUnavailable,
    batched_insert,
    begin_immediate_with_retry,
    open_connection,
    read_pragmas,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


@pytest.fixture
def conn(db_path: Path) -> sqlite3.Connection:
    c = open_connection(db_path)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Pragmas
# ---------------------------------------------------------------------------


def test_pragmas_applied_on_open(conn: sqlite3.Connection) -> None:
    """Every pragma listed in §7.3 is set after opening."""
    pragmas = read_pragmas(conn)
    assert pragmas["journal_mode"] == "wal"
    assert pragmas["synchronous"] == 1  # NORMAL
    assert pragmas["fullfsync"] == 1
    assert pragmas["busy_timeout"] == 5000
    assert pragmas["journal_size_limit"] == 67108864
    assert pragmas["foreign_keys"] == 1


def test_pragmas_applied_on_second_open(db_path: Path) -> None:
    """A second connection (same file) also gets the full pragma list."""
    c1 = open_connection(db_path)
    try:
        c2 = open_connection(db_path)
        try:
            assert read_pragmas(c2) == read_pragmas(c1)
            # And the bits that are *per-connection* (not WAL-shared) are set
            # on the new connection regardless.
            assert read_pragmas(c2)["foreign_keys"] == 1
            assert read_pragmas(c2)["busy_timeout"] == 5000
        finally:
            c2.close()
    finally:
        c1.close()


# ---------------------------------------------------------------------------
# Retry helper — happy path
# ---------------------------------------------------------------------------


def test_begin_immediate_commits_on_success(
    conn: sqlite3.Connection, db_path: Path
) -> None:
    """On clean exit the transaction commits."""
    conn.execute("CREATE TABLE t (x INTEGER)")
    with begin_immediate_with_retry(conn, operation="insert"):
        conn.execute("INSERT INTO t VALUES (1)")

    # Read back from a fresh connection — proves the COMMIT landed.
    c2 = open_connection(db_path)
    try:
        rows = c2.execute("SELECT x FROM t").fetchall()
        assert rows == [(1,)]
    finally:
        c2.close()


def test_begin_immediate_rolls_back_on_caller_error(
    conn: sqlite3.Connection, db_path: Path
) -> None:
    """If the caller raises mid-txn, changes ROLLBACK and the conn stays open."""
    conn.execute("CREATE TABLE t (x INTEGER)")

    class _Boom(RuntimeError):
        pass

    with (
        pytest.raises(_Boom),
        begin_immediate_with_retry(conn, operation="insert"),
    ):
        conn.execute("INSERT INTO t VALUES (99)")
        raise _Boom()

    # The insert must not have committed.
    c2 = open_connection(db_path)
    try:
        rows = c2.execute("SELECT x FROM t").fetchall()
        assert rows == []
    finally:
        c2.close()

    # And the connection is still usable.
    with begin_immediate_with_retry(conn, operation="insert2"):
        conn.execute("INSERT INTO t VALUES (1)")
    assert conn.execute("SELECT x FROM t").fetchall() == [(1,)]


# ---------------------------------------------------------------------------
# Retry helper — lock backoff
# ---------------------------------------------------------------------------


class _FakeClock:
    """Deterministic monotonic + sleep that advances a virtual clock."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


def _make_lock_error() -> sqlite3.OperationalError:
    return sqlite3.OperationalError("database is locked")


def test_retry_helper_backs_off_and_eventually_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lock errors trigger jittered exponential backoff; eventually succeeds."""
    fake_conn = _FakeConn(fail_n=3)
    clock = _FakeClock()
    rng = random.Random(42)

    with begin_immediate_with_retry(
        fake_conn,  # type: ignore[arg-type]
        operation="op",
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        rng=rng,
    ):
        pass

    # Three failures → three backoff sleeps, then BEGIN IMMEDIATE succeeds.
    assert fake_conn.begin_calls == 4
    assert len(clock.sleeps) == 3

    # Backoffs must be roughly 50ms, 100ms, 200ms within ±25% jitter.
    expected_bases = [0.050, 0.100, 0.200]
    for actual, base in zip(clock.sleeps, expected_bases, strict=True):
        assert base * 0.75 <= actual <= base * 1.25, (actual, base)


def test_retry_helper_raises_store_unavailable_at_deadline() -> None:
    """After the 5s deadline the helper raises StoreUnavailable with context."""
    fake_conn = _FakeConn(fail_n=10_000)
    clock = _FakeClock()
    rng = random.Random(0)

    with (
        pytest.raises(StoreUnavailable) as ei,
        begin_immediate_with_retry(
            fake_conn,  # type: ignore[arg-type]
            operation="insert_chunk",
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            rng=rng,
        ),
    ):
        pass

    err = ei.value
    assert err.operation == "insert_chunk"
    assert isinstance(err.last_error, sqlite3.OperationalError)
    assert err.attempts >= 1
    assert err.elapsed_s >= 5.0
    # last_error message preserved.
    assert "locked" in str(err.last_error)


def test_retry_helper_non_lock_operational_error_propagates_immediately() -> None:
    """A non-"database is locked" OperationalError must not retry."""
    err = sqlite3.OperationalError("no such table: missing")
    fake_conn = _FakeConn(custom_error=err)
    clock = _FakeClock()

    with (
        pytest.raises(sqlite3.OperationalError) as ei,
        begin_immediate_with_retry(
            fake_conn,  # type: ignore[arg-type]
            operation="op",
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        ),
    ):
        pass

    assert ei.value is err
    # Critical: no retry, no sleep.
    assert clock.sleeps == []
    assert fake_conn.begin_calls == 1


# ---------------------------------------------------------------------------
# StoreUnavailable structure
# ---------------------------------------------------------------------------


def test_store_unavailable_carries_operation_and_last_error() -> None:
    """Exception preserves the operation label + last sqlite error."""
    inner = sqlite3.OperationalError("database is locked")
    err = StoreUnavailable(operation="batched_insert:delegation_output", last_error=inner)
    assert err.operation == "batched_insert:delegation_output"
    assert err.last_error is inner
    assert "batched_insert:delegation_output" in str(err)
    # It's a real Exception subclass.
    assert isinstance(err, Exception)


# ---------------------------------------------------------------------------
# Batched insert
# ---------------------------------------------------------------------------


def test_batched_insert_writes_all_rows(
    conn: sqlite3.Connection, db_path: Path
) -> None:
    """One-shot batched_insert flushes every row."""
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")
    rows = [(i, f"v{i}") for i in range(250)]
    total = batched_insert(
        conn, "t", ["a", "b"], rows, flush_count=64, flush_interval_s=None
    )
    assert total == 250

    c2 = open_connection(db_path)
    try:
        assert c2.execute("SELECT COUNT(*) FROM t").fetchone() == (250,)
        # Roundtrip a sample row to prove parameterized binding worked.
        assert c2.execute("SELECT b FROM t WHERE a=?", (42,)).fetchone() == ("v42",)
    finally:
        c2.close()


def test_batched_inserter_count_trigger_flushes(
    conn: sqlite3.Connection,
) -> None:
    """Reaching flush_count drains the buffer mid-stream."""
    conn.execute("CREATE TABLE t (x INTEGER)")
    bi = BatchedInserter(
        conn, "t", ["x"], flush_count=5, flush_interval_s=None
    )
    flushed_at = []
    for i in range(12):
        if bi.add((i,)):
            flushed_at.append(i)
    # After 5 and 10 rows we should have flushed; 2 still pending.
    assert flushed_at == [4, 9]
    assert bi.pending == 2
    bi.flush()
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone() == (12,)


def test_batched_inserter_interval_trigger_flushes(
    conn: sqlite3.Connection,
) -> None:
    """Interval trigger flushes even when count threshold not reached."""
    conn.execute("CREATE TABLE t (x INTEGER)")
    fake = _FakeClock()
    bi = BatchedInserter(
        conn,
        "t",
        ["x"],
        flush_count=1_000,
        flush_interval_s=1.0,
        monotonic=fake.monotonic,
    )
    bi.add((1,))
    assert bi.pending == 1
    # Advance the clock past the interval.
    fake.t += 1.5
    flushed = bi.add((2,))
    assert flushed is True
    assert bi.pending == 0
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone() == (2,)


def test_batched_inserter_rejects_bad_row_shape(
    conn: sqlite3.Connection,
) -> None:
    """Row arity mismatch is caught before reaching SQLite."""
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
    bi = BatchedInserter(conn, "t", ["a", "b"])
    with pytest.raises(ValueError):
        bi.add((1,))


def test_batched_inserter_validates_constructor_args(
    conn: sqlite3.Connection,
) -> None:
    with pytest.raises(ValueError):
        BatchedInserter(conn, "t", [])
    with pytest.raises(ValueError):
        BatchedInserter(conn, "t", ["x"], flush_count=0)
    with pytest.raises(ValueError):
        BatchedInserter(conn, "t", ["x"], flush_interval_s=0)


# ---------------------------------------------------------------------------
# Concurrent writers (asyncio) — §6.1 invariant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_writers_never_surface_sqlite_busy(
    tmp_path: Path,
) -> None:
    """N asyncio writers each doing M inserts → zero SQLITE_BUSY to caller."""
    db = tmp_path / "concurrent.db"

    # Seed the schema on a setup connection, then close it.
    setup = open_connection(db)
    try:
        setup.execute("CREATE TABLE chunks (writer INTEGER, idx INTEGER, payload TEXT)")
    finally:
        setup.close()

    n_writers = 20
    per_writer = 50

    def worker(writer_id: int) -> None:
        conn = open_connection(db)
        try:
            for i in range(per_writer):
                with begin_immediate_with_retry(conn, operation=f"w{writer_id}"):
                    conn.execute(
                        "INSERT INTO chunks (writer, idx, payload) VALUES (?,?,?)",
                        (writer_id, i, f"payload-{writer_id}-{i}"),
                    )
        finally:
            conn.close()

    tasks = [asyncio.to_thread(worker, w) for w in range(n_writers)]
    # If any writer surfaced SQLITE_BUSY, gather() re-raises here.
    await asyncio.gather(*tasks)

    verify = open_connection(db)
    try:
        count = verify.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    finally:
        verify.close()
    assert count == n_writers * per_writer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeConn:
    """Minimal stand-in for sqlite3.Connection used by retry-logic tests.

    Counts ``BEGIN IMMEDIATE`` attempts and raises a configurable number of
    "database is locked" OperationalErrors before succeeding. Other ``execute``
    calls (e.g. COMMIT/ROLLBACK) are no-ops.
    """

    def __init__(
        self,
        *,
        fail_n: int = 0,
        custom_error: BaseException | None = None,
    ) -> None:
        self._fail_n = fail_n
        self._custom_error = custom_error
        self.begin_calls = 0
        self.executed: list[str] = []

    def execute(self, sql: str, *params: Any) -> Any:
        if sql.upper().startswith("BEGIN IMMEDIATE"):
            self.begin_calls += 1
            if self._custom_error is not None:
                raise self._custom_error
            if self.begin_calls <= self._fail_n:
                raise sqlite3.OperationalError("database is locked")
        self.executed.append(sql)
        return None


def test_module_exports_public_surface() -> None:
    """All documented names are importable from capo.memory."""
    from capo import memory

    for name in (
        "open_connection",
        "aopen_connection",
        "begin_immediate_with_retry",
        "batched_insert",
        "StoreUnavailable",
    ):
        assert hasattr(memory, name), name
    # And the store module re-exports the same names.
    assert store.open_connection is memory.open_connection

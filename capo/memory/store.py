"""SQLite hardening, BEGIN IMMEDIATE retry helper, batched insert.

This module is the canonical implementation of the §7.3 *Retry Helper Contract*
and the §6.1 invariant that ``SQLITE_BUSY`` MUST NEVER surface to the caller.

The public surface is intentionally small and synchronous; async callers (e.g.
the per-channel dispatcher, #11) should wrap calls in :func:`asyncio.to_thread`
or use the :func:`aopen_connection` convenience.

Spec references:
- §6.1 Performance Requirements ("SQLite write contention" row).
- §7.3 Data Models → "Retry Helper Contract" + canonical pragma list.
- §9.1 Phase 1 deliverable: "SQLite hardening".
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import sqlite3
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Pragma contract (spec §7.3 — exact strings, do not rephrase)
# ---------------------------------------------------------------------------

#: Pragmas applied to every connection at open. Order matters: ``journal_mode``
#: must be set before ``synchronous`` and ``foreign_keys`` to land WAL cleanly.
_PRAGMAS: tuple[tuple[str, str | int], ...] = (
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("fullfsync", "ON"),
    ("busy_timeout", 5000),
    ("journal_size_limit", 67108864),
    ("foreign_keys", "ON"),
)

#: Expected post-open values, used by tests and by the optional verify step.
EXPECTED_PRAGMAS: dict[str, Any] = {
    "journal_mode": "wal",  # sqlite normalizes case on read
    "synchronous": 1,  # NORMAL == 1
    "fullfsync": 1,  # ON == 1
    "busy_timeout": 5000,
    "journal_size_limit": 67108864,
    "foreign_keys": 1,  # ON == 1
}

# ---------------------------------------------------------------------------
# Retry helper constants (spec §7.3 — Retry Helper Contract)
# ---------------------------------------------------------------------------

#: Initial backoff in seconds (50 ms).
_INITIAL_BACKOFF_S: float = 0.050

#: Exponential factor.
_BACKOFF_FACTOR: float = 2.0

#: Jitter range, ±25%.
_JITTER_FRAC: float = 0.25

#: Total wall-clock deadline (matches ``busy_timeout=5000ms``).
_DEADLINE_S: float = 5.000

#: Substring used to detect a busy/lock OperationalError. SQLite's wire text
#: varies slightly across versions; both observed forms include this token.
_LOCK_MARKER: str = "database is locked"


# ---------------------------------------------------------------------------
# Typed exception
# ---------------------------------------------------------------------------


@dataclass
class StoreUnavailable(Exception):
    """Raised when the BEGIN IMMEDIATE retry budget is exhausted.

    Attributes:
        operation: Caller-supplied label identifying the failing write path
            (e.g. ``"insert_delegation_output"``). Surfaces to logs/metrics.
        last_error: The final :class:`sqlite3.OperationalError` we gave up on.
        attempts: Number of attempts made (including the first try).
        elapsed_s: Wall-clock seconds spent retrying.
    """

    operation: str
    last_error: BaseException | None = None
    attempts: int = 0
    elapsed_s: float = 0.0
    # Keep a positional message slot so ``raise StoreUnavailable("...")`` keeps
    # working for ad-hoc call sites that don't care about structured context.
    message: str = field(default="")

    def __post_init__(self) -> None:
        if not self.message:
            self.message = (
                f"store unavailable: operation={self.operation!r} "
                f"attempts={self.attempts} elapsed={self.elapsed_s:.3f}s "
                f"last_error={self.last_error!r}"
            )
        super().__init__(self.message)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


def open_connection(
    db_path: Path | str,
    *,
    timeout: float = 5.0,
    check_same_thread: bool = False,
) -> sqlite3.Connection:
    """Open a hardened SQLite connection at ``db_path``.

    Applies the §7.3 pragma list **before** returning. ``check_same_thread`` is
    disabled by default so the connection can be marshalled across
    :func:`asyncio.to_thread` calls; callers are still responsible for not
    sharing a single connection across simultaneous threads.

    Args:
        db_path: Filesystem path to the SQLite database. Special name
            ``":memory:"`` is supported for tests.
        timeout: Per-statement timeout passed to :func:`sqlite3.connect`. This
            is independent of the ``busy_timeout`` PRAGMA but should be set to
            a similar value so blocking ``execute`` calls don't outlast our
            retry deadline.
        check_same_thread: Forwarded to :func:`sqlite3.connect`.

    Returns:
        A ready-to-use :class:`sqlite3.Connection`.

    Raises:
        sqlite3.OperationalError: If the file cannot be opened.
    """
    db_path_str = str(db_path) if isinstance(db_path, Path) else db_path

    conn = sqlite3.connect(
        db_path_str,
        timeout=timeout,
        check_same_thread=check_same_thread,
        isolation_level=None,  # autocommit; we drive transactions explicitly
    )
    try:
        _apply_pragmas(conn)
    except Exception:
        conn.close()
        raise
    return conn


async def aopen_connection(
    db_path: Path | str,
    *,
    timeout: float = 5.0,
) -> sqlite3.Connection:
    """Async wrapper around :func:`open_connection`.

    Runs the (blocking) connect + pragma application on a worker thread so the
    event loop is never stalled. The returned connection is still a regular
    :class:`sqlite3.Connection`; callers that need async semantics should wrap
    individual statements in :func:`asyncio.to_thread`.
    """
    return await asyncio.to_thread(
        open_connection, db_path, timeout=timeout, check_same_thread=False
    )


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply the §7.3 pragma list. Idempotent; safe to call repeatedly."""
    cur = conn.cursor()
    try:
        for name, value in _PRAGMAS:
            # PRAGMA values cannot be parameterized; this is a fixed table of
            # spec-supplied literals so string interpolation is safe.
            cur.execute(f"PRAGMA {name} = {value}")
            # Drain the result row (some pragmas return the new value).
            cur.fetchall()
    finally:
        cur.close()


def read_pragmas(conn: sqlite3.Connection) -> dict[str, Any]:
    """Read back the pragmas listed in :data:`EXPECTED_PRAGMAS`.

    Useful for tests and boot-time assertions. Returned values are normalized
    the same way SQLite returns them (e.g. ``journal_mode`` is lowercased).
    """
    result: dict[str, Any] = {}
    cur = conn.cursor()
    try:
        for name in EXPECTED_PRAGMAS:
            row = cur.execute(f"PRAGMA {name}").fetchone()
            result[name] = row[0] if row is not None else None
    finally:
        cur.close()
    return result


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def _is_lock_error(exc: BaseException) -> bool:
    """Return True if ``exc`` is the recoverable SQLite-busy error."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    # SQLite error messages we care about: "database is locked" and the WAL
    # variant "database table is locked: <tbl>". Both contain _LOCK_MARKER as
    # a substring once we collapse "table " out.
    msg = str(exc).lower()
    if _LOCK_MARKER in msg:
        return True
    # Defensive: "database table is locked" contains "is locked" plus "database".
    return ("database" in msg) and ("locked" in msg)


def _next_backoff(attempt: int, *, rng: random.Random) -> float:
    """Compute the next backoff per the contract (50ms × 2^attempt × jitter)."""
    base = _INITIAL_BACKOFF_S * (_BACKOFF_FACTOR**attempt)
    jitter = rng.uniform(-_JITTER_FRAC, _JITTER_FRAC)
    return max(0.0, base * (1.0 + jitter))


@contextmanager
def begin_immediate_with_retry(
    conn: sqlite3.Connection,
    *,
    operation: str,
    deadline_s: float = _DEADLINE_S,
    sleep: Any = time.sleep,
    monotonic: Any = time.monotonic,
    rng: random.Random | None = None,
) -> Iterator[sqlite3.Connection]:
    """Context manager wrapping ``BEGIN IMMEDIATE`` with the spec retry contract.

    On entry: repeatedly issue ``BEGIN IMMEDIATE`` until it succeeds or the
    wall-clock deadline is reached. On lock errors we sleep with exponential
    backoff + ±25% jitter (initial 50ms, factor 2). Non-lock
    :class:`sqlite3.OperationalError` propagates immediately.

    On exit: ``COMMIT`` on success, ``ROLLBACK`` on exception. The connection
    is **not** closed — that remains the caller's responsibility.

    Args:
        conn: Hardened :class:`sqlite3.Connection`.
        operation: Caller-supplied label for diagnostics; attached to any
            :class:`StoreUnavailable` raised.
        deadline_s: Total wall-clock budget; defaults to 5.0s to match
            ``busy_timeout``.
        sleep: Injected for tests; defaults to :func:`time.sleep`.
        monotonic: Injected for tests; defaults to :func:`time.monotonic`.
        rng: Injected for tests; defaults to a fresh :class:`random.Random`.

    Yields:
        The same :class:`sqlite3.Connection`, now inside a transaction.

    Raises:
        StoreUnavailable: After the retry budget is exhausted.
        sqlite3.OperationalError: Immediately for non-lock errors.
    """
    rng = rng or random.Random()
    start = monotonic()
    attempts = 0

    while True:
        attempts += 1
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:
            if not _is_lock_error(exc):
                raise
            elapsed = monotonic() - start
            if elapsed >= deadline_s:
                raise StoreUnavailable(
                    operation=operation,
                    last_error=exc,
                    attempts=attempts,
                    elapsed_s=elapsed,
                ) from exc
            backoff = _next_backoff(attempts - 1, rng=rng)
            # Don't oversleep the deadline.
            remaining = deadline_s - elapsed
            sleep(min(backoff, remaining))

    try:
        yield conn
    except BaseException:
        # Roll back; suppress secondary errors so the original exception wins.
        with contextlib.suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Batched insert helper
# ---------------------------------------------------------------------------


class BatchedInserter:
    """Accumulates rows and flushes via :meth:`executemany` on count/interval.

    The helper is intentionally caller-driven rather than background-threaded:
    callers append rows as they arrive (e.g. subprocess stdout chunks per §5.6)
    and either explicitly :meth:`flush` or rely on :meth:`add` to flush when a
    trigger fires.

    Flush triggers (whichever fires first):
    - ``flush_count``: pending row count reaches this threshold.
    - ``flush_interval_s``: this many seconds have passed since the last flush
      (or since construction). Checked on every :meth:`add`.

    All flushes run inside :func:`begin_immediate_with_retry`, so callers
    inherit the §7.3 retry contract automatically.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        table: str,
        columns: Sequence[str],
        *,
        flush_count: int = 100,
        flush_interval_s: float | None = 1.0,
        operation: str | None = None,
        monotonic: Any = time.monotonic,
    ) -> None:
        if not columns:
            raise ValueError("columns must be non-empty")
        if flush_count <= 0:
            raise ValueError("flush_count must be positive")
        if flush_interval_s is not None and flush_interval_s <= 0:
            raise ValueError("flush_interval_s must be positive or None")

        self._conn = conn
        self._table = table
        self._columns = tuple(columns)
        self._flush_count = flush_count
        self._flush_interval_s = flush_interval_s
        self._operation = operation or f"batched_insert:{table}"
        self._monotonic = monotonic
        self._buffer: list[tuple[Any, ...]] = []
        self._last_flush = monotonic()

        placeholders = ",".join(["?"] * len(self._columns))
        col_list = ",".join(self._columns)
        # Table/column names cannot be parameterized; the values they take are
        # caller-supplied identifiers, so this is by design.
        self._insert_sql = (
            f"INSERT INTO {self._table} ({col_list}) VALUES ({placeholders})"
        )

    # -- public API ---------------------------------------------------------

    def add(self, row: Sequence[Any]) -> bool:
        """Append a row; flush if a trigger fires. Returns True if flushed."""
        if len(row) != len(self._columns):
            raise ValueError(
                f"row has {len(row)} values, expected {len(self._columns)}"
            )
        self._buffer.append(tuple(row))
        if self._should_flush():
            self.flush()
            return True
        return False

    def extend(self, rows: Iterable[Sequence[Any]]) -> int:
        """Append many rows; may flush multiple times. Returns rows added."""
        added = 0
        for r in rows:
            self.add(r)
            added += 1
        return added

    def flush(self) -> int:
        """Flush the buffer via ``executemany`` inside a retry-wrapped txn.

        Returns the number of rows written. No-op (returns 0) when empty.
        """
        if not self._buffer:
            self._last_flush = self._monotonic()
            return 0
        rows = self._buffer
        self._buffer = []
        with begin_immediate_with_retry(self._conn, operation=self._operation):
            self._conn.executemany(self._insert_sql, rows)
        self._last_flush = self._monotonic()
        return len(rows)

    def __len__(self) -> int:
        return len(self._buffer)

    @property
    def pending(self) -> int:
        return len(self._buffer)

    # -- internals ----------------------------------------------------------

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self._flush_count:
            return True
        if self._flush_interval_s is None:
            return False
        return (self._monotonic() - self._last_flush) >= self._flush_interval_s


def batched_insert(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    flush_count: int = 100,
    flush_interval_s: float | None = 1.0,
    operation: str | None = None,
) -> int:
    """One-shot batched-insert: feed all ``rows`` through a :class:`BatchedInserter`.

    For long-lived ingestion (e.g. delegation stdout streaming) callers should
    instantiate :class:`BatchedInserter` directly so they can keep the buffer
    alive across multiple ``add`` calls.

    Returns the total number of rows inserted.
    """
    inserter = BatchedInserter(
        conn,
        table,
        columns,
        flush_count=flush_count,
        flush_interval_s=flush_interval_s,
        operation=operation,
    )
    total = inserter.extend(rows)
    inserter.flush()
    return total

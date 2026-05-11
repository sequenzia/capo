"""Async per-delegation subprocess output reader.

This module implements the §5.6 "Continuous output drain" acceptance criterion
and the §6.1 "Zero loss + zero pipe blocking on >=10 MB stdout" performance
contract for Phase 2 (§9.2).

Contract summary
----------------

* A dedicated asyncio reader **per delegation** drains both
  ``process.stdout`` and ``process.stderr`` **continuously and concurrently**,
  so neither pipe can block the subprocess (macOS pipe buffer is ~64 KB; if
  Capo stops draining for any reason the child stalls on the next ``write``).
* Drained chunks are accumulated in an in-memory buffer and flushed to the
  ``delegation_output`` table either every ``flush_interval_s`` seconds
  (default ~50 ms) **or** after ``flush_count`` chunks (default 100),
  whichever fires first. Buffering is implemented by reusing the §7.3
  :class:`~capo.memory.store.BatchedInserter`, which guarantees inserts run
  inside ``BEGIN IMMEDIATE`` with the retry contract — so the reader inherits
  zero-``SQLITE_BUSY``-to-caller behaviour automatically.
* For each stdout line that parses as JSON (Claude Code's
  ``--output-format stream-json --verbose`` emits NDJSON, see spike S-3),
  the row is persisted with ``stream='event'`` and the original JSON line as
  ``chunk``. Non-JSON stdout lines (plain-text preamble, partial output,
  Codex non-JSON segments, ...) are persisted with ``stream='stdout'``.
  Stderr is always ``stream='stderr'``.
* Very long lines (>``MAX_LINE_BYTES`` = 1 MiB) are split at a sane boundary
  (``CHUNK_SPLIT_BYTES`` = 64 KiB) and persisted as multiple ``stdout`` rows,
  so the reader cannot be wedged by an adversarial child that never emits a
  newline.
* On :class:`asyncio.CancelledError` the reader drains every remaining byte
  it can read non-blockingly, flushes the buffer, and re-raises so callers
  observe the cancellation cleanly. On any **other** exception inside the
  reader, the reader logs (via logfire if available, else stdlib logging),
  attempts to terminate the subprocess, flushes whatever it has, and
  re-raises wrapped in :class:`ReaderError`.

This module does NOT own the subprocess lifecycle (spawn, returncode poll,
status row update). Those concerns belong to ``capo/tools/claude_code.py``
(#22) and the DBOS monitor workflow (#28+). The reader is a pure I/O drain.

Spec references
---------------
* §5.6 Acceptance Criteria — "Continuous output drain", "Pipe-block-free".
* §6.1 Performance — "Subprocess output ingest: Zero loss + zero pipe blocking
  on >=10 MB stdout".
* §7.3 Data Models — ``delegation_output`` schema + retry helper contract.
* §7.6 Technical Constraints — "macOS pipe buffer ~64 KB" row.
* §9.2 Phase 2 deliverable — "Async output reader".
* Spike S-3 (`internal/specs/spikes/S-3-cc-json-schema.md`) — CC event schema.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from capo.memory.store import BatchedInserter

logger = logging.getLogger("capo.tools._subprocess")

# Optional logfire instrumentation — Phase 1 already imports logfire elsewhere,
# but we keep the dependency soft so unit tests can run without configuring it.
try:  # pragma: no cover - trivial import guard
    import logfire as _logfire
except Exception:  # pragma: no cover
    _logfire = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Tunables (documented; see module docstring for invariants).
# ---------------------------------------------------------------------------

#: Flush the buffer to DB at most this often (seconds). 50 ms per §5.6.
DEFAULT_FLUSH_INTERVAL_S: float = 0.050

#: Flush the buffer after this many pending rows. 100 chunks per §5.6.
DEFAULT_FLUSH_COUNT: int = 100

#: Hard cap on a single line we are willing to buffer in memory. Anything
#: longer is split into :data:`CHUNK_SPLIT_BYTES` pieces. 1 MiB matches the
#: ``fetch_url`` body cap in :mod:`capo.tools.basic` — keep them aligned so
#: operators have one number to reason about.
MAX_LINE_BYTES: int = 1024 * 1024  # 1 MiB

#: Boundary at which we split overly long lines. 64 KiB matches the macOS
#: pipe buffer size (§7.6) — once we exceed one pipe buffer's worth of data
#: without a newline, we cut.
CHUNK_SPLIT_BYTES: int = 64 * 1024  # 64 KiB

#: How long :meth:`ReaderHandle.drain_cancelled` waits for the read pipes to
#: settle after cancellation before giving up. Bounded so cancellation can't
#: hang the caller forever on a hostile child.
CANCEL_DRAIN_TIMEOUT_S: float = 0.250


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class ReaderError(RuntimeError):
    """Raised by the reader to signal an unrecoverable I/O / DB error.

    The reader catches the original exception, logs it (with logfire when
    instrumented), tries to kill the subprocess, flushes whatever it has,
    and then re-raises wrapped as this type so callers have a single
    catch-point.
    """


@dataclass
class ReaderStats:
    """Mutable counters the reader updates as it runs.

    Exposed via :attr:`ReaderHandle.stats`. Useful for assertions in the
    integration test that >10 MB of stdout was actually captured.
    """

    stdout_bytes: int = 0
    stderr_bytes: int = 0
    event_rows: int = 0
    stdout_rows: int = 0
    stderr_rows: int = 0
    flushes: int = 0
    split_lines: int = 0
    parse_errors: int = 0
    # When a non-cancellation error tore the reader down, this carries it.
    fatal_error: BaseException | None = None
    # True once we've observed EOF on both streams and done the final flush.
    eof_reached: bool = False


@dataclass
class ReaderHandle:
    """Returned by :func:`start_reader`; wraps the underlying asyncio task.

    The handle aggregates the per-stream reader tasks into a single object
    callers can ``await`` (via :meth:`wait`) or cancel (via :meth:`cancel`).

    Session-id capture hook (Task #23 / Task #45)
    ---------------------------------------------
    The reader exposes two attributes consumed by per-agent
    ``_await_session_id`` helpers (in :mod:`capo.tools.claude_code` and
    :mod:`capo.tools.codex`):

    * :attr:`first_event_received` — an :class:`asyncio.Event` that is set the
      first time the reader observes a JSON-parseable stdout line whose
      top-level ``session_id`` field is a non-empty string (Claude Code,
      per spike S-3 §4.1) OR whose top-level ``thread_id`` field is a
      non-empty string (Codex's ``thread.started`` event, per spike
      S-1 §5).
    * :attr:`first_event` — the parsed event dict that triggered the event
      above (or ``None`` until that happens).

    The capture path is **additive**: callers that don't await the hook see
    no behavior change. Malformed JSON lines do NOT set the hook (parser
    keeps looking) — they are still recorded as ``stream='stdout'`` with
    ``stats.parse_errors`` incremented per the existing contract.
    """

    delegation_id: str
    task: asyncio.Task[None]
    stats: ReaderStats = field(default_factory=ReaderStats)
    # We keep the inserter pinned so :meth:`wait` can do a final flush even
    # when the reader was cancelled mid-buffer.
    _inserter: BatchedInserter | None = None
    # Task #23 — session-id capture hook. Lazily constructed in :func:`start_reader`
    # so the Event is bound to the running loop.
    first_event_received: asyncio.Event = field(default_factory=asyncio.Event)
    first_event: dict[str, Any] | None = None

    async def wait(self) -> ReaderStats:
        """Wait for the reader to exit; return final stats.

        Re-raises any non-cancellation exception the reader produced. On
        cancellation, returns whatever stats were accumulated before exit
        rather than re-raising :class:`asyncio.CancelledError` (the cancel
        was the caller's request).
        """
        # Caller-initiated cancel surfaces stats instead of re-raising.
        with contextlib.suppress(asyncio.CancelledError):
            await self.task
        return self.stats

    def cancel(self) -> None:
        """Request that the reader stop. Final flush still runs."""
        if not self.task.done():
            self.task.cancel()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


# Type alias for "anything that quacks like an asyncio.StreamReader".
# We accept ducks so tests can feed synthetic byte streams without spinning
# up real subprocesses.
StreamLike = Any


def start_reader(
    delegation_id: str,
    process: Any,
    *,
    store: sqlite3.Connection,
    flush_count: int = DEFAULT_FLUSH_COUNT,
    flush_interval_s: float = DEFAULT_FLUSH_INTERVAL_S,
    now: Callable[[], datetime] | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> ReaderHandle:
    """Spawn the per-delegation drain task and return a handle.

    The returned task drains ``process.stdout`` and ``process.stderr``
    concurrently. It is the caller's responsibility to call ``handle.wait()``
    or ``await handle.task`` to surface any errors.

    Args:
        delegation_id: Foreign key written into every ``delegation_output``
            row. The caller is expected to have already INSERTed the matching
            ``delegations`` row.
        process: Anything with ``stdout`` and ``stderr`` attributes that
            behave like :class:`asyncio.StreamReader` (i.e. ``readline()``
            / ``read(n)`` returning ``bytes`` and EOF==``b""``). Real
            :class:`asyncio.subprocess.Process` works; the test suite passes
            a fake.
        store: A hardened :class:`sqlite3.Connection` (see
            :func:`capo.memory.store.open_connection`). The reader runs its
            ``executemany`` calls on this connection inside
            ``BEGIN IMMEDIATE`` via :class:`BatchedInserter`. The caller
            retains ownership and must close the connection.
        flush_count: Override for :data:`DEFAULT_FLUSH_COUNT`.
        flush_interval_s: Override for :data:`DEFAULT_FLUSH_INTERVAL_S`.
        now: Injectable timestamp source (returns an aware
            :class:`datetime`). Tests stub this for deterministic
            ``ts`` columns; production passes ``None`` to use UTC now.
        loop: Optional event loop; defaults to the running loop.

    Returns:
        A :class:`ReaderHandle` whose ``.task`` drives I/O. Callers
        typically ``await handle.wait()`` after the subprocess exits.
    """
    if process.stdout is None or process.stderr is None:
        raise ValueError(
            "start_reader: process must have non-None stdout and stderr "
            "(spawn the subprocess with stdout=PIPE, stderr=PIPE)"
        )

    inserter = BatchedInserter(
        store,
        "delegation_output",
        ("delegation_id", "ts", "stream", "chunk"),
        flush_count=flush_count,
        flush_interval_s=flush_interval_s,
        operation=f"delegation_output_reader:{delegation_id}",
    )

    stats = ReaderStats()
    ts_source: Callable[[], datetime] = now or _utc_now

    # Task #23: construct the handle up-front so the reader can populate the
    # session-id capture hook (`first_event` / `first_event_received`) as it
    # observes JSON events. The asyncio.Event is created here, on the loop the
    # caller is running on, so awaiters on the same loop work cleanly.
    handle = ReaderHandle(
        delegation_id=delegation_id,
        task=None,  # type: ignore[arg-type]  -- assigned below
        stats=stats,
        _inserter=inserter,
        first_event_received=asyncio.Event(),
        first_event=None,
    )

    coro = _drive(
        delegation_id=delegation_id,
        process=process,
        inserter=inserter,
        stats=stats,
        now=ts_source,
        handle=handle,
    )
    target_loop = loop or asyncio.get_event_loop()
    task = target_loop.create_task(coro, name=f"delegation-reader-{delegation_id}")
    handle.task = task
    return handle


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(ts: datetime) -> str:
    """Format a datetime to the ISO-8601 string SQLite stores in TIMESTAMP.

    SQLite stores TIMESTAMP as TEXT; we format explicitly (rather than
    relying on adapters) so tests can assert exact wire values.
    """
    # Drop the timezone designator: SQLite's default CURRENT_TIMESTAMP also
    # emits a naive UTC string ("YYYY-MM-DD HH:MM:SS"). Mirroring that format
    # keeps mixed reader/non-reader rows comparable.
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


async def _drive(
    *,
    delegation_id: str,
    process: Any,
    inserter: BatchedInserter,
    stats: ReaderStats,
    now: Callable[[], datetime],
    handle: ReaderHandle | None = None,
) -> None:
    """Top-level coroutine: run stdout + stderr drainers and reap them."""
    stdout_task = asyncio.create_task(
        _read_stream(
            delegation_id=delegation_id,
            reader=process.stdout,
            stream_kind="stdout",
            inserter=inserter,
            stats=stats,
            now=now,
            handle=handle,
        ),
        name=f"reader-stdout-{delegation_id}",
    )
    stderr_task = asyncio.create_task(
        _read_stream(
            delegation_id=delegation_id,
            reader=process.stderr,
            stream_kind="stderr",
            inserter=inserter,
            stats=stats,
            now=now,
            handle=None,  # session-id only appears on stdout
        ),
        name=f"reader-stderr-{delegation_id}",
    )

    try:
        # gather() returns when both drainers hit EOF, or propagates the first
        # exception. We use ``return_exceptions=False`` deliberately so an
        # error tears the whole reader down.
        await asyncio.gather(stdout_task, stderr_task)
        # Both pipes closed cleanly: final flush.
        await _flush_async(inserter, stats)
        stats.eof_reached = True
    except asyncio.CancelledError:
        # Caller asked us to stop. Cascade cancellation, give the streams a
        # tiny window to drain non-blockingly, and flush whatever we have so
        # no rows are lost. Then re-raise.
        stdout_task.cancel()
        stderr_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, return_exceptions=True),
                timeout=CANCEL_DRAIN_TIMEOUT_S,
            )
        # Final flush MUST happen even on cancel — that's the §5.6 contract.
        with contextlib.suppress(Exception):
            await _flush_async(inserter, stats)
        raise
    except BaseException as exc:
        # Something unrecoverable: log, kill the subprocess, flush, wrap.
        stats.fatal_error = exc
        _log_error(delegation_id, exc)
        stdout_task.cancel()
        stderr_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, return_exceptions=True),
                timeout=CANCEL_DRAIN_TIMEOUT_S,
            )
        await _try_terminate(process)
        with contextlib.suppress(Exception):
            await _flush_async(inserter, stats)
        if isinstance(exc, Exception):
            raise ReaderError(
                f"reader failed for delegation {delegation_id}: "
                f"{exc.__class__.__name__}: {exc}"
            ) from exc
        raise


async def _read_stream(
    *,
    delegation_id: str,
    reader: StreamLike,
    stream_kind: str,
    inserter: BatchedInserter,
    stats: ReaderStats,
    now: Callable[[], datetime],
    handle: ReaderHandle | None = None,
) -> None:
    """Drain a single pipe to ``inserter``.

    Reads using ``readline()`` for normal newline-delimited streams, but
    falls back to ``read(CHUNK_SPLIT_BYTES)`` when a single "line" exceeds
    :data:`MAX_LINE_BYTES` — at which point we cut it into ``CHUNK_SPLIT_BYTES``
    pieces and emit them as separate rows. This prevents an adversarial child
    that never emits a newline from wedging the reader on an unbounded
    ``readline()``.
    """
    if reader is None:
        return

    while True:
        try:
            line = await reader.readline()
        except (asyncio.LimitOverrunError, ValueError) as exc:
            # ``LimitOverrunError`` is raised when the line exceeds the
            # StreamReader's internal buffer limit (64 KiB by default).
            # ``ValueError`` from non-asyncio fakes; treat the same way.
            # Fall back to bounded ``read(N)`` until we hit a newline or EOF.
            stats.split_lines += 1
            await _drain_long_line(
                delegation_id=delegation_id,
                reader=reader,
                stream_kind=stream_kind,
                inserter=inserter,
                stats=stats,
                now=now,
                first_exc=exc,
                handle=handle,
            )
            continue

        if not line:
            # EOF on this pipe.
            return

        # Defensive cap: if a "line" came back larger than MAX_LINE_BYTES
        # in a single shot (some StreamReader configs allow this), split it.
        if len(line) > MAX_LINE_BYTES:
            stats.split_lines += 1
            for piece in _split_bytes(line, CHUNK_SPLIT_BYTES):
                await _emit_chunk(
                    delegation_id=delegation_id,
                    raw=piece,
                    stream_kind=stream_kind,
                    inserter=inserter,
                    stats=stats,
                    now=now,
                    force_plain=True,
                    handle=handle,
                )
            continue

        await _emit_chunk(
            delegation_id=delegation_id,
            raw=line,
            stream_kind=stream_kind,
            inserter=inserter,
            stats=stats,
            now=now,
            handle=handle,
        )


async def _drain_long_line(
    *,
    delegation_id: str,
    reader: StreamLike,
    stream_kind: str,
    inserter: BatchedInserter,
    stats: ReaderStats,
    now: Callable[[], datetime],
    first_exc: BaseException,
    handle: ReaderHandle | None = None,
) -> None:
    """Drain a line that exceeded the StreamReader buffer.

    asyncio.StreamReader gives us the partial data via ``read(N)`` after a
    LimitOverrunError; we keep reading bounded chunks until we hit a newline
    or EOF, and write each chunk as a separate row.
    """
    del first_exc  # only used to differentiate the call site

    while True:
        piece = await reader.read(CHUNK_SPLIT_BYTES)
        if not piece:
            return
        # Force plain stream — we never attempt JSON parsing on a line we
        # already know is malformed-or-huge.
        await _emit_chunk(
            delegation_id=delegation_id,
            raw=piece,
            stream_kind=stream_kind,
            inserter=inserter,
            stats=stats,
            now=now,
            force_plain=True,
            handle=handle,
        )
        # If the piece ended at a newline, the next ``readline()`` will resume
        # a fresh line normally — bail back to the caller.
        if piece.endswith(b"\n"):
            return


def _split_bytes(data: bytes, size: int) -> list[bytes]:
    """Slice ``data`` into ``size``-byte pieces (last piece may be shorter)."""
    return [data[i : i + size] for i in range(0, len(data), size)]


async def _emit_chunk(
    *,
    delegation_id: str,
    raw: bytes,
    stream_kind: str,
    inserter: BatchedInserter,
    stats: ReaderStats,
    now: Callable[[], datetime],
    force_plain: bool = False,
    handle: ReaderHandle | None = None,
) -> None:
    """Persist one chunk, classifying as ``event`` when JSON-parseable.

    JSON detection rules (mirrors spike S-3 §4.3 capture algorithm):
    - Only stdout lines are candidates; stderr is always ``stream='stderr'``.
    - Must begin with ``{`` after stripping whitespace (cheap pre-check).
    - Must successfully ``json.loads``.
    - On parse failure we record ``parse_errors`` and fall back to
      ``stream='stdout'`` with the original text — tolerating CC's
      plain-text preamble per S-3 §4.2.
    """
    # Update byte counters before any classification work.
    if stream_kind == "stdout":
        stats.stdout_bytes += len(raw)
    else:
        stats.stderr_bytes += len(raw)

    # Decode permissively — child output is *usually* UTF-8 but binary noise
    # on stderr must not crash the reader.
    text = raw.decode("utf-8", errors="replace")

    chosen_stream = stream_kind
    chunk_text = text

    if stream_kind == "stdout" and not force_plain:
        stripped = text.strip()
        if stripped.startswith("{"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                stats.parse_errors += 1
            else:
                # Re-serialize to canonical form so downstream consumers
                # (get_delegation_output) get stable JSON rather than the
                # original whitespace from the child. Keep keys sorted so
                # tests are deterministic.
                chosen_stream = "event"
                chunk_text = json.dumps(parsed, sort_keys=True, ensure_ascii=False)
                # Task #23 / Task #45 — session-id capture hook.
                #
                # Per spike S-3 §4.1, the canonical Claude Code session
                # token is the top-level ``session_id`` string on every
                # CC event. Per spike S-1 §5, the canonical Codex session
                # token is the top-level ``thread_id`` UUID emitted on
                # the first ``type == "thread.started"`` event. Both are
                # consumed by the per-agent ``_await_session_id`` helper
                # (one in :mod:`capo.tools.claude_code`, one in
                # :mod:`capo.tools.codex`) which extracts the right field
                # from :attr:`ReaderHandle.first_event` and UPDATEs the
                # delegations row via the retry helper.
                #
                # The trigger is "first JSON event that carries EITHER a
                # non-empty top-level ``session_id`` OR a non-empty
                # top-level ``thread_id``". Malformed events lacking both
                # are ignored — the parser keeps looking. This stays
                # backwards-compatible with the Phase 2 CC capture path
                # (CC events always have ``session_id``; Codex events
                # never do until ``thread.started``).
                if (
                    handle is not None
                    and not handle.first_event_received.is_set()
                    and isinstance(parsed, dict)
                ):
                    sid = parsed.get("session_id")
                    tid = parsed.get("thread_id")
                    has_sid = isinstance(sid, str) and sid
                    has_tid = isinstance(tid, str) and tid
                    if has_sid or has_tid:
                        handle.first_event = parsed
                        handle.first_event_received.set()
                del parsed  # explicit, mirrors local in-band JSON consumers

    if chosen_stream == "event":
        stats.event_rows += 1
    elif chosen_stream == "stdout":
        stats.stdout_rows += 1
    else:
        stats.stderr_rows += 1

    row = (delegation_id, _iso(now()), chosen_stream, chunk_text)
    flushed = await asyncio.to_thread(inserter.add, row)
    if flushed:
        stats.flushes += 1


async def _flush_async(inserter: BatchedInserter, stats: ReaderStats) -> int:
    """Run ``inserter.flush()`` off the loop thread."""
    written = await asyncio.to_thread(inserter.flush)
    if written:
        stats.flushes += 1
    return written


async def _try_terminate(process: Any) -> None:
    """Best-effort subprocess kill after a fatal reader error.

    Per §5.6 the DBOS monitor is the canonical kill path, but on a reader
    crash we don't want a runaway child writing forever to a pipe nobody
    reads. Try terminate-then-kill with short timeouts; swallow everything.
    """
    terminate = getattr(process, "terminate", None)
    kill = getattr(process, "kill", None)
    wait = getattr(process, "wait", None)

    if callable(terminate):
        with contextlib.suppress(Exception):
            terminate()
    if callable(wait):
        with contextlib.suppress(Exception):
            await asyncio.wait_for(_maybe_await(wait()), timeout=0.5)
        if _returncode(process) is None and callable(kill):
            with contextlib.suppress(Exception):
                kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(wait()), timeout=0.5)


async def _maybe_await(value: Awaitable[Any] | Any) -> Any:
    """Await a value if it's awaitable; otherwise return it.

    ``Process.wait()`` is async; some fakes return ``None`` synchronously.
    """
    if asyncio.iscoroutine(value) or asyncio.isfuture(value):
        return await value
    return value


def _returncode(process: Any) -> int | None:
    try:
        return process.returncode
    except Exception:  # noqa: BLE001 - fakes may not implement it
        return None


def _log_error(delegation_id: str, exc: BaseException) -> None:
    """Log a fatal reader error via logfire when configured, else stdlib.

    We only forward to logfire when it has been ``configure()``-d at the
    process level — otherwise logfire emits a ``LogfireNotConfiguredWarning``
    that pollutes test output. Detection uses the public
    ``DEFAULT_LOGFIRE_INSTANCE.config.send_to_logfire`` plumbing when
    available, with a defensive fallback to stdlib on anything unexpected.
    """
    msg = (
        f"subprocess reader fatal error: delegation_id={delegation_id} "
        f"err={exc.__class__.__name__}: {exc}"
    )
    if _logfire is not None and _logfire_is_configured():
        with contextlib.suppress(Exception):  # pragma: no cover
            _logfire.error(  # type: ignore[union-attr]
                "subprocess reader fatal error",
                delegation_id=delegation_id,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
            return
    logger.exception(msg)


def _logfire_is_configured() -> bool:
    """Return True iff ``logfire.configure()`` has been called this process.

    Detection: a configured logfire has a non-None ``token`` OR an explicit
    opt-out via ``ignore_no_config``. Otherwise it will emit a
    ``LogfireNotConfiguredWarning`` from inside ``logfire.error``, which we
    don't want in test or unconfigured-dev output.
    """
    if _logfire is None:
        return False
    try:
        inst = getattr(_logfire, "DEFAULT_LOGFIRE_INSTANCE", None)
        cfg = getattr(inst, "config", None) if inst is not None else None
        if cfg is None:
            return False
        return getattr(cfg, "token", None) is not None or bool(
            getattr(cfg, "ignore_no_config", False)
        )
    except Exception:  # noqa: BLE001 - logfire internals are best-effort
        return False


__all__ = [
    "CANCEL_DRAIN_TIMEOUT_S",
    "CHUNK_SPLIT_BYTES",
    "DEFAULT_FLUSH_COUNT",
    "DEFAULT_FLUSH_INTERVAL_S",
    "MAX_LINE_BYTES",
    "ReaderError",
    "ReaderHandle",
    "ReaderStats",
    "start_reader",
]

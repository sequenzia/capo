"""Nightly ``delegation_output`` retention pruning (spec §8.1, §8.4 — Task #55).

This module owns the *delegation_output* retention contract: after a
configurable number of days, output chunks belonging to delegations in a
terminal state (``completed`` / ``failed`` / ``killed``) are deleted from
``state.db``. Rows for **active** delegations (``running`` /
``pending_approval``) are **never** pruned — even if older than the
threshold — because the live monitor / agent loop may still be reading
them.

The cutoff is computed in **UTC** to be clock-skew-tolerant: the
``delegation_output.ts`` column is written by
:func:`capo.tools._subprocess._iso` as a naive UTC string
(``YYYY-MM-DD HH:MM:SS.%f``), so the prune predicate
``WHERE ts < ?`` works directly when the threshold is formatted the same
way.

The public surface is two functions:

* :func:`prune_delegation_output` — synchronous, side-effecting:
  takes a ``db_path``, ``now``, and ``max_age_days``, returns the
  number of rows deleted. Idempotent: re-running with the same
  arguments after a successful prune returns 0 (rows are gone).
* :func:`start_retention_scheduler` — returns an
  :class:`asyncio.Task` that wakes at ``settings.retention.run_hour_local``
  local-time each day, calls :func:`prune_delegation_output`, then
  sleeps until the next wake. A 23h skip-recent-run guard prevents
  double-runs on clock adjustments or rapid restarts.

The scheduler logs three structured events:

* ``capo.maintenance.retention.started`` — wake fires, before prune.
* ``capo.maintenance.retention.completed`` — prune returned cleanly,
  carries ``pruned_count`` + ``duration_ms``.
* ``capo.maintenance.retention.failed`` — prune raised; carries
  ``error_type`` + ``error_message``. Scheduler loop continues
  (single-failure-tolerant).

Spec references:
* §8.1 — Litestream replicated state; retention pruning keeps the
  WAL bounded so replication latency stays low.
* §8.4 — ``delegation_output`` retention default 14 days.
* §7.3 — ``delegations.status`` CHECK
  ``('running','completed','failed','killed','pending_approval')``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from capo.memory.store import begin_immediate_with_retry, open_connection

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings

logger = logging.getLogger("capo.maintenance.retention")


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Default age threshold in days (spec §8.4). Used when a caller can't
#: reach a fully-validated ``Settings`` instance (tests, scripts).
DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS: int = 14

#: Default wake hour (24h, local time). Spec §8.4 / Task #55.
DEFAULT_RETENTION_RUN_HOUR_LOCAL: int = 3

#: Terminal status values from the §7.3 ``delegations.status`` CHECK
#: constraint that gate pruning. Rows for delegations in any OTHER
#: status (``running``, ``pending_approval``) are preserved.
TERMINAL_STATUSES: tuple[str, ...] = ("completed", "failed", "killed")

#: Structured event names emitted by the scheduler.
EVENT_RETENTION_STARTED: str = "capo.maintenance.retention.started"
EVENT_RETENTION_COMPLETED: str = "capo.maintenance.retention.completed"
EVENT_RETENTION_FAILED: str = "capo.maintenance.retention.failed"

#: Minimum gap (seconds) between two consecutive scheduler runs. Spec
#: §8.4: "skip if the previous run is fresher than 23h". This is a
#: belt-and-braces guard against accidental rapid re-fires (clock jumps,
#: rapid restart loops, manual triggers from tests calling tick).
_SKIP_RECENT_GAP_S: float = 23.0 * 3600.0

#: ts format used by :func:`capo.tools._subprocess._iso` for writes;
#: SQLite compares TIMESTAMP TEXT lexically when the format is fixed
#: width, which this one is.
_TS_FMT: str = "%Y-%m-%d %H:%M:%S.%f"


# ---------------------------------------------------------------------------
# Prune implementation
# ---------------------------------------------------------------------------


def _format_cutoff(cutoff: datetime) -> str:
    """Format a UTC datetime to the ``delegation_output.ts`` text shape."""
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=UTC)
    return cutoff.astimezone(UTC).strftime(_TS_FMT)


def prune_delegation_output(
    db_path: Path | str,
    now: datetime,
    max_age_days: int,
) -> int:
    """Delete eligible ``delegation_output`` rows; return the row count pruned.

    A row is eligible iff **both** hold:

    * ``delegation_output.ts < (now_utc - max_age_days)``
    * The parent ``delegations.status`` is one of
      :data:`TERMINAL_STATUSES` (``completed`` / ``failed`` / ``killed``).

    Rows for delegations still ``running`` (or ``pending_approval``) are
    preserved regardless of ``ts``. Rows whose ``delegation_id`` no
    longer references any ``delegations`` row are **also** preserved —
    they're orphaned audit data and not in scope for §8.4 retention.

    Parameters
    ----------
    db_path:
        Filesystem path to ``state.db``. A fresh hardened connection is
        opened, used, and closed.
    now:
        Reference timestamp. ``now - max_age_days`` is the cutoff. UTC
        is used internally regardless of ``now``'s tzinfo — naive
        datetimes are assumed UTC (spec §8.4 "use UTC for the cutoff").
    max_age_days:
        Non-negative integer. ``0`` means "prune everything for
        terminal delegations" (allowed; lets operators force a sweep).

    Returns
    -------
    int
        Number of rows deleted. ``0`` when nothing is eligible.

    Raises
    ------
    ValueError
        ``max_age_days`` is negative.
    sqlite3.DatabaseError
        Propagated from the SQLite layer if the DB is unreadable. The
        scheduler catches and logs at the call site (see
        :func:`start_retention_scheduler`).
    """
    if max_age_days < 0:
        raise ValueError(
            f"max_age_days must be >= 0; got {max_age_days!r}"
        )

    cutoff_dt = now - timedelta(days=max_age_days)
    cutoff_str = _format_cutoff(cutoff_dt)

    # Inline placeholders for the terminal-status IN-clause. Using a
    # static tuple keeps the SQL planner happy and avoids a Python f-
    # string parameter-count hack at the call site.
    placeholders = ",".join("?" * len(TERMINAL_STATUSES))
    sql = (
        "DELETE FROM delegation_output "
        "WHERE ts < ? "
        "  AND delegation_id IN ("
        "    SELECT id FROM delegations "
        f"    WHERE status IN ({placeholders})"
        "  )"
    )
    params: tuple[str, ...] = (cutoff_str, *TERMINAL_STATUSES)

    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation="prune_delegation_output"
        ):
            cursor = conn.execute(sql, params)
            pruned = cursor.rowcount
        return int(pruned) if pruned is not None and pruned >= 0 else 0
    finally:
        conn.close()


def vacuum_db(db_path: Path | str) -> None:
    """Run a blocking ``VACUUM`` on ``db_path``.

    Called only when ``settings.retention.vacuum_after_prune`` is True.
    VACUUM requires an exclusive lock, so this is best run during the
    same nightly window as the prune itself. Open + close a fresh
    connection; do not run inside the prune txn (VACUUM cannot execute
    inside a transaction).

    Any error is logged + suppressed at the scheduler boundary;
    failures here should not abort the loop.
    """
    conn = open_connection(db_path)
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


def _seconds_until_next_run(
    now_local: datetime, run_hour_local: int
) -> float:
    """Return seconds from ``now_local`` until the next ``run_hour_local``.

    If ``now_local`` is at-or-past today's run hour, return seconds
    until tomorrow's. Caller passes the local-time clock value so the
    helper itself stays tz-free and easy to unit test.
    """
    candidate = now_local.replace(
        hour=run_hour_local, minute=0, second=0, microsecond=0
    )
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return max(0.0, (candidate - now_local).total_seconds())


async def _run_one_prune(
    settings: Settings,
    *,
    now_factory: Callable[[], datetime],
    prune: Callable[[Path | str, datetime, int], Awaitable[int] | int]
    | None = None,
) -> int:
    """Execute a single prune (and optional vacuum). Return rows pruned.

    Split out from the scheduler loop so tests can call it directly
    without driving the timer. ``prune`` is injectable so tests can
    stub the SQL layer.
    """
    db_path: Path = settings.paths.db_path
    max_age_days: int = settings.retention.delegation_output_days
    vacuum: bool = settings.retention.vacuum_after_prune

    started = time.monotonic()
    logger.info(
        "retention prune starting (max_age_days=%d, db_path=%s, vacuum=%s)",
        max_age_days,
        db_path,
        vacuum,
        extra={
            "event": EVENT_RETENTION_STARTED,
            "max_age_days": max_age_days,
            "db_path": str(db_path),
            "vacuum_after_prune": vacuum,
        },
    )

    now_utc = now_factory()
    try:
        if prune is None:
            pruned = await asyncio.to_thread(
                prune_delegation_output, db_path, now_utc, max_age_days
            )
        else:
            result = prune(db_path, now_utc, max_age_days)
            if asyncio.iscoroutine(result):
                pruned = await result
            else:
                pruned = int(result)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001 - scheduler must not die
        duration_ms = (time.monotonic() - started) * 1000.0
        logger.exception(
            "retention prune failed after %.1fms: %r",
            duration_ms,
            exc,
            extra={
                "event": EVENT_RETENTION_FAILED,
                "duration_ms": duration_ms,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            },
        )
        return 0

    if vacuum:
        try:
            await asyncio.to_thread(vacuum_db, db_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "retention VACUUM failed (ignored): %r",
                exc,
                extra={
                    "event": EVENT_RETENTION_FAILED,
                    "phase": "vacuum",
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
            )

    duration_ms = (time.monotonic() - started) * 1000.0
    logger.info(
        "retention prune completed: pruned=%d duration_ms=%.1f",
        pruned,
        duration_ms,
        extra={
            "event": EVENT_RETENTION_COMPLETED,
            "pruned_count": int(pruned),
            "duration_ms": duration_ms,
            "max_age_days": max_age_days,
            "vacuum_after_prune": vacuum,
        },
    )
    return int(pruned)


async def _scheduler_loop(
    settings: Settings,
    *,
    now_local_factory: Callable[[], datetime],
    now_utc_factory: Callable[[], datetime],
    sleep: Callable[[float], Awaitable[None]],
    prune: Callable[[Path | str, datetime, int], Awaitable[int] | int]
    | None,
    initial_delay_s: float | None,
) -> None:
    """Inner loop: sleep → check skip-recent → prune → repeat.

    All time sources are injected so unit tests can drive the loop with
    a fast fake clock + a controllable sleep stub. Production wiring
    uses ``datetime.now`` (local + UTC) and :func:`asyncio.sleep`.
    """
    last_run_monotonic: float | None = None

    while True:
        run_hour = settings.retention.run_hour_local
        if initial_delay_s is not None:
            delay = initial_delay_s
            initial_delay_s = None  # only honor on first iteration
        else:
            delay = _seconds_until_next_run(now_local_factory(), run_hour)
        try:
            await sleep(delay)
        except asyncio.CancelledError:
            logger.info(
                "retention scheduler cancelled during sleep",
                extra={"event": "capo.maintenance.retention.cancelled"},
            )
            raise

        # Skip-recent-run guard (spec §8.4): if a prior tick fired
        # within 23h (e.g. clock jump-back or rapid restart), skip
        # this tick and wait for the next scheduled wake.
        if last_run_monotonic is not None:
            gap = time.monotonic() - last_run_monotonic
            if gap < _SKIP_RECENT_GAP_S:
                logger.info(
                    "retention prune skipped (last run %.0fs ago, "
                    "minimum gap %.0fs)",
                    gap,
                    _SKIP_RECENT_GAP_S,
                    extra={
                        "event": "capo.maintenance.retention.skipped",
                        "gap_s": gap,
                        "min_gap_s": _SKIP_RECENT_GAP_S,
                    },
                )
                continue

        try:
            await _run_one_prune(
                settings,
                now_factory=now_utc_factory,
                prune=prune,
            )
        except asyncio.CancelledError:
            logger.info(
                "retention scheduler cancelled during prune",
                extra={"event": "capo.maintenance.retention.cancelled"},
            )
            raise
        # _run_one_prune already swallows non-Cancelled exceptions,
        # but defensive belt + braces in case future edits add new
        # raise sites.
        last_run_monotonic = time.monotonic()


def start_retention_scheduler(
    settings: Settings,
    *,
    name: str = "capo-retention",
    now_local_factory: Callable[[], datetime] | None = None,
    now_utc_factory: Callable[[], datetime] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    prune: Callable[[Path | str, datetime, int], Awaitable[int] | int]
    | None = None,
    initial_delay_s: float | None = None,
) -> asyncio.Task[None]:
    """Spawn the nightly retention scheduler as an asyncio task.

    Returns the :class:`asyncio.Task` so the caller (``capo.main.amain``)
    can cancel it during shutdown. Cancellation is graceful: the loop
    catches :exc:`asyncio.CancelledError` at both sleep and prune
    boundaries and exits without re-raising via ``_run_one_prune``'s
    own catch-all.

    Parameters
    ----------
    settings:
        Validated :class:`capo.config.Settings`. Read at scheduler
        construction AND on every tick — operators can update
        ``[retention]`` config and reload by restarting capo.
    name:
        Task name (visible in ``asyncio.all_tasks()`` and tracebacks).
    now_local_factory:
        Test seam: produces the "current local time" used to compute
        sleep duration. Defaults to ``lambda: datetime.now()``.
    now_utc_factory:
        Test seam: produces the cutoff anchor passed to
        :func:`prune_delegation_output`. Defaults to
        ``lambda: datetime.now(UTC)``.
    sleep:
        Test seam: defaults to :func:`asyncio.sleep`.
    prune:
        Test seam: replaces :func:`prune_delegation_output`. Tests use
        this to assert call args without writing to a real DB.
    initial_delay_s:
        Test seam: if non-None, the FIRST iteration sleeps this many
        seconds instead of computing from the local clock. Lets tests
        fire a tick immediately without waiting until 03:00.
    """
    loop = asyncio.get_event_loop()
    now_local = now_local_factory or datetime.now
    now_utc = now_utc_factory or (lambda: datetime.now(UTC))
    sleeper = sleep or asyncio.sleep

    coro = _scheduler_loop(
        settings,
        now_local_factory=now_local,
        now_utc_factory=now_utc,
        sleep=sleeper,
        prune=prune,
        initial_delay_s=initial_delay_s,
    )
    task = loop.create_task(coro, name=name)
    logger.info(
        "retention scheduler started (run_hour_local=%d, "
        "delegation_output_days=%d)",
        settings.retention.run_hour_local,
        settings.retention.delegation_output_days,
        extra={
            "event": "capo.maintenance.retention.scheduler_started",
            "run_hour_local": settings.retention.run_hour_local,
            "delegation_output_days": settings.retention.delegation_output_days,
            "vacuum_after_prune": settings.retention.vacuum_after_prune,
        },
    )
    return task


async def stop_retention_scheduler(task: asyncio.Task[None]) -> None:
    """Cancel ``task`` and await its graceful exit.

    Safe to call from a shutdown ``finally`` block: it suppresses any
    secondary exception (other than the cancellation itself) so the
    surrounding teardown can continue. Idempotent — calling on an
    already-finished task is a no-op.
    """
    if task.done():
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


__all__ = [
    "DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS",
    "DEFAULT_RETENTION_RUN_HOUR_LOCAL",
    "EVENT_RETENTION_COMPLETED",
    "EVENT_RETENTION_FAILED",
    "EVENT_RETENTION_STARTED",
    "TERMINAL_STATUSES",
    "prune_delegation_output",
    "start_retention_scheduler",
    "stop_retention_scheduler",
    "vacuum_db",
]

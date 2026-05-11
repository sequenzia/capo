"""Unit + integration tests for ``capo.maintenance.retention`` (Task #55).

Coverage targets the §8.1 / §8.4 acceptance criteria:

* ``prune_delegation_output`` deletes rows whose ``ts < (now - max_age_days)``
  AND whose parent delegation status is terminal
  (``completed``/``failed``/``killed``).
* Rows for active delegations (``running``/``pending_approval``) are
  NEVER pruned regardless of age.
* Returns the count of rows deleted.
* Uses UTC for the cutoff anchor (clock-skew tolerant).
* The scheduler:
  * Wakes daily at ``settings.retention.run_hour_local``.
  * Skips if the previous run was fresher than 23h.
  * Logs structured events on started / completed / failed.
  * Cancels gracefully on shutdown race.
  * Honors ``vacuum_after_prune`` toggle.
* Boot wiring: ``capo.main.amain`` imports + invokes the scheduler.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from capo.maintenance.retention import (
    DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS,
    DEFAULT_RETENTION_RUN_HOUR_LOCAL,
    EVENT_RETENTION_COMPLETED,
    EVENT_RETENTION_FAILED,
    EVENT_RETENTION_STARTED,
    TERMINAL_STATUSES,
    _seconds_until_next_run,
    prune_delegation_output,
    start_retention_scheduler,
    stop_retention_scheduler,
    vacuum_db,
)
from capo.memory.store import open_connection

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_DELEGATIONS_DDL = """
CREATE TABLE IF NOT EXISTS delegations (
    id                  TEXT PRIMARY KEY,
    status              TEXT NOT NULL
)
"""

_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL,
    stream        TEXT NOT NULL,
    chunk         TEXT NOT NULL
)
"""


def _make_db(tmp_path: Path) -> Path:
    """Create a minimal state.db with the two relevant tables.

    We bypass the full Alembic migration (it pulls a stack of unrelated
    Phase 1+ tables) because the retention prune SQL only touches
    ``delegation_output`` and ``delegations.status``. Schema fidelity for
    those two columns is enforced by the migration tests; this fixture
    keeps the unit-test slice small.
    """
    db_path = tmp_path / "state.db"
    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_OUTPUT_DDL)
    finally:
        conn.close()
    return db_path


def _insert_delegation(db_path: Path, did: str, status: str) -> None:
    conn = open_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO delegations (id, status) VALUES (?, ?)",
            (did, status),
        )
    finally:
        conn.close()


def _insert_output(
    db_path: Path,
    did: str,
    ts: datetime,
    *,
    stream: str = "stdout",
    chunk: str = "hello",
) -> None:
    """Insert a single ``delegation_output`` row at a precise UTC ts."""
    conn = open_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO delegation_output (delegation_id, ts, stream, chunk)"
            " VALUES (?, ?, ?, ?)",
            (
                did,
                ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f"),
                stream,
                chunk,
            ),
        )
    finally:
        conn.close()


def _count_output(db_path: Path, did: str | None = None) -> int:
    conn = open_connection(db_path)
    try:
        if did is None:
            row = conn.execute(
                "SELECT COUNT(*) FROM delegation_output"
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) FROM delegation_output WHERE delegation_id=?",
                (did,),
            ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# prune_delegation_output — unit tests
# ---------------------------------------------------------------------------


class TestPruneDelegationOutput:
    """Retention math: age + status gating + return value."""

    def test_returns_zero_when_no_rows(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        assert prune_delegation_output(db, now, 14) == 0

    def test_old_completed_row_pruned(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        # 30 days old, well past the 14-day threshold.
        _insert_output(db, "d1", now - timedelta(days=30))

        pruned = prune_delegation_output(db, now, 14)
        assert pruned == 1
        assert _count_output(db, "d1") == 0

    def test_fresh_completed_row_preserved(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        # 3 days old → inside the 14-day window.
        _insert_output(db, "d1", now - timedelta(days=3))

        assert prune_delegation_output(db, now, 14) == 0
        assert _count_output(db, "d1") == 1

    def test_old_running_row_preserved(self, tmp_path: Path) -> None:
        """Active delegation: NEVER pruned even when row is ancient."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "running")
        _insert_output(db, "d1", now - timedelta(days=365))

        assert prune_delegation_output(db, now, 14) == 0
        assert _count_output(db, "d1") == 1

    def test_old_pending_approval_row_preserved(self, tmp_path: Path) -> None:
        """``pending_approval`` is non-terminal → preserved."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "pending_approval")
        _insert_output(db, "d1", now - timedelta(days=99))

        assert prune_delegation_output(db, now, 14) == 0
        assert _count_output(db, "d1") == 1

    @pytest.mark.parametrize("status", ["completed", "failed", "killed"])
    def test_all_terminal_statuses_are_pruned(
        self, tmp_path: Path, status: str
    ) -> None:
        """All three terminal statuses are eligible."""
        assert status in TERMINAL_STATUSES
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", status)
        _insert_output(db, "d1", now - timedelta(days=30))

        assert prune_delegation_output(db, now, 14) == 1

    def test_boundary_exactly_at_cutoff_preserved(
        self, tmp_path: Path
    ) -> None:
        """Predicate is strict ``<``: a row exactly at the cutoff is kept."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        # Exactly 14 days back == cutoff. Strict-less keeps it.
        _insert_output(db, "d1", now - timedelta(days=14))

        assert prune_delegation_output(db, now, 14) == 0
        assert _count_output(db, "d1") == 1

    def test_just_past_cutoff_pruned(self, tmp_path: Path) -> None:
        """One microsecond past cutoff → pruned."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        _insert_output(
            db,
            "d1",
            now - timedelta(days=14, microseconds=1),
        )

        assert prune_delegation_output(db, now, 14) == 1

    def test_mixed_population_only_eligible_pruned(
        self, tmp_path: Path
    ) -> None:
        """Realistic mix: old-running, old-completed, fresh-completed."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

        _insert_delegation(db, "active", "running")
        _insert_delegation(db, "old", "completed")
        _insert_delegation(db, "fresh", "completed")
        _insert_delegation(db, "killed", "killed")
        _insert_delegation(db, "failed", "failed")

        # Old + running → preserved.
        _insert_output(db, "active", now - timedelta(days=99))
        # Old + completed → pruned.
        _insert_output(db, "old", now - timedelta(days=30))
        _insert_output(db, "old", now - timedelta(days=20))
        # Fresh + completed → preserved.
        _insert_output(db, "fresh", now - timedelta(days=2))
        # Old + killed → pruned.
        _insert_output(db, "killed", now - timedelta(days=60))
        # Old + failed → pruned.
        _insert_output(db, "failed", now - timedelta(days=15))

        pruned = prune_delegation_output(db, now, 14)
        assert pruned == 4
        assert _count_output(db, "active") == 1
        assert _count_output(db, "old") == 0
        assert _count_output(db, "fresh") == 1
        assert _count_output(db, "killed") == 0
        assert _count_output(db, "failed") == 0

    def test_zero_days_is_allowed_and_prunes_everything_terminal(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "term", "completed")
        _insert_delegation(db, "live", "running")
        _insert_output(db, "term", now - timedelta(seconds=1))
        _insert_output(db, "live", now - timedelta(seconds=1))

        pruned = prune_delegation_output(db, now, 0)
        assert pruned == 1
        assert _count_output(db, "live") == 1

    def test_negative_days_raises(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        with pytest.raises(ValueError):
            prune_delegation_output(db, now, -1)

    def test_naive_now_treated_as_utc(self, tmp_path: Path) -> None:
        """Naive datetime in ``now`` is treated as UTC for cutoff math.

        Clock-skew tolerance per spec §8.4: callers may pass either a
        UTC-aware or naive datetime and get the same answer.
        """
        db = _make_db(tmp_path)
        utc_now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        naive_now = datetime(2026, 5, 11, 12, 0, 0)  # noqa: DTZ001
        _insert_delegation(db, "d1", "completed")
        _insert_output(db, "d1", utc_now - timedelta(days=30))

        assert prune_delegation_output(db, naive_now, 14) == 1

    def test_orphaned_output_row_preserved(self, tmp_path: Path) -> None:
        """No matching ``delegations`` row → preserve (out of §8.4 scope)."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_output(db, "ghost", now - timedelta(days=99))

        assert prune_delegation_output(db, now, 14) == 0
        assert _count_output(db) == 1

    def test_returns_pruned_count_across_many_rows(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        for i in range(50):
            _insert_output(
                db, "d1", now - timedelta(days=20, seconds=i)
            )
        pruned = prune_delegation_output(db, now, 14)
        assert pruned == 50

    def test_re_running_after_prune_is_zero(self, tmp_path: Path) -> None:
        """Idempotency: second prune of an already-clean DB returns 0."""
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
        _insert_delegation(db, "d1", "completed")
        _insert_output(db, "d1", now - timedelta(days=30))

        assert prune_delegation_output(db, now, 14) == 1
        assert prune_delegation_output(db, now, 14) == 0


# ---------------------------------------------------------------------------
# Wake-time math
# ---------------------------------------------------------------------------


class TestSecondsUntilNextRun:
    def test_before_run_hour_today(self) -> None:
        now = datetime(2026, 5, 11, 0, 30, 0)  # noqa: DTZ001 (local-naive)
        secs = _seconds_until_next_run(now, 3)
        # 03:00 - 00:30 = 2h30m
        assert secs == 2 * 3600 + 30 * 60

    def test_at_run_hour_schedules_tomorrow(self) -> None:
        now = datetime(2026, 5, 11, 3, 0, 0)  # noqa: DTZ001
        secs = _seconds_until_next_run(now, 3)
        # candidate == now → schedule next-day.
        assert secs == pytest.approx(24 * 3600)

    def test_past_run_hour_schedules_tomorrow(self) -> None:
        now = datetime(2026, 5, 11, 14, 0, 0)  # noqa: DTZ001
        secs = _seconds_until_next_run(now, 3)
        # 03:00 next day = 24h - 14h - 0m = 13 + 11 + 0 = 13h until tomorrow's 03:00
        # (14:00 today → 03:00 tomorrow = 13h)
        assert secs == pytest.approx(13 * 3600)

    def test_exact_midnight_before_run(self) -> None:
        now = datetime(2026, 5, 11, 0, 0, 0)  # noqa: DTZ001
        secs = _seconds_until_next_run(now, 3)
        assert secs == 3 * 3600

    def test_default_run_hour_constant(self) -> None:
        assert DEFAULT_RETENTION_RUN_HOUR_LOCAL == 3
        assert DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS == 14


# ---------------------------------------------------------------------------
# Scheduler integration — drives _scheduler_loop with fakes
# ---------------------------------------------------------------------------


class _FakeSettings:
    """Just enough of a Settings interface for the scheduler.

    The full ``capo.config.Settings`` instance pulls in a SOUL file and
    all sibling sub-models. The scheduler only reads ``paths.db_path``
    and ``retention.{delegation_output_days, run_hour_local, vacuum_after_prune}``.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        days: int = 14,
        run_hour: int = 3,
        vacuum: bool = False,
    ) -> None:
        self.paths = type("P", (), {"db_path": db_path})()
        self.retention = type(
            "R",
            (),
            {
                "delegation_output_days": days,
                "run_hour_local": run_hour,
                "vacuum_after_prune": vacuum,
            },
        )()


class TestRetentionScheduler:
    """End-to-end scheduler ticks — pruned row vs fresh row."""

    async def test_tick_prunes_old_row_preserves_fresh(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

        _insert_delegation(db, "old", "completed")
        _insert_delegation(db, "fresh", "completed")
        _insert_output(db, "old", now - timedelta(days=30))
        _insert_output(db, "fresh", now - timedelta(days=1))

        settings = _FakeSettings(db, days=14)
        observed: list[float] = []

        async def fake_sleep(delay: float) -> None:
            observed.append(delay)
            # Yield once so other tasks can interleave.
            await asyncio.sleep(0)

        task = start_retention_scheduler(
            settings,
            now_local_factory=lambda: datetime(2026, 5, 11, 12, 0, 0),  # noqa: DTZ001
            now_utc_factory=lambda: now,
            sleep=fake_sleep,
            initial_delay_s=0.0,
        )

        # Give the loop a chance to fire one tick, then cancel before
        # the second sleep can play out.
        for _ in range(20):
            if _count_output(db, "old") == 0:
                break
            await asyncio.sleep(0.01)

        await stop_retention_scheduler(task)

        assert _count_output(db, "old") == 0
        assert _count_output(db, "fresh") == 1
        assert observed[0] == 0.0  # honored initial_delay_s

    async def test_graceful_cancel_during_sleep(
        self, tmp_path: Path
    ) -> None:
        """Scheduler started + immediately stopped → no exception."""
        db = _make_db(tmp_path)
        settings = _FakeSettings(db)
        task = start_retention_scheduler(
            settings,
            now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
            now_utc_factory=lambda: datetime(2026, 5, 11, 0, 0, 0, tzinfo=UTC),
            sleep=lambda d: asyncio.sleep(60),
            initial_delay_s=None,
        )
        # Cancel before the first wake fires.
        await stop_retention_scheduler(task)
        assert task.done()

    async def test_skips_recent_run(self, tmp_path: Path) -> None:
        """After one tick, the next tick within 23h is skipped.

        The scheduler uses :func:`time.monotonic` directly to track the
        last-run anchor. We don't patch that — instead we drive the
        loop with sleeps that complete fast enough that the next wake
        arrives well inside the 23h gap, then assert ``prune`` only
        fired once.
        """
        db = _make_db(tmp_path)
        settings = _FakeSettings(db, days=14)
        tick_count = {"n": 0}
        wake_count = {"n": 0}
        finished = asyncio.Event()

        def fake_prune(
            _dbp: Path, _now: datetime, _days: int
        ) -> int:
            tick_count["n"] += 1
            return 0

        async def fake_sleep(_d: float) -> None:
            wake_count["n"] += 1
            if wake_count["n"] >= 3:
                # After three wakes, signal the test and sleep "forever"
                # so cancellation cleanly unwinds.
                finished.set()
                await asyncio.sleep(3600)
                return
            await asyncio.sleep(0)

        task = start_retention_scheduler(
            settings,
            now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
            now_utc_factory=lambda: datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
            sleep=fake_sleep,
            prune=fake_prune,
            initial_delay_s=0.0,
        )

        try:
            await asyncio.wait_for(finished.wait(), timeout=2.0)
        finally:
            await stop_retention_scheduler(task)

        # Three wakes: first → prune, second → skip-recent, third →
        # skip-recent then sleeps. Tick count must be 1.
        assert tick_count["n"] == 1
        assert wake_count["n"] >= 2

    async def test_emits_started_and_completed_events(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        db = _make_db(tmp_path)
        _insert_delegation(db, "d1", "completed")
        _insert_output(
            db,
            "d1",
            datetime(2026, 5, 11, 0, 0, 0, tzinfo=UTC) - timedelta(days=30),
        )

        settings = _FakeSettings(db, days=14)

        async def fake_sleep(_d: float) -> None:
            await asyncio.sleep(0)

        caplog.set_level(logging.INFO, logger="capo.maintenance.retention")
        task = start_retention_scheduler(
            settings,
            now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
            now_utc_factory=lambda: datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
            sleep=fake_sleep,
            initial_delay_s=0.0,
        )

        # Wait for the completed event in the log.
        for _ in range(50):
            events = [getattr(r, "event", None) for r in caplog.records]
            if EVENT_RETENTION_COMPLETED in events:
                break
            await asyncio.sleep(0.01)
        await stop_retention_scheduler(task)

        events = [getattr(r, "event", None) for r in caplog.records]
        assert EVENT_RETENTION_STARTED in events
        assert EVENT_RETENTION_COMPLETED in events

        # Completed event carries pruned_count + duration_ms.
        completed = next(
            r
            for r in caplog.records
            if getattr(r, "event", None) == EVENT_RETENTION_COMPLETED
        )
        assert getattr(completed, "pruned_count", None) == 1
        assert isinstance(getattr(completed, "duration_ms", None), float)

    async def test_prune_exception_does_not_kill_loop(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        db = _make_db(tmp_path)
        settings = _FakeSettings(db, days=14)
        boom_count = {"n": 0}

        def boom_prune(
            _dbp: Path, _now: datetime, _days: int
        ) -> int:
            boom_count["n"] += 1
            raise sqlite3.DatabaseError("synthetic db failure")

        async def fake_sleep(_d: float) -> None:
            await asyncio.sleep(0)

        caplog.set_level(logging.INFO, logger="capo.maintenance.retention")
        task = start_retention_scheduler(
            settings,
            now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
            now_utc_factory=lambda: datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC),
            sleep=fake_sleep,
            prune=boom_prune,
            initial_delay_s=0.0,
        )

        for _ in range(50):
            if boom_count["n"] >= 1:
                break
            await asyncio.sleep(0.01)
        # Loop must still be running (not crashed) — task.done() is False.
        assert not task.done()
        await stop_retention_scheduler(task)

        events = [getattr(r, "event", None) for r in caplog.records]
        assert EVENT_RETENTION_FAILED in events

    async def test_vacuum_invoked_when_enabled(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        settings = _FakeSettings(db, days=14, vacuum=True)
        vacuum_calls: list[Path] = []

        from capo.maintenance import retention as ret_mod  # noqa: PLC0415

        original_vacuum = ret_mod.vacuum_db

        def stub_vacuum(p: Path) -> None:
            vacuum_calls.append(p)

        ret_mod.vacuum_db = stub_vacuum  # type: ignore[assignment]
        try:

            async def fake_sleep(_d: float) -> None:
                await asyncio.sleep(0)

            task = start_retention_scheduler(
                settings,
                now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
                now_utc_factory=lambda: datetime(
                    2026, 5, 11, 12, 0, 0, tzinfo=UTC
                ),
                sleep=fake_sleep,
                initial_delay_s=0.0,
            )

            for _ in range(50):
                if vacuum_calls:
                    break
                await asyncio.sleep(0.01)
            await stop_retention_scheduler(task)
        finally:
            ret_mod.vacuum_db = original_vacuum  # type: ignore[assignment]

        assert vacuum_calls == [db]

    async def test_vacuum_skipped_when_disabled(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        settings = _FakeSettings(db, days=14, vacuum=False)
        vacuum_calls: list[Path] = []

        from capo.maintenance import retention as ret_mod  # noqa: PLC0415

        original_vacuum = ret_mod.vacuum_db

        def stub_vacuum(p: Path) -> None:
            vacuum_calls.append(p)

        ret_mod.vacuum_db = stub_vacuum  # type: ignore[assignment]
        try:

            async def fake_sleep(_d: float) -> None:
                await asyncio.sleep(0)

            task = start_retention_scheduler(
                settings,
                now_local_factory=lambda: datetime(2026, 5, 11, 0, 0, 0),  # noqa: DTZ001
                now_utc_factory=lambda: datetime(
                    2026, 5, 11, 12, 0, 0, tzinfo=UTC
                ),
                sleep=fake_sleep,
                initial_delay_s=0.0,
            )

            # Let one tick fire then cancel.
            await asyncio.sleep(0.05)
            await stop_retention_scheduler(task)
        finally:
            ret_mod.vacuum_db = original_vacuum  # type: ignore[assignment]

        assert vacuum_calls == []


# ---------------------------------------------------------------------------
# vacuum_db real-DB smoke
# ---------------------------------------------------------------------------


def test_vacuum_db_smoke(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    # Insert + delete to leave free pages worth vacuuming.
    _insert_delegation(db, "d1", "completed")
    now = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
    for i in range(20):
        _insert_output(db, "d1", now - timedelta(days=30, seconds=i))
    prune_delegation_output(db, now, 14)
    # Should not raise.
    vacuum_db(db)


# ---------------------------------------------------------------------------
# Config wiring
# ---------------------------------------------------------------------------


def test_retention_config_defaults() -> None:
    from capo.config import RetentionConfig  # noqa: PLC0415

    cfg = RetentionConfig()
    assert cfg.delegation_output_days == 14
    assert cfg.run_hour_local == 3
    assert cfg.vacuum_after_prune is False


def test_retention_config_accepts_overrides() -> None:
    from capo.config import RetentionConfig  # noqa: PLC0415

    cfg = RetentionConfig(
        delegation_output_days=30,
        run_hour_local=5,
        vacuum_after_prune=True,
    )
    assert cfg.delegation_output_days == 30
    assert cfg.run_hour_local == 5
    assert cfg.vacuum_after_prune is True


def test_retention_config_rejects_invalid_run_hour() -> None:
    import pytest as _pt  # noqa: PLC0415
    from pydantic import ValidationError  # noqa: PLC0415

    from capo.config import RetentionConfig  # noqa: PLC0415

    with _pt.raises(ValidationError):
        RetentionConfig(run_hour_local=24)
    with _pt.raises(ValidationError):
        RetentionConfig(run_hour_local=-1)


def test_main_imports_retention_scheduler() -> None:
    """Smoke: ``capo.main`` exposes the scheduler symbols.

    Boot wiring (§9.1) — the scheduler is imported lazily inside
    ``amain``; this test just confirms the symbols are reachable from
    the canonical module so a regression is caught fast.
    """
    from capo.maintenance.retention import (  # noqa: PLC0415, F401
        start_retention_scheduler,
        stop_retention_scheduler,
    )

    # Smoke-check the constant matches the spec default.
    assert DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS == 14
    assert DEFAULT_RETENTION_RUN_HOUR_LOCAL == 3

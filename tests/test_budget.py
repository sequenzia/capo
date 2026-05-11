"""Tests for :mod:`capo.budget` (Task #49, Phase 5, §5.9 / §6.5).

Covers the Task #49 acceptance criteria:

Functional
- ``BudgetCheckResult`` shape (status / daily_usd / soft / hard / message).
- Cap math: under-soft → ``ok``; soft ≤ x < hard → ``soft_warn``;
  x ≥ hard → ``hard_block`` (unless override armed → ``overridden``).
- Override semantics: only consumed when a hard block would have fired.
  Soft-warn-only crossings DO NOT change the override state.
- Friendly hard-block message names the cap + suggests ``/override``.

Edge Cases
- ``daily_total_usd`` raises → fail-open (``ok``) + warning logged.
- ``settings.budget`` missing → defaults used ($5 / $20).
- ``/override`` + soft cap (no hard hit yet) → ``soft_warn`` (per
  documented choice in :mod:`capo.budget` docstring).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from capo.budget import (
    DEFAULT_HARD_CAP_USD,
    DEFAULT_SOFT_CAP_USD,
    BudgetCheckResult,
    check_budget,
    format_hard_block_message,
    format_overridden_message,
    format_soft_warn_message,
)
from capo.costs import _ensure_costs_table, record_cost
from capo.memory.store import open_connection

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FakeBudget:
    soft_daily_usd: float
    hard_daily_usd: float


@dataclass(frozen=True)
class _FakePaths:
    db_path: Path


@dataclass(frozen=True)
class _FakeSettings:
    """Minimal settings shim: only ``budget`` + ``paths`` are read by
    :func:`check_budget`."""

    budget: _FakeBudget
    paths: _FakePaths


def _make_settings(
    db_path: Path, *, soft: float = 5.00, hard: float = 20.00
) -> _FakeSettings:
    return _FakeSettings(
        budget=_FakeBudget(soft_daily_usd=soft, hard_daily_usd=hard),
        paths=_FakePaths(db_path=db_path),
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fresh ``state.db`` with the ``costs`` table applied."""
    p = tmp_path / "state.db"
    conn = open_connection(p)
    try:
        _ensure_costs_table(conn)
    finally:
        conn.close()
    return p


async def _seed_spend(
    db_path: Path, user_id: str, total_usd: str, day_iso: str = "2026-05-11"
) -> None:
    """Insert one cost row so ``daily_total_usd`` returns ``total_usd``."""
    # ``record_cost`` requires a known model so cost_usd is non-zero; we
    # bypass by computing cost via a known opus row would over-complicate.
    # Instead, INSERT directly.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO costs(recorded_at, user_id, parent_thread_id, model, "
            "input_tokens, output_tokens, cached_tokens, cost_usd, span_id) "
            "VALUES (?, ?, ?, 'claude-opus-4-7', 0, 0, 0, ?, NULL)",
            (f"{day_iso}T12:00:00Z", user_id, "amc:test", float(total_usd)),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit: BudgetCheckResult shape
# ---------------------------------------------------------------------------


def test_result_is_frozen_dataclass() -> None:
    from dataclasses import FrozenInstanceError

    r = BudgetCheckResult(
        status="ok",
        daily_usd=Decimal("0"),
        soft_cap_usd=Decimal("5"),
        hard_cap_usd=Decimal("20"),
        message=None,
    )
    with pytest.raises(FrozenInstanceError):
        r.status = "soft_warn"  # type: ignore[misc] # frozen


def test_result_status_literal_values() -> None:
    """Sanity: each of the four documented statuses constructs cleanly."""
    for status in ("ok", "soft_warn", "hard_block", "overridden"):
        r = BudgetCheckResult(
            status=status,  # type: ignore[arg-type]
            daily_usd=Decimal("0"),
            soft_cap_usd=Decimal("5"),
            hard_cap_usd=Decimal("20"),
            message=None,
        )
        assert r.status == status


# ---------------------------------------------------------------------------
# Unit: message formatters
# ---------------------------------------------------------------------------


def test_hard_block_message_names_cap_and_suggests_override() -> None:
    msg = format_hard_block_message(Decimal("21.50"), Decimal("20.00"))
    assert "$21.50" in msg
    assert "$20.00" in msg
    assert "/override" in msg


def test_soft_warn_message_names_cap() -> None:
    msg = format_soft_warn_message(Decimal("6.00"), Decimal("5.00"))
    assert "$6.00" in msg
    assert "$5.00" in msg


def test_overridden_message_names_cap_and_total() -> None:
    msg = format_overridden_message(Decimal("21.50"), Decimal("20.00"))
    assert "$21.50" in msg
    assert "$20.00" in msg


# ---------------------------------------------------------------------------
# Unit: cap math (no DB)
# ---------------------------------------------------------------------------


async def test_check_budget_ok_when_under_soft_cap(db_path: Path) -> None:
    await _seed_spend(db_path, "u1", "1.00")
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "ok"
    assert r.message is None
    assert r.daily_usd == Decimal("1.000000")
    assert r.soft_cap_usd == Decimal("5.00")
    assert r.hard_cap_usd == Decimal("20.00")


async def test_check_budget_soft_warn_at_or_above_soft_cap(
    db_path: Path,
) -> None:
    await _seed_spend(db_path, "u1", "5.00")  # exactly at soft cap
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "soft_warn"
    assert r.message is not None
    assert "5.00" in r.message


async def test_check_budget_hard_block_at_or_above_hard_cap(
    db_path: Path,
) -> None:
    await _seed_spend(db_path, "u1", "25.00")  # past hard cap
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "hard_block"
    assert r.message is not None
    assert "/override" in r.message
    assert "$20.00" in r.message  # cap named
    assert "$25.00" in r.message  # daily total named


async def test_check_budget_hard_block_exactly_at_cap(db_path: Path) -> None:
    """``>=`` not ``>``: spending exactly the cap blocks."""
    await _seed_spend(db_path, "u1", "20.00")
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "hard_block"


# ---------------------------------------------------------------------------
# Override semantics
# ---------------------------------------------------------------------------


async def test_override_armed_with_hard_block_returns_overridden(
    db_path: Path,
) -> None:
    await _seed_spend(db_path, "u1", "25.00")
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1",
        "amc:test",
        db_path,
        settings,
        override_armed=True,
        day_iso="2026-05-11",
    )
    assert r.status == "overridden"
    assert r.message is not None
    assert "$25.00" in r.message


async def test_override_armed_with_soft_warn_stays_soft_warn(
    db_path: Path,
) -> None:
    """Per documented design: override is NOT consumed on soft-warn-only.

    The dispatcher's responsibility is to call ``consume_cost_override``
    only when ``status == "overridden"``. The caller can keep
    ``override_armed=True`` set on the next call until a hard block
    actually fires.
    """
    await _seed_spend(db_path, "u1", "6.00")  # past soft, under hard
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1",
        "amc:test",
        db_path,
        settings,
        override_armed=True,
        day_iso="2026-05-11",
    )
    assert r.status == "soft_warn"


async def test_override_armed_with_ok_stays_ok(db_path: Path) -> None:
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1",
        "amc:test",
        db_path,
        settings,
        override_armed=True,
        day_iso="2026-05-11",
    )
    assert r.status == "ok"


# ---------------------------------------------------------------------------
# Edge: fail-open
# ---------------------------------------------------------------------------


async def test_fail_open_when_daily_total_usd_raises(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Missing ``costs`` table → ``daily_total_usd`` raises → ``ok``."""
    bogus_db = tmp_path / "missing.db"
    # Create the file but DO NOT apply the migration. ``costs`` table
    # missing → query raises sqlite3.OperationalError.
    bogus_db.touch()
    settings = _make_settings(bogus_db)
    with caplog.at_level(logging.WARNING, logger="capo.budget"):
        r = await check_budget(
            "u1", "amc:test", bogus_db, settings, day_iso="2026-05-11"
        )
    assert r.status == "ok"
    assert r.daily_usd == Decimal("0.000000")
    # Warning emitted.
    assert any(
        "daily_total_usd failed" in rec.message
        for rec in caplog.records
    )


async def test_fail_open_when_settings_budget_missing(db_path: Path) -> None:
    """Settings missing ``budget`` block → defaults ($5 / $20) used."""

    @dataclass(frozen=True)
    class _Bare:
        paths: _FakePaths
        # no ``budget`` attribute

    bare = _Bare(paths=_FakePaths(db_path=db_path))
    r = await check_budget(
        "u1", "amc:test", db_path, bare, day_iso="2026-05-11"
    )
    assert r.soft_cap_usd == DEFAULT_SOFT_CAP_USD
    assert r.hard_cap_usd == DEFAULT_HARD_CAP_USD


async def test_defaults_match_task_spec() -> None:
    assert Decimal("5.00") == DEFAULT_SOFT_CAP_USD
    assert Decimal("20.00") == DEFAULT_HARD_CAP_USD


# ---------------------------------------------------------------------------
# Decimal precision
# ---------------------------------------------------------------------------


async def test_decimal_cap_comparison_no_float_drift(db_path: Path) -> None:
    """Caps are floats in config; converted via str(...) so no binary noise."""
    # 19.99 < 20.00 strictly → ``ok`` (not ``hard_block``).
    await _seed_spend(db_path, "u1", "4.99")
    settings = _make_settings(db_path, soft=5.00, hard=20.00)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "ok"

    # 5.00 exactly → ``soft_warn`` (>= soft).
    await _seed_spend(db_path, "u2", "5.00")
    r = await check_budget(
        "u2", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "soft_warn"


# ---------------------------------------------------------------------------
# Multi-user isolation
# ---------------------------------------------------------------------------


async def test_per_user_isolation(db_path: Path) -> None:
    """Today's spend by user A must not block user B."""
    await _seed_spend(db_path, "a", "100.00")
    await _seed_spend(db_path, "b", "0.00")
    settings = _make_settings(db_path)
    ra = await check_budget(
        "a", "amc:t", db_path, settings, day_iso="2026-05-11"
    )
    rb = await check_budget(
        "b", "amc:t", db_path, settings, day_iso="2026-05-11"
    )
    assert ra.status == "hard_block"
    assert rb.status == "ok"


# ---------------------------------------------------------------------------
# Daily isolation
# ---------------------------------------------------------------------------


async def test_per_day_isolation(db_path: Path) -> None:
    """Yesterday's spend doesn't count toward today's budget."""
    await _seed_spend(db_path, "u1", "100.00", day_iso="2026-05-10")
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "ok"


# ---------------------------------------------------------------------------
# Integration with real ``record_cost``
# ---------------------------------------------------------------------------


async def test_check_budget_with_record_cost_round_trip(
    db_path: Path,
) -> None:
    """End-to-end: ``record_cost`` writes; ``check_budget`` reads it back."""
    # 1M output tokens at $75/Mtok for opus = $75 (well past $20 hard cap).
    await record_cost(
        db_path,
        model="claude-opus-4-7",
        input_tokens=0,
        output_tokens=1_000_000,
        cached_tokens=0,
        user_id="u1",
        parent_thread_id="amc:test",
        recorded_at="2026-05-11T12:00:00Z",
    )
    settings = _make_settings(db_path)
    r = await check_budget(
        "u1", "amc:test", db_path, settings, day_iso="2026-05-11"
    )
    assert r.status == "hard_block"
    assert r.daily_usd == Decimal("75.000000")

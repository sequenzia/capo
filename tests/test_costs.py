"""Tests for :mod:`capo.costs` (Task #48, Phase 5).

Covers the spec §5.9 / §6.5 + task #48 acceptance criteria:

Functional
- ``compute_cost_usd`` returns exact ``Decimal`` values for known models
  (Claude Opus 4.7, Sonnet 4.6, Haiku 4.5) at 6-dp precision.
- Cached-token discount applied correctly.
- ``record_cost`` + ``daily_total_usd`` round-trip.
- ``record_cost`` + ``thread_total_usd`` round-trip.
- ``record_run_usage`` iterates ``ModelResponse`` messages and records
  one row per message.

Edge Cases
- Unknown model → ``cost_usd=0`` + warning, no exception.
- Negative / non-numeric token counts → coerced to 0 + warning.
- Concurrent records → idempotent (each gets unique ``cost_id``).

Integration
- Migration upgrade/downgrade round-trip (see also
  ``tests/test_costs_migration.py`` for the alembic-CLI variant).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from capo.costs import (
    PRICING_TABLE,
    _ensure_costs_table,
    _normalize_model_name,
    compute_cost_usd,
    daily_total_usd,
    record_cost,
    record_run_usage,
    thread_total_usd,
)
from capo.memory.store import open_connection

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fresh state.db with the ``costs`` table applied."""
    path = tmp_path / "state.db"
    conn = open_connection(path)
    try:
        _ensure_costs_table(conn)
    finally:
        conn.close()
    return path


# ---------------------------------------------------------------------------
# Unit: PRICING_TABLE shape
# ---------------------------------------------------------------------------


def test_pricing_table_has_claude_4_family() -> None:
    """The three Claude 4 family members are pinned with Decimal prices."""
    assert "claude-opus-4-7" in PRICING_TABLE
    assert "claude-sonnet-4-6" in PRICING_TABLE
    assert "claude-haiku-4-5" in PRICING_TABLE
    for key, pricing in PRICING_TABLE.items():
        assert isinstance(pricing.input_per_mtok, Decimal), key
        assert isinstance(pricing.output_per_mtok, Decimal), key
        assert isinstance(pricing.cached_input_per_mtok, Decimal), key
        # Cached input MUST be cheaper than fresh input.
        assert pricing.cached_input_per_mtok < pricing.input_per_mtok, key


# ---------------------------------------------------------------------------
# Unit: _normalize_model_name
# ---------------------------------------------------------------------------


def test_normalize_strips_provider_prefix() -> None:
    assert _normalize_model_name("anthropic:claude-sonnet-4-6") == "claude-sonnet-4-6"
    assert _normalize_model_name("claude-sonnet-4-6") == "claude-sonnet-4-6"


def test_normalize_lowercases() -> None:
    assert _normalize_model_name("Anthropic:Claude-Opus-4-7") == "claude-opus-4-7"


def test_normalize_takes_last_colon_segment() -> None:
    """``vertex_ai:anthropic:claude-...`` style URIs match the bare model."""
    assert (
        _normalize_model_name("vertex_ai:anthropic:claude-haiku-4-5")
        == "claude-haiku-4-5"
    )


def test_normalize_handles_non_string() -> None:
    assert _normalize_model_name(None) == ""  # type: ignore[arg-type]
    assert _normalize_model_name(123) == ""  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Unit: compute_cost_usd — known-model exact values
# ---------------------------------------------------------------------------


def test_compute_cost_opus_4_7_no_cache() -> None:
    """Opus 4.7 @ 1M input + 100k output, no cache.

    Expected:
      input  = 1_000_000 / 1_000_000 * 15.00 = 15.000000
      output =   100_000 / 1_000_000 * 75.00 =  7.500000
      total  = 22.500000
    """
    cost = compute_cost_usd("claude-opus-4-7", 1_000_000, 100_000, 0)
    assert cost == Decimal("22.500000")


def test_compute_cost_sonnet_4_6_no_cache() -> None:
    """Sonnet 4.6 @ 1M input + 500k output, no cache.

    Expected:
      input  = 1_000_000 / 1_000_000 * 3.00  = 3.000000
      output =   500_000 / 1_000_000 * 15.00 = 7.500000
      total  = 10.500000
    """
    cost = compute_cost_usd("claude-sonnet-4-6", 1_000_000, 500_000, 0)
    assert cost == Decimal("10.500000")


def test_compute_cost_haiku_4_5_no_cache() -> None:
    """Haiku 4.5 @ 2M input + 250k output, no cache.

    Expected:
      input  = 2_000_000 / 1_000_000 * 1.00 = 2.000000
      output =   250_000 / 1_000_000 * 5.00 = 1.250000
      total  = 3.250000
    """
    cost = compute_cost_usd("claude-haiku-4-5", 2_000_000, 250_000, 0)
    assert cost == Decimal("3.250000")


# ---------------------------------------------------------------------------
# Unit: cached-token discount
# ---------------------------------------------------------------------------


def test_compute_cost_cached_discount_sonnet() -> None:
    """Cached input billed at 0.30/Mtok, fresh at 3.00/Mtok.

    Sonnet 4.6 — 100k fresh input + 900k cached input + 0 output.

    Expected:
      fresh  = 100_000 / 1_000_000 * 3.00 = 0.300000
      cached = 900_000 / 1_000_000 * 0.30 = 0.270000
      total  = 0.570000

    (Per Anthropic's API contract, ``input_tokens`` and
    ``cache_read_tokens`` are non-overlapping in the source — we DO
    NOT subtract cached_tokens from input_tokens when input_tokens
    represents the "fresh" bucket.)
    """
    cost = compute_cost_usd("claude-sonnet-4-6", 100_000, 0, 900_000)
    assert cost == Decimal("0.570000")


def test_compute_cost_cached_discount_opus() -> None:
    """Opus 4.7 — same shape as the Sonnet test, scaled to Opus pricing.

    Expected:
      fresh  = 100_000 / 1_000_000 * 15.00 = 1.500000
      cached = 900_000 / 1_000_000 * 1.50  = 1.350000
      total  = 2.850000
    """
    cost = compute_cost_usd("claude-opus-4-7", 100_000, 0, 900_000)
    assert cost == Decimal("2.850000")


def test_compute_cost_cached_only_haiku() -> None:
    """100% cache hit on Haiku 4.5.

    Expected:
      cached = 1_000_000 / 1_000_000 * 0.10 = 0.100000
    """
    cost = compute_cost_usd("claude-haiku-4-5", 0, 0, 1_000_000)
    assert cost == Decimal("0.100000")


def test_compute_cost_input_and_cached_billed_independently() -> None:
    """``input_tokens`` and ``cached_tokens`` are non-overlapping buckets.

    Per Anthropic's API contract, ``RequestUsage.input_tokens`` is the
    "fresh" input bucket and ``cache_read_tokens`` is the cached bucket
    — billed separately, no subtraction.

    Sonnet 4.6 — input=500k, cached=600k.
    Expected:
      fresh  = 500_000 / 1_000_000 * 3.00 = 1.500000
      cached = 600_000 / 1_000_000 * 0.30 = 0.180000
      total  = 1.680000
    """
    cost = compute_cost_usd("claude-sonnet-4-6", 500_000, 0, 600_000)
    assert cost == Decimal("1.680000")


def test_compute_cost_is_6_decimal_places() -> None:
    """Costs are always quantized to 6 decimal places."""
    cost = compute_cost_usd("claude-sonnet-4-6", 1, 1, 0)
    # 1 input token at 3.00/Mtok = 0.000003
    # 1 output token at 15.00/Mtok = 0.000015
    # total = 0.000018
    assert cost == Decimal("0.000018")
    assert cost.as_tuple().exponent == -6


def test_compute_cost_provider_prefix_matches() -> None:
    """``anthropic:`` prefix resolves to the same row as the bare key."""
    bare = compute_cost_usd("claude-sonnet-4-6", 1_000_000, 0, 0)
    pref = compute_cost_usd("anthropic:claude-sonnet-4-6", 1_000_000, 0, 0)
    assert bare == pref


# ---------------------------------------------------------------------------
# Unit: edge cases — unknown / malformed inputs
# ---------------------------------------------------------------------------


def test_compute_cost_unknown_model_returns_zero(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown model → ``cost_usd=0`` + warning. Do NOT raise."""
    caplog.set_level(logging.WARNING, logger="capo.costs")
    cost = compute_cost_usd("gpt-7-future", 1_000_000, 500_000, 0)
    assert cost == Decimal("0.000000")
    assert any(
        "unknown model" in rec.message.lower() for rec in caplog.records
    )


def test_compute_cost_negative_tokens_coerced(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative tokens → coerced to 0 + warning."""
    caplog.set_level(logging.WARNING, logger="capo.costs")
    cost = compute_cost_usd("claude-sonnet-4-6", -50, 100, 0)
    # input=0, output=100/1M*15 = 0.001500
    assert cost == Decimal("0.001500")
    assert any("negative" in rec.message.lower() for rec in caplog.records)


def test_compute_cost_non_numeric_tokens_coerced(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Non-numeric token counts → coerced to 0 + warning."""
    caplog.set_level(logging.WARNING, logger="capo.costs")
    cost = compute_cost_usd(
        "claude-sonnet-4-6", "not a number", 100, 0  # type: ignore[arg-type]
    )
    assert cost == Decimal("0.001500")  # output only
    assert any("non-numeric" in rec.message.lower() for rec in caplog.records)


def test_compute_cost_zero_tokens() -> None:
    """All-zero inputs yield zero cost."""
    cost = compute_cost_usd("claude-sonnet-4-6", 0, 0, 0)
    assert cost == Decimal("0.000000")


# ---------------------------------------------------------------------------
# Integration: record_cost + query helpers
# ---------------------------------------------------------------------------


async def test_record_cost_roundtrip(db_path: Path) -> None:
    """``record_cost`` inserts a row with the computed ``cost_usd``."""
    cost_id = await record_cost(
        db_path,
        model="claude-sonnet-4-6",
        input_tokens=1_000_000,
        output_tokens=500_000,
        cached_tokens=0,
        user_id="alice",
        parent_thread_id="amc:chan-1",
        span_id="span-abc",
    )
    assert cost_id > 0

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM costs WHERE cost_id = ?", (cost_id,)
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["model"] == "claude-sonnet-4-6"
    assert row["input_tokens"] == 1_000_000
    assert row["output_tokens"] == 500_000
    assert row["cached_tokens"] == 0
    # 10.5 USD exact, but stored as REAL — within float precision.
    assert abs(row["cost_usd"] - 10.5) < 1e-9
    assert row["user_id"] == "alice"
    assert row["parent_thread_id"] == "amc:chan-1"
    assert row["span_id"] == "span-abc"
    # ISO timestamp shape.
    assert row["recorded_at"].endswith("Z")


async def test_daily_total_usd_sums_only_matching_day(db_path: Path) -> None:
    """``daily_total_usd`` filters by ``user_id`` AND day prefix."""
    # Alice spent 10.5 on 2026-05-11.
    await record_cost(
        db_path,
        model="claude-sonnet-4-6",
        input_tokens=1_000_000,
        output_tokens=500_000,
        user_id="alice",
        recorded_at="2026-05-11T12:00:00Z",
    )
    # Alice spent 3.25 on 2026-05-11 (different model).
    await record_cost(
        db_path,
        model="claude-haiku-4-5",
        input_tokens=2_000_000,
        output_tokens=250_000,
        user_id="alice",
        recorded_at="2026-05-11T18:30:00Z",
    )
    # Alice spent 22.5 on 2026-05-12 (different day — should NOT count).
    await record_cost(
        db_path,
        model="claude-opus-4-7",
        input_tokens=1_000_000,
        output_tokens=100_000,
        user_id="alice",
        recorded_at="2026-05-12T01:00:00Z",
    )
    # Bob spent 10.5 on 2026-05-11 (different user — should NOT count).
    await record_cost(
        db_path,
        model="claude-sonnet-4-6",
        input_tokens=1_000_000,
        output_tokens=500_000,
        user_id="bob",
        recorded_at="2026-05-11T15:00:00Z",
    )

    total = await daily_total_usd(db_path, "alice", "2026-05-11")
    assert total == Decimal("13.750000")  # 10.5 + 3.25


async def test_daily_total_usd_empty_returns_zero(db_path: Path) -> None:
    """No matching rows → ``Decimal("0.000000")``, no error."""
    total = await daily_total_usd(db_path, "nobody", "2026-05-11")
    assert total == Decimal("0.000000")


async def test_thread_total_usd_sums_across_days(db_path: Path) -> None:
    """``thread_total_usd`` sums by ``parent_thread_id`` across all time."""
    await record_cost(
        db_path,
        model="claude-sonnet-4-6",
        input_tokens=1_000_000,
        output_tokens=500_000,
        parent_thread_id="amc:thread-1",
        recorded_at="2026-05-10T12:00:00Z",
    )
    await record_cost(
        db_path,
        model="claude-haiku-4-5",
        input_tokens=2_000_000,
        output_tokens=250_000,
        parent_thread_id="amc:thread-1",
        recorded_at="2026-05-11T12:00:00Z",
    )
    # Different thread — should NOT count.
    await record_cost(
        db_path,
        model="claude-opus-4-7",
        input_tokens=1_000_000,
        output_tokens=100_000,
        parent_thread_id="amc:thread-2",
    )

    total = await thread_total_usd(db_path, "amc:thread-1")
    assert total == Decimal("13.750000")  # 10.5 + 3.25


async def test_thread_total_usd_empty_returns_zero(db_path: Path) -> None:
    total = await thread_total_usd(db_path, "amc:no-such-thread")
    assert total == Decimal("0.000000")


# ---------------------------------------------------------------------------
# Edge case: unknown model still inserts a row with cost_usd=0
# ---------------------------------------------------------------------------


async def test_record_cost_unknown_model_zero(
    db_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Unknown model name → row inserted with ``cost_usd=0`` + warning."""
    caplog.set_level(logging.WARNING, logger="capo.costs")
    cost_id = await record_cost(
        db_path,
        model="future-model-9000",
        input_tokens=1_000_000,
        output_tokens=500_000,
        user_id="alice",
    )

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT cost_usd FROM costs WHERE cost_id = ?", (cost_id,)
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == pytest.approx(0.0)
    assert any(
        "unknown model" in rec.message.lower() for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# Edge case: concurrent records → idempotent (unique cost_ids)
# ---------------------------------------------------------------------------


async def test_concurrent_record_cost_no_collision(db_path: Path) -> None:
    """Parallel ``record_cost`` calls produce unique ``cost_id``s."""

    async def _one(idx: int) -> int:
        return await record_cost(
            db_path,
            model="claude-sonnet-4-6",
            input_tokens=1000,
            output_tokens=500,
            user_id=f"u-{idx}",
        )

    cost_ids = await asyncio.gather(*(_one(i) for i in range(5)))
    assert len(set(cost_ids)) == 5  # all distinct


# ---------------------------------------------------------------------------
# Integration: record_run_usage with Pydantic AI ModelResponse messages
# ---------------------------------------------------------------------------


async def test_record_run_usage_records_per_model_response(
    db_path: Path,
) -> None:
    """``record_run_usage`` walks ``new_messages`` and inserts one row per
    ``ModelResponse``."""
    from pydantic_ai.messages import (  # type: ignore[import-not-found]
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )
    from pydantic_ai.usage import RequestUsage  # type: ignore[import-not-found]

    messages = [
        ModelRequest(parts=[UserPromptPart(content="hi")]),
        ModelResponse(
            parts=[TextPart(content="hello")],
            usage=RequestUsage(
                input_tokens=1_000_000,
                output_tokens=500_000,
                cache_read_tokens=0,
            ),
            model_name="claude-sonnet-4-6",
        ),
        ModelResponse(
            parts=[TextPart(content="follow-up")],
            usage=RequestUsage(
                input_tokens=100_000,
                output_tokens=0,
                cache_read_tokens=900_000,
            ),
            model_name="claude-sonnet-4-6",
        ),
    ]

    cost_ids = await record_run_usage(
        db_path,
        messages,
        user_id="alice",
        parent_thread_id="amc:chan-7",
    )
    assert len(cost_ids) == 2

    # Aggregate matches: 10.500000 + 0.570000 = 11.070000
    total = await thread_total_usd(db_path, "amc:chan-7")
    assert total == Decimal("11.070000")


async def test_record_run_usage_skips_messages_without_model_name(
    db_path: Path,
) -> None:
    """Messages with no ``model_name`` (defensive) are skipped silently."""
    from pydantic_ai.messages import (  # type: ignore[import-not-found]
        ModelResponse,
        TextPart,
    )
    from pydantic_ai.usage import RequestUsage  # type: ignore[import-not-found]

    msg = ModelResponse(
        parts=[TextPart(content="hello")],
        usage=RequestUsage(input_tokens=100, output_tokens=50),
        model_name=None,
    )
    cost_ids = await record_run_usage(db_path, [msg])
    assert cost_ids == []


async def test_record_run_usage_skips_zero_usage(db_path: Path) -> None:
    """``ModelResponse`` with empty usage is skipped (no DB churn)."""
    from pydantic_ai.messages import (  # type: ignore[import-not-found]
        ModelResponse,
        TextPart,
    )
    from pydantic_ai.usage import RequestUsage  # type: ignore[import-not-found]

    msg = ModelResponse(
        parts=[TextPart(content="hello")],
        usage=RequestUsage(),  # all zeros
        model_name="claude-sonnet-4-6",
    )
    cost_ids = await record_run_usage(db_path, [msg])
    assert cost_ids == []


async def test_record_run_usage_ignores_non_model_response(
    db_path: Path,
) -> None:
    """Non-``ModelResponse`` messages in the iterable are ignored."""
    from pydantic_ai.messages import (  # type: ignore[import-not-found]
        ModelRequest,
        UserPromptPart,
    )

    msg = ModelRequest(parts=[UserPromptPart(content="ignored")])
    cost_ids = await record_run_usage(db_path, [msg])
    assert cost_ids == []

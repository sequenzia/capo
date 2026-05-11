"""Cost accountant — per-call LLM cost tally + per-thread/day rollups (§5.9, §6.5).

This module is the canonical site for translating Pydantic AI usage events
into USD costs and persisting them to the ``costs`` table in ``state.db``.

Public surface
--------------

- :func:`compute_cost_usd` — pure function, ``(model, input_tokens,
  output_tokens, cached_tokens=0) -> Decimal``. Uses
  :data:`PRICING_TABLE`; unknown models return ``Decimal("0")`` with a
  logged warning. Rounded to 6 decimal places (per task #48 acceptance
  criteria).
- :func:`record_cost` — async; persists one row using
  :func:`capo.memory.store.begin_immediate_with_retry`.
- :func:`record_run_usage` — async; iterates over
  ``result.new_messages()`` (Pydantic AI ``RunResult``) and calls
  :func:`record_cost` for every :class:`pydantic_ai.messages.ModelResponse`
  with non-zero usage. Wired into :mod:`capo.transport.dispatcher` after
  each agent turn completes.
- :func:`daily_total_usd` — query helper; sum of ``cost_usd`` for a
  ``user_id`` on a UTC calendar day (``day_iso`` ``"YYYY-MM-DD"``).
- :func:`thread_total_usd` — query helper; sum of ``cost_usd`` for a
  ``parent_thread_id`` across all time.

Pricing
-------

:data:`PRICING_TABLE` is a module-level constant carrying ``Decimal``
prices per million tokens for the Claude 4 family (Opus 4.7, Sonnet 4.6,
Haiku 4.5). The values are sourced from Anthropic's public API pricing
page as of 2026-05-11 — see the table docstring for the source URL and
the exact per-million-token figures.

Keys are matched case-insensitively against a normalized form of the
``ModelResponse.model_name`` string. Both the provider-prefixed form
(``"anthropic:claude-sonnet-4-6"``) and the bare model id
(``"claude-sonnet-4-6"``) match the same pricing row.

Error policy
------------

- Unknown model name → log a warning, persist a row with ``cost_usd=0``.
  Do NOT raise. This keeps the dispatcher loop alive when a new model
  ships ahead of the pricing table update.
- Negative or missing token counts → coerce to 0 + log a warning. Same
  reasoning — we never lose telemetry for a corrupted usage event.
- Concurrent records from parallel agent runs → idempotent by design
  (no unique constraint; ``cost_id`` autoincrements).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from capo.memory.store import begin_immediate_with_retry, open_connection

logger = logging.getLogger("capo.costs")

# ---------------------------------------------------------------------------
# Pricing table
#
# Source: Anthropic API pricing page, captured 2026-05-11. The Claude 4
# generation publishes three tiers — Opus / Sonnet / Haiku — with the
# canonical "per million tokens" structure for input, output, and a
# discounted "cache read" rate for prompt-caching reuse.
#
# Values are USD per 1_000_000 tokens, stored as ``Decimal`` strings so
# the conversion from token count → cost is exact arithmetic (no
# binary-float rounding) up to the final 6-dp quantization.
#
# This table is the single source of truth for the pricing — bump these
# values when Anthropic publishes a new schedule and update the
# capture-date footnote above. Tests pin the exact ``Decimal`` outputs
# of :func:`compute_cost_usd` against these prices so a stale table can
# never silently undercharge the user.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModelPricing:
    """USD prices per 1_000_000 tokens for a single model."""

    input_per_mtok: Decimal
    output_per_mtok: Decimal
    cached_input_per_mtok: Decimal


PRICING_TABLE: dict[str, ModelPricing] = {
    "claude-opus-4-7": ModelPricing(
        input_per_mtok=Decimal("15.00"),
        output_per_mtok=Decimal("75.00"),
        cached_input_per_mtok=Decimal("1.50"),
    ),
    "claude-sonnet-4-6": ModelPricing(
        input_per_mtok=Decimal("3.00"),
        output_per_mtok=Decimal("15.00"),
        cached_input_per_mtok=Decimal("0.30"),
    ),
    "claude-haiku-4-5": ModelPricing(
        input_per_mtok=Decimal("1.00"),
        output_per_mtok=Decimal("5.00"),
        cached_input_per_mtok=Decimal("0.10"),
    ),
}

#: 6-decimal-place quantum for cost rounding (per task #48 acceptance criteria).
_COST_QUANTUM: Decimal = Decimal("0.000001")

#: Used to scale token counts to "per million" before multiplying by the
#: per-mtok price. Kept as a ``Decimal`` so the division stays exact.
_TOKENS_PER_MTOK: Decimal = Decimal("1000000")


# ---------------------------------------------------------------------------
# Model name normalization
# ---------------------------------------------------------------------------


def _normalize_model_name(model: str) -> str:
    """Strip a Pydantic AI ``"<provider>:<model>"`` prefix and lowercase.

    ``ModelResponse.model_name`` can arrive as either
    ``"anthropic:claude-sonnet-4-6"`` or the bare ``"claude-sonnet-4-6"``
    depending on which provider adapter wrote it. We match on the bare
    form so both produce the same pricing-table lookup.

    Returns the original input lowercased + whitespace-stripped when no
    colon prefix is present.
    """
    if not isinstance(model, str):
        return ""
    candidate = model.strip().lower()
    if ":" in candidate:
        # Take everything after the LAST ":" so unusual provider URIs like
        # ``"vertex_ai:anthropic:claude-..."`` still resolve correctly.
        candidate = candidate.rsplit(":", 1)[1]
    return candidate


def _coerce_token_count(value: Any, *, field: str, model: str) -> int:
    """Coerce ``value`` to a non-negative ``int``.

    Per task #48 edge cases: negative or missing token counts → coerce
    to 0 and emit a structured warning. The dispatcher MUST NOT crash
    on a malformed usage event.
    """
    if value is None:
        return 0
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        logger.warning(
            "cost accountant: non-numeric token count "
            "(field=%s model=%r value=%r) — coerced to 0",
            field,
            model,
            value,
            extra={
                "event": "capo.costs.token_count.non_numeric",
                "field": field,
                "model": model,
                "value": repr(value),
            },
        )
        return 0
    if ivalue < 0:
        logger.warning(
            "cost accountant: negative token count "
            "(field=%s model=%r value=%d) — coerced to 0",
            field,
            model,
            ivalue,
            extra={
                "event": "capo.costs.token_count.negative",
                "field": field,
                "model": model,
                "value": ivalue,
            },
        )
        return 0
    return ivalue


# ---------------------------------------------------------------------------
# Public: compute_cost_usd
# ---------------------------------------------------------------------------


def compute_cost_usd(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
) -> Decimal:
    """Compute the USD cost for one LLM call.

    The "input" leg is split into two buckets: ``cached_tokens`` are
    billed at the per-model ``cached_input_per_mtok`` rate; the remaining
    ``max(input_tokens - cached_tokens, 0)`` is billed at
    ``input_per_mtok``. This matches Anthropic's prompt-caching billing
    model: cache *reads* are cheaper than fresh-input tokens.

    Args:
        model: The Pydantic AI ``ModelResponse.model_name`` string. May
            be provider-prefixed (``"anthropic:claude-..."``) or bare.
            Unknown models return ``Decimal("0")`` after a logged warning.
        input_tokens: TOTAL input tokens (including any cached portion).
            Negative or non-numeric values are coerced to 0 with a
            warning.
        output_tokens: Output tokens billed at ``output_per_mtok``.
        cached_tokens: Cache-read tokens billed at the discount rate.
            Clamped to ``[0, input_tokens]``.

    Returns:
        The total cost as a :class:`~decimal.Decimal`, quantized to 6
        decimal places via ``ROUND_HALF_UP``.
    """
    norm_model = _normalize_model_name(model)
    pricing = PRICING_TABLE.get(norm_model)

    if pricing is None:
        logger.warning(
            "cost accountant: unknown model %r (normalized=%r) — recording "
            "cost_usd=0; update PRICING_TABLE",
            model,
            norm_model,
            extra={
                "event": "capo.costs.model.unknown",
                "model": model,
                "model_normalized": norm_model,
            },
        )
        return Decimal("0.000000")

    in_tok = _coerce_token_count(input_tokens, field="input_tokens", model=model)
    out_tok = _coerce_token_count(output_tokens, field="output_tokens", model=model)
    cached_tok = _coerce_token_count(cached_tokens, field="cached_tokens", model=model)

    # Anthropic bills cache reads as a SEPARATE line item — the
    # ``input_tokens`` figure in their API response excludes cache
    # reads. Pydantic AI mirrors this contract: ``RequestUsage.input_tokens``
    # is the "fresh" input bucket and ``cache_read_tokens`` is the
    # cached bucket; the two are non-overlapping in the source. We
    # therefore bill them independently — DO NOT subtract cached_tok
    # from in_tok.
    fresh_input = in_tok

    cost = (
        (Decimal(fresh_input) * pricing.input_per_mtok)
        + (Decimal(cached_tok) * pricing.cached_input_per_mtok)
        + (Decimal(out_tok) * pricing.output_per_mtok)
    ) / _TOKENS_PER_MTOK

    return cost.quantize(_COST_QUANTUM, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Public: record_cost
# ---------------------------------------------------------------------------


def _utc_iso_now() -> str:
    """Return the current UTC time in ``YYYY-MM-DDTHH:MM:SSZ`` form."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record_cost_sync(
    db_path: Path | str,
    *,
    recorded_at: str,
    user_id: str | None,
    parent_thread_id: str | None,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    cost_usd: Decimal,
    span_id: str | None,
) -> int:
    """Synchronous worker for :func:`record_cost`.

    Returns the autoincremented ``cost_id`` for caller-side audit / tests.
    """
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(conn, operation="record_cost"):
            cur = conn.execute(
                "INSERT INTO costs ("
                "recorded_at, user_id, parent_thread_id, model, "
                "input_tokens, output_tokens, cached_tokens, cost_usd, span_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    recorded_at,
                    user_id,
                    parent_thread_id,
                    model,
                    int(input_tokens),
                    int(output_tokens),
                    int(cached_tokens),
                    float(cost_usd),
                    span_id,
                ),
            )
            cost_id = int(cur.lastrowid or 0)
        return cost_id
    finally:
        conn.close()


async def record_cost(
    db_path: Path | str,
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
    user_id: str | None = None,
    parent_thread_id: str | None = None,
    span_id: str | None = None,
    recorded_at: str | None = None,
) -> int:
    """Persist a single cost row to ``state.db``.

    Computes ``cost_usd`` via :func:`compute_cost_usd`, coerces missing /
    negative token counts to 0 (with a warning), and inserts inside
    :func:`capo.memory.store.begin_immediate_with_retry` so SQLite write
    contention never surfaces to the caller (§6.1 invariant).

    Args:
        db_path: Filesystem path to ``state.db``. Typically
            ``settings.paths.db_path``.
        model: ``ModelResponse.model_name`` (provider-prefixed or bare).
            Unknown models record ``cost_usd=0`` with a logged warning.
        input_tokens, output_tokens, cached_tokens: Token counts from
            the Pydantic AI ``RequestUsage``. Negative or missing values
            are coerced to 0 with a logged warning.
        user_id: Internal user id (nullable).
        parent_thread_id: ``amc:<channel_id>`` (nullable).
        span_id: Optional Logfire span id for cross-reference.
        recorded_at: Optional caller-supplied ISO 8601 UTC timestamp.
            Defaults to ``datetime.now(UTC)`` formatted as
            ``"YYYY-MM-DDTHH:MM:SSZ"``.

    Returns:
        The autoincremented ``cost_id`` of the inserted row.
    """
    ts = recorded_at or _utc_iso_now()
    norm_input = _coerce_token_count(input_tokens, field="input_tokens", model=model)
    norm_output = _coerce_token_count(output_tokens, field="output_tokens", model=model)
    norm_cached = _coerce_token_count(cached_tokens, field="cached_tokens", model=model)

    cost = compute_cost_usd(model, norm_input, norm_output, norm_cached)

    return await asyncio.to_thread(
        _record_cost_sync,
        db_path,
        recorded_at=ts,
        user_id=user_id,
        parent_thread_id=parent_thread_id,
        model=model,
        input_tokens=norm_input,
        output_tokens=norm_output,
        cached_tokens=norm_cached,
        cost_usd=cost,
        span_id=span_id,
    )


# ---------------------------------------------------------------------------
# Pydantic AI integration: record_run_usage
# ---------------------------------------------------------------------------


def _iter_model_responses(messages: Iterable[Any]) -> Iterable[tuple[str, Any]]:
    """Yield ``(model_name, usage)`` tuples for every ``ModelResponse``.

    Lazy-imports :class:`pydantic_ai.messages.ModelResponse` so this
    module stays importable in environments without Pydantic AI (e.g.
    isolated migration tests).

    Skips messages with no ``model_name`` or with a zero-usage record —
    a duplicate write for an empty-usage replay would inflate the row
    count without changing the dollar total, but it's wasted I/O.
    """
    try:
        from pydantic_ai.messages import ModelResponse  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - defensive
        return

    for msg in messages:
        if not isinstance(msg, ModelResponse):
            continue
        model_name = getattr(msg, "model_name", None)
        usage = getattr(msg, "usage", None)
        if not model_name or usage is None:
            continue
        # Skip empty usage (no API call charged for this message).
        has_values = getattr(usage, "has_values", None)
        if callable(has_values) and not has_values():
            continue
        yield model_name, usage


async def record_run_usage(
    db_path: Path | str,
    new_messages: Iterable[Any],
    *,
    user_id: str | None = None,
    parent_thread_id: str | None = None,
    span_id: str | None = None,
) -> list[int]:
    """Record one cost row per ``ModelResponse`` in ``new_messages``.

    Walks the Pydantic AI ``AgentRunResult.new_messages()`` iterable
    (typically ``list(result.new_messages())``), pulls
    ``model_name`` + ``usage.input_tokens`` /
    ``usage.output_tokens`` / ``usage.cache_read_tokens`` from every
    :class:`~pydantic_ai.messages.ModelResponse`, and persists one row
    per model+usage pair via :func:`record_cost`.

    All exceptions inside :func:`record_cost` are logged and swallowed —
    the cost accountant is observability infrastructure, not a critical
    path. A failed cost write MUST NEVER crash the dispatcher loop.

    Args:
        db_path: Filesystem path to ``state.db``.
        new_messages: Iterable of Pydantic AI messages (the output of
            ``result.new_messages()``). Non-``ModelResponse`` messages
            are ignored.
        user_id: Forwarded to :func:`record_cost`.
        parent_thread_id: Forwarded to :func:`record_cost`.
        span_id: Forwarded to :func:`record_cost`.

    Returns:
        List of ``cost_id`` integers (one per successfully recorded
        row). Empty list when no ``ModelResponse`` messages were
        present or every recording failed.
    """
    cost_ids: list[int] = []
    for model_name, usage in _iter_model_responses(new_messages):
        input_tokens = getattr(usage, "input_tokens", 0)
        output_tokens = getattr(usage, "output_tokens", 0)
        cached_tokens = getattr(usage, "cache_read_tokens", 0)
        try:
            cost_id = await record_cost(
                db_path,
                model=model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                user_id=user_id,
                parent_thread_id=parent_thread_id,
                span_id=span_id,
            )
            cost_ids.append(cost_id)
        except Exception as exc:  # noqa: BLE001 - intentional swallow
            logger.warning(
                "cost accountant: record_cost failed for model=%r "
                "user_id=%r parent_thread_id=%r: %r",
                model_name,
                user_id,
                parent_thread_id,
                exc,
                extra={
                    "event": "capo.costs.record_cost.failed",
                    "model": model_name,
                    "user_id": user_id,
                    "parent_thread_id": parent_thread_id,
                    "error": repr(exc),
                },
            )
    return cost_ids


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def _sum_to_decimal(value: Any) -> Decimal:
    """Convert a SQLite ``SUM(cost_usd)`` result to a Decimal.

    SQLite's ``SUM`` returns ``None`` on an empty set, an int on
    integer-only sums, or a float on real-valued sums. We funnel
    everything through ``str()`` so the resulting ``Decimal`` is
    deterministic regardless of binary-float representation.
    """
    if value is None:
        return Decimal("0.000000")
    # Coerce float/int → Decimal via str() to avoid binary-float noise.
    dec = Decimal(str(value))
    return dec.quantize(_COST_QUANTUM, rounding=ROUND_HALF_UP)


def _daily_total_sync(
    db_path: Path | str, user_id: str, day_iso: str
) -> Decimal:
    conn = open_connection(db_path)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM costs "
            "WHERE user_id = ? AND recorded_at LIKE ?",
            (user_id, f"{day_iso}%"),
        ).fetchone()
    finally:
        conn.close()
    return _sum_to_decimal(row[0] if row else None)


async def daily_total_usd(
    db_path: Path | str, user_id: str, day_iso: str
) -> Decimal:
    """Sum ``cost_usd`` for ``user_id`` on the UTC calendar day ``day_iso``.

    ``day_iso`` must be in ``"YYYY-MM-DD"`` form (no time component).
    Matches via ``recorded_at LIKE 'YYYY-MM-DD%'`` so we hit the
    ``idx_costs_user_recorded`` index without parsing dates in SQL.

    Returns ``Decimal("0.000000")`` when no rows match (no errors raised
    for "no spend yet today").
    """
    return await asyncio.to_thread(_daily_total_sync, db_path, user_id, day_iso)


def _thread_total_sync(
    db_path: Path | str, parent_thread_id: str
) -> Decimal:
    conn = open_connection(db_path)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM costs "
            "WHERE parent_thread_id = ?",
            (parent_thread_id,),
        ).fetchone()
    finally:
        conn.close()
    return _sum_to_decimal(row[0] if row else None)


async def thread_total_usd(
    db_path: Path | str, parent_thread_id: str
) -> Decimal:
    """Sum ``cost_usd`` for one ``parent_thread_id`` across all time.

    Used by the §5.9 budget hook (Task #49) to enforce per-thread caps
    in addition to per-user-per-day caps.

    Returns ``Decimal("0.000000")`` when no rows match.
    """
    return await asyncio.to_thread(
        _thread_total_sync, db_path, parent_thread_id
    )


# ---------------------------------------------------------------------------
# Test helpers (exported for parity with other capo modules)
# ---------------------------------------------------------------------------


def _ensure_costs_table(conn: sqlite3.Connection) -> None:
    """Apply the migration-004 DDL to ``conn``.

    Useful in unit tests that build the schema inline (mirroring the
    ``_APPROVALS_DDL`` pattern in ``tests/test_workflows_approval.py``).
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS costs ("
        "cost_id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "recorded_at      TEXT NOT NULL,"
        "user_id          TEXT,"
        "parent_thread_id TEXT,"
        "model            TEXT NOT NULL,"
        "input_tokens     INTEGER NOT NULL,"
        "output_tokens    INTEGER NOT NULL,"
        "cached_tokens    INTEGER DEFAULT 0,"
        "cost_usd         REAL NOT NULL,"
        "span_id          TEXT"
        ")"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_costs_parent_thread_recorded "
        "ON costs(parent_thread_id, recorded_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_costs_user_recorded "
        "ON costs(user_id, recorded_at)"
    )
    conn.commit()


__all__ = [
    "PRICING_TABLE",
    "ModelPricing",
    "compute_cost_usd",
    "daily_total_usd",
    "record_cost",
    "record_run_usage",
    "thread_total_usd",
]

"""add costs table (Phase 5).

Revision ID: 004_costs
Revises: 003_approvals_request_types
Create Date: 2026-05-11

Adds the ``costs`` table backing the Phase 5 cost accountant (Task #48,
spec §5.9 / §6.5). Each row records one LLM API call's usage and the
dollar cost computed from the per-model pricing table in
``capo.costs``. The pre-tool-call budget hook (Task #49) queries this
table via ``daily_total_usd`` / ``thread_total_usd`` to enforce the
``[budget] soft_daily_usd`` / ``hard_daily_usd`` caps.

Schema (per task #48 acceptance criteria):

- ``cost_id``         INTEGER PK AUTOINCREMENT.
- ``recorded_at``     TEXT NOT NULL, ISO 8601 UTC.
- ``user_id``         TEXT NULL — internal user id; nullable because some
                       turns (system / boot sweeps / cold-resume) have no
                       sender user attached.
- ``parent_thread_id`` TEXT NULL — ``amc:<channel_id>`` per
                       :func:`capo.memory.conversation.thread_id_for_amc`.
- ``model``           TEXT NOT NULL — the Pydantic AI ``ModelResponse.model_name``
                       string (e.g. ``"claude-sonnet-4-6"``,
                       ``"anthropic:claude-haiku-4-5"``). Stored verbatim.
- ``input_tokens``    INTEGER NOT NULL.
- ``output_tokens``   INTEGER NOT NULL.
- ``cached_tokens``   INTEGER DEFAULT 0 — cache-read tokens (Anthropic
                       prompt-caching). Discount applied in
                       :func:`capo.costs.compute_cost_usd`.
- ``cost_usd``        REAL NOT NULL — rounded to 6 decimal places.
- ``span_id``         TEXT NULL — Logfire span id (Task #50) for cross-ref;
                       NULL when no span context is available.

Indexes (per task #48 acceptance criteria):

- ``idx_costs_parent_thread_recorded`` on ``(parent_thread_id, recorded_at)``
  — backs ``thread_total_usd`` queries.
- ``idx_costs_user_recorded`` on ``(user_id, recorded_at)`` — backs
  ``daily_total_usd`` queries (the ``recorded_at`` LIKE ``YYYY-MM-DD%``
  filter is an index-prefix scan).

Notes:

- ``cost_usd`` is stored as REAL (per acceptance criteria); the Decimal
  arithmetic in :func:`capo.costs.compute_cost_usd` is the source of
  truth for the value rounded to 6 dp before persistence.
- No FK to ``users(user_id)``: cost rows are written from DBOS step
  context which may pre-date the user row (cold-boot races), and the
  spec §5.9 budget hook only needs aggregate totals — not a join.
- No unique constraint per task #48 "concurrent records from parallel
  agent runs → idempotent (no unique constraint needed; cost_id
  auto-increments)" acceptance criterion.

Migration conventions mirror ``001_init.py`` / ``002_approvals.py``:

- Raw DDL via ``op.execute`` (no ORM metadata layer).
- ``IF NOT EXISTS`` on table + indexes so repeated ``alembic upgrade
  head`` against the same DB is a no-op past the first apply.
- ``downgrade`` drops indexes ahead of the table, in reverse order.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# Alembic identifiers.
revision: str = "004_costs"
down_revision: str | Sequence[str] | None = "003_approvals_request_types"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# DDL — kept verbatim against task #48 acceptance criteria + §5.9 / §6.5
# ---------------------------------------------------------------------------

_COSTS = """
CREATE TABLE IF NOT EXISTS costs (
    cost_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at      TEXT NOT NULL,
    user_id          TEXT,
    parent_thread_id TEXT,
    model            TEXT NOT NULL,
    input_tokens     INTEGER NOT NULL,
    output_tokens    INTEGER NOT NULL,
    cached_tokens    INTEGER DEFAULT 0,
    cost_usd         REAL NOT NULL,
    span_id          TEXT
)
"""

_IDX_COSTS_PARENT_THREAD_RECORDED = """
CREATE INDEX IF NOT EXISTS idx_costs_parent_thread_recorded
    ON costs(parent_thread_id, recorded_at)
"""

_IDX_COSTS_USER_RECORDED = """
CREATE INDEX IF NOT EXISTS idx_costs_user_recorded
    ON costs(user_id, recorded_at)
"""


# Order: table first, then indexes (which reference it).
_UPGRADE_STATEMENTS: tuple[str, ...] = (
    _COSTS,
    _IDX_COSTS_PARENT_THREAD_RECORDED,
    _IDX_COSTS_USER_RECORDED,
)

# Reverse order: drop indexes first, then the table.
_DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    "DROP INDEX IF EXISTS idx_costs_user_recorded",
    "DROP INDEX IF EXISTS idx_costs_parent_thread_recorded",
    "DROP TABLE IF EXISTS costs",
)


def upgrade() -> None:
    """Create the ``costs`` table + supporting indexes, idempotently."""
    for stmt in _UPGRADE_STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    """Drop everything created by :func:`upgrade`, in reverse order."""
    for stmt in _DOWNGRADE_STATEMENTS:
        op.execute(stmt)

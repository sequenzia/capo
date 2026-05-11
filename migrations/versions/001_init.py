"""init state.db schema (Phase 1).

Revision ID: 001_init
Revises:
Create Date: 2026-05-10

Creates every Phase-1 table in ``state.db`` per spec §7.3. The ``approvals``
table is intentionally NOT in this migration — it lands in Phase 4 (#38).

The SQL is held verbatim (modulo whitespace) against §7.3 so the spec remains
the single source of truth. We emit raw DDL via ``op.execute`` rather than
``op.create_table`` to:

- Preserve the exact CHECK / DEFAULT / partial-index syntax from the spec.
- Avoid an ORM metadata layer for a database we manage by hand-written SQL.
- Keep the diff readable when the spec is revised.

Idempotency: every ``CREATE TABLE`` / ``CREATE INDEX`` uses ``IF NOT EXISTS``
so a repeated ``alembic upgrade head`` against the same DB is a no-op past the
first apply. (Alembic's version-table also blocks re-applies, but the
belt-and-braces guard helps when callers replay the SQL outside Alembic.)
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# Alembic identifiers.
revision: str = "001_init"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# DDL — kept verbatim against capo-SPEC.md §7.3
# ---------------------------------------------------------------------------

_USERS = """
CREATE TABLE IF NOT EXISTS users (
    user_id      TEXT PRIMARY KEY,
    display_name TEXT,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_SESSIONS = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id                 TEXT PRIMARY KEY,
    thread_id                  TEXT NOT NULL,
    user_id                    TEXT NOT NULL REFERENCES users(user_id),
    started_at                 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at                   TIMESTAMP,
    compacted_to_message_index INTEGER NOT NULL DEFAULT 0
)
"""

_IDX_SESSIONS_USER_THREAD_ACTIVE = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_thread_active
    ON sessions(user_id, thread_id)
    WHERE ended_at IS NULL
"""

_CONVERSATION_HISTORY = """
CREATE TABLE IF NOT EXISTS conversation_history (
    user_id            TEXT NOT NULL REFERENCES users(user_id),
    thread_id          TEXT NOT NULL,
    message_index      INTEGER NOT NULL,
    session_id         TEXT NOT NULL REFERENCES sessions(session_id),
    ts                 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    model_message_json TEXT NOT NULL,
    PRIMARY KEY (user_id, thread_id, message_index)
)
"""

_IDX_HISTORY_SESSION = """
CREATE INDEX IF NOT EXISTS idx_history_session
    ON conversation_history(user_id, session_id, message_index)
"""

_DELEGATIONS = """
CREATE TABLE IF NOT EXISTS delegations (
    id                       TEXT PRIMARY KEY,
    user_id                  TEXT NOT NULL REFERENCES users(user_id),
    agent                    TEXT NOT NULL CHECK (agent IN ('claude-code','codex')),
    workspace                TEXT NOT NULL,
    pid                      INTEGER,
    session_id_subagent      TEXT,
    brief_json               TEXT NOT NULL,
    status                   TEXT NOT NULL CHECK (
        status IN ('running','completed','failed','killed','pending_approval')
    ),
    started_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at                 TIMESTAMP,
    parent_thread_id         TEXT,
    summary                  TEXT,
    cost_usd                 REAL NOT NULL DEFAULT 0,
    model                    TEXT,
    heartbeat_intervals_json TEXT NOT NULL DEFAULT '[]'
)
"""

_IDX_DELEGATIONS_USER_STATUS = """
CREATE INDEX IF NOT EXISTS idx_delegations_user_status
    ON delegations(user_id, status)
"""

_IDX_DELEGATIONS_STARTED = """
CREATE INDEX IF NOT EXISTS idx_delegations_started
    ON delegations(started_at DESC)
"""

_DELEGATION_OUTPUT = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL REFERENCES delegations(id),
    ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stream        TEXT NOT NULL CHECK (stream IN ('stdout','stderr','event')),
    chunk         TEXT NOT NULL
)
"""

_IDX_OUTPUT_DELEGATION_TS = """
CREATE INDEX IF NOT EXISTS idx_output_delegation_ts
    ON delegation_output(delegation_id, ts)
"""

_DELEGATION_HEARTBEATS = """
CREATE TABLE IF NOT EXISTS delegation_heartbeats (
    delegation_id     TEXT NOT NULL REFERENCES delegations(id),
    threshold_seconds INTEGER NOT NULL,
    sent_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    idempotency_key   TEXT NOT NULL,
    PRIMARY KEY (delegation_id, threshold_seconds)
)
"""

_DAILY_COSTS = """
CREATE TABLE IF NOT EXISTS daily_costs (
    date      TEXT NOT NULL,
    model     TEXT NOT NULL,
    user_id   TEXT NOT NULL REFERENCES users(user_id),
    usd_total REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (date, model, user_id)
)
"""


# Order: parents before children (FKs reference tables that must exist first).
_UPGRADE_STATEMENTS: tuple[str, ...] = (
    _USERS,
    _SESSIONS,
    _IDX_SESSIONS_USER_THREAD_ACTIVE,
    _CONVERSATION_HISTORY,
    _IDX_HISTORY_SESSION,
    _DELEGATIONS,
    _IDX_DELEGATIONS_USER_STATUS,
    _IDX_DELEGATIONS_STARTED,
    _DELEGATION_OUTPUT,
    _IDX_OUTPUT_DELEGATION_TS,
    _DELEGATION_HEARTBEATS,
    _DAILY_COSTS,
)

# Reverse order, with indexes ahead of their tables, for the downgrade path.
_DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    "DROP TABLE IF EXISTS daily_costs",
    "DROP TABLE IF EXISTS delegation_heartbeats",
    "DROP INDEX IF EXISTS idx_output_delegation_ts",
    "DROP TABLE IF EXISTS delegation_output",
    "DROP INDEX IF EXISTS idx_delegations_started",
    "DROP INDEX IF EXISTS idx_delegations_user_status",
    "DROP TABLE IF EXISTS delegations",
    "DROP INDEX IF EXISTS idx_history_session",
    "DROP TABLE IF EXISTS conversation_history",
    "DROP INDEX IF EXISTS idx_sessions_user_thread_active",
    "DROP TABLE IF EXISTS sessions",
    "DROP TABLE IF EXISTS users",
)


def upgrade() -> None:
    """Apply every Phase-1 table + index, idempotently."""
    for stmt in _UPGRADE_STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    """Drop everything created by :func:`upgrade`, in reverse order."""
    for stmt in _DOWNGRADE_STATEMENTS:
        op.execute(stmt)

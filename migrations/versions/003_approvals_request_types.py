"""extend approvals.request_type CHECK to admit ``delegate_dangerous_sandbox``.

Revision ID: 003_approvals_request_types
Revises: 002_approvals
Create Date: 2026-05-11

Task #43 introduces a new approval ``request_type`` for Codex
delegations that request ``sandbox=danger-full-access`` per spec §5.4 /
§7.5 (operators must explicitly approve before Codex runs without
sandboxing). The §5.8 approval row CHECK constraint introduced by
migration 002 enumerated only the original three values
(``shell_exec``, ``delegate_out_of_root``, ``kill_delegation``); this
migration extends the set to include ``delegate_dangerous_sandbox``.

SQLite has no ``ALTER TABLE … DROP CHECK`` syntax, so we use the
canonical "rebuild table" pattern: rename the existing table out of the
way, recreate it with the new constraint, copy rows, re-create indexes
and drop the old table. All inside a single transaction (op.execute
inherits Alembic's per-migration transaction) so partial-apply leaves
no junk.

We MUST drop the indexes from the renamed table before creating them
on the rebuilt one — SQLite index names are global within a database.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# Alembic identifiers.
revision: str = "003_approvals_request_types"
down_revision: str | Sequence[str] | None = "002_approvals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Upgrade — rebuild with the extended CHECK.
# ---------------------------------------------------------------------------

_UP_DROP_INDEX_PENDING = """
DROP INDEX IF EXISTS idx_approvals_pending_sweep
"""
_UP_DROP_INDEX_PARENT = """
DROP INDEX IF EXISTS idx_approvals_parent_thread
"""
_UP_RENAME = """
ALTER TABLE approvals RENAME TO _approvals_old_002
"""
_UP_CREATE_NEW = """
CREATE TABLE approvals (
    approval_id       TEXT PRIMARY KEY,
    requested_at      TEXT NOT NULL,
    decided_at        TEXT,
    request_type      TEXT NOT NULL CHECK (
        request_type IN (
            'shell_exec',
            'delegate_out_of_root',
            'delegate_dangerous_sandbox',
            'kill_delegation'
        )
    ),
    request_payload   TEXT NOT NULL,
    requester_user_id TEXT,
    parent_thread_id  TEXT,
    status            TEXT NOT NULL CHECK (
        status IN ('pending','approved','denied','expired','cancelled')
    ),
    resolved_by       TEXT,
    reason            TEXT,
    workflow_id       TEXT
)
"""
_UP_COPY = """
INSERT INTO approvals (
    approval_id, requested_at, decided_at, request_type, request_payload,
    requester_user_id, parent_thread_id, status, resolved_by, reason,
    workflow_id
)
SELECT
    approval_id, requested_at, decided_at, request_type, request_payload,
    requester_user_id, parent_thread_id, status, resolved_by, reason,
    workflow_id
FROM _approvals_old_002
"""
_UP_DROP_OLD = """
DROP TABLE _approvals_old_002
"""
_UP_RECREATE_INDEX_PENDING = """
CREATE INDEX IF NOT EXISTS idx_approvals_pending_sweep
    ON approvals(status, requested_at)
"""
_UP_RECREATE_INDEX_PARENT = """
CREATE INDEX IF NOT EXISTS idx_approvals_parent_thread
    ON approvals(parent_thread_id, status)
"""

_UPGRADE_STATEMENTS: tuple[str, ...] = (
    _UP_DROP_INDEX_PENDING,
    _UP_DROP_INDEX_PARENT,
    _UP_RENAME,
    _UP_CREATE_NEW,
    _UP_COPY,
    _UP_DROP_OLD,
    _UP_RECREATE_INDEX_PENDING,
    _UP_RECREATE_INDEX_PARENT,
)


# ---------------------------------------------------------------------------
# Downgrade — rebuild with the original CHECK.
#
# Any rows already carrying ``delegate_dangerous_sandbox`` would violate
# the older CHECK; we filter them out (downgrade is a development-only
# operation and a row with a now-rejected type is an artifact of running
# the newer code path).
# ---------------------------------------------------------------------------

_DOWN_DROP_INDEX_PENDING = _UP_DROP_INDEX_PENDING
_DOWN_DROP_INDEX_PARENT = _UP_DROP_INDEX_PARENT
_DOWN_RENAME = """
ALTER TABLE approvals RENAME TO _approvals_new_003
"""
_DOWN_CREATE_OLD = """
CREATE TABLE approvals (
    approval_id       TEXT PRIMARY KEY,
    requested_at      TEXT NOT NULL,
    decided_at        TEXT,
    request_type      TEXT NOT NULL CHECK (
        request_type IN ('shell_exec','delegate_out_of_root','kill_delegation')
    ),
    request_payload   TEXT NOT NULL,
    requester_user_id TEXT,
    parent_thread_id  TEXT,
    status            TEXT NOT NULL CHECK (
        status IN ('pending','approved','denied','expired','cancelled')
    ),
    resolved_by       TEXT,
    reason            TEXT,
    workflow_id       TEXT
)
"""
_DOWN_COPY = """
INSERT INTO approvals (
    approval_id, requested_at, decided_at, request_type, request_payload,
    requester_user_id, parent_thread_id, status, resolved_by, reason,
    workflow_id
)
SELECT
    approval_id, requested_at, decided_at, request_type, request_payload,
    requester_user_id, parent_thread_id, status, resolved_by, reason,
    workflow_id
FROM _approvals_new_003
WHERE request_type <> 'delegate_dangerous_sandbox'
"""
_DOWN_DROP_NEW = """
DROP TABLE _approvals_new_003
"""
_DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    _DOWN_DROP_INDEX_PENDING,
    _DOWN_DROP_INDEX_PARENT,
    _DOWN_RENAME,
    _DOWN_CREATE_OLD,
    _DOWN_COPY,
    _DOWN_DROP_NEW,
    _UP_RECREATE_INDEX_PENDING,
    _UP_RECREATE_INDEX_PARENT,
)


def upgrade() -> None:
    """Rebuild ``approvals`` with the extended ``request_type`` CHECK."""
    for stmt in _UPGRADE_STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    """Rebuild ``approvals`` with the original ``request_type`` CHECK."""
    for stmt in _DOWNGRADE_STATEMENTS:
        op.execute(stmt)

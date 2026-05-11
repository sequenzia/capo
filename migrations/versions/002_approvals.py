"""add approvals table (Phase 4).

Revision ID: 002_approvals
Revises: 001_init
Create Date: 2026-05-11

Adds the ``approvals`` table backing the Phase 4 approval workflow (§5.8) —
non-allowlisted ``shell_exec``, out-of-``projects_root`` delegations, and
``kill_delegation`` all insert one row per request and ``DBOS.recv`` on the
``approval_id`` until the dispatcher routes the user's reply back via
``DBOS.send``.

Schema is the Phase-4-amended shape (per task #38 acceptance criteria +
spec §5.8 / §5.9 / §9.4), which is richer than the placeholder sketch in
§7.3 of the original Phase-1 spec dump:

- ``approval_id``        TEXT PK, UUIDv4 hex.
- ``requested_at``       TEXT NOT NULL, ISO 8601 UTC.
- ``decided_at``         TEXT NULL, ISO 8601 UTC.
- ``request_type``       TEXT NOT NULL — one of ``shell_exec``,
                          ``delegate_out_of_root``, ``kill_delegation``.
- ``request_payload``    TEXT NOT NULL — JSON blob with action-specific args.
- ``requester_user_id``  TEXT NULL — userspace identifier (NULL when an
                          operator/system-initiated approval has no user yet).
- ``parent_thread_id``   TEXT NULL — AMC channel (``amc:<channel_id>``) used
                          for inbound routing back to the awaiting workflow.
- ``status``             TEXT NOT NULL — ``pending``/``approved``/``denied``/
                          ``expired``/``cancelled``.
- ``resolved_by``        TEXT NULL — user/system who resolved the request.
- ``reason``             TEXT NULL — operator reason / denial reason / timeout
                          message.
- ``workflow_id``        TEXT NULL — DBOS workflow id awaiting this approval.

Indexes:

- ``idx_approvals_pending_sweep`` on ``(status, requested_at)`` — backs the
  cold-boot pending sweep (§5.6 boot-time recovery; per §5.8 expired-timeout
  reconciliation).
- ``idx_approvals_parent_thread`` on ``(parent_thread_id, status)`` — backs
  inbound dispatcher routing of ``/approve``/``/deny`` replies and the "most
  recent pending approval in the thread" lookup (§5.8 inbound contract).

The migration follows ``001_init`` conventions verbatim:

- Raw DDL via ``op.execute`` (no ORM metadata layer).
- ``IF NOT EXISTS`` on table + indexes so repeated ``alembic upgrade head``
  against the same DB is a no-op past the first apply.
- ``downgrade`` drops indexes ahead of the table, in reverse order, for a
  clean reversible round-trip.

No foreign key from ``approvals`` to ``delegations`` (per §5.8: any linkage
is encoded inside ``request_payload``).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# Alembic identifiers.
revision: str = "002_approvals"
down_revision: str | Sequence[str] | None = "001_init"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# DDL — kept verbatim against task #38 acceptance criteria + §5.8 / §5.9
# ---------------------------------------------------------------------------

_APPROVALS = """
CREATE TABLE IF NOT EXISTS approvals (
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

_IDX_APPROVALS_PENDING_SWEEP = """
CREATE INDEX IF NOT EXISTS idx_approvals_pending_sweep
    ON approvals(status, requested_at)
"""

_IDX_APPROVALS_PARENT_THREAD = """
CREATE INDEX IF NOT EXISTS idx_approvals_parent_thread
    ON approvals(parent_thread_id, status)
"""


# Order: table first, then indexes (which reference it).
_UPGRADE_STATEMENTS: tuple[str, ...] = (
    _APPROVALS,
    _IDX_APPROVALS_PENDING_SWEEP,
    _IDX_APPROVALS_PARENT_THREAD,
)

# Reverse order: drop indexes first, then the table.
_DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    "DROP INDEX IF EXISTS idx_approvals_parent_thread",
    "DROP INDEX IF EXISTS idx_approvals_pending_sweep",
    "DROP TABLE IF EXISTS approvals",
)


def upgrade() -> None:
    """Create the ``approvals`` table + supporting indexes, idempotently."""
    for stmt in _UPGRADE_STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    """Drop everything created by :func:`upgrade`, in reverse order."""
    for stmt in _DOWNGRADE_STATEMENTS:
        op.execute(stmt)

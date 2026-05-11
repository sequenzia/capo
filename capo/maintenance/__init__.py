"""Capo maintenance jobs (spec §8.1, §8.4).

Background jobs that run alongside the dispatcher / DBOS workflow engine:

* :mod:`capo.maintenance.retention` — nightly ``delegation_output`` pruning
  (Task #55).

Future maintenance jobs (compaction sweeps, cost-table roll-ups, etc.)
land here as additional submodules. The pattern is:

* Module exposes a top-level ``start_<job>_scheduler(settings) -> asyncio.Task``
  helper that wires an :func:`asyncio.create_task` daily timer.
* ``capo.main.amain`` calls each scheduler after dispatcher start and
  cancels them in the shutdown ``finally`` block.
* Pruning helpers themselves are pure functions: take a connection or
  ``db_path`` + ``now`` + parameters, return a count, raise on hard
  failure. Side effects are confined to SQL.
"""

from __future__ import annotations

from capo.maintenance.retention import (
    DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS,
    DEFAULT_RETENTION_RUN_HOUR_LOCAL,
    EVENT_RETENTION_COMPLETED,
    EVENT_RETENTION_FAILED,
    EVENT_RETENTION_STARTED,
    TERMINAL_STATUSES,
    prune_delegation_output,
    start_retention_scheduler,
)

__all__ = [
    "DEFAULT_DELEGATION_OUTPUT_RETENTION_DAYS",
    "DEFAULT_RETENTION_RUN_HOUR_LOCAL",
    "EVENT_RETENTION_COMPLETED",
    "EVENT_RETENTION_FAILED",
    "EVENT_RETENTION_STARTED",
    "TERMINAL_STATUSES",
    "prune_delegation_output",
    "start_retention_scheduler",
]

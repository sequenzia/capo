"""Capo memory subsystem: SQLite state store + conversation persistence.

Public surface (per spec §7.3):
- :func:`open_connection` — applies hardening pragmas at boot.
- :func:`begin_immediate_with_retry` — contextmanager wrapping `BEGIN IMMEDIATE`
  with the canonical retry helper contract.
- :func:`batched_insert` — high-throughput chunk writer with count/interval
  flush triggers.
- :class:`StoreUnavailable` — typed exception raised when the retry budget
  is exhausted.
"""

from capo.memory.conversation import (
    append_messages,
    append_messages_async,
    load_history,
    load_history_async,
    thread_id_for_amc,
)
from capo.memory.store import (
    StoreUnavailable,
    aopen_connection,
    batched_insert,
    begin_immediate_with_retry,
    open_connection,
)

__all__ = [
    "StoreUnavailable",
    "aopen_connection",
    "append_messages",
    "append_messages_async",
    "batched_insert",
    "begin_immediate_with_retry",
    "load_history",
    "load_history_async",
    "open_connection",
    "thread_id_for_amc",
]

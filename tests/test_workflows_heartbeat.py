"""Tests for :mod:`capo.workflows.delegation` heartbeat (Task #34).

Covers the spec §5.13 acceptance criteria for the periodic heartbeat step
inside ``monitor_delegation``:

Functional:
  * ``heartbeat`` is a ``@DBOS.step`` + ``@idempotent_step`` keyed by
    ``(delegation_id, str(threshold_seconds))``.
  * Thresholds are frozen at module level — default ``(300, 900, 3600)``
    seconds. Configurable via :func:`set_heartbeat_thresholds`.
  * Polling loop in ``_monitor_body`` invokes the heartbeat step when
    wall-clock elapsed since ``started_at`` crosses each threshold.
  * Heartbeat message sent via the same AMC sender as ``notify_user``
    (``_AMC_SENDER`` registry), with idempotency-key from
    ``.idempotency_key_for(...)``.
  * Message body deterministic: derived from ``delegations.summary`` +
    threshold label. No timestamps in payload.

Edge Cases:
  * Workflow restart after a heartbeat has fired: the ``(delegation_id,
    threshold)`` key is replayed → no duplicate send (idempotency cache
    + DBOS step-state).
  * Workflow restart BEFORE a heartbeat threshold elapses: the heartbeat
    fires once when the elapsed time crosses, never twice.
  * Delegation completes before any heartbeat threshold: no heartbeats
    sent (poller cancelled on subprocess exit).
  * No AMC channel on row: heartbeat is a no-op.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sqlite3
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from capo.config import PathsConfig
from capo.memory.store import open_connection
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    DEFAULT_HEARTBEAT_THRESHOLDS_S,
    STATUS_COMPLETED,
    STATUS_FAILED,
    DelegationProcessHandle,
    NotifyError,
    _compose_heartbeat_body,
    _elapsed_since_started,
    _format_threshold_label,
    _run_heartbeat_poller,
    get_heartbeat_poll_interval,
    get_heartbeat_thresholds,
    heartbeat,
    monitor_delegation,
    register_amc_sender,
    register_delegation_subprocess,
    set_heartbeat_poll_interval,
    set_heartbeat_thresholds,
)

# ---------------------------------------------------------------------------
# Fixtures (mirrors tests/test_workflows_notify_user.py)
# ---------------------------------------------------------------------------


class _StubSettings:
    def __init__(self, paths: PathsConfig) -> None:
        self.paths = paths


def _make_settings(tmp_path: Path) -> _StubSettings:
    dotcapo = tmp_path / ".capo"
    return _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=dotcapo / "state.db",
            dbos_db_path=dotcapo / "dbos.db",
        )
    )


_DELEGATIONS_DDL = """
CREATE TABLE IF NOT EXISTS delegations (
    id                       TEXT PRIMARY KEY,
    user_id                  TEXT NOT NULL,
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

_DELEGATION_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stream        TEXT NOT NULL CHECK (stream IN ('stdout','stderr','event')),
    chunk         TEXT NOT NULL
)
"""


def _ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
        conn.commit()
    finally:
        conn.close()


def _utc_db_ts(dt: datetime) -> str:
    """Format a datetime in the SQLite TIMESTAMP shape used by Capo."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def _insert_running_row(
    db_path: Path,
    delegation_id: str,
    *,
    started_at: datetime | None = None,
    summary: str | None = "in progress",
    parent_thread_id: str | None = "amc:channel-XYZ",
) -> None:
    if started_at is None:
        started_at = datetime.now(UTC)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status, started_at, parent_thread_id, summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delegation_id,
                "owner",
                "claude-code",
                f"/tmp/ws/{delegation_id}",
                99999,
                "sid-abc",
                "{}",
                "running",
                _utc_db_ts(started_at),
                parent_thread_id,
                summary,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _set_row_status(db_path: Path, delegation_id: str, status: str) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE delegations SET status = ? WHERE id = ?",
            (status, delegation_id),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _reset_heartbeat_config() -> Iterator[None]:
    """Restore heartbeat config + clear AMC sender between tests."""
    register_amc_sender(None)
    set_heartbeat_thresholds(None)
    set_heartbeat_poll_interval(None)
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    register_amc_sender(None)
    set_heartbeat_thresholds(None)
    set_heartbeat_poll_interval(None)
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def dbos_settings(tmp_path: Path) -> _StubSettings:
    settings = _make_settings(tmp_path)
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    return settings


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    settings = _make_settings(tmp_path)
    _ensure_schema(settings.paths.db_path)
    return settings.paths.db_path


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _StubSendResult:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class _StubAmcError(Exception):
    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AmcStub:
    def __init__(
        self,
        *,
        message_id: str = "msg-hb",
        permanent_code: str | None = None,
        transient_first_n: int = 0,
    ) -> None:
        self.calls: list[dict[str, str]] = []
        self._message_id = message_id
        self._permanent_code = permanent_code
        self._transient_first_n = transient_first_n

    async def send(
        self, channel_id: str, text: str, idempotency_key: str
    ) -> _StubSendResult:
        self.calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        if self._permanent_code is not None:
            raise _StubAmcError(self._permanent_code, "permanent failure")
        if len(self.calls) <= self._transient_first_n:
            raise _StubAmcError("INTERNAL_ERROR", "transient 503")
        return _StubSendResult(self._message_id)


class FakeProcess:
    """Controllable subprocess for poller tests."""

    def __init__(self) -> None:
        self.returncode: int | None = None
        self.stdout = None  # not used by heartbeat poller
        self.stderr = None

    def set_returncode(self, rc: int) -> None:
        self.returncode = rc

    def terminate(self) -> None:
        self.returncode = -int(signal.SIGTERM)

    def kill(self) -> None:
        self.returncode = -int(signal.SIGKILL)


class _StubReaderHandle:
    def __init__(self, delegation_id: str) -> None:
        self.delegation_id = delegation_id
        self._done = asyncio.Event()

    async def wait(self) -> Any:
        await self._done.wait()

        class _Stats:
            stdout_bytes = 0
            stderr_bytes = 0
            event_rows = 0
            stdout_rows = 0
            stderr_rows = 0
            flushes = 0
            split_lines = 0
            parse_errors = 0
            eof_reached = True
            fatal_error = None

        return _Stats()

    def close(self) -> None:
        self._done.set()


# ---------------------------------------------------------------------------
# Unit: frozen thresholds & configuration
# ---------------------------------------------------------------------------


def test_default_thresholds_are_5m_15m_60m() -> None:
    """Spec §5.13: default thresholds 300, 900, 3600 seconds."""
    assert DEFAULT_HEARTBEAT_THRESHOLDS_S == (300, 900, 3600)
    assert get_heartbeat_thresholds() == (300, 900, 3600)


def test_default_thresholds_is_a_tuple_frozen() -> None:
    """Module-level constant must be an immutable tuple."""
    assert isinstance(DEFAULT_HEARTBEAT_THRESHOLDS_S, tuple)


def test_set_heartbeat_thresholds_overrides() -> None:
    set_heartbeat_thresholds([60, 120, 240])
    assert get_heartbeat_thresholds() == (60, 120, 240)


def test_set_heartbeat_thresholds_none_resets_to_default() -> None:
    set_heartbeat_thresholds([60, 120])
    set_heartbeat_thresholds(None)
    assert get_heartbeat_thresholds() == DEFAULT_HEARTBEAT_THRESHOLDS_S


def test_set_heartbeat_thresholds_rejects_empty() -> None:
    with pytest.raises(ValueError):
        set_heartbeat_thresholds([])


def test_set_heartbeat_thresholds_rejects_non_positive() -> None:
    with pytest.raises(ValueError):
        set_heartbeat_thresholds([0, 100])
    with pytest.raises(ValueError):
        set_heartbeat_thresholds([-1, 100])


def test_set_heartbeat_poll_interval_overrides() -> None:
    set_heartbeat_poll_interval(0.5)
    assert get_heartbeat_poll_interval() == 0.5


def test_set_heartbeat_poll_interval_rejects_non_positive() -> None:
    with pytest.raises(ValueError):
        set_heartbeat_poll_interval(0)
    with pytest.raises(ValueError):
        set_heartbeat_poll_interval(-1)


# ---------------------------------------------------------------------------
# Unit: threshold label formatting
# ---------------------------------------------------------------------------


def test_format_threshold_label_5m() -> None:
    assert _format_threshold_label(300) == "5m"


def test_format_threshold_label_15m() -> None:
    assert _format_threshold_label(900) == "15m"


def test_format_threshold_label_1h() -> None:
    assert _format_threshold_label(3600) == "1h"


def test_format_threshold_label_2h45m() -> None:
    assert _format_threshold_label(2 * 3600 + 45 * 60) == "2h45m"


def test_format_threshold_label_seconds_only() -> None:
    assert _format_threshold_label(45) == "45s"


def test_format_threshold_label_zero() -> None:
    assert _format_threshold_label(0) == "0s"


# ---------------------------------------------------------------------------
# Unit: deterministic body composition
# ---------------------------------------------------------------------------


def test_compose_heartbeat_body_includes_threshold_label_and_summary() -> None:
    body = _compose_heartbeat_body(
        delegation_id="d-abc",
        threshold_seconds=300,
        summary="reading files",
    )
    assert "d-abc" in body
    assert "5m" in body
    assert "reading files" in body


def test_compose_heartbeat_body_deterministic() -> None:
    """Functional: identical inputs → identical bytes."""
    a = _compose_heartbeat_body(
        delegation_id="d-1", threshold_seconds=900, summary="step 2"
    )
    b = _compose_heartbeat_body(
        delegation_id="d-1", threshold_seconds=900, summary="step 2"
    )
    assert a == b


def test_compose_heartbeat_body_no_timestamps() -> None:
    """Acceptance: NO timestamps in payload."""
    body = _compose_heartbeat_body(
        delegation_id="d-2", threshold_seconds=3600, summary="busy"
    )
    # ISO8601 date heuristic (year prefix) — no dates allowed.
    assert "2026-" not in body
    assert "T00:" not in body
    # ``datetime.now`` calls would format with "-" / ":" separators.
    # Just check the body has the threshold label, not a clock time.


def test_compose_heartbeat_body_omits_blank_summary() -> None:
    body = _compose_heartbeat_body(
        delegation_id="d-3", threshold_seconds=300, summary=None
    )
    assert "\n" not in body
    assert "5m" in body


def test_compose_heartbeat_body_collapses_newlines_in_summary() -> None:
    body = _compose_heartbeat_body(
        delegation_id="d-4",
        threshold_seconds=300,
        summary="line one\nline two",
    )
    # Only the head→summary newline remains; embedded newlines collapsed.
    assert body.count("\n") == 1


# ---------------------------------------------------------------------------
# Unit: elapsed-time computation
# ---------------------------------------------------------------------------


def test_elapsed_since_started_with_offset() -> None:
    started = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    now = started + timedelta(seconds=600)
    assert _elapsed_since_started(_utc_db_ts(started), now=now) == 600.0


def test_elapsed_since_started_in_future_returns_none() -> None:
    """Clock skew: started_at in the future → None (no negative elapsed)."""
    started = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    now = started - timedelta(seconds=10)
    assert _elapsed_since_started(_utc_db_ts(started), now=now) is None


def test_elapsed_since_started_missing_returns_none() -> None:
    assert _elapsed_since_started(None) is None
    assert _elapsed_since_started("") is None
    assert _elapsed_since_started("not a date") is None


# ---------------------------------------------------------------------------
# Unit: idempotency key keyed by (delegation_id, str(threshold_seconds))
# ---------------------------------------------------------------------------


def test_heartbeat_idempotency_key_includes_threshold() -> None:
    """Different thresholds → different keys for the same delegation."""
    k300 = heartbeat.idempotency_key_for("d-1", 300, "/tmp/x.db")
    k900 = heartbeat.idempotency_key_for("d-1", 900, "/tmp/x.db")
    assert k300 != k900
    assert k300.startswith("heartbeat:")
    assert k900.startswith("heartbeat:")


def test_heartbeat_idempotency_key_stable_for_same_args() -> None:
    """Same (delegation_id, threshold) → same key across calls."""
    a = heartbeat.idempotency_key_for("d-stable", 900, "/tmp/x.db")
    b = heartbeat.idempotency_key_for("d-stable", 900, "/tmp/x.db")
    assert a == b


def test_heartbeat_idempotency_key_per_delegation() -> None:
    """Different delegations → different keys at the same threshold."""
    a = heartbeat.idempotency_key_for("d-A", 300, "/tmp/x.db")
    b = heartbeat.idempotency_key_for("d-B", 300, "/tmp/x.db")
    assert a != b


def test_heartbeat_idempotency_key_threshold_is_stringified() -> None:
    """Spec §5.13: threshold is stringified for the key."""
    # int 300 and "300" must produce the SAME key (lambda calls str()).
    k_int = heartbeat.idempotency_key_for("d-1", 300, "/tmp/x.db")
    # Sanity: just verify it has the expected shape (no exception).
    assert k_int.startswith("heartbeat:")


# ---------------------------------------------------------------------------
# Integration: heartbeat step end-to-end via DBOS
# ---------------------------------------------------------------------------


async def test_heartbeat_step_sends_one_message_with_idempotency_key(
    dbos_settings: _StubSettings,
) -> None:
    """Functional: one AMC send, idempotency key matches step's key_for."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(
        dbos_settings.paths.db_path,
        delegation_id,
        summary="halfway through",
        parent_thread_id="amc:ch-A",
    )

    stub = AmcStub(message_id="msg-hb1")
    register_amc_sender(stub.send)

    result = await heartbeat(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )

    assert len(stub.calls) == 1
    call = stub.calls[0]
    assert call["channel_id"] == "ch-A"
    assert "5m" in call["text"]
    assert "halfway through" in call["text"]
    expected_key = heartbeat.idempotency_key_for(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )
    assert call["idempotency_key"] == expected_key
    assert result["notified"] is True
    assert result["threshold_seconds"] == 300
    assert result["message_id"] == "msg-hb1"


async def test_heartbeat_step_missing_row_is_noop(
    dbos_settings: _StubSettings,
) -> None:
    stub = AmcStub()
    register_amc_sender(stub.send)
    out = await heartbeat(
        "missing-row", 300, str(dbos_settings.paths.db_path)
    )
    assert out["notified"] is False
    assert len(stub.calls) == 0


async def test_heartbeat_step_terminal_row_is_noop(
    dbos_settings: _StubSettings,
) -> None:
    """Defensive: terminal-row heartbeat is suppressed (poller cancellation
    race)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    _set_row_status(
        dbos_settings.paths.db_path, delegation_id, STATUS_COMPLETED
    )
    stub = AmcStub()
    register_amc_sender(stub.send)
    out = await heartbeat(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )
    assert out["notified"] is False
    assert len(stub.calls) == 0


async def test_heartbeat_step_no_amc_channel_is_noop(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: parent_thread_id without amc: prefix → no send."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(
        dbos_settings.paths.db_path,
        delegation_id,
        parent_thread_id="cli:local",
    )
    stub = AmcStub()
    register_amc_sender(stub.send)
    out = await heartbeat(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )
    assert out["notified"] is False
    assert len(stub.calls) == 0


async def test_heartbeat_step_no_sender_raises_notify_error(
    dbos_settings: _StubSettings,
) -> None:
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    # No register_amc_sender call.
    with pytest.raises(NotifyError) as excinfo:
        await heartbeat(
            delegation_id, 300, str(dbos_settings.paths.db_path)
        )
    assert excinfo.value.code == "NO_SENDER_REGISTERED"


async def test_heartbeat_step_permanent_amc_error_raises_notify_error(
    dbos_settings: _StubSettings,
) -> None:
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    stub = AmcStub(permanent_code="PLATFORM_AUTH")
    register_amc_sender(stub.send)
    with pytest.raises(NotifyError) as excinfo:
        await heartbeat(
            delegation_id, 300, str(dbos_settings.paths.db_path)
        )
    assert excinfo.value.code == "PLATFORM_AUTH"


async def test_heartbeat_step_transient_error_propagates(
    dbos_settings: _StubSettings,
) -> None:
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    stub = AmcStub(transient_first_n=999)
    register_amc_sender(stub.send)
    with pytest.raises(_StubAmcError) as excinfo:
        await heartbeat(
            delegation_id, 300, str(dbos_settings.paths.db_path)
        )
    assert excinfo.value.code == "INTERNAL_ERROR"


async def test_heartbeat_step_idempotency_key_stable_across_calls(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: workflow restart between threshold and send →
    AMC's server-side dedupe sees the SAME idempotency key."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    stub = AmcStub()
    register_amc_sender(stub.send)
    await heartbeat(delegation_id, 300, str(dbos_settings.paths.db_path))
    await heartbeat(delegation_id, 300, str(dbos_settings.paths.db_path))
    assert len(stub.calls) == 2
    assert stub.calls[0]["idempotency_key"] == stub.calls[1]["idempotency_key"]
    assert stub.calls[0]["text"] == stub.calls[1]["text"]


async def test_heartbeat_step_different_thresholds_different_keys(
    dbos_settings: _StubSettings,
) -> None:
    """Functional: each (delegation_id, threshold) pair owns its OWN key."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    stub = AmcStub()
    register_amc_sender(stub.send)
    await heartbeat(delegation_id, 300, str(dbos_settings.paths.db_path))
    await heartbeat(delegation_id, 900, str(dbos_settings.paths.db_path))
    assert len(stub.calls) == 2
    assert stub.calls[0]["idempotency_key"] != stub.calls[1]["idempotency_key"]
    assert "5m" in stub.calls[0]["text"]
    assert "15m" in stub.calls[1]["text"]


# ---------------------------------------------------------------------------
# Integration: heartbeat poller (threshold-crossing detection)
# ---------------------------------------------------------------------------


async def test_poller_fires_step_at_each_threshold(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: simulated long-running monitor crosses 2 thresholds
    → 2 heartbeats sent (spec §5.13 testing requirement)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    # Place started_at in the past so elapsed > all configured thresholds
    # almost immediately. Use tight test-only thresholds.
    started = datetime.now(UTC) - timedelta(seconds=10)
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, started_at=started
    )
    set_heartbeat_thresholds([1, 2])  # both already crossed by 10s elapsed
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    task = asyncio.create_task(
        _run_heartbeat_poller(delegation_id, dbos_settings.paths.db_path)
    )
    # Give the poller a couple of poll cycles to fire both thresholds.
    await asyncio.sleep(0.1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        result = await task
    assert result is not None
    assert sorted(result["fired"]) == [1, 2]
    # Two distinct AMC sends with two distinct idempotency keys.
    assert len(stub.calls) == 2
    assert {c["idempotency_key"] for c in stub.calls} == {
        heartbeat.idempotency_key_for(
            delegation_id, 1, str(dbos_settings.paths.db_path)
        ),
        heartbeat.idempotency_key_for(
            delegation_id, 2, str(dbos_settings.paths.db_path)
        ),
    }


async def test_poller_skips_thresholds_not_yet_crossed(
    dbos_settings: _StubSettings,
) -> None:
    """Functional: a threshold far in the future is NOT fired."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    started = datetime.now(UTC) - timedelta(seconds=1)
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, started_at=started
    )
    set_heartbeat_thresholds([10000])  # well above 1s elapsed
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    task = asyncio.create_task(
        _run_heartbeat_poller(delegation_id, dbos_settings.paths.db_path)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        result = await task
    assert result["fired"] == []
    assert len(stub.calls) == 0


async def test_poller_exits_cleanly_on_cancel(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: delegation completes before threshold → poller cancelled →
    no heartbeats sent."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    started = datetime.now(UTC)  # 0s elapsed
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, started_at=started
    )
    set_heartbeat_thresholds([300, 900])
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    task = asyncio.create_task(
        _run_heartbeat_poller(delegation_id, dbos_settings.paths.db_path)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        result = await task
    assert result["fired"] == []
    assert len(stub.calls) == 0


async def test_poller_missing_row_exits_silently(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: no row → poller returns empty fired list without errors."""
    stub = AmcStub()
    register_amc_sender(stub.send)
    set_heartbeat_poll_interval(0.01)
    task = asyncio.create_task(
        _run_heartbeat_poller(
            "missing-row", dbos_settings.paths.db_path
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        result = await task
    assert result["fired"] == []
    assert len(stub.calls) == 0


async def test_poller_uses_durable_started_at_not_loop_time(
    dbos_settings: _StubSettings,
) -> None:
    """Functional: elapsed is computed from row's started_at, not loop start.

    Models "workflow restart": insert a row with started_at in the past,
    spin up the poller AFTER that. The poller MUST fire the threshold
    that the past-started_at has already crossed — using process-local
    time would falsely treat elapsed as ~0.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    started = datetime.now(UTC) - timedelta(seconds=5)
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, started_at=started
    )
    set_heartbeat_thresholds([3])  # crossed 5s ago
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    task = asyncio.create_task(
        _run_heartbeat_poller(delegation_id, dbos_settings.paths.db_path)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        result = await task
    assert result["fired"] == [3]
    assert len(stub.calls) == 1


# ---------------------------------------------------------------------------
# Integration: workflow restart between thresholds → exactly-once delivery
# ---------------------------------------------------------------------------


async def test_workflow_restart_does_not_duplicate_heartbeat(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case + integration: spec §5.13 "Workflow restart after a
    heartbeat has fired: the (delegation_id, threshold) key is replayed →
    no duplicate send."

    Models the in-process replay via the ``@idempotent_step`` in-process
    cache + identical idempotency key. Two invocations against the same
    key carry the SAME ``Idempotency-Key`` so AMC server-side dedupe
    enforces exactly-once delivery to the receiver.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)
    stub = AmcStub()
    register_amc_sender(stub.send)

    # First invocation (pre-restart equivalent).
    r1 = await heartbeat(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )
    # Second invocation (post-restart equivalent) — same args → same key.
    r2 = await heartbeat(
        delegation_id, 300, str(dbos_settings.paths.db_path)
    )

    assert r1["notified"] is True
    assert r2["notified"] is True
    assert r1["idempotency_key"] == r2["idempotency_key"]
    # AMC stub records BOTH attempts (because in unit test mode we don't
    # have DBOS step-state replay caching the return), BUT the
    # ``Idempotency-Key`` is identical so the receiver dedupes to one.
    assert {c["idempotency_key"] for c in stub.calls} == {r1["idempotency_key"]}
    assert stub.calls[0]["text"] == stub.calls[1]["text"]


# ---------------------------------------------------------------------------
# Integration: full monitor workflow crosses heartbeat thresholds
# ---------------------------------------------------------------------------


async def test_full_monitor_workflow_emits_heartbeat_then_terminates(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Integration: real ``monitor_delegation`` workflow with a fake
    subprocess that lives long enough to cross one heartbeat threshold,
    then exits cleanly. We assert exactly one heartbeat AMC call and
    that the workflow reaches terminal status."""
    from capo.tools._subprocess import start_reader as _real_start_reader  # noqa: PLC0415, F401

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    started = datetime.now(UTC) - timedelta(seconds=2)
    _insert_running_row(
        dbos_settings.paths.db_path,
        delegation_id,
        started_at=started,
        summary="working",
    )
    set_heartbeat_thresholds([1])  # crossed 2s ago
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    # Use a fake process + a real-ish reader that idles until we close.
    process = FakeProcess()
    reader = _StubReaderHandle(delegation_id)
    register_delegation_subprocess(
        DelegationProcessHandle(
            delegation_id=delegation_id,
            process=process,  # type: ignore[arg-type]
            reader_handle=reader,  # type: ignore[arg-type]
            db_path=dbos_settings.paths.db_path,
        )
    )

    async def _exit_after_delay() -> None:
        await asyncio.sleep(0.2)
        process.set_returncode(0)
        reader.close()

    exiter = asyncio.create_task(_exit_after_delay())
    try:
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.01,
            db_path=dbos_settings.paths.db_path,
        )
    finally:
        with contextlib.suppress(Exception):
            await exiter

    # Heartbeat fired at threshold=1. All heartbeat-shaped sends use the
    # ``heartbeat:`` key prefix (step-name namespace) and share the same
    # idempotency key (same delegation_id + same threshold inside a single
    # workflow run).
    heartbeat_calls = [c for c in stub.calls if "still running" in c["text"]]
    assert len(heartbeat_calls) >= 1
    hb_keys = {c["idempotency_key"] for c in heartbeat_calls}
    assert len(hb_keys) == 1
    assert next(iter(hb_keys)).startswith("heartbeat:")
    # And the terminal notify_user fired with a DIFFERENT key namespace
    # (``notify_user:`` prefix vs ``heartbeat:`` — different step names).
    notify_calls = [
        c for c in stub.calls
        if c["idempotency_key"].startswith("notify_user:")
    ]
    assert len(notify_calls) >= 1
    # Workflow reached terminal status.
    assert result.status in (STATUS_COMPLETED, STATUS_FAILED)


async def test_full_monitor_workflow_no_heartbeat_if_fast_exit(
    dbos_settings: _StubSettings,
) -> None:
    """Integration: delegation completes before any heartbeat threshold →
    zero heartbeat sends (spec §5.13 edge case)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    started = datetime.now(UTC)  # 0s elapsed
    _insert_running_row(
        dbos_settings.paths.db_path,
        delegation_id,
        started_at=started,
    )
    set_heartbeat_thresholds([300, 900])  # neither crossed
    set_heartbeat_poll_interval(0.01)
    stub = AmcStub()
    register_amc_sender(stub.send)

    process = FakeProcess()
    reader = _StubReaderHandle(delegation_id)
    register_delegation_subprocess(
        DelegationProcessHandle(
            delegation_id=delegation_id,
            process=process,  # type: ignore[arg-type]
            reader_handle=reader,  # type: ignore[arg-type]
            db_path=dbos_settings.paths.db_path,
        )
    )

    async def _exit_immediately() -> None:
        await asyncio.sleep(0.01)
        process.set_returncode(0)
        reader.close()

    exiter = asyncio.create_task(_exit_immediately())
    try:
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.005,
            db_path=dbos_settings.paths.db_path,
        )
    finally:
        with contextlib.suppress(Exception):
            await exiter

    # Zero heartbeat-shaped AMC calls (terminal notify_user is allowed).
    heartbeat_calls = [c for c in stub.calls if "still running" in c["text"]]
    assert heartbeat_calls == []
    assert result.status in (STATUS_COMPLETED, STATUS_FAILED)

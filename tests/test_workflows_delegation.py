"""Tests for :mod:`capo.workflows.delegation` (Task #30 — monitor_delegation).

Covers the spec §5.6 acceptance criteria for the DBOS-managed monitor
workflow:

Functional:
  * ``@DBOS.workflow()`` with ``delegation_id`` as the deterministic input.
  * Continuous output drain step persists chunks via the batched-insert
    buffer (re-uses the Task #21 reader).
  * Returncode polling at a configurable interval (default 1s).
  * Status transitions: running → completed (rc 0) / failed (non-zero or
    exception) / killed (SIGTERM/SIGKILL signaled by user).
  * All ``@DBOS.step`` calls with external side effects use the
    :func:`idempotent_step` decorator (Task #29).

Edge Cases:
  * Subprocess crashes between two polling cycles: transition to ``failed``
    with reason from stderr / exit code.
  * Workflow re-entered while subprocess is still running: do NOT re-spawn;
    look up existing handle via the registry.

Error Handling:
  * Workflow exception surfaced via Logfire span (best-effort) and reflected
  on the row as ``failed``.

Performance:
  * Subprocess output ingest: zero loss + zero pipe blocking on ≥10 MB
    stdout (re-verifies §6.1 within DBOS context).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sqlite3
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from capo.config import PathsConfig
from capo.memory.store import open_connection
from capo.tools._subprocess import start_reader
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    DEFAULT_POLL_INTERVAL_S,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_KILLED,
    DelegationProcessHandle,
    MonitorResult,
    _classify_returncode,
    monitor_delegation,
    register_delegation_subprocess,
    unregister_delegation_subprocess,
)

# ---------------------------------------------------------------------------
# Fixtures
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


def _insert_running_row(
    db_path: Path, delegation_id: str, *, pid: int | None = 99999
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delegation_id,
                "owner",
                "claude-code",
                f"/tmp/ws/{delegation_id}",
                pid,
                None,
                "{}",
                "running",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _read_row(db_path: Path, delegation_id: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, ended_at, summary FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state() -> Iterator[None]:
    """DBOS is a process-level singleton; teardown between tests."""
    # Also clear the in-process registry so tests don't leak handles.
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def dbos_settings(tmp_path: Path) -> _StubSettings:
    """Init + launch DBOS for tests that exercise the real workflow."""
    settings = _make_settings(tmp_path)
    # Ensure the state.db schema is in place — the persistence step
    # writes there via the retry helper.
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    return settings


# ---------------------------------------------------------------------------
# Test doubles: a controllable fake subprocess + a tiny FakeStream
# ---------------------------------------------------------------------------


class FakeStream:
    """Minimal asyncio.StreamReader stand-in for tests."""

    def __init__(self, payload: bytes = b"") -> None:
        self._buf = bytearray(payload)
        self._eof_event = asyncio.Event()

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    def close(self) -> None:
        self._eof_event.set()

    async def readline(self) -> bytes:
        # Wait for data OR for EOF.
        while True:
            if self._buf:
                idx = self._buf.find(b"\n")
                if idx == -1:
                    if self._eof_event.is_set():
                        out = bytes(self._buf)
                        self._buf.clear()
                        return out
                    # Wait for either more data or EOF.
                    await asyncio.sleep(0.005)
                    continue
                out = bytes(self._buf[: idx + 1])
                del self._buf[: idx + 1]
                return out
            if self._eof_event.is_set():
                return b""
            await asyncio.sleep(0.005)

    async def read(self, n: int) -> bytes:
        await asyncio.sleep(0.005)
        if not self._buf:
            if self._eof_event.is_set():
                return b""
            await asyncio.sleep(0.005)
            return b""
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


class FakeProcess:
    """A controllable fake subprocess for monitor_delegation tests.

    Drives the returncode poll loop and the reader without spawning a real
    child. The test calls :meth:`set_returncode` to simulate exit, and
    :meth:`close_streams` to let the reader hit EOF.
    """

    def __init__(self) -> None:
        self.stdout = FakeStream()
        self.stderr = FakeStream()
        self.returncode: int | None = None

    def set_returncode(self, rc: int) -> None:
        self.returncode = rc

    def close_streams(self) -> None:
        self.stdout.close()
        self.stderr.close()

    def terminate(self) -> None:
        self.returncode = -int(signal.SIGTERM)
        self.close_streams()

    def kill(self) -> None:
        self.returncode = -int(signal.SIGKILL)
        self.close_streams()

    async def wait(self) -> int:
        while self.returncode is None:
            await asyncio.sleep(0.005)
        return self.returncode


# ---------------------------------------------------------------------------
# Unit: _classify_returncode
# ---------------------------------------------------------------------------


def test_classify_zero_returncode_is_completed() -> None:
    status, summary = _classify_returncode(0, None)
    assert status == STATUS_COMPLETED
    assert summary is None


def test_classify_positive_nonzero_is_failed() -> None:
    status, summary = _classify_returncode(2, None)
    assert status == STATUS_FAILED
    assert "exit code 2" in (summary or "")


def test_classify_positive_with_stderr_tail_includes_tail() -> None:
    status, summary = _classify_returncode(1, "boom: NPE")
    assert status == STATUS_FAILED
    assert summary is not None
    assert "exit code 1" in summary
    assert "boom: NPE" in summary


def test_classify_sigterm_is_killed() -> None:
    status, summary = _classify_returncode(-int(signal.SIGTERM), None)
    assert status == STATUS_KILLED
    assert summary is not None
    assert "SIGTERM" in summary


def test_classify_sigkill_is_killed() -> None:
    status, summary = _classify_returncode(-int(signal.SIGKILL), None)
    assert status == STATUS_KILLED
    assert summary is not None
    assert "SIGKILL" in summary


def test_classify_segv_is_failed_not_killed() -> None:
    """SIGSEGV is a crash, not a user-initiated kill — surface as ``failed``."""
    status, summary = _classify_returncode(-int(signal.SIGSEGV), None)
    assert status == STATUS_FAILED
    assert summary is not None
    assert "SIGSEGV" in summary
    assert "crashed" in summary


# ---------------------------------------------------------------------------
# Unit: registry
# ---------------------------------------------------------------------------


def test_register_and_unregister(tmp_path: Path) -> None:
    handle = _make_handle("d-1", tmp_path)
    register_delegation_subprocess(handle)
    assert _DELEGATION_REGISTRY["d-1"] is handle
    unregister_delegation_subprocess("d-1")
    assert "d-1" not in _DELEGATION_REGISTRY


class _StubReaderHandle:
    """Minimal reader-handle stub for registry-only tests."""

    def __init__(self, delegation_id: str) -> None:
        self.delegation_id = delegation_id

    async def wait(self) -> Any:
        return None


def _make_handle(delegation_id: str, tmp_path: Path) -> DelegationProcessHandle:
    """Construct a minimal handle (no real reader) for registry tests."""
    return DelegationProcessHandle(
        delegation_id=delegation_id,
        process=FakeProcess(),
        reader_handle=_StubReaderHandle(delegation_id),  # type: ignore[arg-type]
        db_path=tmp_path / "state.db",
    )


# ---------------------------------------------------------------------------
# Workflow: clean completion (rc=0 → completed)
# ---------------------------------------------------------------------------


async def test_monitor_workflow_completes_on_rc_zero(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """End-to-end: spawn → drain → exit(0) → status='completed'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    process = FakeProcess()
    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        # Feed some stdout, then exit cleanly.
        process.stdout.feed(b"hello world\n")
        process.stdout.feed(b'{"event":"ok","session_id":"sid-1"}\n')

        async def _close_after_delay() -> None:
            await asyncio.sleep(0.05)
            process.set_returncode(0)
            process.close_streams()

        closer = asyncio.create_task(_close_after_delay())
        try:
            result = await monitor_delegation(
                delegation_id, poll_interval_s=0.010
            )
        finally:
            await closer

        assert isinstance(result, MonitorResult)
        assert result.status == STATUS_COMPLETED
        assert result.returncode == 0
    finally:
        conn.close()

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_COMPLETED
    assert row["ended_at"] is not None


# ---------------------------------------------------------------------------
# Workflow: non-zero returncode → failed
# ---------------------------------------------------------------------------


async def test_monitor_workflow_marks_failed_on_nonzero_returncode(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Subprocess crashes between two polling cycles: failed with exit code reason."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    process = FakeProcess()
    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        async def _close_after_delay() -> None:
            await asyncio.sleep(0.05)
            # Simulate a non-zero exit (between two polling cycles).
            process.set_returncode(2)
            process.close_streams()

        closer = asyncio.create_task(_close_after_delay())
        try:
            result = await monitor_delegation(
                delegation_id, poll_interval_s=0.010
            )
        finally:
            await closer

        assert result.status == STATUS_FAILED
        assert result.returncode == 2
    finally:
        conn.close()

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    # Task #32 (summarize_run) replaces the classifier-derived "exit code N"
    # summary with a deterministic one-liner. The classifier's heuristic
    # is still surfaced via the Logfire span; the row column now reflects
    # summarize_run's deterministic format.
    assert (row["summary"] or "").startswith("status=failed ")


# ---------------------------------------------------------------------------
# Workflow: SIGTERM/SIGKILL → killed
# ---------------------------------------------------------------------------


async def test_monitor_workflow_marks_killed_on_sigterm(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Subprocess receives SIGTERM → row transitions to 'killed'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    process = FakeProcess()
    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        async def _signal_after_delay() -> None:
            await asyncio.sleep(0.05)
            # Simulate the kill_delegation path: signal the process.
            process.set_returncode(-int(signal.SIGTERM))
            process.close_streams()

        signaller = asyncio.create_task(_signal_after_delay())
        try:
            result = await monitor_delegation(
                delegation_id, poll_interval_s=0.010
            )
        finally:
            await signaller

        assert result.status == STATUS_KILLED
    finally:
        conn.close()

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_KILLED


# ---------------------------------------------------------------------------
# Workflow: re-entry without subprocess (registry empty) → failed (Task #35 hook)
# ---------------------------------------------------------------------------


async def test_monitor_workflow_failed_when_subprocess_missing(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Workflow re-entered after a restart (no subprocess in registry).

    Edge case from spec §5.6: "do NOT re-spawn; check existing PID first".
    Task #30 narrowly surfaces this as 'failed' — Task #35 will replace
    with the resume-spawn step.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    # Deliberately DO NOT register; the monitor must not re-spawn.
    result = await monitor_delegation(delegation_id, poll_interval_s=0.010)
    assert result.status == STATUS_FAILED
    assert result.returncode is None
    # The row should NOT be updated by Task #30 in this branch — the
    # monitor returns the failure but doesn't try to write a row when
    # it doesn't even have a db_path to write to. (Task #31 / #35 own
    # this branch's row update.)


# ---------------------------------------------------------------------------
# Workflow: out-of-band kill — row already 'killed' is honored
# ---------------------------------------------------------------------------


async def test_monitor_honors_out_of_band_kill(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """If the row is 'killed' before the monitor's terminal write, leave it.

    Simulates kill_delegation racing the monitor: the killer writes the
    row to 'killed' during the poll loop; the monitor must observe that
    and NOT overwrite with 'failed'.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    process = FakeProcess()
    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        # Out-of-band: while the monitor is polling, write 'killed' to the row.
        async def _kill_row() -> None:
            await asyncio.sleep(0.030)
            killer = open_connection(dbos_settings.paths.db_path)
            try:
                killer.execute(
                    "UPDATE delegations SET status='killed', ended_at=?, "
                    "summary='killed by test' WHERE id=?",
                    ("2026-05-10 00:00:00.000000", delegation_id),
                )
                killer.commit()
            finally:
                killer.close()
            await asyncio.sleep(0.020)
            # Then let the subprocess exit normally (rc=0).
            process.set_returncode(0)
            process.close_streams()

        killer_task = asyncio.create_task(_kill_row())
        try:
            await monitor_delegation(delegation_id, poll_interval_s=0.010)
        finally:
            await killer_task
    finally:
        conn.close()

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    # The row should still be 'killed', not 'completed'.
    assert row["status"] == STATUS_KILLED


# ---------------------------------------------------------------------------
# Performance: ≥10 MB stdout, no loss, no pipe block — inside DBOS context
# ---------------------------------------------------------------------------


async def test_monitor_perf_10mb_stdout_no_loss_no_block(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """§6.1 re-verified inside DBOS: ≥10 MB stdout flows through the workflow
    with zero loss + zero pipe blocking.

    Uses a real Python child piping >10 MB so we exercise the real pipe-
    buffer/StreamReader interaction the §6.1 contract covers.
    """
    line_size = 250 * 1024  # 250 KiB per line
    line_count = 50
    target_bytes = (line_size + 1) * line_count  # +1 per line for '\n'
    assert target_bytes > 10 * 1024 * 1024  # >10 MiB

    script = (
        "import sys\n"
        f"line = 'x' * {line_size} + '\\n'\n"
        f"for _ in range({line_count}):\n"
        "    sys.stdout.write(line)\n"
        "sys.stdout.flush()\n"
    )

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2 * 1024 * 1024,
    )

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, pid=process.pid
    )

    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        # If the reader ever blocks the pipe, the child stalls and this
        # times out — that's the failure mode we're guarding.
        result = await asyncio.wait_for(
            monitor_delegation(delegation_id, poll_interval_s=0.020),
            timeout=60.0,
        )
        assert result.status == STATUS_COMPLETED
        assert result.returncode == 0

        # Verify all bytes captured by counting persisted bytes.
        cur = conn.execute(
            "SELECT SUM(LENGTH(chunk)) FROM delegation_output WHERE delegation_id=?",
            (delegation_id,),
        )
        total_persisted = cur.fetchone()[0] or 0
        assert total_persisted == target_bytes, (
            f"persisted={total_persisted} target={target_bytes}"
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stress: chatty subprocess (many small JSON events)
# ---------------------------------------------------------------------------


async def test_monitor_stress_chatty_json_events(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Stress integration: a chatty subprocess emitting many JSON events.

    Mirrors the CC stream-json case (lots of small events). Verifies the
    drain step keeps up and the workflow returns 'completed' with all
    events persisted.
    """
    event_count = 500
    script = (
        "import sys, json\n"
        f"for i in range({event_count}):\n"
        "    sys.stdout.write(json.dumps({'session_id': 'sid-test', 'i': i}) + '\\n')\n"
        "sys.stdout.flush()\n"
    )

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2 * 1024 * 1024,
    )

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, pid=process.pid
    )

    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        result = await asyncio.wait_for(
            monitor_delegation(delegation_id, poll_interval_s=0.020),
            timeout=30.0,
        )
        assert result.status == STATUS_COMPLETED

        cur = conn.execute(
            "SELECT COUNT(*) FROM delegation_output WHERE delegation_id=? AND stream='event'",
            (delegation_id,),
        )
        event_rows = cur.fetchone()[0]
        assert event_rows == event_count
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Real subprocess: kill via signal → status='killed'
# ---------------------------------------------------------------------------


async def test_monitor_real_subprocess_sigterm_marks_killed(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Spawn a real child that runs forever, SIGTERM it mid-flight, verify killed."""
    script = (
        "import time, sys\n"
        "while True:\n"
        "    sys.stdout.write('tick\\n')\n"
        "    sys.stdout.flush()\n"
        "    time.sleep(0.05)\n"
    )

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2 * 1024 * 1024,
    )

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(
        dbos_settings.paths.db_path, delegation_id, pid=process.pid
    )

    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        async def _signal_after_delay() -> None:
            await asyncio.sleep(0.15)
            with contextlib.suppress(ProcessLookupError):
                process.terminate()

        signaller = asyncio.create_task(_signal_after_delay())
        try:
            result = await asyncio.wait_for(
                monitor_delegation(delegation_id, poll_interval_s=0.020),
                timeout=15.0,
            )
        finally:
            await signaller

        # SIGTERM on POSIX → returncode == -SIGTERM → killed.
        assert result.status == STATUS_KILLED, (
            f"got status={result.status} rc={result.returncode}"
        )
    finally:
        conn.close()

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_KILLED


# ---------------------------------------------------------------------------
# Configurable poll interval
# ---------------------------------------------------------------------------


def test_default_poll_interval_is_one_second() -> None:
    """§5.6: returncode polling at a configurable interval (default 1s)."""
    assert DEFAULT_POLL_INTERVAL_S == 1.0


async def test_custom_poll_interval_honored(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """The workflow honors a tighter poll interval (used by tests for speed)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_running_row(dbos_settings.paths.db_path, delegation_id)

    process = FakeProcess()
    conn = open_connection(dbos_settings.paths.db_path)
    try:
        reader = start_reader(delegation_id, process, store=conn)
        register_delegation_subprocess(
            DelegationProcessHandle(
                delegation_id=delegation_id,
                process=process,
                reader_handle=reader,
                db_path=dbos_settings.paths.db_path,
            )
        )

        async def _exit_immediately() -> None:
            # Exit almost immediately so the workflow returns quickly under a tight poll.
            await asyncio.sleep(0.01)
            process.set_returncode(0)
            process.close_streams()

        exiter = asyncio.create_task(_exit_immediately())
        try:
            # With a 5ms poll interval, the workflow should finish within ~100ms.
            result = await asyncio.wait_for(
                monitor_delegation(delegation_id, poll_interval_s=0.005),
                timeout=2.0,
            )
        finally:
            await exiter
        assert result.status == STATUS_COMPLETED
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Idempotency framework usage
# ---------------------------------------------------------------------------


def test_drain_step_uses_idempotent_step() -> None:
    """All @DBOS.step calls with external side effects use the idempotency framework.

    Asserts the drain step + persistence step expose the idempotency_key_for
    helper attached by :func:`idempotent_step`.
    """
    from capo.workflows.delegation import _drain_output_step, _persist_terminal_step

    assert hasattr(_drain_output_step, "idempotency_key_for"), (
        "drain step must be wrapped in idempotent_step"
    )
    assert hasattr(_persist_terminal_step, "idempotency_key_for"), (
        "persistence step must be wrapped in idempotent_step"
    )

    # Key derivation is stable for the documented deterministic args.
    k1 = _drain_output_step.idempotency_key_for("delegation-abc")
    k2 = _drain_output_step.idempotency_key_for("delegation-abc")
    assert k1 == k2
    assert k1.startswith("monitor_delegation_drain_output:")

    k3 = _persist_terminal_step.idempotency_key_for(
        "delegation-abc", STATUS_COMPLETED, None, "/tmp/state.db"
    )
    assert k3.startswith("monitor_delegation_persist_terminal:")

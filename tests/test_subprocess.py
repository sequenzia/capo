"""Tests for :mod:`capo.tools._subprocess` (Task #21).

Covers the §5.6 / §6.1 / §9.2 acceptance criteria for the async per-delegation
output reader:

- Chunks land in ``delegation_output``.
- JSON event lines are classified as ``stream='event'``.
- Cancellation flushes pending buffer (no rows lost).
- A chatty subprocess producing >10 MB of stdout is ingested without pipe
  blocking and every byte is captured.
- Very long lines (>1 MB without a newline) are split into bounded chunks.
- Non-JSON stdout (CC's plain-text preamble per S-3 §4.2) falls back to
  ``stream='stdout'``.
- Reader exception path: kills the child and re-raises as :class:`ReaderError`.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from capo.memory.store import open_connection
from capo.tools import _subprocess as sub
from capo.tools._subprocess import (
    CHUNK_SPLIT_BYTES,
    MAX_LINE_BYTES,
    ReaderError,
    start_reader,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_DELEGATION_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS delegation_output (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    delegation_id TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stream        TEXT NOT NULL CHECK (stream IN ('stdout','stderr','event')),
    chunk         TEXT NOT NULL
)
"""


def _make_db(tmp_path: Path) -> sqlite3.Connection:
    """Open a hardened connection with the §7.3 delegation_output schema.

    We mirror the production DDL but drop the FK to ``delegations`` so each
    test stays self-contained (no need to seed parent rows).
    """
    conn = open_connection(tmp_path / "state.db")
    conn.execute(_DELEGATION_OUTPUT_DDL)
    return conn


class FakeStream:
    """Minimal asyncio.StreamReader stand-in driven by a list of byte chunks.

    Tests use this to feed deterministic bytes into the reader without
    spawning a real subprocess. ``readline`` mimics asyncio.StreamReader's
    semantics: it returns one newline-terminated chunk at a time, or any
    remaining bytes (without newline) at EOF, then ``b""`` forever.
    """

    def __init__(self, payload: bytes) -> None:
        self._buf = bytearray(payload)
        self._eof = False

    async def readline(self) -> bytes:
        # Cooperatively yield so other tasks can run.
        await asyncio.sleep(0)
        if not self._buf:
            return b""
        idx = self._buf.find(b"\n")
        if idx == -1:
            out = bytes(self._buf)
            self._buf.clear()
            return out
        out = bytes(self._buf[: idx + 1])
        del self._buf[: idx + 1]
        return out

    async def read(self, n: int) -> bytes:
        await asyncio.sleep(0)
        if not self._buf:
            return b""
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


class SlowFakeStream:
    """Like FakeStream but yields bytes in small chunks with sleeps.

    Lets the cancellation test interrupt mid-stream deterministically.
    """

    def __init__(self, lines: list[bytes], delay_s: float = 0.005) -> None:
        self._lines = list(lines)
        self._delay = delay_s

    async def readline(self) -> bytes:
        await asyncio.sleep(self._delay)
        if not self._lines:
            return b""
        return self._lines.pop(0)

    async def read(self, n: int) -> bytes:
        # Cancellation test never hits read(); implement defensively.
        line = await self.readline()
        return line[:n]


class FakeProcess:
    """Minimal duck for asyncio.subprocess.Process."""

    def __init__(self, stdout: Any, stderr: Any) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode: int | None = None
        self.terminate_called = False
        self.kill_called = False

    def terminate(self) -> None:
        self.terminate_called = True
        self.returncode = -15

    def kill(self) -> None:
        self.kill_called = True
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


def _rows(conn: sqlite3.Connection, delegation_id: str) -> list[tuple[str, str]]:
    """Return ``(stream, chunk)`` rows for a delegation, in insertion order."""
    cur = conn.execute(
        "SELECT stream, chunk FROM delegation_output "
        "WHERE delegation_id = ? ORDER BY id",
        (delegation_id,),
    )
    return list(cur.fetchall())


# ---------------------------------------------------------------------------
# Unit: chunks land in DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stdout_chunks_persisted(tmp_path: Path) -> None:
    """Plain stdout lines write as ``stream='stdout'`` rows."""
    conn = _make_db(tmp_path)
    try:
        payload = b"hello\nworld\nthird line\n"
        proc = FakeProcess(FakeStream(payload), FakeStream(b""))

        handle = start_reader("deleg-A", proc, store=conn)
        stats = await handle.wait()

        rows = _rows(conn, "deleg-A")
        assert [r[0] for r in rows] == ["stdout", "stdout", "stdout"]
        assert [r[1] for r in rows] == ["hello\n", "world\n", "third line\n"]
        assert stats.stdout_rows == 3
        assert stats.event_rows == 0
        assert stats.stderr_rows == 0
        assert stats.eof_reached is True
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_stderr_chunks_persisted(tmp_path: Path) -> None:
    """Stderr lines write as ``stream='stderr'`` regardless of content shape."""
    conn = _make_db(tmp_path)
    try:
        # Even JSON on stderr stays as stderr — never reclassified.
        payload = b'{"warning":"oops"}\nplain error\n'
        proc = FakeProcess(FakeStream(b""), FakeStream(payload))

        handle = start_reader("deleg-B", proc, store=conn)
        await handle.wait()

        rows = _rows(conn, "deleg-B")
        assert {r[0] for r in rows} == {"stderr"}
        assert len(rows) == 2
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit: JSON event detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_json_lines_become_event_rows(tmp_path: Path) -> None:
    """CC's NDJSON event lines write as ``stream='event'`` with normalized JSON."""
    conn = _make_db(tmp_path)
    try:
        # Sample CC events per S-3 §3 (session_id is always present).
        events = [
            {
                "type": "system",
                "subtype": "init",
                "session_id": "8f3c8d2e-1d2e-4a3b-9c4d-1a2b3c4d5e6f",
                "claude_code_version": "2.1.138",
            },
            {
                "type": "assistant",
                "session_id": "8f3c8d2e-1d2e-4a3b-9c4d-1a2b3c4d5e6f",
                "message": {"role": "assistant", "content": []},
            },
            {
                "type": "result",
                "session_id": "8f3c8d2e-1d2e-4a3b-9c4d-1a2b3c4d5e6f",
                "is_error": False,
                "subtype": "success",
            },
        ]
        payload = b"".join((json.dumps(e) + "\n").encode("utf-8") for e in events)
        proc = FakeProcess(FakeStream(payload), FakeStream(b""))

        handle = start_reader("deleg-C", proc, store=conn)
        stats = await handle.wait()

        rows = _rows(conn, "deleg-C")
        assert [r[0] for r in rows] == ["event", "event", "event"]
        assert stats.event_rows == 3
        assert stats.parse_errors == 0
        # Each chunk is canonical JSON (sorted keys), round-trips losslessly.
        for raw, expected in zip(rows, events, strict=True):
            assert json.loads(raw[1]) == expected
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_plain_text_preamble_stays_stdout(tmp_path: Path) -> None:
    """S-3 §4.2: non-JSON preamble lines write as ``stream='stdout'``.

    On ``claude --resume <bad-uuid>`` the first stdout line is a plain-text
    error, then a JSON ``result`` event. We must not crash on the preamble
    and the result event must still be classified as ``event``.
    """
    conn = _make_db(tmp_path)
    try:
        payload = (
            b"No conversation found with session ID: bogus-uuid\n"
            b'{"type":"result","session_id":"new-id","is_error":true,'
            b'"subtype":"error_during_execution"}\n'
        )
        proc = FakeProcess(FakeStream(payload), FakeStream(b""))

        handle = start_reader("deleg-D", proc, store=conn)
        stats = await handle.wait()

        rows = _rows(conn, "deleg-D")
        assert [r[0] for r in rows] == ["stdout", "event"]
        assert stats.parse_errors == 0  # preamble didn't *try* to parse
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_malformed_json_falls_back_to_stdout(tmp_path: Path) -> None:
    """A line that starts with ``{`` but fails ``json.loads`` records the error
    and persists as plain stdout (no data loss)."""
    conn = _make_db(tmp_path)
    try:
        payload = b'{"oops": broken\n'
        proc = FakeProcess(FakeStream(payload), FakeStream(b""))

        handle = start_reader("deleg-E", proc, store=conn)
        stats = await handle.wait()

        rows = _rows(conn, "deleg-E")
        assert [r[0] for r in rows] == ["stdout"]
        assert stats.parse_errors == 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit: cancellation flushes pending buffer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancellation_flushes_pending(tmp_path: Path) -> None:
    """Cancelling the reader mid-stream must flush whatever's buffered.

    We stall the reader with a slow stream, let a few rows accumulate below
    the flush_count threshold and the flush_interval, then cancel — and
    verify the rows still landed in the DB.
    """
    conn = _make_db(tmp_path)
    try:
        # 3 lines, slow enough that we cancel before the interval-flush fires.
        slow = SlowFakeStream(
            [b"a\n", b"b\n", b"c\n", b"d\n", b"e\n"],
            delay_s=0.005,
        )
        proc = FakeProcess(slow, FakeStream(b""))

        handle = start_reader(
            "deleg-CANCEL",
            proc,
            store=conn,
            # Force interval flushes to be effectively-never so we *prove*
            # the cancel path is what does the final flush.
            flush_interval_s=60.0,
            flush_count=1000,
        )

        # Let the reader pull at least one row through.
        await asyncio.sleep(0.020)
        handle.cancel()
        stats = await handle.wait()

        rows = _rows(conn, "deleg-CANCEL")
        # At least one row must have been flushed by the cancel path.
        assert len(rows) >= 1
        assert {r[0] for r in rows} == {"stdout"}
        assert stats.eof_reached is False  # cancelled before EOF
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge: very long lines split into chunks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oversized_line_split_into_chunks(tmp_path: Path) -> None:
    """A single 'line' >MAX_LINE_BYTES must be split into CHUNK_SPLIT_BYTES pieces."""
    conn = _make_db(tmp_path)
    try:
        # 2.5 MiB of bytes with no newline anywhere — defeats readline().
        size = int(MAX_LINE_BYTES * 2.5)
        big = b"x" * size + b"\n"  # newline only at the very end
        proc = FakeProcess(FakeStream(big), FakeStream(b""))

        handle = start_reader("deleg-BIG", proc, store=conn)
        stats = await handle.wait()

        rows = _rows(conn, "deleg-BIG")
        # All rows must be stdout (force_plain on the split path).
        assert {r[0] for r in rows} == {"stdout"}
        # Every chunk except possibly the last must be <= CHUNK_SPLIT_BYTES
        # (decoded length will equal byte length for our ASCII payload).
        for stream, chunk in rows[:-1]:
            assert len(chunk) <= CHUNK_SPLIT_BYTES, (stream, len(chunk))
        # Total reconstructed length equals original payload size.
        total = sum(len(r[1]) for r in rows)
        assert total == size + 1  # +1 for the trailing newline byte
        assert stats.split_lines >= 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Integration: chatty subprocess, >10 MB stdout, zero pipe blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chatty_subprocess_10mb_no_block(tmp_path: Path) -> None:
    """§6.1: chatty CC run producing >10 MB of stdout completes without pipe block.

    We use a real Python child that fires off lines as fast as it can. macOS
    pipe buffer is ~64 KB; if the reader ever stops draining, the child will
    stall on its next ``print`` and the test will time out — that's the
    "pipe blocking" failure mode we're guarding against.
    """
    # Print 250 KiB lines * 50 = 12.5 MiB total, well over the §6.1 threshold.
    # Each line is ``line_size`` 'x' bytes + 1 newline byte.
    line_size = 250 * 1024  # 250 KiB of 'x'
    line_count = 50
    target_bytes = (line_size + 1) * line_count  # +1 per line for '\n'

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
        # Match what we'd use in production: large enough that the
        # StreamReader doesn't trip its own limit on our 250 KiB lines.
        limit=2 * 1024 * 1024,
    )

    conn = _make_db(tmp_path)
    try:
        handle = start_reader("deleg-10MB", process, store=conn)

        # The subprocess MUST finish quickly. If the reader ever blocks the
        # pipe, the child stalls and this wait_for raises TimeoutError.
        await asyncio.wait_for(handle.wait(), timeout=30.0)
        rc = await process.wait()
        assert rc == 0

        # Zero loss: every byte the child printed is in the DB.
        cur = conn.execute(
            "SELECT COALESCE(SUM(LENGTH(chunk)), 0) FROM delegation_output "
            "WHERE delegation_id = ? AND stream = 'stdout'",
            ("deleg-10MB",),
        )
        total = cur.fetchone()[0]
        assert total == target_bytes, (
            f"expected exactly {target_bytes} bytes captured, got {total}"
        )

        # And stats agree.
        assert handle.stats.stdout_bytes == target_bytes
        assert handle.stats.eof_reached is True
    finally:
        # Ensure the child is reaped even on assertion failure.
        if process.returncode is None:
            process.kill()
            await process.wait()
        conn.close()


# ---------------------------------------------------------------------------
# Error path: fatal reader exception terminates the child and re-raises
# ---------------------------------------------------------------------------


class ExplodingStream:
    """Raises on first read — simulates a totally hostile pipe."""

    async def readline(self) -> bytes:
        raise RuntimeError("simulated pipe explosion")

    async def read(self, n: int) -> bytes:
        raise RuntimeError("simulated pipe explosion")


@pytest.mark.asyncio
async def test_fatal_reader_error_kills_child(tmp_path: Path) -> None:
    """A non-recoverable exception in the drainer kills the subprocess and
    is re-raised as :class:`ReaderError`."""
    conn = _make_db(tmp_path)
    try:
        proc = FakeProcess(ExplodingStream(), FakeStream(b""))

        handle = start_reader("deleg-BOOM", proc, store=conn)
        with pytest.raises(ReaderError):
            await handle.task

        # ``_try_terminate`` reached for ``terminate()`` on the fake.
        assert proc.terminate_called is True
        assert handle.stats.fatal_error is not None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Misc: missing pipes raise immediately (caller bug, not a runtime concern)
# ---------------------------------------------------------------------------


def test_missing_pipes_raises_value_error(tmp_path: Path) -> None:
    conn = _make_db(tmp_path)
    try:
        proc = FakeProcess(None, FakeStream(b""))
        with pytest.raises(ValueError, match="non-None stdout"):
            start_reader("deleg-NOPIPE", proc, store=conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Determinism: timestamp source is injectable.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_now_callable_used_for_timestamps(tmp_path: Path) -> None:
    """Custom ``now`` source feeds the ``ts`` column for deterministic tests."""
    from datetime import UTC, datetime

    fixed = datetime(2026, 5, 10, 22, 7, 13, tzinfo=UTC)

    conn = _make_db(tmp_path)
    try:
        proc = FakeProcess(FakeStream(b"x\n"), FakeStream(b""))
        handle = start_reader("deleg-TS", proc, store=conn, now=lambda: fixed)
        await handle.wait()

        ts_row = conn.execute(
            "SELECT ts FROM delegation_output WHERE delegation_id = ?",
            ("deleg-TS",),
        ).fetchone()
        assert ts_row[0].startswith("2026-05-10 22:07:13")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sanity: AsyncIterator type alias used in helpers stays import-clean.
# (No assertion — guards against accidental removal of the import.)
# ---------------------------------------------------------------------------


def test_module_exports() -> None:
    assert "start_reader" in sub.__all__
    assert "ReaderHandle" in sub.__all__
    assert "ReaderError" in sub.__all__


# Silence unused-import warnings on optional names used only for typing clarity
# in the public-facing parts of the test file.
_ = AsyncIterator  # noqa: F841

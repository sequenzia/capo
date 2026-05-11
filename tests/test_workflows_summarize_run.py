"""Tests for :mod:`capo.workflows.delegation.summarize_run` (Task #32).

Covers the spec §5.6 / §5.12 acceptance criteria for the DBOS-managed
``summarize_run`` step:

Functional:
  * ``summarize_run`` is a ``@DBOS.step`` (registered via
    :func:`~capo.workflows._idempotency.idempotent_step`) keyed by
    ``delegation_id``.
  * Produces a deterministic short summary from the ``delegations`` row +
    the last N ``delegation_output`` rows. Same input → same string.
  * Summary is written to the ``delegations.summary`` column.

Edge Cases:
  * No output rows yet (subprocess killed immediately): summary still
    produced; ``last[none]=no output``.
  * Final assistant message > 500 chars: truncated with the explicit
    suffix ``…[truncated N chars]``.

Integration:
  * Workflow → row's ``summary`` column is populated on terminal status.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from capo.config import PathsConfig
from capo.memory.store import open_connection
from capo.tools._subprocess import start_reader
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    STATUS_COMPLETED,
    DelegationProcessHandle,
    _build_run_summary,
    _summary_extract_assistant_text,
    _summary_format_runtime,
    _summary_truncate_last,
    monitor_delegation,
    register_delegation_subprocess,
    summarize_run,
)

# ---------------------------------------------------------------------------
# Fixtures + schema helpers (mirrors tests/test_workflows_delegation.py)
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


def _insert_row(
    db_path: Path,
    delegation_id: str,
    *,
    status: str = "running",
    started_at: str = "2026-05-10 00:00:00.000000",
    ended_at: str | None = None,
    summary: str | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status, started_at, ended_at, summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delegation_id,
                "owner",
                "claude-code",
                f"/tmp/ws/{delegation_id}",
                12345,
                None,
                "{}",
                status,
                started_at,
                ended_at,
                summary,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_output(
    db_path: Path,
    delegation_id: str,
    rows: list[tuple[str, str]],
) -> None:
    """Insert ``(stream, chunk)`` tuples in order. ``ts`` is auto-assigned."""
    conn = sqlite3.connect(str(db_path))
    try:
        for stream, chunk in rows:
            conn.execute(
                "INSERT INTO delegation_output (delegation_id, stream, chunk) "
                "VALUES (?, ?, ?)",
                (delegation_id, stream, chunk),
            )
        conn.commit()
    finally:
        conn.close()


def _read_summary(db_path: Path, delegation_id: str) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            "SELECT summary FROM delegations WHERE id = ?", (delegation_id,)
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else None


@pytest.fixture(autouse=True)
def _clean_dbos_state() -> Iterator[None]:
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "state.db"
    _ensure_schema(p)
    return p


# ---------------------------------------------------------------------------
# Unit: _summary_truncate_last
# ---------------------------------------------------------------------------


def test_truncate_last_short_text_unchanged() -> None:
    text = "hello world"
    assert _summary_truncate_last(text) == text


def test_truncate_last_exact_cap_unchanged() -> None:
    text = "x" * 500
    assert _summary_truncate_last(text) == text


def test_truncate_last_over_cap_truncates_with_suffix() -> None:
    text = "x" * 600
    out = _summary_truncate_last(text)
    # Keep first 500 chars, then ellipsis+bracketed truncation suffix.
    assert out.startswith("x" * 500)
    assert "[truncated 100 chars]" in out
    assert "…" in out


def test_truncate_last_huge_text_preserves_prefix_and_dropped_count() -> None:
    text = "a" * 5000
    out = _summary_truncate_last(text)
    assert out.startswith("a" * 500)
    assert "[truncated 4500 chars]" in out


# ---------------------------------------------------------------------------
# Unit: _summary_extract_assistant_text
# ---------------------------------------------------------------------------


def test_extract_assistant_text_happy_path() -> None:
    chunk = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Hello,"},
                    {"type": "text", "text": "world."},
                ]
            },
        }
    )
    assert _summary_extract_assistant_text(chunk) == "Hello,\nworld."


def test_extract_assistant_text_non_json_returns_none() -> None:
    assert _summary_extract_assistant_text("not json at all") is None


def test_extract_assistant_text_non_assistant_event_returns_none() -> None:
    chunk = json.dumps({"type": "system", "subtype": "init"})
    assert _summary_extract_assistant_text(chunk) is None


def test_extract_assistant_text_tool_use_only_returns_none() -> None:
    # Message with only a tool_use (no text segment).
    chunk = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "tool_use", "name": "Bash", "input": {}}]
            },
        }
    )
    assert _summary_extract_assistant_text(chunk) is None


def test_extract_assistant_text_empty_string_returns_none() -> None:
    assert _summary_extract_assistant_text("") is None
    assert _summary_extract_assistant_text("   \n  ") is None


# ---------------------------------------------------------------------------
# Unit: _summary_format_runtime
# ---------------------------------------------------------------------------


def test_format_runtime_missing_either_side_is_question_mark() -> None:
    assert _summary_format_runtime(None, None) == "?"
    assert _summary_format_runtime("2026-05-10 00:00:00", None) == "?"
    assert _summary_format_runtime(None, "2026-05-10 00:00:00") == "?"


def test_format_runtime_positive_delta() -> None:
    out = _summary_format_runtime(
        "2026-05-10 00:00:00.000000",
        "2026-05-10 00:01:30.000000",
    )
    assert out == "90s"


def test_format_runtime_negative_delta_is_question_mark() -> None:
    # Clock skew shouldn't crash the summary.
    out = _summary_format_runtime(
        "2026-05-10 00:01:00.000000",
        "2026-05-10 00:00:00.000000",
    )
    assert out == "?"


def test_format_runtime_unparseable_string_is_question_mark() -> None:
    out = _summary_format_runtime("not a timestamp", "2026-05-10 00:00:00")
    assert out == "?"


# ---------------------------------------------------------------------------
# Unit: _build_run_summary — DETERMINISM is the core contract
# ---------------------------------------------------------------------------


def test_build_summary_deterministic_same_input_same_output(db_path: Path) -> None:
    """Same row + same output rows → byte-identical string across calls.

    This is the §5.6 restart-resume reproducibility contract. The function
    must NOT read wall-clock time, randomness, or external services.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        started_at="2026-05-10 00:00:00.000000",
        ended_at="2026-05-10 00:00:05.000000",
    )
    _insert_output(
        db_path,
        delegation_id,
        [
            ("stdout", "preamble line\n"),
            (
                "event",
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "content": [
                                {"type": "text", "text": "Refactor done."}
                            ]
                        },
                    }
                ),
            ),
        ],
    )

    s1 = _build_run_summary(db_path, delegation_id)
    s2 = _build_run_summary(db_path, delegation_id)
    s3 = _build_run_summary(db_path, delegation_id)

    assert s1 == s2 == s3
    # And the format is the contracted one-liner.
    assert s1.startswith("status=completed ")
    assert "runtime=5s" in s1
    assert "last[assistant]=Refactor done." in s1


def test_build_summary_includes_per_stream_byte_and_row_counts(
    db_path: Path,
) -> None:
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:01.000000",
    )
    # 2 stdout rows totaling 11 bytes, 1 stderr row of 3 bytes,
    # 1 event row totaling LEN(json) bytes.
    _insert_output(
        db_path,
        delegation_id,
        [
            ("stdout", "hello\n"),  # 6 bytes
            ("stdout", "hi!\n"),  # 4 bytes (total 10; but trailing \n included)
            ("stderr", "oh!"),  # 3 bytes
            ("event", json.dumps({"type": "system"})),
        ],
    )

    summary = _build_run_summary(db_path, delegation_id)

    # Stable layout — assert structural keys (not exact numbers, since
    # JSON encoding length is small but env-stable).
    assert "rows(out=2 err=1 evt=1)" in summary
    assert "bytes(out=" in summary
    assert "err=3" in summary
    assert "evt=" in summary


def test_build_summary_no_output_rows_says_no_output(db_path: Path) -> None:
    """Edge case from spec §5.6 / Task #32: subprocess killed immediately.

    No delegation_output rows yet — the summary should still produce a
    valid string and mention "no output".
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="killed",
        ended_at="2026-05-10 00:00:00.500000",
    )

    summary = _build_run_summary(db_path, delegation_id)

    assert "no output" in summary
    assert "last[none]=" in summary
    assert "rows(out=0 err=0 evt=0)" in summary
    assert "bytes(out=0 err=0 evt=0)" in summary


def test_build_summary_truncates_long_assistant_message(db_path: Path) -> None:
    """Edge case: final assistant message > 500 chars → truncated with suffix."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:10.000000",
    )

    long_text = "A" * 1200
    _insert_output(
        db_path,
        delegation_id,
        [
            (
                "event",
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "content": [{"type": "text", "text": long_text}]
                        },
                    }
                ),
            ),
        ],
    )

    summary = _build_run_summary(db_path, delegation_id)

    assert "last[assistant]=" in summary
    assert "A" * 500 in summary
    # The dropped count is len(long_text) - 500 = 700.
    assert "[truncated 700 chars]" in summary
    # And the FULL string isn't in the summary.
    assert long_text not in summary


def test_build_summary_prefers_stderr_over_assistant_on_failure(
    db_path: Path,
) -> None:
    """Walk tail in reverse-chrono; stderr wins if it's the most recent."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="failed",
        ended_at="2026-05-10 00:00:30.000000",
    )

    _insert_output(
        db_path,
        delegation_id,
        [
            (
                "event",
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "content": [{"type": "text", "text": "Working..."}]
                        },
                    }
                ),
            ),
            # stderr inserted LATER → has higher id → most recent in the
            # ORDER BY id DESC tail.
            ("stderr", "Traceback: NPE\n"),
        ],
    )

    summary = _build_run_summary(db_path, delegation_id)

    assert "last[error]=Traceback: NPE" in summary


def test_build_summary_missing_row_returns_diagnostic(db_path: Path) -> None:
    """Row not in DB → diagnostic string (no crash)."""
    out = _build_run_summary(db_path, "no-such-id")
    assert "no-such-id" in out
    assert "not found" in out


def test_build_summary_runtime_question_mark_when_ended_at_null(
    db_path: Path,
) -> None:
    """Before terminal persist runs, ended_at is NULL — runtime renders '?'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="running",
        ended_at=None,
    )

    summary = _build_run_summary(db_path, delegation_id)
    assert "runtime=?" in summary


def test_build_summary_no_wall_clock_dependency(db_path: Path) -> None:
    """Calling repeatedly with delays in between yields the same string.

    Guards against accidental ``datetime.now()`` calls inside the
    function: if any wall-clock read crept in, the runtime / last-line
    excerpt would shift between calls separated by a real delay.
    """
    import time as _time  # local import to avoid module-level use

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        started_at="2026-05-10 00:00:00.000000",
        ended_at="2026-05-10 00:00:02.000000",
    )
    _insert_output(
        db_path,
        delegation_id,
        [("stdout", "line one\n"), ("stdout", "line two\n")],
    )

    s1 = _build_run_summary(db_path, delegation_id)
    _time.sleep(0.05)
    s2 = _build_run_summary(db_path, delegation_id)
    _time.sleep(0.10)
    s3 = _build_run_summary(db_path, delegation_id)

    assert s1 == s2 == s3


# ---------------------------------------------------------------------------
# Unit: summarize_run public API (writes row's summary column)
# ---------------------------------------------------------------------------


async def test_summarize_run_writes_summary_column(db_path: Path) -> None:
    """``summarize_run(delegation_id, ..., db_path=str(...))`` UPDATEs the row."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:03.000000",
    )
    _insert_output(db_path, delegation_id, [("stdout", "ok\n")])

    out = await summarize_run(delegation_id, None, str(db_path))

    persisted = _read_summary(db_path, delegation_id)
    assert persisted == out
    assert persisted is not None
    assert persisted.startswith("status=completed ")


async def test_summarize_run_no_output_edge_case(db_path: Path) -> None:
    """Subprocess killed immediately: no output rows, but summary persists."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="killed",
        ended_at="2026-05-10 00:00:00.100000",
    )

    out = await summarize_run(delegation_id, "killed", str(db_path))

    assert "no output" in out
    persisted = _read_summary(db_path, delegation_id)
    assert persisted == out


async def test_summarize_run_truncation_edge_case(db_path: Path) -> None:
    """Final assistant message > 500 chars → truncated + persisted."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:01.000000",
    )
    long_text = "Z" * 850
    _insert_output(
        db_path,
        delegation_id,
        [
            (
                "event",
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "content": [{"type": "text", "text": long_text}]
                        },
                    }
                ),
            )
        ],
    )

    out = await summarize_run(delegation_id, "completed", str(db_path))

    assert "[truncated 350 chars]" in out
    assert "Z" * 500 in out
    assert long_text not in out
    # Persisted matches return value.
    assert _read_summary(db_path, delegation_id) == out


async def test_summarize_run_deterministic_repeated_calls(
    db_path: Path,
) -> None:
    """Calling summarize_run multiple times with same inputs yields same string.

    The @idempotent_step framework guarantees DBOS-replay returns the
    cached value; even outside DBOS the function itself is deterministic
    so repeated calls produce the same output.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:04.000000",
    )
    _insert_output(db_path, delegation_id, [("stdout", "x\n")])

    s1 = await summarize_run(delegation_id, None, str(db_path))
    s2 = await summarize_run(delegation_id, None, str(db_path))
    s3 = await summarize_run(delegation_id, None, str(db_path))

    assert s1 == s2 == s3


async def test_summarize_run_falls_back_to_registry_for_db_path(
    db_path: Path, tmp_path: Path
) -> None:
    """When ``db_path`` is not supplied, the registry handle provides it."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        db_path,
        delegation_id,
        status="completed",
        ended_at="2026-05-10 00:00:02.000000",
    )
    _insert_output(db_path, delegation_id, [("stdout", "hi\n")])

    # Register a handle so the no-arg call can find db_path.
    class _Proc:
        returncode: int | None = 0

        async def wait(self) -> int:  # pragma: no cover - not exercised
            return 0

    class _RH:
        async def wait(self) -> object:  # pragma: no cover - not exercised
            return object()

    register_delegation_subprocess(
        DelegationProcessHandle(
            delegation_id=delegation_id,
            process=_Proc(),
            reader_handle=_RH(),  # type: ignore[arg-type]
            db_path=db_path,
        )
    )

    out = await summarize_run(delegation_id)
    assert _read_summary(db_path, delegation_id) == out


async def test_summarize_run_missing_db_path_and_no_registry_returns_diagnostic(
    db_path: Path,
) -> None:
    """No db_path arg + no registry → diagnostic string returned (no crash)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"

    out = await summarize_run(delegation_id)  # no db_path, no registry entry
    assert "unable to resolve db_path" in out


# ---------------------------------------------------------------------------
# Integration: workflow → row.summary populated on terminal status
# ---------------------------------------------------------------------------


async def test_workflow_populates_row_summary_on_terminal_status(
    tmp_path: Path,
) -> None:
    """End-to-end: monitor_delegation runs → row.summary is populated."""
    settings = _make_settings(tmp_path)
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    try:
        delegation_id = f"d-{uuid.uuid4().hex[:8]}"
        _insert_row(
            settings.paths.db_path,
            delegation_id,
            status="running",
            ended_at=None,
        )

        # Use a FakeProcess-style stub via the same pattern as
        # test_workflows_delegation.py.
        class FakeStream:
            def __init__(self) -> None:
                self._buf = bytearray()
                self._eof = asyncio.Event()

            def feed(self, data: bytes) -> None:
                self._buf.extend(data)

            def close(self) -> None:
                self._eof.set()

            async def readline(self) -> bytes:
                while True:
                    if self._buf:
                        idx = self._buf.find(b"\n")
                        if idx == -1:
                            if self._eof.is_set():
                                out = bytes(self._buf)
                                self._buf.clear()
                                return out
                            await asyncio.sleep(0.005)
                            continue
                        out = bytes(self._buf[: idx + 1])
                        del self._buf[: idx + 1]
                        return out
                    if self._eof.is_set():
                        return b""
                    await asyncio.sleep(0.005)

            async def read(self, n: int) -> bytes:
                await asyncio.sleep(0.005)
                if not self._buf:
                    return b"" if self._eof.is_set() else b""
                out = bytes(self._buf[:n])
                del self._buf[:n]
                return out

        class FakeProcess:
            def __init__(self) -> None:
                self.stdout = FakeStream()
                self.stderr = FakeStream()
                self.returncode: int | None = None

            def set_returncode(self, rc: int) -> None:
                self.returncode = rc

            def close_streams(self) -> None:
                self.stdout.close()
                self.stderr.close()

            def terminate(self) -> None:  # pragma: no cover
                self.returncode = -15
                self.close_streams()

            def kill(self) -> None:  # pragma: no cover
                self.returncode = -9
                self.close_streams()

            async def wait(self) -> int:
                while self.returncode is None:
                    await asyncio.sleep(0.005)
                return self.returncode

        process = FakeProcess()
        conn = open_connection(settings.paths.db_path)
        try:
            reader = start_reader(delegation_id, process, store=conn)
            register_delegation_subprocess(
                DelegationProcessHandle(
                    delegation_id=delegation_id,
                    process=process,
                    reader_handle=reader,
                    db_path=settings.paths.db_path,
                )
            )

            process.stdout.feed(b"a line of plain stdout\n")
            process.stdout.feed(
                b'{"type":"assistant","message":'
                b'{"content":[{"type":"text","text":"All done!"}]}}\n'
            )

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
            assert result.status == STATUS_COMPLETED
        finally:
            conn.close()

        # The terminal row must have a non-NULL, non-empty summary that
        # came from summarize_run (so it contains the deterministic
        # one-liner's structure, not just the classifier's "exit code N"
        # heuristic).
        persisted = _read_summary(settings.paths.db_path, delegation_id)
        assert persisted is not None
        assert persisted.startswith("status=completed ")
        # And it captured the assistant text we fed in.
        assert "All done!" in persisted
    finally:
        with contextlib.suppress(Exception):
            destroy_dbos()

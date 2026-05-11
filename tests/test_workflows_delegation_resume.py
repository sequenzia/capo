"""Tests for the §5.6 restart-resume contract (Task #31).

Covers the acceptance criteria of Task #31: when ``monitor_delegation``
re-enters a workflow whose original subprocess is gone, it must:

1. Re-spawn via ``claude --resume <session_id_subagent>`` (per §7.5 / S-3 §4.2)
   when the row has a captured session_id.
2. Mark the row ``failed`` with reason "lost session_id across restart" when
   the row's ``session_id_subagent`` is NULL.
3. Refuse to overwrite ``session_id_subagent`` if the resumed CC's first
   event has ``is_error=true`` (per S-3 §4.2).
4. Be idempotent within a single process lifetime: re-entering twice with
   the registry already populated must NOT re-spawn.
5. Honor a row that is already terminal — derive status without re-spawning
   (Edge case: "Subprocess already terminated before workflow re-entry").

These tests use the `_FAKE_CLAUDE_SCRIPT` pattern established by Phase 2
Task #22 to drive a real subprocess that simulates ``claude --resume`` —
they do NOT require a real Claude Code install.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from capo.config import PathsConfig
from capo.memory.store import open_connection
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    RESUME_REASON_FAILED,
    RESUME_REASON_LOST_SESSION_ID,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    DelegationProcessHandle,
    _build_resume_argv,
    _first_event_is_error,
    _resume_spawn_step,
    monitor_delegation,
    register_delegation_subprocess,
)

# ---------------------------------------------------------------------------
# Fixtures (mirror the Task #30 test suite layout for consistency)
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
    session_id: str | None,
    workspace: str | None = None,
    status: str = STATUS_RUNNING,
    pid: int | None = 99999,
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
                workspace or f"/tmp/ws/{delegation_id}",
                pid,
                session_id,
                "{}",
                status,
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
            "SELECT status, ended_at, summary, session_id_subagent, pid "
            "FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state() -> Iterator[None]:
    """DBOS is a process-level singleton; teardown between tests."""
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
    settings = _make_settings(tmp_path)
    _ensure_schema(settings.paths.db_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    return settings


# ---------------------------------------------------------------------------
# Unit: _build_resume_argv
# ---------------------------------------------------------------------------


def test_build_resume_argv_matches_spec() -> None:
    """Per §7.5 / S-3 §4.2: ``claude --resume <id> --output-format stream-json
    --verbose`` is the canonical resume invocation."""
    argv = _build_resume_argv(
        binary="claude", session_id="sid-abc", model=None
    )
    assert argv == [
        "claude",
        "--resume",
        "sid-abc",
        "--output-format",
        "stream-json",
        "--verbose",
    ]


def test_build_resume_argv_includes_model_when_provided() -> None:
    """Per-delegation ``--model`` is preserved across resume."""
    argv = _build_resume_argv(
        binary="claude", session_id="sid", model="anthropic:claude-sonnet-4-6"
    )
    assert "--model" in argv
    idx = argv.index("--model")
    assert argv[idx + 1] == "anthropic:claude-sonnet-4-6"


def test_build_resume_argv_omits_prompt_flag() -> None:
    """``-p`` is NOT passed on resume — the prompt is inherited."""
    argv = _build_resume_argv(
        binary="claude", session_id="sid", model=None
    )
    assert "-p" not in argv


# ---------------------------------------------------------------------------
# Unit: _first_event_is_error
# ---------------------------------------------------------------------------


def test_first_event_is_error_true() -> None:
    assert _first_event_is_error(
        {"type": "result", "session_id": "synthetic", "is_error": True}
    ) is True


def test_first_event_is_error_false() -> None:
    assert _first_event_is_error(
        {"type": "system", "session_id": "real", "is_error": False}
    ) is False


def test_first_event_is_error_missing_field_treated_as_not_error() -> None:
    """Older CC versions / non-result first events may omit ``is_error``."""
    assert _first_event_is_error({"type": "system", "session_id": "x"}) is False


def test_first_event_is_error_none_input() -> None:
    assert _first_event_is_error(None) is False


# ---------------------------------------------------------------------------
# Integration: missing session_id → failed with reason
# ---------------------------------------------------------------------------


async def test_missing_session_id_marks_row_failed(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: session_id_subagent NULL on row → failed with
    "lost session_id across restart"."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path, delegation_id, session_id=None
    )

    result = await monitor_delegation(
        delegation_id,
        poll_interval_s=0.010,
        db_path=dbos_settings.paths.db_path,
    )
    assert result.status == STATUS_FAILED
    assert result.summary == RESUME_REASON_LOST_SESSION_ID

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    assert row["summary"] == RESUME_REASON_LOST_SESSION_ID
    assert row["session_id_subagent"] is None


# ---------------------------------------------------------------------------
# Integration: resume — fake claude --resume that emits clean events + exits
# ---------------------------------------------------------------------------


_FAKE_CLAUDE_RESUME_HAPPY = """\
#!/usr/bin/env python3
import json, sys, time
# Mimic resumed CC: emit a system/init-like event carrying the existing
# session_id, then a clean result event, then exit 0.
print(json.dumps({
    "type": "system",
    "subtype": "init",
    "session_id": "RESUMED-SID",
    "is_error": False,
}), flush=True)
time.sleep(0.02)
print(json.dumps({
    "type": "result",
    "session_id": "RESUMED-SID",
    "is_error": False,
    "subtype": "success",
}), flush=True)
sys.exit(0)
"""


_FAKE_CLAUDE_RESUME_IS_ERROR = """\
#!/usr/bin/env python3
import json, sys
# Mimic `claude --resume <unknown>`: plain-text error line then a result
# event with is_error=true and a NEW synthetic session_id (per S-3 §4.2).
sys.stdout.write("No conversation found with session ID: bogus\\n")
sys.stdout.flush()
print(json.dumps({
    "type": "result",
    "session_id": "SYNTHETIC-SID",
    "is_error": True,
    "subtype": "error_during_execution",
}), flush=True)
sys.exit(1)
"""


def _write_fake_claude(tmp_path: Path, name: str, script: str) -> Path:
    fake = tmp_path / name
    fake.write_text(script)
    fake.chmod(0o755)
    return fake


async def test_resume_spawns_with_session_id_and_completes(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Workflow re-entry: registry empty + row has session_id → resume-spawn
    via `claude --resume <session_id_subagent>`, drain, complete normally."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        session_id="ORIGINAL-SID",
        workspace=str(workspace),
    )

    fake = _write_fake_claude(
        tmp_path, "fake_claude_resume_happy", _FAKE_CLAUDE_RESUME_HAPPY
    )

    captured_argv: list[list[str]] = []
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        captured_argv.append(list(args))
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos_settings.paths.db_path,
            claude_binary="claude",
        )

    # Verify the resume argv was used.
    assert len(captured_argv) == 1
    argv = captured_argv[0]
    assert argv[0] == "claude"
    assert "--resume" in argv
    assert argv[argv.index("--resume") + 1] == "ORIGINAL-SID"
    assert "stream-json" in argv
    assert "--verbose" in argv
    # No -p / no prompt on resume.
    assert "-p" not in argv

    # Workflow concluded normally via the existing monitor path.
    assert result.status == STATUS_COMPLETED
    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_COMPLETED
    # Original session_id is preserved.
    assert row["session_id_subagent"] == "ORIGINAL-SID"


# ---------------------------------------------------------------------------
# Integration: resume emits is_error=true → refuse to overwrite session_id
# ---------------------------------------------------------------------------


async def test_resume_is_error_refuses_to_overwrite_session_id(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """`claude --resume <unknown>` → is_error=true on first event.
    Parser refuses to overwrite session_id_subagent; row marked failed."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        session_id="ORIGINAL-SID",
        workspace=str(workspace),
    )

    fake = _write_fake_claude(
        tmp_path, "fake_claude_resume_iserror", _FAKE_CLAUDE_RESUME_IS_ERROR
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos_settings.paths.db_path,
            claude_binary="claude",
        )

    # The row must be marked failed with the documented reason.
    assert result.status == STATUS_FAILED
    assert result.summary == RESUME_REASON_FAILED

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    assert row["summary"] == RESUME_REASON_FAILED
    # Critical contract: session_id was NOT overwritten by the synthetic id.
    assert row["session_id_subagent"] == "ORIGINAL-SID"


# ---------------------------------------------------------------------------
# Integration: idempotency — re-entering twice spawns at most once
# ---------------------------------------------------------------------------


async def test_resume_idempotent_within_one_process_lifetime(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Re-entering the workflow twice within a single process must spawn at
    most once. After the first successful resume registers a handle, the
    second invocation observes the live handle and the registry guard
    short-circuits the spawn step."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        session_id="ORIGINAL-SID",
        workspace=str(workspace),
    )

    # Directly drive the spawn step twice in a row. The first call should
    # spawn; the second must skip because the registry now has a handle.
    fake = _write_fake_claude(
        tmp_path, "fake_claude_resume_happy", _FAKE_CLAUDE_RESUME_HAPPY
    )

    spawn_count = 0
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        nonlocal spawn_count
        spawn_count += 1
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    db_path_str = str(dbos_settings.paths.db_path)
    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        first = await _resume_spawn_step(
            delegation_id, db_path_str, "claude"
        )
        # Capture the live handle before the second call so we can keep it
        # alive — _resume_spawn_step's registry guard short-circuits when a
        # handle is already registered.
        assert _DELEGATION_REGISTRY.get(delegation_id) is not None
        second = await _resume_spawn_step(
            delegation_id, db_path_str, "claude"
        )

    assert first["action"] == "spawned"
    assert second["action"] == "skipped_registered"
    assert spawn_count == 1, (
        f"spawn step ran {spawn_count} times; idempotency violated"
    )


# ---------------------------------------------------------------------------
# Integration: row already terminal → derive status without re-spawning
# ---------------------------------------------------------------------------


async def test_already_terminal_row_does_not_respawn(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Edge case: subprocess already terminated before workflow re-entry
    (row status is terminal). Derive status from the row and skip spawning."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        session_id="ORIGINAL-SID",
        status=STATUS_COMPLETED,
    )

    spawn_count = 0
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        nonlocal spawn_count
        spawn_count += 1
        return await real_exec(*args, **kwargs)

    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos_settings.paths.db_path,
        )

    assert result.status == STATUS_COMPLETED
    assert spawn_count == 0, (
        f"resume spawned {spawn_count} times for an already-terminal row"
    )


# ---------------------------------------------------------------------------
# Acceptance: registered handle path is unchanged (Task #30 invariant)
# ---------------------------------------------------------------------------


class _FakeStream:
    """Minimal asyncio.StreamReader stand-in (mirrors Task #30 test helper)."""

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
        if not self._buf and self._eof.is_set():
            return b""
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


class _FakeProcess:
    def __init__(self) -> None:
        self.stdout = _FakeStream()
        self.stderr = _FakeStream()
        self.returncode: int | None = None

    def set_returncode(self, rc: int) -> None:
        self.returncode = rc

    def close_streams(self) -> None:
        self.stdout.close()
        self.stderr.close()

    async def wait(self) -> int:
        while self.returncode is None:
            await asyncio.sleep(0.005)
        return self.returncode


async def test_registered_handle_path_skips_resume(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Sanity: when the registry already has a handle (the happy-path single-
    process case), monitor_delegation must NOT invoke the resume step."""
    from capo.tools._subprocess import start_reader

    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        session_id="ORIGINAL-SID",
    )

    process = _FakeProcess()
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

        spawn_count = 0
        real_exec = asyncio.create_subprocess_exec

        async def hijack(*args: Any, **kwargs: Any) -> Any:
            nonlocal spawn_count
            spawn_count += 1
            return await real_exec(*args, **kwargs)

        async def _close_after_delay() -> None:
            await asyncio.sleep(0.05)
            process.set_returncode(0)
            process.close_streams()

        closer = asyncio.create_task(_close_after_delay())
        with patch(
            "capo.workflows.delegation.asyncio.create_subprocess_exec",
            side_effect=hijack,
        ):
            try:
                result = await monitor_delegation(
                    delegation_id,
                    poll_interval_s=0.010,
                    db_path=dbos_settings.paths.db_path,
                )
            finally:
                await closer

        assert result.status == STATUS_COMPLETED
        assert spawn_count == 0, (
            "resume step spawned when registry already had a handle"
        )
    finally:
        conn.close()

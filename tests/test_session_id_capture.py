"""Tests for the session-id capture path (Task #23, spec §5.3 / spike S-3).

Covers acceptance criteria:

* **Functional**: First CC JSON event's top-level ``session_id`` is captured
  and UPDATEd onto the matching ``delegations`` row via the
  ``begin_immediate_with_retry`` write path. Monitor proceeds only AFTER
  the session_id is durable.
* **Edge**: When the first stdout line lacks ``session_id`` (or carries it
  as a non-string) the parser keeps waiting; a later valid event wins.
* **Edge / error**: A malformed first JSON line is logged but does not
  abort capture — the next valid event still captures cleanly.
* **Timeout**: If no JSON event carrying ``session_id`` arrives within
  ``SESSION_ID_CAPTURE_TIMEOUT_S``, the row transitions to ``failed``
  with a reason mentioning the timeout, the subprocess is killed, and
  no orphan rows remain.
* **Happy path integration**: fake claude script emits one
  ``session_id``-carrying event and exits; row ends ``completed`` with
  ``session_id_subagent`` populated.

The fake-claude pattern is the one Task #22 established (`_FAKE_CLAUDE_SCRIPT`
in `tests/test_delegate_to_claude_code.py`); we reuse it shape-wise and
specialise per-test.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools._subprocess import ReaderHandle, start_reader
from capo.tools.claude_code import (
    SESSION_ID_CAPTURE_TIMEOUT_S,
    ClaudeCodeBrief,
    SessionIdCaptureTimeout,
    _await_session_id,
    _extract_session_id,
    delegate_to_claude_code,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    register_amc_sender,
)

# ---------------------------------------------------------------------------
# Scaffolding — config + on-disk roots + state.db with §7.3 schema.
# Mirrors tests/test_delegate_to_claude_code.py so the two test files agree
# on schema and fixture behaviour.
# ---------------------------------------------------------------------------

_VALID_TOML_TMPL = """\
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "{workspaces_root}"
projects_root   = "{projects_root}"
db_path         = "{db_path}"
dbos_db_path    = "{dbos_db_path}"

[soul]
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8080"
agent_id              = "capo"
listen_host           = "127.0.0.1"
listen_port           = 8090
max_boot_wait_seconds = 60

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[retention]
delegation_output_days = 7

[compaction]
threshold_tokens = 100000
preserve_delegation_handles = true

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

[users.owner]
amc_senders = ["+15551234567"]
display_name = "Owner"

[observability]
logfire_enabled = true
"""

_VALID_ENV = {
    "AMC_WEBHOOK_SECRET": "shhh-webhook",
    "AMC_BEARER_TOKEN": "shhh-bearer",
}


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


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state():
    """DBOS is a process-level singleton; clean teardown between tests."""
    import contextlib as _contextlib

    with _contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    yield
    with _contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    with _contextlib.suppress(Exception):
        register_amc_sender(None)
    if is_launched():
        with _contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()

    db_path = tmp_path / "state.db"
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(db_path),
            dbos_db_path=str(tmp_path / "dbos.db"),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))

    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
    finally:
        conn.close()

    # Task #35: delegate_to_claude_code now hands off to DBOS
    # monitor_delegation. Launch DBOS here so integration tests that
    # drive the spawn site don't trip the DBOSNotLaunchedError guard.
    init_dbos(settings)
    launch_dbos()

    return settings, projects_root, workspaces_root


@pytest.fixture
def git_repo_in_projects(scaffold: tuple[Settings, Path, Path]) -> Path:
    _, projects_root, _ = scaffold
    repo = projects_root / "repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)
    return repo


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def make_deps(
    scaffold: tuple[Settings, Path, Path],
    http_client: httpx.AsyncClient,
):
    settings, _, _ = scaffold

    def _make(
        *, thread_id: str | None = "amc:test-channel", user_id: str = "owner"
    ) -> CapoDeps:
        return CapoDeps.from_settings(
            settings=settings,
            http_client=http_client,
            user_id=user_id,
            thread_id=thread_id,
        )

    return _make


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


def _rows(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM delegations")]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Real S-3 sample loader — exercises the canonical field path against the
# committed sample data.
# ---------------------------------------------------------------------------


_S3_SUCCESS_SAMPLE = (
    Path(__file__).resolve().parents[1]
    / "internal"
    / "specs"
    / "spikes"
    / "S-3-samples"
    / "sample-v2.1.138-success-stream-json.jsonl"
)


def _load_first_s3_event() -> dict[str, Any]:
    """Read the first JSON line out of the v2.1.138 success sample."""
    text = _S3_SUCCESS_SAMPLE.read_text(encoding="utf-8")
    for raw in text.splitlines():
        line = raw.strip()
        if not line or not line.startswith("{"):
            continue
        return json.loads(line)
    raise AssertionError("S-3 sample contained no JSON lines")


# ---------------------------------------------------------------------------
# Unit: parser extracts session_id from the real S-3 sample.
# ---------------------------------------------------------------------------


def test_extract_session_id_from_real_s3_sample() -> None:
    """Spike S-3 §4.1: top-level ``session_id`` on every event.

    Drive the extractor against the actual committed sample (the same data
    operators reviewed during the spike). The first event in the v2.1.138
    success sample is a ``system / hook_started`` carrying a UUID.
    """
    event = _load_first_s3_event()
    sid = _extract_session_id(event)
    # The recorded session_id is a UUID; assert exactly the recorded value
    # so a sample change forces a deliberate test update.
    assert sid == "1a2d4a1e-944f-45a6-9e60-15241646098d"
    assert event["type"] == "system"


def test_extract_session_id_rejects_missing_or_non_string() -> None:
    """Returns ``None`` when ``session_id`` is missing, empty, or wrong type."""
    assert _extract_session_id({"type": "system"}) is None
    assert _extract_session_id({"type": "system", "session_id": ""}) is None
    assert _extract_session_id({"type": "system", "session_id": None}) is None
    assert _extract_session_id({"type": "system", "session_id": 42}) is None


# ---------------------------------------------------------------------------
# Reader-driven fakes for _await_session_id behaviour tests.
# ---------------------------------------------------------------------------


class _ScriptedReader:
    """Async reader that yields a scripted list of ``bytes``, then EOF."""

    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        if not self._lines:
            return b""
        await asyncio.sleep(0)  # yield to the event loop
        return self._lines.pop(0)

    async def read(self, n: int) -> bytes:
        return b""


class _EmptyStream:
    async def readline(self) -> bytes:
        return b""

    async def read(self, n: int) -> bytes:
        return b""


class _ScriptedProc:
    """Duck-typed asyncio.subprocess.Process for reader-driven tests."""

    def __init__(self, stdout_lines: list[bytes], stderr_lines: list[bytes] | None = None) -> None:
        self.stdout = _ScriptedReader(stdout_lines)
        self.stderr = _ScriptedReader(stderr_lines or [])
        self.pid = 9999
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        self.returncode = 0
        return 0


def _init_db(tmp_path: Path) -> Path:
    db = tmp_path / "state.db"
    conn = open_connection(db)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
    finally:
        conn.close()
    return db


def _seed_running_row(db: Path, delegation_id: str) -> None:
    conn = open_connection(db)
    try:
        conn.execute(
            "INSERT INTO delegations ("
            " id, user_id, agent, workspace, brief_json, status"
            ") VALUES (?, 'owner', 'claude-code', '/tmp/ws', '{}', 'running')",
            (delegation_id,),
        )
        conn.commit()
    finally:
        conn.close()


def _session_id_of(db: Path, delegation_id: str) -> str | None:
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT session_id_subagent FROM delegations WHERE id = ?",
            (delegation_id,),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row[0]


# ---------------------------------------------------------------------------
# Unit: _await_session_id captures the first event's session_id and UPDATEs.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_session_id_captures_first_event(tmp_path: Path) -> None:
    """The reader's first JSON event with a session_id is what gets persisted."""
    db = _init_db(tmp_path)
    delegation_id = "deleg-first"
    _seed_running_row(db, delegation_id)

    event = {
        "type": "system",
        "subtype": "init",
        "session_id": "11111111-2222-3333-4444-555555555555",
        "claude_code_version": "2.1.138",
    }
    proc = _ScriptedProc([json.dumps(event).encode() + b"\n"])

    reader_conn = open_connection(db)
    try:
        handle = start_reader(delegation_id, proc, store=reader_conn)
        try:
            sid = await _await_session_id(
                handle, delegation_id, db_path=db, timeout_s=2.0
            )
        finally:
            handle.cancel()
            await handle.wait()
    finally:
        reader_conn.close()

    assert sid == "11111111-2222-3333-4444-555555555555"
    assert _session_id_of(db, delegation_id) == sid


# ---------------------------------------------------------------------------
# Edge: first event lacks session_id; a later one has it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_session_id_skips_event_without_session_id(
    tmp_path: Path,
) -> None:
    """An event missing ``session_id`` does not trip the hook — we wait."""
    db = _init_db(tmp_path)
    delegation_id = "deleg-skip"
    _seed_running_row(db, delegation_id)

    # First event has no session_id (still valid JSON though); second has it.
    lines = [
        json.dumps({"type": "preamble"}).encode() + b"\n",
        json.dumps(
            {
                "type": "system",
                "subtype": "hook_started",
                "session_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            }
        ).encode()
        + b"\n",
    ]
    proc = _ScriptedProc(lines)

    reader_conn = open_connection(db)
    try:
        handle = start_reader(delegation_id, proc, store=reader_conn)
        try:
            sid = await _await_session_id(
                handle, delegation_id, db_path=db, timeout_s=2.0
            )
        finally:
            handle.cancel()
            await handle.wait()
    finally:
        reader_conn.close()

    assert sid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert _session_id_of(db, delegation_id) == sid


# ---------------------------------------------------------------------------
# Edge: malformed first line is logged but does not abort capture.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_session_id_tolerates_malformed_first_line(
    tmp_path: Path,
) -> None:
    """A malformed JSON line increments ``parse_errors`` but the reader
    keeps emitting; the next valid event still captures cleanly.
    """
    db = _init_db(tmp_path)
    delegation_id = "deleg-malformed"
    _seed_running_row(db, delegation_id)

    lines = [
        # Starts with `{` so we attempt to parse it, then it explodes.
        b'{"type": "system", oops not json\n',
        json.dumps(
            {
                "type": "system",
                "subtype": "init",
                "session_id": "deadbeef-0000-0000-0000-000000000000",
            }
        ).encode()
        + b"\n",
    ]
    proc = _ScriptedProc(lines)

    reader_conn = open_connection(db)
    try:
        handle = start_reader(delegation_id, proc, store=reader_conn)
        try:
            sid = await _await_session_id(
                handle, delegation_id, db_path=db, timeout_s=2.0
            )
        finally:
            handle.cancel()
            await handle.wait()
        # The malformed line was counted as a parse error.
        assert handle.stats.parse_errors >= 1
    finally:
        reader_conn.close()

    assert sid == "deadbeef-0000-0000-0000-000000000000"
    assert _session_id_of(db, delegation_id) == sid


# ---------------------------------------------------------------------------
# Edge: plain-text preamble (resume-failure case from S-3 §4.2) is tolerated.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_session_id_tolerates_plain_text_preamble(
    tmp_path: Path,
) -> None:
    """Spike S-3 §4.2 preamble case: non-JSON lines must not abort capture."""
    db = _init_db(tmp_path)
    delegation_id = "deleg-preamble"
    _seed_running_row(db, delegation_id)

    lines = [
        b"No conversation found with session ID: bogus\n",
        b"Some unrelated note\n",
        json.dumps(
            {
                "type": "system",
                "session_id": "feedface-0000-0000-0000-000000000123",
            }
        ).encode()
        + b"\n",
    ]
    proc = _ScriptedProc(lines)

    reader_conn = open_connection(db)
    try:
        handle = start_reader(delegation_id, proc, store=reader_conn)
        try:
            sid = await _await_session_id(
                handle, delegation_id, db_path=db, timeout_s=2.0
            )
        finally:
            handle.cancel()
            await handle.wait()
    finally:
        reader_conn.close()

    assert sid == "feedface-0000-0000-0000-000000000123"


# ---------------------------------------------------------------------------
# Timeout: no session_id event arrives in time → SessionIdCaptureTimeout.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_session_id_timeout(tmp_path: Path) -> None:
    """No session_id-bearing event before deadline → SessionIdCaptureTimeout.

    Uses a reader that emits an event *without* ``session_id`` and then
    stalls — the hook never fires, and ``_await_session_id`` raises.
    """
    db = _init_db(tmp_path)
    delegation_id = "deleg-timeout"
    _seed_running_row(db, delegation_id)

    class _OneNonSessionEventReader:
        def __init__(self) -> None:
            self._emitted = False
            self._stall = asyncio.Event()  # never set → readline blocks

        async def readline(self) -> bytes:
            if not self._emitted:
                self._emitted = True
                return b'{"type":"system"}\n'
            # Block indefinitely until the test's timeout fires.
            await self._stall.wait()
            return b""

        async def read(self, n: int) -> bytes:
            return b""

    proc = _ScriptedProc([])
    proc.stdout = _OneNonSessionEventReader()

    reader_conn = open_connection(db)
    try:
        handle = start_reader(delegation_id, proc, store=reader_conn)
        try:
            with pytest.raises(SessionIdCaptureTimeout):
                await _await_session_id(
                    handle, delegation_id, db_path=db, timeout_s=0.25
                )
        finally:
            handle.cancel()
            await handle.wait()
    finally:
        reader_conn.close()

    # Session-id was never persisted.
    assert _session_id_of(db, delegation_id) is None


# ---------------------------------------------------------------------------
# Integration: timeout end-to-end via a fake claude that never emits session_id.
# ---------------------------------------------------------------------------


_FAKE_CLAUDE_NO_SESSION_ID = """\
#!/usr/bin/env python3
import json, sys, time
# Emit non-session-id events forever until killed. Each event is JSON-parseable
# but has no ``session_id`` field, so the reader's hook never fires.
i = 0
while True:
    print(json.dumps({"type": "noise", "i": i}), flush=True)
    i += 1
    time.sleep(0.05)
"""


@pytest.mark.asyncio
async def test_integration_timeout_marks_row_failed_and_kills_subprocess(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """A real subprocess that never emits ``session_id`` is killed and the
    row transitions to ``failed`` with a timeout reason. No orphan rows.
    """
    settings, _, _ = scaffold
    fake_claude = tmp_path / "fake_claude_noise"
    fake_claude.write_text(_FAKE_CLAUDE_NO_SESSION_ID)
    fake_claude.chmod(0o755)

    deps = make_deps(thread_id="amc:timeout-channel")
    object.__setattr__(deps.settings.agents.claude_code, "binary", sys.executable)

    brief = ClaudeCodeBrief(
        goal="never emit session_id",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="anthropic:claude-sonnet-4-6",
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake_claude),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    # Patch the timeout to a small value so the test runs fast. Keep the
    # patch alive across the polling loop — the monitor task is launched
    # in the background and reads SESSION_ID_CAPTURE_TIMEOUT_S at the moment
    # it awaits _await_session_id, not at delegate_to_claude_code call time.
    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ), patch(
        "capo.tools.claude_code.SESSION_ID_CAPTURE_TIMEOUT_S", 0.5
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

        # Wait for the monitor wrapper to fire timeout + mark failed.
        deadline = asyncio.get_event_loop().time() + 8.0
        final_row: dict[str, Any] | None = None
        while asyncio.get_event_loop().time() < deadline:
            rows = _rows(settings.paths.db_path)
            assert len(rows) == 1, f"orphan rows detected: {rows}"
            if rows[0]["status"] == "failed":
                final_row = rows[0]
                break
            await asyncio.sleep(0.05)

    assert final_row is not None, "row never transitioned to failed"
    assert final_row["id"] == handle.delegation_id
    assert final_row["session_id_subagent"] is None
    # Reason mentions the timeout per §5.3 acceptance criterion.
    assert "session_id" in (final_row["summary"] or "")
    assert re.search(r"within\s+[\d.]+s", final_row["summary"] or "")
    # No orphan rows — exactly one row, and it's the failed one.
    assert len(_rows(settings.paths.db_path)) == 1


# ---------------------------------------------------------------------------
# Integration: happy path — fake claude emits one session_id event then exits.
# ---------------------------------------------------------------------------


_FAKE_CLAUDE_ONE_EVENT_THEN_EXIT = """\
#!/usr/bin/env python3
import json, sys, time
event = {
    "type": "system",
    "subtype": "init",
    "session_id": "abcd1234-0000-4000-8000-000000000001",
    "claude_code_version": "2.1.138",
}
print(json.dumps(event), flush=True)
# Tiny pause so the reader's batched flush definitely picks the event up.
time.sleep(0.05)
final = {
    "type": "result",
    "session_id": "abcd1234-0000-4000-8000-000000000001",
    "is_error": False,
    "subtype": "success",
}
print(json.dumps(final), flush=True)
sys.exit(0)
"""


@pytest.mark.asyncio
async def test_integration_happy_path_session_id_persisted_and_status_completed(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """End-to-end: subprocess emits session_id, exits 0, row ends ``completed``
    with ``session_id_subagent`` populated."""
    settings, _, _ = scaffold
    fake_claude = tmp_path / "fake_claude_happy"
    fake_claude.write_text(_FAKE_CLAUDE_ONE_EVENT_THEN_EXIT)
    fake_claude.chmod(0o755)

    deps = make_deps(thread_id="amc:happy-channel")
    object.__setattr__(deps.settings.agents.claude_code, "binary", sys.executable)

    brief = ClaudeCodeBrief(
        goal="emit one session_id event",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
        model="anthropic:claude-sonnet-4-6",
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake_claude),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    # Persistence-before-yield contract: row exists immediately.
    rows = _rows(settings.paths.db_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == handle.delegation_id
    assert row["status"] == "running"
    # session_id may already be set (the monitor is concurrent) — that's fine.

    # Wait up to ~5s for the monitor to run session-id capture + exit.
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        rows = _rows(settings.paths.db_path)
        if rows[0]["status"] == "completed":
            break
        await asyncio.sleep(0.05)

    rows = _rows(settings.paths.db_path)
    assert rows[0]["status"] == "completed", (
        f"row never transitioned to completed; row={rows[0]}"
    )
    assert rows[0]["session_id_subagent"] == "abcd1234-0000-4000-8000-000000000001"
    assert rows[0]["ended_at"] is not None


# ---------------------------------------------------------------------------
# Constant sanity check — caller can rely on the documented default.
# ---------------------------------------------------------------------------


def test_session_id_capture_timeout_constant_matches_spec() -> None:
    """Spike S-3 §4.3 cites a 30 s default; assert the constant matches."""
    assert SESSION_ID_CAPTURE_TIMEOUT_S == 30.0


# ---------------------------------------------------------------------------
# Hook attribute presence — additive API must not break existing callers.
# ---------------------------------------------------------------------------


def test_reader_handle_exposes_session_id_hook(tmp_path: Path) -> None:
    """``ReaderHandle`` carries ``first_event`` + ``first_event_received``.

    This is the additive contract the monitor wrapper depends on. We don't
    drive the reader here — just assert the attributes exist with the
    right initial state on a freshly constructed handle.
    """
    db = _init_db(tmp_path)
    delegation_id = "deleg-hook"
    _seed_running_row(db, delegation_id)

    async def _check() -> None:
        proc = _ScriptedProc([])  # EOF immediately
        conn = open_connection(db)
        try:
            handle = start_reader(delegation_id, proc, store=conn)
            try:
                assert isinstance(handle, ReaderHandle)
                assert isinstance(handle.first_event_received, asyncio.Event)
                assert not handle.first_event_received.is_set()
                assert handle.first_event is None
            finally:
                handle.cancel()
                await handle.wait()
        finally:
            conn.close()

    asyncio.run(_check())

"""Phase 2 checkpoint integration tests (spec §9.2).

This module anchors the §9.2 checkpoint gate criteria that cross-cut
multiple Phase 2 deliverables. Most criteria are already covered by the
per-task unit suites (#19-#26) — this file fills the gaps the gate
explicitly enumerates and proves the pieces compose end-to-end:

* **Chatty CC (>10 MiB stdout) end-to-end through ``start_reader``**
  with a mix of JSON event lines and plain-text preamble lines. The
  reader must drain continuously (the OS pipe buffer is ~64 KB on
  macOS, so any stall blocks the child) and every byte must land in
  ``delegation_output``. (§6.1 + S-3 risk.)

* **Status accuracy: running** — a long-lived fake CC that emits one
  session_id event then idles. ``check_delegation_status`` must report
  ``status='running'`` with a positive ``runtime_seconds`` and a
  recent ``last_activity_ts``.

* **Status accuracy: completed** — a fake CC that exits cleanly after
  emitting one event. The §5.6 monitor (currently in-process) must
  flip the row to ``status='completed'`` and ``check_delegation_status``
  must see it.

* **Kill forced path** — spawn the long-lived fake CC; call
  ``_kill_delegation_forced``; assert the row flips to ``status='killed'``
  with the caller's ``reason`` persisted, and the OS-level child
  process is gone.

* **`shell_exec` smoke** — run a few allowlisted commands end-to-end
  through the registered tool function to prove §6.2 still works
  after the §5.5 wave landed.

The chatty test uses a fake "claude" binary similar to Task #22's
``_FAKE_CLAUDE_SCRIPT`` so we don't depend on a real CC install.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import subprocess
import sys
import time
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
from capo.tools._subprocess import start_reader
from capo.tools.basic import ShellResult, shell_exec
from capo.tools.claude_code import (
    ClaudeCodeBrief,
    delegate_to_claude_code,
)
from capo.tools.delegations import (
    _kill_delegation_forced,
    check_delegation_status,
)

# ---------------------------------------------------------------------------
# Scaffolding — mirrors tests/test_delegate_to_claude_code.py.
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

    return settings, projects_root, workspaces_root


@pytest.fixture
def git_repo_in_projects(scaffold: tuple[Settings, Path, Path]) -> Path:
    """Create a real git repo with a ``main`` branch inside projects_root."""
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


def _row(db_path: Path, delegation_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        )
        r = cur.fetchone()
        return dict(r) if r else {}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fake CC scripts.
# ---------------------------------------------------------------------------


# Long-lived: emit one session_id event then idle until killed. Used for
# the status='running' and kill-forced tests. The unbuffered prints make
# the event arrive at the reader immediately so the status query is
# meaningful even on slow CI.
_FAKE_CLAUDE_LONG_LIVED = """\
#!/usr/bin/env python3
import json, sys, time
print(json.dumps({
    "type": "system",
    "subtype": "init",
    "session_id": "deadbeef-0000-0000-0000-000000000001",
}), flush=True)
# Idle indefinitely. The test will SIGTERM / SIGKILL us.
while True:
    time.sleep(0.5)
"""


# Short-lived: emit two events then exit cleanly. Used for status=
# 'completed' verification (the §5.6 monitor flips the row).
_FAKE_CLAUDE_QUICK = """\
#!/usr/bin/env python3
import json, sys, time
print(json.dumps({
    "type": "system",
    "subtype": "init",
    "session_id": "deadbeef-0000-0000-0000-000000000002",
}), flush=True)
time.sleep(0.05)
print(json.dumps({
    "type": "result",
    "session_id": "deadbeef-0000-0000-0000-000000000002",
    "is_error": False,
    "subtype": "success",
}), flush=True)
sys.exit(0)
"""


# Chatty: emit a deterministic mix of JSON event lines and plain stdout
# preamble lines totalling >10 MiB. Mirrors S-3 §4.2 (CC sometimes prints
# unstructured preamble before the first JSON event).
#
# Layout per "batch":
#   * 1 plain-text preamble line   (~200 KiB of 'p')
#   * 1 small JSON event line      (~under 1 KiB)
#   * 1 large plain-stdout line    (~250 KiB of 'x')
# Repeating the batch 18 times yields ~18 * (200 KiB + 1 KiB + 250 KiB)
# ~= 8.1 MiB. We instead use 25 batches with bigger lines so we
# comfortably clear the 10 MiB §9.2 threshold.
_FAKE_CLAUDE_CHATTY = """\
#!/usr/bin/env python3
import json, sys
preamble = 'p' * (200 * 1024) + '\\n'
big = 'x' * (250 * 1024) + '\\n'
event_line = json.dumps({
    "type": "system",
    "subtype": "init",
    "session_id": "deadbeef-0000-0000-0000-000000000003",
}) + '\\n'
# 25 batches => ~ 25 * (200 + 250) KiB + tiny events => ~ 11.25 MiB.
for _ in range(25):
    sys.stdout.write(preamble)
    sys.stdout.write(event_line)
    sys.stdout.write(big)
sys.stdout.flush()
sys.exit(0)
"""


def _write_fake_claude(tmp_path: Path, name: str, script: str) -> Path:
    path = tmp_path / name
    path.write_text(script)
    path.chmod(0o755)
    return path


# ---------------------------------------------------------------------------
# 1. Chatty-CC integration: >10 MiB stdout, mixed plain + JSON, no pipe block.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chatty_cc_mixed_streams_over_10mib(tmp_path: Path) -> None:
    """§9.2 + §6.1 + S-3: chatty CC run with >10 MiB mixed stdout completes.

    The reader must drain continuously — the macOS pipe buffer is ~64 KB,
    so any stall blocks the child on its next ``write``. We wall-clock
    the whole ingestion at 60s so a regression manifests as a clear
    ``TimeoutError`` rather than a hang.

    JSON event lines are classified as ``stream='event'``; plain-text
    preamble + big lines fall back to ``stream='stdout'`` (S-3 §4.2).
    Every byte the child wrote must land in ``delegation_output``.
    """
    fake = _write_fake_claude(
        tmp_path, "fake_chatty.py", _FAKE_CLAUDE_CHATTY
    )

    # Expected stdout byte count — preamble + big lines are passed through
    # verbatim by the reader (only JSON event lines are re-serialized).
    preamble_size = 200 * 1024 + 1  # +1 for '\n'
    big_size = 250 * 1024 + 1
    batch_count = 25
    expected_stdout_bytes = (preamble_size + big_size) * batch_count
    expected_event_rows = batch_count
    assert expected_stdout_bytes > 10 * 1024 * 1024, (
        f"chatty test misconfigured; only emits {expected_stdout_bytes} "
        f"stdout bytes"
    )

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(fake),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2 * 1024 * 1024,
    )

    conn = open_connection(tmp_path / "state.db")
    conn.execute(_DELEGATION_OUTPUT_DDL)
    try:
        handle = start_reader("deleg-chatty", process, store=conn)
        # Wall-clock budget: 60s. A regression that re-introduces pipe
        # blocking will time out here instead of hanging the suite.
        await asyncio.wait_for(handle.wait(), timeout=60.0)
        rc = await process.wait()
        assert rc == 0

        cur = conn.execute(
            """
            SELECT stream, COUNT(*), COALESCE(SUM(LENGTH(chunk)), 0)
            FROM delegation_output
            WHERE delegation_id = ?
            GROUP BY stream
            """,
            ("deleg-chatty",),
        )
        per_stream_count: dict[str, int] = {}
        per_stream_bytes: dict[str, int] = {}
        for stream, count, total_bytes in cur.fetchall():
            per_stream_count[stream] = count
            per_stream_bytes[stream] = total_bytes

        # JSON event rows: one per batch — re-serialized to JSON so the
        # exact byte count varies, but the row count is deterministic.
        assert per_stream_count.get("event", 0) == expected_event_rows, (
            f"event row count mismatch: got {per_stream_count.get('event')}, "
            f"expected {expected_event_rows}"
        )
        # Plain preamble + big lines pass through verbatim — byte-exact.
        assert per_stream_bytes.get("stdout", 0) == expected_stdout_bytes, (
            f"stdout bytes mismatch: got {per_stream_bytes.get('stdout')}, "
            f"expected {expected_stdout_bytes}"
        )

        # Stats agree with the DB on row counts. ``stdout_bytes`` in
        # ReaderStats counts EVERY byte read from the stdout pipe
        # (before classifying JSON lines as 'event'), so it sums to
        # ``expected_stdout_bytes + raw event-line bytes``.
        assert handle.stats.eof_reached is True
        assert handle.stats.event_rows == expected_event_rows
        assert handle.stats.stdout_rows == batch_count * 2  # preamble + big
        # Sanity: every byte the pipe yielded was accounted for.
        assert handle.stats.stdout_bytes > expected_stdout_bytes
        assert handle.stats.stdout_bytes > 10 * 1024 * 1024
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        conn.close()


# ---------------------------------------------------------------------------
# 2. Status accuracy: running + completed.
# ---------------------------------------------------------------------------


def _hijack_spawn(fake_script: Path):
    """Return a fake ``create_subprocess_exec`` that runs ``fake_script``.

    Mirrors the hijack pattern from
    ``tests/test_delegate_to_claude_code.py::test_integration_end_to_end_with_fake_claude``.
    """
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*_args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake_script),
            cwd=kwargs.get("cwd"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    return hijack


@pytest.mark.asyncio
async def test_status_running_for_long_lived_cc(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """``check_delegation_status`` reports a live CC as ``running``.

    Spawns the long-lived fake CC, waits briefly for the first event to
    be persisted (so ``last_activity_ts`` is populated), and queries
    status. The fake child is then explicitly killed in the cleanup.
    """
    settings, _, _ = scaffold
    fake = _write_fake_claude(
        tmp_path, "fake_long.py", _FAKE_CLAUDE_LONG_LIVED
    )

    deps = make_deps(thread_id="amc:status-running")
    object.__setattr__(
        deps.settings.agents.claude_code, "binary", sys.executable
    )

    brief = ClaudeCodeBrief(
        goal="status running probe",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=_hijack_spawn(fake),
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    try:
        # Wait for the first JSON event to land so last_activity_ts >
        # started_at and runtime_seconds > 0.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            conn = sqlite3.connect(str(settings.paths.db_path))
            try:
                count = conn.execute(
                    "SELECT COUNT(*) FROM delegation_output WHERE delegation_id=?",
                    (handle.delegation_id,),
                ).fetchone()[0]
            finally:
                conn.close()
            if count > 0:
                break
            await asyncio.sleep(0.05)

        snapshot = await check_delegation_status(
            _ctx(deps), handle.delegation_id
        )
        assert snapshot["delegation_id"] == handle.delegation_id
        assert snapshot["status"] == "running", (
            f"expected running, got {snapshot}"
        )
        assert snapshot["runtime_seconds"] is not None
        assert snapshot["runtime_seconds"] >= 0.0
        assert snapshot["last_activity_ts"] is not None
        # ``summary_one_line`` is NULL while running (only the monitor
        # writes it on terminal status).
        assert snapshot["summary_one_line"] is None
    finally:
        # Kill the still-running fake CC so the test process doesn't
        # leave an orphan. Use _kill_delegation_forced to also flip the
        # row — this doubles as a soft-check that subsequent runs of
        # this fixture aren't tripped up by leftover state.
        await _kill_delegation_forced(
            _ctx(deps), handle.delegation_id, "test cleanup"
        )


@pytest.mark.asyncio
async def test_status_completed_after_clean_exit(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """A fake CC that exits cleanly is reported as ``completed``."""
    settings, _, _ = scaffold
    fake = _write_fake_claude(tmp_path, "fake_quick.py", _FAKE_CLAUDE_QUICK)

    deps = make_deps(thread_id="amc:status-completed")
    object.__setattr__(
        deps.settings.agents.claude_code, "binary", sys.executable
    )

    brief = ClaudeCodeBrief(
        goal="status completed probe",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=_hijack_spawn(fake),
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    # Drive the monitor: wait for the row to flip to completed.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        row = _row(settings.paths.db_path, handle.delegation_id)
        if row.get("status") and row["status"] != "running":
            break
        await asyncio.sleep(0.05)

    snapshot = await check_delegation_status(_ctx(deps), handle.delegation_id)
    assert snapshot["status"] == "completed", (
        f"expected completed, got {snapshot}"
    )
    assert snapshot["runtime_seconds"] is not None
    assert snapshot["runtime_seconds"] >= 0.0
    # last_activity_ts is set (the reader recorded at least one event).
    assert snapshot["last_activity_ts"] is not None


# ---------------------------------------------------------------------------
# 3. Kill forced path: live CC -> SIGTERM -> row='killed'.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kill_forced_terminates_live_cc(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """``_kill_delegation_forced`` terminates a real CC and writes the row.

    Spawns the long-lived fake CC via ``delegate_to_claude_code`` (so
    the row is real, the pid is real, and the §5.6 monitor is wired),
    calls ``_kill_delegation_forced`` with a reason, and verifies:

    * the return dict reports ``status='killed'``;
    * the DB row is ``status='killed'``, ``ended_at`` set, and
      ``summary`` carries the caller's reason;
    * the OS-level subprocess is no longer alive.
    """
    settings, _, _ = scaffold
    fake = _write_fake_claude(
        tmp_path, "fake_kill.py", _FAKE_CLAUDE_LONG_LIVED
    )

    deps = make_deps(thread_id="amc:kill-forced")
    object.__setattr__(
        deps.settings.agents.claude_code, "binary", sys.executable
    )

    brief = ClaudeCodeBrief(
        goal="kill probe",
        repo_path=str(git_repo_in_projects),
        create_worktree=True,
    )

    with patch(
        "capo.tools.claude_code.asyncio.create_subprocess_exec",
        side_effect=_hijack_spawn(fake),
    ):
        handle = await delegate_to_claude_code(_ctx(deps), brief)

    # Grab the pid for the post-kill liveness probe.
    pre = _row(settings.paths.db_path, handle.delegation_id)
    pid = pre["pid"]
    assert isinstance(pid, int) and pid > 0

    # Sanity: child is currently alive.
    import os
    import signal

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pytest.fail("fake CC died before we could kill it")

    result = await _kill_delegation_forced(
        _ctx(deps), handle.delegation_id, "phase 2 test"
    )
    assert result["status"] == "killed"
    assert result["delegation_id"] == handle.delegation_id
    assert result["reason"] == "phase 2 test"

    # Row reflects the kill.
    row = _row(settings.paths.db_path, handle.delegation_id)
    assert row["status"] == "killed"
    assert row["ended_at"] is not None
    assert row["summary"] == "phase 2 test"

    # And the OS-level child is gone (or going). Give it a brief grace
    # window — SIGTERM was already sent inside _kill_delegation_forced.
    deadline = time.monotonic() + 5.0
    alive = True
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            alive = False
            break
        await asyncio.sleep(0.05)
    if alive:
        # Last-resort cleanup to avoid leaking a long-lived child if the
        # assertion below would otherwise fail.
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    assert alive is False, (
        f"fake CC pid={pid} still alive after _kill_delegation_forced"
    )


# ---------------------------------------------------------------------------
# 4. shell_exec smoke through the public tool surface.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shell_exec_smoke_allowlisted_commands(
    scaffold: tuple[Settings, Path, Path],
    git_repo_in_projects: Path,
    make_deps: Any,
) -> None:
    """A handful of allowlisted commands run cleanly through ``shell_exec``.

    Calls ``shell_exec`` directly (it's a sync function — the registered
    tool wraps it 1:1, so the function-level invocation exercises the
    same path).
    """
    deps = make_deps()

    # pwd in the git repo.
    pwd_result = shell_exec(_ctx(deps), "pwd", cwd=str(git_repo_in_projects))
    assert isinstance(pwd_result, ShellResult)
    assert pwd_result.exit_code == 0
    assert str(git_repo_in_projects.resolve()) in pwd_result.stdout

    # ls of the repo: README.md must be there.
    ls_result = shell_exec(_ctx(deps), "ls", cwd=str(git_repo_in_projects))
    assert ls_result.exit_code == 0
    assert "README.md" in ls_result.stdout

    # git log -n 1: succeeds because the fixture made one initial commit.
    log_result = shell_exec(
        _ctx(deps), "git log -n 1", cwd=str(git_repo_in_projects)
    )
    assert log_result.exit_code == 0
    assert "initial" in log_result.stdout

"""Tests for the agent-facing delegation tools (Task #24 / spec §5.5).

Covers:

Unit / Functional
* ``check_delegation_status`` happy path for ``running`` and terminal rows.
* ``check_delegation_status`` raises :class:`DelegationNotFound` on unknown id.
* ``get_delegation_output`` returns chronological tail, filters ``stderr``.
* ``get_delegation_output`` clamps ``tail_lines`` at 1000.
* ``get_delegation_output`` returns ``[]`` for unknown delegation_id.
* ``kill_delegation`` always raises :class:`ApprovalRequired` in Phase 2.
* ``_kill_delegation_forced`` happy path UPDATEs the row to ``killed``.
* ``_kill_delegation_forced`` on a terminal row is idempotent.
* ``list_delegations`` scoped to ``user_id``; respects ``status`` filter;
  returns ``[]`` when user has no rows; orders by ``started_at DESC``.

Integration
* ``_kill_delegation_forced`` actually terminates a long-lived child
  subprocess via SIGTERM and writes the terminal row.
"""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.memory.store import open_connection
from capo.tools.basic import ApprovalRequired
from capo.tools.delegations import (
    DEFAULT_TAIL_LINES,
    MAX_TAIL_LINES,
    DelegationNotFound,
    _kill_delegation_forced,
    _utc_now_iso,
    check_delegation_status,
    get_delegation_output,
    kill_delegation,
    list_delegations,
)

# ---------------------------------------------------------------------------
# Scaffolding — mirrors tests/test_delegate_to_claude_code.py so tests
# can stand alone without spinning up Alembic.
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


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path]:
    """Settings + initialized state.db with §7.3 schema."""
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

    return settings, db_path


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def make_deps(
    scaffold: tuple[Settings, Path],
    http_client: httpx.AsyncClient,
):
    settings, _ = scaffold

    def _make(
        *, user_id: str | None = "owner", thread_id: str = "amc:test-channel"
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


# ---------------------------------------------------------------------------
# DB seed helpers.
# ---------------------------------------------------------------------------


def _insert_delegation(
    db_path: Path,
    *,
    delegation_id: str,
    user_id: str = "owner",
    status: str = "running",
    started_at: str | None = None,
    ended_at: str | None = None,
    summary: str | None = None,
    pid: int | None = 99999,
    agent: str = "claude-code",
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
                user_id,
                agent,
                f"/tmp/ws/{delegation_id}",
                pid,
                None,
                "{}",
                status,
                started_at or _utc_now_iso(),
                ended_at,
                summary,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_output(
    db_path: Path,
    *,
    delegation_id: str,
    stream: str,
    chunk: str,
    ts: str | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegation_output (delegation_id, ts, stream, chunk)
            VALUES (?, ?, ?, ?)
            """,
            (delegation_id, ts or _utc_now_iso(), stream, chunk),
        )
        conn.commit()
    finally:
        conn.close()


def _row(db_path: Path, delegation_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
    finally:
        conn.close()
    assert r is not None, f"row {delegation_id!r} not found"
    return dict(r)


# ---------------------------------------------------------------------------
# check_delegation_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_delegation_status_running(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="d1", status="running")

    result = await check_delegation_status(_ctx(make_deps()), "d1")

    assert result["delegation_id"] == "d1"
    assert result["status"] == "running"
    assert result["started_at"] is not None
    # No output rows → last_activity falls back to started_at.
    assert result["last_activity_ts"] == result["started_at"]
    assert result["summary_one_line"] is None
    # Running runtime should be >= 0 and bounded (test runs fast).
    assert result["runtime_seconds"] is not None
    assert 0.0 <= result["runtime_seconds"] < 60.0


@pytest.mark.asyncio
async def test_check_delegation_status_completed(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    # Hand-craft timestamps 5s apart.
    from datetime import UTC, datetime, timedelta

    t0 = datetime.now(UTC)
    started = t0.strftime("%Y-%m-%d %H:%M:%S.%f")
    ended = (t0 + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S.%f")
    _insert_delegation(
        db_path,
        delegation_id="d2",
        status="completed",
        started_at=started,
        ended_at=ended,
        summary="all green",
    )

    result = await check_delegation_status(_ctx(make_deps()), "d2")
    assert result["status"] == "completed"
    assert result["summary_one_line"] == "all green"
    # 5s ±200ms slack for SQLite roundtrip.
    assert 4.5 < (result["runtime_seconds"] or 0.0) < 6.0


@pytest.mark.asyncio
async def test_check_delegation_status_uses_output_max_ts(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="d3", status="running")
    _insert_output(
        db_path,
        delegation_id="d3",
        stream="stdout",
        chunk="hello",
        ts="2026-05-10 12:00:00.000000",
    )
    _insert_output(
        db_path,
        delegation_id="d3",
        stream="event",
        chunk='{"type":"x"}',
        ts="2026-05-10 12:00:05.000000",
    )

    result = await check_delegation_status(_ctx(make_deps()), "d3")
    assert result["last_activity_ts"] == "2026-05-10 12:00:05.000000"


@pytest.mark.asyncio
async def test_check_delegation_status_unknown_raises(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    with pytest.raises(DelegationNotFound) as exc_info:
        await check_delegation_status(_ctx(make_deps()), "nope")
    assert exc_info.value.delegation_id == "nope"


# ---------------------------------------------------------------------------
# get_delegation_output
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_delegation_output_happy_path(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="d4")
    for i in range(5):
        _insert_output(
            db_path,
            delegation_id="d4",
            stream="stdout",
            chunk=f"line-{i}",
            ts=f"2026-05-10 12:00:0{i}.000000",
        )

    out = await get_delegation_output(_ctx(make_deps()), "d4", tail_lines=3)
    assert len(out) == 3
    # Chronological (oldest of the tail first).
    assert [r["chunk"] for r in out] == ["line-2", "line-3", "line-4"]
    for r in out:
        assert set(r.keys()) == {"ts", "stream", "chunk"}


@pytest.mark.asyncio
async def test_get_delegation_output_filters_stderr(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="d5")
    _insert_output(
        db_path,
        delegation_id="d5",
        stream="stdout",
        chunk="out-1",
        ts="2026-05-10 12:00:00.000000",
    )
    _insert_output(
        db_path,
        delegation_id="d5",
        stream="stderr",
        chunk="error-noise",
        ts="2026-05-10 12:00:01.000000",
    )
    _insert_output(
        db_path,
        delegation_id="d5",
        stream="event",
        chunk='{"type":"ok"}',
        ts="2026-05-10 12:00:02.000000",
    )

    out = await get_delegation_output(_ctx(make_deps()), "d5")
    chunks = [r["chunk"] for r in out]
    assert "error-noise" not in chunks
    assert chunks == ["out-1", '{"type":"ok"}']
    assert {r["stream"] for r in out} == {"stdout", "event"}


@pytest.mark.asyncio
async def test_get_delegation_output_caps_at_1000(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="d6")
    # Insert 1200 stdout rows.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executemany(
            """
            INSERT INTO delegation_output (delegation_id, ts, stream, chunk)
            VALUES (?, ?, 'stdout', ?)
            """,
            [
                ("d6", f"2026-05-10 12:00:{i // 60:02d}.{i % 60:06d}", f"c-{i}")
                for i in range(1200)
            ],
        )
        conn.commit()
    finally:
        conn.close()

    # Ask for 5000; must be clamped to MAX_TAIL_LINES.
    out = await get_delegation_output(
        _ctx(make_deps()), "d6", tail_lines=5000
    )
    assert len(out) == MAX_TAIL_LINES == 1000


@pytest.mark.asyncio
async def test_get_delegation_output_unknown_returns_empty(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    out = await get_delegation_output(_ctx(make_deps()), "missing-id")
    assert out == []


@pytest.mark.asyncio
async def test_get_delegation_output_default_tail(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    """Spec §5.5: default tail is 200 (sanity-check the constant)."""
    assert DEFAULT_TAIL_LINES == 200


# ---------------------------------------------------------------------------
# kill_delegation (Phase 2 → ApprovalRequired)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kill_delegation_raises_approval_required(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="kill-me")

    with pytest.raises(ApprovalRequired) as exc_info:
        await kill_delegation(
            _ctx(make_deps()), "kill-me", "user requested"
        )

    assert "kill-me" in exc_info.value.command
    assert exc_info.value.reason == "user requested"


@pytest.mark.asyncio
async def test_kill_delegation_does_not_touch_row(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    """ApprovalRequired in Phase 2 must NOT update the DB row."""
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="untouched", status="running")
    with pytest.raises(ApprovalRequired):
        await kill_delegation(_ctx(make_deps()), "untouched", "x")
    row = _row(db_path, "untouched")
    assert row["status"] == "running"
    assert row["ended_at"] is None


# ---------------------------------------------------------------------------
# _kill_delegation_forced
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forced_kill_idempotent_on_terminal(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(
        db_path,
        delegation_id="done",
        status="completed",
        ended_at=_utc_now_iso(),
    )

    result = await _kill_delegation_forced(
        _ctx(make_deps()), "done", "too late"
    )
    assert result["status"] == "already_terminal"
    assert result["delegation_id"] == "done"
    assert result["prior_status"] == "completed"
    # Row was not flipped to 'killed'.
    assert _row(db_path, "done")["status"] == "completed"


@pytest.mark.asyncio
async def test_forced_kill_unknown_raises(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    with pytest.raises(DelegationNotFound):
        await _kill_delegation_forced(_ctx(make_deps()), "ghost", "x")


@pytest.mark.asyncio
async def test_forced_kill_null_pid_persists_row(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    """No PID → no signal sent, but the row still flips to 'killed'."""
    _, db_path = scaffold
    _insert_delegation(
        db_path, delegation_id="no-pid", pid=None, status="running"
    )

    result = await _kill_delegation_forced(
        _ctx(make_deps()), "no-pid", "no process"
    )
    assert result["status"] == "killed"
    row = _row(db_path, "no-pid")
    assert row["status"] == "killed"
    assert row["ended_at"] is not None
    assert row["summary"] == "no process"


# ---------------------------------------------------------------------------
# list_delegations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_delegations_scoped_to_user(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="mine-1", user_id="owner")
    _insert_delegation(db_path, delegation_id="mine-2", user_id="owner")
    _insert_delegation(db_path, delegation_id="theirs", user_id="other")

    result = await list_delegations(_ctx(make_deps(user_id="owner")))
    ids = {r["delegation_id"] for r in result}
    assert ids == {"mine-1", "mine-2"}


@pytest.mark.asyncio
async def test_list_delegations_with_status_filter(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(db_path, delegation_id="r", status="running")
    _insert_delegation(
        db_path,
        delegation_id="c",
        status="completed",
        ended_at=_utc_now_iso(),
    )
    _insert_delegation(
        db_path, delegation_id="f", status="failed", ended_at=_utc_now_iso()
    )

    out = await list_delegations(_ctx(make_deps()), status="completed")
    assert [r["delegation_id"] for r in out] == ["c"]
    assert out[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_list_delegations_empty_for_no_rows(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    out = await list_delegations(_ctx(make_deps(user_id="ghost")))
    assert out == []


@pytest.mark.asyncio
async def test_list_delegations_ordered_by_started_desc(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(
        db_path, delegation_id="old", started_at="2026-05-01 00:00:00.000000"
    )
    _insert_delegation(
        db_path, delegation_id="mid", started_at="2026-05-05 00:00:00.000000"
    )
    _insert_delegation(
        db_path, delegation_id="new", started_at="2026-05-10 00:00:00.000000"
    )

    out = await list_delegations(_ctx(make_deps()))
    assert [r["delegation_id"] for r in out] == ["new", "mid", "old"]


@pytest.mark.asyncio
async def test_list_delegations_respects_limit(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    for i in range(5):
        _insert_delegation(
            db_path,
            delegation_id=f"d{i}",
            started_at=f"2026-05-1{i} 00:00:00.000000",
        )

    out = await list_delegations(_ctx(make_deps()), limit=2)
    assert len(out) == 2


@pytest.mark.asyncio
async def test_list_delegations_returns_summary_field(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    _, db_path = scaffold
    _insert_delegation(
        db_path,
        delegation_id="with-sum",
        status="completed",
        ended_at=_utc_now_iso(),
        summary="completed cleanly",
    )
    out = await list_delegations(_ctx(make_deps()))
    assert out[0]["summary_one_line"] == "completed cleanly"
    assert set(out[0].keys()) == {
        "delegation_id",
        "agent",
        "status",
        "started_at",
        "summary_one_line",
    }


@pytest.mark.asyncio
async def test_list_delegations_missing_user_id_raises(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    with pytest.raises(RuntimeError, match="user_id"):
        await list_delegations(_ctx(make_deps(user_id=None)))


# ---------------------------------------------------------------------------
# Integration: forced kill against a real long-lived child.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forced_kill_terminates_real_subprocess(
    scaffold: tuple[Settings, Path], make_deps: Any
) -> None:
    """SIGTERM kills a sleeping child and the row transitions to 'killed'.

    Uses ``python -c "import time; time.sleep(60)"`` as a child the test
    can be sure is alive when ``_kill_delegation_forced`` runs.
    """
    _, db_path = scaffold
    # Spawn a sleeping child via the *current* interpreter (uv-provided venv).
    proc = subprocess.Popen(  # noqa: S603 - argv form, fixed prefix
        [sys.executable, "-c", "import time; time.sleep(60)"],
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # Confirm it's actually alive.
        assert proc.poll() is None
        _insert_delegation(
            db_path,
            delegation_id="real-kill",
            status="running",
            pid=proc.pid,
        )

        result = await _kill_delegation_forced(
            _ctx(make_deps()), "real-kill", "smoke test"
        )
        assert result["status"] == "killed"
        # Child should be reaped quickly under SIGTERM (no kernel delay).
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            await asyncio.sleep(0.05)
        assert proc.poll() is not None, (
            "child still alive after _kill_delegation_forced"
        )

        row = _row(db_path, "real-kill")
        assert row["status"] == "killed"
        assert row["ended_at"] is not None
        assert row["summary"] == "smoke test"
    finally:
        # Belt + braces: don't leak the child if the assert above failed.
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)

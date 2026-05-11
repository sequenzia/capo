"""Tests for the §5.4 / §5.6 / §7.5 Codex restart-resume contract (Task #46).

Covers the acceptance criteria of Task #46: ``_resume_spawn_step`` in
:mod:`capo.workflows.delegation` reads ``delegations.agent`` and
dispatches:

* ``agent='claude-code'`` → existing CC path (covered by
  ``tests/test_workflows_delegation_resume.py``).
* ``agent='codex'`` → ``codex exec resume --json --skip-git-repo-check
  [--model <m>] <thread_id> "<continuation>"`` per spec §5.4 + §7.5
  (re-amended by spike S-1).
* ``agent`` NULL / unknown → row marked failed with reason
  ``"unknown agent type"``.

Argv contract (spec §7.5 + spike S-1 §4.2): the resume argv MUST NOT
include ``--sandbox``, ``--ask-for-approval``, ``-C`` (``--cd``), or
``--add-dir`` — those flags inherit from the original rollout and would
error on ``codex exec resume``.

Mirrors the Task #31 / CC test layout (``_FAKE_*_SCRIPT`` pattern + a
sqlite3 schema fixture) so these tests do NOT require a real Codex
binary on PATH.
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
    AGENT_CLAUDE_CODE,
    AGENT_CODEX,
    CODEX_RESUME_CONTINUATION_PROMPT,
    DEFAULT_CODEX_BINARY,
    RESUME_REASON_FAILED,
    RESUME_REASON_LOST_SESSION_ID,
    RESUME_REASON_UNKNOWN_AGENT,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    _build_codex_resume_argv,
    _resume_spawn_step,
    monitor_delegation,
)

# ---------------------------------------------------------------------------
# Fixtures (mirror tests/test_workflows_delegation_resume.py)
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
    agent                    TEXT,
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
# NOTE: agent column intentionally has NO CHECK constraint here so we can
# exercise the "agent NULL / unknown value" failure branch with raw
# sqlite3 inserts. The production schema (migrations/001_init.py) does
# enforce ``CHECK (agent IN ('claude-code','codex'))`` — that path is
# verified by ``tests/test_migrations.py``.

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
    agent: str | None = AGENT_CODEX,
    session_id: str | None,
    workspace: str | None = None,
    status: str = STATUS_RUNNING,
    pid: int | None = 99999,
    model: str | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status, model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delegation_id,
                "owner",
                agent,
                workspace or f"/tmp/ws/{delegation_id}",
                pid,
                session_id,
                "{}",
                status,
                model,
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
            "SELECT agent, status, ended_at, summary, session_id_subagent, pid "
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
# Unit: _build_codex_resume_argv argv contract
# ---------------------------------------------------------------------------


def test_codex_resume_argv_matches_spec() -> None:
    """Per spec §5.4 / §7.5 (re-amended by spike S-1 §4.2)::

        codex exec resume --json --skip-git-repo-check [--model <m>] \
            <thread_id> "<continuation>"
    """
    argv = _build_codex_resume_argv(
        binary="codex", thread_id="tid-abc", model=None
    )
    assert argv == [
        "codex",
        "exec",
        "resume",
        "--json",
        "--skip-git-repo-check",
        "tid-abc",
        CODEX_RESUME_CONTINUATION_PROMPT,
    ]


def test_codex_resume_argv_includes_model_when_provided() -> None:
    """Per-delegation ``--model`` is preserved across resume."""
    argv = _build_codex_resume_argv(
        binary="codex", thread_id="tid", model="gpt-5"
    )
    assert "--model" in argv
    idx = argv.index("--model")
    assert argv[idx + 1] == "gpt-5"
    # --model must come BEFORE the positional thread_id.
    assert idx < argv.index("tid")


def test_codex_resume_argv_omits_forbidden_flags() -> None:
    """Spec §7.5 + S-1 §4.2: NEVER pass ``--sandbox``,
    ``--ask-for-approval``, ``-C``, ``--cd``, or ``--add-dir`` on
    ``codex exec resume`` — they error with ``unexpected argument``."""
    argv = _build_codex_resume_argv(
        binary="codex", thread_id="tid", model="gpt-5"
    )
    forbidden = {"--sandbox", "--ask-for-approval", "-C", "--cd", "--add-dir"}
    found = set(argv) & forbidden
    assert not found, f"forbidden flag(s) present in resume argv: {found}"


def test_codex_resume_argv_continuation_prompt_is_trailing_positional() -> None:
    """The continuation prompt MUST be the last positional argument
    (per ``codex exec resume <thread_id> "<continuation>"`` shape)."""
    argv = _build_codex_resume_argv(
        binary="codex", thread_id="tid-z", model=None
    )
    assert argv[-1] == CODEX_RESUME_CONTINUATION_PROMPT
    # Thread id immediately precedes the continuation prompt.
    assert argv[-2] == "tid-z"


def test_codex_resume_argv_is_deterministic() -> None:
    """Two builder calls with identical inputs MUST produce identical
    argv — §5.6 restart-resume reproducibility contract."""
    a = _build_codex_resume_argv(binary="codex", thread_id="tid-1", model="m")
    b = _build_codex_resume_argv(binary="codex", thread_id="tid-1", model="m")
    assert a == b


def test_codex_resume_continuation_prompt_is_a_stub() -> None:
    """The continuation prompt is a deterministic 'resume after restart'
    stub — no timestamps, no UUIDs, no env, no original-brief replay."""
    prompt = CODEX_RESUME_CONTINUATION_PROMPT
    # Acceptance criterion: documented choice (stub, not original brief).
    assert "resume" in prompt.lower()
    assert "restart" in prompt.lower()
    # Deterministic — no non-stable substrings.
    assert "{" not in prompt and "}" not in prompt


# ---------------------------------------------------------------------------
# Fake codex scripts (real subprocesses; no Codex install required)
# ---------------------------------------------------------------------------


_FAKE_CODEX_RESUME_HAPPY = """\
#!/usr/bin/env python3
import json, sys, time

# Mimic resumed codex: emit thread.started with the SAME thread_id
# (per spec §7.5 — Codex re-emits the existing thread_id on resume),
# then a turn.completed, then exit 0.
print(json.dumps({
    "type": "thread.started",
    "thread_id": "RESUMED-TID",
}), flush=True)
time.sleep(0.02)
print(json.dumps({
    "type": "turn.completed",
    "usage": {
        "input_tokens": 1,
        "cached_input_tokens": 0,
        "output_tokens": 1,
        "reasoning_output_tokens": 0,
    },
}), flush=True)
sys.exit(0)
"""


_FAKE_CODEX_RESUME_UNKNOWN_THREAD = """\
#!/usr/bin/env python3
import sys
# Mimic `codex exec resume <unknown_thread_id>`: per spec §7.5, emits
# stderr "Error: thread/resume: ... no rollout found for thread id ..."
# then exits non-zero WITHOUT any JSON on stdout. Capo's resume step
# catches this via the first-event timeout branch.
sys.stderr.write(
    "Error: thread/resume: thread/resume failed: no rollout found for "
    "thread id BOGUS-TID (code -32600)\\n"
)
sys.exit(1)
"""


def _write_fake_codex(tmp_path: Path, name: str, script: str) -> Path:
    fake = tmp_path / name
    fake.write_text(script)
    fake.chmod(0o755)
    return fake


# ---------------------------------------------------------------------------
# Integration: agent='codex' → spawns codex exec resume; row completes
# ---------------------------------------------------------------------------


async def test_codex_resume_spawns_with_thread_id_and_completes(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Workflow re-entry: registry empty + row.agent='codex' +
    session_id_subagent populated → resume-spawn via
    ``codex exec resume <thread_id>``, drain, complete normally."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CODEX,
        session_id="ORIGINAL-TID",
        workspace=str(workspace),
    )

    fake = _write_fake_codex(
        tmp_path, "fake_codex_resume_happy", _FAKE_CODEX_RESUME_HAPPY
    )

    captured_argv: list[list[str]] = []
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        captured_argv.append(list(args))
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
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
            codex_binary="codex",
        )

    # Verify the resume argv was for `codex exec resume`, not claude.
    assert len(captured_argv) == 1
    argv = captured_argv[0]
    assert argv[0] == "codex"
    assert argv[1:3] == ["exec", "resume"]
    assert "--json" in argv
    assert "--skip-git-repo-check" in argv
    assert "ORIGINAL-TID" in argv
    # Forbidden flags MUST NOT appear.
    for forbidden in ("--sandbox", "--ask-for-approval", "-C", "--cd", "--add-dir"):
        assert forbidden not in argv, (
            f"forbidden flag {forbidden!r} present in codex resume argv"
        )
    # Continuation prompt is the trailing positional.
    assert argv[-1] == CODEX_RESUME_CONTINUATION_PROMPT

    # Workflow concluded normally via the existing monitor path.
    assert result.status == STATUS_COMPLETED
    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_COMPLETED
    # Original thread_id is preserved (Codex re-emits the SAME id on resume
    # so the reader hook's UPDATE is a no-op write of the original).
    assert row["session_id_subagent"] == "ORIGINAL-TID"


# ---------------------------------------------------------------------------
# Integration: agent='codex' + session_id_subagent NULL → row failed
# ---------------------------------------------------------------------------


async def test_codex_missing_thread_id_marks_row_failed(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: agent='codex' but session_id_subagent (thread_id) is
    NULL → failed with reason 'lost session_id across restart' (shared
    sentinel; thread_id IS the codex session id per Task #45)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CODEX,
        session_id=None,
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
# Integration: agent NULL / unknown → row failed with "unknown agent type"
# ---------------------------------------------------------------------------


async def test_agent_null_marks_row_failed(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: row.agent IS NULL → cannot dispatch — fail with
    'unknown agent type'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=None,
        session_id="something",
    )

    result = await monitor_delegation(
        delegation_id,
        poll_interval_s=0.010,
        db_path=dbos_settings.paths.db_path,
    )
    assert result.status == STATUS_FAILED
    assert result.summary == RESUME_REASON_UNKNOWN_AGENT

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    assert row["summary"] == RESUME_REASON_UNKNOWN_AGENT


async def test_agent_unknown_value_marks_row_failed(
    dbos_settings: _StubSettings,
) -> None:
    """Edge case: row.agent is a non-recognized string → fail with
    'unknown agent type'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent="gemini",  # not a recognized agent
        session_id="something",
    )

    result = await monitor_delegation(
        delegation_id,
        poll_interval_s=0.010,
        db_path=dbos_settings.paths.db_path,
    )
    assert result.status == STATUS_FAILED
    assert result.summary == RESUME_REASON_UNKNOWN_AGENT

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    assert row["summary"] == RESUME_REASON_UNKNOWN_AGENT


# ---------------------------------------------------------------------------
# Integration: codex binary missing → row failed (structured reason)
# ---------------------------------------------------------------------------


async def test_codex_binary_missing_marks_row_failed(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Edge case: codex binary not on PATH at resume time → row failed
    with reason 'session resume failed: codex binary not found'."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CODEX,
        session_id="TID",
        workspace=str(workspace),
    )

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        raise FileNotFoundError("[Errno 2] No such file or directory: 'codex'")

    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos_settings.paths.db_path,
            codex_binary="codex",
        )

    assert result.status == STATUS_FAILED
    assert result.summary is not None
    assert "codex binary not found" in result.summary

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    assert "codex binary not found" in (row["summary"] or "")


# ---------------------------------------------------------------------------
# Integration: idempotency — re-entering twice spawns at most once
# ---------------------------------------------------------------------------


async def test_codex_resume_idempotent_within_one_process_lifetime(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Re-entering the spawn step twice within a single process must
    spawn at most once. After the first successful resume registers a
    handle, the second invocation observes the live handle and the
    registry guard short-circuits the spawn step. Mirrors the CC
    idempotency invariant (DBOS step-state replay covers the same
    property at the workflow level)."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CODEX,
        session_id="ORIGINAL-TID",
        workspace=str(workspace),
    )

    fake = _write_fake_codex(
        tmp_path, "fake_codex_resume_happy", _FAKE_CODEX_RESUME_HAPPY
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
            stdin=kwargs.get("stdin"),
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
            delegation_id, db_path_str, "claude", "codex"
        )
        # Live handle must be present before the second call.
        assert _DELEGATION_REGISTRY.get(delegation_id) is not None
        second = await _resume_spawn_step(
            delegation_id, db_path_str, "claude", "codex"
        )

    assert first["action"] == "spawned"
    assert first.get("agent") == AGENT_CODEX
    assert second["action"] == "skipped_registered"
    assert spawn_count == 1, (
        f"codex spawn step ran {spawn_count} times; idempotency violated"
    )


# ---------------------------------------------------------------------------
# Integration: codex unknown thread_id → no first event → row failed
# ---------------------------------------------------------------------------


async def test_codex_unknown_thread_id_marks_row_failed(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Edge case: ``codex exec resume <unknown_thread_id>`` exits
    non-zero with NO JSON event on stdout (per spec §7.5). The resume
    step's first-event-timeout branch catches this and marks the row
    failed.

    The CC ``is_error=true`` first-event branch is NOT exercised here —
    Codex doesn't emit a synthetic JSON event on resume failure."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CODEX,
        session_id="BOGUS-TID",
        workspace=str(workspace),
    )

    fake = _write_fake_codex(
        tmp_path,
        "fake_codex_resume_unknown",
        _FAKE_CODEX_RESUME_UNKNOWN_THREAD,
    )

    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    # Shorten the first-event timeout so this test runs quickly.
    with patch(
        "capo.workflows.delegation.RESUME_FIRST_EVENT_TIMEOUT_S", 2.0
    ), patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos_settings.paths.db_path,
            codex_binary="codex",
        )

    assert result.status == STATUS_FAILED
    assert result.summary is not None
    assert RESUME_REASON_FAILED in result.summary

    row = _read_row(dbos_settings.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_FAILED
    # Critical: BOGUS-TID was NOT overwritten with anything else.
    assert row["session_id_subagent"] == "BOGUS-TID"


# ---------------------------------------------------------------------------
# Acceptance: CC dispatch is unchanged when agent='claude-code'
# ---------------------------------------------------------------------------


async def test_claude_code_agent_still_uses_claude_resume_argv(
    dbos_settings: _StubSettings, tmp_path: Path
) -> None:
    """Sanity: row.agent='claude-code' MUST still dispatch to the CC
    path (``claude --resume <session_id>``). Documents that Task #46's
    dispatcher did not break the CC path."""
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _insert_row(
        dbos_settings.paths.db_path,
        delegation_id,
        agent=AGENT_CLAUDE_CODE,
        session_id="CC-SID",
        workspace=str(workspace),
    )

    fake_script = """\
#!/usr/bin/env python3
import json, sys
print(json.dumps({
    "type": "system", "subtype": "init",
    "session_id": "CC-SID", "is_error": False,
}), flush=True)
print(json.dumps({
    "type": "result", "session_id": "CC-SID",
    "is_error": False, "subtype": "success",
}), flush=True)
sys.exit(0)
"""
    fake = tmp_path / "fake_claude_for_cc_dispatch"
    fake.write_text(fake_script)
    fake.chmod(0o755)

    captured_argv: list[list[str]] = []
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        captured_argv.append(list(args))
        return await real_exec(
            sys.executable,
            str(fake),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
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

    assert len(captured_argv) == 1
    argv = captured_argv[0]
    # CC argv shape, not codex.
    assert argv[0] == "claude"
    assert "--resume" in argv
    assert "exec" not in argv  # the codex sub-command marker MUST be absent
    assert result.status == STATUS_COMPLETED


# ---------------------------------------------------------------------------
# Sanity: default codex binary constant matches spec
# ---------------------------------------------------------------------------


def test_default_codex_binary_is_codex() -> None:
    """The module-level default must be the bare command name so PATH
    lookup applies — production callers (``capo.main._cold_boot_resume_sweep``)
    override this with the configured ``agents.codex.binary``."""
    assert DEFAULT_CODEX_BINARY == "codex"

"""Phase 4 checkpoint integration tests (spec §9.4 + §10.2 + §5.4 + §5.8 + §5.9 + §5.10 + §7.5).

This module anchors the Phase 4 checkpoint gate (§9.4) for the V1
**approval workflow + Codex parity** contract. The Phase 4 gate has six
acceptance rows:

  * **Approval round-trip**: agent requests ``shell_exec`` for a
    non-allowlisted command → AMC notification fires → user replies
    ``/approve <id>`` → command executes. Verifies exactly-once delivery
    via the ``Idempotency-Key`` header (§5.8 + §7.5).

  * **Approval deny**: same flow, user replies ``/deny <id>`` →
    ``ApprovalRejected`` raised (§5.8).

  * **Approval timeout**: no reply within the configured window →
    ``ApprovalRejected(status='expired')`` raised (§5.8 row 7).

  * **Codex delegation**: ``delegate_to_codex`` → DBOS handoff →
    terminal status → ``notify_user`` exactly-once (§5.4 + §5.6).

  * **Codex restart-resume (compressed)**: row + ``session_id_subagent``
    populated + ``_DELEGATION_REGISTRY`` empty → workflow re-entry → resume
    via ``codex exec resume <thread_id> "<continuation>"`` → terminal
    completion (§5.4 + §5.6 + §7.5). The unmodified wall-clock variant
    lives in ``internal/specs/spikes/S-6-phase4-checkpoint.md``.

  * **Kill cascade**: an in-flight delegation has a pending approval
    tied to its ``delegation_id`` via ``request_payload``; the owner
    kills the delegation → cascade-cancel resolves the tied approval to
    ``cancelled`` (§5.10 + §5.8).

Test scaffold conventions mirror :mod:`tests.test_phase3_checkpoint`:

  * In-memory schema setup (we re-execute the same DDL as
    ``migrations/001_init.py`` + ``migrations/002_approvals.py`` +
    ``migrations/003_approvals_request_types.py`` rather than running
    Alembic in tests; per-migration tests prove the migrations
    themselves).
  * ``AmcStub`` with ``delivered_keys`` set tracking
    receiver-side dedupe by ``Idempotency-Key``.
  * ``FAKE_CODEX_SCRIPT`` patterns from :mod:`tests.test_delegate_to_codex`
    and :mod:`tests.test_workflows_delegation_codex_resume`.
  * DBOS launched per-test against a tmp-path ``state.db`` + ``dbos.db``.
  * ``/approve`` / ``/deny`` driven via ``DBOS.send_async`` against the
    ``approvals.workflow_id`` column (same protocol the dispatcher uses
    in Task #40).

The 30-minute / 24-hour wall-clock approval-timeout variants are
infeasible in CI and are documented as operator gates in
``internal/specs/spikes/S-6-phase4-checkpoint.md``. The 2-second
compressed timeout test in this module exercises the same
``DBOS.recv_async(timeout_seconds=...)`` → ``status='expired'`` code
path the operator runbook validates at deploy time.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
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
from capo.tools.basic import (
    ApprovalRejected,
    ShellResult,
    shell_exec,
)
from capo.tools.codex import CodexBrief, delegate_to_codex
from capo.tools.delegations import kill_delegation
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows.approval import (
    APPROVAL_TOPIC,
    STATUS_CANCELLED,
    STATUS_DENIED,
    STATUS_EXPIRED,
    request_approval,
)
from capo.workflows.delegation import (
    _DELEGATION_REGISTRY,
    AGENT_CODEX,
    CODEX_RESUME_CONTINUATION_PROMPT,
    STATUS_COMPLETED,
    STATUS_RUNNING,
    monitor_delegation,
    register_amc_sender,
    set_heartbeat_poll_interval,
    set_heartbeat_thresholds,
)

# ---------------------------------------------------------------------------
# Settings scaffolding (mirrors tests/test_shell_exec_approval.py + the
# codex test). One TOML covers all six checkpoint tests — the only
# variable bit is the codex binary path, which is wired to a tiny
# version-stub script written into ``tmp_path``.
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
binary = "{codex_binary}"
default_sandbox = "workspace-write"
worktree_base_branch = "main"

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


# ---------------------------------------------------------------------------
# Schemas — mirror migrations/001_init.py + 002_approvals.py +
# 003_approvals_request_types.py. The migration tests prove the DDL is
# correct; we duplicate it here to avoid alembic in unit tests.
# ---------------------------------------------------------------------------

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

_APPROVALS_DDL = """
CREATE TABLE IF NOT EXISTS approvals (
    approval_id       TEXT PRIMARY KEY,
    requested_at      TEXT NOT NULL,
    decided_at        TEXT,
    request_type      TEXT NOT NULL CHECK (
        request_type IN (
            'shell_exec',
            'delegate_out_of_root',
            'delegate_dangerous_sandbox',
            'kill_delegation'
        )
    ),
    request_payload   TEXT NOT NULL,
    requester_user_id TEXT,
    parent_thread_id  TEXT,
    status            TEXT NOT NULL CHECK (
        status IN ('pending','approved','denied','expired','cancelled')
    ),
    resolved_by       TEXT,
    reason            TEXT,
    workflow_id       TEXT
)
"""


def _ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = open_connection(db_path)
    try:
        conn.execute(_DELEGATIONS_DDL)
        conn.execute(_DELEGATION_OUTPUT_DDL)
        conn.execute(_APPROVALS_DDL)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fake codex --version binary. Real spawn paths are hijacked via
# ``patch(create_subprocess_exec)`` inside individual tests.
# ---------------------------------------------------------------------------

_FAKE_CODEX_VERSION_BIN = """\
#!/usr/bin/env python3
import sys
if len(sys.argv) >= 2 and sys.argv[1] == "--version":
    print("codex-cli 0.130.0")
    sys.exit(0)
sys.exit(0)
"""

# Drives the end-to-end Codex delegation test. Emits the canonical
# JSONL event sequence per spike S-1 (a non-JSON prefix line, then
# thread.started, turn.started, item.completed, turn.completed).
_FAKE_CODEX_SCRIPT = """\
#!/usr/bin/env python3
import json, sys, time
# Non-JSON prefix that the reader must tolerate (spec §7.5 + S-1 §6).
print("Reading additional input from stdin...", flush=True)
print(json.dumps({
    "type": "thread.started",
    "thread_id": "PHASE4-CODEX-TID",
}), flush=True)
time.sleep(0.02)
print(json.dumps({"type": "turn.started"}), flush=True)
print(json.dumps({
    "type": "item.completed",
    "item": {"id": "1", "type": "agent_message", "text": "ok"},
}), flush=True)
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

# Drives the compressed Codex restart-resume test. Mimics
# ``codex exec resume <thread_id>``: emits thread.started with the SAME
# thread_id (Codex re-emits the existing id on resume per §7.5), then a
# turn.completed, then exits 0.
_FAKE_CODEX_RESUME_HAPPY = """\
#!/usr/bin/env python3
import json, sys, time
print(json.dumps({
    "type": "thread.started",
    "thread_id": "PHASE4-RESUME-TID",
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


def _git(*args: str, cwd: Path) -> None:
    import subprocess  # noqa: PLC0415

    subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


# ---------------------------------------------------------------------------
# AMC stub — mirrors the Phase 3 checkpoint pattern. Tracks
# ``delivered_keys`` so receiver-side dedupe can be asserted from the
# test (a real AMC server would dedupe by ``Idempotency-Key``).
# ---------------------------------------------------------------------------


class _StubSendResult:
    """Mimics :class:`capo.transport.amc_client.SendResult`."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class AmcStub:
    """Records every send + dedupes by idempotency_key on the receiver.

    Same idempotency_key → same message_id; len(calls) MAY exceed
    unique_deliveries if DBOS retries the step across a workflow replay.
    """

    def __init__(self, *, message_id: str = "msg-phase4") -> None:
        self.calls: list[dict[str, str]] = []
        self.delivered_keys: set[str] = set()
        self._message_id = message_id

    @property
    def unique_deliveries(self) -> int:
        return len(self.delivered_keys)

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
        self.delivered_keys.add(idempotency_key)
        return _StubSendResult(self._message_id)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def codex_version_bin(tmp_path: Path) -> Path:
    p = tmp_path / "codex_fake_version"
    p.write_text(_FAKE_CODEX_VERSION_BIN)
    p.chmod(0o755)
    return p


@pytest.fixture
def scaffold(tmp_path: Path, codex_version_bin: Path) -> tuple[Settings, Path, Path]:
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()
    (tmp_path / ".capo").mkdir()
    db_path = tmp_path / ".capo" / "state.db"
    dbos_path = tmp_path / ".capo" / "dbos.db"

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(db_path),
            dbos_db_path=str(dbos_path),
            codex_binary=str(codex_version_bin),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    _ensure_schema(db_path)
    return settings, projects_root, workspaces_root


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
        *, user_id: str = "user-42", thread_id: str = "amc:ch-PHASE4"
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


@pytest.fixture(autouse=True)
def _reset_workflow_state() -> Iterator[None]:
    """DBOS is a process-level singleton + the approval workflow caches
    its registered callable in module globals. Reset all of that
    between tests.
    """
    import capo.workflows.approval as approval_mod  # noqa: PLC0415

    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    register_amc_sender(None)
    set_heartbeat_thresholds(None)
    set_heartbeat_poll_interval(None)
    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    yield
    with contextlib.suppress(Exception):
        _DELEGATION_REGISTRY.clear()
    register_amc_sender(None)
    set_heartbeat_thresholds(None)
    set_heartbeat_poll_interval(None)
    approval_mod._workflow_registered = False
    approval_mod._approval_workflow = None
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


@pytest.fixture
def dbos(scaffold: tuple[Settings, Path, Path]) -> Iterator[Settings]:
    """Init + launch DBOS for tests that exercise the real workflow."""
    settings, _, _ = scaffold
    init_dbos(settings)
    launch_dbos()
    yield settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_approval_row(db_path: Path, approval_id: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _read_delegation_row(db_path: Path, delegation_id: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM delegations WHERE id = ?", (delegation_id,)
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _insert_delegation_row(
    db_path: Path,
    *,
    delegation_id: str,
    user_id: str = "user-42",
    agent: str = "claude-code",
    session_id: str | None = None,
    workspace: str = "/tmp/ws",
    status: str = STATUS_RUNNING,
    parent_thread_id: str = "amc:ch-PHASE4",
    pid: int | None = None,
    model: str | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO delegations (
                id, user_id, agent, workspace, pid, session_id_subagent,
                brief_json, status, parent_thread_id, model
            ) VALUES (?, ?, ?, ?, ?, ?, '{}', ?, ?, ?)
            """,
            (
                delegation_id,
                user_id,
                agent,
                workspace,
                pid,
                session_id,
                status,
                parent_thread_id,
                model,
            ),
        )
        conn.commit()
    finally:
        conn.close()


async def _resolve_pending_approval_via_dbos(
    db_path: Path,
    *,
    decision: str,
    resolved_by: str,
    reason: str | None,
    amc_stub: AmcStub,
    timeout_s: float = 10.0,
) -> str:
    """Poll for the latest pending approvals row + send the decision.

    Uses the same protocol the dispatcher (Task #40) uses:
    ``DBOS.send_async(workflow_id, payload, APPROVAL_TOPIC)``.

    Returns the approval_id that was resolved (for assertion).
    """
    from dbos import DBOS  # noqa: PLC0415

    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        await asyncio.sleep(0.1)
        if not amc_stub.calls:
            continue
        conn = sqlite3.connect(str(db_path))
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT approval_id, workflow_id FROM approvals "
                "WHERE status='pending' "
                "ORDER BY requested_at DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if row is None or row["workflow_id"] is None:
            continue
        await DBOS.send_async(
            row["workflow_id"],
            {"status": decision, "resolved_by": resolved_by, "reason": reason},
            APPROVAL_TOPIC,
        )
        return str(row["approval_id"])
    raise AssertionError("resolve loop did not see a pending row in time")


# ===========================================================================
# Test 1 — Approval round-trip (approve)
# ===========================================================================
#
# Spec anchors: §5.8 (Approval Flows) + §5.9 (Inbound contract) + §7.5
# (Idempotency-Key header) + §9.4 Phase 4 checkpoint row 1.
#
# Sequence:
#   t=0    Agent calls shell_exec("echo phase4-approve") — NOT in
#          allowlist → routes through request_approval.
#   t=1    Approval row inserted; AMC notification fired (one POST).
#   t=2    Test "user" sends DBOS.send(workflow_id, "approve") — same
#          path the dispatcher uses for inbound /approve.
#   t=3    Workflow falls out of recv → resolves row to 'approved' →
#          shell_exec proceeds to run echo and returns ShellResult.
#
# Idempotency-Key invariant: the receiver-side dedupe counter
# (``unique_deliveries``) equals 1 across the entire round-trip — a
# DBOS retry of the notify step would produce the same key, and the
# AMC stub would record additional ``calls`` entries but only one
# unique delivered message.
# ===========================================================================


@pytest.mark.asyncio
async def test_approval_round_trip_executes_and_idempotent_delivery(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """§9.4 row 1: non-allowlisted shell_exec → /approve → command runs.

    Verifies exactly-once delivery via the Idempotency-Key header
    (§7.5): the AMC stub's ``unique_deliveries`` is 1 for the whole
    flow regardless of how many times the notify step is invoked.
    """
    _, projects_root, _ = scaffold
    settings, _, _ = scaffold
    deps = make_deps()
    stub = AmcStub(message_id="msg-phase4-approve")
    register_amc_sender(stub.send)

    cmd = "echo phase4-approve"

    approver = asyncio.create_task(
        _resolve_pending_approval_via_dbos(
            settings.paths.db_path,
            decision="approve",
            resolved_by="user-42",
            reason=None,
            amc_stub=stub,
        )
    )
    try:
        result = await shell_exec(_ctx(deps), cmd, cwd=str(projects_root))
        approved_id = await approver
    finally:
        if not approver.done():
            approver.cancel()
            with contextlib.suppress(BaseException):
                await approver

    # 1. Command executed.
    assert isinstance(result, ShellResult)
    assert result.exit_code == 0
    assert "phase4-approve" in result.stdout

    # 2. AMC notification fired at least once (DBOS may have replayed
    # the step; in practice we expect exactly 1).
    assert len(stub.calls) >= 1, "approval notify must fire"

    # 3. Exactly-one delivery via Idempotency-Key dedupe.
    assert stub.unique_deliveries == 1, (
        f"AMC receiver should see exactly one unique delivery; "
        f"got {stub.unique_deliveries} unique among {len(stub.calls)} calls"
    )

    # 4. All notify calls carry the same Idempotency-Key.
    first_key = stub.calls[0]["idempotency_key"]
    for call in stub.calls:
        assert call["idempotency_key"] == first_key
    assert first_key.startswith("notify_approval:")

    # 5. AMC notify routed to the right channel (parent_thread_id
    # ``amc:ch-PHASE4`` → ``ch-PHASE4``).
    assert stub.calls[0]["channel_id"] == "ch-PHASE4"
    # Notification body includes the /approve and /deny prompts.
    assert "/approve" in stub.calls[0]["text"]
    assert "/deny" in stub.calls[0]["text"]

    # 6. Persisted row reflects the approved resolution + caller-
    # supplied payload shape (command, args, cwd).
    row = _read_approval_row(settings.paths.db_path, approved_id)
    assert row is not None
    assert row["status"] == "approved"
    assert row["request_type"] == "shell_exec"
    assert row["resolved_by"] == "user-42"
    assert row["requester_user_id"] == "user-42"
    assert row["parent_thread_id"] == "amc:ch-PHASE4"
    payload = json.loads(row["request_payload"])
    assert payload["command"] == cmd
    assert payload["args"] == ["echo", "phase4-approve"]
    assert payload["cwd"] == str(projects_root)


# ===========================================================================
# Test 2 — Approval deny → ApprovalRejected raised
# ===========================================================================
#
# Spec anchors: §5.8 + §9.4 Phase 4 checkpoint row 2.
#
# Same shell_exec flow as test 1 but the user replies /deny. The tool
# raises ApprovalRejected(decision.status='denied'). The persisted row
# lands in 'denied' with resolved_by + reason set.
# ===========================================================================


@pytest.mark.asyncio
async def test_approval_deny_raises_approval_rejected(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """§9.4 row 2: non-allowlisted shell_exec → /deny → ApprovalRejected."""
    _, projects_root, _ = scaffold
    settings, _, _ = scaffold
    deps = make_deps()
    stub = AmcStub(message_id="msg-phase4-deny")
    register_amc_sender(stub.send)

    denier = asyncio.create_task(
        _resolve_pending_approval_via_dbos(
            settings.paths.db_path,
            decision="deny",
            resolved_by="user-42",
            reason="phase4 deny test",
            amc_stub=stub,
        )
    )
    try:
        with pytest.raises(ApprovalRejected) as exc_info:
            await shell_exec(
                _ctx(deps), "echo nope", cwd=str(projects_root)
            )
        denied_id = await denier
    finally:
        if not denier.done():
            denier.cancel()
            with contextlib.suppress(BaseException):
                await denier

    err = exc_info.value
    assert err.status == STATUS_DENIED
    assert err.resolved_by == "user-42"
    assert err.reason == "phase4 deny test"
    assert err.decision.status == STATUS_DENIED
    # str() carries the approval_id for log correlation.
    assert err.approval_id in str(err)

    # Persisted row: denied + same resolved_by / reason.
    row = _read_approval_row(settings.paths.db_path, denied_id)
    assert row is not None
    assert row["status"] == "denied"
    assert row["resolved_by"] == "user-42"
    assert row["reason"] == "phase4 deny test"

    # Exactly-once notify delivery, same Idempotency-Key invariant.
    assert stub.unique_deliveries == 1
    assert stub.calls[0]["idempotency_key"].startswith("notify_approval:")


# ===========================================================================
# Test 3 — Approval timeout → ApprovalRejected(status='expired')
# ===========================================================================
#
# Spec anchors: §5.8 row 7 (Timeout default + behavior) + §9.4 Phase 4
# checkpoint row 3.
#
# Sequence:
#   t=0    Agent calls shell_exec("echo timeout-test") with the
#          ``[approval].timeout_seconds`` overridden to 2 seconds
#          (just above the DBOS SQLite recv polling floor ~1s).
#   t=1    Approval row inserted; AMC notification fired.
#   t=2    NO user reply. DBOS.recv_async times out → workflow body
#          sees raw_message is None → sets status_final='expired',
#          resolved_by=None, reason="timeout after 2s" → UPDATEs the row.
#   t=3    Tool raises ApprovalRejected(status='expired').
#
# This is the **compressed** variant of the §9.4 row 3 acceptance
# criterion. The unmodified 30-minute / 24-hour variant is documented
# in ``internal/specs/spikes/S-6-phase4-checkpoint.md`` (the operator
# runbook); CI cannot wait 30 minutes per gate run.
# ===========================================================================


@pytest.mark.asyncio
async def test_approval_timeout_raises_expired(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """§9.4 row 3 (compressed): non-allowlisted shell_exec with no
    reply within timeout → ApprovalRejected(status='expired').
    """
    settings, projects_root, _ = scaffold
    # Override approval timeout via direct attribute set — the Pydantic
    # frozen guard is bypassed via object.__setattr__. The shell_exec
    # tool reads ``settings.approval.timeout_seconds`` lazily so this
    # takes effect for the next call only.
    object.__setattr__(settings.approval, "timeout_seconds", 2)

    deps_local = make_deps()
    stub = AmcStub(message_id="msg-phase4-timeout")
    register_amc_sender(stub.send)

    with pytest.raises(ApprovalRejected) as exc_info:
        await shell_exec(
            _ctx(deps_local),
            "echo timeout-test",
            cwd=str(projects_root),
        )

    err = exc_info.value
    assert err.status == STATUS_EXPIRED
    # System-resolved → no resolved_by user.
    assert err.resolved_by is None
    assert err.reason is not None
    assert "timeout" in err.reason

    # The notification still fired (this is *before* the timeout).
    assert stub.unique_deliveries == 1

    # Persisted row lands in expired.
    row = _read_approval_row(settings.paths.db_path, err.approval_id)
    assert row is not None
    assert row["status"] == "expired"
    assert row["resolved_by"] is None
    assert row["reason"] is not None
    assert "timeout" in row["reason"]


# ===========================================================================
# Test 4 — Codex delegation end-to-end (DBOS handoff → terminal → notify)
# ===========================================================================
#
# Spec anchors: §5.4 (Codex tool surface) + §5.6 (DBOS handoff +
# notify) + §9.4 Phase 4 checkpoint row 4 + §7.5 (Codex JSONL
# contract).
#
# Sequence:
#   t=0    Agent calls delegate_to_codex(brief) inside projects_root.
#   t=1    Codex spawn argv: ``codex exec --json --skip-git-repo-check
#          --sandbox workspace-write -C <workspace> "<prompt>"``.
#          Subprocess hijacked to FAKE_CODEX_SCRIPT.
#   t=2    Persistence-before-yield: delegations row exists with
#          status='running', agent='codex'.
#   t=3    Reader captures thread_id ("PHASE4-CODEX-TID") into
#          session_id_subagent; fake exits 0.
#   t=4    DBOS workflow drains, classifies rc=0 as completed,
#          persists terminal status, fires notify_user with
#          Idempotency-Key = notify_user:<32hex>.
#   t=5    Test waits for row.status='completed' + AMC stub captures
#          exactly one delivery for this delegation.
#
# This is the §9.4 row 4 acceptance criterion verbatim.
# ===========================================================================


@pytest.mark.asyncio
async def test_codex_delegation_dbos_handoff_to_terminal_and_notify(
    tmp_path: Path,
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """§9.4 row 4: delegate_to_codex → DBOS handoff → terminal status →
    notify_user delivers exactly once.
    """
    settings, projects_root, _ = scaffold
    # Build a small git repo inside projects_root so the worktree helper
    # has a base branch to fork from.
    repo = projects_root / "repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)

    deps = make_deps(thread_id="amc:ch-CODEX")
    stub = AmcStub(message_id="msg-phase4-codex")
    register_amc_sender(stub.send)

    # Suppress heartbeats — the compressed run is sub-30s; thresholds
    # never cross.
    set_heartbeat_thresholds([3600])
    set_heartbeat_poll_interval(10.0)

    brief = CodexBrief(
        goal="phase4 checkpoint",
        repo_path=str(repo),
        create_worktree=True,
        model="openai:gpt-5",
        sandbox="workspace-write",
    )

    fake_codex = tmp_path / "fake_codex_phase4"
    fake_codex.write_text(_FAKE_CODEX_SCRIPT)
    fake_codex.chmod(0o755)

    spawn_argvs: list[list[str]] = []
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        spawn_argvs.append(list(args))
        return await real_exec(
            sys.executable,
            str(fake_codex),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    with patch(
        "capo.tools.codex.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        handle = await delegate_to_codex(_ctx(deps), brief)

    # Persistence-before-yield: row exists immediately.
    assert handle.status == "running"
    assert handle.agent == "codex"
    initial_row = _read_delegation_row(settings.paths.db_path, handle.delegation_id)
    assert initial_row is not None
    assert initial_row["agent"] == "codex"
    assert initial_row["parent_thread_id"] == "amc:ch-CODEX"
    assert initial_row["pid"] is not None

    # Spawn argv canonical form (spec §5.4).
    assert len(spawn_argvs) == 1
    argv = spawn_argvs[0]
    assert "exec" in argv
    assert "--json" in argv
    assert "--skip-git-repo-check" in argv
    assert "--sandbox" in argv
    assert "workspace-write" in argv
    assert "--ephemeral" not in argv, "spec §5.4: NEVER pass --ephemeral"

    # Wait for the DBOS handoff to drive the row to terminal AND for
    # notify_user to fire.
    loop = asyncio.get_event_loop()
    deadline = loop.time() + 15.0
    while loop.time() < deadline:
        row = _read_delegation_row(settings.paths.db_path, handle.delegation_id)
        if row is not None and row["status"] != "running" and stub.calls:
            break
        await asyncio.sleep(0.1)

    # Row reached terminal completed.
    row = _read_delegation_row(settings.paths.db_path, handle.delegation_id)
    assert row is not None
    assert row["status"] == "completed", (
        f"row never reached completed; status={row['status']!r} "
        f"summary={row.get('summary')!r}"
    )
    assert row["ended_at"] is not None
    assert row["session_id_subagent"] == "PHASE4-CODEX-TID"

    # notify_user fired exactly once for the terminal completion.
    assert len(stub.calls) == 1, (
        f"expected one notify_user call; got {len(stub.calls)}: {stub.calls}"
    )
    call = stub.calls[0]
    assert call["channel_id"] == "ch-CODEX"
    assert "COMPLETED" in call["text"]
    assert call["idempotency_key"].startswith("notify_user:")
    assert stub.unique_deliveries == 1


# ===========================================================================
# Test 5 — Codex restart-resume (compressed equivalent)
# ===========================================================================
#
# Spec anchors: §5.4 + §5.6 + §7.5 (Codex resume argv) + §9.4 Phase 4
# checkpoint row 5.
#
# Sequence (compressed equivalent of §10.2 steps 1-7 but for Codex):
#
#   t=0    Insert a delegations row with agent='codex',
#          status='running', and session_id_subagent populated. This
#          models the post-restart steady state — the original codex
#          subprocess was orphaned + reaped, Capo just rebooted, the
#          in-process registry is empty, and the row carries the
#          rolled-up thread_id.
#   t=1    Invoke monitor_delegation(delegation_id, agent='codex').
#          The resume step (Task #46) reads agent → builds
#          ``codex exec resume <thread_id> "<continuation>"`` argv →
#          spawns the fake resume script.
#   t=2    Fake emits thread.started (same thread_id) + turn.completed
#          then exits 0. Workflow body drains, persists terminal
#          completed, fires notify_user once.
#
# The unmodified wall-clock variant lives in
# ``internal/specs/spikes/S-6-phase4-checkpoint.md`` (operator gate).
# ===========================================================================


async def test_codex_restart_resume_completes_and_notifies_once(
    dbos: Settings,
    tmp_path: Path,
) -> None:
    """§9.4 row 5 (compressed): codex delegation row → workflow
    re-entry → codex exec resume → terminal completion → exactly one
    notify_user delivery.
    """
    delegation_id = f"d-{uuid.uuid4().hex[:8]}"
    workspace = tmp_path / "codex_ws"
    workspace.mkdir()
    _insert_delegation_row(
        dbos.paths.db_path,
        delegation_id=delegation_id,
        agent=AGENT_CODEX,
        session_id="PHASE4-RESUME-TID",
        workspace=str(workspace),
        parent_thread_id="amc:ch-CODEX-RESUME",
    )

    stub = AmcStub(message_id="msg-codex-resume")
    register_amc_sender(stub.send)
    # Suppress heartbeats.
    set_heartbeat_thresholds([3600])
    set_heartbeat_poll_interval(10.0)

    resume_fake = tmp_path / "fake_codex_resume_phase4"
    resume_fake.write_text(_FAKE_CODEX_RESUME_HAPPY)
    resume_fake.chmod(0o755)

    captured_argv: list[list[str]] = []
    real_exec = asyncio.create_subprocess_exec

    async def hijack(*args: Any, **kwargs: Any) -> Any:
        captured_argv.append(list(args))
        return await real_exec(
            sys.executable,
            str(resume_fake),
            cwd=kwargs.get("cwd"),
            stdin=kwargs.get("stdin"),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 64 * 1024),
        )

    # The registry MUST be empty on entry — that's the steady-state
    # right after `launchctl kickstart`.
    assert delegation_id not in _DELEGATION_REGISTRY

    with patch(
        "capo.workflows.delegation.asyncio.create_subprocess_exec",
        side_effect=hijack,
    ):
        result = await monitor_delegation(
            delegation_id,
            poll_interval_s=0.010,
            db_path=dbos.paths.db_path,
            codex_binary="codex",
        )

    # Workflow completed normally via the resume path.
    assert result.status == STATUS_COMPLETED, (
        f"expected completed, got {result.status} (summary={result.summary!r})"
    )

    # Resume argv canonical form (spec §5.4 + §7.5).
    assert len(captured_argv) == 1
    argv = captured_argv[0]
    assert argv[0] == "codex"
    assert argv[1:3] == ["exec", "resume"]
    assert "--json" in argv
    assert "--skip-git-repo-check" in argv
    assert "PHASE4-RESUME-TID" in argv
    # Forbidden flags (S-1 §4.2): MUST NOT be present on resume.
    for forbidden in ("--sandbox", "--ask-for-approval", "-C", "--cd", "--add-dir"):
        assert forbidden not in argv, (
            f"forbidden flag {forbidden!r} present in codex resume argv"
        )
    # Continuation prompt is the trailing positional.
    assert argv[-1] == CODEX_RESUME_CONTINUATION_PROMPT

    # Row reached terminal completed; original thread_id preserved.
    row = _read_delegation_row(dbos.paths.db_path, delegation_id)
    assert row is not None
    assert row["status"] == STATUS_COMPLETED
    assert row["ended_at"] is not None
    assert row["session_id_subagent"] == "PHASE4-RESUME-TID"

    # notify_user fired exactly once.
    assert len(stub.calls) == 1
    call = stub.calls[0]
    assert call["channel_id"] == "ch-CODEX-RESUME"
    assert "COMPLETED" in call["text"]
    assert call["idempotency_key"].startswith("notify_user:")
    assert stub.unique_deliveries == 1


# ===========================================================================
# Test 6 — Kill cascade resolves tied pending approval
# ===========================================================================
#
# Spec anchors: §5.10 + §5.8 (Approval Flows external resolution) +
# §9.4 Phase 4 checkpoint row 6.
#
# Sequence:
#   t=0    Insert a delegations row owned by user-42 (status=running).
#   t=1    Background task fires request_approval(...) with payload
#          {"delegation_id": "...", ...}. The workflow body INSERTs a
#          pending approvals row and then suspends in
#          DBOS.recv_async(approval_decision, timeout=...).
#   t=2    Test polls until the approvals row is pending + workflow_id
#          populated.
#   t=3    Owner (user-42) calls kill_delegation(delegation_id). The
#          tool flips the delegation row to killed AND walks
#          approvals WHERE json_extract(request_payload, '$.delegation_id')
#          = ? AND status='pending', then calls force_resolve_approval
#          (status='cancelled') on each.
#   t=4    The awaiting request_approval workflow falls out of recv
#          with the cascade payload, UPDATEs the row to 'cancelled',
#          returns ApprovalDecision(status='cancelled', resolved_by=
#          'system:killer:user-42', reason='delegation <id> killed').
#
# This is the §9.4 row 6 + §5.10 cascade-cancel acceptance criterion.
# ===========================================================================


@pytest.mark.asyncio
async def test_kill_cascade_resolves_tied_pending_approval(
    scaffold: tuple[Settings, Path, Path],
    make_deps: Any,
    dbos: Settings,
) -> None:
    """§9.4 row 6: killing a delegation with a pending approval tied to
    its delegation_id cascade-cancels the approval.
    """
    settings, _, _ = scaffold
    stub = AmcStub(message_id="msg-phase4-cascade")
    register_amc_sender(stub.send)

    delegation_id = "del-phase4-cascade"
    _insert_delegation_row(
        settings.paths.db_path,
        delegation_id=delegation_id,
        user_id="user-42",
        pid=None,  # No subprocess — kill just flips the row.
    )

    # Fire a separate approval whose payload references the delegation.
    # Pin it to 'delegate_out_of_root' so the kill path's own approval
    # (when triggered) doesn't collide.
    tied_approval_id = uuid.uuid4().hex
    tied_requested_at = "2026-05-11T00:00:00Z"

    async def _wait_for_pending() -> None:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 5.0
        while loop.time() < deadline:
            await asyncio.sleep(0.05)
            conn = sqlite3.connect(str(settings.paths.db_path))
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT workflow_id FROM approvals "
                    "WHERE approval_id = ? AND status = 'pending'",
                    (tied_approval_id,),
                ).fetchone()
            finally:
                conn.close()
            if row is not None and row["workflow_id"] is not None:
                return
        raise AssertionError("tied approval never went pending")

    # Start the tied approval workflow as a background task; it will
    # block in recv until our kill_delegation cascade fires the cancel.
    tied_task = asyncio.create_task(
        request_approval(
            tied_approval_id,
            request_type="delegate_out_of_root",
            request_payload={
                "delegation_id": delegation_id,
                "path": "/tmp/elsewhere",
            },
            requester_user_id="user-42",
            parent_thread_id="amc:ch-PHASE4",
            requested_at=tied_requested_at,
            timeout_seconds=30,
            db_path=str(settings.paths.db_path),
        )
    )

    try:
        await _wait_for_pending()

        # Owner kill — no approval round-trip is required for the
        # killer; the cascade is the side effect we care about.
        result = await kill_delegation(
            _ctx(make_deps(user_id="user-42")),
            delegation_id,
            "phase4 cascade kill",
        )
        assert result["status"] == "killed"
        # Exactly one tied approval was cascade-cancelled.
        assert result["cascaded_approvals"] == 1

        # The tied approval workflow returns cancelled.
        decision = await asyncio.wait_for(tied_task, timeout=10.0)
        assert decision.status == STATUS_CANCELLED
        assert decision.resolved_by == "system:killer:user-42"
        assert decision.reason == f"delegation {delegation_id} killed"

        # Persisted row matches.
        row = _read_approval_row(settings.paths.db_path, tied_approval_id)
        assert row is not None
        assert row["status"] == "cancelled"
        assert row["resolved_by"] == "system:killer:user-42"
        assert row["reason"] == f"delegation {delegation_id} killed"

        # Delegation row reflects the kill.
        drow = _read_delegation_row(settings.paths.db_path, delegation_id)
        assert drow is not None
        assert drow["status"] == "killed"
        assert drow["summary"] == "phase4 cascade kill"

        # The tied approval's notify fired exactly once (we never sent
        # /approve or /deny for it — it was force-cancelled).
        assert stub.unique_deliveries == 1
        assert stub.calls[0]["idempotency_key"].startswith("notify_approval:")
    finally:
        if not tied_task.done():
            tied_task.cancel()
            with contextlib.suppress(BaseException):
                await tied_task

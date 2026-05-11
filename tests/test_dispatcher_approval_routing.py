"""Tests for dispatcher approval inbound routing (Task #40).

Covers spec §5.8 / §5.9 / §7.5 acceptance criteria for ``/approve`` and
``/deny`` interception in :class:`capo.transport.dispatcher.Dispatcher`:

* ``/approve <id>`` → ``DBOS.send_async(workflow_id, payload, APPROVAL_TOPIC)``
  + ack ``approval recorded: <id>`` reply.
* ``/approve`` (no id) → most-recent pending in this thread is resolved.
* ``/approve`` (no id, none pending) → ``no pending approval`` reply.
* ``/approve <unknown-id>`` → ``unknown id`` reply.
* ``/approve <id>`` where row is already terminal → ``already resolved`` reply
  and NO ``DBOS.send_async`` call (idempotency).
* ``/approve <id>`` where workflow_id is NULL → wait + retry; still NULL →
  ``still being created`` reply.
* The agent is NEVER invoked for an approval slash command (pre-agent route).

Tests use the dispatcher fakes from :mod:`tests.test_dispatcher` patterns
(local copies of ``_FakeAgent`` / ``_FakeAMCClient``) plus a real in-memory
SQLite seeded with both the ``users`` table (from migration 001) and the
``approvals`` table DDL (mirrors migration 002). DBOS itself is monkey-
patched per-test — we replace ``dbos.DBOS.send_async`` with a recording
callable so we don't need a launched DBOS instance for these tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util as _ilu
import sqlite3
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from capo.config import Settings
from capo.deps import CapoDeps
from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.dispatcher import (
    APPROVAL_REPLY_ALREADY_RESOLVED,
    APPROVAL_REPLY_NO_PENDING,
    APPROVAL_REPLY_STILL_CREATING,
    APPROVAL_REPLY_UNKNOWN_ID,
    ChannelWorker,
    Dispatcher,
    format_approval_recorded_reply,
)
from capo.transport.slash import parse_slash_command
from capo.workflows.approval import APPROVAL_TOPIC
from tests.test_config import _VALID_ENV, _write_config

# ---------------------------------------------------------------------------
# Schema bootstrap — both migrations 001 + 002 inlined.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_001 = REPO_ROOT / "migrations" / "versions" / "001_init.py"
_MIG_002 = REPO_ROOT / "migrations" / "versions" / "002_approvals.py"

sys.path.insert(0, str(_MIG_001.parent))


def _load_migration(path: Path, name: str) -> Any:
    spec = _ilu.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mig_001 = _load_migration(_MIG_001, "_mig_001_dispatcher_approval")
_mig_002 = _load_migration(_MIG_002, "_mig_002_dispatcher_approval")


def _apply_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig_001._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_002._UPGRADE_STATEMENTS:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Fakes — minimal copies of the patterns in tests/test_dispatcher.py.
# ---------------------------------------------------------------------------


class _FakeAgent:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Any], CapoDeps]] = []

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        self.calls.append((text, list(message_history or []), deps))
        return _FakeRunResult(f"agent:{text}", [])


class _FakeRunResult:
    def __init__(self, output: str, new_messages: list[Any]) -> None:
        self.output = output
        self._new_messages = new_messages

    def new_messages(self) -> list[Any]:
        return list(self._new_messages)


class _FakeAMCClient:
    def __init__(self) -> None:
        self.send_calls: list[dict[str, Any]] = []
        self.mark_read_calls: list[list[str]] = []

    async def send(
        self,
        channel_id: str,
        text: str,
        *,
        reply_to_message_id: str | None = None,
        approval: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        self.send_calls.append(
            {
                "channel_id": channel_id,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
            }
        )
        return {
            "message_id": f"out-{len(self.send_calls)}",
            "channel_id": channel_id,
        }

    async def mark_read(self, message_ids: Any) -> Any:
        ids = list(message_ids)
        self.mark_read_calls.append(ids)
        return {"marked_read": ids, "already_read": []}


def _envelope(
    *,
    message_id: str,
    channel_id: str,
    text: str,
    sender_id: str = "+15551234567",
) -> AMCInboundEnvelope:
    return AMCInboundEnvelope(
        id=message_id,
        channel_id=channel_id,
        sender_id=sender_id,
        text=text,
        ts="2026-05-11T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# DBOS fake — records every ``send_async`` call.
# ---------------------------------------------------------------------------


class _DbosSendRecorder:
    """Records every ``DBOS.send_async`` invocation for assertions.

    Monkey-patched in via the ``dbos_send_recorder`` fixture. Defaults to a
    successful no-op; tests can set ``raise_exc`` to simulate "workflow not
    found in DBOS" / similar failures.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, str]] = []
        self.raise_exc: BaseException | None = None

    async def send_async(self, workflow_id: str, payload: Any, topic: str) -> None:
        self.calls.append((workflow_id, payload, topic))
        if self.raise_exc is not None:
            raise self.raise_exc

    # Sync fallback for the BLE001 fallback in
    # :func:`capo.transport.dispatcher._send_approval_decision`. Mirrors the
    # async raise semantics: when ``raise_exc`` is set, BOTH paths raise so
    # the dispatcher's outer except catches the failure.
    def send(self, workflow_id: str, payload: Any, topic: str) -> None:
        self.calls.append((workflow_id, payload, topic))
        if self.raise_exc is not None:
            raise self.raise_exc


@pytest.fixture
def dbos_send_recorder(monkeypatch: pytest.MonkeyPatch) -> _DbosSendRecorder:
    """Replace ``dbos.DBOS.send_async`` with a recorder for the duration of a test."""
    rec = _DbosSendRecorder()
    from dbos import DBOS

    monkeypatch.setattr(DBOS, "send_async", rec.send_async, raising=False)
    monkeypatch.setattr(DBOS, "send", rec.send, raising=False)
    return rec


# ---------------------------------------------------------------------------
# Settings / deps / conn factory fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    cfg = _write_config(tmp_path)
    return Settings.load(cfg, env_override=dict(_VALID_ENV))


@pytest.fixture
def conn_factory(tmp_path: Path) -> Iterator[Callable[[], sqlite3.Connection]]:
    db_path = tmp_path / "state.db"
    boot = sqlite3.connect(str(db_path))
    boot.execute("PRAGMA foreign_keys = ON")
    _apply_schema(boot)
    boot.execute("INSERT INTO users(user_id) VALUES ('owner')")
    boot.commit()
    boot.close()

    opened: list[sqlite3.Connection] = []

    def _factory() -> sqlite3.Connection:
        c = sqlite3.connect(
            str(db_path), isolation_level=None, check_same_thread=False
        )
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("PRAGMA busy_timeout = 5000")
        opened.append(c)
        return c

    yield _factory
    for c in opened:
        with contextlib.suppress(Exception):
            c.close()


def _fake_deps_factory(settings: Settings) -> Callable[[str], CapoDeps]:
    def _f(user_id: str) -> CapoDeps:
        return CapoDeps(
            settings=settings,
            http_client=None,  # type: ignore[arg-type]
            user_id=user_id,
        )

    return _f


def _seed_approval(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    approval_id: str,
    parent_thread_id: str,
    status: str = "pending",
    workflow_id: str | None = "wf-1",
    requested_at: str = "2026-05-11T12:00:00Z",
) -> None:
    conn = conn_factory()
    try:
        conn.execute(
            """
            INSERT INTO approvals (
                approval_id, requested_at, request_type, request_payload,
                status, requester_user_id, parent_thread_id, workflow_id
            ) VALUES (?, ?, 'shell_exec', '{}', ?, 'owner', ?, ?)
            """,
            (approval_id, requested_at, status, parent_thread_id, workflow_id),
        )
        conn.commit()
    finally:
        conn.close()


async def _drain(worker: ChannelWorker) -> None:
    await asyncio.wait_for(worker._queue.join(), timeout=5.0)


# ===========================================================================
# Unit: slash-command parser detection (the dispatcher reuses Task #41's
# ``parse_slash_command``; these tests pin the integration contract).
# ===========================================================================


def test_parser_detects_approve_with_id() -> None:
    cmd = parse_slash_command(f"/approve {uuid.uuid4().hex}")
    assert cmd is not None
    assert cmd.verb == "approve"
    assert cmd.approval_id is not None


def test_parser_detects_deny_with_id() -> None:
    cmd = parse_slash_command(f"/deny {uuid.uuid4().hex}")
    assert cmd is not None
    assert cmd.verb == "deny"
    assert cmd.approval_id is not None


def test_parser_detects_approve_without_id() -> None:
    cmd = parse_slash_command("/approve")
    assert cmd is not None
    assert cmd.verb == "approve"
    assert cmd.approval_id is None


def test_parser_returns_none_for_natural_language() -> None:
    assert parse_slash_command("hey can you approve that?") is None
    assert parse_slash_command("approve") is None  # no leading slash


# ===========================================================================
# Integration: /approve <id> → DBOS.send fires.
# ===========================================================================


async def test_approve_with_id_fires_dbos_send_and_ack(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:c1",
        workflow_id="wf-abc",
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()

    try:
        ok = await dispatcher.enqueue(
            _envelope(
                message_id="m-approve-1",
                channel_id="c1",
                text=f"/approve {approval_id}",
            )
        )
        assert ok
        worker = dispatcher._workers["c1"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    # The agent MUST NOT have been invoked — approval is pre-agent.
    assert agent.calls == []

    # Exactly one DBOS.send_async to the workflow id.
    assert len(dbos_send_recorder.calls) == 1
    wf_id, payload, topic = dbos_send_recorder.calls[0]
    assert wf_id == "wf-abc"
    assert topic == APPROVAL_TOPIC
    assert isinstance(payload, dict)
    assert payload["status"] == "approve"
    assert payload["resolved_by"] == "owner"
    assert payload["reason"] is None

    # AMC ack sent + mark_read.
    assert len(amc.send_calls) == 1
    assert amc.send_calls[0]["text"] == format_approval_recorded_reply(
        approval_id
    )
    assert amc.send_calls[0]["channel_id"] == "c1"
    assert amc.mark_read_calls == [["m-approve-1"]]


async def test_deny_with_id_fires_dbos_send_with_deny_status(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:c2",
        workflow_id="wf-deny",
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-deny-1",
                channel_id="c2",
                text=f"/deny {approval_id} not safe right now",
            )
        )
        worker = dispatcher._workers["c2"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert len(dbos_send_recorder.calls) == 1
    _wf, payload, _topic = dbos_send_recorder.calls[0]
    assert payload["status"] == "deny"
    assert payload["reason"] == "not safe right now"


# ===========================================================================
# Integration: /approve (no id) → most-recent pending lookup.
# ===========================================================================


async def test_approve_no_id_resolves_most_recent_pending_in_thread(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    # Two pending approvals in the same thread; the most-recent one wins.
    older = uuid.uuid4().hex
    newer = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=older,
        parent_thread_id="amc:cA",
        workflow_id="wf-older",
        requested_at="2026-05-11T11:00:00Z",
    )
    _seed_approval(
        conn_factory,
        approval_id=newer,
        parent_thread_id="amc:cA",
        workflow_id="wf-newer",
        requested_at="2026-05-11T12:00:00Z",
    )
    # An unrelated pending approval in a different thread that must NOT
    # be picked up.
    other = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=other,
        parent_thread_id="amc:cB",
        workflow_id="wf-other",
        requested_at="2026-05-11T13:00:00Z",
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-bare",
                channel_id="cA",
                text="/approve",
            )
        )
        worker = dispatcher._workers["cA"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert len(dbos_send_recorder.calls) == 1
    wf_id, _payload, _topic = dbos_send_recorder.calls[0]
    assert wf_id == "wf-newer"
    # The ack carries the resolved approval id, not the user-supplied one
    # (which was absent).
    assert amc.send_calls[0]["text"] == format_approval_recorded_reply(newer)


async def test_approve_no_id_no_pending_replies_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-empty",
                channel_id="cZ",
                text="/approve",
            )
        )
        worker = dispatcher._workers["cZ"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert dbos_send_recorder.calls == []
    assert amc.send_calls[0]["text"] == APPROVAL_REPLY_NO_PENDING
    assert amc.mark_read_calls == [["m-empty"]]


# ===========================================================================
# Integration: /approve <unknown-id> → friendly reply.
# ===========================================================================


async def test_approve_unknown_id_replies_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-unknown",
                channel_id="cQ",
                # Valid id shape but not present in the DB.
                text=f"/approve {uuid.uuid4().hex}",
            )
        )
        worker = dispatcher._workers["cQ"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert dbos_send_recorder.calls == []
    assert amc.send_calls[0]["text"] == APPROVAL_REPLY_UNKNOWN_ID


# ===========================================================================
# Integration: /approve already-resolved → friendly reply (idempotent).
# ===========================================================================


async def test_approve_already_resolved_replies_friendly_and_does_not_send(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:cR",
        status="approved",  # already terminal
        workflow_id="wf-old",
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-r1",
                channel_id="cR",
                text=f"/approve {approval_id}",
            )
        )
        worker = dispatcher._workers["cR"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    # Critical: no decision is re-sent on idempotent retry.
    assert dbos_send_recorder.calls == []
    assert amc.send_calls[0]["text"] == APPROVAL_REPLY_ALREADY_RESOLVED


async def test_repeated_approve_for_same_id_is_idempotent(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    """First /approve fires DBOS.send; row flips terminal (we simulate by
    UPDATE after first call); second /approve replies 'already resolved'
    and does NOT re-send."""
    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:cI",
        workflow_id="wf-idem",
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        # First /approve.
        await dispatcher.enqueue(
            _envelope(
                message_id="m-i1",
                channel_id="cI",
                text=f"/approve {approval_id}",
            )
        )
        worker = dispatcher._workers["cI"]
        await _drain(worker)

        # Simulate the workflow having UPDATEd the row to terminal.
        conn = conn_factory()
        try:
            conn.execute(
                "UPDATE approvals SET status='approved', decided_at='now' "
                "WHERE approval_id = ?",
                (approval_id,),
            )
            conn.commit()
        finally:
            conn.close()

        # Second /approve.
        await dispatcher.enqueue(
            _envelope(
                message_id="m-i2",
                channel_id="cI",
                text=f"/approve {approval_id}",
            )
        )
        await _drain(worker)
    finally:
        await dispatcher.stop()

    # Exactly ONE DBOS.send_async total across two /approve calls.
    assert len(dbos_send_recorder.calls) == 1
    # First reply is the ack; second reply is "already resolved".
    assert amc.send_calls[0]["text"] == format_approval_recorded_reply(approval_id)
    assert amc.send_calls[1]["text"] == APPROVAL_REPLY_ALREADY_RESOLVED


# ===========================================================================
# Edge: workflow_id NULL race (row inserted, wf id not yet persisted).
# ===========================================================================


async def test_approve_with_null_workflow_id_retries_then_friendly(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If workflow_id is NULL (race), the dispatcher waits + retries once;
    if still NULL it replies with the friendly 'still being created' string
    and does NOT call DBOS.send."""
    # Make the 0.5s sleep effectively instant so the test stays fast.
    real_sleep = asyncio.sleep

    async def _fast_sleep(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr("capo.transport.dispatcher.asyncio.sleep", _fast_sleep)

    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:cN",
        workflow_id=None,  # the race state
    )

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-null-wf",
                channel_id="cN",
                text=f"/approve {approval_id}",
            )
        )
        worker = dispatcher._workers["cN"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    assert dbos_send_recorder.calls == []
    assert amc.send_calls[0]["text"] == APPROVAL_REPLY_STILL_CREATING


# ===========================================================================
# Edge: workflow not found in DBOS (already completed / orphaned).
# ===========================================================================


async def test_approve_workflow_not_found_in_dbos_replies_already_resolved(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    """If DBOS.send_async raises (workflow already completed/orphaned), the
    dispatcher surfaces the friendly 'already resolved' reply rather than
    the generic internal-error string."""
    approval_id = uuid.uuid4().hex
    _seed_approval(
        conn_factory,
        approval_id=approval_id,
        parent_thread_id="amc:cF",
        workflow_id="wf-orphan",
    )

    class _NotFound(Exception):
        pass

    dbos_send_recorder.raise_exc = _NotFound("workflow already completed")

    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-orphan",
                channel_id="cF",
                text=f"/approve {approval_id}",
            )
        )
        worker = dispatcher._workers["cF"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert agent.calls == []
    # The dispatcher attempted to send (and the sync fallback also attempts
    # since the async raise triggers the BLE001 fallback in
    # ``_send_approval_decision``). The recorder records both — we don't
    # assert on len here, only on the reply being friendly.
    assert amc.send_calls[0]["text"] == APPROVAL_REPLY_ALREADY_RESOLVED


# ===========================================================================
# Non-slash messages flow through to the agent (regression).
# ===========================================================================


async def test_non_slash_message_runs_agent(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    dbos_send_recorder: _DbosSendRecorder,
) -> None:
    agent = _FakeAgent()
    amc = _FakeAMCClient()
    dispatcher = Dispatcher(
        settings,
        agent,
        amc,  # type: ignore[arg-type]
        conn_factory,
        deps_factory=_fake_deps_factory(settings),
    )
    await dispatcher.start()
    try:
        await dispatcher.enqueue(
            _envelope(
                message_id="m-agent",
                channel_id="cAgent",
                text="hello there, can you help?",
            )
        )
        worker = dispatcher._workers["cAgent"]
        await _drain(worker)
    finally:
        await dispatcher.stop()

    assert len(agent.calls) == 1
    assert agent.calls[0][0] == "hello there, can you help?"
    assert dbos_send_recorder.calls == []
    # The dispatcher sent the agent's reply.
    assert amc.send_calls[0]["text"] == "agent:hello there, can you help?"

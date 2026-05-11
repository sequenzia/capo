"""Phase 5 checkpoint integration tests (spec §9.5 + §5.9 + §5.10 + §5.11
+ §5.12 + §6.5 + §8.1).

This module anchors the Phase 5 checkpoint gate (§9.5) for the V1
**cost-caps + observability + polish** contract. Phase 5 is the final
gate before V1 — it verifies the operator surface (cost caps,
``/healthz``, compaction, retention pruning, session-control commands)
end-to-end through the dispatcher and the maintenance scheduler.

The §9.5 checkpoint gate has six in-process rows + an operator-driven
seventh row (Litestream paired-restore drill). The in-process rows are:

  * **Cost-cap round-trip** (§5.9, §9.5 row 2): a single user
    accumulates spend; the next inbound envelope crosses the hard cap →
    dispatcher refuses the turn with the §5.9-mandated friendly message.
    On replay with ``/override`` armed, the same envelope passes through
    with an "override consumed" prefix. A soft-cap-only crossing prepends
    the soft-warn prefix and still lets the agent run.

  * **/healthz endpoint** (§5.12, §9.5 row 7): ``GET /healthz`` returns
    200 with status="ok" when all critical probes pass; returns 503 with
    status="degraded" when DBOS is not launched.

  * **Compaction round-trip** (§5.7, §9.5 row 5): a thread accumulates
    messages past the ``[compaction].threshold_messages`` threshold; a
    direct ``compact_thread_async`` call replaces the older messages
    with a summary while preserving the most recent
    ``keep_recent_messages`` verbatim. A subsequent ``load_history`` (the
    same call the dispatcher makes on the next agent turn) returns
    ``[summary, kept_1, ..., kept_N]`` in natural order.

  * **Retention pruning** (§5.7, §8.4, §9.5 row 6): a scheduler tick
    invokes :func:`capo.maintenance.retention.prune_delegation_output`
    against a database carrying a mix of stale + fresh + active-
    delegation output rows. Stale terminal-delegation rows are deleted;
    rows belonging to ``running`` / ``pending_approval`` delegations
    are preserved regardless of age.

  * **Session command + NL tool sanity** (§5.10, §9.5 row 4): the
    end-to-end ``/new`` / ``/status`` / ``/clear`` / ``/override`` slash
    paths are covered by ``tests/test_dispatcher_session_commands.py``
    (Task #52) and the NL ``session_new`` / ``session_status`` /
    ``session_clear`` agent tools are covered by
    ``tests/test_session_tools.py`` (Task #53). This module re-asserts
    a thin integration slice — dispatcher handles ``/override`` without
    invoking the agent and arms the per-thread cost-override sentinel.

The operator-driven row (Litestream paired-restore drill + launchd boot
drill + caffeinate observation) is wall-clock-bound and infeasible in
CI; it is documented in
``internal/specs/spikes/S-7-phase5-checkpoint.md``.

Scaffolding follows the Phase 4 checkpoint pattern
(:mod:`tests.test_phase4_checkpoint`): per-test ``tmp_path`` ``state.db``
+ ``dbos.db`` with inline DDL, DBOS launched per-test where needed,
``AmcStub``-style fake AMC client where the dispatcher is exercised.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util as _ilu
import sqlite3
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from capo.config import Settings
from capo.deps import CapoDeps
from capo.maintenance.retention import (
    _run_one_prune,
    prune_delegation_output,
)
from capo.memory import store
from capo.memory.compaction import compact_thread_async
from capo.memory.conversation import append_messages, load_history
from capo.transport.amc_client import AMCInboundEnvelope
from capo.transport.amc_listener import build_app
from capo.transport.dispatcher import (
    ChannelWorker,
    Dispatcher,
)
from capo.transport.health import (
    CRITICAL_PROBES,
    PROBE_NAMES,
    register_health_endpoint,
)
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from tests.test_config import _VALID_ENV, _write_config

# ---------------------------------------------------------------------------
# Migration loader — re-use the verbatim DDL from the alembic files so the
# tests stay aligned with the real schema (same shape as the Phase 4
# checkpoint suite).
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_001 = _REPO_ROOT / "migrations" / "versions" / "001_init.py"
_MIG_002 = _REPO_ROOT / "migrations" / "versions" / "002_approvals.py"
_MIG_003 = _REPO_ROOT / "migrations" / "versions" / "003_approvals_request_types.py"
_MIG_004 = _REPO_ROOT / "migrations" / "versions" / "004_costs.py"

sys.path.insert(0, str(_MIG_001.parent))


def _load_migration(path: Path, name: str) -> Any:
    spec = _ilu.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mig_001 = _load_migration(_MIG_001, "_mig_001_phase5")
_mig_002 = _load_migration(_MIG_002, "_mig_002_phase5")
_mig_003 = _load_migration(_MIG_003, "_mig_003_phase5")
_mig_004 = _load_migration(_MIG_004, "_mig_004_phase5")


def _apply_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig_001._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_002._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_003._UPGRADE_STATEMENTS:
        conn.execute(stmt)
    for stmt in _mig_004._UPGRADE_STATEMENTS:
        conn.execute(stmt)


# ---------------------------------------------------------------------------
# Fakes — dispatcher integration scaffolding (same as Task #49 tests).
# ---------------------------------------------------------------------------


class _FakeAgent:
    """Records every invocation so we can assert agent-called-or-not."""

    def __init__(self, reply: str = "hello from agent") -> None:
        self.calls: list[tuple[str, list[Any], CapoDeps]] = []
        self._reply = reply

    async def run(
        self,
        text: str,
        *,
        message_history: list[Any] | None = None,
        deps: CapoDeps,
    ) -> Any:
        self.calls.append((text, list(message_history or []), deps))
        return _FakeRunResult(self._reply, [])


class _FakeRunResult:
    def __init__(self, output: str, new_messages: list[Any]) -> None:
        self.output = output
        self._new_messages = new_messages

    def new_messages(self) -> list[Any]:
        return list(self._new_messages)


class _FakeAMCClient:
    """Captures send + mark_read calls; covers the surface Dispatcher uses."""

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
# Fixtures — tmp_path-scoped Settings + schema-applied state.db.
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Real Settings with budget soft=25, hard=75 (from the test TOML).

    Paths are remapped into ``tmp_path`` so the dispatcher's writes hit
    a private database — no collision with the developer's ~/.capo/.
    """
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    object.__setattr__(s.paths, "db_path", tmp_path / "state.db")
    object.__setattr__(s.paths, "dbos_db_path", tmp_path / "dbos.db")
    return s


@pytest.fixture
def conn_factory(
    settings: Settings,
) -> Iterator[Callable[[], sqlite3.Connection]]:
    """Apply the four-migration schema once, then hand out fresh connections."""
    db_path = settings.paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
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


def _seed_spend(
    conn_factory: Callable[[], sqlite3.Connection],
    *,
    user_id: str,
    total_usd: float,
    day_iso: str = "2026-05-11",
) -> None:
    """Insert a costs row so ``daily_total_usd`` returns ``total_usd``."""
    conn = conn_factory()
    try:
        conn.execute(
            "INSERT INTO costs(recorded_at, user_id, parent_thread_id, model, "
            "input_tokens, output_tokens, cached_tokens, cost_usd, span_id) "
            "VALUES (?, ?, ?, 'claude-opus-4-7', 0, 0, 0, ?, NULL)",
            (f"{day_iso}T12:00:00Z", user_id, "amc:test", total_usd),
        )
        conn.commit()
    finally:
        conn.close()


def _pin_today(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin ``_utc_today_iso`` inside capo.budget to ``2026-05-11``.

    The TOML caps live at soft=25/hard=75 USD; we seed cost rows at the
    same UTC date so the dispatcher's hook sees them.
    """
    import capo.budget as budget_mod

    fixed = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)

    def _fake_today() -> str:
        return fixed.strftime("%Y-%m-%d")

    monkeypatch.setattr(budget_mod, "_utc_today_iso", _fake_today)


async def _drain(worker: ChannelWorker) -> None:
    await asyncio.wait_for(worker._queue.join(), timeout=5.0)


@pytest.fixture(autouse=True)
def _reset_observability_and_dbos() -> Iterator[None]:
    """Logfire + DBOS are process-globals; reset between Phase 5 tests."""
    from capo.observability import _reset_for_tests

    _reset_for_tests()
    yield
    _reset_for_tests()
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


# ===========================================================================
# Test 1 — Cost-cap integration (hard block + override + soft warn)
# ===========================================================================
#
# Spec anchors: §5.9 (Cost Caps) + §5.10 (`/override`) + §6.5 (budget
# spans) + §9.5 checkpoint row 2.
#
# Sequence:
#   t=0    seed costs at $100 (well past the $75 hard cap).
#   t=1    enqueue inbound text → check_budget fires → hard_block →
#          friendly refusal sent. Agent NEVER invoked.
#   t=2    enqueue ``/override`` → ChannelWorker arms the per-thread
#          sentinel, the dispatcher acks; agent still NOT invoked.
#   t=3    enqueue the same prompt again → overridden path → agent
#          invoked exactly once; reply carries the override prefix;
#          sentinel consumed.
#   t=4    drop spend to $30 (soft-only); enqueue another envelope on
#          a fresh thread → soft_warn → agent invoked; reply carries
#          the soft-cap heads-up prefix.
# ===========================================================================


async def test_cost_cap_hard_block_override_and_soft_warn(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§9.5 row 2: hard cap → blocked; /override → executes; soft → warn."""
    _pin_today(monkeypatch)
    _seed_spend(conn_factory, user_id="owner", total_usd=100.0)

    agent = _FakeAgent(reply="agent answer")
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
        # --- Hard-block path -----------------------------------------
        await dispatcher.enqueue(
            _envelope(
                message_id="m-hb-1",
                channel_id="cHB",
                text="please summarize the readme",
            )
        )
        worker_hb = dispatcher._workers["cHB"]
        await _drain(worker_hb)

        # Agent NOT invoked.
        assert agent.calls == []
        # Friendly refusal reply with cap + total + /override hint.
        assert len(amc.send_calls) == 1
        refusal = amc.send_calls[0]["text"]
        assert "$100.00" in refusal
        assert "$75.00" in refusal
        assert "/override" in refusal
        # Marked-read so AMC stops redelivering.
        assert amc.mark_read_calls == [["m-hb-1"]]

        # --- /override + replay → overridden path --------------------
        await dispatcher.enqueue(
            _envelope(
                message_id="m-ov-1", channel_id="cHB", text="/override"
            )
        )
        await _drain(worker_hb)
        assert worker_hb.is_cost_override_armed("amc:cHB") is True
        # Slash command path also skipped the agent.
        assert agent.calls == []

        await dispatcher.enqueue(
            _envelope(
                message_id="m-hb-2",
                channel_id="cHB",
                text="please summarize the readme",
            )
        )
        await _drain(worker_hb)

        # Agent invoked exactly once; sentinel consumed.
        assert len(agent.calls) == 1
        override_consumed = not worker_hb.is_cost_override_armed("amc:cHB")
        assert override_consumed
        # Last AMC reply carries the override note + agent reply.
        last_reply = amc.send_calls[-1]["text"]
        assert "agent answer" in last_reply
        assert "$100.00" in last_reply  # daily spend named
        assert "$75.00" in last_reply  # hard cap named

        # --- Soft-warn path on a fresh thread ------------------------
        # Wipe the $100 row and seed a $30 row (soft cap=$25, hard=$75).
        wipe = conn_factory()
        try:
            wipe.execute("DELETE FROM costs")
            wipe.commit()
        finally:
            wipe.close()
        _seed_spend(conn_factory, user_id="owner", total_usd=30.0)

        await dispatcher.enqueue(
            _envelope(
                message_id="m-sw-1", channel_id="cSW", text="hello"
            )
        )
        worker_sw = dispatcher._workers["cSW"]
        await _drain(worker_sw)

        # Agent called for the second time overall.
        assert len(agent.calls) == 2
        soft_reply = amc.send_calls[-1]["text"]
        assert "$30.00" in soft_reply  # spend named
        assert "$25.00" in soft_reply  # soft cap named
        assert "agent answer" in soft_reply
        # Prefix appears BEFORE the agent text.
        assert soft_reply.index("$30.00") < soft_reply.index("agent answer")
    finally:
        await dispatcher.stop()


# ===========================================================================
# Test 2 — /healthz: 200 healthy + 503 when DBOS not launched
# ===========================================================================
#
# Spec anchors: §5.12 (Health Check) + §6.5 (observability) + §9.5
# checkpoint row 7.
#
# Sequence A (200 happy):
#   t=0    create state.db on disk; init_dbos + launch_dbos.
#   t=1    GET /healthz → 200, body status="ok", all critical probes ok.
#
# Sequence B (503 degraded):
#   t=0    state.db exists, DBOS NOT launched.
#   t=1    GET /healthz → 503, status="degraded", dbos_launched=false.
# ===========================================================================


class _FakeDispatcher:
    async def enqueue(self, envelope: Any) -> bool:
        return True


async def test_healthz_200_when_all_critical_pass_and_503_when_dbos_down(
    settings: Settings,
) -> None:
    """§9.5 row 7: 200 with all probes passing; 503 with DBOS not launched."""
    # Set up state.db so the state_db probe is ok (FK-on, basic SELECT 1).
    db_path = settings.paths.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _probe (x INTEGER)")
    finally:
        conn.close()

    # --- Sequence B (503): NO DBOS launched ---------------------------
    if is_launched():
        pytest.skip("DBOS leaked from a prior test; cannot test 'not launched'")
    app_b = build_app(settings, _FakeDispatcher())
    register_health_endpoint(app_b, settings, amc_client=None)
    transport_b = httpx.ASGITransport(app=app_b)
    async with httpx.AsyncClient(
        transport=transport_b, base_url="http://testserver"
    ) as client:
        resp_b = await client.get("/healthz")
    assert resp_b.status_code == 503, resp_b.text
    body_b = resp_b.json()
    assert body_b["status"] == "degraded"
    by_name_b = {p["name"]: p for p in body_b["probes"]}
    assert by_name_b["dbos_launched"]["ok"] is False
    # Response shape contract from §5.12.
    assert set(by_name_b.keys()) == set(PROBE_NAMES)

    # --- Sequence A (200): DBOS launched + critical probes ok --------
    init_dbos(settings)
    await asyncio.to_thread(launch_dbos)
    try:
        app_a = build_app(settings, _FakeDispatcher())
        register_health_endpoint(app_a, settings, amc_client=None)
        transport_a = httpx.ASGITransport(app=app_a)
        async with httpx.AsyncClient(
            transport=transport_a, base_url="http://testserver"
        ) as client:
            resp_a = await client.get("/healthz")
        assert resp_a.status_code == 200, resp_a.text
        body_a = resp_a.json()
        assert body_a["status"] == "ok"
        by_name_a = {p["name"]: p for p in body_a["probes"]}
        for critical in CRITICAL_PROBES:
            assert by_name_a[critical]["ok"] is True, (
                f"{critical} should be ok: {by_name_a[critical]}"
            )
    finally:
        await asyncio.to_thread(destroy_dbos)


# ===========================================================================
# Test 3 — Compaction: thread past threshold compacted on next turn,
# keep_recent rows still visible to the agent.
# ===========================================================================
#
# Spec anchors: §5.7 (Conversation Memory + Hybrid Compaction) + §9.5
# checkpoint row 5.
#
# Sequence:
#   t=0    seed 20 messages into the conversation_history rows for
#          (user_id="alice", thread_id="amc:c1", session_id="s1").
#   t=1    invoke compact_thread_async with a stub summarizer (cheap
#          model). Threshold tripped → DELETE+INSERT atomically.
#   t=2    load_history (the same call the dispatcher makes on the
#          next agent turn) returns [summary, kept_1, ..., kept_10].
#          The 10 most recent message bodies appear verbatim in the
#          loaded history (keep_recent contract).
# ===========================================================================


class _StubCompactionCfg:
    """Inline compaction config (test_compaction.py shape)."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        threshold_messages: int = 10,
        threshold_tokens: int = 50_000,
        keep_recent_messages: int = 10,
    ) -> None:
        self.enabled = enabled
        self.threshold_messages = threshold_messages
        self.threshold_tokens = threshold_tokens
        self.keep_recent_messages = keep_recent_messages
        self.preserve_delegation_handles = True


class _CompactionStubSettings:
    def __init__(self, cfg: _StubCompactionCfg) -> None:
        self.compaction = cfg


class _StubSummarizer:
    """Mimics Pydantic AI Agent.run for compaction (Task #54 shape)."""

    def __init__(self, output: str = "summary of older chat") -> None:
        self.output = output
        self.calls: list[str] = []

    async def run(self, prompt: str) -> Any:
        self.calls.append(prompt)

        class _R:
            pass

        r = _R()
        r.output = self.output  # type: ignore[attr-defined]
        return r


def _seed_compaction_messages(db_path: Path, n_pairs: int) -> int:
    """Seed (user='alice', thread='amc:c1', session='s1') with N pairs."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    def _make_pair(i: int) -> list[Any]:
        return [
            ModelRequest(parts=[UserPromptPart(content=f"user-msg-{i}")]),
            ModelResponse(parts=[TextPart(content=f"assistant-reply-{i}")]),
        ]

    conn = store.open_connection(db_path)
    try:
        # Set up the foreign-key prerequisites.
        conn.execute(
            "INSERT OR IGNORE INTO users(user_id) VALUES ('alice')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO sessions(session_id, thread_id, user_id) "
            "VALUES (?, ?, ?)",
            ("s1", "amc:c1", "alice"),
        )
        total = 0
        for i in range(n_pairs):
            total += append_messages(
                conn, "alice", "amc:c1", "s1", _make_pair(i)
            )
        return total
    finally:
        conn.close()


async def test_compaction_triggers_and_preserves_keep_recent(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """§9.5 row 5: thread > threshold → compacted; agent sees keep_recent."""
    # Schema is set up by ``conn_factory`` fixture (which inserts user
    # 'owner'); we add 'alice' for the compaction scenario.
    db_path = settings.paths.db_path
    seeded = _seed_compaction_messages(db_path, n_pairs=10)
    assert seeded == 20

    # Threshold is met (20 > 10), keep_recent=10 → 10 summarized + 10 kept.
    cfg = _StubCompactionCfg(
        threshold_messages=10, keep_recent_messages=10
    )
    cstub = _CompactionStubSettings(cfg)
    summarizer = _StubSummarizer(output="recap of older chat")

    result = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=cstub,
    )
    assert result.compacted is True
    assert result.messages_summarized == 10
    assert result.messages_kept == 10
    assert summarizer.calls, "summarizer must be invoked"

    # The same load_history call the dispatcher makes next turn must see
    # [summary, kept_0, ..., kept_9] in natural order.
    conn = store.open_connection(db_path)
    try:
        history = load_history(conn, "alice", "amc:c1", "s1")
    finally:
        conn.close()
    # 1 summary + 10 verbatim kept rows = 11 logical entries; the count
    # of decoded messages may differ depending on serialization (each
    # row encodes a single message), but the kept user/assistant pairs
    # must still be visible verbatim.
    flat = []
    for msg in history:
        for part in getattr(msg, "parts", []):
            text = getattr(part, "content", None)
            if isinstance(text, str):
                flat.append(text)

    # Summary preamble appears at the head.
    assert any(s.startswith("[Compacted:") for s in flat)
    assert any("recap of older chat" in s for s in flat)
    # All 10 most recent user-messages + assistant-replies are visible.
    for i in range(5, 10):
        assert f"user-msg-{i}" in flat
        assert f"assistant-reply-{i}" in flat
    # Older summarized messages are gone from the natural-language view.
    # (They live inside the summary string, not as standalone messages.)
    for i in range(0, 5):
        assert f"user-msg-{i}" not in flat
        assert f"assistant-reply-{i}" not in flat


# ===========================================================================
# Test 4 — Retention pruning: scheduler tick prunes stale rows but
# preserves rows belonging to active (running/pending_approval)
# delegations.
# ===========================================================================
#
# Spec anchors: §5.7 (retention) + §8.4 + §9.5 checkpoint row 6.
#
# Sequence:
#   t=0    seed 4 delegations: completed-stale, completed-fresh,
#          running-stale, pending_approval-stale.
#   t=1    each delegation owns 2 delegation_output rows at the stale ts
#          (35 days old) or fresh ts (3 days old).
#   t=2    invoke _run_one_prune (the scheduler's per-tick work).
#   t=3    verify rows attached to terminal delegations past the
#          7-day window are gone; rows attached to active delegations
#          (running / pending_approval) are preserved regardless of age.
# ===========================================================================


def _insert_delegation_row(
    db_path: Path,
    *,
    delegation_id: str,
    status: str,
    user_id: str = "owner",
) -> None:
    conn = store.open_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO delegations (id, user_id, agent, workspace, "
            "brief_json, status, parent_thread_id) "
            "VALUES (?, ?, 'claude-code', '/tmp/ws', '{}', ?, 'amc:c1')",
            (delegation_id, user_id, status),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_output_row(
    db_path: Path,
    *,
    delegation_id: str,
    ts: datetime,
    chunk: str = "stdout-chunk",
) -> None:
    conn = store.open_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO delegation_output (delegation_id, ts, stream, chunk) "
            "VALUES (?, ?, 'stdout', ?)",
            (
                delegation_id,
                ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f"),
                chunk,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _count_output_for(
    conn_factory: Callable[[], sqlite3.Connection], delegation_id: str
) -> int:
    conn = conn_factory()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM delegation_output WHERE delegation_id = ?",
            (delegation_id,),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


async def test_retention_scheduler_tick_prunes_terminal_preserves_active(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """§9.5 row 6: scheduler tick prunes terminal-stale, keeps active."""
    db_path = settings.paths.db_path
    # The TOML test config sets retention.delegation_output_days = 7.
    assert settings.retention.delegation_output_days == 7

    now_utc = datetime(2026, 5, 11, 12, 0, 0, tzinfo=UTC)
    stale_ts = now_utc - timedelta(days=35)
    fresh_ts = now_utc - timedelta(days=3)

    # 4 delegations with output rows.
    _insert_delegation_row(db_path, delegation_id="d-comp-stale", status="completed")
    _insert_output_row(db_path, delegation_id="d-comp-stale", ts=stale_ts)
    _insert_output_row(db_path, delegation_id="d-comp-stale", ts=stale_ts)

    _insert_delegation_row(db_path, delegation_id="d-comp-fresh", status="completed")
    _insert_output_row(db_path, delegation_id="d-comp-fresh", ts=fresh_ts)
    _insert_output_row(db_path, delegation_id="d-comp-fresh", ts=fresh_ts)

    _insert_delegation_row(db_path, delegation_id="d-running", status="running")
    _insert_output_row(db_path, delegation_id="d-running", ts=stale_ts)
    _insert_output_row(db_path, delegation_id="d-running", ts=stale_ts)

    _insert_delegation_row(
        db_path, delegation_id="d-pending", status="pending_approval"
    )
    _insert_output_row(db_path, delegation_id="d-pending", ts=stale_ts)
    _insert_output_row(db_path, delegation_id="d-pending", ts=stale_ts)

    # Sanity: 8 rows total before the tick.
    initial_count = _count_output_for(
        conn_factory, "d-comp-stale"
    ) + _count_output_for(
        conn_factory, "d-comp-fresh"
    ) + _count_output_for(
        conn_factory, "d-running"
    ) + _count_output_for(conn_factory, "d-pending")
    assert initial_count == 8

    # Drive the scheduler's per-tick work (same path as the nightly
    # cron-loop entry; bypassing the timer).
    pruned = await _run_one_prune(settings, now_factory=lambda: now_utc)
    assert pruned == 2  # only d-comp-stale's two rows are eligible

    # Stale terminal delegation: pruned.
    assert _count_output_for(conn_factory, "d-comp-stale") == 0

    # Fresh terminal: preserved (inside the 7-day window).
    assert _count_output_for(conn_factory, "d-comp-fresh") == 2

    # Active delegation (running): preserved regardless of row age.
    assert _count_output_for(conn_factory, "d-running") == 2

    # Active delegation (pending_approval): also preserved.
    assert _count_output_for(conn_factory, "d-pending") == 2

    # Also exercise the direct prune helper (idempotent — second call
    # is a no-op on the now-stable rows). This is the same SQL the
    # scheduler invokes under the hood.
    pruned_again = await asyncio.to_thread(
        prune_delegation_output, db_path, now_utc, 7
    )
    assert pruned_again == 0


# ===========================================================================
# Test 5 — Session command + NL tool integration sanity (re-assert green)
# ===========================================================================
#
# Spec anchors: §5.10 (slash commands + NL fallback) + §9.5 checkpoint
# row 4.
#
# The full surface lives in:
#   - tests/test_dispatcher_session_commands.py (Task #52: /new, /status,
#     /clear, /kill, /override).
#   - tests/test_session_tools.py (Task #53: session_new, session_status,
#     session_clear NL agent tools).
#
# This module runs a single representative integration slice:
# ``/override`` arms the per-thread cost-override sentinel WITHOUT
# invoking the agent. The slash dispatcher writes an AMC ack reply.
# This proves the Task #52 pre-agent intercept is wired into the live
# Dispatcher; the Task #53 NL counterpart is exercised in
# test_session_tools.py and not re-staged here.
# ===========================================================================


async def test_slash_override_arms_sentinel_zero_agent_calls(
    settings: Settings,
    conn_factory: Callable[[], sqlite3.Connection],
) -> None:
    """§9.5 row 4: ``/override`` arms sentinel without invoking the agent.

    Complements ``tests/test_dispatcher_session_commands.py`` (the full
    /new // status / clear / kill / override surface) and
    ``tests/test_session_tools.py`` (the NL agent-tool counterparts).
    """
    agent = _FakeAgent(reply="agent answer")
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
                message_id="m-or-1", channel_id="cOR", text="/override"
            )
        )
        worker = dispatcher._workers["cOR"]
        await _drain(worker)
        # Sentinel is now armed for the next agent turn.
        assert worker.is_cost_override_armed("amc:cOR") is True
    finally:
        await dispatcher.stop()

    # Zero-token: agent was NEVER invoked for the slash command.
    assert agent.calls == []
    # AMC received an ack reply (text wording asserted in Task #52 tests).
    assert len(amc.send_calls) == 1
    ack_reply = amc.send_calls[0]["text"]
    assert "override" in ack_reply.lower()

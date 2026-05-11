"""End-to-end Phase 1 integration test (§9.1 checkpoint).

Wires the full Phase 1 pipeline together with NO real AMC and NO real LLM
provider, and asserts that a single signed inbound webhook:

1. Loads + validates :class:`capo.config.Settings` from a real
   ``config.toml`` + ``.env`` on disk.
2. Survives :class:`capo.transport.amc_listener.build_app`'s HMAC verify and
   dedupe stages.
3. Returns ``204`` from the listener fast (well under the §6.1 1s budget).
4. Reaches the per-channel :class:`capo.transport.dispatcher.Dispatcher`
   worker.
5. Resolves the configured AMC sender → ``user_id`` via the §5.11 resolver.
6. Persists a brand-new ``sessions`` row (§5.7 session creation) and the
   agent's reply messages into ``conversation_history`` keyed on the
   correct ``user_id`` (§7.3).
7. Calls the (stubbed) AMC REST client's ``send`` and ``mark_read`` once
   each per envelope (the spec §5.2 worker contract).

The agent uses Pydantic AI's :class:`TestModel` (offline, no API key).
The AMC REST traffic is intercepted via :class:`httpx.MockTransport` so
nothing touches the network. The test is intentionally a *single* end-to-end
case — per-component edge cases live in the per-module tests
(``test_amc_listener.py``, ``test_dispatcher.py``, etc.).
"""

from __future__ import annotations

import importlib.util as _ilu
import json
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

from capo.config import Settings
from capo.transport.amc_client import AMCClient
from capo.transport.amc_listener import build_app
from capo.transport.dispatcher import Dispatcher
from tests.test_config import _VALID_ENV, _VALID_TOML, _write_config

# Pull in the same migration-statements pattern used by test_conversation /
# test_dispatcher so we get the canonical §7.3 schema without paying the
# alembic subprocess cost on every run. Also drop the S-4 harness dir on
# sys.path so we can import its ``signer`` reference (same approach used
# by tests/test_amc_listener.py).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_MIG_PATH = _REPO_ROOT / "migrations" / "versions" / "001_init.py"
_spec = _ilu.spec_from_file_location("_mig_001_init_e2e", _MIG_PATH)
assert _spec is not None and _spec.loader is not None
_mig = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mig)
_HARNESS_DIR = _REPO_ROOT / "internal" / "specs" / "spikes" / "S-4-harness"
if str(_HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(_HARNESS_DIR))
from signer import sign_request  # noqa: E402,I001  - sys.path tweak above


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


def _seed_db(db_path: Path, *, user_id: str) -> None:
    """Create the §7.3 schema and seed the ``users`` row referenced by the test."""
    c = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON")
    _apply_inline_schema(c)
    c.execute("INSERT INTO users(user_id) VALUES (?)", (user_id,))
    c.close()


def _build_amc_client_with_mock(record: dict[str, list]) -> AMCClient:
    """Build an :class:`AMCClient` whose REST calls are intercepted in-process.

    ``record`` accumulates the requests AMCClient makes so the test can
    assert send + mark_read were called exactly once each. ``get_unread``
    returns an empty list (boot sweep no-op) and ``send`` / ``mark_read``
    return their canonical success shapes from §7.5.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/messages/unread"):
            record["get_unread"].append(request)
            return httpx.Response(200, json={"messages": []})
        if path.endswith("/messages/send"):
            record["send"].append(request)
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "message_id": f"out-{uuid.uuid4()}",
                    "channel_id": payload["channel_id"],
                    "status": "sent",
                },
            )
        if path.endswith("/messages/mark_read"):
            record["mark_read"].append(request)
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={"marked_read": payload["message_ids"], "already_read": []},
            )
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND"}})

    settings_like = type(
        "AmcStub",
        (),
        {"base_url": "http://mock-amc.test", "agent_id": "capo"},
    )()
    from pydantic import SecretStr

    return AMCClient(
        base_url=settings_like.base_url,
        bearer_token=SecretStr("token-not-used-by-mock"),
        transport=httpx.MockTransport(_handler),
    )


# ---------------------------------------------------------------------------
# Test.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase1_e2e_signed_webhook_flows_through_full_pipeline(
    tmp_path: Path,
) -> None:
    """Full §9.1 Phase 1 pipeline: listener → dispatcher → memory + AMC send.

    Steps mirror the checkpoint gate criteria:

    * Settings.load(...) — boot-time validation rejects bad configs (covered
      by ``test_config.py``); here we drive the happy path so the rest of
      the pipeline composes.
    * dispatcher + AMCClient + listener composed end-to-end.
    * Signed webhook posted through the listener → 204 in well under 1s.
    * Dispatcher worker processes the envelope:
      - Resolves the configured AMC sender to ``user_id=owner`` (§5.11).
      - Calls TestModel agent (no API key, no network).
      - Persists messages into ``conversation_history`` (§5.7, §7.3).
      - Calls AMC ``send`` + ``mark_read`` exactly once via MockTransport.
    """
    # ---- 1. Materialize config.toml + .env on disk. ---------------------
    # We need an explicit ``[paths]`` pointing at a temp DB so the
    # dispatcher's conn factory works against a real on-disk SQLite — the
    # same shape production uses. Patch _VALID_TOML's paths block in place.
    db_path = tmp_path / "state.db"
    workspaces_root = tmp_path / "workspaces"
    projects_root = tmp_path / "projects"
    workspaces_root.mkdir()
    projects_root.mkdir()
    toml = _VALID_TOML.replace(
        '[paths]\nworkspaces_root = "~/.capo/workspaces"\nprojects_root   = "~/code"\n'
        'db_path         = "~/.capo/state.db"\ndbos_db_path    = "~/.capo/dbos.db"',
        f'[paths]\nworkspaces_root = "{workspaces_root}"\nprojects_root   = "{projects_root}"\n'
        f'db_path         = "{db_path}"\ndbos_db_path    = "{tmp_path}/dbos.db"',
    )
    cfg_path = _write_config(tmp_path, toml)
    # Drop a .env beside config.toml so Settings.load picks it up exactly like
    # production (§6.2). We still pass env_override for determinism.
    (tmp_path / ".env").write_text(
        "\n".join(f"{k}={v}" for k, v in _VALID_ENV.items())
    )

    settings = Settings.load(cfg_path, env_override=dict(_VALID_ENV))

    # ---- 2. Seed the §7.3 schema + the configured ``owner`` user row. ----
    _seed_db(db_path, user_id="owner")

    # ---- 3. Compose AMCClient with a MockTransport (no network). --------
    amc_record: dict[str, list] = {"get_unread": [], "send": [], "mark_read": []}
    amc_client = _build_amc_client_with_mock(amc_record)

    # ---- 4. Build the agent against TestModel (no API key needed). ------
    # ``register_tools=False`` keeps the tool registration path (which would
    # call into capo.tools.basic and require deps_type wiring) out of scope
    # for this single e2e probe — the dispatcher only calls ``agent.run``.
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    def _testmodel_factory(model, instructions, *, deps_type):  # noqa: ANN001
        return Agent(
            model=TestModel(call_tools=[]),
            instructions=instructions,
            deps_type=deps_type,
        )

    from capo.agent import build_agent

    system_prompt_path = cfg_path.parent / "prompts" / "system.md"
    system_prompt_path.parent.mkdir(exist_ok=True)
    if not system_prompt_path.is_file():
        system_prompt_path.write_text("# system prompt for e2e test\nbe helpful.")

    agent = build_agent(
        settings,
        system_prompt_path,
        agent_factory=_testmodel_factory,
        register_tools=False,
    )

    # ---- 5. Compose Dispatcher with a real per-turn store conn factory. -
    from capo.memory.store import open_connection

    def _conn_factory():
        return open_connection(settings.paths.db_path)

    # CapoDeps requires a real httpx.AsyncClient field even when tools are
    # disabled — the dispatcher's default deps factory plumbs through to
    # ``CapoDeps(http_client=...)``. We use a plain client; no requests are
    # ever made through it because tools are unregistered.
    async with httpx.AsyncClient() as shared_http:
        dispatcher = Dispatcher(
            settings,
            agent,
            amc_client,
            _conn_factory,
            http_client=shared_http,
        )
        await dispatcher.start()
        try:
            # ---- 6. Build the listener and post a signed envelope. ------
            app = build_app(settings, dispatcher)
            transport = httpx.ASGITransport(app=app)
            envelope = {
                "id": f"msg-{uuid.uuid4()}",
                "channel_id": "amc:+15551234567",
                # ``+15551234567`` is mapped to user_id=owner by the
                # _VALID_TOML fixture's ``[users.owner]`` block.
                "sender_id": "+15551234567",
                "text": "hi",
                "ts": "2026-05-10T12:34:56Z",
                "approval_id": None,
            }
            secret = settings.amc_webhook_secret.get_secret_value()
            signed = sign_request(secret, envelope)

            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                t0 = time.perf_counter()
                resp = await client.post(
                    "/amc/webhook",
                    content=signed.body,
                    headers=signed.headers,
                )
                ack_ms = (time.perf_counter() - t0) * 1000.0

            # (a) Fast-ACK invariant (§6.1).
            assert resp.status_code == 204
            assert resp.content == b""
            assert ack_ms < 1000.0, f"ACK latency {ack_ms:.2f}ms exceeds 1s"

            # (b) Wait for the dispatcher worker to drain (§5.2 ordering).
            worker = dispatcher._workers[envelope["channel_id"]]
            import asyncio

            await asyncio.wait_for(worker._queue.join(), timeout=5.0)
        finally:
            await dispatcher.stop()
        await amc_client.aclose()

    # ---- 7. Assert the worker ran the full §5.2 pipeline. ----------------
    # (c) AMC send + mark_read each called exactly once.
    assert len(amc_record["send"]) == 1
    assert len(amc_record["mark_read"]) == 1
    send_payload = json.loads(amc_record["send"][0].content.decode("utf-8"))
    assert send_payload["channel_id"] == envelope["channel_id"]
    assert send_payload["reply_to"] == envelope["id"]
    mark_payload = json.loads(amc_record["mark_read"][0].content.decode("utf-8"))
    assert mark_payload["message_ids"] == [envelope["id"]]

    # (d) Conversation memory + session rows are scoped to user_id=owner
    # (§5.7 + §5.11 + §7.3). Open a fresh connection to inspect the DB.
    inspect = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        sessions = inspect.execute(
            "SELECT user_id, thread_id, session_id, ended_at FROM sessions"
        ).fetchall()
        assert len(sessions) == 1, sessions
        assert sessions[0][0] == "owner"
        assert sessions[0][1] == f"amc:{envelope['channel_id']}"
        assert sessions[0][3] is None  # session still active

        history = inspect.execute(
            "SELECT user_id, thread_id, session_id, message_index "
            "FROM conversation_history ORDER BY message_index ASC"
        ).fetchall()
        # TestModel will produce at least one request + one response message.
        assert len(history) >= 2, history
        for row in history:
            assert row[0] == "owner"
            assert row[1] == f"amc:{envelope['channel_id']}"
            assert row[2] == sessions[0][2]
        # message_index should be contiguous starting at 0 (DAO uses MAX+1
        # where MAX is NULL on the first insert, coerced to -1 → 0).
        first = history[0][3]
        assert [r[3] for r in history] == list(
            range(first, first + len(history))
        )
    finally:
        inspect.close()

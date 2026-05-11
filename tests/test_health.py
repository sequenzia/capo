"""Tests for :mod:`capo.transport.health` (Task #56, spec §5.12 / §6.5).

Covers acceptance criteria:

* GET /healthz returns the documented JSON shape (status + probes[]).
* HTTP 200 when all critical probes pass; 503 when any critical probe fails.
* Critical probes: state_db, dbos_db, dbos_launched. Non-critical: amc,
  logfire, claude/codex binaries.
* Each probe is wrapped in a per-probe timeout (default 2s); the endpoint
  has an overall budget (default 5s); slow probes mark ok=false with
  ``details="timeout"``.
* Endpoint registered on the existing FastAPI app via build_app +
  register_health_endpoint.
* Edge: DBOS not launched → critical fail → 503.
* Edge: state.db missing → critical fail → 503.
* Edge: AMC unreachable + everything else healthy → 200 (non-critical fail).
* Endpoint NEVER raises — probe exceptions are caught and reported.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from capo.transport.amc_listener import build_app
from capo.transport.health import (
    CRITICAL_PROBES,
    DEFAULT_OVERALL_TIMEOUT_S,
    DEFAULT_PROBE_TIMEOUT_S,
    PROBE_NAMES,
    compute_status,
    register_health_endpoint,
    run_probes,
)
from tests.test_config import _VALID_ENV, _write_config

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


class FakeDispatcher:
    async def enqueue(self, envelope: Any) -> bool:
        return True


def _make_settings(tmp_path: Path) -> Any:
    """Build a valid Settings object with paths inside tmp_path."""
    from capo.config import Settings

    cfg = _write_config(tmp_path)
    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    # Override file paths so state.db / dbos.db are inside tmp_path
    # (no collision with the developer's real ~/.capo/ during tests).
    object.__setattr__(settings.paths, "db_path", tmp_path / "state.db")
    object.__setattr__(settings.paths, "dbos_db_path", tmp_path / "dbos.db")
    return settings


def _make_state_db(path: Path) -> None:
    """Create a minimal sqlite database at ``path`` so the probe succeeds."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _probe (x INTEGER)")
    finally:
        conn.close()


@pytest.fixture
def settings(tmp_path: Path) -> Any:
    return _make_settings(tmp_path)


@pytest.fixture(autouse=True)
def _reset_observability_state() -> Any:
    """Ensure each test sees a clean Logfire-configured state."""
    from capo.observability import _reset_for_tests

    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.fixture
def app(settings: Any) -> Any:
    """Build the full FastAPI app with /healthz registered."""
    app = build_app(settings, FakeDispatcher())
    register_health_endpoint(app, settings, amc_client=None)
    return app


def _make_client(app: Any) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


# ---------------------------------------------------------------------------
# Probe primitive tests (unit).
# ---------------------------------------------------------------------------


def test_compute_status_all_critical_ok() -> None:
    probes = [
        {"name": "state_db", "ok": True, "latency_ms": 1, "details": None},
        {"name": "dbos_db", "ok": True, "latency_ms": 1, "details": None},
        {"name": "dbos_launched", "ok": True, "latency_ms": 0, "details": None},
        {"name": "amc_reachable", "ok": False, "latency_ms": 100, "details": "x"},
    ]
    status, code = compute_status(probes)
    assert status == "ok"
    assert code == 200


def test_compute_status_critical_fail_yields_degraded() -> None:
    probes = [
        {"name": "state_db", "ok": False, "latency_ms": 1, "details": "missing"},
        {"name": "dbos_db", "ok": True, "latency_ms": 1, "details": None},
        {"name": "dbos_launched", "ok": True, "latency_ms": 0, "details": None},
    ]
    status, code = compute_status(probes)
    assert status == "degraded"
    assert code == 503


def test_critical_probes_set_matches_spec() -> None:
    """Acceptance criterion: critical = state.db + dbos.db + dbos_launched."""
    assert {"state_db", "dbos_db", "dbos_launched"} == CRITICAL_PROBES


def test_probe_names_stable_order_and_complete() -> None:
    """PROBE_NAMES must include every probe mentioned in the spec."""
    expected = {
        "state_db",
        "dbos_db",
        "dbos_launched",
        "amc_reachable",
        "logfire_configured",
        "claude_binary",
        "codex_binary",
    }
    assert set(PROBE_NAMES) == expected
    # Stable ordering keeps logs and dashboards reproducible.
    assert PROBE_NAMES[0] == "state_db"


def test_defaults_match_spec() -> None:
    """Default per-probe timeout 2s, overall 5s (task description)."""
    assert DEFAULT_PROBE_TIMEOUT_S == 2.0
    assert DEFAULT_OVERALL_TIMEOUT_S == 5.0


# ---------------------------------------------------------------------------
# run_probes integration tests.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_probes_state_db_missing_marks_critical_fail(
    settings: Any,
) -> None:
    # state.db does NOT exist on disk yet.
    assert not settings.paths.db_path.exists()
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["state_db"]["ok"] is False
    assert "missing" in (by_name["state_db"]["details"] or "")


@pytest.mark.asyncio
async def test_run_probes_state_db_ok_when_present(settings: Any) -> None:
    _make_state_db(settings.paths.db_path)
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["state_db"]["ok"] is True
    assert by_name["state_db"]["latency_ms"] >= 0


@pytest.mark.asyncio
async def test_run_probes_dbos_db_missing_is_ok(settings: Any) -> None:
    """dbos.db missing on disk → still ok=True (DBOS auto-creates on launch)."""
    _make_state_db(settings.paths.db_path)
    assert not settings.paths.dbos_db_path.exists()
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    # Non-critical "missing → still ok": the dbos_launched probe owns the
    # "DBOS up" critical assertion.
    assert by_name["dbos_db"]["ok"] is True


@pytest.mark.asyncio
async def test_run_probes_dbos_launched_false_when_not_initialized(
    settings: Any,
) -> None:
    """DBOS not launched → dbos_launched probe critical-fails."""
    _make_state_db(settings.paths.db_path)
    # is_launched should be False since no init_dbos has been called.
    from capo.workflows import is_launched

    if is_launched():
        pytest.skip("DBOS leaked from a prior test; cannot test 'not launched'")
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["dbos_launched"]["ok"] is False


@pytest.mark.asyncio
async def test_run_probes_logfire_not_configured(settings: Any) -> None:
    """logfire is reset by the autouse fixture → not configured."""
    _make_state_db(settings.paths.db_path)
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["logfire_configured"]["ok"] is False


@pytest.mark.asyncio
async def test_run_probes_amc_unreachable_returns_non_critical_fail(
    settings: Any,
) -> None:
    """AMC base_url unreachable → ok=False but doesn't flip 503 alone."""
    _make_state_db(settings.paths.db_path)

    async def _fake_head(*args: Any, **kwargs: Any) -> Any:
        raise httpx.ConnectError("connection refused")

    with patch("httpx.AsyncClient.head", new=_fake_head):
        probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["amc_reachable"]["ok"] is False
    # AMC is non-critical → if state.db + dbos.db ok, status would be degraded
    # only because dbos_launched is False here. Verify with the helper:
    status, _ = compute_status(probes)
    assert status == "degraded"  # because dbos_launched is False
    # And specifically: amc_reachable is NOT in the critical set.
    assert "amc_reachable" not in CRITICAL_PROBES


@pytest.mark.asyncio
async def test_run_probes_claude_codex_binaries_non_critical(
    settings: Any,
) -> None:
    """Missing claude/codex binaries are non-critical (still 200-able)."""
    _make_state_db(settings.paths.db_path)
    object.__setattr__(settings.agents.claude_code, "binary", "definitely-not-on-path-claude")
    object.__setattr__(settings.agents.codex, "binary", "definitely-not-on-path-codex")
    probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["claude_binary"]["ok"] is False
    assert by_name["codex_binary"]["ok"] is False
    assert "claude_binary" not in CRITICAL_PROBES
    assert "codex_binary" not in CRITICAL_PROBES


@pytest.mark.asyncio
async def test_run_probes_records_latency_for_each_probe(settings: Any) -> None:
    _make_state_db(settings.paths.db_path)
    probes = await run_probes(settings, amc_client=None)
    for probe in probes:
        assert isinstance(probe["latency_ms"], int)
        assert probe["latency_ms"] >= 0


@pytest.mark.asyncio
async def test_run_probes_slow_probe_marked_timeout(settings: Any) -> None:
    """A probe that hangs past the per-probe timeout is marked timeout."""
    _make_state_db(settings.paths.db_path)

    async def _slow_head(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(5.0)  # longer than the 0.1s probe timeout below
        raise AssertionError("should have been cancelled")

    with patch("httpx.AsyncClient.head", new=_slow_head):
        probes = await run_probes(
            settings,
            amc_client=None,
            probe_timeout_s=0.1,
            overall_timeout_s=1.0,
        )
    by_name = {p["name"]: p for p in probes}
    assert by_name["amc_reachable"]["ok"] is False
    assert by_name["amc_reachable"]["details"] == "timeout"


@pytest.mark.asyncio
async def test_run_probes_never_raises_on_internal_exception(
    settings: Any,
) -> None:
    """Even if a probe primitive raises unexpectedly, run_probes returns."""
    _make_state_db(settings.paths.db_path)
    with patch(
        "capo.transport.health._probe_sqlite_read",
        side_effect=RuntimeError("boom"),
    ):
        probes = await run_probes(settings, amc_client=None)
    by_name = {p["name"]: p for p in probes}
    assert by_name["state_db"]["ok"] is False
    assert "unexpected" in (by_name["state_db"]["details"] or "")


# ---------------------------------------------------------------------------
# Endpoint integration tests (TestClient-style via httpx.ASGITransport).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_healthz_endpoint_registered_on_app(app: Any) -> None:
    """Endpoint is registered on the existing build_app() FastAPI app."""
    async with _make_client(app) as client:
        resp = await client.get("/healthz")
    # Code 200 or 503, but the route exists (no 404).
    assert resp.status_code in (200, 503)


@pytest.mark.asyncio
async def test_healthz_response_shape(settings: Any) -> None:
    """Response body has documented status + probes[] structure."""
    _make_state_db(settings.paths.db_path)
    app = build_app(settings, FakeDispatcher())
    register_health_endpoint(app, settings, amc_client=None)
    async with _make_client(app) as client:
        resp = await client.get("/healthz")
    body = resp.json()
    assert "status" in body
    assert body["status"] in ("ok", "degraded")
    assert "probes" in body
    assert isinstance(body["probes"], list)
    assert len(body["probes"]) == len(PROBE_NAMES)
    for probe in body["probes"]:
        assert set(probe.keys()) == {"name", "ok", "latency_ms", "details"}
        assert isinstance(probe["name"], str)
        assert isinstance(probe["ok"], bool)
        assert isinstance(probe["latency_ms"], int)


@pytest.mark.asyncio
async def test_healthz_503_when_dbos_not_launched(settings: Any) -> None:
    """Edge case: DBOS not launched → endpoint returns 503."""
    _make_state_db(settings.paths.db_path)
    from capo.workflows import is_launched

    if is_launched():
        pytest.skip("DBOS leaked from a prior test; cannot test 'not launched'")
    app = build_app(settings, FakeDispatcher())
    register_health_endpoint(app, settings, amc_client=None)
    async with _make_client(app) as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    by_name = {p["name"]: p for p in body["probes"]}
    assert by_name["dbos_launched"]["ok"] is False


@pytest.mark.asyncio
async def test_healthz_503_when_state_db_missing(settings: Any) -> None:
    """Edge case: state.db missing → endpoint returns 503."""
    # Do NOT create state.db.
    assert not settings.paths.db_path.exists()
    app = build_app(settings, FakeDispatcher())
    register_health_endpoint(app, settings, amc_client=None)
    async with _make_client(app) as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    by_name = {p["name"]: p for p in body["probes"]}
    assert by_name["state_db"]["ok"] is False
    assert "missing" in (by_name["state_db"]["details"] or "")


@pytest.mark.asyncio
async def test_healthz_200_when_all_critical_pass(
    settings: Any, tmp_path: Path
) -> None:
    """Happy path: all critical probes pass → HTTP 200, status=ok.

    This requires DBOS to be launched. We use init_dbos + launch_dbos in
    an autouse-clean fixture pattern.
    """
    _make_state_db(settings.paths.db_path)

    from capo.workflows import destroy_dbos, init_dbos, launch_dbos

    init_dbos(settings)
    await asyncio.to_thread(launch_dbos)
    try:
        app = build_app(settings, FakeDispatcher())
        # AMC may or may not be reachable; that's non-critical.
        register_health_endpoint(app, settings, amc_client=None)
        async with _make_client(app) as client:
            resp = await client.get("/healthz")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
        by_name = {p["name"]: p for p in body["probes"]}
        # All critical probes must be ok.
        for critical in CRITICAL_PROBES:
            assert by_name[critical]["ok"] is True, (
                f"{critical} should be ok: {by_name[critical]}"
            )
    finally:
        await asyncio.to_thread(destroy_dbos)


@pytest.mark.asyncio
async def test_healthz_200_amc_unreachable_but_critical_ok(
    settings: Any,
) -> None:
    """Edge: AMC unreachable but everything else healthy → 200 (non-critical)."""
    _make_state_db(settings.paths.db_path)

    from capo.workflows import destroy_dbos, init_dbos, launch_dbos

    init_dbos(settings)
    await asyncio.to_thread(launch_dbos)
    try:
        async def _fail_head(*args: Any, **kwargs: Any) -> Any:
            raise httpx.ConnectError("amc down")

        app = build_app(settings, FakeDispatcher())
        register_health_endpoint(app, settings, amc_client=None)

        with patch("httpx.AsyncClient.head", new=_fail_head):
            async with _make_client(app) as client:
                resp = await client.get("/healthz")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
        by_name = {p["name"]: p for p in body["probes"]}
        assert by_name["amc_reachable"]["ok"] is False
    finally:
        await asyncio.to_thread(destroy_dbos)


@pytest.mark.asyncio
async def test_healthz_endpoint_never_raises_on_state_missing(
    settings: Any,
) -> None:
    """If app.state has no health_settings, endpoint returns 503 (no 500)."""
    app = build_app(settings, FakeDispatcher())
    # Register then sabotage state — delete the stashed settings.
    register_health_endpoint(app, settings, amc_client=None)
    with contextlib.suppress(AttributeError):
        delattr(app.state, "health_settings")
    async with _make_client(app) as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"


@pytest.mark.asyncio
async def test_healthz_endpoint_never_raises_on_probe_runner_error(
    settings: Any,
) -> None:
    """If run_probes itself raises unexpectedly, endpoint returns 503 (no 500)."""
    _make_state_db(settings.paths.db_path)
    app = build_app(settings, FakeDispatcher())
    register_health_endpoint(app, settings, amc_client=None)
    with patch(
        "capo.transport.health.run_probes",
        side_effect=RuntimeError("catastrophic probe runner bug"),
    ):
        async with _make_client(app) as client:
            resp = await client.get("/healthz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["probes"] == []

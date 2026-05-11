"""``GET /healthz`` endpoint with subsystem probes (spec §5.12, §6.5).

This module owns Capo's liveness/readiness probe. It is registered on the
existing FastAPI app built by :func:`capo.transport.amc_listener.build_app`
at boot (see :func:`register_health_endpoint` and the call site in
:func:`capo.main.amain`).

Probe taxonomy
--------------

Critical (probe failure → HTTP 503):

* ``state_db`` — read probe against ``settings.paths.db_path`` (``state.db``).
  Opens a fresh sqlite3 connection in read-only mode (``?mode=ro``) and runs
  ``SELECT 1``. Missing file → typed probe failure with
  ``details="state.db missing"``.
* ``dbos_db`` — read probe against ``settings.paths.dbos_db_path``. Same
  shape as ``state_db``. Missing file is non-fatal (DBOS creates it on
  first launch) — we still mark ``ok=true`` with ``details="not yet
  created"``; the ``dbos_launched`` probe owns the "DBOS up" assertion.
* ``dbos_launched`` — checks :func:`capo.workflows.is_launched`. False →
  critical failure (the §5.6 "boot waits for DBOS" invariant has been
  violated, the listener should not be serving traffic).

Non-critical (probe failure → 200 with the probe marked ``ok=false``):

* ``amc_reachable`` — best-effort reachability of ``settings.amc.base_url``.
  HEAD request with a short timeout. Connection refused / DNS failure /
  4xx / 5xx all mark this non-critical-fail; we still 200.
* ``logfire_configured`` — :func:`capo.observability.is_configured`. False
  means Logfire is disabled or silently-skipped (``LOGFIRE_IGNORE_NO_CONFIG``
  branch), neither of which blocks operation.
* ``claude_binary`` — ``<claude_binary> --version`` exits 0 within timeout.
  Missing binary is logged but non-critical (a delegation request will
  raise the typed error at that point).
* ``codex_binary`` — same shape as ``claude_binary``.

Timeouts
--------

* Each probe gets a per-probe timeout (default 2s). Slow probes are
  marked ``ok=false`` with ``details="timeout"`` — for critical probes,
  this still triggers 503.
* The endpoint as a whole has an overall budget (default 5s). If the
  fanout takes longer than ``overall_timeout_s`` it returns whatever
  probes did complete plus a synthetic timeout marker for the unfinished
  ones (their original ``ok`` would have determined criticality; we treat
  unfinished critical probes as failure).

Response shape
--------------

::

  {
    "status": "ok" | "degraded",
    "probes": [
      {"name": "state_db", "ok": true, "latency_ms": 1, "details": null},
      ...
    ]
  }

``status`` is ``"ok"`` iff every critical probe passed. ``"degraded"`` is
used for the 503 case (per task wording — spec §5.12 uses ``"error"``;
this module returns ``"degraded"`` to match the task acceptance criterion
verbatim).

Error handling
--------------

The endpoint NEVER raises — every probe is wrapped in try/except and any
exception is caught and reported as a probe failure. This is the §5.12
"endpoint never raises" invariant.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import sqlite3
import subprocess
import time
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings
    from capo.transport.amc_client import AMCClient

logger = logging.getLogger("capo.transport.health")

#: Default per-probe timeout in seconds. Tunable via :func:`register_health_endpoint`.
DEFAULT_PROBE_TIMEOUT_S: float = 2.0

#: Default overall endpoint budget in seconds. Tunable via :func:`register_health_endpoint`.
DEFAULT_OVERALL_TIMEOUT_S: float = 5.0

#: Names of critical probes. A failure on any of these flips the HTTP
#: response to 503 and the body ``status`` to ``"degraded"``.
CRITICAL_PROBES: frozenset[str] = frozenset(
    {"state_db", "dbos_db", "dbos_launched"}
)

#: Names of all probes in stable order (matches the iteration order in the
#: response). Stable ordering keeps logs and downstream monitoring readable.
PROBE_NAMES: tuple[str, ...] = (
    "state_db",
    "dbos_db",
    "dbos_launched",
    "amc_reachable",
    "logfire_configured",
    "claude_binary",
    "codex_binary",
)


# ---------------------------------------------------------------------------
# Probe primitives (sync — wrapped in asyncio.to_thread by the fanout).
# ---------------------------------------------------------------------------


def _probe_sqlite_read(path: str) -> tuple[bool, str | None]:
    """Run a ``SELECT 1`` against ``path`` in read-only mode.

    Returns ``(ok, details)``. ``ok=False`` when the file is missing,
    unreadable, or the query raises. ``details`` carries a short human
    string suitable for inclusion in the JSON response.
    """
    from pathlib import Path  # local import: keeps top-level cheap.

    file_path = Path(path).expanduser()
    if not file_path.exists():
        return False, f"missing: {file_path}"
    try:
        # uri=True so a corrupt header surfaces immediately. Read-only so
        # the probe NEVER acquires a write lock that could race with the
        # boot path (Alembic / DBOS).
        uri = f"file:{file_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    except sqlite3.Error as exc:
        return False, f"connect failed: {exc}"
    try:
        conn.execute("SELECT 1").fetchone()
    except sqlite3.Error as exc:
        return False, f"query failed: {exc}"
    finally:
        conn.close()
    return True, None


def _probe_sqlite_optional(path: str) -> tuple[bool, str | None]:
    """Same as :func:`_probe_sqlite_read` but a missing file is ``ok=true``
    with details ``"not yet created"`` (DBOS auto-creates on first launch).
    """
    from pathlib import Path

    file_path = Path(path).expanduser()
    if not file_path.exists():
        return True, "not yet created"
    return _probe_sqlite_read(path)


def _probe_dbos_launched() -> tuple[bool, str | None]:
    """Check :func:`capo.workflows.is_launched`. False is critical fail."""
    try:
        from capo.workflows import is_launched  # local import: lazy.
    except Exception as exc:  # noqa: BLE001 - any import failure is "down"
        return False, f"import failed: {exc!r}"
    launched = is_launched()
    if not launched:
        return False, "dbos not launched"
    return True, None


async def _probe_amc_reachable(
    base_url: str,
    *,
    amc_client: AMCClient | None,
    timeout_s: float,
) -> tuple[bool, str | None]:
    """Reachability of the AMC HTTP base URL via HEAD request.

    Non-critical: any failure (DNS, refused, 4xx, 5xx) → ``ok=false`` but
    the endpoint still returns 200 unless a CRITICAL probe also failed.

    We prefer reusing the shared :class:`httpx.AsyncClient` inside
    :class:`AMCClient` so the probe inherits the same connection pool —
    avoids spinning up a new pool per probe.
    """
    import httpx  # local import

    try:
        if amc_client is not None and getattr(amc_client, "_client", None) is not None:
            client = amc_client._client  # type: ignore[attr-defined]
            response = await client.head(base_url, timeout=timeout_s)
        else:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                response = await client.head(base_url)
    except httpx.TimeoutException:
        return False, "timeout"
    except httpx.HTTPError as exc:
        return False, f"http error: {exc!r}"
    except Exception as exc:  # noqa: BLE001 - probe never raises
        return False, f"unexpected: {exc!r}"

    status = response.status_code
    # 2xx/3xx/401/403/404/405 all indicate the server is up enough to
    # answer. Only 5xx is treated as "down". HEAD on /healthz may return
    # 405 (method not allowed) — still reachable.
    if status >= 500:
        return False, f"status {status}"
    return True, f"status {status}"


_VERSION_RE = re.compile(r"(\d+\.\d+\.\d+)")


def _probe_binary_version(
    binary: str, *, timeout_s: float
) -> tuple[bool, str | None]:
    """Run ``<binary> --version`` synchronously and return parsed semver.

    Non-critical: a missing binary or non-zero exit marks ``ok=false`` but
    does not flip HTTP status to 503. The version string is returned in
    ``details`` on success.

    Synchronous — callers MUST wrap in :func:`asyncio.to_thread` so the
    event loop isn't blocked.
    """
    resolved = shutil.which(binary)
    if resolved is None:
        return False, f"binary not found on PATH: {binary!r}"
    try:
        result = subprocess.run(  # noqa: S603 - argv list, shell=False
            [resolved, "--version"],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except OSError as exc:
        return False, f"exec failed: {exc!r}"

    if result.returncode != 0:
        return False, (
            f"exit {result.returncode}: "
            f"{(result.stderr or '').strip()!r}"
        )
    raw = (result.stdout or result.stderr or "").strip()
    match = _VERSION_RE.search(raw)
    if match is None:
        # Still consider this OK — binary responded; we just can't parse.
        return True, raw[:80] if raw else "version unparseable"
    return True, match.group(1)


def _probe_logfire_configured() -> tuple[bool, str | None]:
    """Wrap :func:`capo.observability.is_configured` in the probe contract."""
    try:
        from capo.observability import is_configured  # local import
    except Exception as exc:  # noqa: BLE001
        return False, f"import failed: {exc!r}"
    if is_configured():
        return True, "configured"
    return False, "not configured"


# ---------------------------------------------------------------------------
# Probe runner — fanout with per-probe + overall timeout.
# ---------------------------------------------------------------------------


async def _run_with_timeout(
    name: str,
    coro: Any,
    *,
    timeout_s: float,
) -> dict[str, Any]:
    """Run a probe coroutine with an awaitable timeout. NEVER raises.

    Records elapsed ``latency_ms`` from the moment this helper started
    awaiting. Returns a stable dict shape so the response builder doesn't
    have to special-case anything.
    """
    started = time.perf_counter()
    try:
        ok, details = await asyncio.wait_for(coro, timeout=timeout_s)
    except TimeoutError:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "name": name,
            "ok": False,
            "latency_ms": elapsed_ms,
            "details": "timeout",
        }
    except Exception as exc:  # noqa: BLE001 - probes never raise
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "name": name,
            "ok": False,
            "latency_ms": elapsed_ms,
            "details": f"unexpected: {exc!r}",
        }
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return {
        "name": name,
        "ok": bool(ok),
        "latency_ms": elapsed_ms,
        "details": details,
    }


async def run_probes(
    settings: Settings,
    *,
    amc_client: AMCClient | None = None,
    probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
    overall_timeout_s: float = DEFAULT_OVERALL_TIMEOUT_S,
) -> list[dict[str, Any]]:
    """Run all probes concurrently with the overall budget cap.

    Returns the list of probe result dicts in :data:`PROBE_NAMES` order.

    The function NEVER raises — any unhandled exception inside a probe is
    caught and reported as ``ok=false`` with ``details`` carrying the
    exception ``repr``.
    """
    # Resolve binary names from settings with safe fallbacks.
    claude_binary = (
        getattr(getattr(settings.agents, "claude_code", None), "binary", None)
        or "claude"
    )
    codex_binary = (
        getattr(getattr(settings.agents, "codex", None), "binary", None)
        or "codex"
    )

    # Build the probe coroutines. Sync probes go through asyncio.to_thread
    # so a slow filesystem doesn't block the event loop while another
    # probe is racing.
    probes: dict[str, Any] = {
        "state_db": asyncio.to_thread(
            _probe_sqlite_read, str(settings.paths.db_path)
        ),
        "dbos_db": asyncio.to_thread(
            _probe_sqlite_optional, str(settings.paths.dbos_db_path)
        ),
        "dbos_launched": asyncio.to_thread(_probe_dbos_launched),
        "amc_reachable": _probe_amc_reachable(
            settings.amc.base_url,
            amc_client=amc_client,
            timeout_s=probe_timeout_s,
        ),
        "logfire_configured": asyncio.to_thread(_probe_logfire_configured),
        "claude_binary": asyncio.to_thread(
            _probe_binary_version, claude_binary, timeout_s=probe_timeout_s
        ),
        "codex_binary": asyncio.to_thread(
            _probe_binary_version, codex_binary, timeout_s=probe_timeout_s
        ),
    }

    tasks: list[asyncio.Task[dict[str, Any]]] = []
    for name in PROBE_NAMES:
        coro = probes[name]
        tasks.append(
            asyncio.create_task(
                _run_with_timeout(name, coro, timeout_s=probe_timeout_s),
                name=f"healthz-probe-{name}",
            )
        )

    overall_start = time.perf_counter()
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=False),
            timeout=overall_timeout_s,
        )
    except TimeoutError:
        # Some probes didn't complete within the overall budget. Cancel
        # them and synthesize timeout entries so the response is complete.
        for task in tasks:
            if not task.done():
                task.cancel()
        # Best-effort gather: swallow cancellation so we can still read
        # whatever the individual tasks reported.
        await asyncio.gather(*tasks, return_exceptions=True)

    # Collect results in PROBE_NAMES order.
    results: list[dict[str, Any]] = []
    elapsed_overall_ms = int((time.perf_counter() - overall_start) * 1000)
    for name, task in zip(PROBE_NAMES, tasks, strict=True):
        if task.cancelled() or not task.done():
            results.append(
                {
                    "name": name,
                    "ok": False,
                    "latency_ms": elapsed_overall_ms,
                    "details": "timeout",
                }
            )
            continue
        try:
            results.append(task.result())
        except Exception as exc:  # noqa: BLE001 - never raises out of probe
            results.append(
                {
                    "name": name,
                    "ok": False,
                    "latency_ms": elapsed_overall_ms,
                    "details": f"unexpected: {exc!r}",
                }
            )
    return results


def compute_status(probes: list[dict[str, Any]]) -> tuple[str, int]:
    """Determine overall ``status`` string and HTTP code from probe results.

    Returns ``("ok", 200)`` iff every critical probe (per :data:`CRITICAL_PROBES`)
    has ``ok=true``. Otherwise ``("degraded", 503)``.
    """
    for probe in probes:
        if probe["name"] in CRITICAL_PROBES and not probe["ok"]:
            return "degraded", 503
    return "ok", 200


# ---------------------------------------------------------------------------
# FastAPI registration.
# ---------------------------------------------------------------------------


def register_health_endpoint(
    app: FastAPI,
    settings: Settings,
    *,
    amc_client: AMCClient | None = None,
    probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
    overall_timeout_s: float = DEFAULT_OVERALL_TIMEOUT_S,
) -> None:
    """Register ``GET /healthz`` on ``app``.

    Stashes ``settings`` and ``amc_client`` on ``app.state`` so the handler
    closure stays trivial. Tests can override ``amc_client`` to drive the
    reachable / unreachable paths.

    Idempotent: re-registering replaces the previous handler. Production
    code calls this exactly once from :func:`capo.main.amain`.
    """
    app.state.health_settings = settings
    app.state.health_amc_client = amc_client
    app.state.health_probe_timeout_s = probe_timeout_s
    app.state.health_overall_timeout_s = overall_timeout_s

    @app.get("/healthz")
    async def healthz() -> JSONResponse:  # pragma: no cover - exercised by tests
        return await _healthz_handler(app)


async def _healthz_handler(app: FastAPI) -> JSONResponse:
    """Inner handler — pulled out so it's directly testable.

    NEVER raises. Catches every internal exception and translates it into
    a 503 ``{"status": "degraded", "probes": []}`` response so the
    endpoint behaves correctly even when ``app.state`` is malformed.
    """
    try:
        settings = app.state.health_settings
        amc_client = getattr(app.state, "health_amc_client", None)
        probe_timeout_s = getattr(
            app.state,
            "health_probe_timeout_s",
            DEFAULT_PROBE_TIMEOUT_S,
        )
        overall_timeout_s = getattr(
            app.state,
            "health_overall_timeout_s",
            DEFAULT_OVERALL_TIMEOUT_S,
        )
    except Exception as exc:  # noqa: BLE001 - never raises
        logger.exception(
            "healthz: app.state missing settings: %r",
            exc,
            extra={"event": "capo.health.state_missing"},
        )
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "probes": []},
        )

    try:
        probes = await run_probes(
            settings,
            amc_client=amc_client,
            probe_timeout_s=probe_timeout_s,
            overall_timeout_s=overall_timeout_s,
        )
    except Exception as exc:  # noqa: BLE001 - never raises
        logger.exception(
            "healthz: run_probes raised unexpectedly: %r",
            exc,
            extra={"event": "capo.health.run_probes_failed"},
        )
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "probes": []},
        )

    status, http_code = compute_status(probes)
    logger.info(
        "healthz status=%s http=%d probes_ok=%d/%d",
        status,
        http_code,
        sum(1 for p in probes if p["ok"]),
        len(probes),
        extra={
            "event": "capo.health.checked",
            "status": status,
            "http_code": http_code,
            "probes_ok": sum(1 for p in probes if p["ok"]),
            "probes_total": len(probes),
        },
    )
    return JSONResponse(
        status_code=http_code,
        content={"status": status, "probes": probes},
    )


__all__ = [
    "CRITICAL_PROBES",
    "DEFAULT_OVERALL_TIMEOUT_S",
    "DEFAULT_PROBE_TIMEOUT_S",
    "PROBE_NAMES",
    "compute_status",
    "register_health_endpoint",
    "run_probes",
]

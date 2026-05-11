"""Capo runtime entry point (spec §9.1 Phase 1 boot path).

This module wires the full Phase 1 boot sequence:

1. Parse CLI flags (``--config``, ``--no-serve``, ``--version``).
2. Resolve the config path (``--config`` > ``CAPO_CONFIG`` > ``./config.toml``).
3. Load + validate Settings via :class:`capo.config.Settings.load`. An
   explicit-but-missing config is a hard error (exit 2); the default path is
   allowed to be absent so the bare ``python -m capo --version`` smoke test
   keeps passing.
4. Build the Pydantic AI agent (SOUL + system prompt) via
   :func:`capo.agent.build_agent`.
5. When settings load AND ``--no-serve`` is NOT set, hand off to
   :func:`amain` which:

   a. Opens a shared :class:`httpx.AsyncClient` (one per process — pooled
      connections, scoped lifecycle).
   b. Constructs the :class:`AMCClient` for outbound REST.
   c. Constructs the :class:`Dispatcher` with a per-turn store-conn factory
      that opens a fresh sqlite3 connection per envelope via
      :func:`capo.memory.store.open_connection`.
   d. Starts the dispatcher, runs the boot-time unread sweep, builds the
      FastAPI listener, and serves it with :mod:`uvicorn`'s programmatic
      :class:`uvicorn.Server` API so the asyncio event loop stays under our
      control (clean shutdown on SIGINT/SIGTERM).

The `--no-serve` flag (Task #10) remains the canonical hook for boot smoke
tests: it validates Settings + builds the agent then exits 0 without
binding any sockets.

Manual Phase-1 demo
-------------------

Run ``uv run capo --config /path/to/config.toml``. See
``docs/runbook-phase1-demo.md`` for the end-to-end demo procedure including
``.env`` + ``config.toml`` preparation and AMC sender mapping.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from capo import __version__
from capo.config import ConfigError, Settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass

logger = logging.getLogger("capo")


def _configure_logging(level: int = logging.INFO) -> None:
    """Install a minimal structured log handler.

    Format keeps key/value pairs adjacent to the message so downstream log
    shippers (Logfire / OTel) can parse them. Phase 5 will swap this for
    ``logfire.configure()`` per blueprint §"Observability"; for Phase 1 the
    stderr stream IS the trace.
    """
    if logger.handlers:  # Idempotent under repeated import / re-invocation.
        return

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s level=%(levelname)s logger=%(name)s msg=%(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


def _export_provider_secrets_to_env(settings: Settings) -> None:
    """Bridge LLM-provider API keys from validated Settings into ``os.environ``.

    The config loader intentionally keeps ``.env`` values out of the process
    environment (see ``Settings.load`` docstring). But Pydantic AI's
    auto-resolved providers (``AnthropicProvider``, ``OpenAIProvider``) read
    their API keys from ``os.environ`` when no explicit ``api_key`` is passed.
    This helper makes the boot-time bridge explicit and localized to one
    place. Values from ``.env`` win over any pre-existing process env vars,
    matching the precedence inside ``Settings.load`` itself.
    """
    if settings.anthropic_api_key is not None:
        os.environ["ANTHROPIC_API_KEY"] = (
            settings.anthropic_api_key.get_secret_value()
        )
    if settings.openai_api_key is not None:
        os.environ["OPENAI_API_KEY"] = settings.openai_api_key.get_secret_value()


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="capo",
        description="Capo: routing agent that delegates to Claude Code and Codex.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Path to config.toml. Defaults to $CAPO_CONFIG or ./config.toml. "
            "If explicitly provided and missing, capo exits non-zero with a "
            "clear error."
        ),
    )
    parser.add_argument(
        "--no-serve",
        action="store_true",
        help=(
            "Skip starting the AMC webhook listener after settings load. "
            "Useful for boot smoke-tests and CI scaffolding."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"capo {__version__}",
    )
    return parser.parse_args(argv)


def _resolve_config_path(cli_path: Path | None) -> tuple[Path, bool]:
    """Resolve the config path and whether it was explicitly requested.

    Precedence: CLI ``--config`` > ``CAPO_CONFIG`` env var > ``./config.toml``.
    Returns ``(path, explicit)`` where ``explicit`` is True if the user (CLI
    or env) named a path. An explicit-but-missing path is a hard error; the
    default path is allowed to be absent during scaffold so ``python -m capo
    --version`` keeps working in a clean checkout.
    """
    if cli_path is not None:
        return cli_path.expanduser().resolve(), True

    env_path = os.environ.get("CAPO_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve(), True

    return Path.cwd() / "config.toml", False


def _make_amc_send_adapter(amc_client):
    """Build an awaitable adapter ``(channel_id, text, idempotency_key) -> SendResult``.

    The ``register_amc_sender`` contract in :mod:`capo.workflows.delegation`
    (Task #33 / §5.6) expects a positional 3-arg coroutine:

        async def sender(channel_id, text, idempotency_key) -> SendResult

    :class:`capo.transport.amc_client.AMCClient.send` takes ``channel_id``
    and ``text`` positionally but ``idempotency_key`` keyword-only. This
    adapter bridges the two signatures so DBOS workflow steps (notify_user,
    heartbeat) can stay decoupled from the AMC transport package's import
    surface.

    Closing over the live ``amc_client`` means the registered sender
    inherits the client's httpx connection pool — exactly one underlying
    pool per process, matching the §5.6 "shared httpx client" decision.
    """

    async def _adapter(channel_id: str, text: str, idempotency_key: str):
        return await amc_client.send(
            channel_id, text, idempotency_key=idempotency_key
        )

    return _adapter


async def _cold_boot_resume_sweep(settings) -> None:
    """Sweep ``delegations.status='running'`` and re-invoke ``monitor_delegation``.

    §5.6 / §5.13 / §9.3 cold-boot contract (Task #35): after a Capo
    restart, any row left in ``status='running'`` by a previous process
    has no live monitor — DBOS doesn't auto-resume the Phase-2 asyncio
    task, and we now don't even start one. The DBOS ``monitor_delegation``
    workflow does, however, support resume: on entry it looks up the
    in-process registry, finds it empty (registry is process-local),
    falls through to the resume step (Task #31), reads
    ``session_id_subagent`` from the row, and re-spawns the subprocess
    via ``claude --resume <session_id>``.

    This helper is the entrypoint: open a fresh sqlite connection
    against ``state.db``, SELECT all rows with ``status='running'``,
    and invoke ``monitor_delegation(delegation_id, db_path=...,
    claude_binary=...)`` once per row. We schedule each as a
    fire-and-forget asyncio task so the sweep itself returns quickly
    (the workflow then runs in the background while the listener
    accepts traffic).

    DBOS dedupes repeated invocations via workflow ID semantics — if
    the prior process happened to start the workflow before crashing,
    the resume step's idempotency keys ensure no double-spawn.

    No-op cases handled cleanly:
      * No rows with status='running' → return without scheduling anything.
      * state.db missing the delegations table (cold init) → log + return.
      * Individual row resume failures → logged inside the workflow body;
        do not abort the sweep.
    """
    # Imports kept local so this helper doesn't widen the module's
    # top-level import surface (and so capo.main's --version smoke test
    # keeps avoiding the dbos import path).
    import sqlite3  # noqa: PLC0415

    from capo.memory.store import open_connection  # noqa: PLC0415

    db_path = settings.paths.db_path

    def _select_running_ids() -> list[str]:
        conn = open_connection(db_path)
        try:
            try:
                rows = conn.execute(
                    "SELECT id FROM delegations WHERE status = 'running'"
                ).fetchall()
            except sqlite3.OperationalError as exc:
                # Table doesn't exist (e.g. fresh state.db before Alembic
                # has been pointed at it). Cold boot with nothing to
                # resume — return empty.
                logger.info(
                    "cold-boot resume sweep: delegations table not "
                    "queryable (%s); skipping",
                    exc,
                    extra={
                        "event": "capo.main.cold_boot.sweep.no_table",
                    },
                )
                return []
            return [str(r[0]) for r in rows if r and r[0]]
        finally:
            conn.close()

    running_ids = await asyncio.to_thread(_select_running_ids)
    if not running_ids:
        logger.info(
            "cold-boot resume sweep: no running delegations to resume",
            extra={
                "event": "capo.main.cold_boot.sweep.empty",
                "count": 0,
            },
        )
        return

    # Task #59: re-track each running delegation with the caffeinate
    # manager BEFORE scheduling the resume workflow. Without this a
    # cold-boot recovery sweep could let macOS idle-sleep mid-resume
    # (the original Capo process spawned caffeinate; that process exited
    # at restart so caffeinate is no longer running). Track is
    # idempotent — the workflow's terminal-status step releases each ID
    # once the monitor decides ``completed``/``failed``/``killed``.
    from capo.caffeinate import (  # noqa: PLC0415
        get_caffeinate_manager as _get_caf_mgr,
    )

    for _did in running_ids:
        with contextlib.suppress(Exception):
            await _get_caf_mgr().track_delegation(_did)

    logger.info(
        "cold-boot resume sweep: re-invoking monitor_delegation for "
        "%d running delegation(s)",
        len(running_ids),
        extra={
            "event": "capo.main.cold_boot.sweep.start",
            "count": len(running_ids),
        },
    )

    # Local import: deferring it keeps capo.main importable without DBOS
    # pulled into the module graph for the --no-serve smoke path.
    from capo.workflows.delegation import (  # noqa: PLC0415
        monitor_delegation as _monitor_delegation,
    )

    claude_binary = settings.agents.claude_code.binary or "claude"
    # Task #46: Codex resume contract — thread the configured codex binary
    # so ``_resume_spawn_step`` can spawn ``codex exec resume`` for rows
    # with ``agent='codex'``.
    codex_binary = (
        getattr(getattr(settings.agents, "codex", None), "binary", None)
        or "codex"
    )
    loop = asyncio.get_event_loop()
    for delegation_id in running_ids:
        async def _resume_one(did: str = delegation_id) -> None:
            try:
                await _monitor_delegation(
                    did,
                    db_path=db_path,
                    claude_binary=claude_binary,
                    codex_binary=codex_binary,
                )
            except BaseException as exc:  # noqa: BLE001
                logger.exception(
                    "cold-boot resume failed for delegation_id=%s: %r",
                    did,
                    exc,
                    extra={
                        "event": "capo.main.cold_boot.sweep.failed",
                        "delegation_id": did,
                    },
                )

        loop.create_task(
            _resume_one(),
            name=f"cold-boot-resume-{delegation_id}",
        )


async def amain(
    settings: Settings,
    *,
    system_prompt_path: Path,
    agent: object | None = None,
) -> int:
    """Async boot path: DBOS + dispatcher + boot sweep + uvicorn server.

    Composed exactly as the §9.1 Phase 1 checkpoint requires, extended in
    Phase 3 (task #28) to bring up DBOS before the webhook binds:

    1. Open a shared :class:`httpx.AsyncClient` for outbound HTTP (used both
       by :class:`AMCClient` for REST traffic AND by :class:`CapoDeps` for
       the ``fetch_url`` tool).
    2. Construct the :class:`AMCClient` (idempotent send + typed errors).
    3. Build the agent and the :class:`Dispatcher` (with the per-turn
       store-conn factory pointing at ``settings.paths.db_path``).
    4. Build the FastAPI app, then **initialize and launch DBOS** against
       ``settings.paths.dbos_db_path`` — §5.6 mandates boot waits for DBOS
       to be ready before serving the webhook. Launch runs in a worker
       thread so the event loop isn't blocked during DBOS's first-launch
       schema migrations (~100ms cold start per spike S-2 §3).
    5. ``dispatcher.start()`` → run the boot-time unread sweep → serve the
       app with :mod:`uvicorn`.
    6. On shutdown: stop dispatcher → :func:`destroy_dbos` → close httpx
       and AMC clients via their ``async with`` / ``aclose`` paths.

    The function returns 0 on a clean shutdown. SIGINT / SIGTERM are wired
    via :mod:`asyncio` signal handlers so ``asyncio.run(amain(...))`` can
    unwind cleanly.
    """
    import httpx

    from capo.agent import build_agent
    from capo.boot import run_precheck_or_exit
    from capo.caffeinate import (
        CaffeinateManager,
        register_caffeinate_manager,
    )
    from capo.maintenance.retention import (
        start_retention_scheduler,
        stop_retention_scheduler,
    )
    from capo.memory.store import open_connection
    from capo.observability import (
        LogfireMissingError,
        configure_logfire,
        instrument_fastapi_app,
    )
    from capo.transport.amc_client import AMCClient
    from capo.transport.amc_listener import build_app
    from capo.transport.boot_sweep import run_boot_sweep
    from capo.transport.dispatcher import Dispatcher
    from capo.transport.health import register_health_endpoint
    from capo.workflows import DBOSInitError, destroy_dbos, init_dbos, launch_dbos
    from capo.workflows.delegation import register_amc_sender

    # Task #62 / spec §5.4 / §7.5 / §12.1: validate claude + codex CLI
    # versions BEFORE we bring DBOS up or open any sockets. Runs after
    # Settings is loaded (caller's responsibility) and before
    # ``init_dbos`` so a misconfigured machine fails fast with a clear
    # actionable message — no half-initialized DBOS state, no listening
    # webhook. Honors ``settings.boot.skip_binary_precheck`` for
    # smoke/CI runs without the CLI binaries.
    rc = run_precheck_or_exit(settings)
    if rc is not None:
        return rc

    # Phase 5 / Task #50: Logfire configuration at boot. Runs BEFORE
    # construction of httpx clients / AMCClient / FastAPI app so the
    # auto-instrument patches (httpx + pydantic_ai are process-wide; FastAPI
    # needs the live app instance and is applied after build_app() below)
    # land before any traced object is constructed.
    #
    # configure_logfire is intentionally fault-tolerant: it catches its
    # own SDK exceptions and logs them — Logfire is a degradable
    # observability dependency, not a hard-fail boot prerequisite (spec
    # §6.5 / Task #50 acceptance criterion). The ONLY condition that
    # surfaces here is LogfireMissingError (logfire package not
    # importable), which we translate into the standard "single-line
    # stderr + exit 2" boot-failure pattern.
    try:
        configure_logfire(settings)
    except LogfireMissingError as exc:
        sys.stderr.write(f"capo: {exc}\n")
        return 2

    # Shared HTTP client: one per process. The dispatcher's default deps
    # factory uses this for the fetch_url tool; the AMCClient owns its own
    # underlying httpx.AsyncClient (constructed inside __init__) — that's
    # the canonical pattern from Task #12.
    async with httpx.AsyncClient() as shared_http_client:
        amc_client = AMCClient.from_settings(
            settings.amc, settings.amc_bearer_token
        )
        try:
            # Phase 1 default agent. The agent factory is bound by Settings
            # and resolves the SOUL file at construction time. The system
            # prompt path is computed by main() (caller) from the config
            # file's parent directory. Callers may pass a pre-built ``agent``
            # to avoid double-construction (main() builds one for validation
            # before invoking amain).
            if agent is None:
                agent = build_agent(settings, system_prompt_path)

            def _conn_factory():
                return open_connection(settings.paths.db_path)

            dispatcher = Dispatcher(
                settings,
                agent,
                amc_client,
                _conn_factory,
                http_client=shared_http_client,
            )

            # Build the FastAPI app first so DBOS can attach its HTTP
            # tracing during ``init_dbos``. The webhook routes are wired in
            # build_app() but uvicorn doesn't start serving until we call
            # _serve_until_signal() below.
            app = build_app(settings, dispatcher)

            # Task #50: apply Logfire's FastAPI auto-instrument now that
            # the app instance exists. Safe no-op when Logfire was
            # skipped (disabled / no token / missing); otherwise this
            # adds request-level spans to every FastAPI route.
            instrument_fastapi_app(app)

            # Task #56 / §5.12 / §6.5: register the ``GET /healthz``
            # endpoint with subsystem probes. We pass the live amc_client
            # so the AMC reachability probe inherits its httpx connection
            # pool — avoids spinning up a separate pool per probe.
            register_health_endpoint(app, settings, amc_client=amc_client)

            # Phase 3 §5.6: DBOS must be ready before we accept inbound
            # traffic. init_dbos() validates paths and constructs the DBOS
            # instance; launch_dbos() opens dbos.db, applies migrations,
            # and starts the notification listener. Both surface
            # DBOSInitError on failure with the dbos_db_path and the
            # failing operation, which we surface as a single-line stderr
            # message before re-raising for the asyncio.run() boundary.
            try:
                init_dbos(settings, fastapi_app=app)
                # launch is sync + blocking (~100ms cold start). Defer to a
                # worker thread so the asyncio event loop stays responsive.
                await asyncio.to_thread(launch_dbos)
            except DBOSInitError as exc:
                sys.stderr.write(f"capo: {exc}\n")
                return 2

            # Phase 3 §5.6 / §5.13: register the AMC sender BEFORE the
            # dispatcher starts so the DBOS monitor workflow can deliver
            # terminal notifications (Task #33) and heartbeats (Task #34)
            # as soon as a delegation completes. The registered callable
            # must be (channel_id, text, idempotency_key) -> SendResult;
            # we build a thin adapter over AMCClient.send below.
            register_amc_sender(_make_amc_send_adapter(amc_client))

            # Task #59 (§8.1): register the process-wide caffeinate
            # manager. macOS-only: on Linux the manager is constructed
            # disabled and every track/release is a no-op. The
            # cold-boot sweep below tracks any running delegations the
            # previous Capo process left behind; the DBOS workflow's
            # terminal-status step releases on
            # completed/failed/killed transitions.
            caffeinate_manager = CaffeinateManager()
            register_caffeinate_manager(caffeinate_manager)

            # Phase 3 §5.6 cold-boot restart-resume sweep (Task #35).
            # AFTER DBOS.launch() but BEFORE we open the webhook to
            # inbound traffic, sweep ``delegations`` for rows still
            # in ``status='running'`` (left by a previous Capo process
            # that crashed mid-monitor) and re-invoke
            # ``monitor_delegation`` for each. DBOS workflow IDs dedupe
            # repeated invocations; ``monitor_delegation``'s registry
            # lookup is empty on a cold boot so the resume step
            # (Task #31) takes over — it reads ``session_id_subagent``
            # from the row and re-spawns the subprocess via
            # ``claude --resume <session_id>``.
            await _cold_boot_resume_sweep(settings)

            retention_task: asyncio.Task[None] | None = None
            try:
                await dispatcher.start()
                # Task #55 (§8.1, §8.4): start the nightly
                # delegation_output retention scheduler AFTER the
                # dispatcher is up but BEFORE the listener binds. The
                # scheduler sleeps until ``settings.retention.run_hour_local``
                # local-time each day, prunes terminal-delegation output
                # rows older than ``settings.retention.delegation_output_days``,
                # and skips if a prior run is fresher than 23h. Failures
                # inside the loop are logged + swallowed so this background
                # task can NEVER take down Capo.
                retention_task = start_retention_scheduler(settings)
                try:
                    # Drain anything AMC was buffering before opening the
                    # webhook (§5.2 invariant: deliver-once across restart).
                    await run_boot_sweep(amc_client, dispatcher, settings)

                    rc = await _serve_until_signal(
                        app,
                        host=settings.amc.listen_host,
                        port=settings.amc.listen_port,
                    )
                    return rc
                finally:
                    await dispatcher.stop()
            finally:
                # Task #55: cancel the retention scheduler before tearing
                # down DBOS so the loop unwinds cleanly. ``stop_retention_scheduler``
                # is idempotent and tolerant of None / already-finished tasks.
                if retention_task is not None:
                    with contextlib.suppress(Exception):
                        await stop_retention_scheduler(retention_task)
                # Clear the AMC sender registration so a subsequent
                # in-process restart (tests) doesn't see a sender bound to
                # a closed amc_client.
                with contextlib.suppress(Exception):
                    register_amc_sender(None)
                # Task #59: stop caffeinate + clear the manager
                # registration so an in-process restart doesn't see a
                # manager bound to a reaped subprocess. ``stop()`` also
                # reaps any caffeinate child if one is still running.
                with contextlib.suppress(Exception):
                    await caffeinate_manager.stop()
                with contextlib.suppress(Exception):
                    register_caffeinate_manager(None)
                # Tear DBOS down before the surrounding clients close.
                # destroy_dbos() is idempotent and safe to call even if
                # launch failed mid-way (state is cleared atomically).
                try:
                    await asyncio.to_thread(destroy_dbos)
                except DBOSInitError as exc:
                    # Log but don't mask the original return code — this is
                    # shutdown cleanup.
                    logger.warning(
                        "DBOS destroy failed during shutdown: %s",
                        exc,
                        extra={"event": "capo.main.dbos.destroy_failed"},
                    )
        finally:
            await amc_client.aclose()


async def _serve_until_signal(
    app, *, host: str, port: int
) -> int:
    """Run uvicorn programmatically on the current event loop until SIGINT/SIGTERM.

    Using :class:`uvicorn.Server` (rather than :func:`uvicorn.run`) keeps
    the event loop under :func:`asyncio.run`'s control so the surrounding
    ``async with`` blocks in :func:`amain` finalize the AMCClient and the
    shared httpx.AsyncClient cleanly on shutdown.

    Signal handling: we attach SIGINT/SIGTERM handlers to the running loop
    that set ``server.should_exit = True``. On platforms where
    :meth:`asyncio.AbstractEventLoop.add_signal_handler` is unavailable
    (e.g. Windows under selector loop), we fall back to uvicorn's own
    signal hookup, which is fine for the Phase 1 boot — the manual demo
    runs on macOS/Linux.
    """
    import uvicorn

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_config=None,
        # We own logging via the root ``capo`` logger configured in main().
        # Disabling uvicorn's default log_config prevents it from clobbering
        # the formatter when ``capo`` is invoked under ``uv run capo``.
        access_log=False,
        lifespan="off",
    )
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    installed_handlers: list[int] = []
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_shutdown, server, sig_name)
            installed_handlers.append(sig)
        except (NotImplementedError, RuntimeError):
            # add_signal_handler is unavailable on Windows + some embed
            # contexts; uvicorn.Server installs its own SIGINT handler
            # in that case (install_signal_handlers default).
            continue

    logger.info(
        "capo listener starting on %s:%d", host, port,
        extra={"event": "capo.main.listener.starting", "host": host, "port": port},
    )
    try:
        await server.serve()
    finally:
        for sig in installed_handlers:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(sig)

    logger.info(
        "capo listener stopped",
        extra={"event": "capo.main.listener.stopped"},
    )
    return 0


def _request_shutdown(server, sig_name: str) -> None:
    """SIGINT/SIGTERM handler: ask uvicorn to exit on the next loop tick."""
    logger.info(
        "capo received %s; initiating clean shutdown", sig_name,
        extra={"event": "capo.main.shutdown.requested", "signal": sig_name},
    )
    server.should_exit = True


def main(argv: list[str] | None = None) -> int:
    """Entry point registered as the ``capo`` console script.

    Returns a process exit code so ``python -m capo`` and tests can drive it
    deterministically without ``SystemExit`` ambiguity.
    """
    _configure_logging()

    try:
        args = _parse_args(argv)
    except SystemExit as exc:  # argparse already wrote to stderr.
        return int(exc.code) if exc.code is not None else 2

    config_path, explicit = _resolve_config_path(args.config)

    if explicit and not config_path.is_file():
        sys.stderr.write(
            f"capo: config file not found: {config_path}\n"
            "  Provide --config <path> or set CAPO_CONFIG to an existing file.\n"
        )
        return 2

    config_state = "present" if config_path.is_file() else "absent (scaffold stub)"

    # If a config file is present, validate it via Pydantic Settings.
    # Missing default config (no explicit flag) is still allowed so the
    # `python -m capo --version` smoke test keeps working in a clean
    # checkout (no config.toml on disk).
    settings: Settings | None = None
    if config_path.is_file():
        env_file = config_path.parent / ".env"
        try:
            settings = Settings.load(
                config_path,
                env_file=env_file if env_file.is_file() else None,
            )
        except ConfigError as exc:
            # Single-line, no stack trace — matches the existing
            # missing-config error pattern.
            sys.stderr.write(f"capo: {exc}\n")
            return 2

        # Pydantic AI's auto-provider lookup (AnthropicProvider, OpenAIProvider)
        # reads ANTHROPIC_API_KEY / OPENAI_API_KEY from os.environ. The config
        # loader keeps .env values out of os.environ to stay pure, so we bridge
        # them here at the application boot path — once, after Settings is
        # validated.
        _export_provider_secrets_to_env(settings)

    # Construct the Pydantic AI agent (SOUL + ops prompt) — spec §5.1.
    # Imported lazily so the no-config boot path stays import-free of
    # pydantic_ai (and so the smoke tests don't need an LLM provider).
    agent_state = "skipped (no settings)"
    agent_obj: object | None = None
    system_prompt_path = config_path.parent / "prompts" / "system.md"
    if settings is not None:
        from capo.agent import AgentBuildError, build_agent

        try:
            agent_obj = build_agent(settings, system_prompt_path)
        except AgentBuildError as exc:
            sys.stderr.write(f"capo: {exc}\n")
            return 2
        agent_state = "built"

    logger.info(
        "capo starting version=%s config_path=%s config=%s settings_loaded=%s agent=%s",
        __version__,
        config_path,
        config_state,
        settings is not None,
        agent_state,
    )

    # Startup banner — also goes to stdout so it's visible without log capture.
    settings_state = "loaded" if settings is not None else "skipped (no config)"
    if settings is None:
        listener_state = "not started (no settings)"
    elif args.no_serve:
        listener_state = "not started (--no-serve)"
    else:
        listener_state = (
            f"binding {settings.amc.listen_host}:{settings.amc.listen_port}"
        )
    banner = (
        f"capo v{__version__}\n"
        f"  config: {config_path} [{config_state}]\n"
        f"  settings: {settings_state}\n"
        f"  agent: {agent_state}\n"
        f"  listener: {listener_state}\n"
    )
    sys.stdout.write(banner)
    sys.stdout.flush()

    # Boot smoke / CI path: settings present but caller asked for --no-serve,
    # or no settings at all (scaffold stub). Exit cleanly without binding a
    # socket so the test_smoke.py invariants still hold.
    if settings is None or args.no_serve:
        logger.info("capo exiting cleanly (no listener requested)")
        return 0

    # Full Phase 1 boot path. asyncio.run owns the event loop; amain
    # composes the dispatcher + boot sweep + uvicorn server lifecycle.
    try:
        return asyncio.run(
            amain(
                settings,
                system_prompt_path=system_prompt_path,
                agent=agent_obj,
            )
        )
    except KeyboardInterrupt:
        # Defensive: SIGINT path on platforms where add_signal_handler
        # didn't take effect — surface a clean exit code rather than the
        # default 130.
        logger.info("capo interrupted by user")
        return 0


if __name__ == "__main__":  # pragma: no cover - exercised via __main__.py
    raise SystemExit(main())

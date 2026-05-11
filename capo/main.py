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


async def amain(
    settings: Settings,
    *,
    system_prompt_path: Path,
    agent: object | None = None,
) -> int:
    """Async boot path: dispatcher + boot sweep + uvicorn server.

    Composed exactly as the §9.1 Phase 1 checkpoint requires:

    1. Open a shared :class:`httpx.AsyncClient` for outbound HTTP (used both
       by :class:`AMCClient` for REST traffic AND by :class:`CapoDeps` for
       the ``fetch_url`` tool).
    2. Construct the :class:`AMCClient` (idempotent send + typed errors).
    3. Build the agent and the :class:`Dispatcher` (with the per-turn
       store-conn factory pointing at ``settings.paths.db_path``).
    4. ``dispatcher.start()`` → run the boot-time unread sweep → build the
       FastAPI listener → serve it with :mod:`uvicorn`.

    The function returns 0 on a clean shutdown. SIGINT / SIGTERM are wired
    via :mod:`asyncio` signal handlers so ``asyncio.run(amain(...))`` can
    unwind cleanly: the uvicorn server is stopped, then the dispatcher's
    workers are cancelled and drained.
    """
    import httpx

    from capo.agent import build_agent
    from capo.memory.store import open_connection
    from capo.transport.amc_client import AMCClient
    from capo.transport.amc_listener import build_app
    from capo.transport.boot_sweep import run_boot_sweep
    from capo.transport.dispatcher import Dispatcher

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
            await dispatcher.start()
            try:
                # Drain anything AMC was buffering before opening the
                # webhook (§5.2 invariant: deliver-once across restart).
                await run_boot_sweep(amc_client, dispatcher, settings)

                app = build_app(settings, dispatcher)
                rc = await _serve_until_signal(
                    app,
                    host=settings.amc.listen_host,
                    port=settings.amc.listen_port,
                )
                return rc
            finally:
                await dispatcher.stop()
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

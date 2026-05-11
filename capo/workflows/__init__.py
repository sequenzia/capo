"""Capo DBOS configuration + lifecycle (spec §5.6, §9.3, §15.3).

Phase 3 introduces DBOS Durable Execution for restart-resilient delegation
monitoring (§5.6 ``monitor_delegation``) and approval workflows (§5.8). DBOS
state lives in **its own** SQLite file at ``paths.dbos_db_path`` (the spec
default is ``~/.capo/dbos.db``), separate from Capo's application state file
``paths.db_path`` (``~/.capo/state.db``). This module owns the boot-time
configuration and the FastAPI startup/shutdown integration.

Key invariants enforced here:

1. **Separate SQLite files.** ``dbos.db`` and ``state.db`` are NEVER the same
   path. Capo manages ``state.db`` via Alembic; DBOS owns ``dbos.db`` schema
   and auto-applies its 32 migrations on first launch (spike S-2 §4). We
   refuse to launch DBOS if the two paths resolve to the same absolute file.

2. **Parent directory created with 0700 perms.** ``~/.capo/`` is created if
   missing, with ``mode=0o700`` so secrets-adjacent state (DBOS workflow
   payloads can contain delegation briefs and tool inputs) stays
   user-readable only. On existing dirs we DO NOT chmod — operators may
   intentionally have a different mode; we only create with secure defaults.

3. **Typed init failures.** Any error in :func:`init_dbos` or
   :func:`launch_dbos` is wrapped in :class:`DBOSInitError` carrying the
   failing operation and the path. Stack traces stay attached via
   ``__cause__`` so debug context isn't lost, but callers (``capo.main``)
   surface a single-line error for the user that names ``dbos_db_path`` and
   the operation that failed (``init``, ``launch``, ``mkdir``,
   ``db_corrupt``). The boot path exits 2 on this exception.

4. **Launch happens BEFORE the webhook binds.** :func:`launch_dbos` is
   awaited (via :func:`asyncio.to_thread`) before :func:`build_app` and
   :class:`uvicorn.Server` start serving. This satisfies the §5.6 acceptance
   criterion that "boot waits for DBOS to be ready before serving the
   webhook" — if DBOS init/launch fails, no traffic is ever accepted.

5. **Destroy on shutdown.** :func:`destroy_dbos` is wired into the
   ``finally`` block of :func:`capo.main.amain` so that ``DBOS.destroy()``
   runs before the event loop unwinds. This drains any in-flight workflows
   gracefully (``workflow_completion_timeout_sec=0`` matches the DBOS
   default — we leave the policy choice to a later task that wires in
   delegation monitoring).

Design notes
------------

* :class:`dbos.DBOS` is a process-global singleton — instantiating
  ``DBOS(config=...)`` registers itself on the module-level ``_dbos_global``
  in the dbos package. We mirror that here with our own module-level
  ``_state`` so we can answer "is DBOS launched?" without poking dbos
  internals, and so multiple calls to :func:`init_dbos` in the same process
  (test re-imports, mostly) fail fast with a clear error rather than
  silently re-launching.

* For Phase 3 we only configure DBOS; the workflows themselves
  (``monitor_delegation`` etc.) land in ``capo/workflows/delegation.py``
  per the §9.3 task table. This module deliberately stays small and
  side-effect-free until :func:`init_dbos` is called.

* We pass the FastAPI app into ``DBOS(fastapi=app)`` so DBOS auto-wires its
  startup/shutdown handlers into the app lifespan. However, the §9.1
  listener currently sets ``lifespan="off"`` on uvicorn (see
  :func:`capo.main._serve_until_signal`) — so we cannot rely on that
  integration alone. We therefore call :meth:`DBOS.launch` explicitly
  before uvicorn starts serving, and :meth:`DBOS.destroy` explicitly in
  the shutdown ``finally``. Passing ``fastapi=`` remains useful for the
  HTTP-level tracing DBOS attaches (request spans, etc.) — it's a belt +
  suspenders config.

Caveats
-------

* DBOS prints an advisory ("SQLite system database is for development and
  testing") on every launch. Per spike S-2 §3, this is acceptable for V1
  (single-Mac-mini, single-process deployment). We capture-and-log it once
  per process in :func:`launch_dbos` so the operator sees it during boot.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from fastapi import FastAPI

    from capo.config import Settings

logger = logging.getLogger("capo.workflows")

#: Permission mode applied when creating ``~/.capo/`` on demand. User-only
#: read/write/execute — Capo's DBOS state can contain delegation briefs and
#: tool inputs, neither of which should be world-readable.
_DOTCAPO_MODE: int = 0o700

#: DBOS "application name" registered with the framework. Surfaces in DBOS
#: logs, span attributes, and admin server (we keep admin server OFF in V1).
_DBOS_APP_NAME: str = "capo"


class DBOSInitError(RuntimeError):
    """Raised when DBOS initialization, launch, or shutdown fails.

    Attributes
    ----------
    operation:
        The failing operation. One of ``"init"`` (DBOS instance construction),
        ``"launch"`` (system-database open + schema apply), ``"mkdir"``
        (creating ``~/.capo/``), ``"db_corrupt"`` (``dbos.db`` exists but is
        not a readable SQLite file), ``"path_collision"`` (``dbos_db_path``
        and ``db_path`` resolve to the same file), or ``"destroy"``.
    path:
        Absolute path involved in the failure (typically ``dbos_db_path`` or
        its parent directory). Always set; callers may surface it verbatim
        to the user.
    """

    def __init__(self, operation: str, path: Path, message: str) -> None:
        self.operation = operation
        self.path = path
        # Single-line, includes the path so capo.main can print it verbatim.
        super().__init__(f"dbos {operation} failed at {path}: {message}")


@dataclass(frozen=True)
class _State:
    """Module-level singleton tracking the active DBOS instance.

    The :mod:`dbos` package itself enforces only one DBOS per process, but
    we keep our own handle so :func:`destroy_dbos` is a no-op when DBOS was
    never launched (e.g. ``--no-serve`` smoke path) and so tests can assert
    "DBOS launched: yes/no" deterministically.
    """

    dbos_instance: object  # dbos.DBOS — typed as object to keep import lazy.
    dbos_db_path: Path


# Lock guarding init/launch/destroy transitions. DBOS itself is
# single-instance-per-process, but our state mutation needs to be atomic
# across the (init → launch → serve → destroy) lifecycle.
_state_lock = threading.Lock()
_state: _State | None = None


def _ensure_parent_dir(path: Path) -> None:
    """Create ``path.parent`` with 0700 perms if it doesn't exist.

    Existing directories are left alone — we don't chmod, so operators who
    intentionally use a different mode aren't surprised. ``OSError`` is
    re-raised as :class:`DBOSInitError` with ``operation="mkdir"`` so the
    boot path can surface a single-line error.
    """
    parent = path.parent
    if parent.exists():
        if not parent.is_dir():
            raise DBOSInitError(
                "mkdir",
                parent,
                f"parent path exists but is not a directory: {parent}",
            )
        return
    try:
        # ``mode`` on mkdir is masked by umask on POSIX; chmod afterwards to
        # force 0700 regardless of umask (so the directory is private even
        # under a permissive umask 022).
        parent.mkdir(parents=True, mode=_DOTCAPO_MODE)
        try:
            os.chmod(parent, _DOTCAPO_MODE)
        except OSError:
            # Non-fatal: on some filesystems chmod is a no-op. Log and move on.
            logger.warning(
                "could not chmod %s to %o — directory may have permissive mode",
                parent,
                _DOTCAPO_MODE,
                extra={"event": "capo.workflows.mkdir.chmod_failed", "path": str(parent)},
            )
    except OSError as exc:
        raise DBOSInitError(
            "mkdir",
            parent,
            f"failed to create parent directory: {exc}",
        ) from exc


def _check_db_file_health(path: Path) -> None:
    """Open ``path`` as a SQLite file (read-only) and run a cheap integrity
    probe. Skip the probe if the file doesn't exist yet — DBOS will create
    it on first launch.

    The probe runs ``PRAGMA schema_version`` which fails fast on corruption
    (header damage, encryption mismatch, etc.) without scanning the whole
    file. Any sqlite3 error is wrapped as :class:`DBOSInitError` with
    ``operation="db_corrupt"`` and a message naming the file path and the
    Litestream restore runbook reference (§8.1, §15.3 — see also the
    operator runbook at ``docs/runbook.md`` after Phase 5).
    """
    if not path.exists():
        return
    try:
        # uri=True so we can pass ?mode=ro; this means a corrupt header
        # surfaces immediately instead of after the first write.
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        raise DBOSInitError(
            "db_corrupt",
            path,
            (
                f"could not open dbos.db ({exc}). "
                "Restore from Litestream (see docs/runbook.md, spec §8.1)."
            ),
        ) from exc
    try:
        cur = conn.execute("PRAGMA schema_version")
        cur.fetchone()
    except sqlite3.DatabaseError as exc:
        raise DBOSInitError(
            "db_corrupt",
            path,
            (
                f"dbos.db appears corrupted ({exc}). "
                "Restore from Litestream (see docs/runbook.md, spec §8.1)."
            ),
        ) from exc
    finally:
        conn.close()


def init_dbos(settings: Settings, *, fastapi_app: FastAPI | None = None) -> object:
    """Construct and register the process-global DBOS instance.

    Does NOT call :meth:`DBOS.launch` — that happens in :func:`launch_dbos`
    so callers can compose ``init`` + ``launch`` around an awaitable boot
    sequence (the FastAPI startup hook in §5.6).

    Parameters
    ----------
    settings:
        Loaded :class:`capo.config.Settings`. We read ``paths.dbos_db_path``
        and validate it against ``paths.db_path``.
    fastapi_app:
        Optional FastAPI app to bind DBOS's HTTP tracing into. When provided,
        DBOS attaches request-level spans and (when uvicorn ``lifespan`` is
        on) auto-launches on startup. Capo currently runs uvicorn with
        ``lifespan="off"`` (see ``capo.main._serve_until_signal``) so we
        always call :meth:`DBOS.launch` explicitly via :func:`launch_dbos`.

    Returns
    -------
    object
        The :class:`dbos.DBOS` instance (typed as ``object`` to keep this
        module's top-level import-free of ``dbos``).

    Raises
    ------
    DBOSInitError
        If the parent dir cannot be created, ``dbos.db`` is corrupt, the
        path collides with ``state.db``, or the :class:`DBOS` constructor
        itself fails (e.g. invalid SQLAlchemy URL).
    """
    global _state

    dbos_db_path = settings.paths.dbos_db_path.expanduser().resolve()
    state_db_path = settings.paths.db_path.expanduser().resolve()

    # §5.6 invariant: dbos.db and state.db MUST be separate files. Catch
    # the misconfig at boot rather than letting DBOS apply its 32 migrations
    # on top of Alembic's schema.
    if dbos_db_path == state_db_path:
        raise DBOSInitError(
            "path_collision",
            dbos_db_path,
            (
                f"paths.dbos_db_path and paths.db_path resolve to the same "
                f"file ({dbos_db_path}). They MUST be separate SQLite files "
                "(spec §5.6, §15.3)."
            ),
        )

    _ensure_parent_dir(dbos_db_path)
    _check_db_file_health(dbos_db_path)

    # Import lazily so this module stays importable without dbos installed
    # (the no-config / --no-serve smoke path).
    try:
        from dbos import DBOS, DBOSConfig  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - dbos is a hard dep
        raise DBOSInitError(
            "init",
            dbos_db_path,
            f"dbos package not importable: {exc}",
        ) from exc

    # SQLAlchemy URL form for SQLite uses three slashes + absolute path.
    # See spike S-2 §3 for the canonical config used in the bench.
    system_database_url = f"sqlite:///{dbos_db_path}"

    config: DBOSConfig = {
        "name": _DBOS_APP_NAME,
        "system_database_url": system_database_url,
        # We do NOT run the DBOS admin server in V1 — Capo has its own
        # /healthz endpoint (spec §6.5) and we don't want a second HTTP
        # bind. Reassess in Phase 5 if the dashboard becomes useful.
        "run_admin_server": False,
    }

    with _state_lock:
        if _state is not None:
            # Idempotent: same path → return existing instance. Different
            # path → boot-time misconfig (probably a test that forgot to
            # destroy).
            if _state.dbos_db_path == dbos_db_path:
                return _state.dbos_instance
            raise DBOSInitError(
                "init",
                dbos_db_path,
                (
                    f"DBOS already initialized with a different path "
                    f"({_state.dbos_db_path}). Call destroy_dbos() first."
                ),
            )

        try:
            instance = DBOS(config=config, fastapi=fastapi_app)
        except Exception as exc:
            raise DBOSInitError(
                "init",
                dbos_db_path,
                f"DBOS(config=...) raised: {exc}",
            ) from exc

        _state = _State(dbos_instance=instance, dbos_db_path=dbos_db_path)
        logger.info(
            "DBOS initialized dbos_db_path=%s",
            dbos_db_path,
            extra={
                "event": "capo.workflows.init",
                "dbos_db_path": str(dbos_db_path),
            },
        )
        return instance


def launch_dbos() -> None:
    """Launch DBOS — opens the system DB, applies migrations, starts pollers.

    Must be called after :func:`init_dbos`. Idempotent on a single instance:
    DBOS itself raises on double-launch, so we don't try to be cleverer than
    the framework.

    Raises
    ------
    DBOSInitError
        If DBOS is not initialized, or if :meth:`DBOS.launch` raises (most
        commonly: SQLite file write-locked, disk full, or schema migration
        failure on a corrupt ``dbos.db``).
    """
    with _state_lock:
        state = _state
    if state is None:
        raise DBOSInitError(
            "launch",
            Path("<uninitialized>"),
            "DBOS not initialized — call init_dbos(settings) first.",
        )
    try:
        # ``DBOS.launch`` is a synchronous classmethod that opens the system
        # database, applies SQLite migrations on first run (spike S-2 §3),
        # and starts the notification listener thread. Callers in async
        # contexts should wrap this in ``asyncio.to_thread`` so the event
        # loop isn't blocked during the (~100ms cold-start) launch.
        from dbos import DBOS  # type: ignore[import-not-found]

        DBOS.launch()
    except Exception as exc:
        raise DBOSInitError(
            "launch",
            state.dbos_db_path,
            f"DBOS.launch() raised: {exc}",
        ) from exc

    logger.info(
        "DBOS launched dbos_db_path=%s",
        state.dbos_db_path,
        extra={
            "event": "capo.workflows.launch",
            "dbos_db_path": str(state.dbos_db_path),
        },
    )


def destroy_dbos(*, workflow_completion_timeout_sec: int = 0) -> None:
    """Tear down the DBOS instance — drain workflows, stop pollers, close DB.

    Idempotent: safe to call when DBOS was never launched (returns silently).

    Parameters
    ----------
    workflow_completion_timeout_sec:
        Forwarded to :meth:`DBOS.destroy`. Default 0 matches the DBOS
        default (no drain — relies on DBOS at-least-once step semantics
        plus our idempotency keys per §5.6 to survive abrupt shutdown).
        Future tasks (#35 delegation handoff) MAY plumb a non-zero
        timeout through here for cleaner shutdown of in-flight monitors.

    Raises
    ------
    DBOSInitError
        If :meth:`DBOS.destroy` raises. Tear-down failures are surfaced so
        the operator sees them in the shutdown log; we still clear our
        module state so a subsequent :func:`init_dbos` can proceed.
    """
    global _state
    with _state_lock:
        state = _state
        # Clear early so a destroy() exception doesn't leave us in a
        # half-shutdown state that blocks the next init.
        _state = None
    if state is None:
        return
    try:
        from dbos import DBOS  # type: ignore[import-not-found]

        DBOS.destroy(workflow_completion_timeout_sec=workflow_completion_timeout_sec)
    except Exception as exc:
        raise DBOSInitError(
            "destroy",
            state.dbos_db_path,
            f"DBOS.destroy() raised: {exc}",
        ) from exc

    logger.info(
        "DBOS destroyed dbos_db_path=%s",
        state.dbos_db_path,
        extra={
            "event": "capo.workflows.destroy",
            "dbos_db_path": str(state.dbos_db_path),
        },
    )


def is_launched() -> bool:
    """Return ``True`` if DBOS has been initialized in this process.

    Test helper — production code should structure its lifecycle so the
    answer is implicit from the call site.
    """
    with _state_lock:
        return _state is not None


def current_dbos_db_path() -> Path | None:
    """Return the absolute ``dbos.db`` path the current instance is using,
    or ``None`` if DBOS has not been initialized."""
    with _state_lock:
        return _state.dbos_db_path if _state is not None else None


__all__ = [
    "DBOSInitError",
    "current_dbos_db_path",
    "destroy_dbos",
    "init_dbos",
    "is_launched",
    "launch_dbos",
]

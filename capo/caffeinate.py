"""Caffeinate lifecycle helper (spec §8.1 / Task #59).

While at least one delegation row is in ``status='running'`` we want
macOS NOT to sleep (idle / display / disk) so the Capo process can keep
draining subprocess output and persisting checkpoints. The standard tool
for this on macOS is ``caffeinate(1)``; ``caffeinate -i`` "prevents the
system from idle sleeping" and exits when *we* terminate it.

Design
------

This module owns a **single process-wide manager** —
:class:`CaffeinateManager` — that:

* Maintains an in-memory set of "tracked" delegation IDs.
* When the set transitions ``empty → non-empty`` (first
  :meth:`track_delegation`), spawns ``caffeinate -i`` via
  :func:`asyncio.create_subprocess_exec`.
* When the set transitions ``non-empty → empty`` (last
  :meth:`release_delegation`), sends ``SIGTERM`` to the child and waits
  for it to exit.
* Refcounts by tracked-set membership: re-tracking the same ID is a
  no-op; the spawn / reap transitions only fire on the *boundary* moves.
* Uses an :class:`asyncio.Lock` to serialize state transitions so a
  concurrent ``track_delegation`` + ``release_delegation`` race can't
  leave the child orphaned or double-spawn.

Platform behavior
~~~~~~~~~~~~~~~~~

* **macOS** (``sys.platform == "darwin"``): full lifecycle as described.
* **Other platforms** (Linux / Windows): every method is a structured
  no-op (logged once at construction time). The manager still records
  the tracked set so callers can introspect; the spawn step is just
  skipped.
* **``caffeinate`` binary missing on macOS**: ``FileNotFoundError`` is
  caught at spawn time, logged at WARNING level, and the spawn is
  considered a no-op. The tracked set still ticks normally — caller
  contracts are unchanged.

Wiring
~~~~~~

A module-level singleton + registry helper is exposed:

* :func:`get_caffeinate_manager` — returns the registered manager or a
  no-op fallback if none has been installed yet.
* :func:`register_caffeinate_manager` — installs the manager (called
  from :func:`capo.main.amain`).
* :func:`reset_caffeinate_manager_for_tests` — test hook.

The two delegation tools (``delegate_to_claude_code`` /
``delegate_to_codex``) call ``get_caffeinate_manager().track_delegation(
delegation_id)`` immediately after the ``INSERT`` that flips the row to
``status='running'``. The DBOS monitor workflow
(:func:`capo.workflows.delegation._monitor_body`) calls
``release_delegation(delegation_id)`` from inside the terminal-status
step. Cold-boot sweep
(:func:`capo.main._cold_boot_resume_sweep`) re-tracks every running
row before scheduling the resume workflow.

Concurrency
~~~~~~~~~~~

The state transitions ``empty → spawned`` and ``spawned → reaped`` are
guarded by an :class:`asyncio.Lock`. Tests that exercise concurrent
track + release verify the invariant: a single ``caffeinate`` instance
ever exists at any time. The lock is per-manager, not module-level, so
test instances can run in parallel.

Spec references
~~~~~~~~~~~~~~~

* §8.1 "Operational Considerations" — launchd plist + caffeinate.
* §9.5 Phase 5 deliverable table — ``caffeinate`` helper line:
  "Runs ``caffeinate -i`` while any delegation in ``running`` state".
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
import sys
import threading
from typing import Any

logger = logging.getLogger("capo.caffeinate")

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: Argv used to start caffeinate. ``-i`` prevents idle sleep; we
#: intentionally OMIT ``-w <pid>`` so caffeinate doesn't auto-exit when
#: some unrelated process dies — *we* manage lifetime by sending SIGTERM.
_CAFFEINATE_ARGV: tuple[str, ...] = ("caffeinate", "-i")

#: How long to wait for caffeinate to exit after SIGTERM before giving
#: up. caffeinate handles SIGTERM cleanly and exits ~immediately, so this
#: is a defensive ceiling. On timeout we send SIGKILL.
_CAFFEINATE_TERM_WAIT_S: float = 5.0


def _is_macos() -> bool:
    """Return True on macOS, False otherwise.

    Wrapped in a helper so tests can monkeypatch the platform check
    without touching :data:`sys.platform`.
    """
    return sys.platform == "darwin"


def _caffeinate_binary_available() -> bool:
    """Return True if the ``caffeinate`` binary is on PATH.

    On non-macOS platforms returns False unconditionally so callers can
    short-circuit without any subprocess attempt.
    """
    if not _is_macos():
        return False
    return shutil.which("caffeinate") is not None


# ---------------------------------------------------------------------------
# CaffeinateManager
# ---------------------------------------------------------------------------


class CaffeinateManager:
    """Refcounted ``caffeinate -i`` lifecycle for active delegations.

    One instance per Capo process (installed via
    :func:`register_caffeinate_manager`). Public API:

    * :meth:`track_delegation(delegation_id)` — add ``delegation_id`` to
      the active set. On the empty→non-empty boundary, spawn
      ``caffeinate -i``.
    * :meth:`release_delegation(delegation_id)` — remove
      ``delegation_id`` from the active set. On the non-empty→empty
      boundary, SIGTERM the caffeinate child and wait for it to exit.
    * :meth:`start()` / :meth:`stop()` — explicit lifecycle hooks for
      callers that want to manage the manager independently of any
      delegation set (e.g. boot-time keepalive during a long-running
      maintenance window). ``start()`` is a no-op if caffeinate is
      already running; ``stop()`` clears the tracked set and reaps.
    * :meth:`active_delegations()` — read-only snapshot of the tracked
      set (returned as a sorted tuple for stable test assertions).
    * :meth:`is_running()` — True if a caffeinate child is currently
      live in this process.

    Concurrency: an :class:`asyncio.Lock` serializes spawn / reap
    transitions. Methods are coroutine-safe; calling
    :meth:`track_delegation` from one task while another awaits
    :meth:`release_delegation` is fine — the lock guarantees the
    transitions are serialized.
    """

    def __init__(self, *, enabled: bool | None = None) -> None:
        """Initialize the manager.

        Parameters
        ----------
        enabled:
            Force-enable or force-disable lifecycle behavior. When
            ``None`` (default), the manager auto-detects: enabled on
            macOS, disabled elsewhere. Disabled managers still track
            the delegation set so :meth:`active_delegations` works for
            introspection; they just never spawn anything.
        """
        if enabled is None:
            enabled = _is_macos()
        self._enabled: bool = enabled
        self._lock: asyncio.Lock = asyncio.Lock()
        self._active: set[str] = set()
        self._process: Any = None  # asyncio.subprocess.Process | None
        self._spawn_failed: bool = False
        # Used by tests to override the spawn callable.
        self._spawner: Any = None

        if not self._enabled and _is_macos():
            # Caller explicitly forced disabled on macOS — informational.
            logger.info(
                "caffeinate manager constructed disabled on macOS",
                extra={"event": "capo.caffeinate.disabled_explicit"},
            )
        elif not self._enabled:
            logger.info(
                "caffeinate manager disabled (non-macOS platform: %s); "
                "track/release calls will no-op",
                sys.platform,
                extra={
                    "event": "capo.caffeinate.disabled_platform",
                    "platform": sys.platform,
                },
            )

    # -- introspection ---------------------------------------------------

    def is_enabled(self) -> bool:
        """Return True if lifecycle behavior is active on this platform."""
        return self._enabled

    def is_running(self) -> bool:
        """Return True if a caffeinate child subprocess is currently live.

        "Live" means the manager spawned a child and it hasn't been
        reaped (or auto-exited). Best-effort: if the child exited on its
        own (rare for ``caffeinate -i``), we'll detect that on the next
        :meth:`release_delegation` / :meth:`stop` call.
        """
        proc = self._process
        if proc is None:
            return False
        # ``returncode`` is ``None`` while the process is still running.
        return proc.returncode is None

    def active_delegations(self) -> tuple[str, ...]:
        """Snapshot of currently-tracked delegation IDs (sorted)."""
        return tuple(sorted(self._active))

    # -- public lifecycle ------------------------------------------------

    async def start(self) -> None:
        """Explicitly start caffeinate (no delegation context).

        Useful as a debugging / maintenance hook. Most callers should
        rely on :meth:`track_delegation` instead — the empty→non-empty
        boundary calls :meth:`_spawn` under the same lock.

        No-op if the manager is disabled, the binary is missing, or
        caffeinate is already running.
        """
        async with self._lock:
            await self._spawn_if_needed()

    async def stop(self) -> None:
        """Explicitly stop caffeinate.

        Clears the tracked-delegation set and reaps the child. Use at
        process shutdown if the caller wants to guarantee no lingering
        ``caffeinate`` process on exit, even if delegations are still
        marked ``running`` in the database (those will be picked up by
        the cold-boot sweep on next start).
        """
        async with self._lock:
            self._active.clear()
            await self._reap_if_running()

    # -- refcount API ----------------------------------------------------

    async def track_delegation(self, delegation_id: str) -> None:
        """Mark ``delegation_id`` as active; spawn caffeinate if first.

        Idempotent for the same ``delegation_id`` — re-tracking is a
        no-op. The first call (empty→non-empty boundary) spawns
        ``caffeinate -i``. Spawn failure (binary missing) is logged at
        WARNING and the active set is still updated; subsequent
        releases will reap normally (no-op).
        """
        if not delegation_id:
            raise ValueError("delegation_id must be non-empty")
        async with self._lock:
            was_empty = not self._active
            self._active.add(delegation_id)
            if was_empty:
                logger.info(
                    "caffeinate: first delegation tracked (%s) — spawning",
                    delegation_id,
                    extra={
                        "event": "capo.caffeinate.track.first",
                        "delegation_id": delegation_id,
                    },
                )
                await self._spawn_if_needed()
            else:
                logger.debug(
                    "caffeinate: tracking delegation (%s); active=%d",
                    delegation_id,
                    len(self._active),
                    extra={
                        "event": "capo.caffeinate.track.add",
                        "delegation_id": delegation_id,
                        "active_count": len(self._active),
                    },
                )

    async def release_delegation(self, delegation_id: str) -> None:
        """Remove ``delegation_id`` from active set; reap if last.

        Safe to call multiple times for the same ID — only the first
        call mutates state. On the last-release (non-empty→empty
        boundary) sends SIGTERM to the caffeinate child and awaits its
        exit.
        """
        if not delegation_id:
            raise ValueError("delegation_id must be non-empty")
        async with self._lock:
            if delegation_id not in self._active:
                logger.debug(
                    "caffeinate: release_delegation(%s) — not tracked, "
                    "no-op",
                    delegation_id,
                    extra={
                        "event": "capo.caffeinate.release.unknown",
                        "delegation_id": delegation_id,
                    },
                )
                return
            self._active.discard(delegation_id)
            if not self._active:
                logger.info(
                    "caffeinate: last delegation released (%s) — reaping",
                    delegation_id,
                    extra={
                        "event": "capo.caffeinate.release.last",
                        "delegation_id": delegation_id,
                    },
                )
                await self._reap_if_running()
            else:
                logger.debug(
                    "caffeinate: released delegation (%s); active=%d",
                    delegation_id,
                    len(self._active),
                    extra={
                        "event": "capo.caffeinate.release.remove",
                        "delegation_id": delegation_id,
                        "active_count": len(self._active),
                    },
                )

    # -- internals -------------------------------------------------------

    def _set_spawner_for_tests(self, spawner: Any) -> None:
        """Inject a custom spawner callable for integration tests.

        The spawner must be an awaitable returning an object with
        ``returncode`` / ``terminate()`` / ``kill()`` / ``wait()``
        attributes (same shape as :class:`asyncio.subprocess.Process`).
        """
        self._spawner = spawner

    async def _spawn_if_needed(self) -> None:
        """Spawn ``caffeinate -i`` if not already running.

        Caller MUST hold :attr:`_lock`. No-op when the manager is
        disabled, the binary is missing, or a previous spawn already
        failed (latched). Logs WARNING + clears the spawn-failed latch
        on success.
        """
        if not self._enabled:
            return
        if self.is_running():
            return
        if self._spawn_failed:
            # Already tried + failed; don't retry every track call —
            # operator should fix PATH and restart Capo.
            return

        spawner = self._spawner
        try:
            if spawner is not None:
                proc = await spawner()
            else:
                # Belt + suspenders: explicit which-check so we get a
                # clear log message rather than a generic
                # FileNotFoundError from create_subprocess_exec.
                if shutil.which(_CAFFEINATE_ARGV[0]) is None:
                    raise FileNotFoundError(
                        f"{_CAFFEINATE_ARGV[0]!r} not on PATH"
                    )
                proc = await asyncio.create_subprocess_exec(
                    *_CAFFEINATE_ARGV,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    stdin=asyncio.subprocess.DEVNULL,
                )
        except FileNotFoundError as exc:
            logger.warning(
                "caffeinate binary missing — sleep prevention disabled. "
                "Install caffeinate (ships with macOS by default) or "
                "ignore this on non-macOS hosts. (%s)",
                exc,
                extra={"event": "capo.caffeinate.spawn.binary_missing"},
            )
            self._spawn_failed = True
            return
        except OSError as exc:
            logger.warning(
                "caffeinate spawn failed (%s: %s) — sleep prevention "
                "disabled",
                exc.__class__.__name__,
                exc,
                extra={"event": "capo.caffeinate.spawn.failed"},
            )
            self._spawn_failed = True
            return

        self._process = proc
        logger.info(
            "caffeinate spawned (pid=%s)",
            getattr(proc, "pid", "?"),
            extra={
                "event": "capo.caffeinate.spawn.ok",
                "pid": getattr(proc, "pid", None),
            },
        )

    async def _reap_if_running(self) -> None:
        """SIGTERM caffeinate + wait; SIGKILL on timeout.

        Caller MUST hold :attr:`_lock`. No-op if no process is tracked
        or the process already exited on its own.
        """
        proc = self._process
        self._process = None
        if proc is None:
            return
        if proc.returncode is not None:
            # Already exited on its own — nothing to do.
            logger.debug(
                "caffeinate already exited (rc=%s); skipping SIGTERM",
                proc.returncode,
                extra={
                    "event": "capo.caffeinate.reap.already_exited",
                    "returncode": proc.returncode,
                },
            )
            return

        try:
            proc.terminate()
        except ProcessLookupError:
            # Already gone (OS reaped it between the rc check and the
            # signal). Treat as success.
            logger.debug(
                "caffeinate already gone before terminate()",
                extra={"event": "capo.caffeinate.reap.race"},
            )
            return
        except OSError as exc:
            logger.warning(
                "caffeinate terminate() failed (%s: %s)",
                exc.__class__.__name__,
                exc,
                extra={"event": "capo.caffeinate.reap.terminate_failed"},
            )
            # Continue to wait anyway so we don't leak the handle.

        try:
            await asyncio.wait_for(proc.wait(), timeout=_CAFFEINATE_TERM_WAIT_S)
        except TimeoutError:
            logger.warning(
                "caffeinate did not exit within %ss of SIGTERM — sending "
                "SIGKILL",
                _CAFFEINATE_TERM_WAIT_S,
                extra={"event": "capo.caffeinate.reap.sigkill"},
            )
            with contextlib.suppress(ProcessLookupError, OSError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

        logger.info(
            "caffeinate reaped (rc=%s)",
            proc.returncode,
            extra={
                "event": "capo.caffeinate.reap.ok",
                "returncode": proc.returncode,
            },
        )


# ---------------------------------------------------------------------------
# Module-level singleton registry
# ---------------------------------------------------------------------------


_registry_lock = threading.Lock()
_MANAGER: CaffeinateManager | None = None
_NOOP_MANAGER: CaffeinateManager | None = None


def _get_noop_manager() -> CaffeinateManager:
    """Return a singleton disabled manager for the fallback path.

    Constructed lazily so we don't allocate one on every import. Always
    returns the same instance so callers can ``await
    get_caffeinate_manager().track_delegation(...)`` without surprise.
    """
    global _NOOP_MANAGER
    if _NOOP_MANAGER is None:
        _NOOP_MANAGER = CaffeinateManager(enabled=False)
    return _NOOP_MANAGER


def get_caffeinate_manager() -> CaffeinateManager:
    """Return the registered manager, or a no-op fallback.

    Callers in delegation tools / workflow steps should always go
    through this helper — it lets unit tests avoid touching the
    registry while keeping production wiring uniform. If
    :func:`register_caffeinate_manager` hasn't been called yet (e.g.
    during boot before :func:`capo.main.amain` has fully wired
    everything), this returns a disabled :class:`CaffeinateManager`
    that no-ops every method.
    """
    with _registry_lock:
        mgr = _MANAGER
    if mgr is not None:
        return mgr
    return _get_noop_manager()


def register_caffeinate_manager(manager: CaffeinateManager | None) -> None:
    """Install (or clear) the process-wide caffeinate manager.

    Called by :func:`capo.main.amain` with a fresh instance during
    boot, and with ``None`` during shutdown to clear the registration
    so a subsequent in-process restart (tests) doesn't see a manager
    bound to a stopped subprocess.
    """
    global _MANAGER
    with _registry_lock:
        _MANAGER = manager


def reset_caffeinate_manager_for_tests() -> None:
    """Drop both the registered manager and the no-op fallback singleton.

    Tests that exercise the registry should call this in their teardown
    so cross-test bleed doesn't leak a stale (potentially closed)
    manager into the next test.
    """
    global _MANAGER, _NOOP_MANAGER
    with _registry_lock:
        _MANAGER = None
        _NOOP_MANAGER = None


__all__ = [
    "CaffeinateManager",
    "get_caffeinate_manager",
    "register_caffeinate_manager",
    "reset_caffeinate_manager_for_tests",
]

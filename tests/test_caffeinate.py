"""Tests for ``capo.caffeinate`` (Task #59 — caffeinate lifecycle helper).

Covers the §8.1 / Task #59 acceptance criteria:

* ``CaffeinateManager`` exposes ``start`` / ``stop`` /
  ``track_delegation`` / ``release_delegation``.
* First ``track_delegation`` spawns ``caffeinate -i``; subsequent
  tracks for the same / new IDs do not re-spawn.
* Last ``release_delegation`` sends SIGTERM to the child and waits
  for exit.
* Wiring: spawn site (``delegate_to_claude_code`` /
  ``delegate_to_codex``) calls ``track_delegation`` after row INSERT;
  workflow terminal-status step calls ``release_delegation``.
* macOS-only — Linux gracefully no-ops.
* Cold-boot recovery sweep re-tracks running rows.
* Missing binary → log warning + no-op (no exception bubbles up).
* Concurrent track + release serialized by ``asyncio.Lock``.
* Singleton registry: ``get_caffeinate_manager`` returns the
  registered instance or a disabled fallback.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any

import pytest

from capo import caffeinate
from capo.caffeinate import (
    CaffeinateManager,
    get_caffeinate_manager,
    register_caffeinate_manager,
    reset_caffeinate_manager_for_tests,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeProcess:
    """In-memory stand-in for :class:`asyncio.subprocess.Process`.

    Tracks ``terminate()`` / ``kill()`` / ``wait()`` calls so tests can
    assert lifecycle. ``returncode`` flips from ``None`` to a finite
    int when the process is "exited" (via ``terminate()`` + ``wait()``
    or by an external call to :meth:`_simulate_exit`).
    """

    def __init__(
        self,
        *,
        pid: int = 12345,
        ignore_sigterm: bool = False,
    ) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = 0
        self._ignore_sigterm = ignore_sigterm
        self._exit_event = asyncio.Event()
        self._terminate_set_rc: int = -signal.SIGTERM

    def terminate(self) -> None:
        self.terminate_calls += 1
        if not self._ignore_sigterm:
            self.returncode = self._terminate_set_rc
            self._exit_event.set()

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -signal.SIGKILL
        self._exit_event.set()

    async def wait(self) -> int:
        self.wait_calls += 1
        if self.returncode is None:
            await self._exit_event.wait()
        assert self.returncode is not None
        return self.returncode

    def _simulate_exit(self, rc: int) -> None:
        self.returncode = rc
        self._exit_event.set()


def _make_manager_with_fake(
    fake_proc: _FakeProcess | None = None,
    *,
    enabled: bool = True,
) -> tuple[CaffeinateManager, list[int]]:
    """Construct an enabled manager + inject a controllable spawner.

    Returns ``(manager, spawn_call_counter)``. The counter is a list
    that gets appended to on each spawn invocation; tests assert
    ``len(counter)`` to count spawn events.
    """
    mgr = CaffeinateManager(enabled=enabled)
    counter: list[int] = []
    if fake_proc is not None:
        async def _spawner() -> _FakeProcess:
            counter.append(1)
            return fake_proc
    else:
        async def _spawner() -> _FakeProcess:
            counter.append(1)
            return _FakeProcess()
    mgr._set_spawner_for_tests(_spawner)
    return mgr, counter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_registry() -> None:
    reset_caffeinate_manager_for_tests()
    yield
    reset_caffeinate_manager_for_tests()


# ---------------------------------------------------------------------------
# Refcount logic — Functional acceptance criteria
# ---------------------------------------------------------------------------


async def test_first_track_spawns_caffeinate() -> None:
    fake = _FakeProcess()
    mgr, counter = _make_manager_with_fake(fake)
    assert not mgr.is_running()

    await mgr.track_delegation("d1")

    assert len(counter) == 1
    assert mgr.is_running()
    assert mgr.active_delegations() == ("d1",)


async def test_second_track_same_id_is_noop() -> None:
    fake = _FakeProcess()
    mgr, counter = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")
    await mgr.track_delegation("d1")

    assert len(counter) == 1
    assert mgr.active_delegations() == ("d1",)


async def test_second_track_different_id_does_not_respawn() -> None:
    fake = _FakeProcess()
    mgr, counter = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")
    await mgr.track_delegation("d2")

    assert len(counter) == 1
    assert mgr.active_delegations() == ("d1", "d2")
    assert mgr.is_running()


async def test_release_last_id_sends_sigterm() -> None:
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")

    assert fake.terminate_calls == 1
    assert fake.wait_calls >= 1
    assert not mgr.is_running()
    assert mgr.active_delegations() == ()


async def test_release_one_of_two_keeps_caffeinate_running() -> None:
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")
    await mgr.track_delegation("d2")
    await mgr.release_delegation("d1")

    assert fake.terminate_calls == 0
    assert mgr.is_running()
    assert mgr.active_delegations() == ("d2",)


async def test_release_unknown_id_is_noop() -> None:
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")

    await mgr.release_delegation("never-tracked")

    assert fake.terminate_calls == 0
    assert mgr.is_running()
    assert mgr.active_delegations() == ("d1",)


async def test_release_then_re_track_respawns() -> None:
    """After last release, a new track must spawn a NEW caffeinate."""
    fake_a = _FakeProcess(pid=1)
    fake_b = _FakeProcess(pid=2)
    procs = [fake_a, fake_b]
    counter: list[int] = []
    mgr = CaffeinateManager(enabled=True)

    async def _spawner() -> _FakeProcess:
        counter.append(1)
        return procs.pop(0)

    mgr._set_spawner_for_tests(_spawner)

    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")
    await mgr.track_delegation("d2")

    assert len(counter) == 2
    assert mgr.is_running()


async def test_explicit_start_and_stop() -> None:
    fake = _FakeProcess()
    mgr, counter = _make_manager_with_fake(fake)

    await mgr.start()
    assert mgr.is_running()
    assert len(counter) == 1
    # Active set unaffected by explicit start.
    assert mgr.active_delegations() == ()

    await mgr.stop()
    assert not mgr.is_running()
    assert fake.terminate_calls == 1


async def test_stop_clears_active_set_and_reaps() -> None:
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")
    await mgr.track_delegation("d2")
    assert mgr.is_running()

    await mgr.stop()

    assert mgr.active_delegations() == ()
    assert fake.terminate_calls == 1
    assert not mgr.is_running()


async def test_start_when_already_running_is_noop() -> None:
    fake = _FakeProcess()
    mgr, counter = _make_manager_with_fake(fake)
    await mgr.start()
    await mgr.start()
    assert len(counter) == 1


# ---------------------------------------------------------------------------
# Platform / binary edge cases
# ---------------------------------------------------------------------------


async def test_disabled_manager_track_is_noop() -> None:
    """Non-macOS: enabled=False — track/release work but never spawn."""
    mgr, counter = _make_manager_with_fake(_FakeProcess(), enabled=False)

    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")

    assert len(counter) == 0
    assert not mgr.is_running()


async def test_disabled_manager_tracks_set_for_introspection() -> None:
    """Disabled manager still records tracked IDs."""
    mgr = CaffeinateManager(enabled=False)
    await mgr.track_delegation("d1")
    await mgr.track_delegation("d2")
    assert mgr.active_delegations() == ("d1", "d2")
    assert mgr.is_enabled() is False


async def test_missing_binary_logs_and_noops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``caffeinate`` not on PATH → spawn logs WARNING + no-op."""
    mgr = CaffeinateManager(enabled=True)

    async def _raise() -> Any:
        raise FileNotFoundError("caffeinate not on PATH")

    mgr._set_spawner_for_tests(_raise)

    # Should NOT raise.
    await mgr.track_delegation("d1")

    assert not mgr.is_running()
    # Tracked set still incremented — caller contract unaffected.
    assert mgr.active_delegations() == ("d1",)

    # Latched: further tracks don't retry the spawn either.
    counter: list[int] = []

    async def _spawner_counting() -> Any:
        counter.append(1)
        raise FileNotFoundError("still missing")

    mgr._set_spawner_for_tests(_spawner_counting)
    await mgr.track_delegation("d2")
    assert len(counter) == 0  # latched — never called


async def test_track_then_release_with_missing_binary_no_error() -> None:
    """End-to-end no-binary path must never raise."""
    mgr = CaffeinateManager(enabled=True)

    async def _raise() -> Any:
        raise FileNotFoundError("caffeinate not on PATH")

    mgr._set_spawner_for_tests(_raise)
    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")
    assert not mgr.is_running()


async def test_default_enabled_matches_macos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``enabled=None``: macOS → enabled, others → disabled."""
    monkeypatch.setattr(caffeinate, "_is_macos", lambda: True)
    mgr = CaffeinateManager()
    assert mgr.is_enabled() is True

    monkeypatch.setattr(caffeinate, "_is_macos", lambda: False)
    mgr2 = CaffeinateManager()
    assert mgr2.is_enabled() is False


# ---------------------------------------------------------------------------
# Concurrency / asyncio.Lock serialization
# ---------------------------------------------------------------------------


async def test_concurrent_track_calls_spawn_exactly_once() -> None:
    """N parallel tracks for distinct IDs must spawn exactly one caffeinate."""
    fake = _FakeProcess()
    counter: list[int] = []
    mgr = CaffeinateManager(enabled=True)
    spawn_started = asyncio.Event()
    spawn_can_finish = asyncio.Event()

    async def _slow_spawner() -> _FakeProcess:
        counter.append(1)
        spawn_started.set()
        await spawn_can_finish.wait()
        return fake

    mgr._set_spawner_for_tests(_slow_spawner)

    tasks = [
        asyncio.create_task(mgr.track_delegation(f"d{i}"))
        for i in range(5)
    ]
    await spawn_started.wait()
    # All 4 other tasks queued on the lock waiting for the first
    # track's spawn to complete.
    spawn_can_finish.set()
    await asyncio.gather(*tasks)

    assert len(counter) == 1
    assert len(mgr.active_delegations()) == 5


async def test_concurrent_track_and_release_serialized() -> None:
    """Track + release race ends with no caffeinate leak."""
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)

    # Seed one active so concurrent ops have something to race over.
    await mgr.track_delegation("base")

    async def _add(did: str) -> None:
        await mgr.track_delegation(did)

    async def _rm(did: str) -> None:
        await mgr.release_delegation(did)

    tasks = []
    for i in range(10):
        tasks.append(asyncio.create_task(_add(f"d{i}")))
        tasks.append(asyncio.create_task(_rm(f"d{i}")))

    await asyncio.gather(*tasks)

    # ``base`` remained; ``d0..d9`` were added then removed.
    assert mgr.active_delegations() == ("base",)
    assert mgr.is_running()

    # Final release drops to zero.
    await mgr.release_delegation("base")
    assert mgr.active_delegations() == ()
    assert not mgr.is_running()
    assert fake.terminate_calls == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


async def test_track_empty_id_raises() -> None:
    mgr = CaffeinateManager(enabled=False)
    with pytest.raises(ValueError):
        await mgr.track_delegation("")


async def test_release_empty_id_raises() -> None:
    mgr = CaffeinateManager(enabled=False)
    with pytest.raises(ValueError):
        await mgr.release_delegation("")


# ---------------------------------------------------------------------------
# Reap edge cases
# ---------------------------------------------------------------------------


async def test_reap_when_caffeinate_already_exited() -> None:
    """If caffeinate exited on its own, reap is a no-op."""
    fake = _FakeProcess()
    mgr, _ = _make_manager_with_fake(fake)
    await mgr.track_delegation("d1")

    # Simulate caffeinate exiting on its own.
    fake._simulate_exit(0)
    assert not mgr.is_running()

    await mgr.release_delegation("d1")

    # Should not have called terminate — process was already gone.
    assert fake.terminate_calls == 0


async def test_reap_with_sigterm_timeout_falls_back_to_sigkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If caffeinate ignores SIGTERM, manager falls back to SIGKILL."""
    monkeypatch.setattr(caffeinate, "_CAFFEINATE_TERM_WAIT_S", 0.05)

    fake = _FakeProcess(ignore_sigterm=True)

    mgr = CaffeinateManager(enabled=True)

    async def _spawner() -> _FakeProcess:
        return fake

    mgr._set_spawner_for_tests(_spawner)

    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")

    assert fake.terminate_calls == 1
    assert fake.kill_calls == 1
    assert not mgr.is_running()


async def test_terminate_processlookuperror_handled() -> None:
    """terminate() raising ProcessLookupError treated as success."""
    fake = _FakeProcess()

    def _raise_lookup() -> None:
        raise ProcessLookupError

    fake.terminate = _raise_lookup  # type: ignore[method-assign]

    mgr = CaffeinateManager(enabled=True)

    async def _spawner() -> _FakeProcess:
        return fake

    mgr._set_spawner_for_tests(_spawner)

    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")
    # No exception bubbled.
    assert not mgr.is_running()


# ---------------------------------------------------------------------------
# Module-level singleton registry
# ---------------------------------------------------------------------------


def test_get_returns_noop_when_unregistered() -> None:
    """Default get returns a disabled fallback singleton."""
    mgr = get_caffeinate_manager()
    assert isinstance(mgr, CaffeinateManager)
    assert mgr.is_enabled() is False


def test_get_returns_registered_manager() -> None:
    custom = CaffeinateManager(enabled=False)
    register_caffeinate_manager(custom)
    assert get_caffeinate_manager() is custom


def test_register_none_clears_registration() -> None:
    custom = CaffeinateManager(enabled=False)
    register_caffeinate_manager(custom)
    register_caffeinate_manager(None)
    # Falls back to noop singleton again.
    assert get_caffeinate_manager() is not custom


async def test_noop_fallback_is_safe_to_use() -> None:
    """Unregistered get → returned fallback supports track/release."""
    mgr = get_caffeinate_manager()
    await mgr.track_delegation("d1")
    await mgr.release_delegation("d1")
    assert not mgr.is_running()


# ---------------------------------------------------------------------------
# Workflow integration — release wired into _persist_terminal_step
# ---------------------------------------------------------------------------


def test_workflow_terminal_step_wires_caffeinate_release() -> None:
    """Wiring check: ``_persist_terminal_step`` releases caffeinate.

    The terminal-status step is wrapped twice (idempotent_step +
    DBOS.step), so unit-testing the body in isolation requires a DBOS
    workflow context. Instead we lock in the wiring by source
    inspection — a regression that removes the release call would
    silently leak caffeinate at end-of-delegation.
    """
    from pathlib import Path

    import capo.workflows.delegation as wf_delegation

    src = Path(wf_delegation.__file__).read_text()
    # Extract just the _persist_terminal_step body for a tight match.
    marker = "async def _persist_terminal_step("
    assert marker in src
    body_start = src.index(marker)
    # Find the next top-level "async def" or "def" to bound the body.
    rest = src[body_start + len(marker):]
    next_async = rest.find("\nasync def ")
    next_def = rest.find("\ndef ")
    candidates = [c for c in (next_async, next_def) if c != -1]
    body_end = body_start + len(marker) + (min(candidates) if candidates else len(rest))
    body = src[body_start:body_end]
    assert "release_delegation" in body, (
        "release_delegation call must remain wired into "
        "_persist_terminal_step (Task #59)"
    )
    assert "get_caffeinate_manager" in body


def test_delegate_claude_code_tracks_caffeinate() -> None:
    """Wiring check: ``delegate_to_claude_code`` calls track_delegation."""
    from pathlib import Path

    import capo.tools.claude_code as cc_mod

    src = Path(cc_mod.__file__).read_text()
    assert "get_caffeinate_manager().track_delegation(delegation_id)" in src
    # And release on the failure paths.
    assert "release_delegation(delegation_id)" in src


def test_delegate_codex_tracks_caffeinate() -> None:
    """Wiring check: ``delegate_to_codex`` calls track_delegation."""
    from pathlib import Path

    import capo.tools.codex as codex_mod

    src = Path(codex_mod.__file__).read_text()
    assert "get_caffeinate_manager().track_delegation(delegation_id)" in src
    assert "release_delegation(delegation_id)" in src


def test_main_amain_registers_manager() -> None:
    """Wiring check: ``capo.main.amain`` constructs + registers manager."""
    from pathlib import Path

    import capo.main as main_mod

    src = Path(main_mod.__file__).read_text()
    assert "CaffeinateManager" in src
    assert "register_caffeinate_manager(caffeinate_manager)" in src


def test_main_cold_boot_sweep_tracks_running() -> None:
    """Wiring check: cold-boot sweep tracks rows BEFORE resume invocation."""
    from pathlib import Path

    import capo.main as main_mod

    src = Path(main_mod.__file__).read_text()
    # Pin the call shape — the cold-boot sweep must track each running id.
    assert "_get_caf_mgr().track_delegation(_did)" in src


# ---------------------------------------------------------------------------
# Integration: spawn real subprocess (sleep) + SIGTERM on release
# ---------------------------------------------------------------------------


async def test_real_subprocess_sigterm_on_release() -> None:
    """Spawn a fake "caffeinate" (sleep subprocess) — verify SIGTERM."""
    mgr = CaffeinateManager(enabled=True)

    spawn_pids: list[int] = []

    async def _spawner() -> Any:
        proc = await asyncio.create_subprocess_exec(
            "sleep",
            "30",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        spawn_pids.append(proc.pid)
        return proc

    mgr._set_spawner_for_tests(_spawner)

    await mgr.track_delegation("d-real")
    assert mgr.is_running()
    assert len(spawn_pids) == 1

    await mgr.release_delegation("d-real")

    assert not mgr.is_running()
    # Process should be reaped — returncode is the negative signal
    # (sleep exits on SIGTERM with returncode == -SIGTERM on POSIX).

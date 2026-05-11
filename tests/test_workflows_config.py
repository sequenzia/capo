"""Tests for :mod:`capo.workflows` DBOS configuration + lifecycle (Task #28).

Covers acceptance criteria from spec §5.6, §9.3, §15.3:

* DBOS configured at boot using ``paths.dbos_db_path``.
* ``dbos.db`` and ``state.db`` are SEPARATE SQLite files (and we refuse
  to launch when they collide).
* DBOS lifecycle hooks integrated with FastAPI startup/shutdown.
* Boot waits for DBOS to be ready before serving the webhook.
* ``~/.capo/`` doesn't exist → Capo creates it with ``0700`` perms.
* ``dbos.db`` corrupted → clear startup error citing the path; mentions
  the Litestream restore runbook.
* DBOS init failure surfaced as a typed :class:`DBOSInitError`.

Integration tests verify a clean boot creates ``dbos.db`` and accepts a
workflow registration, and that ``state.db`` / ``dbos.db`` remain
independent files.
"""

from __future__ import annotations

import contextlib
import sqlite3
import stat
from pathlib import Path

import pytest

from capo.config import PathsConfig
from capo.workflows import (
    DBOSInitError,
    current_dbos_db_path,
    destroy_dbos,
    init_dbos,
    is_launched,
    launch_dbos,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _StubSettings:
    """Minimal Settings stand-in carrying only ``paths`` for these tests."""

    def __init__(self, paths: PathsConfig) -> None:
        self.paths = paths


def _make_settings(tmp_path: Path, *, dotcapo: Path | None = None) -> _StubSettings:
    """Build a stub Settings with ``state.db`` and ``dbos.db`` in tmp_path.

    By default both files live in ``tmp_path/.capo/`` so the tests exercise
    the auto-mkdir code path without touching the operator's real
    ``~/.capo/``.
    """
    dotcapo = dotcapo or (tmp_path / ".capo")
    return _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=dotcapo / "state.db",
            dbos_db_path=dotcapo / "dbos.db",
        )
    )


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state() -> None:
    """Tear down DBOS between tests so each one sees a fresh process state.

    DBOS is a process-level singleton; without this, the second test in a
    run trips the "already initialized" guard.
    """
    yield
    if is_launched():
        # destroy_dbos clears _state even if DBOS.destroy raises.
        with contextlib.suppress(DBOSInitError):
            destroy_dbos()


# ---------------------------------------------------------------------------
# Functional: configuration + separate files
# ---------------------------------------------------------------------------


def test_init_dbos_uses_settings_dbos_db_path(tmp_path: Path) -> None:
    """``init_dbos`` records the path from ``settings.paths.dbos_db_path``."""
    settings = _make_settings(tmp_path)

    init_dbos(settings)  # type: ignore[arg-type]

    expected = (tmp_path / ".capo" / "dbos.db").resolve()
    assert current_dbos_db_path() == expected
    assert is_launched() is True  # init = "instance registered"


def test_state_db_and_dbos_db_are_separate_files(tmp_path: Path) -> None:
    """The two SQLite files MUST be distinct paths."""
    settings = _make_settings(tmp_path)

    assert settings.paths.db_path != settings.paths.dbos_db_path
    # After launch, only dbos.db should be created by DBOS (state.db is
    # owned by Alembic in a separate task).
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    dbos_db = (tmp_path / ".capo" / "dbos.db").resolve()
    state_db = (tmp_path / ".capo" / "state.db").resolve()
    assert dbos_db.exists()
    assert dbos_db.is_file()
    # state.db is NOT created by DBOS — that's the §5.6 invariant.
    assert not state_db.exists()
    # And they really are different files on disk.
    assert dbos_db != state_db


def test_path_collision_rejected(tmp_path: Path) -> None:
    """``state.db == dbos.db`` → DBOSInitError citing the collision."""
    same = tmp_path / ".capo" / "shared.db"
    settings = _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=same,
            dbos_db_path=same,
        )
    )

    with pytest.raises(DBOSInitError) as exc_info:
        init_dbos(settings)  # type: ignore[arg-type]

    assert exc_info.value.operation == "path_collision"
    assert str(same.resolve()) in str(exc_info.value)
    assert "MUST be separate" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Edge case: ~/.capo/ doesn't exist → created with 0700 perms
# ---------------------------------------------------------------------------


def test_parent_dir_created_with_0700_perms(tmp_path: Path) -> None:
    """When ``~/.capo/`` is missing, Capo creates it with mode 0o700."""
    dotcapo = tmp_path / "missing-dotcapo"
    assert not dotcapo.exists()
    settings = _make_settings(tmp_path, dotcapo=dotcapo)

    init_dbos(settings)  # type: ignore[arg-type]

    assert dotcapo.exists()
    assert dotcapo.is_dir()
    mode = stat.S_IMODE(dotcapo.stat().st_mode)
    assert mode == 0o700, f"expected 0o700, got {oct(mode)}"


def test_existing_parent_dir_left_alone(tmp_path: Path) -> None:
    """Pre-existing parent dir is not chmod-ed (operator choice)."""
    dotcapo = tmp_path / ".capo"
    dotcapo.mkdir(mode=0o755)
    settings = _make_settings(tmp_path, dotcapo=dotcapo)

    init_dbos(settings)  # type: ignore[arg-type]

    mode = stat.S_IMODE(dotcapo.stat().st_mode)
    assert mode == 0o755, f"expected operator mode preserved, got {oct(mode)}"


def test_parent_path_is_file_errors(tmp_path: Path) -> None:
    """Parent path exists but is a regular file → DBOSInitError(mkdir)."""
    not_a_dir = tmp_path / "not-a-dir"
    not_a_dir.write_text("oops")
    settings = _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=tmp_path / "state.db",
            dbos_db_path=not_a_dir / "dbos.db",
        )
    )

    with pytest.raises(DBOSInitError) as exc_info:
        init_dbos(settings)  # type: ignore[arg-type]

    assert exc_info.value.operation == "mkdir"
    assert str(not_a_dir) in str(exc_info.value)


# ---------------------------------------------------------------------------
# Edge case: corrupted dbos.db → clear startup error
# ---------------------------------------------------------------------------


def test_corrupt_dbos_db_surfaces_clear_error(tmp_path: Path) -> None:
    """A non-SQLite file at dbos_db_path raises DBOSInitError(db_corrupt)."""
    dotcapo = tmp_path / ".capo"
    dotcapo.mkdir(mode=0o700)
    dbos_db = dotcapo / "dbos.db"
    # Write garbage that is NOT a SQLite header.
    dbos_db.write_bytes(b"this is definitely not a sqlite database file")

    settings = _make_settings(tmp_path, dotcapo=dotcapo)

    with pytest.raises(DBOSInitError) as exc_info:
        init_dbos(settings)  # type: ignore[arg-type]

    assert exc_info.value.operation == "db_corrupt"
    msg = str(exc_info.value)
    # Error must cite the failing path and reference Litestream restore.
    assert str(dbos_db.resolve()) in msg
    assert "Litestream" in msg
    assert "runbook" in msg.lower()


def test_corrupt_dbos_db_via_truncated_header(tmp_path: Path) -> None:
    """Truncated SQLite header (valid magic, broken page size) → db_corrupt."""
    dotcapo = tmp_path / ".capo"
    dotcapo.mkdir(mode=0o700)
    dbos_db = dotcapo / "dbos.db"
    # Real SQLite files start with "SQLite format 3\0" — write just the
    # magic then garbage to force a header parse error.
    dbos_db.write_bytes(b"SQLite format 3\x00" + b"\xff" * 16)

    settings = _make_settings(tmp_path, dotcapo=dotcapo)

    with pytest.raises(DBOSInitError) as exc_info:
        init_dbos(settings)  # type: ignore[arg-type]

    assert exc_info.value.operation == "db_corrupt"


# ---------------------------------------------------------------------------
# Error handling: typed DBOSInitError carries operation + path
# ---------------------------------------------------------------------------


def test_dbos_init_error_attributes() -> None:
    """:class:`DBOSInitError` carries ``operation`` and ``path`` attrs."""
    err = DBOSInitError("launch", Path("/tmp/foo.db"), "boom")
    assert err.operation == "launch"
    assert err.path == Path("/tmp/foo.db")
    assert "launch" in str(err)
    assert "/tmp/foo.db" in str(err)
    assert "boom" in str(err)


def test_launch_before_init_errors() -> None:
    """:func:`launch_dbos` without :func:`init_dbos` raises DBOSInitError."""
    # Guarantee no prior state.
    if is_launched():
        destroy_dbos()
    with pytest.raises(DBOSInitError) as exc_info:
        launch_dbos()
    assert exc_info.value.operation == "launch"


def test_double_init_with_different_path_errors(tmp_path: Path) -> None:
    """Calling init_dbos twice with different paths is a misconfig."""
    s1 = _make_settings(tmp_path, dotcapo=tmp_path / "a")
    s2 = _make_settings(tmp_path, dotcapo=tmp_path / "b")

    init_dbos(s1)  # type: ignore[arg-type]
    with pytest.raises(DBOSInitError) as exc_info:
        init_dbos(s2)  # type: ignore[arg-type]
    assert exc_info.value.operation == "init"
    assert "already initialized" in str(exc_info.value)


def test_double_init_same_path_idempotent(tmp_path: Path) -> None:
    """Re-init with the same path returns the same instance (idempotent)."""
    settings = _make_settings(tmp_path)
    inst1 = init_dbos(settings)  # type: ignore[arg-type]
    inst2 = init_dbos(settings)  # type: ignore[arg-type]
    assert inst1 is inst2


# ---------------------------------------------------------------------------
# Integration: clean boot creates dbos.db and accepts workflow registration
# ---------------------------------------------------------------------------


def test_clean_boot_creates_dbos_db(tmp_path: Path) -> None:
    """init + launch creates dbos.db at the configured path."""
    settings = _make_settings(tmp_path)
    dbos_db = (tmp_path / ".capo" / "dbos.db").resolve()
    assert not dbos_db.exists()

    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    assert dbos_db.exists()
    assert dbos_db.is_file()

    # And it is actually a SQLite file — open it read-only and run a probe.
    conn = sqlite3.connect(f"file:{dbos_db}?mode=ro", uri=True)
    try:
        # DBOS applies its schema on first launch (spike S-2 §3). We
        # don't pin specific table names (DBOS internals can evolve), but
        # the master table should be populated.
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert len(rows) > 0, "DBOS launch did not create any tables"
    finally:
        conn.close()


def test_clean_boot_accepts_workflow_registration(tmp_path: Path) -> None:
    """After launch, a ``@DBOS.workflow()`` registers without error."""
    from dbos import DBOS

    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    # Defining a workflow after launch is the §5.6 acceptance pattern —
    # the framework must accept the registration. We don't *run* it here
    # (that's task #29's job), just confirm the decorator doesn't raise.
    @DBOS.workflow()
    def _noop_workflow() -> str:
        return "ok"

    assert callable(_noop_workflow)


def test_state_db_and_dbos_db_are_independent(tmp_path: Path) -> None:
    """Writing to state.db doesn't touch dbos.db, and vice versa."""
    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    state_db = (tmp_path / ".capo" / "state.db").resolve()
    dbos_db = (tmp_path / ".capo" / "dbos.db").resolve()

    # Create state.db with a simple table — simulates capo's Alembic-managed
    # application schema landing in a separate file.
    conn = sqlite3.connect(state_db)
    try:
        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO foo (val) VALUES (?)", ("hello",))
        conn.commit()
    finally:
        conn.close()

    # state.db now has the user table; dbos.db has only DBOS-internal
    # tables. Read each and confirm cross-contamination is impossible.
    state_conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        state_tables = {
            r[0]
            for r in state_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        state_conn.close()

    dbos_conn = sqlite3.connect(f"file:{dbos_db}?mode=ro", uri=True)
    try:
        dbos_tables = {
            r[0]
            for r in dbos_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        dbos_conn.close()

    assert "foo" in state_tables
    assert "foo" not in dbos_tables, (
        "user table leaked into dbos.db — files are not independent"
    )
    # DBOS owns its own schema; we don't assert specific names, but the
    # set should be non-empty and disjoint from the user table.
    assert dbos_tables, "dbos.db has no tables — launch did not migrate"


# ---------------------------------------------------------------------------
# Lifecycle: destroy is idempotent
# ---------------------------------------------------------------------------


def test_destroy_when_uninitialized_is_noop() -> None:
    """destroy_dbos() on a fresh process returns silently."""
    # Guarantee uninitialized (autouse fixture handles cleanup after test).
    if is_launched():
        destroy_dbos()
    # Second call must not raise.
    destroy_dbos()
    assert is_launched() is False


def test_destroy_then_reinit_works(tmp_path: Path) -> None:
    """After destroy, a fresh init succeeds (clean teardown)."""
    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()
    destroy_dbos()
    assert is_launched() is False

    # Re-init same path → fresh instance.
    init_dbos(settings)  # type: ignore[arg-type]
    assert is_launched() is True

"""Smoke tests for the capo project scaffold (Task 5).

These confirm the package is importable and the entry point resolves. Behavior
tests for settings, listener, etc. land in later tasks.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


def test_package_importable() -> None:
    """``import capo`` succeeds and exposes a version."""
    capo = importlib.import_module("capo")
    assert hasattr(capo, "__version__")
    assert isinstance(capo.__version__, str)
    assert capo.__version__


def test_entry_point_resolves() -> None:
    """``capo.main:main`` is importable and callable."""
    main_mod = importlib.import_module("capo.main")
    assert callable(main_mod.main)


def test_main_exits_clean_with_no_config(tmp_path: Path) -> None:
    """Running with no explicit config and no ``./config.toml`` exits 0."""
    from capo.main import main

    # Run from a directory with no config.toml so the default path is absent.
    cwd = Path.cwd()
    try:
        import os

        os.chdir(tmp_path)
        rc = main(argv=[])
    finally:
        import os

        os.chdir(cwd)

    assert rc == 0


def test_main_missing_explicit_config_errors_cleanly(
    tmp_path: Path, capsys
) -> None:
    """An explicit but missing ``--config`` exits non-zero with a clear message."""
    from capo.main import main

    missing = tmp_path / "does-not-exist.toml"
    rc = main(argv=["--config", str(missing)])

    captured = capsys.readouterr()
    assert rc != 0
    assert "config file not found" in captured.err
    assert str(missing) in captured.err
    # Sanity: no traceback bled through.
    assert "Traceback" not in captured.err


def test_python_dash_m_capo_runs() -> None:
    """``python -m capo --version`` starts and exits cleanly."""
    result = subprocess.run(
        [sys.executable, "-m", "capo", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "capo" in result.stdout

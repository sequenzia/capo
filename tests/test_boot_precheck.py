"""Tests for :mod:`capo.boot` boot-time binary pre-checks (Task #62).

Covers acceptance criteria from spec §5.4, §7.5, §12.1:

* ``precheck_binaries(settings)`` runs ``claude --version`` and
  ``codex --version`` via subprocess with a 5 s timeout each.
* Raises typed :class:`BinaryPrecheckError` when a binary is missing,
  hangs, or reports a version below the minimum.
* Error message includes the resolved binary path, observed version
  (or ``"not found"``), required minimum, and a remediation hint.
* ``settings.boot.skip_binary_precheck=true`` short-circuits the check.
* Boot wiring: ``capo.main.amain`` calls precheck AFTER config load
  but BEFORE ``init_dbos``. On precheck failure: single-line stderr +
  exit 2, no DBOS state created.

Unit tests use disk-resident shell stubs (write a tiny script, chmod
+x) so we exercise the real :func:`subprocess.run` path with full
fidelity (timeout, non-zero exit, unparseable output).
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from capo.boot import (
    BinaryPrecheckError,
    _parse_semver,
    _precheck_one,
    precheck_binaries,
    run_precheck_or_exit,
)

# ---------------------------------------------------------------------------
# Stub binaries (disk-resident, executable).
# ---------------------------------------------------------------------------


def _write_stub(
    path: Path, body: str, *, executable: bool = True
) -> Path:
    """Write a Python shebang script at ``path`` and chmod it 0o755."""
    path.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(body))
    if executable:
        path.chmod(0o755)
    return path


_VERSION_OK = """
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        print("{name} {version}")
        sys.exit(0)
    sys.exit(1)
"""

_VERSION_BELOW_MIN = """
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        print("{name} {version}")
        sys.exit(0)
    sys.exit(1)
"""

_VERSION_UNPARSEABLE = """
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        print("not a version string at all")
        sys.exit(0)
    sys.exit(1)
"""

_VERSION_HANG = """
    import sys, time
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        # Sleep well past the 5s precheck timeout so we hit
        # TimeoutExpired deterministically.
        time.sleep(30)
    sys.exit(0)
"""

_VERSION_NONZERO = """
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        sys.stderr.write("boom\\n")
        sys.exit(7)
    sys.exit(7)
"""


# ---------------------------------------------------------------------------
# Settings stub.
# ---------------------------------------------------------------------------


class _AgentBin:
    def __init__(self, binary: str) -> None:
        self.binary = binary


class _Agents:
    def __init__(self, claude: str, codex: str) -> None:
        self.claude_code = _AgentBin(claude)
        self.codex = _AgentBin(codex)


class _BootSection:
    def __init__(self, skip: bool = False) -> None:
        self.skip_binary_precheck = skip


class _Settings:
    """Tiny stand-in carrying only the fields :mod:`capo.boot` reads."""

    def __init__(
        self,
        *,
        claude: str = "claude",
        codex: str = "codex",
        skip: bool = False,
    ) -> None:
        self.agents = _Agents(claude, codex)
        self.boot = _BootSection(skip=skip)


# ===========================================================================
# Unit: SemVer parser.
# ===========================================================================


def test_parse_semver_basic() -> None:
    assert _parse_semver("2.1.138") == (2, 1, 138)


def test_parse_semver_tolerates_prefix() -> None:
    assert _parse_semver("Claude Code 2.1.138") == (2, 1, 138)
    assert _parse_semver("codex-cli/0.130.0") == (0, 130, 0)
    assert _parse_semver("claude 2.1.138 (abc123)") == (2, 1, 138)


def test_parse_semver_returns_none_on_garbage() -> None:
    assert _parse_semver("not a version") is None
    assert _parse_semver("") is None


# ===========================================================================
# Unit: _precheck_one — per-binary check.
# ===========================================================================


def test_precheck_one_passes_when_min_met(tmp_path: Path) -> None:
    """Exact min version is accepted."""
    stub = _write_stub(
        tmp_path / "claude_ok",
        _VERSION_OK.format(name="Claude Code", version="2.1.138"),
    )
    observed = _precheck_one("claude", str(stub), "2.1.138")
    assert "2.1.138" in observed


def test_precheck_one_passes_when_above_min(tmp_path: Path) -> None:
    """Newer version is accepted."""
    stub = _write_stub(
        tmp_path / "claude_newer",
        _VERSION_OK.format(name="Claude Code", version="2.5.0"),
    )
    observed = _precheck_one("claude", str(stub), "2.1.138")
    assert "2.5.0" in observed


def test_precheck_one_rejects_below_min_claude(tmp_path: Path) -> None:
    """Below-min raises with min-version diagnostics."""
    stub = _write_stub(
        tmp_path / "claude_old",
        _VERSION_BELOW_MIN.format(name="Claude Code", version="2.0.0"),
    )
    with pytest.raises(BinaryPrecheckError) as exc:
        _precheck_one("claude", str(stub), "2.1.138")
    assert "below required minimum" in str(exc.value)
    assert "2.1.138" in str(exc.value)
    assert exc.value.binary == "claude"
    assert exc.value.required_version == "2.1.138"
    assert "2.0.0" in exc.value.observed_version
    assert exc.value.binary_path == str(stub)


def test_precheck_one_rejects_below_min_codex(tmp_path: Path) -> None:
    """Codex below-min raises with min-version diagnostics."""
    stub = _write_stub(
        tmp_path / "codex_old",
        _VERSION_BELOW_MIN.format(name="codex-cli", version="0.129.0"),
    )
    with pytest.raises(BinaryPrecheckError) as exc:
        _precheck_one("codex", str(stub), "0.130.0")
    assert "below required minimum" in str(exc.value)
    assert "0.130.0" in str(exc.value)
    assert exc.value.binary == "codex"
    assert "0.129.0" in exc.value.observed_version


def test_precheck_one_missing_binary_raises() -> None:
    """Missing binary surfaces 'not found' observed_version."""
    with pytest.raises(BinaryPrecheckError) as exc:
        _precheck_one("claude", "/no/such/path-xyz", "2.1.138")
    assert "not found" in str(exc.value).lower()
    assert exc.value.observed_version == "not found"
    assert exc.value.required_version == "2.1.138"


def test_precheck_one_unparseable_logs_and_raises(
    tmp_path: Path,
) -> None:
    """Unparseable output → warn log + treated as below-min.

    The ``capo`` logger is configured with ``propagate=False`` in
    ``capo.main._configure_logging`` (so pytest's caplog can't see it).
    Attach a local handler directly to the ``capo.boot`` logger to
    capture the warning for assertion.
    """
    import logging

    stub = _write_stub(tmp_path / "weird", _VERSION_UNPARSEABLE)
    captured: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = captured.append  # type: ignore[assignment]
    handler.setLevel(logging.WARNING)
    boot_logger = logging.getLogger("capo.boot")
    boot_logger.addHandler(handler)
    boot_logger.setLevel(logging.WARNING)
    try:
        with pytest.raises(BinaryPrecheckError) as exc:
            _precheck_one("claude", str(stub), "2.1.138")
    finally:
        boot_logger.removeHandler(handler)
    assert "unparseable" in str(exc.value).lower()
    assert any(
        "could not parse" in r.getMessage() for r in captured
    )


def test_precheck_one_timeout_raises(tmp_path: Path) -> None:
    """A hanging --version is killed and surfaces a timeout error."""
    stub = _write_stub(tmp_path / "hang", _VERSION_HANG)
    # Patch the module-level timeout to keep the test fast.
    with patch("capo.boot._VERSION_CHECK_TIMEOUT_S", 0.3), pytest.raises(
        BinaryPrecheckError
    ) as exc:
        _precheck_one("claude", str(stub), "2.1.138")
    assert "timed out" in str(exc.value).lower()
    assert exc.value.observed_version == "<timeout>"


def test_precheck_one_nonzero_exit_raises(tmp_path: Path) -> None:
    """A non-zero exit from --version is treated as a precheck failure."""
    stub = _write_stub(tmp_path / "boom", _VERSION_NONZERO)
    with pytest.raises(BinaryPrecheckError) as exc:
        _precheck_one("claude", str(stub), "2.1.138")
    assert "exited 7" in str(exc.value)


def test_precheck_one_error_message_contains_remediation(
    tmp_path: Path,
) -> None:
    """Error message must include an install hint per acceptance criteria."""
    with pytest.raises(BinaryPrecheckError) as exc:
        _precheck_one("claude", "/no/such/cli-xyz", "2.1.138")
    msg = str(exc.value)
    assert "Install" in msg
    assert "claude" in msg.lower()

    with pytest.raises(BinaryPrecheckError) as exc2:
        _precheck_one("codex", "/no/such/cli-xyz", "0.130.0")
    msg2 = str(exc2.value)
    assert "Install" in msg2
    assert "codex" in msg2.lower()


# ===========================================================================
# Unit: precheck_binaries — orchestrates both checks.
# ===========================================================================


def test_precheck_binaries_both_ok(tmp_path: Path) -> None:
    """Both binaries at min → no exception."""
    claude = _write_stub(
        tmp_path / "claude_ok",
        _VERSION_OK.format(name="Claude Code", version="2.1.138"),
    )
    codex = _write_stub(
        tmp_path / "codex_ok",
        _VERSION_OK.format(name="codex-cli", version="0.130.0"),
    )
    settings = _Settings(claude=str(claude), codex=str(codex))
    # Should return None (no raise).
    assert precheck_binaries(settings) is None


def test_precheck_binaries_claude_below_min(tmp_path: Path) -> None:
    """A bad claude is reported with binary='claude'."""
    claude = _write_stub(
        tmp_path / "claude_old",
        _VERSION_OK.format(name="Claude Code", version="2.0.0"),
    )
    codex = _write_stub(
        tmp_path / "codex_ok",
        _VERSION_OK.format(name="codex-cli", version="0.130.0"),
    )
    settings = _Settings(claude=str(claude), codex=str(codex))
    with pytest.raises(BinaryPrecheckError) as exc:
        precheck_binaries(settings)
    assert exc.value.binary == "claude"


def test_precheck_binaries_codex_below_min(tmp_path: Path) -> None:
    """A bad codex is reported with binary='codex' (claude still checked first)."""
    claude = _write_stub(
        tmp_path / "claude_ok",
        _VERSION_OK.format(name="Claude Code", version="2.1.138"),
    )
    codex = _write_stub(
        tmp_path / "codex_old",
        _VERSION_OK.format(name="codex-cli", version="0.129.0"),
    )
    settings = _Settings(claude=str(claude), codex=str(codex))
    with pytest.raises(BinaryPrecheckError) as exc:
        precheck_binaries(settings)
    assert exc.value.binary == "codex"
    assert exc.value.required_version == "0.130.0"


def test_precheck_binaries_missing_claude() -> None:
    """Missing claude raises with binary='claude'."""
    settings = _Settings(
        claude="/no/such/claude-xyz",
        codex="/no/such/codex-xyz",
    )
    with pytest.raises(BinaryPrecheckError) as exc:
        precheck_binaries(settings)
    # claude is checked first.
    assert exc.value.binary == "claude"
    assert exc.value.observed_version == "not found"


def test_precheck_binaries_skip_flag_short_circuits() -> None:
    """skip_binary_precheck=True → no subprocess, no exception, log line.

    Local handler capture (see note on
    :func:`test_precheck_one_unparseable_logs_and_raises`).
    """
    import logging

    # Configure binaries that WOULD fail if we actually ran the check.
    settings = _Settings(
        claude="/no/such/claude-xyz",
        codex="/no/such/codex-xyz",
        skip=True,
    )
    captured: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = captured.append  # type: ignore[assignment]
    handler.setLevel(logging.INFO)
    boot_logger = logging.getLogger("capo.boot")
    boot_logger.addHandler(handler)
    boot_logger.setLevel(logging.INFO)
    try:
        # If we tried to run subprocess.run, the missing-binary path
        # would raise BinaryPrecheckError. Skip must bypass that
        # entirely.
        assert precheck_binaries(settings) is None
    finally:
        boot_logger.removeHandler(handler)
    assert any(
        r.getMessage().startswith("capo.boot: binary precheck SKIPPED")
        for r in captured
    )


def test_precheck_binaries_uses_configured_binary_name(
    tmp_path: Path,
) -> None:
    """The precheck honors settings.agents.{claude_code,codex}.binary.

    NOT 'claude' / 'codex' literals — operator config drives the
    subprocess exec.
    """
    # Build a stub binary with a custom name to prove the configured
    # value is what gets exec'd.
    custom_claude = _write_stub(
        tmp_path / "my-custom-claude",
        _VERSION_OK.format(name="Claude Code", version="2.1.138"),
    )
    custom_codex = _write_stub(
        tmp_path / "my-custom-codex",
        _VERSION_OK.format(name="codex-cli", version="0.130.0"),
    )
    settings = _Settings(
        claude=str(custom_claude), codex=str(custom_codex)
    )
    # Real subprocess.run against the custom binaries succeeds.
    assert precheck_binaries(settings) is None


# ===========================================================================
# Unit: run_precheck_or_exit — boot-path adapter.
# ===========================================================================


def test_run_precheck_or_exit_success_returns_none(tmp_path: Path) -> None:
    """Happy path: returns None, no stderr."""
    claude = _write_stub(
        tmp_path / "c",
        _VERSION_OK.format(name="Claude Code", version="2.1.138"),
    )
    codex = _write_stub(
        tmp_path / "cx",
        _VERSION_OK.format(name="codex-cli", version="0.130.0"),
    )
    settings = _Settings(claude=str(claude), codex=str(codex))
    assert run_precheck_or_exit(settings) is None


def test_run_precheck_or_exit_failure_returns_2_and_writes_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Failure path: stderr line + return 2."""
    settings = _Settings(
        claude="/no/such/claude-xyz",
        codex="/no/such/codex-xyz",
    )
    rc = run_precheck_or_exit(settings)
    captured = capsys.readouterr()
    assert rc == 2
    assert captured.err.startswith("capo:")
    assert "claude" in captured.err.lower()


# ===========================================================================
# Integration: real claude/codex binaries (when available).
# ===========================================================================


def _has_binary(name: str) -> bool:
    import shutil

    return shutil.which(name) is not None


@pytest.mark.skipif(
    not (_has_binary("claude") and _has_binary("codex")),
    reason="real claude+codex CLIs not installed",
)
def test_precheck_binaries_real_binaries_ok() -> None:
    """If the env has both binaries, precheck must pass at their actual versions."""
    settings = _Settings(claude="claude", codex="codex")
    # Real claude/codex present at ≥ pinned minimums per Phase 1+4 setup.
    assert precheck_binaries(settings) is None


# ===========================================================================
# Boot wiring: capo.main.amain calls precheck before init_dbos.
# ===========================================================================


@pytest.mark.asyncio
async def test_amain_precheck_failure_exits_2_before_dbos(
    tmp_path: Path,
) -> None:
    """When precheck fails, amain returns 2 WITHOUT init_dbos being called."""
    import capo.main as main_mod

    # Stub Settings with a known-bad claude binary and skip=False.
    # The precheck path is the ONLY thing this test exercises — we
    # patch out every other amain dependency with side_effect that
    # would fail the test if called.

    class _Paths:
        db_path = tmp_path / "state.db"
        dbos_db_path = tmp_path / "dbos.db"
        workspaces_root = tmp_path / "ws"
        projects_root = tmp_path / "proj"

    class _Amc:
        listen_host = "127.0.0.1"
        listen_port = 8090
        base_url = "http://x"
        agent_id = "capo"
        max_boot_wait_seconds = 1

    class _Obs:
        logfire_enabled = False
        token = None
        service_name = "capo"
        environment = "test"

    class _S:
        paths = _Paths()
        amc = _Amc()
        observability = _Obs()
        agents = _Agents("/no/such/claude-xyz", "/no/such/codex-xyz")
        boot = _BootSection(skip=False)
        amc_bearer_token = None
        anthropic_api_key = None
        openai_api_key = None

    settings = _S()

    # If init_dbos is called, fail the test.
    init_called = {"count": 0}

    def _init_dbos_should_not_run(*a: Any, **kw: Any) -> None:
        init_called["count"] += 1
        raise AssertionError(
            "init_dbos must NOT be called when precheck fails"
        )

    # Patch configure_logfire to a no-op (avoids real Logfire SDK).
    with patch(
        "capo.observability.configure_logfire", return_value=None
    ), patch(
        "capo.workflows.init_dbos",
        side_effect=_init_dbos_should_not_run,
    ):
        rc = await main_mod.amain(
            settings,  # type: ignore[arg-type]
            system_prompt_path=tmp_path / "system.md",
            agent=object(),
        )

    assert rc == 2
    assert init_called["count"] == 0


def test_amain_invokes_run_precheck_or_exit_after_settings(
    tmp_path: Path,
) -> None:
    """Static wiring check: ``capo.main.amain`` source references run_precheck_or_exit.

    The precheck wiring is a load-bearing structural check: amain must
    call ``run_precheck_or_exit(settings)`` AFTER Settings is in hand and
    BEFORE ``init_dbos``. We verify by reading the source — runtime
    integration tests would require mocking the full AMCClient /
    dispatcher / DBOS / uvicorn stack, which is brittle. The runtime
    skip path is exercised by
    :func:`test_precheck_binaries_skip_flag_short_circuits` at the
    function level; the failure path is exercised by
    :func:`test_run_precheck_or_exit_failure_returns_2_and_writes_stderr`.
    """
    import inspect

    import capo.main as main_mod

    src = inspect.getsource(main_mod.amain)
    assert "run_precheck_or_exit" in src, (
        "amain must call run_precheck_or_exit (Task #62)"
    )
    # Confirm precheck precedes init_dbos in the source order. This
    # encodes the "AFTER config load, BEFORE init_dbos" criterion.
    precheck_idx = src.find("run_precheck_or_exit")
    init_dbos_idx = src.find("init_dbos(")
    assert precheck_idx != -1
    assert init_dbos_idx != -1
    assert precheck_idx < init_dbos_idx, (
        "run_precheck_or_exit must appear BEFORE init_dbos() in amain"
    )

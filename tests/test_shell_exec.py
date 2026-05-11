"""Unit tests for Task #25 — :func:`capo.tools.basic.shell_exec`.

Covers acceptance criteria from spec §5.8, §6.2, §9.2:

Functional
* Default allowlist binaries execute (we sample ``pwd``, ``ls``, ``which``).
* Allowlisted commands run with ``shell=False`` and return a structured
  :class:`ShellResult` with ``exit_code=0`` on success.
* CWD is scoped to ``projects_root`` ∪ ``workspaces_root``; ``None``
  defaults to ``projects_root``.
* stdout/stderr are capped at :data:`SHELL_EXEC_OUTPUT_MAX_BYTES`.

Reject patterns (all raise :class:`ApprovalRequired`)
* Shell metacharacters: ``;``, ``&&``, ``||``, ``|``, backticks, ``$(``,
  ``>``, ``<``.
* ``sudo`` (even with allowlisted binary after it).
* Non-allowlisted binary.
* Quoted-metachar escape attempts (e.g. ``git log '; rm -rf /'``).
* cwd outside ``projects_root`` ∪ ``workspaces_root``.

Edge cases
* Empty / whitespace command → :class:`ValueError`.
* Timeout: long-running allowlisted command exceeding the timeout
  returns a :class:`ShellResult` with ``exit_code=-1`` and a clear
  stderr marker.

Error attributes
* :class:`ApprovalRequired` carries the original command + a one-line
  reason.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from capo.config import Settings
from capo.deps import CapoDeps
from capo.tools import basic as tools_basic
from capo.tools.basic import (
    SHELL_EXEC_OUTPUT_MAX_BYTES,
    ApprovalRequired,
    ShellResult,
    shell_exec,
)

# ---------------------------------------------------------------------------
# Scaffolding — mirrors tests/test_agent_tools.py to keep config shape
# consistent across the Phase 2 test suite.
# ---------------------------------------------------------------------------

_VALID_TOML_TMPL = """\
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "{workspaces_root}"
projects_root   = "{projects_root}"
db_path         = "{db_path}"
dbos_db_path    = "{dbos_db_path}"

[soul]
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8080"
agent_id              = "capo"
listen_host           = "127.0.0.1"
listen_port           = 8090
max_boot_wait_seconds = 60

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[retention]
delegation_output_days = 7

[compaction]
threshold_tokens = 100000
preserve_delegation_handles = true

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

[users.owner]
amc_senders = ["+15551234567"]
display_name = "Owner"

[observability]
logfire_enabled = true
"""

_VALID_ENV = {
    "AMC_WEBHOOK_SECRET": "shhh-webhook",
    "AMC_BEARER_TOKEN": "shhh-bearer",
}


@pytest.fixture
def scaffold(tmp_path: Path) -> tuple[Settings, Path, Path]:
    """Return ``(settings, projects_root, workspaces_root)`` for a real
    on-disk scaffold rooted under ``tmp_path``.

    Both roots exist so cwd resolution returns a real absolute path; the
    config also sets ``shell.allowlist`` to the spec default so we exercise
    the production allowlist.
    """
    projects_root = tmp_path / "projects"
    workspaces_root = tmp_path / "workspaces"
    projects_root.mkdir()
    workspaces_root.mkdir()

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        _VALID_TOML_TMPL.format(
            workspaces_root=str(workspaces_root),
            projects_root=str(projects_root),
            db_path=str(tmp_path / "state.db"),
            dbos_db_path=str(tmp_path / "dbos.db"),
        )
    )
    (tmp_path / "souls").mkdir()
    (tmp_path / "souls" / "default.md").write_text("# default soul\n")

    settings = Settings.load(cfg, env_override=dict(_VALID_ENV))
    return settings, projects_root, workspaces_root


@pytest.fixture
def deps(scaffold: tuple[Settings, Path, Path]) -> CapoDeps:
    """A :class:`CapoDeps` wired with the scaffold's roots."""
    settings, _, _ = scaffold
    return CapoDeps.from_settings(
        settings=settings,
        http_client=httpx.AsyncClient(),  # not used by shell_exec
    )


def _ctx(deps: CapoDeps) -> RunContext[CapoDeps]:
    """Minimal :class:`RunContext` for direct shell_exec invocation."""
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


# ---------------------------------------------------------------------------
# Functional — allowlisted binaries execute.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("binary", ["pwd", "ls", "which"])
def test_allowlisted_binary_executes(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path], binary: str
) -> None:
    """Each of pwd / ls / which returns a ShellResult with exit_code 0."""
    _, projects_root, _ = scaffold
    command = {
        "pwd": "pwd",
        "ls": "ls",
        "which": "which ls",
    }[binary]

    result = shell_exec(_ctx(deps), command, cwd=str(projects_root))

    assert isinstance(result, ShellResult)
    assert result.exit_code == 0
    assert result.runtime_seconds >= 0.0


def test_pwd_returns_resolved_cwd(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path]
) -> None:
    """``pwd`` echoes the resolved cwd we passed in."""
    _, projects_root, _ = scaffold
    result = shell_exec(_ctx(deps), "pwd", cwd=str(projects_root))
    assert result.exit_code == 0
    assert result.stdout.strip() == str(projects_root.resolve())


def test_cwd_none_defaults_to_projects_root(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path]
) -> None:
    """``cwd=None`` defaults to projects_root per task spec."""
    _, projects_root, _ = scaffold
    result = shell_exec(_ctx(deps), "pwd")
    assert result.exit_code == 0
    assert result.stdout.strip() == str(projects_root.resolve())


def test_cwd_inside_workspaces_root_allowed(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path]
) -> None:
    """A subdirectory of workspaces_root is an allowed cwd."""
    _, _, workspaces_root = scaffold
    sub = workspaces_root / "sub"
    sub.mkdir()
    result = shell_exec(_ctx(deps), "pwd", cwd=str(sub))
    assert result.exit_code == 0
    assert result.stdout.strip() == str(sub.resolve())


# ---------------------------------------------------------------------------
# Reject patterns — every metachar + sudo + non-allowlist.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        "ls ; rm -rf /",
        "ls && rm -rf /",
        "ls || rm -rf /",
        "ls | cat",
        "echo `whoami`",
        "echo $(whoami)",
        "ls > /tmp/out",
        "cat < /etc/passwd",
    ],
)
def test_metacharacters_rejected(deps: CapoDeps, command: str) -> None:
    """All shell metacharacters trip ApprovalRequired."""
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), command)
    # The exception carries the original command + a one-line reason.
    assert exc_info.value.command == command.strip()
    assert exc_info.value.reason
    assert "metacharacter" in exc_info.value.reason or "sudo" in exc_info.value.reason


def test_sudo_rejected_as_first_token(deps: CapoDeps) -> None:
    """``sudo ls`` is rejected with a sudo-specific reason."""
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "sudo ls")
    assert "sudo" in exc_info.value.reason


def test_sudo_rejected_when_embedded(deps: CapoDeps) -> None:
    """``ls sudo`` (sudo as a non-first arg) is also rejected."""
    # Note: ``ls`` is allowlisted; the sudo guard fires before allowlist.
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "ls sudo")
    assert "sudo" in exc_info.value.reason


def test_non_allowlisted_binary_rejected(deps: CapoDeps) -> None:
    """A binary outside the configured allowlist raises ApprovalRequired."""
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "rm -rf /tmp/foo")
    assert "rm" in exc_info.value.reason
    assert "allowlist" in exc_info.value.reason
    assert exc_info.value.command == "rm -rf /tmp/foo"


def test_quoted_metachar_injection_rejected(deps: CapoDeps) -> None:
    """The classic ``git log; rm -rf /`` injection is rejected.

    This is the security regression test called out in the task's testing
    requirements — we want to confirm the raw-string scan catches it even
    though shlex would otherwise turn it into clean tokens.
    """
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "git log; rm -rf /")
    assert "metacharacter" in exc_info.value.reason
    # Original command surfaced verbatim for the approval UI.
    assert exc_info.value.command == "git log; rm -rf /"


def test_quoted_metachar_inside_arg_rejected(deps: CapoDeps) -> None:
    """``git log '; rm -rf /'`` — quoted metachar — is also rejected."""
    with pytest.raises(ApprovalRequired):
        shell_exec(_ctx(deps), "git log '; rm -rf /'")


# ---------------------------------------------------------------------------
# cwd scoping.
# ---------------------------------------------------------------------------


def test_cwd_outside_roots_rejected(
    deps: CapoDeps, tmp_path: Path
) -> None:
    """A cwd outside both roots raises ApprovalRequired with a clear reason.

    Per task spec we treat path-scope violations the same as other
    non-approvable commands so callers have one exception type to handle.
    """
    outside = tmp_path / "outside-the-roots"
    outside.mkdir()
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "pwd", cwd=str(outside))
    assert "cwd" in exc_info.value.reason
    assert "outside" in exc_info.value.reason


# ---------------------------------------------------------------------------
# Empty command.
# ---------------------------------------------------------------------------


def test_empty_command_rejected(deps: CapoDeps) -> None:
    """Empty command string raises ValueError (NOT ApprovalRequired)."""
    with pytest.raises(ValueError, match="non-empty"):
        shell_exec(_ctx(deps), "")


def test_whitespace_command_rejected(deps: CapoDeps) -> None:
    """Whitespace-only command also raises ValueError."""
    with pytest.raises(ValueError, match="non-empty"):
        shell_exec(_ctx(deps), "   \t  ")


# ---------------------------------------------------------------------------
# Output capping.
# ---------------------------------------------------------------------------


def test_stdout_truncated_to_cap(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path]
) -> None:
    """Large stdout is truncated to SHELL_EXEC_OUTPUT_MAX_BYTES with marker."""
    _, projects_root, _ = scaffold
    # Create a file ~200 KiB; ``cat`` it; assert truncation marker.
    big = projects_root / "big.txt"
    big.write_bytes(b"x" * (200 * 1024))
    result = shell_exec(
        _ctx(deps), f"cat {big.name}", cwd=str(projects_root)
    )
    assert result.exit_code == 0
    assert "[... output truncated at" in result.stdout
    # The visible body before the marker should be <= cap + a tiny marker
    # delimiter (we append a single ``\n`` between body and marker). The
    # important invariant is that we didn't return the entire 200 KiB.
    body, _, _marker = result.stdout.partition("[... output truncated at")
    assert len(body.encode("utf-8")) <= SHELL_EXEC_OUTPUT_MAX_BYTES + 2
    # And the full string is far smaller than the raw subprocess output.
    assert len(result.stdout.encode("utf-8")) < 200 * 1024


# ---------------------------------------------------------------------------
# Timeout.
# ---------------------------------------------------------------------------


def test_timeout_returns_exit_code_minus_one(
    deps: CapoDeps, scaffold: tuple[Settings, Path, Path]
) -> None:
    """A command that exceeds the timeout is killed; exit_code is -1.

    We monkeypatch the module-level timeout to 0.2s so this test is fast
    and deterministic. The command is ``find /`` — allowlisted, slow
    enough on any sane system to blow past 200 ms.
    """
    _, projects_root, _ = scaffold

    # Patch the timeout to something very small so the test is fast.
    with patch.object(tools_basic, "SHELL_EXEC_TIMEOUT_S", 0.2):
        result = shell_exec(
            _ctx(deps), "find /", cwd=str(projects_root)
        )

    assert result.exit_code == -1
    assert "timed out" in result.stderr.lower()
    assert result.runtime_seconds >= 0.0


# ---------------------------------------------------------------------------
# ApprovalRequired attribute contract.
# ---------------------------------------------------------------------------


def test_approval_required_carries_command_and_reason(deps: CapoDeps) -> None:
    """ApprovalRequired exposes both ``command`` and ``reason`` attrs."""
    with pytest.raises(ApprovalRequired) as exc_info:
        shell_exec(_ctx(deps), "make build")  # ``make`` not allowlisted

    assert exc_info.value.command == "make build"
    assert "make" in exc_info.value.reason
    # Stringification includes both fields so logs are self-explanatory.
    msg = str(exc_info.value)
    assert "make build" in msg
    assert "approval required" in msg

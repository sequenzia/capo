"""Boot-time pre-flight checks (spec §5.4, §7.5, §12.1).

Task #62 owns the binary version pre-check that runs AFTER Settings load
but BEFORE :func:`capo.workflows.init_dbos`. The contract is intentionally
narrow:

1. Resolve the configured ``claude`` / ``codex`` binary paths.
2. Spawn each with ``--version`` (5 s timeout).
3. Parse the first ``X.Y.Z`` semver triple out of stdout/stderr.
4. Compare against :data:`capo.tools.claude_code.MIN_CLAUDE_CODE_VERSION`
   / :data:`capo.tools.codex.MIN_CODEX_VERSION`.
5. Raise a typed :class:`BinaryPrecheckError` on missing / unparseable /
   below-minimum / timeout. The boot path (``capo.main.amain``) catches
   the error, writes a single-line stderr message including the binary
   path, observed version (or ``"not found"``), required minimum, and a
   remediation hint, then exits ``2``.

The check is intentionally *synchronous* (``subprocess.run``) so the
boot path can call it before the asyncio event loop spins. Each binary
gets its own 5 s timeout. ``settings.boot.skip_binary_precheck=true``
short-circuits the entire check and emits an informational log line —
this is the documented escape hatch for smoke runs / CI environments
without the CLI binaries on PATH (spec §12.1 acceptance criterion).

Why not reuse :func:`capo.tools.codex._check_codex_version`?
    The codex helper is used at spawn time and raises
    :class:`capo.tools.codex.CodexBinaryError`. The boot precheck has a
    different error contract (a single typed error covering both
    binaries, distinct exit code path) and runs once per process. Two
    callers can coexist — the spawn-site check stays a defense-in-depth
    guard for crash-restart paths that skip boot entirely.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING

from capo.tools.claude_code import MIN_CLAUDE_CODE_VERSION
from capo.tools.codex import MIN_CODEX_VERSION

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings

logger = logging.getLogger("capo.boot")


#: Hard timeout on each ``<binary> --version`` invocation (seconds). The
#: acceptance criteria pin this at 5 s — long enough for a slow disk /
#: NodeJS cold start, short enough that a hung binary doesn't stall boot.
_VERSION_CHECK_TIMEOUT_S: float = 5.0

#: Permissive SemVer triple matcher. Tolerant of prefixes like
#: ``codex-cli/0.130.0``, ``claude 2.1.138 (build ...)``, or
#: ``Claude Code 2.1.138``. Identical to the matcher in
#: :mod:`capo.tools.codex`; duplicated here so the boot module has no
#: hard dependency on the tools package's private regex.
_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


# ---------------------------------------------------------------------------
# Typed errors.
# ---------------------------------------------------------------------------


class BinaryPrecheckError(RuntimeError):
    """Raised when a boot-time binary pre-check fails.

    Attributes:
        binary: Logical name of the binary being checked
            (``"claude"`` or ``"codex"``).
        observed_version: Parsed version string, or ``"not found"`` when
            the binary is missing entirely, or ``"<unparseable>"`` when
            ``--version`` produced output we could not parse.
        required_version: Minimum required version (e.g. ``"2.1.138"``).
        binary_path: Resolved absolute path to the binary, or the
            configured/basename value when ``shutil.which`` returned None.
    """

    def __init__(
        self,
        message: str,
        *,
        binary: str,
        observed_version: str,
        required_version: str,
        binary_path: str,
    ) -> None:
        super().__init__(message)
        self.binary: str = binary
        self.observed_version: str = observed_version
        self.required_version: str = required_version
        self.binary_path: str = binary_path


# ---------------------------------------------------------------------------
# SemVer helpers.
# ---------------------------------------------------------------------------


def _parse_semver(text: str) -> tuple[int, int, int] | None:
    """Extract the first ``X.Y.Z`` semver triple from ``text``.

    Returns ``None`` when no triple is found — the caller treats this as
    "below min" per the §12.1 edge case: ``"version output unparseable
    (binary upgraded to weird format) → log warning + treat as below
    min"``.
    """
    match = _VERSION_RE.search(text)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _format_remediation(binary: str) -> str:
    """Return an operator-friendly install hint for ``binary``."""
    if binary == "claude":
        return (
            "Install Claude Code CLI >= "
            f"{MIN_CLAUDE_CODE_VERSION} (https://docs.claude.com/en/docs/claude-code) "
            "and ensure 'claude' is on PATH, or set 'agents.claude_code.binary' "
            "in config.toml."
        )
    if binary == "codex":
        return (
            "Install Codex CLI >= "
            f"{MIN_CODEX_VERSION} (https://github.com/openai/codex) "
            "and ensure 'codex' is on PATH, or set 'agents.codex.binary' in "
            "config.toml."
        )
    return f"Install {binary!r} and ensure it is on PATH."


# ---------------------------------------------------------------------------
# Per-binary check.
# ---------------------------------------------------------------------------


def _precheck_one(binary_name: str, configured: str, required: str) -> str:
    """Run ``<configured> --version`` and validate it meets ``required``.

    Returns the observed version string on success. Raises
    :class:`BinaryPrecheckError` on any failure mode covered by the §12.1
    acceptance criteria:

    * Binary missing entirely (``shutil.which`` returns None).
    * ``--version`` exits non-zero / OS error / timeout.
    * Output unparseable (no SemVer triple matched).
    * Parsed version below ``required``.

    The function is synchronous; callers wrap in :func:`asyncio.to_thread`
    if they need to release the event loop (not needed at boot — boot
    runs before the loop is constructed).
    """
    resolved = shutil.which(configured)
    if resolved is None:
        raise BinaryPrecheckError(
            (
                f"{binary_name} binary not found on PATH: {configured!r}. "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version="not found",
            required_version=required,
            binary_path=configured,
        )

    try:
        result = subprocess.run(  # noqa: S603 - argv list, shell=False
            [resolved, "--version"],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=_VERSION_CHECK_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        # §12.1 edge case: --version timeout (binary hangs) → kill +
        # treat as failure. subprocess.run already kills the child on
        # timeout; we just surface the typed error.
        raise BinaryPrecheckError(
            (
                f"{binary_name} --version timed out after "
                f"{_VERSION_CHECK_TIMEOUT_S:.1f}s (binary={resolved!r}). "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version="<timeout>",
            required_version=required,
            binary_path=resolved,
        ) from exc
    except OSError as exc:
        raise BinaryPrecheckError(
            (
                f"{binary_name} --version failed for {resolved!r}: {exc!r}. "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version="<error>",
            required_version=required,
            binary_path=resolved,
        ) from exc

    if result.returncode != 0:
        raise BinaryPrecheckError(
            (
                f"{binary_name} --version exited {result.returncode} for "
                f"{resolved!r}: stderr={result.stderr.strip()!r}. "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version="<error>",
            required_version=required,
            binary_path=resolved,
        )

    observed = (result.stdout or result.stderr or "").strip()
    parsed = _parse_semver(observed)
    required_parsed = _parse_semver(required)
    if parsed is None or required_parsed is None:
        # §12.1: unparseable → log warning + treat as below min.
        logger.warning(
            "capo.boot: could not parse %s --version output: %r — "
            "treating as below minimum %s",
            binary_name,
            observed,
            required,
            extra={
                "event": "capo.boot.precheck.unparseable",
                "binary": binary_name,
                "binary_path": resolved,
                "observed": observed,
                "required": required,
            },
        )
        raise BinaryPrecheckError(
            (
                f"{binary_name} --version output unparseable "
                f"(observed={observed!r}, binary={resolved!r}); "
                f"required >= {required}. "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version=observed or "<unparseable>",
            required_version=required,
            binary_path=resolved,
        )

    if parsed < required_parsed:
        raise BinaryPrecheckError(
            (
                f"{binary_name} {observed!r} is below required minimum "
                f"{required!r} (binary={resolved!r}). "
                + _format_remediation(binary_name)
            ),
            binary=binary_name,
            observed_version=observed,
            required_version=required,
            binary_path=resolved,
        )

    logger.info(
        "capo.boot: %s pre-check OK (observed=%s, required>=%s, path=%s)",
        binary_name,
        observed,
        required,
        resolved,
        extra={
            "event": "capo.boot.precheck.ok",
            "binary": binary_name,
            "binary_path": resolved,
            "observed": observed,
            "required": required,
        },
    )
    return observed


# ---------------------------------------------------------------------------
# Public entry point.
# ---------------------------------------------------------------------------


def precheck_binaries(settings: Settings) -> None:
    """Validate ``claude`` and ``codex`` CLI versions at boot.

    Reads the configured binary names from
    ``settings.agents.claude_code.binary`` and
    ``settings.agents.codex.binary``. Runs each with ``--version`` (5 s
    timeout) and asserts the parsed SemVer is ``>=`` the module-level
    minimums.

    Raises:
        BinaryPrecheckError: A binary is missing, hangs, exits non-zero,
            produces unparseable output, or reports a version below the
            required minimum. The caller (``capo.main.amain``) translates
            this into a single-line stderr message and process exit 2.

    Side effects:
        Emits info-level log events on success
        (``capo.boot.precheck.ok``); warning on unparseable output
        (``capo.boot.precheck.unparseable``); info on skip
        (``capo.boot.precheck.skipped``).
    """
    if settings.boot.skip_binary_precheck:
        logger.info(
            "capo.boot: binary precheck SKIPPED via boot.skip_binary_precheck",
            extra={
                "event": "capo.boot.precheck.skipped",
                "reason": "boot.skip_binary_precheck=true",
            },
        )
        return

    # Claude Code first — most boots delegate to it.
    _precheck_one(
        "claude",
        settings.agents.claude_code.binary,
        MIN_CLAUDE_CODE_VERSION,
    )
    # Codex second.
    _precheck_one(
        "codex",
        settings.agents.codex.binary,
        MIN_CODEX_VERSION,
    )


# ---------------------------------------------------------------------------
# Boot-path adapter (used by capo.main.amain).
# ---------------------------------------------------------------------------


def run_precheck_or_exit(settings: Settings) -> int | None:
    """Run :func:`precheck_binaries`; on failure write stderr + return 2.

    Convenience wrapper for the boot path: ``capo.main.amain`` calls this
    immediately after Settings is loaded and BEFORE
    :func:`capo.workflows.init_dbos`. Returns ``None`` on success (so the
    caller falls through to the rest of boot) or ``2`` on a precheck
    failure (so the caller can ``return 2`` upstream).

    The stderr line follows the existing capo boot-error pattern:
    ``capo: <message>`` (single line, no stack trace).
    """
    try:
        precheck_binaries(settings)
    except BinaryPrecheckError as exc:
        sys.stderr.write(f"capo: {exc}\n")
        return 2
    return None


__all__ = [
    "BinaryPrecheckError",
    "MIN_CLAUDE_CODE_VERSION",
    "MIN_CODEX_VERSION",
    "precheck_binaries",
    "run_precheck_or_exit",
]

"""Codex CLI delegation tool (spec §5.4, §7.5, §9.4).

This module is the Codex CLI counterpart to :mod:`capo.tools.claude_code`.
It owns the **structured shape** of a Codex delegation request
(:class:`CodexBrief`) and the spawn-site lifecycle for ``codex exec``
subprocesses (:func:`delegate_to_codex`).

Phase 4 / Task #45 scope
------------------------

Task #45 implements the spawn site. Lifecycle mirrors
:func:`capo.tools.claude_code.delegate_to_claude_code` (Tasks #22 + #35)
verbatim — same persistence-before-yield contract, same DBOS workflow
handoff, same cleanup-on-failure semantics. Differences from the CC path
are isolated to:

1. **Argv** — :func:`delegate_to_codex` builds
   ``codex exec --json --skip-git-repo-check --sandbox <mode> -C <dir>
   "<prompt>"`` per spec §5.4 (amended by Task #37). Optional flags:
   ``--model <brief.model>``. We NEVER pass ``--ephemeral`` (it disables
   rollout persistence and breaks resume per spike S-1).
2. **First-event field** — Codex's session token is
   ``thread.started.thread_id`` (string UUID) per spike S-1 §5. Capture
   path persists this string to ``delegations.session_id_subagent``,
   reusing the existing column. Task #46 reads it back for
   ``codex exec resume``.
3. **Min version** — :data:`MIN_CODEX_VERSION` is ``"0.130.0"`` per
   spike S-1 §3. A pre-flight version check (Task #62 owns the boot
   sweep; this module exposes :func:`_check_codex_version` for direct
   spawn-time enforcement) raises :class:`CodexBinaryError` when the
   binary is missing or too old.
4. **Sandbox validation** — :class:`CodexBrief` validates
   ``sandbox`` against ``{"read-only", "workspace-write",
   "danger-full-access"}`` at construction time (Pydantic
   ``ValidationError``).

Resume (Task #46) and approval gating (Task #43) live elsewhere — this
module only handles the **initial** spawn.

Invariants
----------

* **SOUL is never present in the rendered brief.** Spec §5.1 anchors
  SOUL in the *main* agent's ``instructions=`` only. Subagent prompts
  are built from :class:`CodexBrief` alone and have no access to the
  SOUL file.
* **NEVER pass ``--ephemeral``.** It disables rollout persistence,
  which breaks ``codex exec resume`` after a restart (spike S-1 §6).
* **Stdin is closed on spawn** so the trailing positional ``<prompt>``
  is the sole input (per spec §7.5 — if both prompt arg and piped
  stdin are present, Codex appends a ``<stdin>`` block, which is
  surprising and avoided).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai import RunContext

from capo.deps import CapoDeps
from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.tools._approval import (
    REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX,
    REQUEST_TYPE_DELEGATE_OUT_OF_ROOT,
    ApprovalRejected,
    ApprovalRequired,
    ApprovalUnavailableError,
    new_approval_id,
    request_tool_approval,
    utc_iso_now,
)
from capo.tools._subprocess import ReaderHandle, start_reader
from capo.tools._worktree import WorktreeError, create_worktree

logger = logging.getLogger("capo.tools.codex")


# ---------------------------------------------------------------------------
# Module-level constants (mirrors :mod:`capo.tools.claude_code`).
# ---------------------------------------------------------------------------

#: Minimum required Codex CLI version per spike S-1 §3 / spec §5.4.
#: Re-run S-1 on minor-version bumps before changing this.
MIN_CODEX_VERSION: str = "0.130.0"

#: Allowed sandbox modes per spec §7.5 "Integration: Codex CLI".
#: ``read-only`` is the safest; ``workspace-write`` is the default for
#: Capo; ``danger-full-access`` should require approval (§5.8) — that
#: enforcement is Task #43's responsibility, not this module's.
CodexSandboxMode = Literal[
    "read-only", "workspace-write", "danger-full-access"
]

#: Wire-level allowed sandbox values (kept in sync with
#: :data:`CodexSandboxMode`). Used both for Pydantic validation and for
#: argv assembly. Frozen so it can't be mutated by importers.
_ALLOWED_SANDBOX_MODES: frozenset[str] = frozenset(
    {"read-only", "workspace-write", "danger-full-access"}
)

#: Default sandbox mode when neither the brief nor config specifies one.
#: Per spec §7.5: "Default for Capo: ``workspace-write``."
DEFAULT_SANDBOX_MODE: str = "workspace-write"

#: pipe buffer for ``asyncio.create_subprocess_exec``. Aligned with the
#: 2 MiB limit used by the CC spawn site (§6.1). Codex JSON events are
#: typically smaller than CC's but the reader still benefits from the
#: large buffer when JSONL lines are concatenated by the OS.
_SUBPROCESS_STREAM_LIMIT: int = 2 * 1024 * 1024

#: Default Codex session-id (thread_id) capture timeout. Spike S-1 §5
#: observed first-event latency under 1 s; 5 s is the conservative
#: ceiling specified by the Task #45 brief.
CODEX_THREAD_ID_CAPTURE_TIMEOUT_S: float = 5.0

#: Hard timeout on the ``codex --version`` pre-flight check.
_VERSION_CHECK_TIMEOUT_S: float = 10.0

_MONITOR_TASK_NAME_PREFIX: str = "delegate-codex-monitor-"


# ---------------------------------------------------------------------------
# Typed errors.
# ---------------------------------------------------------------------------


class CodexBinaryError(RuntimeError):
    """Raised when the ``codex`` binary is missing or below the min version.

    Carries both the resolved binary path (or basename) and the observed
    version string for operator-friendly diagnostics. The spawn site uses
    this error to short-circuit BEFORE any DB write or worktree creation,
    so a misconfigured machine never produces a half-created delegation.
    """

    def __init__(
        self,
        message: str,
        *,
        binary: str,
        observed_version: str | None = None,
    ) -> None:
        super().__init__(message)
        self.binary: str = binary
        self.observed_version: str | None = observed_version


class ThreadIdCaptureTimeout(RuntimeError):
    """Codex did not emit ``thread.started`` within the capture timeout.

    The spawn site catches this exception, marks the row ``failed`` with
    a summary mentioning the timeout, kills the subprocess, and removes
    it from the registry. Mirrors
    :class:`capo.tools.claude_code.SessionIdCaptureTimeout`.
    """


class DBOSNotLaunchedError(RuntimeError):
    """Raised by :func:`delegate_to_codex` when DBOS has not been launched
    before the spawn site is invoked.

    Same contract as :class:`capo.tools.claude_code.DBOSNotLaunchedError`:
    the spawn site hands off the live subprocess to the DBOS
    ``monitor_delegation`` workflow, and that handoff requires
    :func:`capo.workflows.init_dbos` + :func:`capo.workflows.launch_dbos`
    to have run during boot. If a caller invokes the tool before then,
    we mark the row ``failed`` and surface this typed error.
    """


# ---------------------------------------------------------------------------
# CodexBrief — the Pydantic value object the agent constructs to delegate.
# ---------------------------------------------------------------------------


class CodexBrief(BaseModel):
    """Structured Codex delegation request (spec §5.4).

    Mirrors :class:`capo.tools.claude_code.ClaudeCodeBrief` in field
    shape; differences:

    * ``sandbox`` (string Literal) replaces CC's ``permission_mode``.
      Validated against :data:`_ALLOWED_SANDBOX_MODES` at construction
      time — out-of-set values trigger Pydantic ``ValidationError``.
    * No template-rendered prompt: Codex's ``codex exec`` consumes the
      prompt as a trailing positional argument, so the rendered brief
      bytes ARE the prompt. The brief carries ``goal`` + structured
      lists (``constraints``, ``success_criteria``, ``relevant_files``)
      and :func:`_render_codex_prompt` assembles them inline (no Jinja).

    Attributes
    ----------
    goal:
        Short, imperative description of what the subagent should do.
        Required and non-empty (whitespace stripped before validation).
    repo_path:
        Absolute path to the repository the subagent should operate on.
        Out-of-``projects_root`` paths trigger the approval flow at the
        tool boundary (§5.8); this model itself does not enforce that —
        it only ensures the string is present and non-empty.
    sandbox:
        Codex sandbox mode. One of
        ``{"read-only", "workspace-write", "danger-full-access"}``.
        Defaults to :data:`DEFAULT_SANDBOX_MODE` (``workspace-write``).
    constraints:
        Free-form "don't do X" / "must use Y" rules. May be empty.
    success_criteria:
        Verifiable conditions for completion. May be empty.
    relevant_files:
        Hints for the subagent to read first. May be empty.
    create_worktree:
        When ``True`` (default), the spawn step will create an isolated
        ``git worktree`` under ``~/.capo/workspaces/<delegation_id>/``.
        Setting this to ``False`` is meaningful for non-git repositories
        or explicit override paths.
    model:
        Optional per-delegation model override. ``None`` means "use
        ``config.models.subagents.codex``" per §5.9.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
    )

    goal: str = Field(min_length=1, description="What the subagent should do.")
    repo_path: str = Field(min_length=1, description="Absolute path to target repo.")
    sandbox: CodexSandboxMode = DEFAULT_SANDBOX_MODE  # type: ignore[assignment]
    constraints: list[str] = Field(default_factory=list)
    success_criteria: list[str] = Field(default_factory=list)
    relevant_files: list[str] = Field(default_factory=list)
    create_worktree: bool = True
    model: str | None = None


# ---------------------------------------------------------------------------
# CodexDelegationHandle — the agent-visible return value.
# ---------------------------------------------------------------------------


class CodexDelegationHandle(BaseModel):
    """Agent-visible handle returned by :func:`delegate_to_codex`.

    Spec §5.4 acceptance criterion: "Same lifecycle as CC: persist row →
    spawn → register DBOS workflow → return handle." The shape mirrors
    :class:`capo.tools.claude_code.DelegationHandle` except that
    ``agent`` is fixed to ``"codex"`` so downstream tools
    (:func:`check_delegation_status` etc.) can branch on it.
    """

    model_config = ConfigDict(frozen=True)

    delegation_id: str = Field(min_length=1)
    agent: Literal["codex"] = "codex"
    workspace: str = Field(min_length=1)
    status: str = Field(min_length=1)
    started_at: str = Field(min_length=1)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Rendering — assemble the brief into the positional prompt argument.
# ---------------------------------------------------------------------------


def _bullets(items: list[str]) -> str:
    """Render ``items`` as a markdown bullet list, or '_None._' when empty.

    Mirrors :func:`capo.tools.claude_code._render_bulleted` so a brief
    composed for either agent reads the same to the human eye.
    """
    if not items:
        return "_None._"
    return "\n".join(f"- {item}" for item in items)


def _render_codex_prompt(brief: CodexBrief) -> str:
    """Render ``brief`` into the trailing positional prompt for ``codex exec``.

    The output is **deterministic** for a given ``brief`` — no
    timestamps, no environment reads, no randomness — so DBOS resume
    (Task #46) reproduces the same prompt bytes on restart. Same
    determinism contract as CC's :func:`render_brief`.
    """
    parts = [
        "# Goal",
        brief.goal,
        "",
        "# Repository",
        brief.repo_path,
        "",
        "# Constraints",
        _bullets(brief.constraints),
        "",
        "# Success Criteria",
        _bullets(brief.success_criteria),
        "",
        "# Relevant Files",
        _bullets(brief.relevant_files),
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Version check.
# ---------------------------------------------------------------------------


_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


def _parse_semver(text: str) -> tuple[int, int, int] | None:
    """Extract the first ``X.Y.Z`` semver triple from ``text``.

    Tolerant of prefixes like ``codex-cli/0.130.0`` or
    ``codex 0.130.0 (build abcdef)``. Returns ``None`` when no triple
    is found.
    """
    match = _VERSION_RE.search(text)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _check_codex_version(binary: str) -> str:
    """Run ``<binary> --version`` and return the observed version string.

    Raises:
        CodexBinaryError: ``codex`` is missing from PATH OR
            ``--version`` exited non-zero OR the parsed version is
            below :data:`MIN_CODEX_VERSION`.

    Synchronous (subprocess.run). Callers wrap in
    :func:`asyncio.to_thread`. The pre-flight check runs BEFORE any
    side effect (no worktree, no DB row) so a misconfigured machine
    never leaves an orphan.
    """
    resolved = shutil.which(binary)
    if resolved is None:
        raise CodexBinaryError(
            f"codex binary not found on PATH: {binary!r}. "
            f"Install codex>={MIN_CODEX_VERSION} and ensure the binary "
            "is reachable, or set 'agents.codex.binary' in config.toml.",
            binary=binary,
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
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CodexBinaryError(
            f"codex --version failed for {resolved!r}: {exc!r}",
            binary=resolved,
        ) from exc

    if result.returncode != 0:
        raise CodexBinaryError(
            f"codex --version exited {result.returncode} for {resolved!r}: "
            f"stderr={result.stderr.strip()!r}",
            binary=resolved,
        )

    observed = (result.stdout or result.stderr or "").strip()
    parsed = _parse_semver(observed)
    min_parsed = _parse_semver(MIN_CODEX_VERSION)
    if parsed is None or min_parsed is None:
        raise CodexBinaryError(
            f"could not parse codex version from output: {observed!r}",
            binary=resolved,
            observed_version=observed,
        )
    if parsed < min_parsed:
        raise CodexBinaryError(
            f"codex {observed!r} is below required minimum "
            f"{MIN_CODEX_VERSION!r} (binary={resolved!r}). "
            "Re-run spike S-1 after upgrading.",
            binary=resolved,
            observed_version=observed,
        )
    return observed


# ---------------------------------------------------------------------------
# DB helpers (sync — callers wrap in :func:`asyncio.to_thread`).
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """ISO-8601 UTC timestamp the ``delegations.started_at`` column accepts.

    Matches the format produced by
    :func:`capo.tools.claude_code._utc_now_iso` so mixed-agent rows are
    comparable.
    """
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _insert_running_row(
    conn: sqlite3.Connection,
    *,
    delegation_id: str,
    user_id: str,
    workspace: str,
    pid: int | None,
    brief_json: str,
    parent_thread_id: str | None,
    model: str | None,
    started_at: str,
) -> None:
    """INSERT a ``status='running'`` delegations row inside BEGIN IMMEDIATE.

    Mirror of :func:`capo.tools.claude_code._insert_delegation_row` with
    ``agent='codex'``. Goes through :func:`begin_immediate_with_retry`
    for the §7.3 zero-``SQLITE_BUSY`` contract.
    """
    with begin_immediate_with_retry(
        conn, operation=f"insert_codex_delegation:{delegation_id}"
    ):
        conn.execute(
            "INSERT INTO delegations ("
            "id, user_id, agent, workspace, pid, session_id_subagent, "
            "brief_json, status, started_at, parent_thread_id, summary, model"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                delegation_id,
                user_id,
                "codex",
                workspace,
                pid,
                None,  # session_id_subagent — populated after thread.started.
                brief_json,
                "running",
                started_at,
                parent_thread_id,
                None,
                model,
            ),
        )


def _insert_failed_row(
    db_path: Path,
    *,
    delegation_id: str,
    user_id: str,
    workspace: str,
    brief_json: str,
    parent_thread_id: str | None,
    model: str | None,
    reason: str,
) -> None:
    """INSERT a terminal-``failed`` codex row from a sync worker thread.

    Used when setup fails (version-check, worktree, spawn, persist) so
    there's never an orphan workspace on disk without a corresponding DB
    row. Mirrors :func:`capo.tools.claude_code._insert_failed_row`.
    """
    now = _utc_now_iso()
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn,
            operation=f"insert_failed_codex_delegation:{delegation_id}",
        ):
            conn.execute(
                "INSERT INTO delegations ("
                "id, user_id, agent, workspace, pid, session_id_subagent, "
                "brief_json, status, started_at, ended_at, "
                "parent_thread_id, summary, model"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    delegation_id,
                    user_id,
                    "codex",
                    workspace,
                    None,
                    None,
                    brief_json,
                    "failed",
                    now,
                    now,
                    parent_thread_id,
                    reason,
                    model,
                ),
            )
    finally:
        conn.close()


def _update_status(
    db_path: Path,
    *,
    delegation_id: str,
    status: str,
    summary: str | None,
) -> None:
    """UPDATE a delegations row to a terminal status + summary."""
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation=f"update_codex_status:{delegation_id}"
        ):
            conn.execute(
                "UPDATE delegations SET status = ?, ended_at = ?, summary = ? "
                "WHERE id = ?",
                (status, _utc_now_iso(), summary, delegation_id),
            )
    finally:
        conn.close()


def _update_session_id(
    db_path: Path, delegation_id: str, thread_id: str
) -> None:
    """UPDATE ``delegations.session_id_subagent`` with the Codex thread_id.

    Per spec §5.4 / spike S-1 §5: ``thread_id`` is Codex's resume token.
    We reuse the existing ``session_id_subagent`` column (per task brief)
    so Task #46's resume step can look it up the same way the CC resume
    step does.
    """
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn, operation=f"update_codex_session_id:{delegation_id}"
        ):
            conn.execute(
                "UPDATE delegations SET session_id_subagent = ? WHERE id = ?",
                (thread_id, delegation_id),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# repo_path scope check.
# ---------------------------------------------------------------------------


def _classify_repo_path_scope(
    repo_path: str, projects_root: Path | None
) -> tuple[bool, str | None]:
    """Classify ``repo_path`` against ``projects_root``.

    Returns a ``(in_scope, reason)`` tuple. See the matching helper in
    :mod:`capo.tools.claude_code` for the same contract — they intentionally
    mirror each other because both spawn sites gate on the same §5.8 rule.
    """
    if projects_root is None:
        return True, None
    try:
        resolved_repo = Path(repo_path).expanduser().resolve()
    except (OSError, ValueError) as exc:
        raise ApprovalRequired(
            repo_path,
            f"could not resolve repo_path: {exc!r}",
        ) from exc
    resolved_root = projects_root.resolve()
    if resolved_repo == resolved_root or resolved_repo.is_relative_to(
        resolved_root
    ):
        return True, None
    reason = (
        f"repo_path {str(resolved_repo)!r} is outside projects_root "
        f"{str(resolved_root)!r}"
    )
    return False, reason


def _prompt_hash(rendered_prompt: str) -> str:
    """Return a short SHA-256 fingerprint of the rendered prompt.

    Mirrors :func:`capo.tools.claude_code._prompt_hash`. Used in the
    approval ``request_payload`` so reviewers can correlate the approval
    row with the exact prompt that was about to be sent — WITHOUT
    leaking the prompt text itself into the approvals table.
    """
    return hashlib.sha256(rendered_prompt.encode("utf-8")).hexdigest()[:16]


async def _await_codex_approval(
    ctx: RunContext[CapoDeps],
    *,
    request_type: str,
    repo_path: str,
    model: str | None,
    sandbox: str,
    prompt_hash: str,
    reason: str,
) -> None:
    """Request a §5.8 approval and raise on non-``approved`` outcomes.

    Calls :func:`_request_codex_approval` and translates the decision:

    * ``approved`` → return ``None`` (caller proceeds).
    * ``denied`` / ``expired`` / ``cancelled`` → raise
      :class:`ApprovalRejected`.
    * Approval workflow unavailable (DBOS not launched, etc.) → raise
      :class:`ApprovalRequired` (legacy fallback, same as
      :func:`shell_exec` and the CC spawn site).
    """
    try:
        decision = await _request_codex_approval(
            ctx,
            request_type=request_type,
            repo_path=repo_path,
            model=model,
            sandbox=sandbox,
            prompt_hash=prompt_hash,
            reason=reason,
        )
    except ApprovalUnavailableError as exc:
        raise ApprovalRequired(
            repo_path,
            (
                f"{reason} and approval workflow is unavailable "
                f"({exc.reason})"
            ),
        ) from exc

    if decision.status == "approved":
        logger.info(
            "delegate_to_codex: approval %s approved by %s — proceeding "
            "(request_type=%s repo_path=%r)",
            decision.approval_id,
            decision.resolved_by,
            request_type,
            repo_path,
            extra={
                "event": f"capo.tools.codex.{request_type}.approved",
                "approval_id": decision.approval_id,
                "resolved_by": decision.resolved_by,
                "repo_path": repo_path,
            },
        )
        return

    logger.info(
        "delegate_to_codex: approval %s resolved %s by %s "
        "(reason=%r request_type=%s)",
        decision.approval_id,
        decision.status,
        decision.resolved_by,
        decision.reason,
        request_type,
        extra={
            "event": f"capo.tools.codex.{request_type}.rejected",
            "approval_id": decision.approval_id,
            "status": decision.status,
            "resolved_by": decision.resolved_by,
        },
    )
    raise ApprovalRejected(decision)


async def _request_codex_approval(
    ctx: RunContext[CapoDeps],
    *,
    request_type: str,
    repo_path: str,
    model: str | None,
    sandbox: str,
    prompt_hash: str,
    reason: str,
) -> Any:
    """Invoke the §5.8 approval workflow for a Codex delegation.

    Used for BOTH ``delegate_out_of_root`` and ``delegate_dangerous_sandbox``
    request types. ``approval_id`` and ``requested_at`` are generated
    BEFORE the call so DBOS replay observes the same values.
    """
    approval_id = new_approval_id()
    requested_at = utc_iso_now()
    request_payload: dict[str, Any] = {
        "agent": "codex",
        "repo_path": repo_path,
        "model": model,
        "sandbox": sandbox,
        "mode": sandbox,
        "prompt_hash": prompt_hash,
        "reason": reason,
    }
    return await request_tool_approval(
        ctx.deps,
        request_type=request_type,
        request_payload=request_payload,
        approval_id=approval_id,
        requested_at=requested_at,
        log_subject=(
            f"delegate_to_codex request_type={request_type} "
            f"repo_path={repo_path!r}"
        ),
    )


# ---------------------------------------------------------------------------
# thread_id capture.
# ---------------------------------------------------------------------------


def _extract_thread_id(event: dict[str, Any]) -> str | None:
    """Return ``event['thread_id']`` when ``type == 'thread.started'``.

    Per spike S-1 §5 the canonical Codex session token is the
    ``thread_id`` UUID emitted on the FIRST stdout event whose
    ``type`` is ``"thread.started"``. The reader's first-event hook
    (Task #45 / :mod:`capo.tools._subprocess`) already filters for
    events that carry either ``session_id`` (CC) or ``thread_id``
    (Codex); this helper performs the final shape check.
    """
    if event.get("type") != "thread.started":
        return None
    tid = event.get("thread_id")
    if isinstance(tid, str) and tid:
        return tid
    return None


async def _await_thread_id(
    reader_handle: ReaderHandle,
    delegation_id: str,
    *,
    db_path: Path,
    timeout_s: float = CODEX_THREAD_ID_CAPTURE_TIMEOUT_S,
) -> str:
    """Wait for the reader to surface the ``thread.started`` event.

    Subscribes to :attr:`ReaderHandle.first_event_received`; once set,
    reads :attr:`ReaderHandle.first_event` and extracts ``thread_id``
    per spike S-1 §5. The row's ``session_id_subagent`` column is
    UPDATEd inside :func:`begin_immediate_with_retry`.

    Raises:
        ThreadIdCaptureTimeout: The reader's
            :attr:`first_event_received` never fired within
            ``timeout_s`` OR the captured event was not a valid
            ``thread.started`` payload.
    """
    try:
        await asyncio.wait_for(
            reader_handle.first_event_received.wait(), timeout=timeout_s
        )
    except TimeoutError as exc:
        raise ThreadIdCaptureTimeout(
            f"thread_id not captured within {timeout_s}s "
            f"(delegation_id={delegation_id})"
        ) from exc

    event = reader_handle.first_event or {}
    thread_id = _extract_thread_id(event)
    if thread_id is None:
        # The reader fires the hook on EITHER session_id (CC) OR
        # thread_id (Codex). A CC-shaped event reaching this helper is
        # a misconfiguration (wrong binary for the agent kind). Surface
        # the same timeout-shaped error so the caller's cleanup path
        # treats it uniformly.
        raise ThreadIdCaptureTimeout(
            f"first event from codex did not carry a thread.started "
            f"thread_id (delegation_id={delegation_id}, event_type="
            f"{event.get('type')!r})"
        )

    await asyncio.to_thread(
        _update_session_id, db_path, delegation_id, thread_id
    )
    return thread_id


# ---------------------------------------------------------------------------
# Cleanup helpers (best-effort, never raise).
# ---------------------------------------------------------------------------


async def _terminate_process(process: Any) -> None:
    """Best-effort kill of ``process`` after a setup-time failure."""
    if process is None:
        return
    terminate = getattr(process, "terminate", None)
    kill = getattr(process, "kill", None)
    wait = getattr(process, "wait", None)
    if callable(terminate):
        with contextlib.suppress(Exception):
            terminate()
    if callable(wait):
        with contextlib.suppress(Exception):
            await asyncio.wait_for(wait(), timeout=0.5)
        if getattr(process, "returncode", None) is None and callable(kill):
            with contextlib.suppress(Exception):
                kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(wait(), timeout=0.5)


def _best_effort_cleanup_worktree(
    repo_path: str, workspace: Path, branch_name: str
) -> None:
    """Run ``git worktree remove --force`` + ``git branch -D`` quietly.

    Mirror of :func:`capo.tools.claude_code._best_effort_cleanup_worktree`
    — kept local so this module doesn't reach into the CC module's
    privates. Used when worktree creation succeeded but a later setup
    step (version check, spawn, persist) failed.
    """
    for args in (
        ["git", "worktree", "remove", "--force", str(workspace)],
        ["git", "branch", "-D", branch_name],
        ["git", "worktree", "prune"],
    ):
        with contextlib.suppress(Exception):
            subprocess.run(  # noqa: S603 - argv form, fixed prefix
                args,
                cwd=repo_path,
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                timeout=30.0,
            )


def _rmtree_silent(path: Path) -> None:
    """Best-effort recursive remove. Never raises."""
    if not path.exists():
        return
    try:
        import shutil as _shutil

        _shutil.rmtree(path, ignore_errors=True)
    except Exception:  # noqa: BLE001 - best-effort cleanup
        pass


# ---------------------------------------------------------------------------
# Config helpers.
# ---------------------------------------------------------------------------


def _agents_codex_value(deps: CapoDeps, key: str, default: str | None) -> str | None:
    """Read ``settings.agents.codex.<key>`` with a graceful default.

    Mirror of :func:`capo.tools.claude_code._agents_claude_code_value`.
    ``AgentBinaryConfig`` declares only ``binary`` explicitly; everything
    else (``default_sandbox`` etc.) lives in ``model_extra``. Returns
    ``default`` when neither location has a value.
    """
    cx = deps.settings.agents.codex
    extra = getattr(cx, "model_extra", None) or {}
    value = extra.get(key) if isinstance(extra, dict) else None
    if value is None:
        value = getattr(cx, key, None)
    if value is None:
        return default
    return str(value)


# ---------------------------------------------------------------------------
# delegate_to_codex — the public agent tool.
# ---------------------------------------------------------------------------


async def delegate_to_codex(
    ctx: RunContext[CapoDeps], codex_brief: CodexBrief
) -> CodexDelegationHandle:
    """Delegate a coding task to Codex CLI (spec §5.4, §9.4).

    Lifecycle (mirrors :func:`capo.tools.claude_code.delegate_to_claude_code`):

    1. Generate a fresh ``delegation_id`` (uuid4 hex).
    2. Pre-flight: verify the codex binary exists and is ``>=
       MIN_CODEX_VERSION``. Raises :class:`CodexBinaryError` BEFORE any
       side effects so a misconfigured machine never leaves orphans.
    3. Validate ``codex_brief.repo_path`` is inside ``projects_root`` —
       raise :class:`ApprovalRequired` otherwise (Task #43 wires the
       actual approval gate; this task just raises).
    4. Create ``workspaces_root/<delegation_id>/``.
    5. If ``codex_brief.create_worktree``, ``git worktree add`` a fresh
       branch off ``agents.codex.worktree_base_branch`` (default
       ``"main"``). On failure clean up + INSERT a ``failed`` row +
       re-raise.
    6. Render the Codex prompt via :func:`_render_codex_prompt`.
    7. Build the Codex argv per spec §5.4 (NEVER pass ``--ephemeral``):

           codex exec --json --skip-git-repo-check --sandbox <mode>
                      -C <workspace> [--model <m>] "<prompt>"

    8. Spawn with ``asyncio.create_subprocess_exec(...)``, close stdin
       immediately (so the prompt arg is the sole input), limit=2 MiB.
    9. **Persist BEFORE returning**: INSERT one ``delegations`` row with
       ``agent='codex'``, ``status='running'``, ``pid``,
       ``parent_thread_id`` (from ``ctx.deps.thread_id``),
       ``session_id_subagent=NULL``, ``model``, ``brief_json``.
    10. Start the output reader (Task #21) — the reader skips Codex's
        ``"Reading additional input from stdin..."`` non-JSON prefix
        per spike S-1 §6.
    11. **Hand off monitoring to DBOS** (mirrors Task #35): pre-flight
        :func:`is_launched()` (raise :class:`DBOSNotLaunchedError`
        otherwise), register the live ``(process, reader_handle)`` via
        :func:`register_delegation_subprocess`, then invoke
        :func:`monitor_delegation` as a fire-and-forget asyncio task
        after the first-event ``thread_id`` capture completes.
    12. Return a :class:`CodexDelegationHandle` immediately.

    Failure handling between steps 4 and 9:
      * Kill the subprocess if it spawned.
      * Best-effort ``cleanup_worktree`` if the worktree was created.
      * INSERT a ``failed`` row with a clear reason.
      * Re-raise the underlying exception.

    Args:
        ctx: Pydantic AI :class:`RunContext` carrying
            :class:`~capo.deps.CapoDeps` with ``settings``, ``user_id``,
            ``thread_id``, ``workspaces_root``, ``projects_root``.
        codex_brief: Validated :class:`CodexBrief`. Same brief is
            JSON-serialized into ``brief_json`` so Task #46 resume can
            reconstruct it.

    Returns:
        :class:`CodexDelegationHandle` with ``status='running'``.

    Raises:
        CodexBinaryError: codex missing or below
            :data:`MIN_CODEX_VERSION` — raised BEFORE any side effects.
        ApprovalRequired: ``codex_brief.repo_path`` is outside
            ``ctx.deps.projects_root`` AND the approval workflow is
            unavailable (DBOS not launched, approval module not
            importable). Also raised eagerly for unresolvable
            ``repo_path`` strings.
        ApprovalRejected: The §5.8 approval workflow ran and resolved
            to ``denied`` / ``expired`` / ``cancelled``. Raised for
            either out-of-``projects_root`` or
            ``sandbox=danger-full-access`` gating (Task #43).
        WorktreeError: Subclass thereof when worktree creation fails —
            the failed row is INSERTed first.
        DBOSNotLaunchedError: Spawn site reached step 11 but
            :func:`capo.workflows.launch_dbos` has not run yet.
    """
    deps: CapoDeps = ctx.deps
    settings = deps.settings
    db_path: Path = settings.paths.db_path
    workspaces_root: Path | None = deps.workspaces_root or (
        settings.paths.workspaces_root
    )
    if workspaces_root is None:
        raise ValueError(
            "delegate_to_codex: workspaces_root is not configured on "
            "deps or settings.paths"
        )

    # --- 1. delegation_id ---------------------------------------------------
    delegation_id = uuid.uuid4().hex
    brief_json = codex_brief.model_dump_json()
    user_id = deps.user_id or "unknown"
    parent_thread_id = deps.thread_id
    model = codex_brief.model or _agents_codex_value(
        deps, "model", settings.models.subagents.codex
    )
    sandbox = codex_brief.sandbox or _agents_codex_value(
        deps, "default_sandbox", DEFAULT_SANDBOX_MODE
    ) or DEFAULT_SANDBOX_MODE
    if sandbox not in _ALLOWED_SANDBOX_MODES:
        # Defense in depth: the brief's Literal already enforces this,
        # but config-override could in principle smuggle a bad value.
        raise ValueError(
            f"delegate_to_codex: sandbox={sandbox!r} is not one of "
            f"{sorted(_ALLOWED_SANDBOX_MODES)!r}"
        )
    base_branch = _agents_codex_value(
        deps, "worktree_base_branch", "main"
    ) or "main"
    binary = settings.agents.codex.binary or "codex"

    # --- 2. version pre-flight ---------------------------------------------
    # Runs BEFORE any side effects — failure here means no row, no
    # workspace, no subprocess. Matches the "typed error before any side
    # effects" Edge Cases requirement.
    await asyncio.to_thread(_check_codex_version, binary)

    # --- 3. approval gating (Task #43) -------------------------------------
    # Two approval gates apply to Codex delegations:
    #
    # (a) Out-of-projects_root — same §5.8 rule as the CC spawn site
    #     (Task #43 / spec §5.4 / §5.8).
    # (b) Dangerous sandbox — ``sandbox=danger-full-access`` is the Codex
    #     equivalent of disabling the sandbox entirely. Spec §7.5 (line
    #     1209) requires §5.8 approval before doing so. We use a
    #     distinct ``request_type`` so the reviewer can tell the two
    #     gates apart in logs / UI.
    #
    # If BOTH apply we ask twice (out-of-root first, then dangerous
    # sandbox) — independent approvals so an operator can deny one
    # without forfeiting the other. In practice the agent rarely asks
    # for both at once, but the contract stays simple.
    in_scope, scope_reason = _classify_repo_path_scope(
        codex_brief.repo_path, deps.projects_root
    )
    # Compute the prompt hash up front so the approval reviewer can
    # correlate the row with the prompt the subagent would see.
    preview_prompt = _render_codex_prompt(codex_brief)
    prompt_hash = _prompt_hash(preview_prompt)

    if not in_scope:
        await _await_codex_approval(
            ctx,
            request_type=REQUEST_TYPE_DELEGATE_OUT_OF_ROOT,
            repo_path=codex_brief.repo_path,
            model=model,
            sandbox=sandbox,
            prompt_hash=prompt_hash,
            reason=scope_reason or "out-of-projects_root",
        )

    if sandbox == "danger-full-access":
        await _await_codex_approval(
            ctx,
            request_type=REQUEST_TYPE_DELEGATE_DANGEROUS_SANDBOX,
            repo_path=codex_brief.repo_path,
            model=model,
            sandbox=sandbox,
            prompt_hash=prompt_hash,
            reason=(
                "Codex delegation requested sandbox=danger-full-access "
                "(spec §5.4 / §7.5)"
            ),
        )

    # --- 4. workspace dir ---------------------------------------------------
    workspace_dir = workspaces_root / delegation_id
    workspace_str = os.fspath(workspace_dir)
    try:
        workspaces_root.mkdir(parents=True, exist_ok=True)
        if not codex_brief.create_worktree:
            workspace_dir.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason=f"workspace directory creation failed: {exc!r}",
        )
        raise

    # --- 5. worktree (optional) --------------------------------------------
    worktree_created = False
    if codex_brief.create_worktree:
        try:
            await asyncio.to_thread(
                create_worktree,
                codex_brief.repo_path,
                workspace_str,
                delegation_id,
                base_branch,
            )
            worktree_created = True
        except WorktreeError as exc:
            _rmtree_silent(workspace_dir)
            reason = (
                f"worktree creation failed: {exc.__class__.__name__}: {exc}"
            )
            await asyncio.to_thread(
                _insert_failed_row,
                db_path,
                delegation_id=delegation_id,
                user_id=user_id,
                workspace=workspace_str,
                brief_json=brief_json,
                parent_thread_id=parent_thread_id,
                model=model,
                reason=reason,
            )
            raise

    # --- 6. render prompt ---------------------------------------------------
    rendered_prompt = _render_codex_prompt(codex_brief)

    # --- 7. argv ------------------------------------------------------------
    # Spec §5.4 canonical form (NEVER pass --ephemeral). ``-C`` is the
    # workspace because that's the actual working directory of the
    # worktree; ``--skip-git-repo-check`` is required because the
    # worktree may not itself look like a fresh git repo to codex.
    argv: list[str] = [
        binary,
        "exec",
        "--json",
        "--skip-git-repo-check",
        "--sandbox",
        sandbox,
        "-C",
        workspace_str,
    ]
    if model:
        argv.extend(["--model", model])
    # Prompt is the trailing positional argument per spec §7.5.
    argv.append(rendered_prompt)

    # --- 8. spawn -----------------------------------------------------------
    process: Any | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=workspace_str,
            stdin=asyncio.subprocess.DEVNULL,  # spec §7.5: close stdin
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_SUBPROCESS_STREAM_LIMIT,
        )
    except FileNotFoundError:
        # Defense-in-depth: version check should have caught this, but
        # PATH can change between the check and the spawn.
        if worktree_created:
            _best_effort_cleanup_worktree(
                codex_brief.repo_path, workspace_dir, delegation_id
            )
        _rmtree_silent(workspace_dir)
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason="codex binary not found on PATH",
        )
        raise
    except (OSError, ValueError) as exc:
        if worktree_created:
            _best_effort_cleanup_worktree(
                codex_brief.repo_path, workspace_dir, delegation_id
            )
        _rmtree_silent(workspace_dir)
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason=(
                f"codex spawn failed: {exc.__class__.__name__}: {exc}"
            ),
        )
        raise

    pid = getattr(process, "pid", None)
    started_at = _utc_now_iso()

    # --- 9. PERSIST BEFORE YIELD -------------------------------------------
    try:

        def _do_insert() -> None:
            conn = open_connection(db_path)
            try:
                _insert_running_row(
                    conn,
                    delegation_id=delegation_id,
                    user_id=user_id,
                    workspace=workspace_str,
                    pid=pid,
                    brief_json=brief_json,
                    parent_thread_id=parent_thread_id,
                    model=model,
                    started_at=started_at,
                )
            finally:
                conn.close()

        await asyncio.to_thread(_do_insert)
    except Exception as exc:
        await _terminate_process(process)
        if worktree_created:
            _best_effort_cleanup_worktree(
                codex_brief.repo_path, workspace_dir, delegation_id
            )
        _rmtree_silent(workspace_dir)
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                _insert_failed_row,
                db_path,
                delegation_id=delegation_id,
                user_id=user_id,
                workspace=workspace_str,
                brief_json=brief_json,
                parent_thread_id=parent_thread_id,
                model=model,
                reason=(
                    f"persistence-before-yield INSERT failed: "
                    f"{exc.__class__.__name__}: {exc}"
                ),
            )
        # No caffeinate track needed: row was never 'running'.
        raise

    # --- 9b. caffeinate track (Task #59) -----------------------------------
    # Row is now durably ``status='running'``. Track with the
    # process-wide caffeinate manager so macOS won't idle-sleep while
    # any delegation (CC or Codex) is alive. Release pairs in: reader
    # spawn failure, DBOS-not-launched failure, thread-id capture
    # timeout, or the DBOS workflow's terminal-status step.
    from capo.caffeinate import get_caffeinate_manager  # noqa: PLC0415

    with contextlib.suppress(Exception):
        await get_caffeinate_manager().track_delegation(delegation_id)

    # --- 10. start reader (Task #21) ---------------------------------------
    reader_conn = open_connection(db_path)
    try:
        reader_handle = start_reader(
            delegation_id, process, store=reader_conn
        )
    except Exception as exc:
        reader_conn.close()
        await _terminate_process(process)
        logger.exception(
            "delegate_to_codex: reader spawn failed for %s: %s",
            delegation_id,
            exc,
        )
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                _update_status,
                db_path,
                delegation_id=delegation_id,
                status="failed",
                summary=(
                    f"reader spawn failed: {exc.__class__.__name__}: {exc}"
                ),
            )
        # Release caffeinate refcount: workflow terminal step won't run.
        with contextlib.suppress(Exception):
            await get_caffeinate_manager().release_delegation(delegation_id)
        raise

    # --- 11. handoff to DBOS monitor_delegation (mirrors Task #35) ---------
    # Pre-flight: DBOS must be launched. The spawn site can be invoked
    # from the agent tool registry as soon as the FastAPI listener is
    # accepting webhook traffic, and ``capo.main.amain`` enforces "launch
    # DBOS BEFORE dispatcher.start()" — but we defend the contract here
    # too so tests / programmatic callers can't trip the workflow
    # registration with a clear, typed failure.
    from capo.workflows import is_launched as _dbos_is_launched  # noqa: PLC0415
    from capo.workflows.delegation import (  # noqa: PLC0415
        DelegationProcessHandle,
        monitor_delegation,
        register_delegation_subprocess,
        unregister_delegation_subprocess,
    )

    if not _dbos_is_launched():
        await _terminate_process(process)
        with contextlib.suppress(Exception):
            await reader_handle.wait()
        with contextlib.suppress(Exception):
            reader_conn.close()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                _update_status,
                db_path,
                delegation_id=delegation_id,
                status="failed",
                summary="DBOS not launched",
            )
        # Release caffeinate refcount: workflow terminal step won't run (Task #59).
        with contextlib.suppress(Exception):
            await get_caffeinate_manager().release_delegation(delegation_id)
        raise DBOSNotLaunchedError(
            f"delegate_to_codex: DBOS not launched — cannot hand off "
            f"monitor for delegation_id={delegation_id!r}. Caller must "
            "invoke capo.workflows.init_dbos(...) + launch_dbos() "
            "during boot before serving traffic."
        )

    register_delegation_subprocess(
        DelegationProcessHandle(
            delegation_id=delegation_id,
            process=process,
            reader_handle=reader_handle,
            db_path=db_path,
        )
    )

    async def _handoff_to_dbos_monitor() -> None:
        try:
            try:
                await _await_thread_id(
                    reader_handle,
                    delegation_id,
                    db_path=db_path,
                    timeout_s=CODEX_THREAD_ID_CAPTURE_TIMEOUT_S,
                )
            except ThreadIdCaptureTimeout as exc:
                logger.error(
                    "orphan codex delegation: %s — %s",
                    delegation_id,
                    exc,
                )
                await _terminate_process(process)
                with contextlib.suppress(Exception):
                    await reader_handle.wait()
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(
                        _update_status,
                        db_path,
                        delegation_id=delegation_id,
                        status="failed",
                        summary=str(exc),
                    )
                unregister_delegation_subprocess(delegation_id)
                # Release caffeinate refcount: workflow terminal step won't run.
                with contextlib.suppress(Exception):
                    await get_caffeinate_manager().release_delegation(
                        delegation_id
                    )
                return

            # NB: monitor_delegation's resume step is currently CC-shaped
            # (claude --resume). Task #46 generalises it to dispatch on
            # ``delegations.agent``. For the INITIAL spawn here the
            # registry is populated so the resume branch is not taken;
            # the workflow just drains the reader + polls returncode.
            try:
                await monitor_delegation(delegation_id, db_path=db_path)
            except BaseException as exc:  # noqa: BLE001
                logger.exception(
                    "delegate_to_codex: monitor_delegation raised for "
                    "delegation_id=%s: %r",
                    delegation_id,
                    exc,
                )
        finally:
            with contextlib.suppress(Exception):
                unregister_delegation_subprocess(delegation_id)
            with contextlib.suppress(Exception):
                reader_conn.close()

    loop = asyncio.get_event_loop()
    monitor_task = loop.create_task(
        _handoff_to_dbos_monitor(),
        name=f"{_MONITOR_TASK_NAME_PREFIX}{delegation_id}",
    )
    monitor_task.add_done_callback(
        lambda t: _log_monitor_exit(delegation_id, t)
    )

    notes = [
        f"agent=codex model={model}",
        f"workspace={workspace_str}",
        f"sandbox={sandbox}",
    ]
    if worktree_created:
        notes.append(f"worktree branch={delegation_id} base={base_branch}")

    return CodexDelegationHandle(
        delegation_id=delegation_id,
        agent="codex",
        workspace=workspace_str,
        status="running",
        started_at=started_at,
        notes=notes,
    )


def _log_monitor_exit(
    delegation_id: str, task: asyncio.Task[Any]
) -> None:
    """Best-effort log when the monitor exits — used as a done_callback."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    logger.error(
        "delegate_to_codex monitor exited with error for "
        "delegation_id=%s: %r",
        delegation_id,
        exc,
    )


__all__ = [
    "CODEX_THREAD_ID_CAPTURE_TIMEOUT_S",
    "DEFAULT_SANDBOX_MODE",
    "MIN_CODEX_VERSION",
    "ApprovalRejected",
    "ApprovalRequired",
    "CodexBinaryError",
    "CodexBrief",
    "CodexDelegationHandle",
    "CodexSandboxMode",
    "DBOSNotLaunchedError",
    "ThreadIdCaptureTimeout",
    "delegate_to_codex",
]

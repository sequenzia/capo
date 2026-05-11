"""Claude Code delegation brief + prompt rendering (spec §5.3, §7.3).

This module owns the **structured shape** of a Claude Code delegation
request and the **deterministic rendering** of that request into the
prompt the ``claude -p ...`` subprocess actually receives.

Phase 2 scope
-------------

Task #19 (this module's first cut) defines just the model + renderer.
The actual ``delegate_to_claude_code`` tool (Task #22) and the
session-id capture (Task #23) layer on top of these primitives in
later tasks.

Invariants
----------

* **SOUL is never present in the rendered brief.** Spec §5.1 anchors
  SOUL in the *main* agent's ``instructions=`` only. Subagent prompts
  are built from :class:`ClaudeCodeBrief` + ``prompts/delegation_brief.md``
  and have no access to the SOUL file. The unit tests in
  ``tests/test_claude_code_brief.py`` assert this property by scanning
  rendered output for SOUL markers loaded from ``souls/``.
* **Rendering is deterministic.** Same input → byte-identical output.
  No timestamps, no env reads, no randomness. This lets snapshot tests
  and replayed restarts produce identical subagent prompts.
* **Empty lists render cleanly.** ``constraints``, ``success_criteria``,
  and ``relevant_files`` may all be empty; the template renders a
  placeholder line ("_None._") rather than leaving the section blank
  so the subagent still sees the section headers (and can't mistake
  "no constraints" for "section omitted by error").
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai import RunContext

from capo.deps import CapoDeps
from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.tools._subprocess import ReaderHandle, start_reader
from capo.tools._worktree import WorktreeError, create_worktree
from capo.tools.basic import ApprovalRequired

logger = logging.getLogger("capo.tools.claude_code")

# ---------------------------------------------------------------------------
# Template location.
#
# The template ships under the repo's top-level ``prompts/`` directory (next
# to ``system.md``). We resolve it relative to *this* file so behavior is
# stable regardless of CWD — the spawn step (Task #22) and tests both load
# through this helper.
# ---------------------------------------------------------------------------


def _default_template_path() -> Path:
    """Return the absolute path to ``prompts/delegation_brief.md``.

    Computed from this file's location so tests and the runtime spawn step
    agree on the source of truth. The repo layout is::

        capo/tools/claude_code.py   <-- this file
        prompts/delegation_brief.md <-- the template
    """
    return Path(__file__).resolve().parents[2] / "prompts" / "delegation_brief.md"


#: Placeholder string used when a list-valued brief field is empty. Centralized
#: here so the snapshot test and template logic agree on the exact bytes.
EMPTY_LIST_PLACEHOLDER: str = "_None._"


# ---------------------------------------------------------------------------
# The brief model itself.
# ---------------------------------------------------------------------------


class ClaudeCodeBrief(BaseModel):
    """Structured Claude Code delegation request (spec §5.3, §7.3).

    Field shapes mirror the §7.3 class diagram exactly so the JSON we
    persist on the ``delegations`` row (via ``brief_json``) round-trips
    cleanly through Pydantic v2 ``model_dump_json`` / ``model_validate_json``.

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
    constraints:
        Free-form "don't do X" / "must use Y" rules. May be empty.
    success_criteria:
        Verifiable conditions for completion. May be empty.
    relevant_files:
        Hints for the subagent to read first. May be empty.
    create_worktree:
        When ``True`` (default), the spawn step will create an isolated
        ``git worktree`` under ``~/.capo/workspaces/<delegation_id>/``.
        Setting this to ``False`` is only meaningful for non-git
        repositories or explicit override paths.
    model:
        Optional per-delegation model override. ``None`` means "use
        ``config.models.subagents.claude_code``" per §5.9.
    """

    model_config = ConfigDict(
        # Frozen so the same brief can be safely shared between the
        # spawn step and the persistence path without anybody mutating
        # it under the other.
        frozen=True,
        # Reject unknown fields outright. Callers that need to attach
        # extra metadata should add a typed field here rather than smuggle
        # it through `extra="allow"`.
        extra="forbid",
        # Strip whitespace on str fields so a trailing newline in
        # `goal` doesn't break determinism of the rendered output.
        str_strip_whitespace=True,
    )

    goal: str = Field(min_length=1, description="What the subagent should do.")
    repo_path: str = Field(min_length=1, description="Absolute path to target repo.")
    constraints: list[str] = Field(default_factory=list)
    success_criteria: list[str] = Field(default_factory=list)
    relevant_files: list[str] = Field(default_factory=list)
    create_worktree: bool = True
    model: str | None = None


# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------


def _render_bulleted(items: list[str]) -> str:
    """Render ``items`` as a markdown bullet list, or the empty placeholder.

    Empty list → :data:`EMPTY_LIST_PLACEHOLDER`. Otherwise one bullet per
    item, in input order (preserving the caller's intended priority).
    """
    if not items:
        return EMPTY_LIST_PLACEHOLDER
    return "\n".join(f"- {item}" for item in items)


def render_brief(
    brief: ClaudeCodeBrief, *, template_path: Path | None = None
) -> str:
    """Render ``brief`` into the prompt string passed to ``claude -p``.

    The output is **deterministic** for a given ``brief`` and template
    file — no timestamps, no environment reads, no randomness — so the
    snapshot test in ``tests/test_claude_code_brief.py`` can pin exact
    bytes and the DBOS resume path (Phase 3) can reproduce the same
    prompt on restart.

    Args:
        brief: The validated :class:`ClaudeCodeBrief`.
        template_path: Optional override for the template file location.
            Tests inject a tmp-path; production code uses the default
            (``prompts/delegation_brief.md`` relative to this file).

    Returns:
        The fully-substituted prompt. **Never** contains SOUL content;
        SOUL files are not read by this function under any branch.
    """
    path = template_path or _default_template_path()
    template = path.read_text(encoding="utf-8")

    return template.format(
        goal=brief.goal,
        repo_path=brief.repo_path,
        constraints_block=_render_bulleted(brief.constraints),
        success_criteria_block=_render_bulleted(brief.success_criteria),
        relevant_files_block=_render_bulleted(brief.relevant_files),
    )


# ---------------------------------------------------------------------------
# DelegationHandle — the agent-visible return value of delegate_to_claude_code.
# ---------------------------------------------------------------------------


class DelegationHandle(BaseModel):
    """Agent-visible handle returned by :func:`delegate_to_claude_code`.

    Spec §5.3 acceptance criterion: "Returns a ``DelegationHandle`` with
    ``delegation_id``, ``agent="claude-code"``, ``workspace``,
    ``status="running"``, short ``notes``." The agent uses
    :func:`~capo.tools.delegations.check_delegation_status` / friends
    (Task #24) to advance state from there.
    """

    model_config = ConfigDict(frozen=True)

    delegation_id: str = Field(min_length=1)
    agent: Literal["claude-code"] = "claude-code"
    workspace: str = Field(min_length=1)
    status: str = Field(min_length=1)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# delegate_to_claude_code — spawn CC, persist BEFORE yield, return handle.
# ---------------------------------------------------------------------------


#: How long to wait for the in-process subprocess monitor to observe exit
#: when computing the final status. The monitor itself awaits ``process.wait()``
#: with no timeout; this constant is exported so tests can stop the monitor
#: deterministically if needed in the future.
_MONITOR_TASK_NAME_PREFIX: str = "delegate-monitor-"
_READER_TASK_NAME_PREFIX: str = "delegate-reader-"

#: pipe buffer for ``asyncio.create_subprocess_exec``. Per Wave 1 notes the
#: chatty-CC integration test in `tests/test_subprocess.py` proved 2 MiB lets
#: us ``readline()`` on single CC events (which can be ~120 KiB) without
#: hitting :class:`asyncio.LimitOverrunError`. Aligned with §6.1.
_SUBPROCESS_STREAM_LIMIT: int = 2 * 1024 * 1024  # 2 MiB

#: Default session-id capture timeout (spike S-3 §4.3). v2.1.138 observed
#: first-event latency is < 200 ms locally; 30 s is a very generous ceiling.
SESSION_ID_CAPTURE_TIMEOUT_S: float = 30.0


class SessionIdCaptureTimeout(RuntimeError):
    """Raised by :func:`_await_session_id` when the reader did not surface a
    JSON event with a non-empty top-level ``session_id`` within the timeout.

    The monitor wrapper catches this exception, marks the row ``failed``
    with a reason mentioning the timeout, kills the subprocess, and logs
    the orphan delegation for boot-recovery to pick up (Phase 3 / Task #31).
    """


def _utc_now_iso() -> str:
    """ISO-8601 UTC timestamp the ``delegations.started_at`` column accepts.

    SQLite ``TIMESTAMP`` is TEXT; we mirror the format used by
    ``capo.tools._subprocess._iso`` so reader and parent rows compare cleanly.
    """
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _validate_repo_path_in_projects_root(
    repo_path: str, projects_root: Path | None
) -> None:
    """Raise :class:`ApprovalRequired` if ``repo_path`` is outside ``projects_root``.

    Per spec §5.3 / §5.8 — out-of-``projects_root`` delegations must go
    through the (Phase 4) approval workflow. Phase 2 just raises the same
    :class:`ApprovalRequired` exception ``shell_exec`` already uses.
    """
    if projects_root is None:
        # No projects_root configured → no constraint to enforce. (Tests
        # without a configured root can still drive delegations.)
        return
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
        return
    raise ApprovalRequired(
        repo_path,
        f"repo_path {str(resolved_repo)!r} is outside projects_root "
        f"{str(resolved_root)!r}",
    )


def _agents_claude_code_value(deps: CapoDeps, key: str, default: str) -> str:
    """Read ``settings.agents.claude_code.<key>`` with a graceful default.

    ``AgentBinaryConfig`` declares only ``binary`` explicitly; everything
    else (``default_permission_mode``, ``worktree_base_branch``, ...) lives
    in ``model_extra``. We read via ``model_extra`` then fall back to
    ``getattr`` so future explicit fields keep working.
    """
    cc = deps.settings.agents.claude_code
    extra = getattr(cc, "model_extra", None) or {}
    value = extra.get(key) if isinstance(extra, dict) else None
    if value is None:
        value = getattr(cc, key, None)
    if value is None:
        return default
    return str(value)


def _insert_delegation_row(
    conn: sqlite3.Connection,
    *,
    delegation_id: str,
    user_id: str,
    workspace: str,
    pid: int | None,
    brief_json: str,
    status: str,
    parent_thread_id: str | None,
    model: str | None,
    started_at: str,
    summary: str | None = None,
) -> None:
    """INSERT one row into ``delegations`` inside ``BEGIN IMMEDIATE``.

    Synchronous helper — callers wrap it in :func:`asyncio.to_thread` so the
    event loop isn't pinned on SQLite. Goes through the retry helper, so
    callers inherit the §7.3 zero-``SQLITE_BUSY``-to-caller contract.
    """
    with begin_immediate_with_retry(
        conn, operation=f"insert_delegations:{delegation_id}"
    ):
        conn.execute(
            "INSERT INTO delegations ("
            "id, user_id, agent, workspace, pid, session_id_subagent, "
            "brief_json, status, started_at, parent_thread_id, summary, model"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                delegation_id,
                user_id,
                "claude-code",
                workspace,
                pid,
                None,  # session_id_subagent — captured later by Task #23.
                brief_json,
                status,
                started_at,
                parent_thread_id,
                summary,
                model,
            ),
        )


def _update_delegation_status(
    conn: sqlite3.Connection,
    *,
    delegation_id: str,
    status: str,
    ended_at: str | None,
    summary: str | None = None,
) -> None:
    """UPDATE ``delegations`` to terminal ``status`` + optional summary."""
    with begin_immediate_with_retry(
        conn, operation=f"update_delegation_status:{delegation_id}"
    ):
        conn.execute(
            "UPDATE delegations SET status = ?, ended_at = ?, summary = ? "
            "WHERE id = ?",
            (status, ended_at, summary, delegation_id),
        )


def _update_delegation_session_id(
    conn: sqlite3.Connection,
    *,
    delegation_id: str,
    session_id: str,
) -> None:
    """UPDATE ``delegations.session_id_subagent`` for ``delegation_id``.

    Task #23 acceptance criterion: "When the first JSON event yields
    ``session_id_subagent``, UPDATE the row in the same retry-helper write
    path." Goes through :func:`begin_immediate_with_retry` so callers
    inherit zero-``SQLITE_BUSY``-to-caller per §7.3.
    """
    with begin_immediate_with_retry(
        conn, operation=f"update_session_id:{delegation_id}"
    ):
        conn.execute(
            "UPDATE delegations SET session_id_subagent = ? WHERE id = ?",
            (session_id, delegation_id),
        )


def _run_session_id_update(
    db_path: Path,
    delegation_id: str,
    session_id: str,
) -> None:
    """Open a fresh connection in a worker thread, UPDATE, close."""
    conn = open_connection(db_path)
    try:
        _update_delegation_session_id(
            conn, delegation_id=delegation_id, session_id=session_id
        )
    finally:
        conn.close()


def _run_terminal_failure(
    db_path: Path,
    delegation_id: str,
    reason: str,
) -> None:
    """Open a fresh connection, mark ``delegation_id`` failed + ended_at now."""
    conn = open_connection(db_path)
    try:
        _update_delegation_status(
            conn,
            delegation_id=delegation_id,
            status="failed",
            ended_at=_utc_now_iso(),
            summary=reason,
        )
    finally:
        conn.close()


def _extract_session_id(event: dict[str, Any]) -> str | None:
    """Return ``event['session_id']`` when it is a non-empty string.

    Per spike S-3 §4.1: ``session_id`` is a **top-level string field on
    every event** at v2.1.138. The same value appears nested in
    ``assistant``/``user`` events alongside ``message``; the top-level
    field is canonical.
    """
    sid = event.get("session_id")
    if isinstance(sid, str) and sid:
        return sid
    return None


async def _await_session_id(
    reader_handle: ReaderHandle,
    delegation_id: str,
    *,
    db_path: Path,
    timeout_s: float = SESSION_ID_CAPTURE_TIMEOUT_S,
) -> str:
    """Wait for the reader to surface the first JSON event with a session_id.

    Subscribes to :attr:`ReaderHandle.first_event_received`; once set, reads
    :attr:`ReaderHandle.first_event` and extracts the top-level ``session_id``
    per spike S-3 §4.1. The row's ``session_id_subagent`` column is UPDATEd
    inside :func:`begin_immediate_with_retry` (same retry-helper write path
    used for the initial INSERT).

    Args:
        reader_handle: The :class:`ReaderHandle` returned by
            :func:`~capo.tools._subprocess.start_reader`.
        delegation_id: Foreign key on ``delegations.id``.
        db_path: Absolute path to ``state.db``.
        timeout_s: How long to wait before giving up. Default is
            :data:`SESSION_ID_CAPTURE_TIMEOUT_S` (30 s). On v2.1.138 the
            observed first-event latency is < 200 ms.

    Returns:
        The captured ``session_id`` string. Also persisted on the
        ``delegations`` row before this function returns.

    Raises:
        SessionIdCaptureTimeout: The reader's :attr:`first_event_received`
            never fired within ``timeout_s``. The caller (monitor wrapper)
            is responsible for marking the row ``failed`` and killing the
            subprocess.

    Notes
    -----
    The reader's hook is set only when a JSON line is **both** parseable
    **and** carries a non-empty top-level ``session_id`` string. Malformed
    JSON lines (or JSON lines lacking session_id) don't trip the hook —
    the reader keeps emitting until the timeout elapses, which matches
    spike S-3's "tolerate plain-text preamble" requirement (§4.2).
    """
    try:
        await asyncio.wait_for(
            reader_handle.first_event_received.wait(), timeout=timeout_s
        )
    except TimeoutError as exc:
        raise SessionIdCaptureTimeout(
            f"session_id not captured within {timeout_s}s "
            f"(delegation_id={delegation_id})"
        ) from exc

    event = reader_handle.first_event or {}
    session_id = _extract_session_id(event)
    if session_id is None:
        # The reader only sets the hook when session_id is a non-empty
        # string; this branch is defense-in-depth.
        raise SessionIdCaptureTimeout(
            f"reader signalled first event but session_id was missing or "
            f"empty (delegation_id={delegation_id})"
        )

    # Persist on the row through the retry helper (same write path as INSERT).
    await asyncio.to_thread(
        _run_session_id_update, db_path, delegation_id, session_id
    )
    return session_id


async def _spawn_monitor(
    process: Any,
    *,
    delegation_id: str,
    db_path: Path,
    reader_handle: ReaderHandle,
) -> None:
    """Await ``process.wait()`` then mark the row succeeded/failed.

    Phase 2 in-process monitor — narrow on purpose. Phase 3 (Task #35)
    replaces this with the DBOS ``monitor_delegation`` workflow; until then
    we just need terminal-status persistence to satisfy §5.6 acceptance
    criteria and the §9.2 checkpoint gate.

    Behavior:
    - Wait for the child's exit code (no timeout — DBOS will own deadlines).
    - Wait for the reader to drain so ``delegation_output`` is fully flushed
      before we publish the terminal row. ``ReaderHandle.wait`` swallows
      :class:`asyncio.CancelledError`; we wrap it in ``suppress`` for any
      non-cancellation reader fault so a reader bug never strands the row.
    - UPDATE status (``completed`` on rc==0, else ``failed``).
    - On cancellation (Capo shutting down), do **not** update the row —
      Phase 3 boot recovery owns that path.
    """
    try:
        returncode = await process.wait()
    except asyncio.CancelledError:
        raise

    # Drain the reader before publishing the terminal row.
    with contextlib.suppress(Exception):
        await reader_handle.wait()

    status = "completed" if returncode == 0 else "failed"
    summary = None if returncode == 0 else f"exit code {returncode}"
    try:
        await asyncio.to_thread(
            _run_status_update,
            db_path,
            delegation_id,
            status,
            summary,
        )
    except Exception:  # noqa: BLE001 - monitor must not crash unhandled
        logger.exception(
            "delegate_to_claude_code: monitor failed to update terminal "
            "status for delegation_id=%s",
            delegation_id,
        )


def _run_status_update(
    db_path: Path,
    delegation_id: str,
    status: str,
    summary: str | None,
) -> None:
    """Open a fresh connection in the worker thread, update, close.

    Connections aren't shared across threads — the monitor uses its own.
    """
    conn = open_connection(db_path)
    try:
        _update_delegation_status(
            conn,
            delegation_id=delegation_id,
            status=status,
            ended_at=_utc_now_iso(),
            summary=summary,
        )
    finally:
        conn.close()


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

    Mirrors :func:`capo.tools._worktree._cleanup` but reachable from this
    module without exporting the private helper. Used when worktree
    creation succeeded but a *later* setup step (spawn, persist) failed.
    """
    import subprocess

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
    """INSERT a terminal-``failed`` delegations row from a sync worker thread.

    Used when setup fails (worktree, spawn, persist) so there's never an
    orphan workspace on disk without a corresponding DB row — operators can
    grep ``delegations`` for the row and locate the workspace.
    """
    now = _utc_now_iso()
    conn = open_connection(db_path)
    try:
        with begin_immediate_with_retry(
            conn,
            operation=f"insert_failed_delegations:{delegation_id}",
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
                    "claude-code",
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


async def delegate_to_claude_code(
    ctx: RunContext[CapoDeps], brief: ClaudeCodeBrief
) -> DelegationHandle:
    """Delegate a coding task to Claude Code (spec §5.3, §9.2).

    Lifecycle (the **persistence-before-yield** contract):

    1. Generate a fresh ``delegation_id`` (uuid4 hex).
    2. Validate ``brief.repo_path`` is inside ``projects_root`` — raise
       :class:`ApprovalRequired` otherwise. Phase 4 (§5.8) routes that
       exception through an approval workflow; Phase 2 just surfaces it.
    3. Create ``workspaces_root/<delegation_id>/``.
    4. If ``brief.create_worktree``, ``git worktree add`` a fresh branch
       off ``agents.claude_code.worktree_base_branch``. On failure clean up
       the workspace dir, INSERT a ``failed`` row, and re-raise.
    5. Render the CC prompt via :func:`render_brief`.
    6. Build the CC argv (``claude -p ... --output-format stream-json
       --verbose --permission-mode <mode>``; optionally ``--model``).
       NOTE: §7.5 / spike S-3 amended this from ``--output-format json``
       to ``--output-format stream-json --verbose`` — line-by-line JSON
       events are what the reader consumes.
    7. Spawn with ``asyncio.create_subprocess_exec(...)``,
       ``limit=2 MiB`` so the chatty-CC reader can ``readline()`` on big
       events (§6.1).
    8. **Persist BEFORE returning**: INSERT one ``delegations`` row with
       ``status='running'``, ``pid``, ``parent_thread_id`` (from
       ``ctx.deps.thread_id``), ``session_id_subagent=NULL``, ``model``,
       ``brief_json``, ``parent_user_id``. Goes through
       :func:`begin_immediate_with_retry` — zero-``SQLITE_BUSY`` to the
       caller per §7.3.
    9. Start the output reader (Task #21) and an in-process exit-monitor
       asyncio task (Phase 2 placeholder; Phase 3 / Task #35 swaps it for
       DBOS).
    10. Return a :class:`DelegationHandle` describing the running
        delegation.

    Failure handling between steps 4 and 8:
      * Kill the subprocess if it spawned.
      * Best-effort ``cleanup_worktree`` if the worktree was created.
      * INSERT a ``failed`` row with a clear reason (no orphan
        workspaces without DB rows).
      * Re-raise the underlying exception with ``__cause__`` chained.

    Args:
        ctx: Pydantic AI :class:`RunContext`. Provides ``ctx.deps`` (a
            :class:`~capo.deps.CapoDeps`) carrying ``settings``,
            ``user_id``, ``thread_id``, and the configured roots.
        brief: Validated :class:`ClaudeCodeBrief` describing the
            delegation. The same brief is JSON-serialized into the
            ``brief_json`` column so Phase 3 restart-resume can
            reconstruct it.

    Returns:
        :class:`DelegationHandle` with ``status='running'``. The agent
        polls :func:`check_delegation_status` (Task #24) to advance state.

    Raises:
        ApprovalRequired: ``brief.repo_path`` is outside
            ``ctx.deps.projects_root``.
        FileNotFoundError: ``claude`` binary not on PATH. A row is INSERTed
            with ``status='failed'`` and ``summary="claude binary not found
            on PATH"`` before the exception is re-raised, so the operator
            can find the failed delegation in ``state.db``.
        WorktreeError: Subclass thereof when worktree creation fails — the
            row is INSERTed as ``failed`` first.
    """
    deps: CapoDeps = ctx.deps
    settings = deps.settings
    db_path: Path = settings.paths.db_path
    workspaces_root: Path | None = deps.workspaces_root or (
        settings.paths.workspaces_root
    )
    if workspaces_root is None:
        raise ValueError(
            "delegate_to_claude_code: workspaces_root is not configured on "
            "deps or settings.paths"
        )

    # --- 1. delegation_id ----------------------------------------------------
    delegation_id = uuid.uuid4().hex

    # --- 2. repo_path scope check (raises ApprovalRequired) ------------------
    _validate_repo_path_in_projects_root(brief.repo_path, deps.projects_root)

    # JSON snapshot of the brief — same shape as Phase 3 restart-resume will
    # consume, so a future DBOS workflow can rehydrate without us changing
    # anything here.
    brief_json = brief.model_dump_json()
    user_id = deps.user_id or "unknown"
    parent_thread_id = deps.thread_id
    model = brief.model or _agents_claude_code_value(
        deps, "model", settings.models.subagents.claude_code
    )
    permission_mode = _agents_claude_code_value(
        deps, "default_permission_mode", "acceptEdits"
    )
    base_branch = _agents_claude_code_value(
        deps, "worktree_base_branch", "main"
    )

    # --- 3. workspace dir ----------------------------------------------------
    workspace_dir = workspaces_root / delegation_id
    workspace_str = os.fspath(workspace_dir)
    try:
        workspaces_root.mkdir(parents=True, exist_ok=True)
        # Don't pre-create workspace_dir when we'll ask `git worktree add` to
        # create it (git refuses if the target already exists). Create it
        # ourselves only when create_worktree=False.
        if not brief.create_worktree:
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

    # --- 4. worktree (optional) ---------------------------------------------
    worktree_created = False
    if brief.create_worktree:
        try:
            await asyncio.to_thread(
                create_worktree,
                brief.repo_path,
                workspace_str,
                delegation_id,
                base_branch,
            )
            worktree_created = True
        except WorktreeError as exc:
            # Clean up anything left behind, then INSERT failed row.
            with contextlib.suppress(Exception):
                if workspace_dir.exists():
                    import shutil

                    shutil.rmtree(workspace_dir, ignore_errors=True)
            reason = f"worktree creation failed: {exc.__class__.__name__}: {exc}"
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

    # --- 5. render prompt ----------------------------------------------------
    try:
        rendered_prompt = render_brief(brief)
    except Exception as exc:
        # Template missing / malformed — clean up worktree if we made one.
        if worktree_created:
            _best_effort_cleanup_worktree(
                brief.repo_path, workspace_dir, delegation_id
            )
        with contextlib.suppress(Exception):
            if workspace_dir.exists():
                import shutil

                shutil.rmtree(workspace_dir, ignore_errors=True)
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason=f"render_brief failed: {exc!r}",
        )
        raise

    # --- 6. argv -------------------------------------------------------------
    binary = settings.agents.claude_code.binary or "claude"
    argv: list[str] = [
        binary,
        "-p",
        rendered_prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        permission_mode,
    ]
    if model:
        argv.extend(["--model", model])

    # --- 7. spawn ------------------------------------------------------------
    process: Any | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=workspace_str,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_SUBPROCESS_STREAM_LIMIT,
        )
    except FileNotFoundError:
        # `claude` binary missing — defense in depth alongside the
        # spec §11.2 boot pre-check (Task #62).
        if worktree_created:
            _best_effort_cleanup_worktree(
                brief.repo_path, workspace_dir, delegation_id
            )
        with contextlib.suppress(Exception):
            if workspace_dir.exists():
                import shutil

                shutil.rmtree(workspace_dir, ignore_errors=True)
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason="claude binary not found on PATH",
        )
        raise
    except (OSError, ValueError) as exc:
        if worktree_created:
            _best_effort_cleanup_worktree(
                brief.repo_path, workspace_dir, delegation_id
            )
        with contextlib.suppress(Exception):
            if workspace_dir.exists():
                import shutil

                shutil.rmtree(workspace_dir, ignore_errors=True)
        await asyncio.to_thread(
            _insert_failed_row,
            db_path,
            delegation_id=delegation_id,
            user_id=user_id,
            workspace=workspace_str,
            brief_json=brief_json,
            parent_thread_id=parent_thread_id,
            model=model,
            reason=f"subprocess spawn failed: {exc.__class__.__name__}: {exc}",
        )
        raise

    pid = getattr(process, "pid", None)

    # --- 8. PERSIST BEFORE YIELD --------------------------------------------
    # This is the contract that gives Phase 3 restart-recovery a hook: even
    # if Capo dies the next millisecond, the row exists.
    started_at = _utc_now_iso()
    try:

        def _do_insert() -> None:
            conn = open_connection(db_path)
            try:
                _insert_delegation_row(
                    conn,
                    delegation_id=delegation_id,
                    user_id=user_id,
                    workspace=workspace_str,
                    pid=pid,
                    brief_json=brief_json,
                    status="running",
                    parent_thread_id=parent_thread_id,
                    model=model,
                    started_at=started_at,
                )
            finally:
                conn.close()

        await asyncio.to_thread(_do_insert)
    except Exception as exc:
        # Persist failed: kill the child, clean worktree, record failed row
        # so there's no orphan workspace on disk.
        await _terminate_process(process)
        if worktree_created:
            _best_effort_cleanup_worktree(
                brief.repo_path, workspace_dir, delegation_id
            )
        with contextlib.suppress(Exception):
            if workspace_dir.exists():
                import shutil

                shutil.rmtree(workspace_dir, ignore_errors=True)
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
        raise

    # --- 9. start reader (Task #21) -----------------------------------------
    # Reader needs its own sqlite connection. Open it now; the reader owns it
    # for the lifetime of the drain. It is closed by the monitor task wrapper
    # after the reader exits.
    reader_conn = open_connection(db_path)
    try:
        reader_handle = start_reader(
            delegation_id, process, store=reader_conn
        )
    except Exception as exc:
        # Reader spawn fault — close conn, kill child, but DO NOT touch the
        # delegations row state here. Phase 3 boot recovery will mark it
        # failed; Phase 2 just surfaces the error.
        reader_conn.close()
        await _terminate_process(process)
        logger.exception(
            "delegate_to_claude_code: reader spawn failed for %s: %s",
            delegation_id,
            exc,
        )
        # Still try to mark the row failed so we don't leave a dangling
        # 'running' row pointing at a dead pid.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                _run_status_update,
                db_path,
                delegation_id,
                "failed",
                f"reader spawn failed: {exc.__class__.__name__}: {exc}",
            )
        raise

    # --- 10. monitor task ---------------------------------------------------
    async def _monitor_wrapper() -> None:
        try:
            # Task #23: capture session_id from the first CC JSON event
            # BEFORE the process-exit monitor takes over. Spec §5.3:
            # "Hand off to monitor only after `session_id_subagent` is
            # durable." On timeout we mark the row failed, kill the
            # subprocess, and log the orphan delegation. Phase 3 boot
            # recovery (Task #31) will pick up any stragglers.
            try:
                await _await_session_id(
                    reader_handle,
                    delegation_id,
                    db_path=db_path,
                    timeout_s=SESSION_ID_CAPTURE_TIMEOUT_S,
                )
            except SessionIdCaptureTimeout as exc:
                logger.error(
                    "orphan delegation: %s — %s", delegation_id, exc
                )
                # Best-effort kill so the subprocess doesn't keep running.
                await _terminate_process(process)
                # Drain the reader so any pending output is persisted.
                with contextlib.suppress(Exception):
                    await reader_handle.wait()
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(
                        _run_terminal_failure,
                        db_path,
                        delegation_id,
                        str(exc),
                    )
                return  # do not proceed to _spawn_monitor

            await _spawn_monitor(
                process,
                delegation_id=delegation_id,
                db_path=db_path,
                reader_handle=reader_handle,
            )
        finally:
            with contextlib.suppress(Exception):
                reader_conn.close()

    loop = asyncio.get_event_loop()
    monitor_task = loop.create_task(
        _monitor_wrapper(),
        name=f"{_MONITOR_TASK_NAME_PREFIX}{delegation_id}",
    )
    # Suppress 'task was destroyed but pending' on shutdown for the rare
    # case where the agent loop exits while a delegation is still alive.
    monitor_task.add_done_callback(lambda t: _log_monitor_exit(delegation_id, t))

    notes = [
        f"agent=claude-code model={model}",
        f"workspace={workspace_str}",
    ]
    if worktree_created:
        notes.append(f"worktree branch={delegation_id} base={base_branch}")

    return DelegationHandle(
        delegation_id=delegation_id,
        agent="claude-code",
        workspace=workspace_str,
        status="running",
        notes=notes,
    )


def _log_monitor_exit(delegation_id: str, task: asyncio.Task[Any]) -> None:
    """Best-effort log when the monitor exits — used as a done_callback.

    On cancellation we say nothing (Capo is shutting down). On exception we
    log at error level so the operator sees it. Never raises.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    logger.error(
        "delegate_to_claude_code monitor exited with error for "
        "delegation_id=%s: %r",
        delegation_id,
        exc,
    )


__all__ = [
    "EMPTY_LIST_PLACEHOLDER",
    "SESSION_ID_CAPTURE_TIMEOUT_S",
    "ApprovalRequired",
    "ClaudeCodeBrief",
    "DelegationHandle",
    "SessionIdCaptureTimeout",
    "delegate_to_claude_code",
    "render_brief",
]

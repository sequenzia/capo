"""Git worktree creator helper used by ``delegate_to_claude_code``.

This module is **private** to :mod:`capo.tools` (note the leading underscore):
it exists to give the CC delegation path a single, well-tested entry point
for spinning up an isolated worktree per delegation.

Design contract (spec §5.3 / §9.2)
---------------------------------

* :func:`create_worktree` runs ``git worktree add <workspace> -b
  <branch_name> <base_branch>`` via ``subprocess.run([...], shell=False)``.
* **Pre-flight validation runs before any git invocation** so we fail fast
  with structured errors rather than letting git produce its own less
  helpful messages:

  - ``repo_path`` must be (or live inside) a directory containing a
    ``.git`` entry — else :class:`NotAGitRepo`.
  - ``workspace`` must not already exist — else
    :class:`WorkspaceAlreadyExists`.

* On ANY failure during ``git worktree add`` we run a best-effort cleanup
  (``git worktree remove --force <workspace>`` AND ``git branch -D
  <branch_name>``) and re-raise. Cleanup itself never raises — its
  stderr/stdout is folded into the original error as ``__notes__`` for
  debugging.
* Git's stderr is always captured and surfaced verbatim on the raised
  exception so callers can show operators what actually went wrong.
* Errors are **raised** (not returned). The calling tool layer
  (``capo/tools/claude_code.py``) translates raised exceptions into the
  user-visible structured ``DelegationError`` per §5.3.
"""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger("capo.tools.worktree")


# ---------------------------------------------------------------------------
# Custom error types — all derive from :class:`WorktreeError` so callers
# can ``except WorktreeError`` to catch any failure from this module.
# ---------------------------------------------------------------------------


class WorktreeError(Exception):
    """Base class for any failure from :func:`create_worktree`.

    Subclasses preserve the original git stderr (when applicable) in
    ``self.stderr`` so the calling tool can echo it to the operator.
    """

    def __init__(self, message: str, *, stderr: str | None = None) -> None:
        super().__init__(message)
        self.stderr: str | None = stderr


class NotAGitRepo(WorktreeError):
    """``repo_path`` is not a git repository (no ``.git`` dir or file found)."""


class WorkspaceAlreadyExists(WorktreeError):
    """``workspace`` already exists; refusing to clobber it."""


class BaseBranchMissing(WorktreeError):
    """``base_branch`` does not exist in the repo at ``repo_path``.

    The error message includes a hint to override
    ``agents.claude_code.worktree_base_branch`` in ``config.toml``.
    """


class WorktreeCreationFailed(WorktreeError):
    """``git worktree add`` failed for some other reason (disk full, etc).

    Cleanup has already been attempted by the time this is raised.
    """


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Hard timeout for any single git invocation. Worktree creation is fast in
#: practice; if git hangs for this long, something is very wrong.
_GIT_TIMEOUT_S: float = 60.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_git_repo(repo_path: Path) -> bool:
    """Return True if ``repo_path`` (or an ancestor) contains ``.git``.

    Walks up the directory tree so callers can pass a path *inside* the
    working tree (matching what ``git`` itself accepts). The ``.git``
    entry may be a directory (normal repo) or a file (worktrees + submodules
    use a ``.git`` file pointing at the gitdir).
    """
    try:
        resolved = repo_path.resolve(strict=True)
    except (FileNotFoundError, OSError):
        return False
    if not resolved.is_dir():
        return False
    for candidate in (resolved, *resolved.parents):
        dot_git = candidate / ".git"
        if dot_git.exists():
            return True
    return False


def _run_git(
    args: list[str], *, cwd: Path, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run a git command with hardened defaults.

    ``shell=False`` is mandatory (per spec §5.2 / §9.1 — no shell metachar
    injection surface). Output is captured as text so callers can fold
    stderr into raised exceptions.
    """
    return subprocess.run(  # noqa: S603 - shell=False, args is a list literal
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=check,
        capture_output=True,
        text=True,
        timeout=_GIT_TIMEOUT_S,
    )


def _cleanup(repo_path: Path, workspace: Path, branch_name: str) -> str:
    """Best-effort cleanup of a half-created worktree + branch.

    Never raises — all errors are collected and returned as a single
    multi-line string so the caller can attach them as ``__notes__`` on
    the originally-raised exception. The two cleanup steps run
    independently: branch deletion is attempted even if worktree removal
    fails (and vice-versa).
    """
    notes: list[str] = []

    # 1. Remove the worktree (force, in case it's in a partial state).
    try:
        result = _run_git(
            ["worktree", "remove", "--force", str(workspace)],
            cwd=repo_path,
            check=False,
        )
        if result.returncode != 0:
            notes.append(
                f"cleanup: 'git worktree remove --force {workspace}' "
                f"exited {result.returncode}: {result.stderr.strip()}"
            )
    except (subprocess.TimeoutExpired, OSError) as exc:
        notes.append(f"cleanup: worktree remove failed: {exc!r}")

    # 2. Delete the branch (independent of step 1 — best-effort).
    try:
        result = _run_git(
            ["branch", "-D", branch_name],
            cwd=repo_path,
            check=False,
        )
        if result.returncode != 0:
            # Branch may not have been created at all; that's fine.
            notes.append(
                f"cleanup: 'git branch -D {branch_name}' exited "
                f"{result.returncode}: {result.stderr.strip()}"
            )
    except (subprocess.TimeoutExpired, OSError) as exc:
        notes.append(f"cleanup: branch delete failed: {exc!r}")

    # 3. Belt-and-suspenders: if `git worktree remove` left the directory
    # behind (rare but possible if it was never registered), nudge git
    # to forget about it via ``worktree prune``. Ignore errors.
    with contextlib.suppress(subprocess.TimeoutExpired, OSError):
        _run_git(["worktree", "prune"], cwd=repo_path, check=False)

    return "\n".join(notes)


def _base_branch_exists(repo_path: Path, base_branch: str) -> bool:
    """Return True if ``base_branch`` resolves to a ref in ``repo_path``.

    Uses ``git rev-parse --verify`` which exits non-zero (without writing
    anything to stdout) for missing refs.
    """
    try:
        result = _run_git(
            ["rev-parse", "--verify", "--quiet", f"refs/heads/{base_branch}"],
            cwd=repo_path,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_worktree(
    repo_path: str,
    workspace: str,
    branch_name: str,
    base_branch: str,
) -> str:
    """Create a fresh git worktree at ``workspace`` off ``base_branch``.

    Equivalent to::

        git -C <repo_path> worktree add <workspace> -b <branch_name> <base_branch>

    plus pre-flight validation and best-effort cleanup-on-failure.

    Args:
        repo_path: Absolute path to (or inside) the source git repository.
        workspace: Absolute path where the worktree should be created.
            Must NOT already exist.
        branch_name: Name of the new branch to create at ``workspace``.
            Typically the ``delegation_id`` per spec §5.3.
        base_branch: Existing branch to base the new branch on. Typically
            ``agents.claude_code.worktree_base_branch`` from config
            (default ``main``).

    Returns:
        The absolute path of the created workspace (echoes ``workspace``
        as a normalized string) on success.

    Raises:
        NotAGitRepo: ``repo_path`` is not (and is not inside) a git repo.
        WorkspaceAlreadyExists: ``workspace`` already exists on disk.
        BaseBranchMissing: ``base_branch`` does not exist in the repo;
            message suggests overriding
            ``agents.claude_code.worktree_base_branch`` in ``config.toml``.
        WorktreeCreationFailed: any other failure during ``git worktree
            add``. Cleanup of partial state has already been attempted;
            git's stderr is preserved on ``exc.stderr``.
    """
    repo = Path(repo_path)
    ws = Path(workspace)

    # --- Pre-flight validation ------------------------------------------
    if not _is_git_repo(repo):
        raise NotAGitRepo(
            f"repo_path is not a git repository (no .git dir found at "
            f"or above {repo_path!r}). Pass a valid repo path, or set "
            f"create_worktree=False on the brief."
        )

    if ws.exists():
        raise WorkspaceAlreadyExists(
            f"workspace path already exists: {workspace!r}. Refusing to "
            f"clobber. Pick a fresh path or remove the existing one first."
        )

    if not _base_branch_exists(repo, base_branch):
        raise BaseBranchMissing(
            f"base branch {base_branch!r} does not exist in repo at "
            f"{repo_path!r}. Override "
            f"'agents.claude_code.worktree_base_branch' in config.toml to "
            f"point at an existing branch (e.g. 'master' or 'develop')."
        )

    # Ensure the parent of the workspace exists; git worktree add won't
    # create intermediate directories.
    try:
        ws.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise WorktreeCreationFailed(
            f"could not create parent directory for workspace "
            f"{workspace!r}: {exc!r}",
            stderr=None,
        ) from exc

    # --- The actual worktree add ----------------------------------------
    try:
        _run_git(
            [
                "worktree",
                "add",
                str(ws),
                "-b",
                branch_name,
                base_branch,
            ],
            cwd=repo,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        cleanup_notes = _cleanup(repo, ws, branch_name)
        err = WorktreeCreationFailed(
            f"git worktree add failed (exit={exc.returncode}): {stderr}",
            stderr=stderr,
        )
        if cleanup_notes:
            err.add_note(cleanup_notes)
        logger.warning(
            "create_worktree: git worktree add failed; cleanup ran. "
            "workspace=%s branch=%s base=%s stderr=%s",
            workspace,
            branch_name,
            base_branch,
            stderr,
        )
        raise err from exc
    except (subprocess.TimeoutExpired, OSError) as exc:
        stderr = ""
        if isinstance(exc, subprocess.TimeoutExpired) and exc.stderr:
            stderr = (
                exc.stderr.decode("utf-8", errors="replace")
                if isinstance(exc.stderr, bytes)
                else exc.stderr
            ).strip()
        cleanup_notes = _cleanup(repo, ws, branch_name)
        err = WorktreeCreationFailed(
            f"git worktree add did not complete: {exc!r}",
            stderr=stderr or None,
        )
        if cleanup_notes:
            err.add_note(cleanup_notes)
        raise err from exc

    return os.fspath(ws)


__all__ = [
    "BaseBranchMissing",
    "NotAGitRepo",
    "WorkspaceAlreadyExists",
    "WorktreeCreationFailed",
    "WorktreeError",
    "create_worktree",
]

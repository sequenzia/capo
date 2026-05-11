"""Unit + integration tests for :mod:`capo.tools._worktree` (Task #20).

Covers acceptance criteria from spec §5.3 / §9.2:

Functional
* Happy path: :func:`create_worktree` creates the worktree, returns its
  path, and a follow-up ``git worktree remove`` cleans it up.
* ``repo_path`` not a git repo → :class:`NotAGitRepo`.
* ``base_branch`` doesn't exist → :class:`BaseBranchMissing` with a hint
  to override ``agents.claude_code.worktree_base_branch``.
* ``workspace`` already exists → :class:`WorkspaceAlreadyExists`.

Error handling
* Git stderr is captured and surfaced on the raised exception.

Integration
* Cleanup actually runs (worktree directory removed AND branch deleted)
  when ``git worktree add`` fails mid-way.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from capo.tools._worktree import (
    BaseBranchMissing,
    NotAGitRepo,
    WorkspaceAlreadyExists,
    WorktreeCreationFailed,
    create_worktree,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run ``git`` with hardened defaults; raises on non-zero exit."""
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    """Create a real git repo with a ``main`` branch containing one commit.

    Returns the repo's absolute path. Configures user.name/user.email so
    ``git commit`` works without ambient git config bleeding in.
    """
    repo = tmp_path / "src"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@capo.local", cwd=repo)
    _git("config", "user.name", "Capo Test", cwd=repo)
    (repo / "README.md").write_text("hello\n")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-q", "-m", "initial", cwd=repo)
    return repo


def _branch_exists(repo: Path, name: str) -> bool:
    """Return True if local branch ``name`` exists in ``repo``."""
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{name}"],
        cwd=str(repo),
        shell=False,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _worktree_registered(repo: Path, path: Path) -> bool:
    """Return True if ``path`` is currently registered as a worktree of ``repo``."""
    result = subprocess.run(
        ["git", "worktree", "list", "--porcelain"],
        cwd=str(repo),
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )
    target = str(path.resolve())
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            registered = line.removeprefix("worktree ").strip()
            if Path(registered).resolve() == Path(target):
                return True
    return False


# ---------------------------------------------------------------------------
# Functional tests
# ---------------------------------------------------------------------------


def test_happy_path_creates_and_cleans_up(git_repo: Path, tmp_path: Path) -> None:
    """``create_worktree`` creates the directory + branch; cleanup removes them."""
    workspace = tmp_path / "wt-happy"
    branch = "delegation-happy"

    result = create_worktree(
        repo_path=str(git_repo),
        workspace=str(workspace),
        branch_name=branch,
        base_branch="main",
    )

    assert Path(result) == workspace
    assert workspace.is_dir()
    assert (workspace / "README.md").exists()
    assert _worktree_registered(git_repo, workspace)
    assert _branch_exists(git_repo, branch)

    # Cleanup is the caller's responsibility on success — verify the same
    # primitives the helper uses internally remove things cleanly.
    _git("worktree", "remove", "--force", str(workspace), cwd=git_repo)
    _git("branch", "-D", branch, cwd=git_repo)
    assert not workspace.exists()
    assert not _branch_exists(git_repo, branch)


def test_non_git_repo_raises_not_a_git_repo(tmp_path: Path) -> None:
    """A path with no ``.git`` (or ancestor with one) raises :class:`NotAGitRepo`."""
    plain = tmp_path / "plain"
    plain.mkdir()
    workspace = tmp_path / "wt"

    with pytest.raises(NotAGitRepo) as excinfo:
        create_worktree(
            repo_path=str(plain),
            workspace=str(workspace),
            branch_name="b1",
            base_branch="main",
        )

    assert "not a git repository" in str(excinfo.value)
    # No partial state left behind.
    assert not workspace.exists()


def test_missing_base_branch_raises_clear_error(
    git_repo: Path, tmp_path: Path
) -> None:
    """A non-existent ``base_branch`` raises with a config-override hint."""
    workspace = tmp_path / "wt"

    with pytest.raises(BaseBranchMissing) as excinfo:
        create_worktree(
            repo_path=str(git_repo),
            workspace=str(workspace),
            branch_name="b1",
            base_branch="does-not-exist",
        )

    msg = str(excinfo.value)
    assert "does-not-exist" in msg
    assert "worktree_base_branch" in msg
    # Pre-flight failed → nothing was created.
    assert not workspace.exists()
    assert not _branch_exists(git_repo, "b1")


def test_workspace_already_exists_raises(git_repo: Path, tmp_path: Path) -> None:
    """A pre-existing workspace path raises :class:`WorkspaceAlreadyExists`."""
    workspace = tmp_path / "wt-exists"
    workspace.mkdir()

    with pytest.raises(WorkspaceAlreadyExists) as excinfo:
        create_worktree(
            repo_path=str(git_repo),
            workspace=str(workspace),
            branch_name="b1",
            base_branch="main",
        )

    assert "already exists" in str(excinfo.value)
    # Untouched.
    assert workspace.exists()
    assert not _branch_exists(git_repo, "b1")


def test_repo_path_inside_worktree_is_accepted(
    git_repo: Path, tmp_path: Path
) -> None:
    """Passing a path *inside* the repo (not the root) still works — git CLI
    does the same ``.git``-discovery walk, and so do we."""
    nested = git_repo / "subdir"
    nested.mkdir()
    workspace = tmp_path / "wt-nested"

    result = create_worktree(
        repo_path=str(nested),
        workspace=str(workspace),
        branch_name="nested-branch",
        base_branch="main",
    )

    assert Path(result) == workspace
    assert workspace.is_dir()

    # Tidy up so pytest's tmp_path teardown isn't surprised.
    _git("worktree", "remove", "--force", str(workspace), cwd=git_repo)
    _git("branch", "-D", "nested-branch", cwd=git_repo)


# ---------------------------------------------------------------------------
# Error-handling tests
# ---------------------------------------------------------------------------


def test_git_stderr_is_surfaced_on_failure(
    git_repo: Path, tmp_path: Path
) -> None:
    """Force a ``git worktree add`` failure by re-using an existing branch
    name. The branch already exists, so ``-b <branch>`` triggers an error;
    git's stderr must appear on the raised exception."""
    # Create a branch up front so ``-b <branch>`` clashes with it.
    _git("branch", "preexisting", cwd=git_repo)

    workspace = tmp_path / "wt-stderr"

    with pytest.raises(WorktreeCreationFailed) as excinfo:
        create_worktree(
            repo_path=str(git_repo),
            workspace=str(workspace),
            branch_name="preexisting",
            base_branch="main",
        )

    err = excinfo.value
    # Stderr is preserved verbatim (lowercased for portability across git
    # versions which vary in capitalization).
    assert err.stderr is not None
    assert "preexisting" in err.stderr.lower()
    assert "preexisting" in str(err).lower()


# ---------------------------------------------------------------------------
# Integration: cleanup-on-failure
# ---------------------------------------------------------------------------


def test_cleanup_runs_when_add_fails_partway(
    git_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ``git worktree add`` partially succeeds and then fails, the
    helper's cleanup must remove the partial worktree directory AND
    delete the branch.

    We simulate "partial success then failure" by monkey-patching the
    helper's ``_run_git`` to:

    1. Forward the worktree-add invocation to real git (so the directory
       and branch are created),
    2. Then raise :class:`subprocess.CalledProcessError` as if the final
       step of ``git worktree add`` had failed.
    """
    import capo.tools._worktree as wt_mod

    real_run_git = wt_mod._run_git
    saw_add = {"hit": False}

    def fake_run_git(args, *, cwd, check=True):  # type: ignore[no-untyped-def]
        # Let pre-flight calls (rev-parse) pass through untouched.
        if args[:2] != ["worktree", "add"]:
            return real_run_git(args, cwd=cwd, check=check)

        # Run the real add so we actually create the partial state...
        real_run_git(args, cwd=cwd, check=check)
        saw_add["hit"] = True
        # ...then pretend it failed at the very last moment.
        raise subprocess.CalledProcessError(
            returncode=128,
            cmd=["git", *args],
            output="",
            stderr="simulated post-create failure",
        )

    monkeypatch.setattr(wt_mod, "_run_git", fake_run_git)

    workspace = tmp_path / "wt-cleanup"
    branch = "delegation-cleanup"

    with pytest.raises(WorktreeCreationFailed) as excinfo:
        create_worktree(
            repo_path=str(git_repo),
            workspace=str(workspace),
            branch_name=branch,
            base_branch="main",
        )

    # Sanity: our fake actually triggered, and the partial state was real.
    assert saw_add["hit"]
    assert "simulated post-create failure" in str(excinfo.value)

    # The crucial assertions: cleanup undid both halves of the partial
    # state. Use the real git to check, NOT the patched helper.
    assert not _worktree_registered(git_repo, workspace), (
        "worktree should have been deregistered by cleanup"
    )
    assert not workspace.exists(), (
        "worktree directory should have been removed by cleanup"
    )
    assert not _branch_exists(git_repo, branch), (
        "branch should have been deleted by cleanup"
    )

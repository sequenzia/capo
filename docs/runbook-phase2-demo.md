# Phase 2 Manual Demo Runbook

Spec reference: `internal/specs/capo-SPEC.md` §9.2 (Phase 2 Checkpoint Gate).

This runbook is the **manual gate** for the Phase 2 checkpoint. All
automated criteria (§9.2) are exercised by `uv run pytest -q` and are
already green; this procedure proves the new Claude Code (CC) delegation
pipeline end-to-end against a real `claude` binary and a real AMC server.
Run it once before declaring Phase 2 done.

The Phase 1 runbook (`docs/runbook-phase1-demo.md`) is a prerequisite —
this gate inherits the AMC/auth/configuration steps from Phase 1 and adds
the CC-specific pieces.

## Prerequisites

- Everything from Phase 1's runbook (working AMC server, `.env` with
  `AMC_WEBHOOK_SECRET` + `AMC_BEARER_TOKEN`, an Anthropic API key, an AMC
  sender id mapped to a Capo `user_id`).
- The `claude` CLI installed and on `$PATH`. **The §12.1 pinned minimum
  is `2.1.138`** (from spike S-3). Verify with:

  ```bash
  claude --version
  ```

  Anything older needs an upgrade — Capo will fail-fast at boot on
  older versions once the §12.1 enforcement lands.
- A real git repo on disk that lives **inside** `paths.projects_root`.
  Any small repo works — a clone of `capo` itself is fine. Capo refuses
  to delegate against paths outside `projects_root` (it raises
  `ApprovalRequired` instead — that's §5.8's path-scoping guard).

## 1. Confirm Phase 2 config keys

In addition to the Phase 1 keys, `config.toml` must have:

```toml
[paths]
# Per-delegation worktrees land here.
workspaces_root = "~/.capo/workspaces"
# Capo refuses to delegate to repos outside this prefix.
projects_root   = "~/code"

[agents.claude_code]
binary                  = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch    = "main"
# Optional: pin a CC subagent model. Falls back to models.subagents.claude_code.
# model = "anthropic:claude-sonnet-4-6"

[shell]
# shell_exec allowlist. Defaults are conservative; add tools as needed.
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
# Hard cap on parallel delegations.
max_delegations = 3
```

The Phase 1 db migration already created the `delegations` and
`delegation_output` tables — no extra migration step is needed for the
demo.

## 2. Start Capo

```bash
uv run capo --config /absolute/path/to/config.toml
```

Same boot sequence as Phase 1. Capo additionally:

- Validates `agents.claude_code.binary` resolves on `$PATH` (boot-time
  `shutil.which`).
- Registers the Phase 2 tools (`shell_exec`, `delegate_to_claude_code`,
  `check_delegation_status`, `get_delegation_output`, `kill_delegation`,
  `list_delegations`) — see `capo.tools.register_phase2_tools`.

If `claude` is missing from `$PATH`, boot still succeeds (the binary is
resolved on first delegation, not at boot) — but the first delegation
will surface `FileNotFoundError` and write a `status='failed'` row with
"claude binary not found on PATH" in `summary`.

## 3. Trigger a delegation

From your mapped AMC sender, text Capo something like:

> delegate to claude code: refactor the README in /Users/me/code/some-repo

The agent loop will:

1. Materialize the conversation history.
2. Resolve `repo_path` against `projects_root`.
3. Call `delegate_to_claude_code(brief)`.
4. The tool creates a fresh git worktree under
   `workspaces_root/<delegation_id>/`, spawns `claude -p <prompt>
   --output-format stream-json --verbose --permission-mode acceptEdits
   --model <subagent_model>`, **persists the row before yielding**, and
   returns a `DelegationHandle`.
5. Capo replies with a one-liner that includes the `delegation_id`.

Expected reply latency: **a few seconds** (the spawn + INSERT is fast;
the user-visible latency is mostly the agent's own reasoning loop).

## 4. Poll status

From the same AMC channel, text:

> what's the status of <delegation_id>

Capo will call `check_delegation_status(delegation_id)` and reply with
the `status` (`running` / `completed` / `failed` / `killed` /
`pending_approval`), `runtime_seconds`, and the `summary_one_line` once
the run finishes.

You can also peek directly:

```bash
sqlite3 ~/.capo/state.db \
  "SELECT id, agent, status, started_at, ended_at, summary FROM delegations
   ORDER BY started_at DESC LIMIT 5"
```

## 5. Inspect the trace

Useful stderr log tags for Phase 2:

- `capo.tools.claude_code` — spawn, monitor, session-id capture, cleanup.
- `capo.tools._subprocess` — reader stats (`stdout_bytes`, `event_rows`,
  `flushes`, `parse_errors`).
- `capo.tools.delegations` — kill, status, list.
- `orphan delegation: <id>` — emitted by the §5.6 monitor if cleanup
  hits an unrecoverable state. Should be absent on healthy runs;
  Phase 3 (#31) will scan these for boot-recovery.

And direct DB inspection:

```bash
# All events from a delegation, oldest first.
sqlite3 ~/.capo/state.db \
  "SELECT ts, stream, substr(chunk,1,80) FROM delegation_output
   WHERE delegation_id = '<id>' ORDER BY id"
```

## 6. (Optional) Cancel a long-running delegation

The user-visible cancel path is `kill_delegation(<id>, reason)`. In
Phase 2 this **always** raises `ApprovalRequired` — the dispatcher's
fallback reply is the locked stub
`"That action (kill_delegation(<id>)) needs approval. Reason: <reason>.
Full approval flow lands in Phase 4."` (see
`capo.transport.dispatcher.format_approval_required_reply`).

The forced-kill path (`_kill_delegation_forced`) is integration-tested
(`tests/test_phase2_checkpoint.py::test_kill_forced_terminates_live_cc`)
but **not** wired to a user gesture until Phase 4 #41.

## 7. Cleanup

- Workspaces live at `~/.capo/workspaces/<delegation_id>/`. They are
  the delegation's git worktrees. Phase 2 does **not** GC them on
  successful completion — that's a Phase 5 housekeeping deliverable.
  Manual cleanup if your disk fills up:

  ```bash
  cd /Users/me/code/some-repo
  git worktree remove --force ~/.capo/workspaces/<delegation_id>
  git branch -D capo/<delegation_id>   # if you want the branch gone too
  ```

  Or, for blast-radius cleanup:

  ```bash
  find ~/.capo/workspaces -mindepth 1 -maxdepth 1 -type d -print0 |
    xargs -0 -I{} sh -c 'git -C $(cat {}/.git 2>/dev/null | sed -E "s|.*gitdir: (.*)/worktrees/.*|\1|") worktree remove --force {}'
  ```

- `delegation_output` retention defaults to 7 days (configurable via
  `[retention]`). Phase 5 will add the actual GC job.

## Troubleshooting

- **`ApprovalRequired: repo_path outside projects_root`** — your target
  repo isn't under `paths.projects_root`. Move it, symlink it, or
  widen `projects_root`.
- **`FileNotFoundError: claude binary not found on PATH`** — `claude`
  CLI not installed or shell `$PATH` doesn't include it when Capo
  spawned (launchd vs. interactive shell PATH mismatch is the usual
  culprit on macOS).
- **`WorktreeError: base branch 'main' not found`** — your test repo
  uses a different default branch. Set
  `[agents.claude_code].worktree_base_branch = "master"` (or whatever
  your repo uses).
- **Row stays `running` after the child clearly exited** — the §5.6
  monitor only runs while Capo itself is up. Phase 3 (DBOS) makes
  this restart-resilient; in Phase 2, restarting Capo orphans
  monitors. Check the orphan-delegation log line and manually update
  the row if needed.
- **`parse_errors > 0` in reader stats** — CC emitted a stdout line
  that started with `{` but failed `json.loads`. The line is preserved
  as `stream='stdout'` per S-3 §4.2 fallback; this is informational,
  not fatal. If you see lots of these, file an issue with the bad
  line so we can tighten the §4.2 contract.

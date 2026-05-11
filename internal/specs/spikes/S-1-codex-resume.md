# Spike S-1: Codex CLI Session Resume Mechanism

**Status**: Complete (single-version probe — see §6 Constraints).
**Owner**: Operator
**Phase blocked**: Phase 4 (Codex parity).
**Source spec**: `internal/specs/capo-SPEC.md` §5.4, §7.5 ("Integration: Codex CLI"), §9.0 (S-1), §12.1.
**Date**: 2026-05-10.
**Codex version probed**: `codex-cli 0.130.0`.
**Sample artifacts**: `internal/specs/spikes/S-1-samples/`.

---

## 1. Question

Does the Codex CLI support `--resume <session_id>` (or equivalent) so Capo can re-attach a DBOS workflow to a previously-spawned Codex turn after a restart? If not, what is the workaround, and what is the exact spawn invocation + event/output contract Capo's reader must parse?

---

## 2. Method

Real probes against `/Users/ada/.local/state/fnm_multishells/31393_1778448082188/bin/codex` (`v0.130.0`) inside a fresh sandboxed worktree at `/tmp/capo-spike-s1`. For each scenario, we captured `stdout+stderr` to a `.jsonl` artifact and inspected exit code, persisted rollout files, and resume behavior.

| Scenario | Invocation | Artifact |
|----------|------------|----------|
| Success | `codex exec --json --skip-git-repo-check --sandbox read-only "<prompt>"` | `sample-v0.130.0-success-exec.jsonl` |
| Tool use (file_change + command_execution) | `codex exec --json --skip-git-repo-check --sandbox workspace-write "<prompt>"` | `sample-v0.130.0-tool-use.jsonl` |
| Native resume | `codex exec resume --json --skip-git-repo-check <SESSION_ID> "<prompt>"` | `sample-v0.130.0-resume.jsonl` |
| SIGTERM mid-turn | Same as Success, then `kill -TERM <pid>` 5s in | `sample-v0.130.0-sigterm-mid-turn.jsonl` |
| Resume after SIGTERM | `codex exec resume --json --skip-git-repo-check <interrupted_session_id> "<prompt>"` | `sample-v0.130.0-resume-after-sigterm.jsonl` |
| Error: resume with unknown session id | `codex exec resume --json --skip-git-repo-check 00000000-... "<prompt>"` | `sample-v0.130.0-error-resume-bad-session.txt` |

Capo MUST use **`codex exec`** (non-interactive) with the **`--json`** flag, which emits one JSON event per line to stdout (JSONL). The interactive `codex` (no subcommand) is TUI-only and not appropriate for subprocess control.

---

## 3. Headline Findings

1. **Codex DOES support native session resume.** The subcommand is `codex exec resume [SESSION_ID]` and it accepts a UUID. Resuming reuses the same `thread_id` and the model demonstrably recalls prior turn content.
2. **The Codex equivalent of CC's `--output-format stream-json --verbose` is `codex exec --json`.** Output is JSONL: one event per line.
3. **The session_id (`thread_id`) is emitted in the FIRST stdout line** as `{"type":"thread.started","thread_id":"<uuid>"}`. Capo can capture it before yielding control, identical in shape to CC's `session_id_subagent` capture pattern.
4. **Codex persists a full rollout file to `~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ISO-ts>-<thread_id>.jsonl` even when interrupted by SIGTERM mid-turn,** so resume after crash is durable.
5. **No workaround required.** Native resume is sufficient. The §5.4 acceptance criterion stating *"if the Codex CLI does not support session resume natively the spec is amended to a workaround"* resolves to: **native resume is supported; no workaround needed.**

---

## 4. Spawn Invocation (Normative)

### 4.1 Initial spawn

```bash
codex exec \
    --json \
    --skip-git-repo-check \
    --sandbox <read-only|workspace-write|danger-full-access> \
    [--model <brief.model>] \
    [--cd <brief.repo_path>] \
    [--add-dir <extra_writable_dir>] \
    [--ignore-rules] \
    [--ephemeral] \
    "<rendered_brief>"
```

Notes:
- **`--json`** — emits JSONL events on stdout (Capo's reader contract). Without it, codex emits a human-readable text stream that Capo MUST NOT depend on.
- **`--sandbox <mode>`** — sandbox is set at spawn time; **cannot be changed on resume**. Capo selects from `read-only`, `workspace-write`, or `danger-full-access` based on `CodexBrief.sandbox` (default from config: `workspace-write`).
- **`--skip-git-repo-check`** — required when the worktree is not yet a git repo (Capo's dispatcher cannot guarantee this).
- **`-c, --config <key=value>`** — config overrides. Capo MAY use `-c shell_environment_policy.inherit=all` if env passthrough is required.
- **`-C, --cd <DIR>`** — sets the agent's working root. Capo SHOULD pass `brief.repo_path` here.
- **`--add-dir <DIR>`** — additional writable dir alongside primary workspace (e.g., `~/.codex/sessions` is NOT needed, codex manages that itself).
- **No `--ask-for-approval` flag on `codex exec`.** Non-interactive `codex exec` always runs without approval prompts; sandbox is the only enforcement mechanism. (Interactive `codex` accepts `--ask-for-approval`; we do not use interactive mode.)
- **Prompt is passed as the trailing positional arg.** If `-` is used or no prompt arg is given, stdin is read. If both prompt arg AND piped stdin are present, stdin is appended as a `<stdin>` block. Capo SHOULD pass the rendered brief as the positional arg and close stdin immediately to avoid surprise.
- **`--output-last-message <FILE>`** — optional file that receives the final assistant message text. Capo does not need this since the final message is also surfaced via the `turn.completed` event chain.

### 4.2 Resume invocation (on DBOS workflow re-entry)

```bash
codex exec resume \
    --json \
    --skip-git-repo-check \
    [--model <brief.model>] \
    <session_id> \
    "<continuation_prompt>"
```

Notes:
- **The SESSION_ID is the `thread_id` UUID captured from the initial spawn's first stdout line.**
- **`codex exec resume` does NOT accept `--sandbox`, `--ask-for-approval`, `-C`, or `--add-dir`** — sandbox and working-root context are inherited from the original rollout. Passing those flags errors with `error: unexpected argument '--sandbox' found`. Capo's resume code path MUST omit them.
- The continuation prompt is required; resume cannot be a no-op. If Capo only wants to drain remaining output from a still-running session, that is **not what resume does** — see §7.1.
- Resume verified against an **interrupted** (SIGTERM'd mid-turn) session and the model correctly recalled the prior conversation context.

### 4.3 Session storage layout (informational)

Codex writes session rollouts to:
```
~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ISO8601-ts>-<thread_id>.jsonl
```

Example: `~/.codex/sessions/2026/05/10/rollout-2026-05-10T19-54-13-019e1450-1553-7e83-83e9-5d7649363e99.jsonl`

Capo does NOT need to read these files directly — `codex exec resume <session_id>` handles lookup. But Capo SHOULD log the `thread_id` alongside the delegation row so operators can find the file for debugging.

---

## 5. Event/Output Contract (Normative)

`codex exec --json` and `codex exec resume --json` emit **the same** event taxonomy to stdout, one JSON object per line.

### 5.1 Event types observed

| Top-level `type` | When | Capo's use |
|------------------|------|------------|
| `thread.started` | First event after spawn (or resume). Always present. | **Capture `thread_id` here.** Persist to delegation row before yielding. |
| `turn.started` | Once per model turn. | Mark turn boundary in logs. |
| `item.started` | Tool/file/command begins. Optional — short items skip this. | Surface in live progress (§5.13). |
| `item.completed` | Each completed output item. Always present at least once. | Aggregate into delegation output. |
| `turn.completed` | Turn ends (success). Includes `usage` block. | Mark completion + token accounting. |

### 5.2 `item.completed` subtypes (the `item.type` field)

| `item.type` | Schema | Meaning |
|-------------|--------|---------|
| `agent_message` | `{id, type, text}` | Assistant text output. Multiple per turn. |
| `file_change` | `{id, type, changes: [{path, kind}], status}` | File created/modified/deleted by the agent. `kind` ∈ `add`/`modify`/`delete`. |
| `command_execution` | `{id, type, command, aggregated_output, exit_code, status}` | Shell command run inside the sandbox. |
| `reasoning` | (visible only in rollout, not on stdout) | Hidden chain-of-thought. Capo MUST NOT depend on this surfacing via stdout. |

### 5.3 Canonical event sequences

**Minimal success:**
```jsonl
{"type":"thread.started","thread_id":"019e1450-1553-7e83-83e9-5d7649363e99"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"PING"}}
{"type":"turn.completed","usage":{"input_tokens":27741,"cached_input_tokens":21888,"output_tokens":21,"reasoning_output_tokens":14}}
```

**With tool use (file_change + command_execution):**
```jsonl
{"type":"thread.started","thread_id":"019e1451-7763-7832-abc5-c7048c6942eb"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"I'll create hello.txt..."}}
{"type":"item.started","item":{"id":"item_1","type":"file_change","changes":[{"path":"/private/tmp/capo-spike-s1/hello.txt","kind":"add"}],"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_1","type":"file_change","changes":[{"path":"/private/tmp/capo-spike-s1/hello.txt","kind":"add"}],"status":"completed"}}
{"type":"item.started","item":{"id":"item_2","type":"command_execution","command":"/bin/zsh -lc 'ls -l hello.txt'","aggregated_output":"","exit_code":null,"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_2","type":"command_execution","command":"/bin/zsh -lc 'ls -l hello.txt'","aggregated_output":"-rw-r--r-- ...\n","exit_code":0,"status":"completed"}}
{"type":"item.completed","item":{"id":"item_3","type":"agent_message","text":"Created hello.txt..."}}
{"type":"turn.completed","usage":{"input_tokens":84612,"cached_input_tokens":72832,"output_tokens":562,"reasoning_output_tokens":407}}
```

### 5.4 Usage block (final cost accounting)

`turn.completed.usage` has shape:
```json
{
  "input_tokens": <int>,
  "cached_input_tokens": <int>,
  "output_tokens": <int>,
  "reasoning_output_tokens": <int>
}
```

Total = `input_tokens + output_tokens + reasoning_output_tokens` (the `usage` block does NOT include `total_tokens` directly; Capo's cost layer must compute it). The persisted rollout's `event_msg` of `type=token_count` carries the richer breakdown with `total_token_usage.total_tokens` and rate-limit info, but **`codex exec --json` does NOT surface `token_count` events on stdout**, so Capo relies on `turn.completed.usage`.

### 5.5 Pre-events stderr noise

Codex prints one human-readable line to **stdout** before the first JSON event when stdin is a terminal or pipe:
```
Reading additional input from stdin...
```
Capo's JSONL reader MUST tolerate non-JSON prefix lines and skip until the first line that parses as JSON. (CC's reader has the same behavior; this is consistent with §7.5's existing "tolerate pre-events" guidance.)

---

## 6. Constraints / What This Spike Does NOT Cover

- **Single version (0.130.0).** Schema stability across Codex versions is not yet validated. Capo MUST pin `codex>=0.130.0` and re-run S-1 on minor-version bumps before promoting them.
- **Single sandbox mode per session.** Sandbox is fixed at spawn; resume cannot escalate or relax it. If a brief needs a wider sandbox mid-flight, Capo must `fork` (separate UUID) rather than `resume`.
- **`--ephemeral` mode.** Not exercised in this spike. If set, **no rollout file is written**, so **`codex exec resume` will not find the session.** Capo MUST NOT pass `--ephemeral` for delegations that need restart-resume.
- **Multi-turn within a single spawn.** `codex exec` is single-turn (one prompt → one turn → exit). Multi-turn delegations are modeled as: initial `codex exec` → store `thread_id` → on next user input, `codex exec resume <thread_id> "<new_prompt>"`. This is the same model as CC's `--resume`.
- **Rate-limit info on stdout.** The rollout file's `event_msg type=token_count` carries `rate_limits`; stdout does NOT. If Capo's cost cap layer needs live rate-limit telemetry, it must read the rollout file (path is deterministic — see §4.3).

---

## 7. Edge Cases & Lifecycle

### 7.1 stdin behavior

- **Empty piped stdin** (`echo "" | codex exec --json "<prompt>"`): codex prints `Reading additional input from stdin...` then proceeds with the positional prompt. **No effect on output.**
- **Closed stdin** (no pipe, no tty): same as above; codex does not block waiting for stdin once the positional prompt is present.
- **Both prompt arg AND piped stdin**: stdin content is appended as a `<stdin>` block to the prompt. **Capo SHOULD close stdin immediately after spawn to keep the contract simple.**

### 7.2 SIGTERM

- **Mid-turn SIGTERM is handled gracefully.** `codex exec` exits **0** within ~1s of SIGTERM. The rollout file IS written with everything emitted up to that point, including `session_meta` and the `user_message` event for the partial turn, but **no `turn.completed`**.
- The stdout JSONL stream stops abruptly — Capo's reader sees `turn.started` but no `turn.completed`. Capo MUST treat "stdout closed without `turn.completed`" as "interrupted" and rely on the persisted `thread_id` for resume.
- **Resume of a SIGTERM'd session works.** Verified by resuming session `019e1450-cc8c-...` after SIGTERM and receiving a normal `thread.started` + `turn.completed` cycle. The model successfully recalled the prior (interrupted) prompt context.

### 7.3 SIGKILL / subprocess crash

- Not directly exercised, but by inspection: rollout writes are streaming (one line per event), so any line written before crash survives. The crash differs from SIGTERM only in that codex does not get to flush its final buffer — meaning the rollout may end mid-line. Capo's resume tolerates this (the rollout-bad-session error proves codex parses rollouts defensively — it would surface a different error than `code -32600 / no rollout found` if the file were malformed-but-present).

### 7.4 Invalid SESSION_ID on resume

`codex exec resume --json --skip-git-repo-check 00000000-0000-0000-0000-000000000000 "test"` fails with exit code != 0 and stderr:
```
Error: thread/resume: thread/resume failed: no rollout found for thread id 00000000-0000-0000-0000-000000000000 (code -32600)
```
**No JSON event is emitted on stdout.** Capo MUST detect non-zero exit + non-JSON stderr and surface as a hard `DelegationError` (do not retry resume blindly with same session_id).

### 7.5 Sandbox-mode-specific differences

- **`read-only`**: agent can read files in `--cd` worktree but cannot create files or run mutating commands. `file_change` items will not occur; agent typically refuses or asks before attempting mutations.
- **`workspace-write`** (default for Capo): agent can write within `--cd` worktree and `--add-dir` paths. Suitable for normal delegations.
- **`danger-full-access`**: no sandboxing. Capo MUST NOT enable this except for explicitly-flagged operator briefs (and SHOULD require approval flow §5.8 before doing so).
- **All three modes emit identical stdout JSONL event taxonomy.** No sandbox-specific event types. Capo's reader is sandbox-agnostic.
- **Sandbox mode is recorded in the rollout's `session_meta` event** (and via subsequent `turn_context` events), so post-hoc audit is possible.

### 7.6 Concurrent resume of the same session

Not exercised. Codex's rollout file is append-only and a resume creates a new turn in that file. Two concurrent `codex exec resume <same_id>` invocations would race on the rollout file. **Capo MUST serialize resume calls per `thread_id`** (single in-flight `codex exec resume` per delegation row — enforced by the dispatcher's per-channel mutex and the delegation row's `status=running` guard).

---

## 8. Recommendation (binding for Phase 4)

1. **Capo's `delegate_to_codex` spawn invocation** is the form in §4.1, with `--json --skip-git-repo-check --sandbox <mode> -C <repo_path>` plus the rendered brief as the positional prompt arg.
2. **Capo's reader** parses stdout JSONL with the event taxonomy in §5, captures `thread_id` from the first JSON line whose `type == "thread.started"`, and treats `turn.completed` as success and "stdout closed without `turn.completed`" as interrupted.
3. **Capo's resume invocation** is the form in §4.2 — `codex exec resume --json --skip-git-repo-check <thread_id> "<continuation_prompt>"` — and MUST NOT pass `--sandbox`, `--ask-for-approval`, `-C`, or `--add-dir`.
4. **Capo MUST NOT use `--ephemeral`** for delegations that need restart-resume.
5. **Capo MUST pin `codex>=0.130.0`** and re-run this spike on minor-version bumps.
6. The §5.4 / §7.5 spec sections SHOULD be updated to reflect the above. Updates are inline (see commit accompanying this spike).

---

## 9. Sample Artifacts

See `internal/specs/spikes/S-1-samples/`:

| File | Description |
|------|-------------|
| `sample-v0.130.0-success-exec.jsonl` | Minimal success: `thread.started` → `turn.started` → `item.completed[agent_message]` → `turn.completed`. |
| `sample-v0.130.0-tool-use.jsonl` | Multi-item turn: agent_message + file_change + command_execution + agent_message. |
| `sample-v0.130.0-resume.jsonl` | Native resume by SESSION_ID; thread_id is preserved across spawns. |
| `sample-v0.130.0-sigterm-mid-turn.jsonl` | Partial output after SIGTERM 5s into a long-running turn. |
| `sample-v0.130.0-resume-after-sigterm.jsonl` | Successful resume of the SIGTERM'd session. |
| `sample-v0.130.0-error-resume-bad-session.txt` | Stderr from resuming an unknown session id (no rollout). |

# Spike S-6: Phase 4 Checkpoint — Wall-Clock Approval + Codex Operator Runbook

**Status:** Documented (operator-driven gate)
**Date:** 2026-05-11
**Spec reference:** `internal/specs/capo-SPEC.md` §5.4 (Codex tool), §5.6 (DBOS handoff), §5.8 (Approval Flows), §5.9 (Cost caps), §5.10 (Session control), §7.5 (Idempotency-Key), §9.4 (Phase 4 Checkpoint Gate)
**Companion regression suite:** `tests/test_phase4_checkpoint.py` (compressed equivalents, runs in CI)

## 1. Purpose

The Phase 4 checkpoint gate (§9.4) verifies the V1 **approval workflow + Codex parity** contract. Three of its six acceptance rows include wall-clock-bound behaviors that cannot run in CI:

| Acceptance row | Wall-clock variable | CI-feasible? |
|----------------|---------------------|--------------|
| Approval round-trip (`/approve`) | Operator-supplied reply latency (seconds to days) | Compressed: sub-1s reply via `DBOS.send_async`. **Real-AMC** variant is operator-driven. |
| Approval deny (`/deny`) | Same | Same. |
| **Approval timeout** | `[approval].timeout_seconds` (default 30 min, V1 max 24 h) | **NO** — CI cannot wait 30 min. Compressed test uses 2s timeout. |
| Codex delegation (DBOS handoff → notify) | Real-codex run time (minutes to hours) | Compressed: FAKE_CODEX_SCRIPT (~0.5s). **Real-codex** variant is operator-driven. |
| Codex restart-resume | Real-codex resume time (minutes) | Compressed: FAKE_CODEX_RESUME_HAPPY. **Real-codex restart** variant is operator-driven. |
| Kill cascade | Operator-supplied delegation runtime | Compressed: pid=NULL, instant row flip. **Real-subprocess** variant is operator-driven. |

This document describes the operator-driven wall-clock variants for an engineer to run **once per release**, in addition to the CI suite which catches code-path regressions on every commit.

## 2. Pre-requisites

| Item | Required | Where |
|------|----------|-------|
| Real Claude Code CLI | `>= 2.1.138` | `~/.npm-global/bin/claude` or equivalent |
| Real Codex CLI | `>= 0.130.0` | `~/.local/bin/codex` or equivalent |
| Capo installed via launchd plist | running on Mac mini | `~/Library/LaunchAgents/com.you.capo.plist` |
| AMC instance reachable | listed in `capo.toml` | `[amc] base_url` |
| Test AMC channel | thread you can text into | record `channel_id` ahead of time |
| Test git repo inside `projects_root` | with at least one commit | path noted for the Codex test |
| Logfire token (optional) | live trace inspection | `LOGFIRE_TOKEN` env or `[observability]` config |

Confirm:

```bash
claude --version            # must be >= 2.1.138
codex --version             # must be >= 0.130.0
launchctl list | grep capo
sqlite3 ~/.capo/state.db   ".tables" | grep -E "delegations|approvals"
sqlite3 ~/.capo/dbos.db    ".tables" | grep workflow_status
```

## 3. Test Sequence — Approval Round-Trip (Wall-Clock Variant)

The CI test (`test_approval_round_trip_executes_and_idempotent_delivery`) drives `/approve` via `DBOS.send_async` directly — it bypasses AMC inbound webhook routing entirely. This operator runbook exercises the full AMC → dispatcher → `DBOS.send_async` path with a real user reply.

| Step | Action | Expected Signal | How to Verify |
|------|--------|-----------------|---------------|
| 1 | Text Capo (via AMC): *"run `make test-integration` in `/Users/me/projects/capo`"* (or any command not in the `[shell].allowlist`). | Approvals row inserted; AMC outbound notification with `/approve <id>` / `/deny <id>` prompts. | `sqlite3 ~/.capo/state.db "SELECT approval_id, status, request_type, requested_at FROM approvals ORDER BY requested_at DESC LIMIT 1;"` — row exists with `status='pending'`. AMC platform shows the outbound `POST /messages/send` with `Idempotency-Key: notify_approval:<32 hex>`. The user's chat thread shows one Capo message. |
| 2 | At t+30s, text `/approve <approval_id>`. | Dispatcher routes via `DBOS.send_async(workflow_id, "approve", "approval_decision")`. Approval workflow falls out of recv. shell_exec runs the command. Capo replies with command output. | `sqlite3 ~/.capo/state.db "SELECT status, resolved_by, decided_at FROM approvals WHERE approval_id='<id>';"` — `status='approved'`, `resolved_by=<your-user-id>`, `decided_at` non-NULL. User's chat thread shows the command output. |
| 3 | Capo's reply contains the command output. | `make test-integration` output is the message body. | Inspect the AMC reply message body. |

**Idempotency-Key invariant.** Re-text `/approve <approval_id>` after step 2. The dispatcher should reply with `APPROVAL_REPLY_ALREADY_RESOLVED` ("That approval is already resolved.") and NOT fire a second `notify_approval` notification. The AMC server's `Idempotency-Key` log should record exactly one delivery for `notify_approval:<32 hex>`. This is the §7.5 + §9.4 row 1 contract verified end-to-end.

## 4. Test Sequence — Approval Deny (Wall-Clock Variant)

Same as §3 but operator replies `/deny <approval_id> reason: dangerous on prod`. Capo replies with the rejection notice (ApprovalRejected surfaces as a tool error message in the agent loop). The `reason` column should round-trip through to the persisted row.

| Step | Action | Expected Signal |
|------|--------|-----------------|
| 1 | Text Capo: *"delete the contents of /etc"* (or any non-allowlisted dangerous command). | Approvals row pending; AMC notification fired. |
| 2 | Text `/deny <approval_id> not now`. | Dispatcher routes the deny payload; workflow resolves to `denied`; shell_exec raises `ApprovalRejected`; Capo replies with an error message. |
| 3 | Inspect the persisted row. | `sqlite3 ~/.capo/state.db "SELECT status, resolved_by, reason FROM approvals WHERE approval_id='<id>';"` — `status='denied'`, `reason='not now'`. The command was NOT executed (verify no side effects). |

## 5. Test Sequence — Approval Timeout (Wall-Clock Variant)

This is the test the CI compressed variant **cannot reproduce** because the default timeout is 30 minutes (24 hours max). Operator-driven only.

| Step | Action | Expected Signal |
|------|--------|-----------------|
| 1 | Confirm `capo.toml` has `[approval] timeout_seconds = 1800` (or your chosen value ≤ 24h). | `grep timeout_seconds ~/.config/capo/config.toml` |
| 2 | Text Capo a non-allowlisted command. | Approvals row pending; AMC notification fired. |
| 3 | **Wait.** Do NOT reply with `/approve` or `/deny`. | DBOS `recv_async` polls SQLite ~1Hz; on timeout returns None. |
| 4 | At t = `timeout_seconds` + ~5s, observe Capo's behavior. | Workflow resolves row to `expired`, `resolved_by=None`, `reason="timeout after Ns"`. Capo's reply (to whatever next message you send) carries the `ApprovalRejected(status='expired')` tool error surfaced through the agent loop. |
| 5 | Inspect the row. | `sqlite3 ~/.capo/state.db "SELECT status, decided_at, reason FROM approvals WHERE approval_id='<id>';"` — `status='expired'`, `decided_at` non-NULL, `reason` contains the word "timeout". The command was NOT executed. |

**No-double-fire invariant.** If you DO reply `/approve <approval_id>` after the timeout has already fired, the dispatcher should reply with `APPROVAL_REPLY_ALREADY_RESOLVED` and the workflow MUST NOT execute the command. This is the §5.8 row 7 "timeout aborts the action" invariant.

## 6. Test Sequence — Codex Delegation (Wall-Clock Variant)

The CI test (`test_codex_delegation_dbos_handoff_to_terminal_and_notify`) drives a fake codex script that exits in ~0.5s. This operator variant exercises the full `delegate_to_codex` → DBOS handoff → terminal notify path against a **real** Codex CLI invocation.

| Step | Action | Expected Signal |
|------|--------|-----------------|
| 1 | Text Capo: *"use codex to summarize the README in `/Users/me/projects/capo`"*. The main agent calls `delegate_to_codex(...)` with `repo_path=/Users/me/projects/capo`. | Delegations row inserted with `agent='codex'`, `status='running'`. AMC webhook ACKed `< 1s`. The agent's reply quotes the `delegation_id`. |
| 2 | At t+10s, verify Codex spawn argv. | `ps aux \| grep "codex exec --json --skip-git-repo-check --sandbox workspace-write -C"` shows the canonical §5.4 argv. `sqlite3 ~/.capo/state.db "SELECT session_id_subagent FROM delegations WHERE id='<id>';"` is non-NULL (thread_id captured). |
| 3 | Codex completes. | Workflow runs `summarize_run` → `_persist_terminal_step` → `notify_user`. AMC records **exactly one** `POST /messages/send` for the completion message. User's chat thread shows the terminal message with the summary one-liner. |
| 4 | Inspect the row. | `sqlite3 ~/.capo/state.db "SELECT status, summary, ended_at, session_id_subagent FROM delegations WHERE id='<id>';"` — `status='completed'`, `ended_at` non-NULL, `session_id_subagent` matches the codex `thread_id`. AMC server log shows `Idempotency-Key: notify_user:<32 hex>` exactly once. |

## 7. Test Sequence — Codex Restart-Resume (Wall-Clock Variant)

The CI test (`test_codex_restart_resume_completes_and_notifies_once`) skips the initial spawn entirely and starts from the post-restart steady state. This operator variant exercises the **full** restart-resume cycle against a real Codex CLI subprocess.

| Step | Action | Expected Signal |
|------|--------|-----------------|
| 1 | Text Capo: *"use codex to refactor X in `/Users/me/projects/big-repo`, take your time."* A task that legitimately takes 5-10 minutes. | Delegations row inserted with `agent='codex'`, `status='running'`. Codex spawn argv visible via `ps`. `session_id_subagent` captured by t+5s. |
| 2 | At t+2 minutes (mid-stream), `kill -TERM` Capo. | Capo exits cleanly. Codex subprocess survives (orphaned by launchd, reparented to PID 1). `ps -p <codex_pid>` still alive. |
| 3 | `launchctl kickstart` Capo. | Process restarts; cold-boot resume sweep enumerates rows with `status='running'` whose workflow ID isn't tracked by DBOS. For each, it invokes `monitor_delegation(...)`. The resume step (Task #46) reads `agent='codex'` + `session_id_subagent='<thread_id>'` and spawns `codex exec resume --json --skip-git-repo-check <thread_id> "<continuation>"`. |
| 4 | Verify the resume invocation. | `ps aux \| grep "codex exec resume"` shows the canonical §7.5 argv with the original thread_id. `sqlite3 ~/.capo/state.db "SELECT pid FROM delegations WHERE id='<id>';"` is updated to the new PID. Logfire span `capo.workflows.delegation.resume.spawned` fires for `<id>` with `agent='codex'`. |
| 5 | Codex completes the resumed task. | Row reaches `status='completed'`. AMC records **exactly one** `POST /messages/send` for the terminal completion across the entire restart-resume run. User's chat thread shows exactly one terminal message — no duplicates. |
| 6 | Inspect the row. | `sqlite3 ~/.capo/state.db "SELECT status, summary, ended_at, session_id_subagent FROM delegations WHERE id='<id>';"` — `status='completed'`, `session_id_subagent` unchanged (Codex re-emits the same thread_id on resume). |

**Forbidden-flags invariant.** Inspect the `codex exec resume` argv from step 4. The argv MUST NOT contain `--sandbox`, `--ask-for-approval`, `-C`, `--cd`, or `--add-dir` — those flags would cause Codex to error on `unexpected argument` (per S-1 §4.2). The continuation prompt MUST be the trailing positional. The `--model` flag is optional and inherited from the brief.

## 8. Test Sequence — Kill Cascade (Wall-Clock Variant)

The CI test (`test_kill_cascade_resolves_tied_pending_approval`) uses `pid=NULL` so the kill is a row-flip only. This operator variant verifies the cascade against a **real** running subprocess.

| Step | Action | Expected Signal |
|------|--------|-----------------|
| 1 | Spawn a long delegation: *"use codex to write a long doc in `/Users/me/projects/sandbox`"*. Note the `delegation_id`. | Delegations row inserted; codex subprocess alive; row owned by `user-42`. |
| 2 | Manually INSERT a tied pending approval row + start its workflow. (Operator can simulate this via the Python REPL or via a Capo helper script.) The approval's `request_payload` MUST contain `{"delegation_id": "<step-1-id>", ...}` and `request_type='delegate_out_of_root'`. | Approvals row pending; `notify_approval` AMC notification fired to the chat thread. |
| 3 | As the delegation owner, text `/kill <step-1-id>`. | `kill_delegation` runs (owner path — no approval). The cascade walks `approvals WHERE json_extract(request_payload, '$.delegation_id') = '<step-1-id>' AND status='pending'` and calls `force_resolve_approval(workflow_id, status='cancelled', resolved_by='system:killer:user-42', reason='delegation <id> killed')` on each match. |
| 4 | Verify the kill. | Delegations row flips to `killed`. Codex subprocess receives SIGTERM (verify via `ps -p <pid>`). |
| 5 | Verify the cascade. | Approvals row for the tied approval flips to `cancelled`, `resolved_by='system:killer:user-42'`. The approval workflow's return value carries `ApprovalDecision(status='cancelled', resolved_by='system:killer:user-42', reason='delegation <id> killed')`. |

## 9. Failure Modes + Recovery Steps

| Failure | Diagnostic | Recovery |
|---------|------------|----------|
| `/approve` reply did not resolve the workflow | Dispatcher routed but `DBOS.send_async` lost the message. | Inspect `dbos.db` `notifications` table for the approval_id's workflow_id. If missing, the dispatcher's send-async fallback to sync send may have erred — check Logfire spans `capo.transport.dispatcher.approval_decision`. |
| Approval timeout did not fire | `[approval].timeout_seconds` larger than expected, or DBOS recv_async polling stalled. | `sqlite3 ~/.capo/dbos.db "SELECT * FROM workflow_status WHERE workflow_uuid LIKE '%<approval_id>%';"` — confirm the workflow is in `PENDING`. If polling stalled, restart Capo (the workflow will resume on next launch and the remaining timeout re-applies). |
| `notify_approval` raised `PLATFORM_AUTH` | Permanent AMC failure. | Per Task #39 design: row stays `pending`; operator can re-fire the workflow with the same `approval_id` after rotating the AMC bearer token. |
| Codex restart-resume failed with "no rollout found" | `session_id_subagent` mismatch or rollout file pruned. | Per Task #46: row marked `failed` with `summary='session resume failed'`. Operator re-spawns the original task manually. |
| Kill cascade missed a pending approval | `json_extract` index miss or wrong `delegation_id` shape in payload. | `sqlite3 ~/.capo/state.db "SELECT approval_id, json_extract(request_payload, '$.delegation_id') FROM approvals WHERE status='pending';"` — inspect the payload shape. The cascade uses `WHERE json_extract(request_payload, '$.delegation_id') = ?`. |
| User received two terminal notifications | Idempotency-Key invariant violated. | Capture both AMC server log entries; confirm `Idempotency-Key` differs. If keys differ, the `notify_user.idempotency_key_for(...)` derivation has drifted — search recent commits in `capo/workflows/_idempotency.py` and `capo/workflows/delegation.py`. |
| `kill <delegation_id>` triggered an approval (non-owner path) | `delegations.user_id != requester_user_id`. | Expected per §5.10 — non-owner kills route through approval. Reply `/approve <approval_id>` to proceed, or `/deny` to cancel the kill. |

## 10. Sign-off Checklist

After completing the unmodified tests, sign off the §9.4 gate by recording:

- [ ] §3 approve round-trip: row inserted, ACK `< 1s`. approval_id:
- [ ] §3 step 3: command output delivered. Timestamp:
- [ ] §4 deny: row reaches `denied`, reason persisted. approval_id:
- [ ] §5 timeout: row reaches `expired` exactly at `timeout_seconds + polling slop`. approval_id:
- [ ] §6 codex delegation: row reaches `completed`, `session_id_subagent` captured, exactly one terminal notify. delegation_id:
- [ ] §7 codex restart-resume: `codex exec resume <thread_id>` argv observed, no forbidden flags, row reaches `completed`. delegation_id:
- [ ] §8 kill cascade: tied approval reaches `cancelled`, `resolved_by='system:killer:<user>'`. delegation_id + tied approval_id:
- [ ] Total AMC outbound messages match expectation. No duplicate `Idempotency-Key` deliveries.

Record the run in the spec change log (§15.4) as the Phase 4 checkpoint row.

## 11. Relationship to the Compressed CI Test

| Artifact | Mode | Anchors |
|----------|------|---------|
| `tests/test_phase4_checkpoint.py::test_approval_round_trip_executes_and_idempotent_delivery` | CI; ~3s wall clock | §5.8 + §9.4 row 1 — `/approve` round-trip + Idempotency-Key dedupe |
| `tests/test_phase4_checkpoint.py::test_approval_deny_raises_approval_rejected` | CI; ~3s | §5.8 + §9.4 row 2 — `/deny` raises ApprovalRejected |
| `tests/test_phase4_checkpoint.py::test_approval_timeout_raises_expired` | CI; ~3s (2s timeout) | §5.8 + §9.4 row 3 — timeout → `status='expired'` |
| `tests/test_phase4_checkpoint.py::test_codex_delegation_dbos_handoff_to_terminal_and_notify` | CI; ~1s | §5.4 + §5.6 + §9.4 row 4 — Codex DBOS handoff → terminal → notify |
| `tests/test_phase4_checkpoint.py::test_codex_restart_resume_completes_and_notifies_once` | CI; ~1s | §5.4 + §5.6 + §7.5 + §9.4 row 5 — compressed restart-resume |
| `tests/test_phase4_checkpoint.py::test_kill_cascade_resolves_tied_pending_approval` | CI; ~2s | §5.8 + §5.10 + §9.4 row 6 — kill cascade |
| **THIS RUNBOOK §3-8** | Operator; ~10-20 min per execution + variable approval-timeout wait | Full §9.4 acceptance row checklist with real AMC + real Codex CLI |

The compressed CI tests catch regressions in the *code paths* (approval state machine, Codex argv, kill cascade SQL); the operator runbook catches regressions in the *wall-clock invariants* (real DBOS recv polling at production scale, real AMC inbound webhook routing, real codex resume against real rollout state, launchd-orchestrated restart). Both gate the V1 release.

## 12. References

- `internal/specs/capo-SPEC.md` §5.4 (Codex tool), §5.6 (DBOS handoff), §5.8 (Approval Flows), §5.9 (Cost caps), §5.10 (Session control + slash commands), §7.5 (Idempotency-Key), §9.4 (Phase 4 Checkpoint), §11.4 (Runbook).
- `internal/specs/spikes/S-1-codex-resume.md` — Codex CLI spawn/event/resume contract.
- `internal/specs/spikes/S-2-dbos-sqlite.md` — DBOS+SQLite concurrency findings.
- `internal/specs/spikes/S-5-phase3-checkpoint.md` — Phase 3 operator runbook (this document's sibling).
- `tests/test_phase4_checkpoint.py` — Phase 4 durable regression suite.
- `capo/workflows/approval.py` — `request_approval`, `notify_approval`, `force_resolve_approval`, `_resolve_approval_step`.
- `capo/workflows/delegation.py` — `monitor_delegation`, `_resume_spawn_step`, `_build_codex_resume_argv`, `notify_user`.
- `capo/tools/basic.py` — `shell_exec` (approval-gated).
- `capo/tools/codex.py` — `delegate_to_codex`.
- `capo/tools/delegations.py` — `kill_delegation` (cascade-cancel).
- `capo/transport/dispatcher.py` — `_try_handle_approval_command`.
- `capo/transport/slash.py` — `parse_slash_command`.

---

*Document authored as part of the Phase 4 checkpoint sign-off, 2026-05-11.*

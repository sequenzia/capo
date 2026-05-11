# Spike S-5: Phase 3 Checkpoint — 90-Minute Restart-Resume Operator Runbook

**Status:** Documented (operator-driven gate)
**Date:** 2026-05-11
**Spec reference:** `internal/specs/capo-SPEC.md` §3.2 row 1 (V1 success metric), §9.3 (Phase 3 Checkpoint Gate), §10.2 (Critical Path: Restart-Resilient Long Delegation)
**Companion regression suite:** `tests/test_phase3_checkpoint.py` (compressed equivalent, runs in CI)

## 1. Purpose

The V1 success metric (§3.2 row 1) reads:

> *A 90-minute long Claude Code coding delegation survives a Capo restart at the 45-minute mark and the user receives the completion notification.*

The §9.3 checkpoint gate restates this as the third checklist row:

> *Success-metric integration test: spawn CC with a slow 90-minute task, `kill -TERM` Capo at minute 45, `launchctl kickstart` it, verify CC completes and notification arrives.*

A 90-minute wall-clock test is **infeasible inside the automated CI**. We therefore split the gate into two artifacts:

1. **Compressed regression suite** (`tests/test_phase3_checkpoint.py`) — runs in CI. Uses a `FAKE_CLAUDE_SCRIPT` that completes in seconds but exercises the **same code path** (registry-empty workflow re-entry → `claude --resume <session_id>` spawn → drain → terminal status → `notify_user`). Anchors §3.2 row 1, §9.3 rows 3-4, and §10.2 steps 1-7.

2. **Operator runbook** (this document) — run by hand against a real Claude Code at deploy time. This is the unmodified §10.2 critical-path test against a real long-running CC. **No code change required between runs**; everything is observed via existing logs + AMC traffic + SQLite state.

## 2. Pre-requisites

| Item | Required | Where |
|------|----------|-------|
| Real Claude Code CLI installed | `>= 2.1.138` | `~/.npm-global/bin/claude` or equivalent |
| Capo installed via `launchd` plist | running on Mac mini | `~/Library/LaunchAgents/com.you.capo.plist` |
| AMC instance reachable | listed in `capo.toml` | `[amc] base_url` |
| Test AMC channel | a chat thread you can text into | record `channel_id` ahead of time |
| `litestream` configured (optional) | for paired state.db + dbos.db backup | `scripts/litestream.yml` (Phase 5) |
| Logfire token (optional) | for live trace inspection | `LOGFIRE_TOKEN` env or `[observability]` config |

Confirm:

```bash
claude --version   # must be >= 2.1.138
launchctl list | grep capo
sqlite3 ~/.capo/state.db ".tables" | grep delegations
sqlite3 ~/.capo/dbos.db  ".tables" | grep workflow_status
```

## 3. Test Sequence (Unmodified §10.2 Critical Path)

The §10.2 step table reproduced verbatim, with checkpoint signals an operator can verify at each step.

| Step | Action | Expected Signal | How to Verify |
|------|--------|-----------------|---------------|
| 1 | Text Capo (via AMC): *"spawn a long CC coding task — refactor X in repo Y, run the full test suite, then summarize."* The repo + task must legitimately take ~90 minutes (e.g. a non-trivial multi-file refactor in a large repo). | Delegation row inserted; webhook ACKed `< 1s`. | `sqlite3 ~/.capo/state.db 'SELECT id, status, started_at FROM delegations ORDER BY started_at DESC LIMIT 1;'` — row exists with `status='running'`. AMC platform shows the webhook ACK round-trip. |
| 2 | At t+5s, verify DBOS workflow active. | Workflow has captured `session_id_subagent`; output flowing. | `sqlite3 ~/.capo/state.db "SELECT session_id_subagent FROM delegations WHERE id='<id>';"` — non-NULL. `sqlite3 ~/.capo/state.db "SELECT COUNT(*) FROM delegation_output WHERE delegation_id='<id>';"` — increasing on repeat queries. `sqlite3 ~/.capo/dbos.db "SELECT status FROM workflow_status WHERE workflow_uuid LIKE '%<id>%';"` — `RUNNING`. |
| 3 | At t+45m, `kill -TERM` the Capo process. | Capo exits; CC subprocess survives (orphaned by `launchd` — verified by spike S-2 §6). | `launchctl kill TERM gui/$(id -u)/com.you.capo` (or `pkill -TERM -f capo`). Then `ps -p <capo_pid>` returns nothing; `ps -p <cc_pid>` still alive — `cc_pid` is `delegations.pid`. CC's PPID has reparented to 1 (launchd) — that's expected and the resume contract doesn't require Capo to reattach. |
| 4 | `launchctl kickstart` Capo. | Process restarts; DBOS resumes the workflow. | `launchctl kickstart -k gui/$(id -u)/com.you.capo` — new Capo PID. Logfire (or stdlib log) shows event `capo.workflows.delegation.resume.spawned` for `<id>`. `sqlite3 ~/.capo/dbos.db "SELECT recovery_attempts FROM workflow_status WHERE workflow_uuid LIKE '%<id>%';"` increments. |
| 5 | The resume step re-spawns CC with `--resume <session_id>`. | A NEW `claude` process is alive carrying the original session_id; the original orphan is killed by the spawn step's first-event check (or, more commonly, has already exited and the new process is now the live one). | `ps aux \| grep 'claude --resume'` shows the resume invocation. `sqlite3 ~/.capo/state.db "SELECT pid FROM delegations WHERE id='<id>';"` — updated to the new PID. Logfire span `capo.workflows.delegation.resume.spawned` shows the new PID. |
| 6 | CC eventually completes (~90 minutes total wall clock). | Workflow runs `summarize_run` → `_persist_terminal_step` → `notify_user`. AMC records **exactly one** `POST /messages/send` for the completion message, verified via `Idempotency-Key` header and call count. | `sqlite3 ~/.capo/state.db "SELECT status, summary, ended_at FROM delegations WHERE id='<id>';"` — `status='completed'`, `ended_at` non-NULL, `summary` is the deterministic one-liner. AMC server log (or your chosen AMC instance's webhook dashboard) shows one inbound `POST /messages/send` whose `Idempotency-Key` matches `notify_user:<32-hex>` derived from `notify_user.idempotency_key_for(<id>, <db_path>)`. **The user's chat thread shows exactly one terminal message — no duplicates.** |
| 7 | Text Capo `/status`. | No running delegations; last entry shows the completed run with cost. | AMC reply lists the completed delegation. `sqlite3 ~/.capo/state.db "SELECT COUNT(*) FROM delegations WHERE status='running';"` returns `0`. |

## 4. Heartbeat Sub-Test (§5.13)

The 90-minute test naturally crosses the default heartbeat thresholds (5m / 15m / 60m per `capo.toml` `[heartbeat].intervals_seconds`). Verify in passing:

- The user's chat thread shows heartbeat lines at roughly `t=5m`, `t=15m`, `t=60m`, **and** the terminal notification at `t≈90m`. Total of 4 inbound messages from Capo.
- Each heartbeat's `Idempotency-Key` is unique (`heartbeat:<32-hex>`) — captured in the AMC server log.
- After the restart at `t=45m`, the heartbeat poller re-fires for any threshold already crossed, BUT DBOS step-state replay returns the cached result without sending a second copy. **The user sees one heartbeat per threshold, period.** This is the §5.13 row 4 acceptance criterion verified end-to-end.

## 5. Failure Modes + Recovery Steps

| Failure | Diagnostic | Recovery |
|---------|------------|----------|
| Capo fails to restart after `launchctl kickstart` | `launchctl print gui/$(id -u)/com.you.capo` shows last-exit reason. | Check `~/Library/Logs/capo/stderr.log` for `DBOSInitError`. If `db_corrupt`, restore both `state.db` AND `dbos.db` from the paired Litestream backup (§11.4 runbook). |
| Workflow resumes but `claude --resume` returns `is_error=true` | Logfire event `capo.workflows.delegation.resume.is_error` fires; row marked `failed` with `summary="session resume failed"`. | Per S-3 §4.2 + the row preservation contract: the original `session_id_subagent` is **not** overwritten. Operator can re-spawn manually by texting the same task to Capo. |
| `notify_user` raises `NotifyError(code="PLATFORM_AUTH")` after restart | DBOS marks the workflow failed in `dbos.db`; the row is still terminal (`status='completed'`, summary persisted). User just didn't get notified. | Rotate AMC bearer token in keychain, restart Capo; the operator can verify completion via `sqlite3 ~/.capo/state.db "SELECT * FROM delegations WHERE id='<id>';"`. The user-visible notification is lost (acceptable — this is a permanent platform failure, not a Capo bug). |
| User receives **two** terminal notifications | Idempotency invariant violated — investigate immediately. | Capture both AMC server log entries; confirm `Idempotency-Key` differs. If the keys differ, the `notify_user.idempotency_key_for(...)` derivation is non-deterministic — search recent commits for changes in `capo/workflows/_idempotency.py`. If the keys match but the AMC server still delivered twice, that's an AMC-side dedupe bug — file with the AMC team. |
| User receives **zero** terminal notifications but the row is `completed` | `notify_user` never ran. | Check Logfire for `capo.workflows.delegation.notify_no_channel` (delegation came in without an `amc:` `parent_thread_id`) or `capo.workflows.delegation.notify_no_row` (race with retention pruning). Re-send manually if needed. |
| DBOS recovery loops on `workflow_status` rows that point to unknown step ids | Workflow definition changed across the restart (decorator signature or step name). | DBOS at-least-once retries forever; intervene by `dbos.db` surgery — `UPDATE workflow_status SET status='ERROR' WHERE workflow_uuid='<uuid>'` then mark the row failed by hand. File issue documenting the schema-evolution path. |

## 6. Sign-off Checklist

After completing the unmodified test, sign off the §9.3 gate by recording:

- [ ] Step 1: row inserted, ACK `< 1s`. Timestamp:
- [ ] Step 2: session_id captured by `t+5s`. session_id:
- [ ] Step 3: Capo `kill -TERM` at `t=45m`; CC PID `<pid>` still alive at `t=45m + 30s`. Timestamp:
- [ ] Step 4: `launchctl kickstart` succeeds; new Capo PID:
- [ ] Step 5: `--resume <session_id>` argv observed in `ps`. New CC PID:
- [ ] Step 6: row reaches `completed`. Exactly **one** AMC `POST /messages/send` with `Idempotency-Key=<key>` captured. Total user-visible messages in chat thread:
- [ ] Step 7: `/status` reports zero running, lists the completed run.
- [ ] Heartbeats: 3 inbound messages at 5m / 15m / 60m. No duplicates across the restart at 45m.

Record the run in the spec change log (§15.4) as a separate row with date and operator initials.

## 7. Relationship to the Compressed CI Test

| Artifact | Mode | Anchors |
|----------|------|---------|
| `tests/test_phase3_checkpoint.py::test_compressed_restart_resume_completes_and_notifies_once` | CI; ~3s wall clock | §3.2 row 1 (code-path verification), §10.2 step rows 4-6 (resume + terminal + notify), §9.3 row 3 |
| `tests/test_phase3_checkpoint.py::test_restart_mid_completion_idempotent_notify_no_double_send` | CI; ~0.2s | §5.6 Edge Case "Restart between completion and notify_user", §9.3 row 4 (idempotency invariant) |
| `tests/test_phase3_checkpoint.py::test_three_concurrent_delegations_complete_without_lock_contention` | CI; ~1s | §9.3 row 5 (3 concurrent), §6.1 row 4 (max_delegations=3 without contention), §10.3 row 3 |
| **THIS RUNBOOK** | Operator; ~90 min wall clock per execution | §3.2 row 1 (wall-clock variant), §10.2 in full, §5.13 heartbeat sub-test |

The compressed tests catch regressions in the *code path*; the operator runbook catches regressions in the *wall-clock invariants* (heartbeat polling interval, `launchctl` orchestration, real `claude --resume` behavior at scale). Both gate the V1 release.

## 8. References

- `internal/specs/capo-SPEC.md` §3.2 (Success Metrics), §5.6 (DBOS Durable Monitoring + Restart Resume), §5.13 (Live Progress Reporting), §9.3 (Phase 3 Checkpoint), §10.2 (Critical Path), §11.4 (Runbook).
- `internal/specs/spikes/S-2-dbos-sqlite.md` — concurrency findings the §10.3 test plan relies on.
- `internal/specs/spikes/S-3-cc-json-schema.md` §4.2 — resume invocation contract.
- `tests/test_phase3_checkpoint.py` — durable Phase 3 regression suite.
- `capo/workflows/delegation.py` — `_resume_spawn_step`, `monitor_delegation`, `notify_user`, `_run_heartbeat_poller`.

---

*Document authored as part of the Phase 3 checkpoint sign-off, 2026-05-11.*

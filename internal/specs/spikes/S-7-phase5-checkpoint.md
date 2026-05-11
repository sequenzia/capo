# Spike S-7: Phase 5 Checkpoint — Litestream Paired Restore + launchd Boot Drill + caffeinate Observation

**Status:** Documented (operator-driven gate)
**Date:** 2026-05-11
**Spec reference:** `internal/specs/capo-SPEC.md` §5.7 (Compaction + Retention), §5.9 (Cost Caps), §5.10 (Slash Commands), §5.11 (Multi-User), §5.12 (Health Check), §6.5 (Observability), §8.1 (V1 In-Scope: Litestream + launchd + caffeinate), §9.5 (Phase 5 Checkpoint Gate), §11.4 (Runbook)
**Companion regression suite:** `tests/test_phase5_checkpoint.py` (compressed equivalents, runs in CI)

## 1. Purpose

The Phase 5 checkpoint gate (§9.5) verifies the V1 **cost-caps + observability + polish** contract. Most of its acceptance rows are CI-feasible (compressed under `tests/test_phase5_checkpoint.py`), but three rows are wall-clock-bound and therefore operator-driven:

| Acceptance row | Wall-clock variable | CI-feasible? |
|----------------|---------------------|--------------|
| Cost-cap round-trip (hard block + `/override` + soft warn) | UTC-calendar-day accumulator | Compressed: seed `costs` rows + pin `_utc_today_iso`. **Real-spend** variant is operator-driven (requires real LLM costs to accumulate). |
| `/healthz` 200 / 503 contract | DBOS launch + AMC reachability | Compressed: TestClient ASGI in-proc. **launchd-supervised** variant is operator-driven. |
| Compaction round-trip | Real agent run accumulating >30 messages | Compressed: stub summarizer + seeded history. **Real-Pydantic-AI** variant is operator-driven. |
| Retention pruning | Real `delegation_output` accumulation over 7+ days | Compressed: seed rows at past timestamps + invoke `_run_one_prune`. **Real wall-clock** variant is operator-driven. |
| Session command + NL tool sanity | n/a | Fully CI (Task #52 + Task #53 suites). |
| **Litestream paired-restore drill** | Restore real `state.db` + `dbos.db` from S3/object-store backups | **NO** — requires real Litestream replicas with snapshot history. |
| **launchd boot drill** | Mac mini reboot or `launchctl kickstart` cycle | **NO** — requires real launchd integration on the production host. |
| **caffeinate observation** | Real delegation running >20 min while screen idle | **NO** — requires a real long-running delegation. |

This document describes the operator-driven wall-clock variants for an engineer to run **once per release**, in addition to the CI suite which catches code-path regressions on every commit.

## 2. Pre-requisites

| Item | Required | Where |
|------|----------|-------|
| Capo installed via launchd plist | running on Mac mini | `~/Library/LaunchAgents/com.you.capo.plist` |
| Litestream binary | `>= 0.5.x` | `~/.local/bin/litestream` or `/usr/local/bin/litestream` |
| Litestream config | applied + running | `~/.capo/litestream.yml` (operator-edited copy of `internal/ops/litestream.yml`) |
| Replica destination reachable | S3/B2/etc bucket | URLs in `~/.capo/litestream.yml` `[dbs[].replicas]` |
| Real AMC channel | thread you can text into | record `channel_id` ahead of time |
| Real LLM credentials | for the cost-cap variant | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` |
| Recent Litestream snapshot | confirms backups are flowing | `litestream snapshots ~/.capo/state.db` shows entries |
| `caffeinate` binary | macOS built-in | `/usr/bin/caffeinate` |

Confirm:

```bash
launchctl list | grep capo                    # plist loaded
litestream version                              # binary present
litestream snapshots ~/.capo/state.db           # snapshots exist
litestream snapshots ~/.capo/dbos.db            # paired snapshots exist
sqlite3 ~/.capo/state.db   ".tables" | grep costs   # cost accountant migration applied
sqlite3 ~/.capo/dbos.db    ".tables" | grep workflow_status   # dbos schema present
```

## 3. Test Sequence — Litestream Paired Restore Drill (operator-only)

This is the §9.5 row 8 acceptance criterion. The CI suite has no Litestream variant — paired restore requires a real replica.

**Goal:** Restore `state.db` and `dbos.db` from snapshots at the **same wall-clock timestamp** and verify that the in-process workflow state stays consistent with the application state.

| Step | Action | Expected Signal | How to Verify |
|------|--------|-----------------|---------------|
| 1 | Pick a snapshot timestamp `T` from `litestream snapshots ~/.capo/state.db` (use one ≥ 1 hour old to ensure both files have snapshots at-or-before T). | A snapshot tuple is identified. | Record the exact ISO-8601 timestamp. |
| 2 | Stop Capo: `launchctl unload ~/Library/LaunchAgents/com.you.capo.plist`. | Capo process exits cleanly. | `ps aux \| grep capo` empty; `launchctl list \| grep capo` shows the entry but PID=0. |
| 3 | Move the live DBs aside: `mv ~/.capo/state.db ~/.capo/state.db.pre-restore`; `mv ~/.capo/state.db-wal ~/.capo/state.db-wal.pre-restore` (if present); `mv ~/.capo/state.db-shm ~/.capo/state.db-shm.pre-restore` (if present); same for `dbos.db*`. | Live files preserved out-of-band; restore target directory empty. | `ls ~/.capo/*.db*` shows only `*.pre-restore` files. |
| 4 | Restore `state.db` at timestamp `T`: `litestream restore -timestamp $T -o ~/.capo/state.db s3://<bucket>/state.db` (or your configured replica URL). | A new `state.db` written to `~/.capo/` with the schema + data as of `T`. | `sqlite3 ~/.capo/state.db "SELECT MAX(recorded_at) FROM costs;"` returns a value at-or-before `T`. |
| 5 | Restore `dbos.db` at the **same** timestamp `T`: `litestream restore -timestamp $T -o ~/.capo/dbos.db s3://<bucket>/dbos.db`. | A new `dbos.db` written with workflow state as of `T`. | `sqlite3 ~/.capo/dbos.db "SELECT MAX(created_at) FROM workflow_status;"` returns a value at-or-before `T`. |
| 6 | Verify the restore pair is consistent. For any delegation row in `state.db` with `status='running'`, the matching workflow in `dbos.db` MUST exist (in `PENDING` or `SUCCESS` state). | Paired consistency holds. | `sqlite3 ~/.capo/state.db "SELECT id FROM delegations WHERE status='running';"` and cross-reference each id against `sqlite3 ~/.capo/dbos.db "SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid LIKE '%<id>%';"` — every running delegation should have a workflow row. |
| 7 | Start Capo: `launchctl load ~/Library/LaunchAgents/com.you.capo.plist`. | Capo boots; cold-boot resume sweep re-tracks the `status='running'` delegation rows. | Logfire spans `capo.boot` (success) + `capo.workflow.delegation.monitor` (re-entry for each restored delegation). |
| 8 | Wait 60 seconds and inspect for `notify_user` double-fires. The §7.5 `Idempotency-Key` contract guarantees that any re-fired completion notification carries the **same** key as the original — AMC's receiver dedupe absorbs the duplicate. | Zero user-visible duplicate completion messages. | Inspect the AMC platform's outbound message log for the test channel: `Idempotency-Key: notify_user:<32 hex>` keys should appear at-most-once per delegation across the entire restore window. |

**Restore-window invariant.** The point of paired restore is the assumption that `state.db.<T>` and `dbos.db.<T>` are at the same Litestream snapshot tier. Diverging timestamps risk a workflow ID in `dbos.db` referencing a delegation row that has been GC'd from `state.db`, or vice versa. The runbook in `internal/ops/litestream-install.md` documents the snapshot policy that keeps the two in lock-step.

## 4. Test Sequence — launchd Boot Drill (operator-only)

This is the §9.5 row 8 / §8.1 acceptance criterion. The CI suite has no launchd variant.

**Goal:** Verify that Capo boots cleanly on Mac mini reboot, `KeepAlive` triggers on crash, and the plist's PATH covers both Apple Silicon (`/opt/homebrew/bin`) and Intel (`/usr/local/bin`) Homebrew locations so `claude` and `codex` are findable.

| Step | Action | Expected Signal | How to Verify |
|------|--------|-----------------|---------------|
| 1 | Reboot the Mac mini: `sudo shutdown -r now`. | System reboots; on relogin Capo's launchd job loads automatically (`RunAtLoad=true`). | After login, `launchctl list \| grep capo` shows PID > 0 + ExitStatus=0. |
| 2 | Inspect Capo's first-boot Logfire span. | `capo.boot` span fires successfully; subsystem probes pass. | `curl -s http://127.0.0.1:8090/healthz \| jq '.status'` returns `"ok"`. |
| 3 | Kill Capo: `launchctl kickstart -k gui/$UID/com.you.capo` (the `-k` flag stops then restarts). | Capo PID changes; new process boots via launchd's `KeepAlive`. | `launchctl list \| grep capo` shows a new PID within ~5 seconds (`ThrottleInterval=10` sets the upper bound). |
| 4 | Force a crash: `kill -KILL <capo_pid>`. | launchd respawns Capo within ~10 seconds (`KeepAlive={SuccessfulExit=false}`). | `launchctl list \| grep capo` shows a new PID with ExitStatus reflecting the kill. |
| 5 | Verify PATH covers both Homebrew prefixes. | `claude` and `codex` binaries are findable from inside the Capo process. | `curl -s http://127.0.0.1:8090/healthz \| jq '.probes[] \| select(.name == "claude_binary" or .name == "codex_binary")'` shows `ok=true` for both. |
| 6 | Stop Capo cleanly: `launchctl unload ~/Library/LaunchAgents/com.you.capo.plist`. | Process exits cleanly; KeepAlive does NOT trigger a respawn. | `launchctl list \| grep capo` returns no rows; `ps aux \| grep capo` empty. |
| 7 | Reload: `launchctl load ~/Library/LaunchAgents/com.you.capo.plist`. | Capo boots; same as step 1. | Same Logfire + healthz signals. |

**App Nap invariant.** macOS may suspend background processes after long idle. Capo's plist disables App Nap via `LSUIElement=true` + the `caffeinate` helper (step §5 below). To verify: leave Capo idle for 30 minutes, then text it via AMC — the inbound webhook handler must ACK within `< 1s` (§6.1).

## 5. Test Sequence — caffeinate Observation (operator-only)

This is the §9.5 row 8 / §8.1 acceptance criterion. The CI suite covers the `CaffeinateManager` unit/integration logic in `tests/test_caffeinate.py`; this drill validates the wall-clock behavior against a real long delegation.

**Goal:** Verify that `caffeinate -i` (idle-disable) runs for the **entire duration** of any `running` delegation and is terminated once all delegations reach terminal status.

| Step | Action | Expected Signal | How to Verify |
|------|--------|-----------------|---------------|
| 1 | Pre-condition: no delegations running. | `ps aux \| grep "caffeinate -i"` returns no rows. | Confirmed before starting. |
| 2 | Spawn a long delegation: *"use claude-code to refactor X in /Users/me/projects/<repo>, take your time"*. The task should take >20 minutes. | Delegations row inserted with `status='running'`; Capo's `CaffeinateManager` spawns `caffeinate -i <capo_pid>`. | `ps aux \| grep "caffeinate -i"` shows a row with `<capo_pid>` as the target. `pmset -g` shows `sleep` disabled while caffeinate runs. |
| 3 | Leave the Mac mini idle (don't move the mouse, don't press keys) for the next 20 minutes. | System does NOT enter idle sleep. | After 20 minutes, run `pmset -g log \| grep "Display is turned" \| tail -3` — no "Display is turned off" entry past step-2 timestamp. The delegation continues running; CC subprocess stdout still flowing into `delegation_output`. |
| 4 | Spawn a SECOND long delegation while the first is still running. | Refcount on the `CaffeinateManager` set goes to 2; still exactly ONE `caffeinate -i` process. | `ps aux \| grep "caffeinate -i" \| wc -l` returns `1` (single process, refcount 2). |
| 5 | First delegation completes (or you `/kill` it). | Refcount drops to 1; caffeinate persists. | `ps aux \| grep "caffeinate -i"` still shows the process. |
| 6 | Second delegation completes. | Refcount drops to 0; caffeinate process terminated. | `ps aux \| grep "caffeinate -i"` returns no rows within ~2 seconds. |
| 7 | Capo shutdown / restart cycle while a delegation is running. | On Capo restart, the cold-boot sweep MUST re-track each `status='running'` delegation. The CaffeinateManager re-spawns one `caffeinate -i` for the active set. | `ps aux \| grep "caffeinate -i"` returns exactly 1 row after restart, targeting the new Capo PID. |

**Refcount invariant.** Multiple concurrent delegations must NOT spawn multiple caffeinate processes — there is exactly one per Capo instance, with the per-Capo `CaffeinateManager` owning a refcount. This is asserted in `tests/test_caffeinate.py::test_concurrent_delegations_single_caffeinate_process` but the wall-clock variant validates the persistence across real subprocess lifecycle events.

## 6. Sign-off Checklist

After completing the operator-driven drills, sign off the §9.5 gate by recording:

- [ ] §3 Litestream paired restore: `state.db` + `dbos.db` restored at the same `T`. Snapshot timestamp: ________________
- [ ] §3 step 6: paired-consistency check passed. Running-delegation count: __
- [ ] §3 step 8: zero duplicate `Idempotency-Key: notify_user:*` deliveries observed.
- [ ] §4 step 1: Mac mini reboot → Capo auto-launched via `RunAtLoad`. Reboot timestamp: ________________
- [ ] §4 step 4: `kill -KILL` → KeepAlive respawn within ~10s. New PID: ________________
- [ ] §4 step 5: claude_binary + codex_binary probes ok=true (PATH coverage).
- [ ] §5 step 3: 20-minute idle window observed; system did NOT sleep. Display-off events: none post step-2.
- [ ] §5 step 4: concurrent delegations → exactly 1 caffeinate process.
- [ ] §5 step 6: zero caffeinate processes after both delegations terminal.

Record the run in the spec change log (§15.4) as the Phase 5 checkpoint row.

## 7. Failure Modes + Recovery Steps

| Failure | Diagnostic | Recovery |
|---------|------------|----------|
| Litestream restore at timestamp `T` shows divergent state vs dbos.db | One of the two files has a snapshot gap at `T`. Litestream's snapshot policy (1h sync + 24h/168h tiers) should keep them aligned. | Pick a closer aligned timestamp from both files: `litestream snapshots ~/.capo/state.db -timestamp $T` and `litestream snapshots ~/.capo/dbos.db -timestamp $T`; intersect. If no aligned tuple exists, restore both at the most recent snapshot before `T` and accept a small data window. |
| Capo respawns repeatedly on launchd boot | `~/Library/Logs/capo.stderr.log` shows boot-time pre-check failures (claude/codex below min version, state.db corrupt, etc.). | Fix the root cause (upgrade CLI binary, restore state.db). Set `[boot].skip_binary_precheck = true` in `~/.capo/config.toml` as an emergency bypass — but DO NOT leave this on in production. |
| `caffeinate -i` not running while delegations active | Race: `CaffeinateManager.track()` fired before subprocess INSERT, or `release()` fired on a non-terminal status. | Inspect Logfire span `capo.caffeinate.track` / `capo.caffeinate.release` for the affected `delegation_id`. The refcount-by-set design guarantees idempotency; a mismatch usually points to a missing terminal-status row in `delegations`. |
| `/healthz` returns 503 in production | Critical probe (state_db / dbos_db / dbos_launched) failing. | `curl -s http://127.0.0.1:8090/healthz \| jq '.probes[] \| select(.ok == false)'` — inspect the specific probe's `details` field. For dbos_launched=false, restart Capo via launchctl. For state_db missing, check `~/.capo/` permissions + recent Litestream restore. |
| Cost cap fired unexpectedly mid-day | `daily_total_usd` shows spend > `[budget].hard_daily_usd`. | Query `sqlite3 ~/.capo/state.db "SELECT recorded_at, model, cost_usd FROM costs WHERE recorded_at > '$(date -u +%Y-%m-%d)' ORDER BY cost_usd DESC LIMIT 10;"` to identify the source. Issue `/override` to unblock the current turn. Adjust `[budget].hard_daily_usd` in config + restart if the cap is unrealistic. |
| Compaction did not trigger | `[compaction].enabled=true` and message count > threshold, but `compacted_to_message_index` did not advance. | Inspect Logfire span `capo.memory.compact` for the thread. Possible: snapshot-drift retry (another writer raced), or the summarizer raised (cost cap, network). The compaction layer is fail-open by design — failures log at WARNING; never crash a turn. |
| Retention prune fired but no rows pruned | All `delegation_output` rows belong to active delegations, OR all rows are inside the retention window. | Verify the scheduler tick fired (`capo.maintenance.retention.completed` span) with `pruned_count=0`. Check `[retention].delegation_output_days` if the window seems too generous. |

## 8. Relationship to the Compressed CI Tests

| Artifact | Mode | Anchors |
|----------|------|---------|
| `tests/test_phase5_checkpoint.py::test_cost_cap_hard_block_override_and_soft_warn` | CI; ~1s | §5.9 + §5.10 + §9.5 row 2 — hard cap blocks, `/override` unblocks, soft cap warns |
| `tests/test_phase5_checkpoint.py::test_healthz_200_when_all_critical_pass_and_503_when_dbos_down` | CI; ~3s | §5.12 + §9.5 row 7 — healthz happy path + DBOS-not-launched degraded path |
| `tests/test_phase5_checkpoint.py::test_compaction_triggers_and_preserves_keep_recent` | CI; ~1s | §5.7 + §9.5 row 5 — threshold crossed → summarized; keep_recent visible |
| `tests/test_phase5_checkpoint.py::test_retention_scheduler_tick_prunes_terminal_preserves_active` | CI; ~1s | §5.7 + §8.4 + §9.5 row 6 — scheduler prunes stale terminal, keeps active |
| `tests/test_phase5_checkpoint.py::test_slash_override_arms_sentinel_zero_agent_calls` | CI; ~1s | §5.10 + §9.5 row 4 — `/override` arms sentinel without invoking agent |
| `tests/test_dispatcher_session_commands.py::*` | CI; ~30 tests | §5.10 — full `/new` / `/status` / `/clear` / `/kill` / `/override` surface |
| `tests/test_session_tools.py::*` | CI; 17 tests | §5.10 — NL agent tools `session_new` / `session_status` / `session_clear` |
| `tests/test_costs.py` + `tests/test_dispatcher_budget_hook.py` | CI; ~26 tests | §5.9 — cost accountant + pre-agent budget hook |
| `tests/test_health.py` | CI; 23 tests | §5.12 — probe primitives + endpoint integration |
| `tests/test_compaction.py` | CI; 19 tests | §5.7 — compaction unit + integration |
| `tests/test_retention.py` | CI; 34 tests | §5.7 + §8.4 — prune SQL + scheduler |
| `tests/test_caffeinate.py` | CI; 32 tests | §8.1 — refcount manager + lifecycle integration |
| **THIS RUNBOOK §3-5** | Operator; ~30-60 min per execution + wall-clock variable | §9.5 row 8 + §8.1 — Litestream paired restore + launchd boot drill + caffeinate observation against a real Mac mini |

The compressed CI tests catch regressions in the *code paths* (cost cap state machine, healthz probe matrix, compaction snapshot+commit txn, retention SQL, slash override sentinel); the operator runbook catches regressions in the *wall-clock + ops invariants* (Litestream snapshot alignment, launchd KeepAlive policy, caffeinate refcount across real subprocess lifecycle, App Nap behavior). Both gate the V1 release.

## 9. References

- `internal/specs/capo-SPEC.md` §5.7 (Conversation Memory + Compaction + Retention), §5.9 (Cost Caps), §5.10 (Slash Commands), §5.11 (Multi-User), §5.12 (Health Check), §6.5 (Observability), §8.1 (V1 In-Scope), §8.4 (Retention), §9.5 (Phase 5 Checkpoint), §11.4 (Runbook).
- `internal/specs/spikes/S-2-dbos-sqlite.md` — DBOS+SQLite concurrency findings (informs Litestream paired-restore consistency).
- `internal/specs/spikes/S-5-phase3-checkpoint.md` — Phase 3 operator runbook (restart-resume drill — sibling document).
- `internal/specs/spikes/S-6-phase4-checkpoint.md` — Phase 4 operator runbook (approval round-trip + Codex restart-resume — sibling document).
- `internal/ops/litestream.yml` — Litestream config template (paired `state.db` + `dbos.db`, 1h sync, 24h/168h compaction).
- `internal/ops/litestream-install.md` — Litestream install runbook.
- `internal/ops/com.you.capo.plist` — launchd plist template (`KeepAlive`, `RunAtLoad`, PATH).
- `internal/ops/launchd.md` — launchd install runbook.
- `internal/ops/RUNBOOK.md` — top-level operator runbook (Day-2 ops + troubleshooting).
- `tests/test_phase5_checkpoint.py` — Phase 5 durable regression suite.
- `capo/budget.py` — pre-agent budget hook.
- `capo/costs.py` — cost accountant.
- `capo/transport/health.py` — `/healthz` probe primitives + endpoint.
- `capo/memory/compaction.py` — hybrid compaction.
- `capo/maintenance/retention.py` — nightly retention pruning + scheduler.
- `capo/caffeinate.py` — `CaffeinateManager` + refcount semantics.

---

*Document authored as part of the Phase 5 checkpoint sign-off, 2026-05-11.*

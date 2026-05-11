# Spike S-2: DBOS + SQLite Concurrent Workflows

**Status:** GO (with caveats)
**Date:** 2026-05-10
**Spec reference:** `internal/specs/capo-SPEC.md` §9.0 (Spike S-2), §7 Risks row "DBOS + SQLite concurrent workflows", §14 Open Question #2
**Bench code:** `internal/specs/spikes/S-2-bench/`
**DBOS version under test:** `dbos==2.21.0` (latest stable on PyPI as of 2026-05-10)
**Python:** 3.12.13 on macOS (darwin 25.4.0, arm64)

## 1. Question

> Does DBOS handle ≥3 concurrent monitor workflows on SQLite without state corruption or pathological lock contention?

Capo V1 ships with two SQLite files (`state.db`, `dbos.db`). Phase 3 plans to run multiple long-lived monitor workflows in parallel (one per active CC/Codex delegation) plus an approval workflow. If DBOS on SQLite cannot tolerate this concurrency, the plan is to migrate to Postgres before Phase 3.

## 2. TL;DR

**Go.** DBOS 2.21.0 has first-class SQLite support, applies its own schema on startup, and handled the target workload cleanly:

- **Scenario A (5 concurrent step-heavy workflows, 100 steps total):** zero errors, p50 = **0.54 ms**, p99 = **1.17 ms**, max = **1.17 ms**. No `SQLITE_BUSY` surfaced. Throughput ~887 steps/sec on the test machine.
- **Scenario B (3 concurrent `DBOS.send`/`DBOS.recv` pairs, 10 ping-pong rounds each):** all 60 messages delivered, zero losses, but the **recv latency floor is ~1 second per receive** on SQLite — caused by polling, not lock contention (see §5).
- **Restart-during-step:** all 4 in-flight workflows recovered on relaunch and completed to `SUCCESS` with the correct return value. Step side-effects were duplicated for the step that was in-flight at crash time, as expected by DBOS's at-least-once step semantics.

The recv polling floor is the one number to plan around: capo's notification-style workflows (delegation completion, approval response) should expect ~1s latency between `DBOS.send` and the recipient's `DBOS.recv` returning. That is comfortably below any user-facing budget we have. **No Postgres migration is required for V1.**

## 3. Setup

Self-contained bench harness at `internal/specs/spikes/S-2-bench/` using `uv`:

```
S-2-bench/
├── pyproject.toml         # pins dbos
├── uv.lock
├── smoke.py               # 1-workflow smoke test
├── bench_concurrent.py    # Scenarios A + B
└── bench_restart.py       # Restart / recovery test
```

To reproduce:

```bash
cd internal/specs/spikes/S-2-bench
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python dbos
.venv/bin/python smoke.py
.venv/bin/python bench_concurrent.py
DBDIR=$(mktemp -d -t s2-restart-XXXX)
S2_PHASE=start S2_DBDIR="$DBDIR" .venv/bin/python bench_restart.py
S2_PHASE=recover S2_DBDIR="$DBDIR" .venv/bin/python bench_restart.py
```

DBOS configuration used:

```python
config: DBOSConfig = {
    "name": "s2-bench",
    "system_database_url": f"sqlite:///{db_path}",
    "run_admin_server": False,
}
```

On launch, DBOS automatically applies 32 schema migrations to the SQLite file (the `_sys_db_sqlite.py` migration path) and logs an explicit advisory:

> `Using SQLite as a system database. The SQLite system database is for development and testing. PostgreSQL is recommended for production use.`

This warning is noted but does not block V1 — capo's single-process, single-user, single-Mac-mini deployment is exactly the profile SQLite is acceptable for. The recommendation is a "recommended for production" preference, not a hard limitation. We re-evaluate on the migration triggers in §8.2 of the spec.

## 4. Scenario A — concurrent step-heavy workflows

Five workflows running concurrently on a thread pool, each executing 20 DBOS-checkpointed steps that write step state to the shared `dbos.db`. Per-step latency is measured around the step invocation (DBOS checkpoint cost + minimal user code).

**Results** (from `bench_concurrent.py`):

```json
{
  "n_workflows": 5,
  "steps_per_workflow": 20,
  "completed_workflows": 5,
  "wall_clock_ms": 112.77,
  "total_steps": 100,
  "throughput_steps_per_sec": 886.78,
  "latency_ms": {
    "p50": 0.544,
    "p95": 0.682,
    "p99": 1.172,
    "max": 1.172,
    "min": 0.476,
    "mean": 0.561
  }
}
```

- **No errors, no `SQLITE_BUSY`, no timeouts.** Errors list was empty.
- Latency distribution is tight: max is only ~2.5× the median. The single writer model of SQLite is serializing checkpoints cleanly at this concurrency.
- DB file ended at 208 KB after the full bench (Scenario A + B combined).

**Documented floor for step latency under contention:** **~0.5 ms median, ~1.2 ms p99** for cheap steps at 5-way concurrency. Real capo steps will be dominated by their own work (subprocess polling, HTTP calls), so DBOS checkpoint overhead is negligible.

## 5. Scenario B — `DBOS.send` / `DBOS.recv` ping-pong

Three concurrent pairs of workflows. Each pair: `pinger` sends a message tagged with its own workflow ID, `ponger` recvs it and sends a reply back. 10 rounds per pair, 60 messages total.

**Results:**

```json
{
  "n_pairs": 3,
  "rounds_per_pair": 10,
  "wall_clock_ms": 20067.46,
  "total_messages": 60,
  "msgs_per_sec": 2.99,
  "pair_results": [
    {"pair": 0, "sent": 10, "recv": 10},
    {"pair": 1, "sent": 10, "recv": 10},
    {"pair": 2, "sent": 10, "recv": 10}
  ]
}
```

- **Zero message loss.** All 60 messages delivered.
- **~3 messages/sec aggregate, which works out to ~1 second per `recv` call.** This is the recv-poll floor on SQLite.

**Why ~1 s/recv?** From DBOS docs (`docs.dbos.dev/python/reference/configuration`): SQLite has no equivalent of Postgres's `LISTEN/NOTIFY`, so `recv` polls the system DB on a configurable interval (`notification_listener_polling_interval_sec`, default ~1 second). This is the polling overhead, not lock contention.

**Implication for capo:**
- Delegation-completion notifications and approval responses will have a ~1 s post-send latency floor on SQLite. Fine for human-in-the-loop and minute-scale flows.
- If sub-second `send`→`recv` becomes a requirement (it currently isn't in any spec section), we can either (a) lower `notification_listener_polling_interval_sec` in `DBOSConfig` for that workflow class, or (b) migrate to Postgres for LISTEN/NOTIFY. Neither is required for V1.

## 6. Edge case — workflow restart while mid-step

Strategy: start N=4 workflows that each emit 8 steps with `time.sleep(0.25)` between steps. After ~0.85 s (enough for ~3 steps each, with the 4th in flight) call `os._exit(0)` to simulate an abrupt crash without graceful DBOS shutdown. Then relaunch DBOS against the same `dbos.db` and observe automatic recovery.

**Results** (`bench_restart.py`):

- **All 4 workflows recovered to `SUCCESS`** with `result=8` (i.e., the framework re-executed the missing steps and the workflows ran to completion). Recovery took ~1.6 s wall-clock after relaunch.
- **Side-effect log analysis:** rows written = 16 pre-crash + 20 post-recovery = 36 total. For each workflow, step index `3` (the one that was mid-flight at crash time) appeared **twice** in the side-effect log. No other indices duplicated.

Interpretation:
- **Workflow-level correctness:** PASS. No lost workflows, no corruption, no manual recovery required, no `SQLITE_BUSY` on relaunch.
- **Step-level semantics:** at-least-once for steps that were in-flight at crash time, as DBOS documents. The step's *return value* is checkpointed only on completion, so if the process dies mid-step the step is re-executed on recovery.

**Action item for capo implementation (§7 risks row already mandates this):** every DBOS step that has externally-visible side effects (sending an AMC message, killing a process, writing a file the user cares about) must be idempotent OR be wrapped such that the side effect is the *last* operation in the step. This is consistent with the existing "DBOS step idempotency + session-resume contract" mitigation in spec §7.

## 7. Failure modes observed

| # | Mode | Severity | Mitigation |
|---|------|----------|------------|
| 1 | `recv` latency floor of ~1 s on SQLite (no `LISTEN/NOTIFY`) | Low | Acceptable for V1; tune `notification_listener_polling_interval_sec` if needed. |
| 2 | Step duplication on crash mid-step (at-least-once semantics) | Med | Mandatory idempotent step design — already covered by spec §7 risk row. Add a Phase 3 test that kills capo mid-step and asserts no duplicate AMC sends. |
| 3 | DBOS prints an advisory that SQLite is for "development and testing" | Informational | Documented here; reassess on triggers in spec §8.2 (second long-lived process, vector memory, VPS move). |
| 4 | DBOS auto-runs 32 schema migrations on every cold start against a fresh DB | Informational | One-time per `dbos.db`. Subsequent launches against the same file skip migrations. Litestream replication of `dbos.db` (spec §8.1) will capture the migrated schema. |

**Not observed:** `SQLITE_BUSY`, `database is locked`, corruption, lost workflows, dropped messages, hung workflows, deadlocks. None of these surfaced at our target concurrency under our test patterns.

## 8. Go / No-Go decision

**GO for SQLite in V1.** The bench validates the spec's existing V1 plan (`state.db` + `dbos.db` on SQLite, single Python process, Litestream replication). Documented floors:

- Step checkpoint p99: **~1 ms** (negligible vs subprocess/HTTP work).
- `send` → `recv` floor: **~1 s** (acceptable for delegation completion / approval flows).
- Restart recovery: **automatic, ~1.6 s** for 4 in-flight workflows. Steps are at-least-once → idempotency required (already a spec invariant).

## 9. Postgres fallback plan (retained for reference)

We are NOT taking this path for V1. Documenting it here so the team has a ready playbook if any of the migration triggers in spec §8.2 fire later.

**Trigger conditions (from spec §8.2):**
1. A second long-lived process needs DBOS state.
2. Vector memory commits to the same store.
3. Capo moves off the Mac mini to a VPS.

**Migration steps (each independently achievable):**

1. **Stand up Postgres.** Either local `postgres.app` / `brew services start postgresql@16` on the Mac mini, or a managed instance when on VPS. Set `DBOS_SYSTEM_DATABASE_URL=postgresql://user:pw@host:5432/dbos`.
2. **Switch capo's DBOS config.** Single env-var swap — `DBOSConfig.system_database_url` already pulls from env in the patterns we surveyed.
3. **State migration.** DBOS does not ship a SQLite→Postgres state migrator. If active workflows are in flight at migration time, drain them first: stop accepting new delegations, wait for `dbos.workflow_status` to show no running workflows, then cut over with an empty Postgres `dbos` database. Litestream-replicated `dbos.db` is preserved as an archive for post-migration debugging.
4. **`state.db` (capo application state) stays on SQLite** unless trigger (2) fires; only the DBOS system DB needs to move.
5. **Update spec §8.2 and §13 risks** to reflect the change.

**Cost of fallback if invoked:** ~half-day of work plus a clean-drain window. The bench shows we won't need it for V1.

## 10. Open follow-ups (non-blocking)

- Add a Phase 3 integration test that asserts step-idempotency end-to-end: kill capo mid-delegation-monitor-step and verify no duplicate AMC notification.
- When implementing the approval workflow (Phase 4), document the ~1 s `send`/`recv` latency in the approval-flow timing budget.
- Consider exposing `notification_listener_polling_interval_sec` as a tuning knob if any future workflow needs sub-second messaging.

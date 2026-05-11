# Execution Plan — All Pending (Tasks #28–63)

task_execution_id: capo-20260510-231618
timestamp: 2026-05-10T23:16:18Z
scope: All pending tasks (#28 through #63) per user confirmation
max_parallel: 3
retries_per_task: 3
total_tasks: 36
total_waves: 17

## Wave 1 (1 task)
- [28] Configure DBOS to use ~/.capo/dbos.db

## Wave 2 (1 task)
- [29] Implement step idempotency framework — after [28]

## Wave 3 (1 task)
- [30] Implement monitor_delegation DBOS workflow — after [28, 29]

## Wave 4 (3 tasks)
- [31] Implement restart-resume contract via claude --resume — after [30]
- [32] Implement summarize_run DBOS step — after [30]
- [33] Implement notify_user DBOS step — after [30, 29]

## Wave 5 (2 tasks)
- [34] Implement heartbeat step with frozen-threshold idempotency — after [30, 29]
- [35] Replace Phase 2 in-process monitor with DBOS workflow handoff — after [30]

## Wave 6 (1 task)
- [36] Phase 3 checkpoint: 90-minute restart-resume success metric — after [28-35]

## Wave 7 (2 tasks)
- [37] Amend §5.4 and §7.5 with Codex spawn/event/resume contract — after [36]
- [38] Add approvals table via Alembic migration — after [36]

## Wave 8 (2 tasks)
- [39] Implement approval DBOS workflow — after [28, 29, 38]
- [45] Implement delegate_to_codex tool (Codex spawn + reader) — after [37, 30]

## Wave 9 (3 tasks)
- [40] Wire approval inbound routing in dispatcher — after [39]
- [41] Implement /approve and /deny slash command parser — after [39]
- [42] Wire shell_exec approval gating to approval workflow — after [39]

## Wave 10 (3 tasks)
- [43] Wire delegation out-of-projects_root approval gating — after [39]
- [44] Wire kill_delegation approval gating — after [39]
- [46] Implement Codex resume contract in monitor_delegation — after [37, 31, 45]

## Wave 11 (1 task)
- [47] Phase 4 checkpoint: approval and Codex integration tests — after [37-46]

## Wave 12 (3 tasks)
- [48] Implement cost accountant — after [47]
- [50] Configure Logfire instrumentation — after [47]
- [52] Implement session-control slash commands — after [47]

## Wave 13 (3 tasks)
- [49] Implement pre-tool-call budget hook — after [48]
- [51] Enforce span taxonomy from §6.5 across the codebase — after [50]
- [53] Implement session_new / session_status / session_clear NL agent tools — after [52]

## Wave 14 (3 tasks)
- [56] Implement /healthz endpoint with subsystem probes — after [28, 47]
- [57] Create Litestream config replicating state.db and dbos.db — after [47]
- [58] Create launchd plist with KeepAlive and explicit PATH — after [47]

## Wave 15 (3 tasks)
- [59] Implement caffeinate helper — after [47, 45]
- [60] Configure Logfire alerts per §11.3 monitoring table — after [50, 51]
- [61] Write operator runbook — after [57, 58, 56]

## Wave 16 (3 tasks)
- [54] Implement hybrid conversation compaction — after [47]
- [55] Implement nightly delegation_output retention pruning — after [47]
- [62] Add boot-time pre-checks for claude and codex CLI versions — after [47]

## Wave 17 (1 task)
- [63] Phase 5 checkpoint: cost caps, observability, Litestream restore — after [48-62]

## Notes
- 17 waves across Phases 3, 4, and 5
- Heavy sequential dependencies in Phase 3 (Waves 1–6) limit parallelism early
- Wave 4 has 5 ready tasks but capped at 3 (max_parallel=3)
- Wave 9 has 6 ready tasks but capped at 3
- Wave 12+ are mostly independent and limited only by max_parallel

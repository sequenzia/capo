# Execution Plan — Phase 1 (Tasks #1-18)

task_execution_id: capo-20260510-235001
timestamp: 2026-05-10T23:50:01Z
scope: Phase 1 only (tasks #1 through #18) per user confirmation
max_parallel: 5
retries_per_task: 3

## Wave 1 (5 tasks, no deps)
- [1] Run spike S-4: AMC webhook end-to-end smoke harness
- [2] Run spike S-3: Claude Code JSON event schema
- [3] Run spike S-2: DBOS + SQLite concurrent workflows
- [4] Run spike S-1: Codex CLI session resume mechanism
- [5] Scaffold capo project structure and dependencies

## Wave 2 (2 tasks)
- [6] Implement Pydantic Settings — after [5]
- [7] Implement SQLite hardening and BEGIN IMMEDIATE retry helper — after [5]

## Wave 3 (4 tasks)
- [8] Set up Alembic and write initial state.db migration — after [7]
- [9] Implement SOUL + ops prompt loader — after [6]
- [12] Implement AMC REST client with idempotency and typed error codes — after [6]
- [17] Implement multi-user AMC sender → user_id resolution — after [6]

## Wave 4 (2 tasks)
- [14] Implement basic Pydantic AI agent with web_search and fetch_url tools — after [9]
- [15] Implement per-thread conversation memory with ModelMessage persistence — after [7, 8]

## Wave 5 (1 task)
- [16] Implement implicit session creation on first thread message — after [15, 8]

## Wave 6 (1 task)
- [11] Implement per-channel asyncio dispatcher — after [6, 12, 14, 15, 16, 17]

## Wave 7 (2 tasks)
- [10] Implement AMC webhook listener with HMAC verify, dedupe, and fast-ACK — after [6, 11]
- [13] Implement boot-time unread sweep — after [12, 11]

## Wave 8 (1 task)
- [18] Phase 1 checkpoint: integration tests + manual demo — after [1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

## Out of scope this run (Phase 2-5)
Tasks #19–#63 remain pending. Run /agent-alchemy-sdd-tools:execute-tasks again after Phase 1 completes/demos.

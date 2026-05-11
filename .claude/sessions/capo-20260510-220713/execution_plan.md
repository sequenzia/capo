# Execution Plan — Phase 2 (capo agent system)

**Session ID**: `capo-20260510-220713`
**Scope**: Phase 2 tasks #19-#27 — Claude Code Delegation + Basic Supervision (§9.2)
**Retry limit**: 3 per task
**Max parallel**: 5 per wave

## Waves

### Wave 1 (4 tasks, all parallel — no pending dependencies)
- [19] Define ClaudeCodeBrief Pydantic model and delegation_brief.md template (critical)
- [20] Implement git worktree creator helper with cleanup on failure
- [21] Implement async per-delegation subprocess output reader
- [25] Implement shell_exec tool with allowlist enforcement

### Wave 2 (1 task)
- [22] Implement delegate_to_claude_code tool with persist-before-yield contract — after [19, 20, 21]

### Wave 3 (2 tasks, parallel)
- [23] Capture session_id_subagent from CC's first JSON event — after [21, 22]
- [24] Implement check_delegation_status / get_delegation_output / kill_delegation / list_delegations — after [22]

### Wave 4 (1 task)
- [26] Register all Phase 2 tools with the agent — after [22, 24, 25]

### Wave 5 (1 task — Phase 2 checkpoint)
- [27] Phase 2 checkpoint: integration tests + manual demo — after [19, 20, 21, 22, 23, 24, 25, 26]

## Out of Scope This Session
- Phase 3 (#28-#36): DBOS workflow integration (blocked by #27)
- Phase 4 (#37-#47): Approvals + Codex delegation (blocked by #36)
- Phase 5 (#48-#63): Cost caps + observability + ops (blocked by #47)

## Completed (Phase 1)
- #1-#18: All 18 Phase 1 tasks complete (see `capo-20260510-235001/session_summary.md`)

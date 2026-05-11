# Execution Session Summary — Phase 2

**Session ID**: `capo-20260510-220713`
**Date**: 2026-05-10
**Scope**: Phase 2 of the capo agent system (tasks #19-#27) per `internal/specs/capo-SPEC.md` §9.2 — Claude Code Delegation + Basic Supervision.

## Outcomes

- **9 of 9 tasks attempted, all completed** across 5 waves.
- **0 retries** — every task PASS on first attempt.
- **310/310 tests passing** (189 Phase 1 → 310 Phase 2 = +121 new tests). `ruff check capo/ tests/` clean.
- **1 PARTIAL outcome accepted as completed** with explicit deferral rationale:
  - **#27 Phase 2 checkpoint**: 5/6 Functional automated criteria PASS (Phase 2 unit + integration suite, chatty CC >10 MiB, status accuracy running+completed, forced-kill terminates CC, §5.3/§5.5 sign-off). Manual demo ("delegate to claude code: refactor X in repo Y") gated to user with credentials — runbook at `docs/runbook-phase2-demo.md`. Mirrors Phase 1 #18 pattern.

## Spec Amendments Landed This Session

- **§15.4 Change Log**: Phase 2 row appended covering §5.3 + §5.5 sign-off, the §7.5 CC argv amendment (`--output-format stream-json --verbose`, NOT `--output-format json`), schema-divergence notes for migration 001, and DEFERRED note for the manual demo.

## Schema Divergences Surfaced and Documented

Migration 001's `delegations` table diverges from spec §5.5 wording. Documented in `capo/tools/delegations.py` module docstring + §15.4 change log:
- PK is `id`, not `delegation_id`. SQL maps `WHERE id = ?`; dict returns `delegation_id` key.
- No `last_activity_ts` column → derived `MAX(delegation_output.ts) WHERE delegation_id = id` via correlated subquery, fallback to `started_at`.
- No `summary_one_line` / `error_reason` → both map to existing `summary` column. Kill `reason` persists to `summary`.

Phase 3+/5 migrations MAY add dedicated columns if a richer schema is wanted.

## Code Footprint Added This Session

```
capo/
├── tools/
│   ├── __init__.py                          — added register_phase2_tools(agent)
│   ├── claude_code.py                       — NEW (Task #19) extended (#22, #23):
│   │                                          • ClaudeCodeBrief Pydantic model + render_brief() + EMPTY_LIST_PLACEHOLDER (#19)
│   │                                          • DelegationHandle + delegate_to_claude_code() with persist-before-yield (#22)
│   │                                          • SESSION_ID_CAPTURE_TIMEOUT_S, SessionIdCaptureTimeout, _await_session_id, _extract_session_id (#23)
│   ├── delegations.py                       — NEW (#24) — 4 agent-facing tools + _kill_delegation_forced + DelegationNotFound
│   ├── _worktree.py                         — NEW (#20) — create_worktree() + WorktreeError hierarchy + cleanup-on-failure
│   ├── _subprocess.py                       — NEW (#21) extended (#23):
│   │                                          • start_reader, ReaderHandle, ReaderStats, ReaderError; batched DB writes (#21)
│   │                                          • ReaderHandle.first_event_received + first_event hook (#23)
│   └── basic.py                             — extended (#25) — shell_exec, ShellResult, ApprovalRequired
├── agent.py                                 — build_agent now calls register_phase2_tools (#26)
├── deps.py                                  — added CapoDeps.thread_id (#22)
└── transport/dispatcher.py                  — added ApprovalRequired catch + format_approval_required_reply() (#26)

prompts/
└── delegation_brief.md                      — NEW (#19) — single-prompt template, 5 str.format placeholders, SOUL-free

tests/                                       — 8 new test files, 121 net new tests
├── test_claude_code_brief.py                — 23 tests (#19)
├── test_worktree.py                         —  7 tests (#20)
├── test_subprocess.py                       — 12 tests (#21)
├── test_shell_exec.py                       — 25 tests (#25)
├── test_delegate_to_claude_code.py          —  8 tests (#22)
├── test_session_id_capture.py               — 11 tests (#23)
├── test_delegations.py                      — 22 tests (#24)
├── test_agent_phase2_registration.py        —  8 tests (#26)
└── test_phase2_checkpoint.py                —  5 tests (#27)

docs/
└── runbook-phase2-demo.md                   — NEW (#27) — manual demo SOP, user-gated

internal/specs/
└── capo-SPEC.md                             — §15.4 change log row appended for Phase 2 (#27)
```

## Key Decisions and Patterns Established

- **Pydantic AI tool registration evaluates type hints at registration time** — any module exporting a tool MUST import `RunContext` and `CapoDeps` at runtime (NOT under `TYPE_CHECKING`). This is now true for `basic.py`, `claude_code.py`, `delegations.py`. Phase 3+ tool modules must follow.
- **CC argv contract** (spec §7.5 amendment from S-3): `["claude", "-p", rendered_prompt, "--output-format", "stream-json", "--verbose", "--permission-mode", <mode>, "--model", <model>?]`. Spawn with `asyncio.create_subprocess_exec(..., limit=2*1024*1024)` so reader's `readline()` doesn't trip `LimitOverrunError`.
- **Persist-before-yield contract**: `delegate_to_claude_code` INSERTs the `delegations` row inside `asyncio.to_thread` BEFORE returning the handle. Session-id capture and final status updates happen in a background monitor task.
- **Reader hook pattern for "wait for first event of kind X"**: expose an `asyncio.Event` + parsed-value attribute on the handle; producer flips the event from inside its JSON-detection branch. Consumer uses `asyncio.wait_for(event.wait(), timeout=T)`. Avoids DB polling.
- **Approval gating in Phase 2**: tools raise `ApprovalRequired(command, reason)` from `capo.tools.basic`. Dispatcher catches at the `agent.run` boundary (BEFORE the broad `except Exception`) and sends the locked stub: `"That action ({cmd}) needs approval. Reason: {reason}. Full approval flow lands in Phase 4."`. Phase 4 will swap `format_approval_required_reply()` in place.
- **`_FAKE_CLAUDE_SCRIPT` + `_hijack_spawn(fake_script_path)`**: canonical test pattern for driving end-to-end delegation tests without depending on `claude` CLI. Three flavors documented: long-lived, quick-exit, chatty mixed-stream.
- **§10.3 "100 MB / 60 s" performance test**: deferred (TODO in `capo/tools/_subprocess.py`). §6.1 "10 MB without pipe block" is covered by `test_chatty_subprocess_10mb_no_block` (12.5 MiB) and `test_chatty_cc_mixed_streams_over_10mib` (11.25 MiB).
- **Phase 2 in-process monitor is a placeholder for DBOS workflow handoff in Task #35**. Sequencing: `await capture` → `await wait + finalize`. On capture failure: terminate subprocess, mark row failed, log `"orphan delegation: {id} — {reason}"`.

## Cumulative Metrics

- **Total agent execution time**: ~49 minutes (2,924.8 s).
- **Total tokens consumed**: 906,786 across 9 agents.
- **Average per-task duration**: 5.4 minutes.
- **Test growth**: +121 tests (Phase 1 ended 189, Phase 2 ends 310).

## Next Steps for the User

1. **Run the manual demo gate for Phase 2** following `docs/runbook-phase2-demo.md`:
   - Confirm `claude` CLI ≥ 2.1.138 is on PATH.
   - Run `uv run capo --config /path/to/config.toml` with `projects_root` set to a real test repo.
   - Text Capo a delegation request (e.g. "delegate to claude code: refactor README in /path/to/test/repo").
   - Capo should reply with a `delegation_id` within seconds; status follow-ups should return `running`/`completed`.

2. **Commit Phase 2**: ~16 new files + 5 modified. Session-state dir at `.claude/sessions/capo-20260510-220713/` is the archived artifact.

3. **Phase 3 is unblocked** (#28-#36): DBOS workflow integration replacing the in-process monitor. Phase 3 checkpoint (#36) requires a 90-minute restart-resume success metric.

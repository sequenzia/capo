# Execution Context — Phase 2

**Session ID**: `capo-20260510-220713`
**Scope**: Phase 2 of capo agent system (tasks #19-#27).

## Project Patterns

(Carried forward from Phase 1 — see `.claude/sessions/capo-20260510-235001/` plus CLAUDE.md and code under `capo/`.)

- Pydantic Settings for all config; secrets via `.env`.
- DB writes go through the `BEGIN IMMEDIATE` retry helper in `capo/memory/store.py`.
- `BatchedInserter` (§7.3) provides safe batched writes; `flush_count` + `flush_interval_s` thresholds; `.add(row)` auto-flushes. Insert runs inside `begin_immediate_with_retry`.
- Tests use pytest + `pytest-asyncio` (auto-mode). `ruff` for linting. `uv` for dependency management.
- SOUL files (`souls/*.md`) are user-facing personality; **never** included in delegation briefs (§5.1 invariant).
- AMC transport in `capo/transport/`; conversation memory in `capo/memory/`; tools in `capo/tools/`.

### Patterns added in Wave 1
- **Pydantic value-object pattern**: `BaseModel` + `ConfigDict(frozen=True, extra="forbid")`. Use `str_strip_whitespace=True` when string fields might receive trailing whitespace from JSON/CLI inputs. Matches `SearchResult` / `FetchError` in `capo/deps.py`.
- **Subagent prompt rendering**: use plain `str.format` for templating; **no Jinja in this codebase**. Always provide an optional `template_path` override on render functions for test injection.
- **SOUL absence test pattern**: load distinctive markers from `souls/*.md` (first non-heading non-empty line), assert each marker is absent from any rendered subagent-facing string. Pattern reusable for Codex brief renderer in Phase 4.
- **Subprocess invocation style**: list-of-args + `shell=False` + `text=True` + `capture_output=True`. Tag with `# noqa: S603` when ruff complains.
- **Async-side DB writes for high-frequency producers** (subprocess streams) MUST route through `asyncio.to_thread(inserter.add, ...)` to avoid pinning the event loop on SQLite executemany.
- **JSON event detection contract for CC streams**: only stdout, must start with `{` after `.strip()`, parse must succeed. On parse failure: record `parse_errors` and fall back to `stream='stdout'` with the original line (preserves S-3 §4.2 plain-text preamble).
- **Module-private helpers prefixed with `_`** (`_run_git`, `_cleanup`, `_is_git_repo`) so the public surface is just the function + error types.
- **Errors raised, not returned**, for non-recoverable conditions in tool internals; the agent-facing tool layer translates them to structured `FetchError`-style values per spec §5.1.
- **Determinism**: render functions have zero env reads, zero timestamps, zero randomness. Required for Phase 3 DBOS restart-resume reproducibility.

## Key Decisions

- **Empty-list rendering**: empty list → `"_None._"` placeholder (constant `EMPTY_LIST_PLACEHOLDER` in `capo/tools/claude_code.py`). Section header always preserved.
- **Event serialization**: event chunks re-serialized with `json.dumps(sort_keys=True)` so `get_delegation_output` consumers (#24) get stable JSON regardless of child formatting.
- **`ApprovalRequired` exception** defined in `capo/tools/basic.py` (alongside `shell_exec`). Has `command` and `reason` attrs. A later task may centralize it (e.g. to `capo/tools/_errors.py`); for now import from `capo.tools.basic`.
- **logfire forwarding gate**: `_logfire_is_configured()` probe (checks `config.token` / `ignore_no_config`) — prevents `LogfireNotConfiguredWarning` noise in tests.
- **§10.3 "100 MB / 60 s" performance test** deferred (TODO in code comments) per task brief. §6.1 "10 MB without pipe block" is covered by `test_chatty_subprocess_10mb_no_block` (12.5 MiB).
- **Worktree cleanup pattern**: two independent git calls (`worktree remove --force` + `branch -D`) plus belt-and-suspenders `worktree prune`, all `check=False`, collecting notes into `exc.add_note(...)` so original failure isn't shadowed.

## Known Issues

- None blocking. Pre-existing F401s in `capo/tools/basic.py` were resolved by task #25 (added the proper imports).

## File Map

### Phase 1 (pre-existing)
- `capo/tools/basic.py` — Phase 1 tools (`web_search`, `fetch_url`, `FetchError`). Now also exports `shell_exec`, `ShellResult`, `ApprovalRequired`, `SHELL_EXEC_TIMEOUT_S`, `SHELL_EXEC_OUTPUT_MAX_BYTES`.
- `capo/memory/store.py` — provides `BatchedInserter`, `begin_immediate_with_retry`.
- `capo/deps.py` — `CapoDeps.from_settings` mirrors `projects_root` / `workspaces_root` for tools.

### Wave 1 additions
- `capo/tools/claude_code.py` — Phase 2 entry point for CC delegation. Currently: `ClaudeCodeBrief` Pydantic model + `render_brief()` + `_default_template_path()` + `EMPTY_LIST_PLACEHOLDER`. **Task #22 will append `delegate_to_claude_code`; #23 will append session-id capture logic.**
- `capo/tools/_worktree.py` — `create_worktree(repo_path, workspace, branch_name, base_branch) -> str` plus `WorktreeError` / `NotAGitRepo` / `WorkspaceAlreadyExists` / `BaseBranchMissing` / `WorktreeCreationFailed`. Pre-flight validation + cleanup on failure.
- `capo/tools/_subprocess.py` — async per-delegation reader. Public: `start_reader(delegation_id, process, *, store, ...) -> ReaderHandle`. `ReaderError`, `ReaderStats`. Tunables: `DEFAULT_FLUSH_INTERVAL_S=0.050`, `DEFAULT_FLUSH_COUNT=100`, `MAX_LINE_BYTES=1MiB`, `CHUNK_SPLIT_BYTES=64KiB`. **JSON stdout → stream='event'; plain stdout → stream='stdout'; stderr → stream='stderr'.**
- `prompts/delegation_brief.md` — single-prompt template for `claude -p <rendered>`. Five `str.format` placeholders, no literal braces. **Editing this template breaks restart-resume reproducibility — must update the snapshot test in `tests/test_claude_code_brief.py::TestRenderBrief::test_render_snapshot` in the same commit.**
- `tests/test_claude_code_brief.py` — 23 tests (validation, render, empty-list edges, SOUL absence + snapshot).
- `tests/test_worktree.py` — 7 tests (happy path, non-git repo, missing base branch, pre-existing workspace, nested repo_path, git stderr surfacing, cleanup-on-partial-failure).
- `tests/test_subprocess.py` — 12 tests (unit + edge + integration incl. 12.5 MiB chatty subprocess).
- `tests/test_shell_exec.py` — 25 tests.

## Task History

### Wave 1 (2026-05-10)

#### Task [19]: Define ClaudeCodeBrief Pydantic model and delegation_brief.md template — PASS (1/3)
- Files: `capo/tools/claude_code.py` (new), `prompts/delegation_brief.md` (new), `tests/test_claude_code_brief.py` (new, 23 tests).
- Verification: 4/4 Functional, 2/2 Edge Cases. Full suite 219/219.
- Notable: frozen+extra=forbid+str_strip_whitespace on ClaudeCodeBrief; `str.format` substitution (no Jinja dep); deterministic rendering (no env reads / timestamps / randomness); SOUL absence verified via souls/*.md marker scan.

#### Task [20]: Implement git worktree creator helper with cleanup on failure — PASS (1/3)
- Files: `capo/tools/_worktree.py` (new), `tests/test_worktree.py` (new, 7 tests).
- Verification: 4/4 Functional, 2/2 Edge, 1/1 Error Handling. Full suite 219/219.
- Notable: pre-flight validation (is-git-repo + workspace-not-exist + base-branch-exists via `git rev-parse --verify --quiet refs/heads/<name>`); error hierarchy under `WorktreeError`; walk-up `.git` discovery accepts paths inside the working tree.

#### Task [21]: Implement async per-delegation subprocess output reader — PASS (1/3)
- Files: `capo/tools/_subprocess.py` (new, 600+ lines), `tests/test_subprocess.py` (new, 12 tests).
- Verification: 5/5 Functional, 3/3 Edge, 2/2 Error Handling. Full suite 256/256.
- Notable: §6.1 10 MB no-pipe-block proven by 12.5 MiB integration test; `create_subprocess_exec(..., limit=2MiB)` recommended for chatty children to allow `readline()` on large lines; `LimitOverrunError` (subclass of ValueError) signals "switch to `read(N)`" for long-line splitter; `asyncio.to_thread(inserter.add, ...)` wraps DB writes to avoid event-loop pinning.

#### Task [25]: Implement shell_exec tool with allowlist enforcement — PASS (1/3)
- Files: `capo/tools/basic.py` (edited), `tests/test_shell_exec.py` (new, 25 tests).
- Verification: 5/5 Functional, 2/2 Edge, 1/1 Error Handling. Full suite 256/256.
- Notable: metachar check is layered (scan raw command string AND each tokenized arg), so quoted-metachar injection like `git log '; rm -rf /'` is caught; `shell_exec` is sync (matches `subprocess.run` semantics) even though other tools in `basic.py` are async; pulls `projects_root`/`workspaces_root` from `CapoDeps`, not directly from settings.

### Wave 2 (2026-05-10)

#### Task [22]: Implement delegate_to_claude_code with persist-before-yield contract — PASS (1/3)
- Files: `capo/tools/claude_code.py` (appended ~600 lines: `DelegationHandle`, `delegate_to_claude_code`, monitor + cleanup helpers; re-exports `ApprovalRequired`); `capo/deps.py` (added `thread_id: str | None` to `CapoDeps` + `from_settings`); `tests/test_delegate_to_claude_code.py` (new, 8 tests).
- Verification: 8/8 Functional, 2/2 Edge, 3/3 Error Handling. Full suite 264/264.
- Notable:
  - `delegation_id` = `uuid.uuid4().hex` — matches `capo/memory/conversation.py:135` pattern.
  - **Argv contract**: `["claude", "-p", rendered_prompt, "--output-format", "stream-json", "--verbose", "--permission-mode", <mode>, "--model", <model>?]`. NOT `--output-format json`.
  - **Subprocess `limit=2*1024*1024`** on `create_subprocess_exec` so reader's `readline()` path doesn't trip `LimitOverrunError` on normal CC events.
  - **Config key**: `default_permission_mode` (matches spec §7.5 + existing config.toml), NOT `permission_mode`.
  - **DB column**: `user_id` (not `parent_user_id`).
  - **`_agents_claude_code_value(deps, key, default)`** helper reads `AgentBinaryConfig.model_extra` first (where `default_permission_mode`/`worktree_base_branch` live), falls back to `getattr`, then a default. **Reusable for #23/#24/#26.**
  - **Persistence-before-yield**: INSERT runs inside `asyncio.to_thread`; test verifies via a connection proxy that timestamps `INSERT INTO delegations` against the function return. Note: `sqlite3.Connection.execute` is C-level read-only — tests must use a wrapper class with `__getattr__` delegation, not attribute assignment.
  - **Monitor task** (Phase 2 placeholder, replaced by DBOS in #35): one `asyncio.create_task` per delegation, named `delegate-monitor-<id>`. Awaits `process.wait()` → `reader_handle.wait()` → UPDATE status. Reader has its own sqlite3.Connection, closed in `finally`.
  - **Cleanup pattern on failure**: kill child if spawned + best-effort `git worktree remove --force` + `git branch -D` + `shutil.rmtree(workspace, ignore_errors=True)` + INSERT row with `status='failed'` + reason. Guarantees "no orphan workspaces without a DB row".
  - **`_FAKE_CLAUDE_SCRIPT` integration test pattern**: small in-tree Python file emits CC-shaped JSON events then exits. Reusable for #23 (session-id capture) and #27 (Phase 2 demo).

### Wave 3 (2026-05-10)

#### Task [23]: Wire CC's first JSON event into session_id_subagent capture — PASS (1/3)
- Files: `capo/tools/_subprocess.py` (added `ReaderHandle.first_event_received: asyncio.Event` + `first_event: dict | None`, set inside `_emit_chunk`); `capo/tools/claude_code.py` (added `SESSION_ID_CAPTURE_TIMEOUT_S=30.0`, `SessionIdCaptureTimeout`, `_update_delegation_session_id`, `_await_session_id`, `_extract_session_id`; `_monitor_wrapper` sequences capture→spawn_monitor); `tests/test_session_id_capture.py` (new, 11 tests).
- Verification: 6/6 Functional, 3/3 Edge, 1/1 Error Handling. Full suite 297/297.
- Notable:
  - **S-3 field path**: top-level `event["session_id"]` (same value on every event of a normal spawn).
  - **Reader-hook design**: `asyncio.Event` + parsed-value attribute (not callback/queue). Consumer uses `asyncio.wait_for(handle.first_event_received.wait(), timeout=T)`. Hook set only when stdout line parses as dict with top-level non-empty `session_id: str`.
  - **Persist-before-yield preserved**: `delegate_to_claude_code` still returns immediately after INSERT; UPDATE happens in the background monitor.
  - **Test gotcha**: `_monitor_wrapper` reads `SESSION_ID_CAPTURE_TIMEOUT_S` at await time — `patch(...)` block must stay open across the polling loop.
  - **Orphan log format**: `"orphan delegation: {id} — {reason}"` at `logger.error` via `capo.tools.claude_code` logger. Phase 3 / #31 will scan these for boot-recovery.
  - UPDATE for session_id_subagent is a separate retry-helper-wrapped statement with `operation=update_session_id:{delegation_id}` tag.

#### Task [24]: Implement check_delegation_status / get_delegation_output / kill_delegation / list_delegations — PASS (1/3)
- Files: `capo/tools/delegations.py` (new ~470 lines: 4 tools + `_kill_delegation_forced` + `DelegationNotFound`); `tests/test_delegations.py` (new, 22 tests).
- Verification: 4/4 Functional, 4/4 Edge, 2/2 Error Handling. Full suite 296-297/297 (one timing-sensitive flake in #23's test on first run, passed on retry).
- **Schema reality check** (important for #26, #27): The migration 001 schema diverges from spec §5.5 wording:
  - PK column is `id`, NOT `delegation_id`. SQL maps `WHERE id = ?`; dict returns `delegation_id` key.
  - No `last_activity_ts` column → derived `MAX(delegation_output.ts) WHERE delegation_id = id` via correlated subquery, fallback to `started_at`.
  - No `summary_one_line` / `error_reason` columns → both map to existing `summary` column. Kill `reason` persists to `summary`.
- Notable:
  - **`get_delegation_output` unknown-id**: returns `[]` (matches "no output yet" race), not raising. Agent uses `check_delegation_status` to confirm existence.
  - **`kill_delegation` Phase 2 semantics**: ALWAYS raises `ApprovalRequired` — does NOT inspect the row first. Phase 4 validates row existence inside approval workflow before calling `_kill_delegation_forced`.
  - **`_kill_delegation_forced`**: module-private, not in `__all__`. SIGTERM with 5s grace, escalates to SIGKILL. Polling via `asyncio.to_thread`. Handles `pid=None` case (Phase 4 pending_approval rows).
  - **`list_delegations` missing user_id**: raises `RuntimeError` (surfaces dispatcher bug fast vs silent cross-user leak).
- **Patterns added**:
  - **Sync DB-read helper pattern**: `_read_*_rows(db_path, ...)` opens own connection, sets `row_factory = sqlite3.Row`, executes SELECT, returns plain dicts, closes. Async tool wraps via `await asyncio.to_thread(_read_..., db_path, ...)`.
  - **Status-snapshot pattern**: when status response needs a column value AND child-table aggregate, use a correlated subquery in a single SELECT.

### Wave 4 (2026-05-10)

#### Task [26]: Register all Phase 2 tools with the agent — PASS (1/3)
- Files: `capo/agent.py` (`build_agent` now calls both `register_basic_tools` + `register_phase2_tools`); `capo/tools/__init__.py` (new `register_phase2_tools(agent)` helper); `capo/tools/claude_code.py` + `capo/tools/delegations.py` (promoted `RunContext` + `CapoDeps` from `TYPE_CHECKING` to runtime imports); `capo/transport/dispatcher.py` (added `format_approval_required_reply(exc)` + `except ApprovalRequired` branch); `tests/test_agent_phase2_registration.py` (new, 8 tests).
- Verification: 4/4 Functional, 2/2 Edge, 1/1 Error Handling. Full suite 305/305.
- **Critical gotcha**: **Pydantic AI tool registration evaluates type hints at registration time** via `typing.get_type_hints`. If a tool function annotates `ctx: RunContext[CapoDeps]` with `RunContext` imported under `TYPE_CHECKING`, `agent.tool(fn)` fails with `NameError`. Fix: import `RunContext` AND `CapoDeps` at module top level for any module exporting an agent-registered tool. **Pattern carried forward** — Phase 3+ tool modules must follow.
- **`capo/tools/basic.py`'s `ApprovalRequired` is distinct from `pydantic_ai.exceptions.ApprovalRequired`** — both are Exception subclasses with different constructors. Dispatcher catches `capo.tools.basic.ApprovalRequired` specifically.
- **Dispatcher catch order**: `except ApprovalRequired` MUST precede `except Exception` (subclass-catch issue). Catch is inside the `agent.run` try block; does NOT persist new messages on approval-required paths; `mark_read` still fires.
- **Locked stub wording**: `"That action ({cmd}) needs approval. Reason: {reason}. Full approval flow lands in Phase 4."` — exposed as `format_approval_required_reply()` so Phase 4's real approval flow can swap it in place.
- **Phase-split registration helpers**: `register_basic_tools` (Phase 1) + `register_phase2_tools` (Phase 2). Phase 3/4 should follow with `register_phase3_tools` etc.
- Pre-existing unraisable warning (`Event loop is closed` in `BaseSubprocessTransport.__del__`) in real-subprocess integration tests — non-blocking, same race as #22.

### Wave 5 (2026-05-10)

#### Task [27]: Phase 2 checkpoint per §9.2 — PARTIAL (1/3, manual demo deferred to user gate)
- Files: `tests/test_phase2_checkpoint.py` (new, 5 tests); `docs/runbook-phase2-demo.md` (new, manual-demo SOP); `internal/specs/capo-SPEC.md` §15.4 (Phase 2 change-log row appended).
- Test totals: **310 passing** (Phase 1 ended at 189; Phase 2 added 121). Ruff clean.
- §9.2 Functional criteria walk:
  1. ✅ All Phase 2 unit + integration tests pass — 310 passed.
  2. ✅ Chatty CC >10 MiB stdout, no pipe block — `test_chatty_cc_mixed_streams_over_10mib` (11.25 MiB stdout + 25 events, 60s budget); also `test_chatty_subprocess_10mb_no_block` (12.5 MiB pure stdout) from #21.
  3. ✅ Status accurate for running + completed — `test_status_running_for_long_lived_cc` + `test_status_completed_after_clean_exit`.
  4. ✅ Kill terminates running CC + row='killed' with reason — `test_kill_forced_terminates_live_cc` (real `delegate_to_claude_code` + `_kill_delegation_forced`).
  5. ⏸️ Manual demo — **DEFERRED** to `docs/runbook-phase2-demo.md` (requires user-credentialed AMC + working `claude` CLI ≥ §12.1's 2.1.138). Same pattern as Phase 1 #18.
  6. ✅ §5.3, §5.5 sign-off in §15.4 change log — row appended.
- **Patterns added**:
  - **JSON-event byte equality is wrong in tests**: when asserting against `delegation_output` rows for `stream='event'`, compare row COUNTS not byte SUMs (re-serialization with `sort_keys=True` makes byte total implementation-defined).
  - **`_hijack_spawn(fake_script_path)` factory**: async stand-in for `asyncio.create_subprocess_exec` that runs a Python file under `sys.executable`. Use with `patch("capo.tools.claude_code.asyncio.create_subprocess_exec", side_effect=_hijack_spawn(script))`. Reusable for future tests that need a tame "claude" stand-in.
  - **Live-status test must wait for first event**: poll `delegation_output.COUNT(*) > 0` (up to ~5s) before asserting `last_activity_ts != NULL` — querying immediately after `delegate_to_claude_code` returns can race the reader's first flush.
  - **Kill test polling window**: `_kill_delegation_forced` SIGTERM → 5s grace; tests must allow ≥5s for child exit. Use `os.kill(pid, 0)` as liveness probe.

### Schema Divergences (Known Issues)
- Migration 001 `delegations` PK is `id`, not `delegation_id`. No `last_activity_ts`, `summary_one_line`, `error_reason` columns. Phase 2 code adapts in `capo/tools/delegations.py`:
  - `delegation_id` accepted as function arg, mapped to `WHERE id = ?` in SQL.
  - `last_activity_ts` derived from `MAX(delegation_output.ts) WHERE delegation_id = id` via correlated subquery, fallback to `started_at`.
  - `summary_one_line` and kill `reason` both persist to the `summary` column.
- Phase 3+/5 migrations MAY add dedicated columns if needed. Documented in §15.4 Phase 2 change-log row.

# Execution Context — capo-20260510-231618

Started: 2026-05-10T23:16:18Z
Scope: All pending tasks (#28–63) — Phases 3, 4, and 5.
max_parallel: 3
retries_per_task: 3

## Prior Session References (READ BEFORE STARTING)

- `.claude/sessions/capo-20260510-235001/execution_context.md` — Phase 1 (#1–18) learnings: spike findings, DBOS+SQLite GO decision, CC + Codex CLI contracts, AMC HMAC + error codes, pinned deps, CLI precedence.
- `.claude/sessions/capo-20260510-220713/execution_context.md` — Phase 2 (#19–27) learnings: delegation tools, ClaudeCodeBrief, worktree helper, subprocess reader, shell_exec.
- `CLAUDE.md` (if present) — project conventions.
- `internal/specs/capo-SPEC.md` — Authoritative spec. Sections referenced in each task description.

## Project Patterns (inherited)

### Build & Tooling
- Python 3.12+, `uv` + `hatchling` build backend. PEP 735 `[dependency-groups].dev`. Install: `uv sync --group dev`.
- `ruff`: line-length 100, target py312, rules `E/W/F/I/B/UP/SIM`.
- `pytest`: `testpaths=["tests"]`, `asyncio_mode="auto"`, `--strict-markers`.
- Pinned deps (2026-05-10): pydantic-ai 1.93.0, fastapi 0.136.1, dbos 2.21.0, pydantic-settings 2.14.1, httpx 0.28.1, alembic 1.18.4, logfire 4.32.1, sqlalchemy 2.0.49.

### Code Style
- Pydantic value-objects: `BaseModel` + `ConfigDict(frozen=True, extra="forbid")` (+ `str_strip_whitespace=True` for str inputs).
- Module-private helpers prefixed `_`.
- Errors raised, not returned, for non-recoverable internal conditions; tool-layer translates to structured values (`FetchError`-style) per §5.1.
- Subprocess style: list-of-args + `shell=False` + `text=True` + `capture_output=True`; tag `# noqa: S603` for ruff.
- DB writes go through `begin_immediate_with_retry` (`capo/memory/store.py`) and `BatchedInserter`.
- High-frequency producers route DB writes via `asyncio.to_thread(inserter.add, ...)` to avoid event-loop pinning.

### Rendering Determinism
- Render/template functions: zero env reads, zero timestamps, zero randomness. Required for Phase 3 DBOS restart-resume reproducibility.

### SOUL Invariant
- `souls/*.md` files are user-facing personality; **never** included in delegation briefs (§5.1). Always assert SOUL markers absent from any subagent-facing string.

## Key Decisions (inherited — relevant to Phase 3+)

### DBOS + SQLite
- **GO for V1.** No Postgres needed. (S-2: p99 step checkpoint ~1.2 ms at 5-way concurrency, no SQLITE_BUSY.)
- DBOS step semantics: **at-least-once on crash**. Externally-visible side effects MUST be idempotent.
- Send/recv polling floor on SQLite is ~1 s (no LISTEN/NOTIFY). Configurable via `notification_listener_polling_interval_sec`.
- `dbos.db` and `state.db` are **SEPARATE** SQLite files. Capo manages `state.db` via Alembic; DBOS owns `dbos.db` schema.

### Claude Code CLI Contract
- Spawn: `claude -p "<brief>" --output-format stream-json --verbose --permission-mode <mode> [--model <X>]`. `stream-json` REQUIRES `--verbose`.
- Subprocess `limit=2*1024*1024` so reader's `readline()` doesn't trip `LimitOverrunError`.
- Completion detection: switch on `result.is_error` (boolean). NEVER use `result.subtype`.
- Min CC version `2.1.138`. Constant `MIN_CLAUDE_CODE_VERSION`. Boot-time SemVer check.
- Parser MUST tolerate non-JSON preamble lines before first JSON event.
- `claude --resume <unknown>` emits plain-text error then a `result` event with `is_error: true` and a NEW synthetic session_id. Parser must refuse to overwrite `session_id_subagent` when first JSON has `is_error: true`.
- Hook events (`hook_started`, `hook_response`) fire BEFORE `system/init` in plugin envs — do NOT tie `claude_code_version` capture to event index 0.

### Codex CLI Contract
- Spawn: `codex exec --json --skip-git-repo-check --sandbox <mode> -C <dir> "<prompt>"`. **NEVER** pass `--ephemeral` (disables rollout → breaks resume).
- Resume: `codex exec resume <thread_id> "<continuation>"` works end-to-end across SIGTERM.
- `codex exec resume` does NOT accept `--sandbox`, `--ask-for-approval`, `-C`, or `--add-dir` (inherited from original rollout).
- Reader must skip `Reading additional input from stdin...` non-JSON prefix.
- Min Codex version `0.130.0`.
- `codex exec resume` is single-threaded per `thread_id` — Capo MUST serialize resume calls per delegation.

### Phase 2 Carry-overs
- Phase 2 delegation has an in-process monitor (`asyncio.create_task` named `delegate-monitor-<id>`) — Task #35 replaces this with DBOS workflow handoff.
- `delegate_to_claude_code` is in `capo/tools/claude_code.py`; brief rendering uses `str.format` (no Jinja).
- `shell_exec` is in `capo/tools/basic.py` with `ApprovalRequired` exception (may be centralized in #42).
- Subprocess reader in `capo/tools/_subprocess.py` — `start_reader(...) -> ReaderHandle`, JSON-event vs plain-stdout discrimination.

## Known Issues (inherited)

- **task-executor agents do NOT have `TaskUpdate` tool** despite the agent description. Orchestrator marks tasks completed based on result-file status.
- `uv.lock` currently gitignored.
- DBOS pulls Temporal SDK transitively (~80 deps total). First `uv sync` ~30s.

## File Map (inherited — key landmarks)

- `internal/specs/capo-SPEC.md` — Authoritative spec; amended by spikes S-1/S-3.
- `internal/specs/spikes/S-{1..4}-*.md` — Spike findings.
- `capo/__init__.py`, `capo/__main__.py`, `capo/main.py` — package + entrypoint.
- `capo/memory/store.py` — `begin_immediate_with_retry`, `BatchedInserter`.
- `capo/memory/conversation.py` — per-thread conversation memory.
- `capo/deps.py` — `CapoDeps` (carries `projects_root`, `workspaces_root`, `thread_id`).
- `capo/transport/` — AMC HTTP + webhook.
- `capo/tools/basic.py` — `web_search`, `fetch_url`, `shell_exec`, `ApprovalRequired`.
- `capo/tools/claude_code.py` — `ClaudeCodeBrief`, `render_brief`, `delegate_to_claude_code`, `DelegationHandle`.
- `capo/tools/_worktree.py` — `create_worktree`.
- `capo/tools/_subprocess.py` — async reader (`start_reader`, `ReaderHandle`).
- `prompts/delegation_brief.md` — single-prompt CC template; editing breaks restart-resume reproducibility (snapshot test in `tests/test_claude_code_brief.py`).
- `souls/*.md` — user-facing personalities; must not leak into briefs.
- `alembic/` — state.db migrations (set up in #8).

## Task History

### Wave 1 (2026-05-10)

### Task [28]: Configure DBOS to use ~/.capo/dbos.db - PASS

- Files modified:
  - `capo/workflows/__init__.py` (new) — DBOS config + lifecycle + typed `DBOSInitError`.
  - `capo/main.py` — `amain()` now calls `init_dbos` + `launch_dbos` (via `asyncio.to_thread`) BEFORE `dispatcher.start()` and the boot sweep, and `destroy_dbos` in the shutdown `finally`. DBOSInitError surfaces as a single-line stderr message and exit code 2.
  - `tests/test_workflows_config.py` (new) — 17 unit + integration tests covering all Phase-3 task #28 acceptance criteria.

- Key learnings:
  - **DBOS API surface for Phase 3**:
    - `DBOSConfig` is a TypedDict; pass `name`, `system_database_url`, `run_admin_server=False`.
    - SQLite URL form: `sqlite:///{absolute_path}` (three slashes + absolute path).
    - `DBOS(config=..., fastapi=app)` registers the singleton; `DBOS.launch()` opens the DB + applies 32 migrations + starts the notification poller (sync, ~100ms cold start — wrap in `asyncio.to_thread`).
    - `DBOS.destroy(workflow_completion_timeout_sec=0)` for teardown. Class-level methods, NOT instance methods.
    - DBOS is a **process-level singleton** — `DBOS(...)` registers itself in the dbos package's module state; the instance returned is just a handle. Re-`DBOS(...)` calls collide. Our module-level `_state` mirrors this so we can answer "is launched?" without poking dbos internals.
  - **Path collision check** (`dbos.db == state.db`) is essential — DBOS auto-migrates the file, so a collision corrupts Alembic-owned schema. Caught at `init_dbos` via `.resolve()` comparison.
  - **0700 mkdir nuance**: `Path.mkdir(mode=0o700)` is masked by umask on POSIX. Follow with explicit `os.chmod(parent, 0o700)` to force private perms even under a permissive umask 022.
  - **SQLite corruption probe**: open with `sqlite3.connect("file:{path}?mode=ro", uri=True)` + `PRAGMA schema_version`. Fast (microseconds), catches header damage before DBOS's heavier migration path.
  - **Test isolation for DBOS**: autouse fixture calls `destroy_dbos()` after each test — DBOS is process-global and the second test would otherwise trip the "already initialized" guard. `is_launched()` + suppress `DBOSInitError` makes the teardown safe even when a test failed mid-setup.
  - **FastAPI integration mode**: passing `fastapi=app` to `DBOS(...)` attaches HTTP tracing, but Capo's listener uses `lifespan="off"` on uvicorn (see `_serve_until_signal`), so the auto-startup hook does NOT fire. We call `launch_dbos()` explicitly before `uvicorn.Server.serve()` — satisfies §5.6 "boot waits for DBOS before serving webhook" without relying on uvicorn lifespan.

- Issues encountered:
  - DBOS prints a "SQLite is for development and testing" advisory on every launch — already documented in spike S-2; the test suite tolerates it.
  - `DBOS.workflow()` decorator MUST run AFTER `DBOS.launch()` — confirmed in the integration test `test_clean_boot_accepts_workflow_registration`.

### Project Patterns (additions)

- **DBOS lifecycle helpers** live in `capo/workflows/__init__.py`. Pattern: `init_dbos(settings, fastapi_app=app)` → `await asyncio.to_thread(launch_dbos)` → `await asyncio.to_thread(destroy_dbos)` in `finally`. Future workflow modules (`capo/workflows/delegation.py`, `capo/workflows/approval.py`) import `DBOS` from `dbos` directly and register decorators; they MUST be imported AFTER `init_dbos` so the `@DBOS.workflow()` decorator sees the live instance.
- **Typed init errors**: follow the `DBOSInitError(operation, path, message)` pattern — operation is a short slug (`init`, `launch`, `mkdir`, `db_corrupt`, `path_collision`, `destroy`), path is the file involved, message is human-readable. `capo.main` exits 2 with a single-line stderr on any of these (no traceback).

### Key Decisions (additions — Phase 3)

- **DBOS launches BEFORE dispatcher starts and BEFORE boot sweep**. This is the §5.6 "boot waits for DBOS" criterion. If DBOS init/launch fails, no traffic is accepted (the listener never binds).
- **`run_admin_server=False`** in `DBOSConfig` — Capo has its own `/healthz` endpoint (§6.5); we don't want a second HTTP bind in V1.
- **`workflow_completion_timeout_sec=0`** (DBOS default) on shutdown. Phase 3 relies on DBOS at-least-once + our idempotency keys (§5.6 acceptance criterion) to survive abrupt teardown — no need for graceful drain in V1.
- **`fastapi=app` on `DBOS(...)`** is "belt + suspenders" — uvicorn `lifespan="off"` makes the auto-launch hook a no-op, but the HTTP tracing DBOS attaches is still useful.

### File Map (additions)

- `capo/workflows/__init__.py` — DBOS configuration + lifecycle (`init_dbos`, `launch_dbos`, `destroy_dbos`, `DBOSInitError`, `is_launched`, `current_dbos_db_path`). Future workflow bodies (Task #29+) land in `capo/workflows/delegation.py`, `capo/workflows/approval.py`, `capo/workflows/_idempotency.py`.
- `tests/test_workflows_config.py` — Phase 3 task #28 acceptance criteria tests (17 cases).

### Task [29]: Implement step idempotency framework - PASS

- Files modified:
  - `capo/workflows/_idempotency.py` (new, ~430 lines) — `idempotency_key()` + `@idempotent_step` + typed errors.
  - `tests/test_idempotency.py` (new, 24 tests).

- Key learnings:
  - **DBOS step-state IS the idempotency persistence mechanism.** `@DBOS.step` checkpoints step return values by `(workflow_id, function_id)` and replays the cached value on workflow re-entry. Our `@idempotent_step` composes around `@DBOS.step` — no separate "skip if key seen" check needed.
  - **`DBOS.SetWorkflowID(...)`** (from top-level `dbos` package) is the test fixture for resume semantics. Without it, every workflow invocation gets a fresh ID and replay never triggers.
  - **`DBOS.set_event` / `DBOS.get_event`** is the workflow-local K/V store; `get_event(wf_id, key, timeout_seconds=0)` returns `None` for missing keys (some versions raise — catch both). Used to persist `{key, payload_fp}` records under `capo.idem.<step_name>.<key>` for operator audit. set_event is fire-and-forget; we layer in-process mismatch detection on top.
  - **`DBOS.workflow_id`** is a property that raises outside a workflow context — wrap access in try/except with sentinel `__capo_idem_no_workflow__` for unit tests.
  - **Canonicalization rules for stable hashing**: tuples → lists, dicts sorted by `str(key)` recursively (including inside lists), small value-objects fall back to `f"{module}.{qualname}:{value}"`, functions/generators/live sockets → `TypeError` at key-derivation time (fail fast).
  - **Key format**: `"<step_name>:<32 hex chars of sha256>"`. Payload fingerprint is 16 hex chars (64 bits).
  - **Deterministic args are explicit, not implicit.** Callers pass `deterministic_args=lambda *a, **kw: (...)` to exclude non-deterministic inputs (timestamps, fresh UUIDs).

### Project Patterns (additions)

- **`@idempotent_step(step_name=..., deterministic_args=lambda ...: (...))`** is now the canonical decorator for ANY `@DBOS.step` with externally-visible side effects. Callers MUST provide a `deterministic_args` selector when any args are non-deterministic across retries.
- **Idempotency-key derivation lives ONLY in `capo/workflows/_idempotency.py`.** Future sites (Tasks #30+) MUST import `idempotency_key` from here. The `.idempotency_key_for(*args, **kwargs)` attribute on a decorated step exposes the same key — handy for `Idempotency-Key` HTTP headers on AMC sends.
- **`IdempotencyMismatchError`** is the loud-failure signal for "same key, different payload". Should NEVER be caught.

### Key Decisions (additions — Phase 3)

- **Reused DBOS step-state for persistence rather than building a parallel `state.db` table.** Per spec §5.6 verbatim. Keeps idempotency invariant inside `dbos.db` (replicated by Litestream per §8.1).
- **Heartbeat key shape matches §5.13 verbatim**: `(delegation_id, str(threshold_seconds))`.

### File Map (additions)

- `capo/workflows/_idempotency.py` — `idempotency_key()`, `idempotent_step()`, `IdempotencyError`, `IdempotencyMismatchError`. Imports `dbos` lazily.
- `tests/test_idempotency.py` — 24 tests.

### Notes for Task #30 (monitor_delegation workflow)

- `notify_user` (§5.6): `@idempotent_step(step_name="notify_user", deterministic_args=lambda delegation_id, **_: (delegation_id,))`.
- Heartbeat (§5.13): `@idempotent_step(step_name="heartbeat", deterministic_args=lambda delegation_id, threshold_seconds, **_: (delegation_id, str(threshold_seconds)))`.
- Subprocess-spawn step (§5.6 restart contract): use `delegation_id` as the deterministic arg; the `session_id` returned by the first invocation is the resume token for subsequent re-entries (DBOS replays cached return value).
- To get the key BEFORE invoking the step (e.g. for `Idempotency-Key` HTTP header), call `step.idempotency_key_for(*same_args, **same_kwargs)`.

### Task [30]: Implement monitor_delegation DBOS workflow - PASS

- Files modified:
  - `capo/workflows/delegation.py` (new, ~640 lines) — `monitor_delegation` workflow, drain/persistence steps, in-process subprocess registry, returncode classifier, lazy DBOS registration helper.
  - `tests/test_workflows_delegation.py` (new, 18 tests).

- Key learnings:
  - **DBOS `@DBOS.workflow()` registration MUST happen lazily after `DBOS.launch()`.** Defer the decorator call to first invocation via `_register_workflow()` (module-level lock + flag). Keeps the module importable before DBOS is launched.
  - **`idempotent_step` lambdas need flexible signatures.** DBOS forwards arguments positionally; `lambda delegation_id, status, **_: ...` trips a TypeError. Use `lambda *args, **kwargs: (args[0] if len(args) >= 1 else kwargs.get(...), ...)` instead.
  - **`DBOS.set_event` is async-only when inside an event loop in DBOS 2.21.0.** Idempotency framework already wraps in `contextlib.suppress`; warning surfaces in test output but tolerated.
  - **Returncode classification**: rc==0 → completed; rc<0 with -rc in {SIGTERM,SIGKILL,SIGINT} → killed; rc<0 with other signals (SEGV, ABRT) → failed; rc>0 → failed.
  - **"No re-spawn on re-entry" satisfied by an in-process registry** (`_DELEGATION_REGISTRY`). Spawn site (Task #35) will register; workflow body reads. On restart, registry is empty → Task #35 replaces the current "return failed" branch with a resume-spawn step (`claude --resume <session_id>`).
  - **Reader-handle injection via `DelegationProcessHandle`**: workflow body can't pickle live asyncio objects through dbos.db, so a process-local registry bridges spawn site to monitor. DURABLE state stays in `delegations` row; registry only carries live objects.

### Project Patterns (additions — Phase 3)

- **Lazy DBOS workflow registration**: wrap `@DBOS.workflow()` calls in `_register_workflow()` helper guarded by module-level lock + `_workflow_registered` flag. Call from public entrypoints.
- **`idempotent_step` lambda pattern**:
  ```python
  @idempotent_step(
      step_name="...",
      deterministic_args=lambda *args, **kwargs: (
          args[0] if len(args) >= 1 else kwargs.get("delegation_id"),
          ...
      ),
  )
  ```
- **In-process delegation registry**: module-level dict with `threading.Lock` for live subprocess + reader handles. Durable state stays in `state.db`.

### Key Decisions (additions — Phase 3 / Task #30)

- **Drain step is a `@DBOS.step` that awaits `reader_handle.wait()`** rather than re-running the reader. Reader was started by the spawn site (Task #21's `start_reader`); workflow keeps it alive until EOF. Preserves §6.1 zero-pipe-block contract.
- **Returncode polling defaults to 1s** per §5.6. Tests override to 5–20ms. Polling loop also checks the row status each cycle so out-of-band kills are honored without overwriting.
- **Out-of-band kill detection**: after subprocess exits, re-read row status. If already `killed`, do NOT overwrite. Race-safe via `begin_immediate_with_retry`.
- **Workflow-exception → row=failed**: wrap `_monitor_body` in try/except, set row to `failed` with `summary="monitor crashed: <repr>"` AND re-raise so DBOS marks workflow failed in `dbos.db`.
- **`max_runtime_s` optional** (`None` = no ceiling).

### File Map (additions)

- `capo/workflows/delegation.py` — `monitor_delegation`, `DelegationProcessHandle`, `register_delegation_subprocess`, `unregister_delegation_subprocess`, `MonitorResult`, `STATUS_*` constants, `DEFAULT_POLL_INTERVAL_S`, `_classify_returncode`. Handoff point for Task #35: `register_delegation_subprocess(handle)`.
- `tests/test_workflows_delegation.py` — 18 tests including 12.5 MiB perf regression inside DBOS context.

### Notes for downstream Phase 3 tasks

- **Task #31 (restart-resume via `claude --resume session_id`)**: SELECT rows with `status='running'` whose workflow ID isn't tracked by DBOS; resume via session_id (preferred) or mark `failed`. The current `_monitor_body` branch handling missing registry entry is a model — see `_lookup_delegation(...) is None`.
- **Task #32 (`summarize_run` step)**: add as `@idempotent_step` in delegation.py, called from `_monitor_body` BEFORE `_persist_terminal_step`. Feeds row's `summary` column.
- **Task #33 (`notify_user` step)**: `deterministic_args=lambda *args, **kwargs: (args[0] if len(args) >= 1 else kwargs.get("delegation_id"),)` per Task #30 finding.
- **Task #34 (heartbeat)**: per-threshold idempotent step inside the polling loop; deterministic args = `(delegation_id, str(threshold_seconds))` per §5.13.
- **Task #35 (replace Phase 2 in-process monitor)**: wire `delegate_to_claude_code._monitor_wrapper` to (1) call `register_delegation_subprocess(handle)` right after spawn, (2) invoke `monitor_delegation(delegation_id)` instead of `_spawn_monitor`. Add the spawn-or-resume step that runs FIRST inside the workflow body when registry is empty.

### Task [31]: Implement restart-resume contract via claude --resume - PASS

- Added "Restart resume (Task #31)" region in `capo/workflows/delegation.py` (~330 lines): `_resume_spawn_step` (idempotent step) + helpers. Replaced `_monitor_body`'s failed-fast branch with the resume contract.
- `tests/test_workflows_delegation_resume.py` — 13 tests.

- Key learnings:
  - **§7.5/S-3 §4.2 resume invocation is parameter-light**: `claude --resume <session_id> --output-format stream-json --verbose [--model X]`. No `-p`/prompt (inherited from rollout), no `--permission-mode` (inherited), cwd is original workspace.
  - **`session_id_subagent` overwrite refusal is by OMISSION, not active rejection.** The resume path never calls `_await_session_id`, so the synthetic session_id from a failed-resume event is observed but not persisted. Cleaner than mutating reader semantics.
  - **Workflow args must be JSON-serializable for DBOS.** `Path` has no stable JSON form; registered workflow takes `db_path_str: str | None`, public `monitor_delegation` takes `db_path: Path | None` and converts at the boundary.
  - **Parallel-task interference**: Tasks #31/#32/#33 all touched `delegation.py` concurrently. Resolved via banner-delimited regions; defensive try-import pattern for cross-task symbol references.

### Project Patterns (additions — Phase 3 / Task #31)

- **Restart-resume step pattern**: registry guard → row lookup → action-discriminator return value (`{"action": "spawned" | "row_terminal" | "missing_session_id" | "resume_failed_is_error" | …}`) → all side effects through `begin_immediate_with_retry`.
- **Parallel-task defensive imports**: when adding a hook in `delegation.py` that calls a symbol another parallel task is responsible for, wrap symbol lookup in try-import: `try: from capo.workflows.delegation import X; except ImportError: X = None`.

### Key Decisions (additions — Phase 3 / Task #31)

- **Resumed CC subprocess goes through the SAME monitoring flow** as the original spawn. After `_resume_spawn_step` registers the new handle, `_monitor_body` falls through to the existing drain + poll + terminal-persist logic.
- **`db_path` and `claude_binary` are workflow kwargs**, not module-level globals. Task #35 will populate from `CapoDeps`. Default `claude_binary="claude"`.

### Notes for Task #35

- Pass `db_path` and `claude_binary` from settings: `monitor_delegation(delegation_id, db_path=ctx.deps.settings.paths.db_path, claude_binary=ctx.deps.settings.agents.claude_code.binary or "claude")`.
- Resume step is wired automatically — Task #35 just needs to ensure workflow is invoked on EVERY entry (cold-boot recovery sweep included).

### Task [32]: Implement summarize_run DBOS step - PASS

- Added `# === summarize_run (Task #32) ===` region (~360 lines). `summarize_run` is `@DBOS.step` + `@idempotent_step(delegation_id)`. Hook in `_monitor_body` captures return value and feeds into `_persist_terminal_step`. `tests/test_workflows_summarize_run.py` (28 tests).

- Key learnings:
  - **Determinism contract enforced by Task #32 amendment**: §5.6 spec originally called for an LLM-generated summary, but that breaks DBOS at-least-once step semantics (LLM replays produce different strings). Task #32 downgrades to a deterministic one-liner built from durable DB state. LLM enrichment can layer later as a non-checkpointed background step.
  - **Output contract** is a fixed-shape string: `"status=<S> runtime=<row-derived> bytes(out=<n> err=<n> evt=<n>) rows(out=<n> err=<n> evt=<n>) | last[<kind>]=<excerpt>"`.
  - **Status field uses `status_override`** (passed from hook) because summarize_run runs BEFORE `_persist_terminal_step` writes the row's status. Pure-functional on inputs.
  - **Runtime field** uses `ended_at - started_at` from the row, NEVER `datetime.now()`. Renders `runtime=?` when NULL/unparseable/negative.
  - **Last-segment selection**: walk tail in reverse-chronological; first stderr OR assistant-event-text wins. Fall back to last non-empty stdout. `last[none]=no output` if nothing.
  - **`_persist_run_summary` writes ONLY `summary` column** (no status, no ended_at) so terminal-persist's status+ended_at write isn't interfered with.

### Project Patterns (additions — Phase 3 / Task #32)

- **Deterministic summary steps** source ALL data from durable DB state (no `datetime.now`, no `uuid`, no `random`, no external services).
- **`status_override` pattern**: when a step needs to reflect a value to be written immediately after, pass explicitly rather than reading post-write column.

### Notes for downstream tasks (Task #32)

- **Task #34 (heartbeat)** can read `delegations.summary` to surface "last status excerpt" without re-running summarize_run.
- **LLM enrichment (future)**: layer as NON-checkpointed background step writing to separate `summary_llm` column.

### Task [33]: Implement notify_user DBOS step (no agent re-entry) - PASS

- Added Task #33 region at end of `delegation.py`: `NotifyError`, `_NOTIFY_PERMANENT_CODES`, `_AmcSenderFn`, `register_amc_sender`/`_lookup_amc_sender`, `_channel_id_from_parent_thread`, `_read_row_for_notify`, `_compose_notify_body`, `notify_user`. Hook in `_monitor_body` calls `notify_user(delegation_id, str(db_path))` AFTER `_persist_terminal_step`. `tests/test_workflows_notify_user.py` (19 tests).

- Key learnings:
  - **AMC sender registry pattern**: module-level `_AMC_SENDER` (registered by `capo.main.amain` after `AMCClient` construction) bridges DBOS workflow body to live `AMCClient` (not picklable). Signature: `Callable[[channel_id, text, idempotency_key], Awaitable[Any]]` — typed as plain callable to avoid pulling `httpx` into workflow imports.
  - **Idempotency key as HTTP header**: `notify_user.idempotency_key_for(delegation_id, db_path)` returns same key wrapped step uses internally. Passed as AMC `Idempotency-Key` HTTP header → AMC server-side dedupe + DBOS step replay align on the same token.
  - **Deterministic message body**: `_compose_notify_body(delegation_id, status, summary)` is pure — uppercase status, collapsed-newline summary, no timestamps/env/randomness.
  - **Permanent vs transient AMC errors**: permanent codes (PLATFORM_AUTH, CHANNEL_NOT_FOUND, ATTACHMENT_TOO_LARGE, VALIDATION_ERROR) → `NotifyError` re-raised; transient → propagate original for DBOS retry. AMC `Idempotency-Key` keeps receiver deduped across retries.
  - **No-op paths return `{"notified": False}`**: missing row / non-terminal / no AMC channel (parent_thread_id without `amc:` prefix) → log + no-op.
  - **Channel-id extraction**: `parent_thread_id` is `amc:<channel_id>` per `capo.memory.conversation.thread_id_for_amc` (§5.7).

### Project Patterns (additions — Phase 3 / Task #33)

- **Module-level service-registry for non-picklable DBOS step deps**: `_REGISTRY` dict (or single slot) + `threading.Lock` + `register_*`/`_lookup_*` helpers. Boot registers; workflow body looks up by ID. Durable state in `state.db`; registry only carries live objects.
- **`@idempotent_step` + `.idempotency_key_for(...)` as HTTP idempotency token**: side-effecting step calling external service → pass `step.idempotency_key_for(*det_args)` verbatim to dedupe across DBOS retries AND restart resume.
- **Typed `NotifyError(code, message, *, delegation_id)`**: exposes AMC error code as `.code` for log filtering.

### Notes for Task #35 + downstream (Task #33)

- Wire `capo.main.amain` to call `register_amc_sender(amc_client.send_adapter)` BEFORE `dispatcher.start()`. Without this, terminal notifications raise `NotifyError(code="NO_SENDER_REGISTERED")`.
- **Task #34 (heartbeat)**: same pattern. Deterministic args `(delegation_id, str(threshold_seconds))` per §5.13. Reuse `_AMC_SENDER` (step_name prefix separates idempotency-key namespace).

### Task [34]: Implement heartbeat step with frozen-threshold idempotency - PASS

- Added `# === heartbeat (Task #34) ===` region at EOF of `delegation.py` (~470 lines). Public API: `DEFAULT_HEARTBEAT_THRESHOLDS_S = (300, 900, 3600)`, `heartbeat` step, `set/get_heartbeat_thresholds`, `set/get_heartbeat_poll_interval`, `_run_heartbeat_poller`. Hook in `_monitor_body` launches poller alongside drain_task; cancels on subprocess exit. `tests/test_workflows_heartbeat.py` (43 tests).

- Key learnings:
  - **Wall-clock anchor MUST come from durable column (`started_at`), NOT process-local clock**. Workflow restart computes the same elapsed and crosses the same thresholds — @idempotent_step key + DBOS step-state replay deduplicate per-threshold sends.
  - **Step-name prefix gives heartbeat its own idempotency-key namespace** (`heartbeat:<hex>` vs `notify_user:<hex>`) — no extra namespacing needed.
  - **Heartbeat poller as a concurrent asyncio task** parallels drain task pattern. Cancel on subprocess exit → "delegation completes before any threshold: no heartbeats" satisfied for free.
  - **`HeartbeatConfig` already exists in `capo/config.py`** (`intervals_seconds: list[int]`); Task #35 wires via `set_heartbeat_thresholds(settings.heartbeat.intervals_seconds)` at boot.

### Project Patterns (additions — Phase 3 / Task #34)

- **Module-level frozen configuration with setter/getter pair** for runtime tunables: `DEFAULT_X`, `_X_LOCK`, `_X`, `set_x(None)` for reset, `get_x() -> tuple[...]`. Test autouse fixture resets via `set_x(None)`.
- **Format-threshold label helper**: pure function `(300, 900, 3600, 7245) -> ("5m", "15m", "1h", "2h45s")`. No locale, no env, no timestamp.

### Task [35]: Replace Phase 2 in-process monitor with DBOS workflow handoff - PASS

- `capo/tools/claude_code.py`: removed `_spawn_monitor`; replaced step-10 `_monitor_wrapper` with DBOS handoff (pre-flight `is_launched()` check raises `DBOSNotLaunchedError` + marks row failed; then `register_delegation_subprocess(...)`; fire-and-forget asyncio task captures session_id then invokes `monitor_delegation(...)`).
- `capo/main.py`: `amain` now calls `register_amc_sender(_make_amc_send_adapter(amc_client))` + `_cold_boot_resume_sweep(settings)` AFTER `launch_dbos()` and BEFORE `dispatcher.start()`. Clears AMC sender in shutdown `finally`.
- Updated 4 existing Phase 2 test files with autouse DBOS-clean fixtures + scaffold launches DBOS. Added `tests/test_task35_dbos_handoff.py` (4 tests).

- Key learnings:
  - **Fire-and-forget handoff is the right call** for `delegate_to_claude_code`: awaiting `monitor_delegation` would block agent tool return until CC subprocess exits, defeating §5.3 "return DelegationHandle immediately". DBOS owns durability in `dbos.db`; local asyncio task is just convenience. If process dies before local task runs, cold-boot sweep picks the row up.
  - **Session_id capture must stay before workflow invocation** — Task #31 resume step inside `monitor_delegation` reads `session_id_subagent` from row to re-spawn via `claude --resume`. On SessionIdCaptureTimeout: mark row failed, kill subprocess, do NOT invoke workflow.
  - **AMC sender adapter pattern**: `register_amc_sender` expects positional `(channel_id, text, idempotency_key)` but `AMCClient.send` takes `idempotency_key` keyword-only. 4-line adapter (`_make_amc_send_adapter`) bridges without coupling workflow package to httpx.
  - **Cold-boot sweep uses fire-and-forget asyncio tasks** so sweep returns quickly and listener can bind socket. Each task wraps workflow call in try/except so one resume failure doesn't abort others.

### Project Patterns (additions — Phase 3 / Task #35)

- **DBOS workflow handoff at spawn site** — canonical wiring for any agent tool driving in-flight subprocess: (1) INSERT `running` row → (2) spawn subprocess + start reader → (3) pre-flight `is_launched()` check (raise typed error + mark row failed if not) → (4) `register_delegation_subprocess(...)` → (5) fire-and-forget asyncio task captures session_id then `await monitor_delegation(...)` → (6) return `DelegationHandle` immediately.
- **`capo.main.amain` is the canonical registration site** for module-level service registries (BEFORE `dispatcher.start()`). Pattern: `register_amc_sender(_make_amc_send_adapter(amc_client))` + `set_heartbeat_thresholds(settings.heartbeat.intervals_seconds)` + `_cold_boot_resume_sweep(settings)`.

### Notes for downstream tasks (Task #35)

- **Task #45 (`delegate_to_codex_cli`)**: mirror the Task #35 pattern verbatim — same 6-step spawn-site shape. Codex doesn't share `register_delegation_subprocess` semantics yet (registry is CC-specific), but workflow handoff + cold-boot sweep contract is identical.
- **Phase 4 approval**: when spawn site raises `ApprovalRequired`, no row INSERTed, no workspace created. DBOS handoff only runs after approval check, so this is unaffected.

### Task [36]: Phase 3 checkpoint - PASS

- `tests/test_phase3_checkpoint.py` (new, ~520 lines) — 3 integration tests anchoring §9.3 checkpoint gate. 479/479 passing.
- `internal/specs/spikes/S-5-phase3-checkpoint.md` (new) — operator runbook for unmodified 90-min wall-clock variant.
- `internal/specs/capo-SPEC.md` — `> _Updated by Phase 3 checkpoint on 2026-05-11._` footnotes on §5.6 and §5.13. §15.4 Phase 3 change log row appended.

- Key learnings:
  - **Compressed restart-resume design** skips Phase 1 (initial spawn) entirely and starts from post-restart steady-state: `status='running'` row + captured `session_id_subagent` + empty `_DELEGATION_REGISTRY`. Calling `monitor_delegation(...)` from that state IS what the cold-boot sweep does in production.
  - **AMC stub with dedupe counter**: track `_delivered_keys: set[str]` alongside `calls: list[dict]`. `unique_deliveries == 1` while `len(calls) == 2` proves the §5.6 receiver-side dedupe invariant.
  - **3-concurrent test routing**: `asyncio.create_subprocess_exec` hijack with per-`sid` dispatch — each delegation gets its own fake script + workspace + parent_thread_id.
  - **Heartbeat suppression in compressed tests**: thresholds default to (300, 900, 3600); compressed tests run <60s so thresholds never cross. Belt-and-brace with `thresholds=[3600]` + autouse-fixture-reset of `set_heartbeat_poll_interval(None)`.

### Project Patterns (additions — Phase 3 / Task #36)

- **Phase-checkpoint test layout**: `tests/test_phaseN_checkpoint.py` is canonical for checkpoint-gate regression suites.
- **Operator-runbook-as-spike**: when an acceptance criterion is wall-clock-bound and infeasible in CI, document as `internal/specs/spikes/S-N-*.md` with pre-reqs, step-by-step verification signals (SQL queries, log events, AMC traffic), failure-mode recovery table, sign-off checklist.
- **Spec inline footnote pattern**: `> _Updated by Phase N checkpoint on YYYY-MM-DD._` under the section heading is the canonical "validated end-to-end" signal.

### Notes for Phase 4 (Task #37+)

- Phase 4 checkpoint (`tests/test_phase4_checkpoint.py`) follows same shape: one test per §9.4 row, fakes for Codex CLI mirroring CC fakes, AMC stub with dedupe counter for approval round-trip idempotency.
- When Phase 4 adds `approvals` table, kill_delegation test needs a parallel idempotency check (approval round-trip → DBOS recv → resolution; per-approval_id idempotency key, no double-resolve).

### Task [37]: Amend §5.4 and §7.5 with Codex spawn/event/resume contract from S-1 - PASS

- `internal/specs/capo-SPEC.md` §5.4 + §7.5 amended inline with S-1 findings. Footnotes refreshed to 2026-05-11. New §15.4 "S-1 (re-amend)" row appended after Phase 3 row.

- Key learnings:
  - **§5.4 vs §7.5 split**: §5.4 (Feature Acceptance Criteria) repeats high-signal contract bits — spawn argv, resume argv, forbidden flags — because implementers read §5.4 first. §7.5 carries full normative reader contract (event taxonomy, edge cases, sandbox-mode notes).
  - **Footnote canonical pattern**: `> _Updated by spike S-N on YYYY-MM-DD._` directly below amended subsection.

### Notes for Tasks #45 (delegate_to_codex) and #46 (Codex resume in monitor_delegation)

- §5.4 spawn argv and §5.4 resume argv are implementer-facing source of truth. Resume flag set is exhaustive — additional flags will error per S-1 §4.2.
- Task #46 implementation MUST guard `codex exec resume` calls behind per-delegation `status=running` mutex (single-threaded per `thread_id`).
- Pin `MIN_CODEX_VERSION = "0.130.0"` mirroring `MIN_CLAUDE_CODE_VERSION` pattern.

### Task [38]: Add approvals table via Alembic migration - PASS

- `migrations/versions/002_approvals.py` (new) — `approvals` table + 2 indexes (`idx_approvals_pending_sweep` on `(status, requested_at)`, `idx_approvals_parent_thread` on `(parent_thread_id, status)`). Raw DDL via `op.execute` with `IF NOT EXISTS`, mirrors `001_init.py`. CHECK constraints on `request_type` and `status`. No FK to delegations (§5.8).
- `tests/test_approvals_migration.py` (new, 11 tests). `tests/test_migrations.py` aggregate test updated.

- Key learnings:
  - **Project uses `migrations/` not `alembic/versions/`** as `script_location`.
  - **`001_init.py` is the canonical migration template** — raw DDL via `op.execute`, `IF NOT EXISTS` everywhere, `_UPGRADE_STATEMENTS`/`_DOWNGRADE_STATEMENTS` tuples.
  - **Schema introspection tests pattern**: per-table `tests/test_<table>_migration.py` for Phase 4+. Pin column shape via `PRAGMA table_info` tuple, index columns + ORDER via `PRAGMA index_info`, CHECK constraints via insert-then-IntegrityError, FK invariants via `PRAGMA foreign_key_list`.
  - **`requested_at`/`decided_at` as TEXT (ISO 8601)**, not `TIMESTAMP`. Caller-controlled UTC ISO timestamps for DBOS replay determinism — NOT `CURRENT_TIMESTAMP` (non-deterministic per step retry).

### Key Decisions (additions — Phase 4 / Task #38)

- **CHECK constraint on `request_type`** enumerates `('shell_exec','delegate_out_of_root','kill_delegation')`.
- **CHECK constraint on `status`** enumerates `('pending','approved','denied','expired','cancelled')`. Note `cancelled` is new vs §7.3.
- **No FK from `approvals` to `delegations`** — §5.8 explicit. Linkage stays in `request_payload` JSON.

### Notes for Task #39 (approval workflow)

- INSERT: `INSERT INTO approvals (approval_id, requested_at, request_type, request_payload, status, requester_user_id, parent_thread_id, workflow_id) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)`. `requested_at` MUST be caller-supplied UTC ISO 8601 (DBOS replay determinism).
- Timeout sweeper: `SELECT approval_id, requested_at, workflow_id FROM approvals WHERE status='pending' AND requested_at < ?` — `idx_approvals_pending_sweep` makes this an index scan.
- Inbound `/approve <id>` routing: `SELECT approval_id, status FROM approvals WHERE parent_thread_id=? AND status='pending' ORDER BY requested_at DESC LIMIT 1`.
- Resolution: `UPDATE approvals SET status=?, decided_at=?, resolved_by=?, reason=? WHERE approval_id=?`.

### Task [39]: Implement approval DBOS workflow - PASS

- `capo/workflows/approval.py` (new, ~810 lines) — `request_approval` workflow, `ApprovalDecision`, `ApprovalError`, `notify_approval` idempotent step, `force_resolve_approval` external-cancel helper, lazy `_register_workflow`. `tests/test_workflows_approval.py` (17 tests).

- Key learnings:
  - **`DBOS.workflow_id` (property)** is the canonical way for a workflow body to know its own id — persist to `approvals.workflow_id` so the inbound dispatcher (Task #40) routes via `DBOS.send(workflow_id, ...)`.
  - **`DBOS.start_workflow_async` returns `WorkflowHandleAsync`** with `.get_result()` — canonical "fire-and-await" pattern for integration tests.
  - **`SetWorkflowID(custom_id)`** context-manages workflow id assignment.
  - **`DBOS.recv_async(topic, timeout_seconds)` returns `None` on timeout** — canonical signal for `expired` status. SQLite polling floor ~1s.
  - **Permanent-AMC-error policy**: re-raise `ApprovalError` and leave row `pending` (no terminal write). Operator can re-fire with same `approval_id` after fixing AMC.

### Project Patterns (additions — Phase 4 / Task #39)

- **Approval workflow body pattern**: (1) INSERT pending (idempotent step keyed by `approval_id`) → (2) AMC notify (idempotent step, same key feeds `Idempotency-Key` header) → (3) `DBOS.recv_async("approval_decision", timeout_seconds=...)` → (4) normalize decision verb/noun → (5) check external resolve race → (6) UPDATE row via idempotent step → (7) return `ApprovalDecision`.
- **External force-resolution pattern**: `force_resolve_approval(workflow_id, status="cancelled", ...)` uses `DBOS.send_async(workflow_id, payload, "approval_decision")` to wake an awaiting workflow. Public API for the killer path (Task #42).
- **Decision normalization**: accept BOTH verb (`approve`/`deny`/`cancel`/`expire`) AND noun forms.

### Notes for downstream Phase 4 tasks

- **Task #40 (inbound webhook routing for `/approve` / `/deny`)**: parse verb from AMC message, look up pending approval by `parent_thread_id` + `status='pending'` (use `idx_approvals_parent_thread`), then `await DBOS.send_async(row.workflow_id, {"status": "approve|deny", "resolved_by": user_id, "reason": optional_text}, APPROVAL_TOPIC)`. Import `APPROVAL_TOPIC` from `capo.workflows.approval`.
- **Task #41 (`shell_exec` approval gating)**: caller generates `approval_id = uuid.uuid4().hex`, captures `requested_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")` BEFORE calling `request_approval`. Branch on `decision.status`: `approved` → proceed; `denied`/`expired`/`cancelled` → raise.
- **Task #42 (`kill_delegation`)**: call `force_resolve_approval(workflow_id, status="cancelled", resolved_by="system:killer", reason=f"delegation {delegation_id} killed")`.
- **Pending-approval sweeper (future)**: `SELECT approval_id, workflow_id, requested_at FROM approvals WHERE status='pending' AND requested_at < ?` → for each, `force_resolve_approval(workflow_id, status="expired", reason="boot-time timeout reconciliation")`.

### Task [45]: Implement delegate_to_codex tool (Codex spawn + reader) - PASS

- `capo/tools/codex.py` (new, ~770 lines) — `CodexBrief`, `CodexDelegationHandle`, `delegate_to_codex`, `MIN_CODEX_VERSION="0.130.0"`, `CodexBinaryError`, `ThreadIdCaptureTimeout`, `DBOSNotLaunchedError`. Mirrors `claude_code.py` 1:1.
- `capo/tools/_subprocess.py` extended — first-event capture hook now fires on EITHER top-level `session_id` (CC) OR `thread_id` (Codex). Additive, backwards-compat.
- `tests/test_delegate_to_codex.py` (new, 26 tests). 529/529 full suite pass.

- Key learnings:
  - **Codex's session token is `thread.started.thread_id`, NOT `session_id`.** Spike S-1 §5: Codex emits `{"type":"thread.started","thread_id":"<uuid>"}` as first JSON event. Reader hook trigger on EITHER field — single hook serves both agents.
  - **Reuse `session_id_subagent` column for Codex thread_id.** Codex's thread_id IS the resume token. No schema change.
  - **Spec §5.4 argv canonical form**: `codex exec --json --skip-git-repo-check --sandbox <mode> -C <workspace> [--model <m>] "<prompt>"`. `-C` points at WORKSPACE (worktree), not raw `repo_path`. Trailing positional prompt + `stdin=DEVNULL`.
  - **Pre-flight version check runs BEFORE any side effect.** `_check_codex_version` runs first; failure means no row, no workspace, no subprocess.
  - **Sandbox Literal validation at model construction**: `CodexSandboxMode = Literal["read-only", "workspace-write", "danger-full-access"]`.

### Project Patterns (additions — Phase 4 / Task #45)

- **Codex spawn-site shape mirrors Task #35 (CC spawn site)** with three localized differences: (1) argv builder for `codex exec`, (2) thread_id capture via `_await_thread_id`, (3) pre-flight `_check_codex_version`. All else identical.
- **First-event capture hook now generalized** — fires on EITHER `session_id` OR `thread_id` non-empty top-level string. Per-agent `_await_*` helper does final shape check.
- **`stdin=asyncio.subprocess.DEVNULL` on codex spawn** required (spec §7.5).

### Notes for Task #46 (Codex resume in monitor_delegation)

- Read `delegations.session_id_subagent` for Codex thread_id; pass to `codex exec resume --json --skip-git-repo-check [--model <m>] <thread_id> "<continuation_prompt>"`. NEVER `--sandbox`, `--ask-for-approval`, `-C`, `--add-dir`.
- Workflow resume step dispatch: existing `_resume_spawn_step` is CC-shaped. Task #46 reads `delegations.agent` and dispatches — Codex builds different argv, reuses generalized `thread_id`-based first-event capture.

### Task [40]: Wire approval inbound routing in dispatcher - PASS

- `capo/transport/dispatcher.py`: Added Task #40 region — slash-command interception via `parse_slash_command` (Task #41), DBOS.send_async routing on APPROVAL_TOPIC, friendly-reply constants (`APPROVAL_REPLY_NO_PENDING`, `APPROVAL_REPLY_UNKNOWN_ID`, `APPROVAL_REPLY_ALREADY_RESOLVED`, `APPROVAL_REPLY_STILL_CREATING`), `_select_approval_row`, `_send_approval_decision` (DBOS.send_async + sync fallback), `format_approval_recorded_reply`, `_try_handle_approval_command` + `_send_reply_and_mark_read`. Hooked in `_handle_envelope` AFTER user-resolution and BEFORE the agent run.
- `tests/test_dispatcher_approval_routing.py` (14 tests).

- Key learnings:
  - **DBOS.send_async with sync fallback**: mirror `force_resolve_approval` pattern. Tests must monkey-patch BOTH `send_async` AND `send` to fully simulate "workflow not found".
  - **Idempotency for repeat `/approve <id>`**: after row flips terminal, dispatcher's status check short-circuits and replies friendly WITHOUT calling DBOS.send_async.
  - **workflow_id NULL race window**: dispatcher tolerates with single 500ms sleep + retry.
  - **V1 cross-user authz**: any sender in same `parent_thread_id` may resolve (§5.8). `requester_user_id` is NOT enforced against inbound `user_id`.

### Task [41]: Implement /approve and /deny slash command parser - PASS

- `capo/transport/slash.py` (new, ~230 lines) — `parse_slash_command()` + frozen `SlashCommand`. Public surface: `parse_slash_command`, `SlashCommand`, `APPROVAL_ID_RE`, `RECOGNIZED_VERBS`.
- `tests/test_slash_parser.py` (47 tests).

- Key learnings:
  - **`uuid.uuid4().hex` is canonical approval-id shape** — `^[a-f0-9]{32}$`. Uppercase/dashed UUIDs fall through to reason text.
  - **Verb boundary regex**: `r"^/([A-Za-z]+)(?:\s|$)"` — `(?:\s|$)` boundary rejects `/approveall`/`/approver`.
  - **Multi-line truncation via `splitlines()[0]`** before strip.
  - **Pure function, no env/timestamp/random** — rendering-determinism style.

### Project Patterns (additions — Phase 4 / Task #41)

- **Pre-agent slash command parser pattern**: pure function in `capo/transport/slash.py` returning `SlashCommand | None`. `None` means "fall through to agent loop." Future expansion (`/new`, `/status`, `/clear`, `/kill`, `/override` for §5.10) extends this module with enlarged `verb` Literal.
- **`APPROVAL_ID_RE` is the canonical RE** — import from `capo.transport.slash`, don't redefine.

### Task [42]: Wire shell_exec approval gating to approval workflow - PASS

- `capo/tools/basic.py`: `shell_exec` is now `async`; non-allowlisted commands route through `request_approval`. Added `ApprovalRejected(decision)` typed error. Refactored body into `_shell_exec_precheck` / `_execute_command` / `_request_shell_approval`. `approval_id = uuid.uuid4().hex` and `requested_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")` BEFORE the call.
- `tests/test_shell_exec.py` — 25 tests converted to async.
- `tests/test_shell_exec_approval.py` (new, 6 integration tests).

- Key learnings:
  - **DBOS-not-launched fallback policy**: tool falls back to `ApprovalRequired` with explicit message "approval workflow is unavailable (DBOS not launched)" — keeps existing 25 unit tests working without DBOS.
  - **`request_payload` shape**: `{"command": pre.raw, "args": list(pre.argv), "cwd": cwd if cwd is not None else str(pre.resolved_cwd)}`. JSON-encoded in `request_approval` itself.
  - **`timeout_seconds` from settings.approval**: `getattr(settings.approval, "timeout_seconds", 0)`; if 0/missing, omit kwarg → workflow uses 24h default.
  - **`ApprovalDecision` is frozen dataclass** (not Pydantic). Imported behind `TYPE_CHECKING` in `basic.py` to avoid circular import.

### Project Patterns (additions — Phase 4 / Task #42)

- **Approval-gating spawn-site shape**: (1) sync structural pre-check raises `ApprovalRequired` for argv-contract violations; (2) generate `approval_id`/`requested_at` BEFORE the call (DBOS-determinism); (3) `request_payload = {...}`; (4) await `request_approval(...)`; (5) branch on `decision.status`: `approved` → execute; `denied`/`expired`/`cancelled` → raise `ApprovalRejected(decision)`.

### Task [43]: Wire delegation out-of-projects_root approval gating - PASS

- `capo/tools/_approval.py` (new) — centralized `ApprovalRequired`/`ApprovalRejected`/`ApprovalUnavailableError`, `request_tool_approval()`, `new_approval_id()`/`utc_iso_now()`, four `REQUEST_TYPE_*` constants. All approval-gated tools import from here. Legacy import path `from capo.tools.basic import ApprovalRequired` still works.
- `capo/tools/claude_code.py` + `capo/tools/codex.py` — out-of-root + dangerous-sandbox (Codex only) gates wired to workflow. `_classify_repo_path_scope` (returns `(in_scope, reason)`) replaced raising validator. `_prompt_hash` for payload field (avoid raw prompt leak).
- `migrations/versions/003_approvals_request_types.py` (new) — extends `request_type` CHECK to admit `delegate_dangerous_sandbox`. SQLite rename-create-copy-drop pattern (no `ALTER … DROP CHECK`).
- `tests/test_delegation_approval.py` (9 integration tests) + scaffold + migration test updates.

- Key learnings:
  - **Approval-gating canonical shape**: (1) classify structurally (split "raises" helper into `(bool, reason)`); (2) `new_approval_id()` + `utc_iso_now()` BEFORE call (DBOS-determinism); (3) `request_payload` carries `agent`, `repo_path`, `model`, `mode`/`sandbox`, `prompt_hash`, `reason` — NEVER raw prompt; (4) `await request_tool_approval(deps, request_type=..., request_payload=..., approval_id=..., requested_at=...)`; (5) `decision.status` branch; (6) catch `ApprovalUnavailableError` → translate to `ApprovalRequired`.
  - **Prompt-hash payload field**: `hashlib.sha256(rendered_prompt.encode()).hexdigest()[:16]` for audit correlation without leaking secrets.
  - **SQLite `ALTER TABLE` lacks `DROP CHECK`**: extending CHECK requires rename-create-copy-drop pattern. Migration 003 is canonical shape.
  - **Migration 003 stacking changes downgrade math**: `alembic downgrade -1` now drops only the CHECK rebuild; use `downgrade 001_init` to drop approvals.

### Project Patterns (additions — Phase 4 / Task #43)

- **`capo.tools._approval` is the single home for approval-gating types + helpers** — `ApprovalRequired`, `ApprovalRejected`, `ApprovalUnavailableError`, `request_tool_approval`, `new_approval_id`, `utc_iso_now`.
- **Path-classification refactor pattern**: when "raises ApprovalRequired" validator needs to become routable, split into `(in_scope: bool, reason: str | None)`. Hard structural rejections (unresolvable path, broken symlink) still raise — those aren't human-reviewable.
- **`request_payload` shape for delegations**: `{"agent", "repo_path", "model", "mode" (CC) or "sandbox" (Codex), "prompt_hash", "reason"}`. Uniform across tools.

### Task [44]: Wire kill_delegation approval gating - PASS

- `capo/tools/delegations.py` — owner-vs-non-owner gating: owner kills proceed directly; non-owner routes through `request_approval(request_type="kill_delegation")`. On kill, cascade-cancel pending approvals tied to delegation_id via `force_resolve_approval`. Added `_request_kill_approval`, `_cascade_cancel_tied_approvals`, `_find_pending_approvals_for_delegation`.
- `tests/test_kill_delegation_approval.py` (new, 4 integration tests).

- Key learnings:
  - **Owner check by `delegations.user_id == ctx.deps.user_id`**, NOT `requester_user_id`. Delegation's owner is canonical source of truth per §5.10.
  - **`_find_pending_approvals_for_delegation` uses SQLite `json_extract`**: `WHERE json_extract(request_payload, '$.delegation_id') = ?`. Tolerates `OperationalError` → empty list.
  - **`force_resolve_approval` is fire-and-tolerate**: each cascade target wrapped in try/except.
  - **Resolver tag format**: `f"system:killer:{requester_user_id}"`.

### Project Patterns (additions — Phase 4 / Task #44)

- **Kill-gating spawn-site shape**: (1) read row → 404 if missing; (2) terminal → no-op idempotent fast-path; (3) owner check → direct kill; (4) else `request_approval(request_type="kill_delegation", payload={"delegation_id", "reason"})`; (5) decision branch; (6) after kill proceeds, cascade-cancel pending approvals.
- **Cascade-cancel pattern via `json_extract`**: works for any future "kill X cascades to pending approvals tied to X" feature.

### Task [46]: Implement Codex resume contract in monitor_delegation - PASS

- `capo/workflows/delegation.py`: dispatch on `delegations.agent`; added `_build_codex_resume_argv`, `_execute_resume_spawn` (shared driver), `AGENT_CLAUDE_CODE`/`AGENT_CODEX`/`DEFAULT_CODEX_BINARY`/`CODEX_RESUME_CONTINUATION_PROMPT`/`RESUME_REASON_UNKNOWN_AGENT` constants. Threaded `codex_binary` through `_monitor_body` and DBOS workflow.
- `capo/main.py`: `_cold_boot_resume_sweep` resolves `codex_binary` from settings.
- `tests/test_workflows_delegation_codex_resume.py` (15 new tests).

- Key learnings:
  - **`agent` column already in `001_init.py`** with CHECK `('claude-code','codex')`. No migration needed.
  - **Codex resume failure mode differs from CC's**: CC emits synthetic `result` event with `is_error: true` (explicit `_first_event_is_error` branch); Codex on unknown thread_id exits non-zero with NO JSON event (first-event-timeout branch covers it). Shared `_execute_resume_spawn` takes `check_first_event_is_error: bool` switch.
  - **Codex re-emits SAME thread_id on successful resume**.
  - **Resume argv (spec §5.4/§7.5/S-1 §4.2)**: `codex exec resume --json --skip-git-repo-check [--model <m>] <thread_id> "<continuation>"`. MUST NOT pass `--sandbox`, `--ask-for-approval`, `-C`, `--add-dir`.
  - **Continuation prompt**: deterministic stub `"Resume after Capo restart. Continue the previous task; the original brief is inherited from the rollout."` Brief is INHERITED from rollout; trailing arg is next-turn user message.
  - **NULL / unknown `agent` → row failed with `RESUME_REASON_UNKNOWN_AGENT`** — refuse-to-spawn is safer default (wrong-binary spawn would corrupt the rollout).

### Project Patterns (additions — Phase 4 / Task #46)

- **Per-agent resume dispatch pattern**: read `delegations.agent`, dispatch on value. Each agent gets its own argv builder; shared `_execute_resume_spawn(...)` owns subprocess lifecycle + reader-handle wiring + idempotent terminal-status persistence.
- **Asymmetric first-event-failure handling**: CC has `is_error=true`; Codex exits with no JSON. Boolean flag on shared driver expresses contract difference without code duplication.
- **`monitor_delegation` workflow signature is now**: `(delegation_id, poll_interval_s, max_runtime_s, db_path, claude_binary, codex_binary)`. DBOS workflow positional shape changed.

### Task [47]: Phase 4 checkpoint - PASS

- `tests/test_phase4_checkpoint.py` (new, ~900 lines) — 6 integration tests anchoring §9.4 gate. 635/635 pass.
- `internal/specs/spikes/S-6-phase4-checkpoint.md` (new) — operator runbook for wall-clock-bound variants.
- `internal/specs/capo-SPEC.md` — Phase 4 footnotes on §5.4, §5.8, §5.9, §5.10, §7.5; §15.4 change log row.

- Key learnings:
  - **`DBOS.send_async`-based approval resolver pattern**: poll for pending approvals row, then `DBOS.send_async(row.workflow_id, payload, APPROVAL_TOPIC)`. Same protocol dispatcher (Task #40) uses — covers integration path without spinning up FastAPI listener.
  - **Compressed timeout test**: `object.__setattr__(settings.approval, "timeout_seconds", 2)` overrides frozen Pydantic field.
  - **Codex compressed restart-resume**: skip initial spawn — INSERT `status='running'` row with `agent='codex'` + `session_id_subagent` populated + empty `_DELEGATION_REGISTRY`, then call `monitor_delegation(...)`. Same steady-state as post-launchctl-kickstart.

### Notes for Phase 5 (Tasks #48-#63)

- Phase 5 checkpoint (`tests/test_phase5_checkpoint.py`, §9.5) follows same shape as Tasks #36/#47. Wall-clock variants → `internal/specs/spikes/S-7-phase5-checkpoint.md`.
- Phase 5 footnotes for §5.9 (cost caps), §5.10 (slash commands), §5.11 (multi-user), §5.12 (healthz), §6.5 (observability), §8.1 (Litestream backup).
- `AmcStub` + `_resolve_pending_approval_via_dbos` helpers in `tests/test_phase4_checkpoint.py` are reusable.

### Task [48]: Implement cost accountant - PASS

- `migrations/versions/004_costs.py` (new) — `costs` table + 2 indexes. `cost_id INTEGER PK AUTOINCREMENT`, nullable user_id/parent_thread_id/span_id.
- `capo/costs.py` (new, ~470 lines) — `PRICING_TABLE` (Claude 4 family: Opus 4.7 @ 15/75/1.50 per Mtok, Sonnet 4.6 @ 3/15/0.30, Haiku 4.5 @ 1/5/0.10; per Anthropic 2026-05-11), `compute_cost_usd` (Decimal, 6 dp via ROUND_HALF_UP), `record_cost` async, `record_run_usage` for Pydantic AI integration, `daily_total_usd` + `thread_total_usd` query helpers.
- `capo/transport/dispatcher.py` — wired `record_run_usage` after `_persist(new_messages)`. Failures logged + swallowed.
- `tests/test_costs.py` + `tests/test_costs_migration.py` (39 new tests).

- Key learnings:
  - **Pydantic AI usage taxonomy (1.93.0)**: `AgentRunResult.new_messages()` returns per-turn messages including `ModelResponse` with `.model_name` and `.usage` (`RequestUsage` with `input_tokens`/`output_tokens`/`cache_read_tokens`/`cache_write_tokens`). `RequestUsage.has_values()` returns True when any field non-zero.
  - **Cost-recording is per-`ModelResponse`, NOT per-`RunResult`** — single agent turn may make multiple API calls; each yields its own ModelResponse with correct per-call model attribution.
  - **`input_tokens` and `cache_read_tokens` are NON-OVERLAPPING buckets** in Anthropic's API contract. Bill independently.
  - **Decimal + 6-dp quantize** for money: `Decimal(tokens) * Decimal("3.00") / Decimal("1000000")`, quantize at the end.
  - **Model name normalization**: strip provider prefix via `rsplit(":", 1)[1]` (handles `vertex_ai:anthropic:claude-...`), then lowercase.

### Task [50]: Configure Logfire instrumentation - PASS

- `capo/observability.py` (new, ~330 lines) — `configure_logfire(settings)`, `with_span(name, **attrs)`, `is_configured()`, `instrument_fastapi_app(app)`, `LogfireMissingError`, `_reset_for_tests`. Module-level `_CONFIGURED` latch + threading.Lock for idempotency.
- `capo/config.py` — extended `ObservabilityConfig` with `token: SecretStr | None`, `service_name: str = "capo"`, `environment: str = "dev"`. All defaulted.
- `capo/main.py::amain` — `configure_logfire(settings)` BEFORE AMCClient/agent/dispatcher; `instrument_fastapi_app(app)` after `build_app()`.
- `tests/test_observability.py` (20 tests).

- Key learnings:
  - **`logfire.configure()` API (logfire 4.32.1)**: `token`, `service_name`, `service_version`, `environment`, `send_to_logfire`, `console`, `scrubbing`. `token=None` + `send_to_logfire=False` is canonical "configure without push".
  - **Three auto-instrument hooks**: `instrument_httpx()` (process-wide), `instrument_pydantic_ai()` (process-wide), `instrument_fastapi(app)` (needs LIVE FastAPI instance — AFTER `build_app()`). Each wrapped in try/except.
  - **`LOGFIRE_IGNORE_NO_CONFIG=1` silent-skip pattern**: when token unset AND env="1", skip configure entirely.
  - **Fault-tolerant configure**: bad token/network failure never aborts boot. Only `LogfireMissingError` (ImportError) propagates.

### Task [52]: Implement session-control slash commands - PASS

- `capo/transport/slash.py` — `RECOGNIZED_VERBS` expanded to full 7-verb §5.10 set (approve, deny, new, status, clear, kill, override). Added `SlashVerb` Literal, `DELEGATION_ID_RE`, `SlashCommand.delegation_id` field.
- `capo/transport/dispatcher.py` — Task #52 region with per-verb handlers, `_dispatch_slash_command` switch, `is_cost_override_armed`/`consume_cost_override` public API on `ChannelWorker` for Task #49 cap-bypass.
- 45 new tests across slash parser + dispatcher integration.

- Key learnings:
  - **`/override` sentinel is per-thread, stored on `ChannelWorker`** (`set[str]` of thread_ids, single-turn consumption). Task #49 reads via `consume_cost_override(thread_id)` (atomic test-and-clear).
  - **RunContext construction outside agent.run**: `pydantic_ai`'s `RunContext` REQUIRES `model` + `usage`. The dispatcher's `_handle_kill_command` uses `TestModel()` + `RunUsage()` as inert placeholders. `kill_delegation` only reads `ctx.deps`.
  - **`/kill` delegation_id shape == approval_id shape** — both `uuid.uuid4().hex`. Parser captures raw tokens; dispatcher validates with `DELEGATION_ID_RE`.
  - **Settings db_path test override** — Settings sub-models are frozen Pydantic models. Use `object.__setattr__(settings.paths, "db_path", tmp_path / "state.db")` for tmp_path scoping.
  - **`/clear` ends active session AND deletes per-thread messages** wrapped in `begin_immediate_with_retry`. `/new` ends session without touching messages.

### Task [49]: Pre-tool-call budget hook - PASS

- `capo/budget.py` (new) — `check_budget` + `BudgetCheckResult` (frozen dataclass with status Literal). Module-level `format_*_message` helpers. Fail-open on `daily_total_usd` error. Defaults: soft=$5, hard=$20.
- `capo/transport/dispatcher.py` — added step "1.6 Pre-agent budget hook" in `_handle_envelope` (between slash and agent.run). `hard_block` → reply+mark_read+return; `overridden` → `consume_cost_override` + prepend message; `soft_warn` → prepend message.
- `tests/test_budget.py` (19 tests) + `tests/test_dispatcher_budget_hook.py` (7 tests).

- Key learnings:
  - **Settings namespace is `settings.budget.{soft_daily_usd, hard_daily_usd}`** (defined in Phase 1's `capo/config.py::BudgetConfig`). NOT `settings.cost.*` — task wording suggested cost, but implementation follows existing namespace.
  - **Override semantics**: `/override` consumed ONLY on `hard_block` → `overridden`. Soft cap with override armed passes through as `soft_warn` (override preserved for the actual hard hit).
  - **Fail-open on DB errors**: if `daily_total_usd` errors (missing table, transient), return `ok` and log warning. Soft/hard caps never block on accountant failure.

### Task [51]: Enforce span taxonomy from §6.5 - PASS

- `capo/observability.py` — added 12 named span constructors per §6.5 + `CANONICAL_SPAN_NAMES` + slug validators.
- Migrated `capo/transport/amc_listener.py`, `capo/transport/amc_client.py`, `capo/transport/dispatcher.py`, `capo/workflows/delegation.py`, `capo/workflows/approval.py` to named span constructors.
- `tests/test_span_taxonomy.py` (26 tests including audit + perf microbenchmark).

- Key learnings:
  - **Spec §6.5 canonical span names**: `capo.amc.webhook.in`, `capo.amc.send`, `capo.dispatcher.envelope`, `capo.agent.run`, `capo.workflow.delegation.monitor`, `capo.workflow.approval.request`, etc. Task description had drift; implementation follows spec verbatim.
  - **Named constructors enforce attribute schema** at the call site — `webhook_span(*, envelope_id, channel_id, ...)` requires those exact keys. Drift impossible.
  - **Audit test** scans `capo/` for raw `with_span(`/`logfire.span(` calls; allowlist for test files.

### Task [53]: session_new / session_status / session_clear NL agent tools - PASS

- `capo/tools/session.py` (new) — shared DB helpers + 3 NL tools + `register_session_tools`.
- `capo/tools/__init__.py::register_phase5_tools` registers them with the agent.
- `capo/agent.py::build_agent` calls `register_phase5_tools(agent)` after phase-2 registration.
- `capo/transport/dispatcher.py` — refactored Task #52 private helpers to import from `capo.tools.session`.
- `tests/test_session_tools.py` (17 tests).

- Key learnings:
  - **deps.thread_id must be set on CapoDeps post-factory** by dispatcher so NL tools have the scoping field. CapoDeps from settings doesn't populate it; per-envelope set.
  - **Shared helpers between dispatcher slash handlers and NL tools** live in `capo/tools/session.py` — pure-function helpers; both layers import.
  - **Agent tool registration is now 3-tier**: `register_phase1_tools` + `register_phase2_tools` + `register_phase5_tools`. Total = 11 tools.

### Task [56]: /healthz endpoint with subsystem probes - PASS

- `capo/transport/health.py` (new) — `register_health_endpoint`, `run_probes`, `compute_status`, 7 probe primitives (state_db, dbos_db, dbos_launched, amc_reachable, logfire_configured, claude_binary, codex_binary), `CRITICAL_PROBES`, `PROBE_NAMES`. Defaults: 2s per-probe, 5s overall.
- `capo/main.py::amain` registers endpoint after `instrument_fastapi_app(app)`.
- `tests/test_health.py` (23 tests).

### Task [57]: Litestream config - PASS

- `internal/ops/litestream.yml` — Litestream 0.5.x config; two `dbs` entries (state.db + dbos.db), env-var-templated replica URLs, 24h/168h snapshot policy, 1s sync, multi-level compaction, Prometheus on 127.0.0.1:9091.
- `internal/ops/litestream-install.md` — operator runbook (install, replica matrix, verification drill, paired restore drill, troubleshooting).

### Task [58]: launchd plist - PASS

- `internal/ops/com.you.capo.plist` — template with `{{USER}}`/`{{HOME}}`/`{{CAPO_DIR}}`/`{{LOGFIRE_TOKEN_REF}}` placeholders. `KeepAlive={SuccessfulExit=false}`, `RunAtLoad=true`, `ThrottleInterval=10`. PATH covers Apple Silicon + Intel + uv default.
- `internal/ops/launchd.md` — operator runbook (install, start/kickstart, status, restart, uninstall, drill).

### Task [59]: caffeinate helper - PASS

- `capo/caffeinate.py` (new) — `CaffeinateManager` + singleton registry; asyncio.Lock-serialised refcount-by-set; spawn `caffeinate -i`; SIGTERM-then-SIGKILL reap; macOS-only, no-ops elsewhere.
- Wired in `capo/tools/claude_code.py` + `capo/tools/codex.py` (track after INSERT, release on failure paths), `capo/workflows/delegation.py::_persist_terminal_step` (release on terminal status, inside idempotent step → DBOS replay applies), `capo/main.py` (cold-boot sweep re-tracks running rows; shutdown stops manager).
- `tests/test_caffeinate.py` (32 tests).

### Task [60]: Logfire alerts - PASS

- `internal/ops/logfire-alerts.yml` — 9-alert declarative catalogue. Queries select from Logfire `records` table using canonical §6.5 span names enforced by Task #51.
- `internal/ops/logfire-alerts.md` — runbook with per-alert `mcp__logfire__alert_create` invocation shape.

### Task [61]: Operator runbook - PASS

- `internal/ops/RUNBOOK.md` (989 lines) — top-level operator runbook with 10 sections: Prerequisites, Install, First Boot, Day-2 Ops, Troubleshooting (5 sub), Restart-Resume Drill, Cost Cap Response, Kill Drill, Maintenance, Cross-references.

### Task [54]: Hybrid conversation compaction - PASS

- `capo/memory/compaction.py` (new) — `should_compact`, `compact_thread_async`/`compact_thread`, `CompactionResult`, `SummarizerProtocol`, `estimate_tokens`, `format_summary_preamble`. Atomic DELETE+INSERT under `begin_immediate_with_retry` with snapshot-drift detection.
- `capo/config.py`: `CompactionConfig` extended with `threshold_messages` (30), `keep_recent_messages` (10), `enabled` (True); `threshold_tokens` default 50,000.
- `capo/transport/dispatcher.py`: `ChannelWorker`/`Dispatcher` accept optional `compaction_summarizer`; `_maybe_compact` called after each successful agent turn (fail-open).
- `tests/test_compaction.py` (19 tests).

### Task [55]: Nightly retention pruning - PASS

- `capo/maintenance/retention.py` (new) — `prune_delegation_output` + scheduler (~430 lines). Wakes at 03:00 local daily; skip if previous run <23h ago.
- `capo.main.amain` starts/stops scheduler.
- `capo/config.py::RetentionConfig` extended with `delegation_output_days` (14), `run_hour_local`, `vacuum_after_prune`.
- `tests/test_retention.py` (34 tests).

### Task [62]: Boot-time CLI version pre-checks - PASS

- `capo/boot.py` (new) — `precheck_binaries(settings)`, `BinaryPrecheckError`, `run_precheck_or_exit`. 5s timeout per binary; unparseable output → below-min.
- `capo/tools/claude_code.py`: `MIN_CLAUDE_CODE_VERSION = "2.1.138"` constant + `__all__` export.
- `capo/config.py`: new `BootConfig` (with `skip_binary_precheck: bool = False`).
- `capo/main.py::amain` calls `run_precheck_or_exit` AFTER settings load, BEFORE `configure_logfire` / `init_dbos`. Exit 2 on failure.
- `tests/test_boot_precheck.py` (23 tests).

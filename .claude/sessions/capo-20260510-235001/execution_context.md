# Execution Context

## Project Patterns
- **Build**: Python 3.12+ project, `uv` + `hatchling` build backend.
- **Dev deps**: PEP 735 `[dependency-groups].dev` (NOT deprecated `[tool.uv].dev-dependencies`). Install with `uv sync --group dev`.
- **Ruff**: line-length 100, target py312, rules `E/W/F/I/B/UP/SIM`.
- **Pytest**: `testpaths=["tests"]`, `asyncio_mode="auto"`, `--strict-markers`.
- **Module entry**: `<pkg>/__main__.py` delegates to `<pkg>.main:main()` which returns an int exit code.
- **Spike layout**: findings at `internal/specs/spikes/S-<N>-<topic>.md`, companion `S-<N>-samples/` for raw JSONL, and `S-<N>-bench/` or `S-<N>-harness/` for runnable code.
- **Spec amendments from spikes**: edit the section inline + `> _Updated by spike S-<N> on YYYY-MM-DD._` footnote, plus a row in §15.4 Change Log.

## Key Decisions
- **DBOS + SQLite is GO for V1.** No Postgres migration needed. (S-2 measured: p99 step checkpoint ~1.2 ms at 5-way concurrency, no SQLITE_BUSY.) Postgres fallback playbook retained in S-2 findings for §8.2 triggers.
- **DBOS step semantics are at-least-once on crash.** Externally-visible side-effects (AMC sends, kills, file writes) MUST be idempotent. Matches spec §7 risk-row mitigation.
- **DBOS send/recv polling floor on SQLite is ~1 second** (no LISTEN/NOTIFY). Plan delegation-completion and approval flow timings around this; configurable via `notification_listener_polling_interval_sec`.
- **Claude Code spawn**: `claude -p "<brief>" --output-format stream-json --verbose --permission-mode <mode> [--model <X>]`. `stream-json` REQUIRES `--verbose`. The single-shot `json` mode only emits the final `result` event → cannot satisfy session_id-on-first-event requirement.
- **CC completion detection**: switch on `result.is_error` (boolean). NEVER use `result.subtype` — it stays `"success"` even on in-band errors.
- **CC min version**: `2.1.138`. Constant: `MIN_CLAUDE_CODE_VERSION`. Boot-time `claude --version` SemVer check.
- **CC parser MUST tolerate non-JSON preamble lines** before the first JSON event (e.g., resume-not-found prints plain-text error first).
- **Codex resume is NATIVE**: `codex exec resume <thread_id> "<continuation>"` works end-to-end (including across SIGTERM). No workaround needed. Phase 4 entry blocker resolved.
- **Codex spawn**: `codex exec --json --skip-git-repo-check --sandbox <mode> -C <dir> "<prompt>"`. NEVER pass `--ephemeral` (disables rollout → breaks resume).
- **Codex resume gotchas**: `codex exec resume` does NOT accept `--sandbox`, `--ask-for-approval`, `-C`, or `--add-dir` (sandbox + working-root inherited from original rollout).
- **Codex non-JSON prefix**: `Reading additional input from stdin...` prints before first JSON line. Reader must skip non-JSON prefix.
- **Codex min version**: `0.130.0`.
- **HMAC-SHA256 contract for AMC webhook** (§7.4): `sha256=<lowercase hex>` of HMAC-SHA256(secret, raw body), `hmac.compare_digest`. Verify BEFORE parse/log.
- **AMC dedupe LRU window**: ≥ 15 min. AMC retry window ~13 min / 5 retries. Keep 15-min dedupe invariant.
- **Six canonical AMC error codes** (§7.5): `RATE_LIMITED`, `PLATFORM_AUTH`, `CHANNEL_NOT_FOUND`, `ATTACHMENT_TOO_LARGE`, `VALIDATION_ERROR`, `INTERNAL_ERROR`. Envelope: `{ "error": { "code", "message", "retry_after_seconds": int|null } }`.
- **Capo→AMC headers**: `Authorization: Bearer $AMC_BEARER_TOKEN`, `X-Agent-ID: capo`. `send` adds fresh UUIDv4 `Idempotency-Key`.
- **Missing `X-AMC-Delivery-Id`** → return 400 VALIDATION_ERROR (spec gap; documented in S-4 findings §6 as recommended).
- **Pinned core deps (2026-05-10)**: pydantic-ai 1.93.0, fastapi 0.136.1, dbos 2.21.0, pydantic-settings 2.14.1, httpx 0.28.1, alembic 1.18.4, logfire 4.32.1, sqlalchemy 2.0.49. Pinned `==` rather than range — V1 reproducibility.
- **CLI precedence**: `--config` > `CAPO_CONFIG` env > `./config.toml`. Missing explicit `--config` path is hard error (exit 2); missing default `./config.toml` allowed at scaffold stage (#6 will tighten).

## Known Issues
- **task-executor agents do NOT have `TaskUpdate` tool** despite the agent description. Orchestrator must mark tasks completed based on result-file status. (Confirmed by all 5 wave-1 agents.)
- `uv.lock` is currently gitignored — if reproducible CI is wanted, follow-up task should commit it.
- DBOS pulls in Temporal SDK transitively (~80 deps total). First `uv sync` ~30s.
- CC `--resume <unknown>` emits plain-text error before any JSON, then a `result` event with `is_error: true` and a NEW synthetic session_id. Parser must refuse to overwrite `session_id_subagent` when first JSON has `is_error: true`.
- Hook events (`hook_started`, `hook_response`) fire BEFORE `system/init` in plugin-enabled CC environments — do NOT tie `claude_code_version` capture to event index 0.
- `codex exec resume` is single-threaded per `thread_id` (append-only rollout). Capo MUST serialize resume calls per delegation.
- Schema stability across Codex minor-version bumps is unvalidated; S-1 should be re-run on each bump.
- S-3 multi-version sampling deferred: only CC v2.1.138 locally available. Stable subset chosen depends only on top-level discriminators that minor schema additions can't break.
- S-4 live-AMC verification (retry dedupe, mark_read idempotency on real AMC, error-code surface) deferred to Task #10 (listener) — harness scaffolding is reusable.
- `uv init` auto-creates a workspace pyproject at the nearest parent — for repo-internal spikes/bench dirs, build a self-contained venv or use `--no-workspace` if available.

## File Map
- `internal/specs/capo-SPEC.md` — Authoritative spec; amended §5.4, §7.5, §12.1, §15.4 by spikes S-1 + S-3.
- `internal/blueprints/capo-blueprint.md` — Project structure blueprint.
- `internal/specs/spikes/S-1-codex-resume.md` — Codex CLI contract (post-S-1).
- `internal/specs/spikes/S-1-samples/` — Codex JSONL samples (v0.130.0).
- `internal/specs/spikes/S-2-dbos-sqlite.md` — DBOS+SQLite GO decision + fallback.
- `internal/specs/spikes/S-2-bench/` — Self-contained `uv` bench env (DBOS 2.21.0).
- `internal/specs/spikes/S-3-cc-json-schema.md` — CC stream-json contract.
- `internal/specs/spikes/S-3-samples/` — CC JSONL samples (v2.1.138).
- `internal/specs/spikes/S-4-amc-webhook-e2e.md` — AMC webhook findings + harness usage.
- `internal/specs/spikes/S-4-harness/` — Reusable smoke-test harness (signer, scenarios, stub listener, CLI driver). Phase 1 listener tests + §10.2 restart-resilience tests import from here.
- `pyproject.toml` — Project metadata, pinned deps, ruff/pytest/uv config.
- `capo/__init__.py` — Package marker, `__version__`.
- `capo/__main__.py` — `python -m capo` shim.
- `capo/main.py` — Stub entry point; future Settings → store → listener wiring lives here.
- `tests/test_smoke.py` — 5 smoke tests (import, entry-point, clean exit, missing-config error, `-m capo`).
- `.gitignore` — Ignores `.env`, `config.toml`, `~/.capo/`, `*.db*`, `uv.lock`, caches.

## Task History

### Task [1]: Run spike S-4: AMC webhook end-to-end smoke harness - PARTIAL → accepted as completed
- Harness + findings doc built. 1/4 Functional verified locally; 3/4 deferred to Task #10 (no live AMC reachable to this agent).
- Edge Cases 2/2. Spec gap on missing `X-AMC-Delivery-Id` documented (recommend 400).
- Files: `internal/specs/spikes/S-4-amc-webhook-e2e.md`, `S-4-harness/{README,signer,scenarios,harness,stub_listener,fixtures/envelope.json}`.

### Task [2]: Run spike S-3: Claude Code JSON event schema - PASS
- CC v2.1.138 probed. Stable event subset: top-level `session_id` on every event; first JSON line is `system/hook_started` (plugin envs) or `system/init`. `result.is_error` is canonical, NOT `result.subtype`. Min CC version pinned 2.1.138 in spec §12.1.
- Spec §7.5 updated: spawn = `claude -p ... --output-format stream-json --verbose --permission-mode ...`. Previous draft `--output-format json --session-id-output ...` does NOT exist in v2.1.138.
- Multi-version sampling deferred (only one CC version locally).

### Task [3]: Run spike S-2: DBOS + SQLite concurrent workflows - PASS
- DBOS 2.21.0 + SQLite: p99 step checkpoint ~1.2 ms at 5-way concurrency; no SQLITE_BUSY. Restart-during-step recovers automatically. Steps are at-least-once → idempotency mandatory. Send/recv polling floor ~1 s. **GO for V1.**
- Bench scaffolding at `internal/specs/spikes/S-2-bench/` (self-contained `uv` env).

### Task [4]: Run spike S-1: Codex CLI session resume mechanism - PASS
- Codex v0.130.0 supports native `codex exec resume <thread_id>` — works across SIGTERM. No workaround needed.
- Spawn: `codex exec --json --skip-git-repo-check --sandbox <mode> -C <dir> "<prompt>"`. NEVER `--ephemeral`. Resume does NOT accept `--sandbox`, `--ask-for-approval`, `-C`, `--add-dir` (inherited from original rollout).
- Event taxonomy: `thread.started` (carries `thread_id`), `turn.started`, `item.started/completed` (subtypes: `agent_message`, `file_change`, `command_execution`), `turn.completed` (carries `usage`).
- Spec §5.4, §7.5, §15.4 amended.

### Task [5]: Scaffold capo project structure and dependencies - PASS
- pyproject.toml + capo/{__init__,__main__,main}.py + tests/test_smoke.py + .gitignore + uv.lock all landed.
- `uv run pytest` → 5 passed. `uv run ruff check` → clean. `python -m capo` exits 0; missing explicit `--config` exits 2 with single-line error.
- Pinned deps current as of 2026-05-10.

### Task [6]: Implement Pydantic Settings (config.toml + .env validation) - PASS (54/54)
- `capo/config.py` + 14 per-table sub-models (ModelsConfig, PathsConfig, SoulConfig, AmcConfig, AgentsConfig, BudgetConfig, ShellConfig, ConcurrencyConfig, RetentionConfig, CompactionConfig, ApprovalConfig, UsersConfig, ObservabilityConfig, HeartbeatConfig).
- `Settings.load(config_path, env_file=..., env_override=...)` is the public entry. Returns Settings or raises ConfigError. All secrets are `SecretStr`.
- **Pattern: override `settings_customise_sources` to use only `init_settings`** when any field name might collide with env vars (e.g., `shell` shadows `$SHELL`). Feed env-sourced fields via aliases through `__init__` kwargs.
- **In-house `.env` parser** (~20 lines) — keeps loader pure (no os.environ mutation), gives precise ConfigError(.env at <path>:<lineno>: ...) messages, dodges pydantic-settings env-source collision entirely.
- **Single-line error formatter**: `".".join(loc) + ": " + msg` for each ValidationError, joined with `"; "` — printed verbatim to stderr by `main.py` as `capo: <msg>` + exit 2. No tracebacks on misconfig.
- **Unknown TOML keys**: strip top-level unknown keys before passing to Pydantic with `logging.warning` each; use `extra="forbid"` on sub-models so unknown *nested* keys still fail loudly with key path.
- **Dynamic-key TOML tables (`[users.<id>]`)**: BaseModel with `extra="allow"` + `@model_validator(mode="after")` that coerces each extra into a typed sub-model (`UserEntry`); inner ValidationError re-raised as ValueError prefixed with offending user_id → outer formatter shows `users.<id>.<field>: <msg>`.
- **Spec field correction**: task description used `amc.listener_bind`; §15.3 uses `amc.listen_host` + `amc.listen_port` — used spec names.
- TOML `[soul] dir` resolved relative to config file's parent directory in loader (mutates `raw` before validation) → all soul-file lookups use absolute paths.

### Task [7]: Implement SQLite hardening and BEGIN IMMEDIATE retry helper - PASS (15/15)
- `capo/memory/store.py`: `open_connection`, `aopen_connection`, `begin_immediate_with_retry`, `BatchedInserter`, `batched_insert`, `StoreUnavailable`, `read_pragmas`.
- Pragmas applied at open exactly per §7.3: `journal_mode=WAL`, `synchronous=NORMAL`, `fullfsync=ON`, `busy_timeout=5000`, `journal_size_limit=67108864`, `foreign_keys=ON`.
- **Connection invariants**: `isolation_level=None` (autocommit; the retry helper drives BEGIN IMMEDIATE/COMMIT/ROLLBACK explicitly), `check_same_thread=False` (lets async callers marshal across `asyncio.to_thread`).
- **Retry helper signature**: `begin_immediate_with_retry(conn, *, operation: str, deadline_s=5.0, sleep=time.sleep, monotonic=time.monotonic, rng=None) -> contextmanager`. Detects "database is locked" only; non-lock OperationalError propagates immediately. Backoff: 50ms × 2^attempt × ±25% jitter. Injected `sleep`/`monotonic`/`rng` for deterministic tests.
- **Project pattern**: SQLite write path is ALWAYS `with begin_immediate_with_retry(conn, operation="..."): conn.execute(...)`. NEVER call `conn.execute("BEGIN")` directly.
- `StoreUnavailable` is a `@dataclass(Exception)` carrying `operation`, `last_error`, `attempts`, `elapsed_s` for structured logs.
- `BatchedInserter` flush triggers: `flush_count` rows pending OR `flush_interval_s` wall time since last flush. Defaults 100 / 1.0s. Both can be None.
- Concurrent-writers stress test: 20 writers × 50 inserts each — zero SQLITE_BUSY surfaced to caller.

### Task [8]: Set up Alembic and write initial state.db migration - PASS (70/70)
- `alembic.ini` at project root; `migrations/env.py` resolves DB URL with precedence: `-x dburl=` > `CAPO_STATE_DB` env > `alembic.ini`. Enables SQLite FKs via connect event listener.
- `migrations/versions/001_init.py`: 7 tables (`users`, `sessions`, `conversation_history`, `delegations`, `delegation_output`, `delegation_heartbeats`, `daily_costs`), 5 indexes per §7.3. Raw DDL via `op.execute` (verbatim §7.3) with `IF NOT EXISTS` for idempotency.
- `approvals` table deliberately EXCLUDED (Phase 4 / Task #38).
- Spec ambiguity noted: §7.3 SQL omits ON DELETE actions; took spec SQL as authoritative (NO ACTION default).
- Pattern: Capo SQL is canonical in §7.3. Migrations mirror it verbatim. No SQLAlchemy ORM models in V1 — Alembic uses raw DDL.

### Task [9]: Implement SOUL + ops prompt loader - PASS (9 new tests)
- `capo/agent.py`: `build_agent(settings) -> Agent` factory + `build_composite_instructions` helper + `AgentBuildError` + `SOUL_RECOMMENDED_MAX_LINES=50`.
- Composite = `<soul_text>\n\n<system_text>` constructed once per call; `Agent(instructions=composite)` via injectable Agent factory parameter (testable without real pydantic_ai network deps).
- Lazy-imports `pydantic_ai` to keep `python -m capo --config <missing>` exit-2 path working without the AI provider configured.
- Exemplar `souls/default.md`, `souls/concise.md`, `prompts/system.md` created.
- `capo/main.py` wires `build_agent` after `Settings.load`; banner now includes `agent: ...` line.

### Task [12]: Implement AMC REST client with idempotency and typed error codes - PASS (31 new tests)
- `capo/transport/amc_client.py`: `AMCClient` async REST wrapper around `httpx.AsyncClient`. Methods: `get_unread`, `send`, `mark_read`. `AMCInboundEnvelope` Pydantic model from §7.5.
- Common header builder: `{Authorization: Bearer ..., X-Agent-ID: capo, Content-Type: application/json}`. `send` auto-generates UUIDv4 `Idempotency-Key` when not provided.
- Typed exception hierarchy: `AMCError` base + `RateLimited`, `PlatformAuth`, `ChannelNotFound`, `AttachmentTooLarge`, `ValidationError`, `AMCInternalError`. Dispatched by `error.code` from §7.5 response envelope.
- Retry policy: `RATE_LIMITED` + 5xx → retry honoring `Retry-After`; `PLATFORM_AUTH`, `CHANNEL_NOT_FOUND`, `ATTACHMENT_TOO_LARGE`, `VALIDATION_ERROR` → never retried; `INTERNAL_ERROR` → retry once. Bounded attempts + 30s total deadline.
- Tests use `httpx.MockTransport` (no new deps).
- `send` reuses the SAME `Idempotency-Key` across retries — that's the whole point of the header.

### Task [17]: Implement multi-user AMC sender → user_id resolution - PASS (12 new tests)
- `capo/transport/user_resolver.py`: pure-function `resolve_user_id(sender_id, settings) -> str | None` + `UNKNOWN_SENDER` sentinel + `format_unknown_sender_log_record()` for dispatcher's structured rejection log.
- `capo/config.py.UsersConfig._parse_user_entries` now eagerly rejects empty `[users]` at boot: `ConfigError("users: at least one user is required ...")`.
- Two senders mapped to the same user_id → both resolve to that user_id (intended for shared conversation history).
- DEFERRED to Task #11: dispatcher integration that writes `user_id` to `conversation_history`. Helper is ready; dispatcher will import directly.

## Project Patterns (Wave 3 additions)
- **Spec SQL is canonical**: §7.3 SQL is the source of truth. Migrations use `op.execute(raw_sql)`, NOT SQLAlchemy ORM models. No model layer in V1.
- **Pydantic AI Agent factory injection**: tests pass a fake `agent_factory` callable to `build_agent`; production passes the real `pydantic_ai.Agent` (lazy-imported). Pattern reusable for any future Agent-construction code.
- **HTTP test pattern**: use `httpx.MockTransport(handler)` and an `httpx.AsyncClient(transport=...)`. No `respx` or other extra deps.

### Task [14]: Implement basic Pydantic AI agent with web_search and fetch_url tools - PASS (13 new tests)
- `capo/deps.py`: `CapoDeps` dataclass (deps_type for the agent), `SearchResult`, `FetchError`, `WebSearchClient` Protocol.
- `capo/tools/basic.py`: `web_search`, `fetch_url` async tool fns + `register_basic_tools(agent)` helper. `FETCH_URL_MAX_BYTES = 1 MiB`, `FETCH_URL_TIMEOUT_S = 15.0`.
- `capo/agent.py`: extended `AgentFactory` signature; default factory passes `deps_type=CapoDeps`. `build_agent` sniffs factory signature for Task #9 back-compat (`'deps_type' in parameters or any VAR_KEYWORD`) and registers basic tools when the constructed agent quacks like real pydantic_ai.Agent.
- **Pydantic AI 1.93 API used**: `Agent(model, instructions, deps_type=X)`, `@agent.tool` on fns with first param `ctx: RunContext[X]`. Tool descriptions auto-generated from docstrings.
- **TestModel for offline tests**: `TestModel(call_tools=[])` exercises `agent.run` without API keys AND without auto-firing every tool (default `call_tools='all'` triggers tool inputs and broke our first iteration).
- **`agent._instructions` is a list[str]** in Pydantic AI 1.x; `agent.instructions` is a bound method. Tests asserting SOUL content must read `_instructions[0]` directly.
- **Tool error contract** (capo project pattern): empty/invalid inputs → `ModelRetry("clear message")` for LLM self-correction; recoverable failures (non-2xx, body cap, network) → return structured `FetchError` BaseModel; only programmer errors bubble up.
- `fetch_url` follows redirects by default (`follow_redirects=True`).
- `web_search` has NO default backend in V1; `CapoDeps.web_search_client` is None → tool returns `ModelRetry("no web-search provider configured")`. Real provider in Phase 5.

### Task [15]: Implement per-thread conversation memory with ModelMessage persistence - PASS (13 new tests)
- `capo/memory/conversation.py`: DAO for `conversation_history` table — `thread_id_for_amc`, `load_history(_async)`, `append_messages(_async)`. Writes via `begin_immediate_with_retry(conn, operation="...")`.
- **Pattern: DAO module per §7.3 table-group**, pure functions (no class) taking `conn` as first param. Reads bypass retry helper; writes wrap with `begin_immediate_with_retry`. Async wrappers are thin `asyncio.to_thread` shims.
- **ModelMessage JSON API**: `from pydantic_ai.messages import ModelMessagesTypeAdapter`. `dump_json(list) -> bytes`, `validate_json(bytes|str) -> list`. List-only API → store one message per row as a JSON-array-of-one (compaction-friendly).
- **`message_index` is MAX+1 inside the BEGIN IMMEDIATE txn**: safe because only one writer holds the SQLite write lock. Concurrent test (2 asyncio tasks × 10 batches × 2 messages) yields 40 contiguous indices, zero SQLITE_BUSY.
- **Test schema setup pattern**: import migration `_UPGRADE_STATEMENTS` tuple via `importlib.util.spec_from_file_location` instead of subprocess `alembic upgrade head` (~3-5s per test). One belt-and-braces test still drives real alembic.
- **Pydantic AI lazy-import**: any module touching `pydantic_ai` lazy-imports inside the function — keeps `python -m capo --config <missing>` fast and tolerant of missing AI provider.
- **`ModelMessage` is a Union alias**, only useful in `TYPE_CHECKING` blocks. Construct via concrete `ModelRequest(parts=[UserPromptPart(content=...)])` and `ModelResponse(parts=[TextPart(content=...)])`.

### Task [16]: Implement implicit session creation on first thread message - PASS (11 new tests)
- `capo/memory/conversation.py` (extended): `get_or_create_active_session(+_async)`, `end_session(+_async)`. Single DAO module for §5.7 + §7.3 (history + sessions).
- **Project pattern — session create / INSERT-if-absent under partial unique index**:
  1. Fast-path: read outside the txn — if active session exists, return immediately.
  2. Slow-path: `begin_immediate_with_retry` → re-SELECT inside txn → INSERT (use `uuid.uuid4().hex` for opaque session_id) → catch `sqlite3.IntegrityError` from `idx_sessions_user_thread_active` partial unique → final SELECT to discover the winner's session_id.
- **`idx_sessions_user_thread_active` is the serialization point**: partial unique `(user_id, thread_id) WHERE ended_at IS NULL` + BEGIN IMMEDIATE → 4-way asyncio race converges on a single session_id deterministically.
- **`end_session` is idempotent**: `UPDATE ... WHERE ... AND ended_at IS NULL`; second call → 0 rows → returns False. Unknown session_id likewise. Phase 5 /new and /clear can call defensively.
- Spec §7.3 `session_id` is opaque TEXT — no format prescribed. `uuid.uuid4().hex` chosen.

### Task [11]: Implement per-channel asyncio dispatcher - PASS (11 new tests; 159/159 total)
- `capo/transport/dispatcher.py`: `Dispatcher` + `ChannelWorker`. Lazy worker creation under `asyncio.Lock`. `asyncio.Queue(maxsize=settings.concurrency.queue_depth_max)` per channel. `enqueue()` returns bool (`False` on queue full).
- `capo/config.py`: added `ConcurrencyConfig.queue_depth_max` (default 100, ge=1) per §15.3.
- **CRITICAL project pattern — sqlite3 + asyncio**: do NOT share a sqlite3 connection across multiple `asyncio.to_thread` calls in the same coroutine. CPython can segfault under load. Always open+use+close the connection inside a SINGLE `asyncio.to_thread` closure for multi-step DB work. The `_async` DAO wrappers from #15/#16 are fine for single-call sites; the dispatcher's multi-step turn owns its connection lifecycle in one thread closure.
- **Retry policy boundary**: `AMCClient` already retries `RATE_LIMITED` + 5xx with Retry-After + 30s deadline. The dispatcher does NOT retry on top — logs and moves on if retry-class error surfaces past the client. Keeps retry semantics in ONE place (§7.5).
- **`PLATFORM_AUTH` fallback reply** is best-effort: dispatcher attempts ONE `amc.send` with `PLATFORM_AUTH_REPLY`; if THAT raises, swallow + log + continue. Worker NEVER dies across turn failures.
- **`CHANNEL_NOT_FOUND` is terminal-no-fallback**: channel is gone, can't deliver "I can't reach that channel" reply. Log + skip. `CHANNEL_NOT_FOUND_REPLY` constant still exported for upstream callers.
- **`mark_read` always attempted** post-send regardless of send success/failure (idempotent on `message_id`).
- **Unknown sender** → still `mark_read` (defense-in-depth so AMC stops redelivering); agent NOT invoked; no domain row written; structured log via `format_unknown_sender_log_record`.
- **Pydantic AI `RunResult.new_messages()`** is the canonical API for grabbing messages added during a turn (1.93.0).
- **`thread_id_for_amc(channel_id)`** prepends `amc:` — envelope's `channel_id` is raw (e.g. phone number / discord id), NOT `amc:<id>`. Easy gotcha: double-prefixing breaks history isolation.
- **Queue overflow policy** (Phase 1): drop+log; `enqueue()` returns False. Task #10 may later wire to 429 surface.
- **Dispatcher structured-log schema**: every record uses an `event` key in `extra=`. Convention: `capo.dispatcher.<category>.<subcategory>` (e.g. `capo.dispatcher.amc.platform_auth`, `capo.dispatcher.envelope.dropped.queue_full`).
- **Per-channel worker structure**: lazy create on first envelope + `asyncio.Lock` for concurrent create safety + `asyncio.Queue(maxsize=cap)` + single `asyncio.Task` running `while True: env = await queue.get()` with `try/except BaseException/finally task_done()`. Worker only exits via explicit `stop()` cancel.

### Task [10]: AMC webhook listener (HMAC + dedupe + fast-ACK) - PASS (20 new tests; 188/188 total)
- `capo/transport/amc_listener.py`: `build_app(settings, dispatcher, ...)` FastAPI factory. HMAC-FIRST verification (use `request.body()` raw bytes, NEVER parse before verify), 15-min TTL `DedupeLRU` keyed on `X-AMC-Delivery-Id`, structured `capo.listener.*` logs.
- `capo/main.py`: added `--no-serve` flag + banner listener line. Uvicorn binding deferred to Task #18 wave (needs Dispatcher composition with httpx client lifecycle).
- 401 body matches §7.4 EXACTLY: `{"error": {"code": "BAD_SIGNATURE", "message": "X-AMC-Signature does not match body HMAC"}}`.
- Missing `X-AMC-Delivery-Id` → 400 VALIDATION_ERROR (S-4 findings recommendation, since spec was ambiguous).
- Performance: 1000 sequential signed in-process requests → 0.18s total → well under §6.1's 1s P99 budget.
- `dispatcher.enqueue` returning False or raising → STILL return 204 (idempotent retries are safe).

### Task [13]: Boot-time unread sweep - PASS (9 new tests)
- `capo/transport/boot_sweep.py`: `async def run_boot_sweep(amc_client, dispatcher, settings, *, sleep, monotonic) -> int`. Exponential backoff (1→2→4→…cap 10s) with `max_boot_wait_seconds` deadline. Structured events: `capo.boot.sweep.{complete,timeout,attempt_failed,enqueue_failed,enqueue_dropped}`.
- Never raises (except `CancelledError`). Returns 0 on timeout with WARNING log.
- INTENTIONALLY did NOT edit `capo/main.py` to avoid merge conflict with concurrent Task #10. Task #18 amain() will compose: `Dispatcher(...).start()` → `run_boot_sweep(...)` → `uvicorn.run(...)`.

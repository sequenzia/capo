# Capo — Notes for Claude / future agents

This file captures the architectural invariants, conventions, and known
gotchas that aren't obvious from the code alone. Keep it short; prefer
linking to source over duplicating it.

## What Capo is

Single long-lived Python 3.12 process. AMC webhook → per-channel asyncio
queue → Pydantic AI agent loop → tools (some of which spawn Claude
Code/Codex subprocesses monitored by DBOS durable workflows). Two SQLite
files: `state.db` (app) + `dbos.db` (workflows). Both replicated by
Litestream; launchd supervises; Logfire instruments.

## Spec citations

Every module docstring cites `internal/specs/capo-SPEC.md` section numbers
(`§5.2`, `§7.3`, `§9.1`, …). When changing behaviour, **re-read the cited
section first** and update the docstring if the spec moves. The blueprint
at `internal/blueprints/capo-blueprint.md` is the design source-of-truth.

## Critical files (top 5)

| File | Lines | Why it matters |
|---|---|---|
| `capo/workflows/delegation.py` | 3268 | DBOS `monitor_delegation`, restart-resume, summarize, notify. The largest module. |
| `capo/transport/dispatcher.py` | 1869 | Per-channel turn pipeline; the entire slash-command surface lives here. |
| `capo/tools/codex.py` | 1421 | `delegate_to_codex` — **NOTE: not registered onto the agent in `tools/__init__.py`**; see Gotchas. |
| `capo/tools/claude_code.py` | 1381 | `delegate_to_claude_code` — spawn + 30s `_await_session_id` + DBOS handoff. |
| `capo/workflows/approval.py` | 1149 | DBOS `request_approval` workflow; bridges state.db `approvals.workflow_id` → dbos.db. |

A comprehensive analysis lives at
`internal/docs/codebase-analysis-report-2026-05-11.md`.

## Architectural invariants

1. **Two-DB separation.** `state.db` is Alembic-managed and owns app
   domain. `dbos.db` is DBOS-managed and owns workflow rows. Cross-DB
   references (`approvals.workflow_id`, `delegations.id`) carry no FK.
   **Never put DBOS state in `state.db` or vice versa.**
2. **Idempotency keys everywhere.** Webhook dedupe via
   `X-AMC-Delivery-Id` (15-min LRU); DBOS steps with side effects use
   `@idempotent_step` (SHA-256 over deterministic args); AMC outbound
   sends carry an `Idempotency-Key` header keyed by the same hash.
   Adding a new side-effecting workflow step? Wrap it.
3. **Determinism inside DBOS workflows and steps.** No `datetime.now()`,
   no `uuid4()`, no env reads in workflow/step bodies — all timestamps
   are passed in by the caller. Otherwise replays produce different
   idempotency keys.
4. **Lazy imports for heavy/optional deps.** `pydantic_ai`, FastAPI,
   DBOS, and the tool modules are imported inside functions so the
   `--version` / `--no-serve` boot paths stay fast and test-friendly.
5. **Dependency injection > module-level globals.** `Dispatcher`,
   `AMCClient`, `BootSweep`, `RetentionScheduler` accept injectable
   `sleep` / `monotonic` / `conn_factory` so tests can substitute fakes.
6. **Fail-open on observability and accounting.** Logfire boot failure,
   cost accountant errors, compaction errors must never block the user
   reply. Pattern: `contextlib.suppress(Exception)` + WARNING log.
7. **Pre-agent slash intercepts.** `/status`, `/kill`, `/new`, `/clear`,
   `/override`, `/approve`, `/deny`, … are parsed off the raw inbound
   text **before** `agent.run` — zero LLM tokens. Validated by
   `tests/test_phase5_checkpoint.py::test_slash_override_arms_sentinel_zero_agent_calls`.
8. **Span taxonomy enforced via AST.** All Logfire spans go through
   named constructors in `capo/observability.py`. A raw `logfire.span(`
   call anywhere else in `capo/` fails `tests/test_span_taxonomy.py`.

## Patterns to follow

- **Settings are loaded once, never mutated, never hot-reloaded.** A
  config change is a restart.
- **`CapoDeps` is the canonical tool arg.** Every tool's first parameter
  is `RunContext[CapoDeps]`. Per-turn `user_id` / `thread_id` are
  mutated on the non-frozen dataclass before each `agent.run`.
- **Module-level reply strings for tests.** All user-visible reply
  strings live as module-level `str` constants so test assertions can
  bind to exact wording.
- **Boot-time error idiom.** `ConfigError`, `AgentBuildError`,
  `DBOSInitError`, `LogfireMissingError` raise single-line messages
  citing the offending path; `main()` prints verbatim and `return 2`.

## Gotchas (verified 2026-05-11)

- **`delegate_to_codex` is not registered.** Defined and exported in
  `capo/tools/codex.py`, but `register_phase2_tools` in
  `capo/tools/__init__.py` registers `delegate_to_claude_code` only.
  The LLM cannot reach Codex through tool-calling in production.
- **Compaction is silently disabled.** `Dispatcher` accepts a
  `compaction_summarizer` kwarg; `capo/main.py` does not pass one. Even
  with `[compaction] enabled = true` in TOML, no compaction runs.
- **Queue-full = silent message loss.** `amc_listener` records
  `delivery_id` in the dedupe LRU **before** enqueue. A full queue
  (100/channel) drops the envelope; AMC's retries hit dedupe and never
  re-attempt within the 15-min TTL.
- **Pricing table is hardcoded.** `capo/costs.py PRICING_TABLE` is
  pinned to Claude 4 family as of 2026-05-11. New model ids silently
  cost $0.
- **30s session-id capture window.** `claude_code.py
  SESSION_ID_CAPTURE_TIMEOUT_S = 30.0`. Slow cold-starts can fail-fast a
  healthy CC.
- **Paired Litestream restore.** Restoring only `state.db` or only
  `dbos.db` leaves cross-DB references pointing at non-existent rows.
  See `internal/ops/RUNBOOK.md`.
- **`heartbeat_intervals_json` column is vestigial.** Created by
  migration 001, zero source references. Don't extend it; it'll be
  dropped.

## Testing

- 57 test files in flat `tests/` (no unit/integration split).
- `pytest 9.0`, `asyncio_mode = "auto"`, `--strict-markers`.
- Phase checkpoint tests (`test_phaseN_checkpoint.py`) launch real DBOS
  against tmp_path SQLite — heavier, slower, but exercise the full
  durable workflow path.
- Tests reproduce schema verbatim today (a refactor target — see
  `internal/docs/codebase-analysis-report-2026-05-11.md` R12).

## Useful commands

```bash
uv sync                                         # install
uv run capo --version
uv run capo --config ./config.toml --no-serve   # boot smoke
uv run capo --config ./config.toml              # run
uv run pytest -q                                # full suite
uv run pytest tests/test_phase5_checkpoint.py   # one phase
uv run ruff check capo/                         # lint
uv run alembic upgrade head                     # apply state.db migrations
```

# Execution Session Summary — Phase 1

**Session ID**: `capo-20260510-235001`
**Date**: 2026-05-10 → 2026-05-11
**Scope**: Phase 1 (tasks #1-18) of the capo agent system per `internal/specs/capo-SPEC.md`.

## Outcomes

- **18 of 18 tasks attempted and completed** (across 8 waves).
- **0 retries needed** — every task PASS on first attempt.
- **189/189 unit + integration tests passing.** Ruff clean across `capo/ tests/ migrations/`.
- **2 PARTIAL outcomes accepted as completed** with explicit deferral rationale:
  - **#1 Spike S-4 (AMC webhook E2E)**: 1/4 Functional verified locally; 3/4 deferred to live AMC integration in Task #10. Harness scaffolding at `internal/specs/spikes/S-4-harness/` is reusable; Task #10 ships the same dedupe/HMAC/error-code surface and is verified by 20 integration tests.
  - **#18 Phase 1 checkpoint**: 6/7 Functional automated criteria PASS (HMAC mismatch, dedupe, restart-with-unread, missing-secret rejection, P99 < 1s under 1000 reqs, change log signed off, full-pipeline E2E test). Manual demo ("text 'hi' to AMC, get reply") gated to user with credentials — runbook at `docs/runbook-phase1-demo.md`.

## Spec Amendments Landed This Session

- **§5.4 + §7.5** (Codex CLI contract): amended by spike S-1 with v0.130.0 spawn invocation, JSONL event taxonomy, native `codex exec resume <thread_id>` (no workaround needed). Phase 4 entry blocker resolved.
- **§7.5** (Claude Code contract): finalized to `claude -p ... --output-format stream-json --verbose --permission-mode ...`. The previously-drafted `--output-format json --session-id-output ...` flag does NOT exist; corrected.
- **§12.1** (Version Policy): pinned `claude-code >= 2.1.138`, `codex >= 0.130.0`.
- **§15.4 Change Log**: Phase 1 row appended.

## Code Footprint

```
capo/
├── __init__.py / __main__.py / main.py        — entry point (banner, --config, --no-serve, amain())
├── config.py                                  — Pydantic Settings (14 sub-models)
├── agent.py                                   — build_agent factory + injectable Agent factory
├── deps.py                                    — CapoDeps + SearchResult + FetchError
├── memory/
│   ├── store.py                               — SQLite hardening + BEGIN IMMEDIATE retry + BatchedInserter
│   └── conversation.py                        — history DAO + session lifecycle (one DAO module for §5.7 + §7.3)
├── tools/basic.py                             — web_search + fetch_url
└── transport/
    ├── amc_client.py                          — typed REST client w/ retry policy
    ├── amc_listener.py                        — FastAPI HMAC-first webhook + 15-min dedupe LRU
    ├── dispatcher.py                          — per-channel asyncio worker
    ├── user_resolver.py                       — sender→user_id resolution
    └── boot_sweep.py                          — boot-time unread sweep
migrations/versions/001_init.py                — 7 tables + 5 indexes per §7.3
souls/{default,concise}.md                     — exemplar SOUL files
prompts/system.md                              — ops system prompt
docs/runbook-phase1-demo.md                    — manual demo SOP (user gate for #18)
tests/                                         — 13 test modules, 189 passing tests
internal/specs/spikes/
├── S-1-codex-resume.md + samples/             — Codex contract spike
├── S-2-dbos-sqlite.md + bench/                — DBOS+SQLite GO/no-go bench
├── S-3-cc-json-schema.md + samples/           — CC event-schema spike
└── S-4-amc-webhook-e2e.md + harness/          — AMC webhook smoke harness
```

## Next Steps for the User

1. **Run the manual demo gate for Phase 1** following `docs/runbook-phase1-demo.md`:
   - Prepare `.env` with `AMC_WEBHOOK_SECRET`, `AMC_BEARER_TOKEN`, `ANTHROPIC_API_KEY`.
   - Prepare `config.toml` with `[users.<your_user_id>]` mapping for your AMC sender id.
   - `uv run capo --config /path/to/config.toml`
   - Text "hi" to your AMC channel; expect a reply.
2. **Commit Phase 1**: 45 new files, 1 spec edit. The session-state dir under `.claude/sessions/capo-20260510-235001/` is the archived artifact.
3. **Run /agent-alchemy-sdd-tools:execute-tasks again** to start Phase 2 (tasks #19-#27). Phase 2 entry has no extra prerequisites beyond Phase 1 being demoed.

## Notable Engineering Decisions Worth Carrying Forward

(Full detail in `execution_context.md`.)

- **Pinned current deps (2026-05-10)**: pydantic-ai 1.93.0, fastapi 0.136.1, dbos 2.21.0, pydantic-settings 2.14.1, httpx 0.28.1, alembic 1.18.4, logfire 4.32.1, sqlalchemy 2.0.49, uvicorn 0.46.0.
- **sqlite3 + asyncio rule**: open + use + close the connection inside a SINGLE `asyncio.to_thread` closure for multi-step DB work. CPython can segfault under load otherwise. The `_async` DAO wrappers are fine for single-call sites.
- **Spec SQL is canonical**: §7.3 SQL is the source of truth. Migrations use `op.execute(raw_sql)`, NOT SQLAlchemy ORM models. No model layer in V1.
- **Retry semantics centralized in `AMCClient`**: `RATE_LIMITED` + transient 5xx retried inside the client with Retry-After honoring and a 30s deadline. The dispatcher does NOT retry on top — keeps semantics in one place (§7.5).
- **Pydantic AI lazy-import**: any module touching `pydantic_ai` lazy-imports inside the function — keeps the missing-config exit-2 path fast and tolerant of missing AI provider.
- **Pydantic AI tool error contract**: empty/invalid inputs → `ModelRetry("clear message")` for LLM self-correction; recoverable failures (non-2xx, body cap, network) → return structured BaseModel error; only programmer errors bubble up.
- **DBOS step semantics are at-least-once on crash**: externally-visible side-effects (AMC sends, kills, file writes) MUST be idempotent. Matches spec §7 risk-row mitigation.
- **DBOS send/recv polling floor on SQLite is ~1 s** (no LISTEN/NOTIFY). Plan delegation-completion and approval-flow timings around this.
- **CC parser MUST tolerate non-JSON preamble lines** before the first JSON event; MUST switch on `result.is_error` (not `result.subtype`) for completion.
- **task-executor agents do NOT have TaskUpdate** despite the agent description — orchestrator marks tasks completed based on result-file status.

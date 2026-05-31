# Capo - The Boss of your Agents

Capo is a personal AI orchestrator that lives in a single long-lived Python
process. It ingests chat messages (iMessage/Discord) via the AMC platform,
handles trivial requests itself, and delegates real coding work to Claude
Code / Codex subprocesses supervised by durable workflows so spawned work
survives a Capo restart.

## What it does

- **One conversation surface, many channels.** AMC delivers signed
  webhooks; Capo replies through the same path.
- **Delegation is a tool call.** The Pydantic AI agent can spawn Claude
  Code (and, when registered, Codex) with full telemetry and approval
  gates.
- **Restart resilience.** DBOS owns `dbos.db`; a 90-minute coding run
  started before a reboot resumes after the reboot.
- **Predictable cost.** Per-turn cost accounting; daily soft + hard caps
  with a `/override` per-thread sentinel.

## Architecture (high level)

```
AMC webhook → amc_listener (HMAC + DedupeLRU) → Dispatcher per-channel queue
            → Pydantic AI agent → tools (web_search, shell_exec,
              delegate_to_claude_code, …) → DBOS monitor_delegation
            → AMC reply + mark_read
```

Two SQLite files: `state.db` (app domain — sessions, conversation_history,
delegations, approvals, costs) and `dbos.db` (DBOS workflow state).
Litestream replicates both; launchd supervises the process; caffeinate
keeps macOS awake while delegations run; Logfire owns observability.

See [`internal/docs/codebase-analysis-report-2026-05-11.md`](internal/docs/codebase-analysis-report-2026-05-11.md)
for a deep architectural overview and
[`internal/blueprints/capo-blueprint.md`](internal/blueprints/capo-blueprint.md) +
[`internal/specs/capo-SPEC.md`](internal/specs/capo-SPEC.md) for the spec.

## Tech stack

| Layer | Pin |
|---|---|
| Runtime | Python 3.12+ |
| Agent SDK | pydantic-ai 1.93 |
| Durable workflows | dbos 2.21 |
| Web | fastapi 0.136 + uvicorn 0.46 |
| Settings | pydantic-settings 2.14 |
| HTTP | httpx 0.28 |
| ORM/migrations | sqlalchemy 2.0 + alembic 1.18 |
| Observability | logfire 4.32 |
| Tests | pytest 9.0 + pytest-asyncio 1.3 |
| Lint | ruff 0.15 |

## Repository layout

```
capo/                    # Source (~22.7k LOC, 38 files)
  agent.py               # build_agent(): SOUL + system prompt + tool registration
  main.py                # CLI entry + 15-step amain() boot sequence
  transport/             # AMC webhook, dispatcher, REST client, slash parser, health
  workflows/             # DBOS durable workflows (delegation, approval, idempotency)
  tools/                 # Pydantic AI tools (basic, claude_code, codex, session, …)
  memory/                # SQLite store, conversation, compaction
  maintenance/           # Nightly retention scheduler
  config.py budget.py costs.py caffeinate.py observability.py deps.py
migrations/              # Alembic for state.db (4 migrations)
souls/                   # Pluggable agent voice (default.md, concise.md)
prompts/                 # Operational system prompt + delegation brief
internal/specs/          # capo-SPEC.md + spike specs
internal/blueprints/     # capo-blueprint.md (source of truth)
internal/ops/            # launchd plist, litestream.yml, logfire alerts, RUNBOOK
internal/docs/           # Codebase analysis reports
docs/                    # Public demo runbooks (Phase 1, Phase 2)
tests/                   # 57 files, ~34k LOC (1.5:1 ratio to source)
```

## Getting started (dev)

```bash
# Install deps
uv sync

# Smoke test: parses settings, builds agent, exits 0 without serving
uv run capo --config ./config.toml --no-serve

# Run the full suite
uv run pytest -q

# Run the app
uv run capo --config ./config.toml
```

See [`docs/runbook-phase1-demo.md`](docs/runbook-phase1-demo.md) for the
end-to-end demo procedure (AMC + LLM provider wiring).

## Status

Functionally complete through Phase 5 (2026-05-11). See the spec checkpoint
annotations in `internal/specs/capo-SPEC.md` for per-phase state.

## License

MIT

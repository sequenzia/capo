# Agents & Tools — Capo

Capo runs **one Pydantic AI `Agent`** per process, built once at boot by
`capo.agent.build_agent()`. The agent's instructions are the active SOUL
file concatenated with the operational system prompt
(`<soul>\n\n<system>`). Tools are registered via three phase-split
helpers in `capo/tools/__init__.py`.

## The orchestrator agent

| Property | Value |
|---|---|
| Construction | `capo.agent.build_agent(settings, factory=...)` |
| Default model | `settings.models.default` (e.g. `anthropic:claude-sonnet-4-6`) |
| Lifecycle | Built once at boot; settings changes require restart |
| Deps type | `CapoDeps` (frozen-ish dataclass; `user_id` / `thread_id` mutated per turn) |
| SOUL pluggability | `settings.soul.active` selects a file under `settings.soul.dir/` (default `souls/default.md`, alt `souls/concise.md`) |

A SOUL file longer than 50 lines logs a `logging.warning` but does not
block boot.

## Tool inventory

Registration is **phase-split** so the model surface area grows in step
with the spec phases.

### Phase 1 — `register_basic_tools` (always)

| Tool | Source | Purpose |
|---|---|---|
| `web_search` | `tools/basic.py` | LLM-driven web search |
| `fetch_url` | `tools/basic.py` | URL fetcher |

### Phase 2 — `register_phase2_tools`

| Tool | Source | Purpose |
|---|---|---|
| `shell_exec` | `tools/basic.py` | Allowlisted subprocess runner (§6.2). Non-allowlisted invocations raise `ApprovalRequired`. |
| `delegate_to_claude_code` | `tools/claude_code.py` | Spawn a Claude Code subagent; capture session id; hand off to `monitor_delegation`. |
| `check_delegation_status` | `tools/delegations.py` | Status snapshot for a delegation id. |
| `get_delegation_output` | `tools/delegations.py` | Tail recent `delegation_output` chunks. |
| `kill_delegation` | `tools/delegations.py` | Request kill; always raises `ApprovalRequired` in Phase 2. |
| `list_delegations` | `tools/delegations.py` | User-scoped recent delegations. |

### Phase 5 — `register_phase5_tools`

| Tool | Source | Purpose |
|---|---|---|
| `session_new` | `tools/session.py` | NL counterpart of `/new`. Ends active session. |
| `session_status` | `tools/session.py` | Active delegation count, today's USD spend, orchestrator model, message count. |
| `session_clear` | `tools/session.py` | Deletes per-thread memory + ends session. |

### Not registered (gap — see `internal/docs/codebase-analysis-report-2026-05-11.md` R2)

| Symbol | Source | Status |
|---|---|---|
| `delegate_to_codex` | `tools/codex.py` (1421 LOC) | Defined, exported in `__all__`, has a dedicated test file, referenced by `caffeinate.py` / `observability.py` / `_approval.py`. **Not added to any `register_*` helper.** The LLM cannot invoke Codex through tool-calling. (Codex is still reachable via the dispatcher's slash/NL surface and cold-boot resume.) |

## Approval-gated tools

- `shell_exec` (non-allowlisted argv) → `ApprovalRequired` of type `shell_exec`
- `delegate_to_claude_code` outside `paths.projects_root` → `delegate_out_of_root`
- `delegate_to_codex` with `--sandbox danger-full-access` → `delegate_dangerous_sandbox`
- `kill_delegation` → `kill_delegation`

Each raises a typed `ApprovalRequired`; the workflow at
`capo/workflows/approval.py` inserts an `approvals` row, sends a notify
to the parent thread, and suspends on `DBOS.recv("approval_decision")`
for up to 24 hours. The dispatcher routes `/approve <id>` and
`/deny <id>` to the workflow via `DBOS.send_async`.

## Slash-command surface (pre-agent)

Parsed off raw inbound text **before** `agent.run` — zero LLM tokens
consumed. Lives in `capo/transport/dispatcher.py` and
`capo/transport/slash.py`.

| Verb | Handler |
|---|---|
| `/new` | End active session; next turn opens a fresh one |
| `/status` | Active delegation count + daily cost + model + message count |
| `/clear` | Delete thread history + end session |
| `/kill <id>` | Request kill of delegation `<id>` |
| `/override` | Arm a per-thread sentinel that consumes one budget hard-block |
| `/approve <id>` / `/deny <id>` | Resolve a pending approval workflow |

Unknown verbs return a friendly `"Unknown command: /verb"` reply.

## Delegation handoff (Claude Code & Codex)

The two delegation tools share an 11-step lifecycle:

1. Generate delegation id
2. Approval gate (if needed)
3. Resolve workspace
4. Create git worktree (best-effort cleanup on failure)
5. Render brief (CC: SOUL-free template; Codex: similar)
6. Build argv
7. Spawn subprocess via `_subprocess.start_reader`
8. Persist `delegations` row with `status='running'` (persistence-before-yield invariant)
9. Concurrent stdout+stderr drain task
10. Hand off to `monitor_delegation` DBOS workflow (fire-and-forget)
11. Return handle to LLM

`monitor_delegation` then drains output, fires heartbeats (default 300 /
900 / 3600s), polls returncode, classifies terminal status
(completed/failed/killed), summarizes with a deterministic one-liner,
persists, and sends the terminal AMC notification — every external side
effect wrapped in `@idempotent_step` for replay safety.

## Cold-boot resume

On startup, `_cold_boot_resume_sweep` in `capo/main.py` selects every
`delegations.status='running'` row, re-tracks each with the caffeinate
manager, and re-fires `monitor_delegation(id)` as a fire-and-forget
asyncio task. DBOS dedupes via workflow-id; the resume step's
in-process registry guard provides a second layer.

## Model tiers (per spec §5.7)

- **Orchestrator** (`settings.models.default`) — the long-running agent
  loop. Default: `anthropic:claude-sonnet-4-6`.
- **Subagent — Claude Code** (`settings.models.subagents.claude_code`) —
  passed via `--model` if set.
- **Subagent — Codex** (`settings.models.subagents.codex`) — passed via
  `--model` if set.
- **Compaction summarizer** — implemented in
  `capo/memory/compaction.py`. **Currently never injected into
  production `Dispatcher`** (see `internal/docs/codebase-analysis-report-2026-05-11.md` R3).

Pricing for the orchestrator / cost-tracked turns is in
`capo/costs.py PRICING_TABLE` (hardcoded Claude 4 family, dated
2026-05-11). Daily soft/hard caps in `capo/budget.py` consult this
table; unknown models silently cost $0 (R8).

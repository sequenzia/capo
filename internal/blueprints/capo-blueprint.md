# Capo

Capo is a Pydantic AI based personal orchestrator for Mac mini. The harness is a thin set of primitives — an agent loop, a few tools, a SQLite store, a durable workflow runtime, and a webhook listener — wired together to field requests from you, handle trivial tasks itself, and delegate real work to Claude Code and Codex while supervising them. It is intentionally small enough to read in an evening.

## Design principles

1. **The harness is yours.** The framework provides primitives, not opinions. You can read every line of the orchestrator in an evening.
2. **Delegation is a tool call.** Spawning Claude Code or Codex is just another tool the agent can choose, with the same telemetry and approval surface as `web_search`.
3. **Durability is mandatory.** A delegated coding task may run 90 minutes. The orchestrator must survive process restarts, OS reboots, and API failures without losing track of in-flight work.
4. **Model flexibility.** Use cheap models (Haiku, GPT-5 Mini) for routing decisions. Use Opus for hard reasoning. Capo is the only place this choice lives.
5. **Observable by default.** Every tool call, every delegation, every model call is traced. You should be able to answer "what did Capo do at 3am last Tuesday?" in seconds.
6. **One process to rule, many to do work.** Capo is a single long-lived Python process. Subagents (Claude Code, Codex) are subprocess children with their own working directories.

## High-level architecture

```mermaid
flowchart TD
    subgraph mac["Mac mini (always on)"]
        direction TB
        AMC["AMC<br/>iMessage + Discord<br/>(separate process)"]:::secondary

        subgraph capo["Capo (Pydantic AI)"]
            direction TB
            loop["Agent Loop"]:::primary
            tools["Tools<br/>web_search • shell_exec<br/>delegate_* • status"]:::primary
            instructions["instructions =<br/>souls/&lt;active&gt;.md +<br/>prompts/system.md"]:::neutral
            dbos["DBOS durable workflows"]:::primary
            db[("SQLite<br/>state, history")]:::neutral

            loop <--> tools
            instructions -.-> loop
            loop --> dbos
            dbos --> db
        end

        subgraph subagents["Subagent subprocesses"]
            direction LR
            cc_a["Claude Code A<br/>(worktree-1)"]:::success
            cc_b["Claude Code B<br/>(worktree-2)"]:::success
            codex["Codex<br/>(sandbox)"]:::success
        end

        AMC -->|webhook POST| loop
        loop -.->|REST: send / mark_read| AMC
        dbos --> subagents
    end

    logfire["Logfire (cloud)<br/>traces, evals, costs"]:::warning
    capo --> logfire

    classDef primary fill:#dbeafe,stroke:#1e40af,stroke-width:2px,color:#000
    classDef secondary fill:#f3e8ff,stroke:#6b21a8,stroke-width:2px,color:#000
    classDef success fill:#dcfce7,stroke:#15803d,stroke-width:2px,color:#000
    classDef warning fill:#fef3c7,stroke:#b45309,stroke-width:2px,color:#000
    classDef neutral fill:#f3f4f6,stroke:#4b5563,stroke-width:2px,color:#000

    style mac fill:#f1f5f9,stroke:#334155,stroke-width:2px,color:#000
    style capo fill:#ffffff,stroke:#1e293b,stroke-width:2px,color:#000
    style subagents fill:#ffffff,stroke:#1e293b,stroke-width:2px,color:#000

    linkStyle default stroke:#1e293b,stroke-width:2px,color:#000
    linkStyle 1 stroke:#475569,stroke-width:2px,stroke-dasharray:5 5,color:#000
    linkStyle 5 stroke:#475569,stroke-width:2px,stroke-dasharray:5 5,color:#000
```

Capo is a single Python process. It owns the conversation state, picks tools, and decides when to delegate. Inbound user messages arrive as HMAC-signed webhook POSTs from AMC; outbound replies and read-acks go back over AMC's REST API. Delegation tools spawn subprocess children in isolated git worktrees, register them in SQLite, and (for long jobs) hand control to a DBOS workflow that polls and reports. The active SOUL (`souls/<active>.md`) is prepended to `prompts/system.md` at boot to give Capo a consistent voice without entangling personality with operational rules.

## Project structure

```
capo/
├── pyproject.toml
├── README.md
├── .env                          # API keys + AMC bearer/secret
├── config.toml                   # Model choices, paths, active soul, AMC settings
├── souls/                        # Personalities. One file per persona.
│   ├── default.md                # Active soul (selected via [soul] in config)
│   └── concise.md                # Example alternate persona
├── capo/
│   ├── __init__.py
│   ├── main.py                   # Entry point. Boots agent, AMC listener, DBOS.
│   ├── agent.py                  # Pydantic AI Agent + composite (SOUL+system) prompt
│   ├── deps.py                   # RunContext dependencies (db, paths, AMC client)
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── basic.py              # web_search, shell_exec, fetch_url
│   │   ├── claude_code.py        # delegate_to_claude_code, status, output
│   │   ├── codex.py              # delegate_to_codex, status, output
│   │   └── delegations.py        # list_delegations, kill_delegation
│   ├── workflows/
│   │   ├── __init__.py
│   │   └── delegation.py         # DBOS @workflow for long-running delegations
│   ├── memory/
│   │   ├── __init__.py
│   │   ├── store.py              # SQLite schema + queries
│   │   └── conversation.py       # Per-thread message history
│   ├── transport/
│   │   ├── __init__.py
│   │   ├── amc_listener.py       # FastAPI webhook receiver for AMC envelopes
│   │   └── amc_client.py         # Thin wrapper over AMC REST (send, mark_read)
│   └── prompts/
│       ├── system.md             # Operational rules — what tools exist, when to delegate
│       └── delegation_brief.md   # Template for briefing subagents (no SOUL)
├── tests/
│   └── ...
└── scripts/
    └── com.you.capo.plist     # launchd template for Mac mini
```

## Core components

### Capo itself

Capo is a single Pydantic AI `Agent` with a small, carefully chosen tool surface. It does not try to be clever. Its only job is routing, brief-writing, and reporting.

```python
# capo/agent.py
from dataclasses import dataclass
from pathlib import Path
from pydantic_ai import Agent, RunContext
from .deps import CapoDeps
from .tools import basic, claude_code, codex, delegations
from .config import settings

HERE = Path(__file__).parent
SOUL = (HERE.parent / settings.soul.dir / f"{settings.soul.active}.md").read_text()
OPS  = HERE.joinpath("prompts/system.md").read_text()

# SOUL first (voice/personality), then operational rules. Cache-stable region.
INSTRUCTIONS = f"{SOUL}\n\n---\n\n{OPS}"

capo = Agent(
    "anthropic:claude-sonnet-4-6",        # Default; override per-run for cheap routing
    deps_type=CapoDeps,
    instructions=INSTRUCTIONS,
)

# Register tools
basic.register(capo)
claude_code.register(capo)
codex.register(capo)
delegations.register(capo)
```

The system prompt is the contract. `prompts/system.md` describes available tools, when to delegate vs handle directly, how to brief subagents, and how to report results back to you. Keep it under 1500 tokens. The active SOUL is prepended to it at boot — see the *Persona via SOUL* section below for what belongs in each file. Voice goes in SOUL, operational rules go in `system.md`; do not mix them.

A sketch of the routing logic in the system prompt:

> Handle yourself: questions answerable from one or two tool calls, simple file reads, status checks on existing delegations, anything under ~5 minutes.
>
> Delegate to Claude Code: any task involving editing, refactoring, or creating code in a real repo; multi-file changes; anything that needs to run tests.
>
> Delegate to Codex: tasks where OpenAI's models perform better (specific languages, specific framework knowledge), or when you want a second opinion on a Claude Code result.
>
> When delegating, write a focused brief: goal, constraints, success criteria, what files matter, what to ignore. Do not paste the whole conversation.

### Dependencies via RunContext

Pydantic AI's dependency injection is the right place for shared infrastructure: the SQLite connection, the path to your projects directory, the AMC client. This keeps tools testable and avoids global state.

```python
# capo/deps.py
from dataclasses import dataclass
from pathlib import Path
import sqlite3

@dataclass
class CapoDeps:
    db: sqlite3.Connection
    amc: "AMCClient"          # Used by the listener for send / mark_read
    workspaces_root: Path     # Where subagent worktrees live
    projects_root: Path       # Where your real projects live
    user_id: str              # Used for memory partitioning
```

### Tool: delegate to Claude Code

This is the heart of Capo. The tool spawns Claude Code as a subprocess in a fresh git worktree, captures its session ID, persists everything, and returns a delegation handle back to the agent.

```python
# capo/tools/claude_code.py
import asyncio
import json
import uuid
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_ai import RunContext
from ..deps import CapoDeps

class DelegationHandle(BaseModel):
    delegation_id: str
    agent: str = "claude-code"
    workspace: Path
    status: str = "running"
    notes: str = ""

class ClaudeCodeBrief(BaseModel):
    goal: str = Field(..., description="One sentence: what should be done.")
    repo_path: str = Field(..., description="Absolute path to the repo or project.")
    constraints: list[str] = Field(default_factory=list)
    success_criteria: list[str] = Field(default_factory=list)
    relevant_files: list[str] = Field(
        default_factory=list,
        description="Hints for the subagent. Optional but reduces wasted exploration.",
    )
    create_worktree: bool = Field(
        default=True,
        description="If true, work in a new git worktree to avoid stomping main.",
    )

def register(agent):
    @agent.tool
    async def delegate_to_claude_code(
        ctx: RunContext[CapoDeps],
        brief: ClaudeCodeBrief,
    ) -> DelegationHandle:
        """Hand a coding task to Claude Code. Returns a delegation handle. Poll
        for status and output via check_delegation_status / get_delegation_output.
        Use this for anything involving editing real code in a real repo."""

        delegation_id = f"cc-{uuid.uuid4().hex[:8]}"
        workspace = ctx.deps.workspaces_root / delegation_id
        workspace.mkdir(parents=True, exist_ok=True)

        if brief.create_worktree:
            await _create_worktree(brief.repo_path, workspace, branch=delegation_id)
            cwd = workspace
        else:
            cwd = Path(brief.repo_path)

        # Render the brief into a single prompt for Claude Code
        prompt = _render_brief(brief)

        # Spawn Claude Code in headless / non-interactive mode
        process = await asyncio.create_subprocess_exec(
            "claude",
            "-p", prompt,
            "--output-format", "json",
            "--permission-mode", "acceptEdits",
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Persist the delegation immediately so a crash mid-spawn doesn't lose it
        ctx.deps.db.execute(
            "INSERT INTO delegations (id, agent, workspace, pid, brief, status) "
            "VALUES (?, 'claude-code', ?, ?, ?, 'running')",
            (delegation_id, str(workspace), process.pid, brief.model_dump_json()),
        )
        ctx.deps.db.commit()

        # Hand off to a background DBOS workflow that monitors and persists output
        from ..workflows.delegation import monitor_delegation
        monitor_delegation.start(delegation_id, process.pid)

        return DelegationHandle(
            delegation_id=delegation_id,
            workspace=workspace,
            notes=f"Claude Code spawned, pid={process.pid}",
        )
```

A few details worth flagging:

- **Headless mode.** Use `claude -p "<prompt>"` with `--output-format json` so Claude Code emits structured events to stdout. Capo can stream and persist these.
- **Worktrees, not branches.** Each delegation gets its own git worktree under `~/.capo/workspaces/<id>`. This is how Conductor and similar Mac tools handle parallel coding agents. You can run three Claude Codes against the same repo without merge conflicts.
- **Permission mode.** `acceptEdits` is permissive. For risky repos, use `default` and surface approval requests back through AMC by sending a question to the originating channel.
- **Persist before yielding.** The row goes into SQLite before the tool returns. If Capo crashes between `subprocess_exec` and the next line, the delegation still gets reaped and tracked.

### Tool: delegate to Codex

Same shape, different binary. Codex CLI's App Server protocol gives you JSON-RPC for fine-grained streaming, but for v1 you can just shell out.

```python
# capo/tools/codex.py (sketch)

class CodexBrief(BaseModel):
    goal: str
    repo_path: str
    sandbox: str = Field(default="modal", description="local | modal | e2b | daytona")
    constraints: list[str] = Field(default_factory=list)

@agent.tool
async def delegate_to_codex(
    ctx: RunContext[CapoDeps],
    brief: CodexBrief,
) -> DelegationHandle:
    """Hand a coding task to Codex. Prefer this when GPT-5 family models are
    likely to do better, when you want a second opinion, or when the sandbox
    options Codex supports (Modal, E2B) matter."""
    ...
```

### Tool: check status, get output, kill

These three are how Capo supervises its delegations. They're cheap, side-effect-free, and called frequently.

```python
@agent.tool
async def check_delegation_status(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
) -> dict:
    """Returns status (running | completed | failed | killed), runtime, last
    activity timestamp, and a one-line summary. Cheap. Call this freely."""

@agent.tool
async def get_delegation_output(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
    tail_lines: int = 200,
) -> str:
    """Returns the latest output from a delegation. Use tail_lines to control
    how much you pull into your context."""

@agent.tool
async def kill_delegation(
    ctx: RunContext[CapoDeps],
    delegation_id: str,
    reason: str,
) -> dict:
    """Stop a running delegation. Use sparingly; prefer letting them finish."""
```

### Durable execution with DBOS

The job of DBOS in this architecture is narrow but critical: monitor each running delegation in a workflow that survives process restarts. If you reboot the Mac mini at hour two of a three-hour Claude Code run, the workflow picks up where it left off when the process restarts.

```python
# capo/workflows/delegation.py
from dbos import DBOS, WorkflowContext

@DBOS.workflow()
def monitor_delegation(ctx: WorkflowContext, delegation_id: str, pid: int) -> dict:
    """Long-running workflow that polls a subprocess, captures its output,
    persists progress, and reports back to Capo on completion.

    Survives crashes because DBOS checkpoints state to Postgres / SQLite
    after every step."""

    while True:
        status = poll_subprocess(pid)            # @DBOS.step
        capture_output_chunk(delegation_id)      # @DBOS.step
        if status in ("completed", "failed", "killed"):
            break
        DBOS.sleep(10)  # Durable sleep. Survives restarts.

    summary = summarize_run(delegation_id)       # @DBOS.step
    notify_user(delegation_id, summary)          # @DBOS.step
    return {"delegation_id": delegation_id, "status": status, "summary": summary}
```

Pydantic AI's docs explicitly call out DBOS, Temporal, and Prefect as supported durable execution adapters. For personal use, DBOS is the lightest option; it can use SQLite as its durable store, so you don't need Postgres.

Why not just rely on Pydantic AI's own agent durability? Because it covers the agent loop (model calls, tool execution) but not unbounded subprocess monitoring. You want the workflow layer for "this delegation will run for an hour or more."

**Subprocess output is drained continuously, not polled.** The kernel pipe buffer for a subprocess's `stdout` is ~64 KB on macOS. If the workflow only polls every 10 seconds without actively reading the pipe, a chatty Claude Code run fills the buffer and the subprocess blocks on write — appearing to hang at minute three. Spec it as a dedicated asyncio reader task per delegation that drains `stdout` continuously into the batched-insert buffer described in *SQLite hardening*. The workflow's poll step only checks `returncode` and flushes batches. The reader task is registered as a `@DBOS.step` so its progress is checkpointed.

**Restart contract.** When launchd restarts Capo at minute 45 of a 90-minute run, the subprocess is orphaned and macOS will eventually reap it. On restart, DBOS sees the workflow is unfinished and re-enters from its last completed step. The workflow's first step is to **re-spawn Claude Code with `claude --resume <session_id>`**, using the session ID captured on the delegation row — Claude Code supports session resume. Don't try to re-attach to a surviving subprocess by `pid`; that's fragile across reboots and launchd restart cycles. Resumes are deterministic; re-attaches are not.

**`notify_user` is not an agent tool.** When a delegation completes, the workflow calls `amc.send` directly via `transport/amc_client.py` using the `parent_thread_id` on the delegation row, with the summary it just generated. Do not re-enter the agent loop on completion. The "one output per turn" rule conflicts with autonomous send-on-completion, the workflow already has everything it needs to write the message, and re-entering opens a much larger surface (model call, tool selection, history loading) for what is fundamentally a one-line "done" notification.

### Persona via SOUL

Capo has a personality layer modeled on OpenClaw's [`SOUL.md`](https://docs.openclaw.ai/concepts/soul). The point is to isolate *voice* — tone, opinions, brevity, humor, default level of bluntness — from operational rules. Mixing them tends to produce two failure modes: an agent that sounds bland and corporate because the system prompt is all rules, or an agent whose tone leaks into how it picks tools because everything is one giant blob. Splitting them keeps each layer authoring-friendly.

**Layout.** Personalities live as plain markdown files in `souls/`. One file per persona — `default.md`, `concise.md`, etc. The active soul is selected in `config.toml`:

```toml
[soul]
active = "default"
dir    = "souls"
```

Switching personas is a config change plus restart in v0. A runtime swap tool can come later if it proves useful; it isn't needed to validate the design.

**Sections.** Borrowing OpenClaw's structure verbatim, since it has held up well:

- **Core Truths** — non-negotiable facts about who the agent is.
- **Boundaries** — what it will and won't do, in voice terms (e.g. "don't apologize for tool failures, just report them").
- **Vibe** — concrete tone rules. Author behavior, not values.
- **Continuity** — how it refers to past conversations and ongoing work.
- **Related** — pointers to other files (AGENTS-equivalent, tool docs, project memory) for the curious reader.

**Loading.** At process start, `capo/agent.py` reads `souls/<active>.md` and prepends it to `prompts/system.md` to form the `instructions=` value passed into `Agent(...)`. SOUL goes first so it sits in the cache-stable region above the tool descriptions Pydantic AI appends. The composite prompt is constructed once per process, not per run.

**Scope rule: Capo only.** SOUL is **not** included in delegation briefs. `tools/claude_code.py` and `tools/codex.py` build their prompts from `prompts/delegation_brief.md` plus the structured `Brief` model — never from SOUL. Subagents do real work in real repos; they should be neutral and goal-focused. The voice belongs to the user-facing turn.

**Authoring guide.** Concrete behavioral rules beat abstract values. A few examples in the spirit of OpenClaw's template:

```markdown
# SOUL — default

## Core Truths
You are Capo, my orchestrator — not a chatbot. You delegate when the task
warrants it and answer directly when it doesn't.

## Vibe
- Brevity is mandatory. If the answer fits in one sentence, give one sentence.
- Never open with "Great question," "I'd be happy to help," or "Absolutely."
- If I'm about to do something dumb, say so.
- Be genuinely helpful, not performatively helpful.

## Boundaries
- Don't narrate tool calls. Report results.
- Don't ask permission for routine work I've already approved.
```

Keep each soul under ~50 lines. Skip life stories, mission statements, and "maintain a positive supportive experience" mush — those are the patterns OpenClaw flags as ineffective. If a rule wouldn't change a single response, it doesn't belong here.

### Safety boundaries and approvals

**`shell_exec` is not a free tool.** As named in `tools/basic.py` it would let the model run any command. For a process with internet access on a personal machine, that's a footgun. Constrain it: an allowlist of binaries (`git`, `ls`, `rg`, `cat`, `pwd`, `which`, `head`, `tail`, ...), reject shell metacharacters (`;`, `&&`, `||`, `|`, backticks, `$(...)`), reject `sudo`, and reject paths outside `projects_root` and `workspaces_root`. Anything outside the allowlist routes through the approval flow below — it never just runs.

**Approval flows.** Some calls — non-allowlisted shells, delegations against repos outside `projects_root`, `kill_delegation`, anything destructive — should pause and ask. The wait must survive restarts, so it lives inside a DBOS workflow: Capo sends a question to AMC tagged with an `approval_id`, the workflow does `DBOS.recv(approval_id, timeout=...)`, and the next inbound webhook with a matching `approval_id` resumes the workflow with the user's answer. Pydantic AI supports tool approval natively (v2 roadmap item); spec the envelope shape now — `{ approval_id, prompt, options }` outbound, `{ approval_id, choice }` inbound — so `tools/basic.py` and `tools/claude_code.py` can be written against the same primitive.

### Memory and state

Two stores, both in the same SQLite file:

**Conversation memory.** Per-thread message history, keyed by AMC `channel_id` (prefixed `amc:` to leave room for future sources). Used by Capo to maintain context with you across messages. Implement as a simple table with `(thread_id, ts, role, content)` and feed the last N messages into each agent run via `message_history`.

**Delegation state.** The source of truth for "what is Capo currently working on." Schema:

```sql
CREATE TABLE delegations (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL,            -- 'claude-code' | 'codex'
    workspace TEXT NOT NULL,
    pid INTEGER,
    brief TEXT NOT NULL,             -- JSON
    status TEXT NOT NULL,            -- running | completed | failed | killed
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    parent_thread_id TEXT,           -- The conversation that spawned this
    summary TEXT
);

CREATE TABLE delegation_output (
    delegation_id TEXT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stream TEXT,                     -- stdout | stderr | event
    chunk TEXT,
    FOREIGN KEY (delegation_id) REFERENCES delegations(id)
);
```

For project-specific knowledge (preferences, conventions, "this repo uses pnpm not npm"), start with a flat markdown file per project loaded into the system prompt at run time. Avoid a vector database until you have a concrete reason for one. Hermes does FTS5 full-text search across past conversations, which is a reasonable next step if simple recall isn't enough.

**SQLite hardening.** The defaults will bite you at 3am. Set these pragmas at boot before any other SQL runs:

```sql
PRAGMA journal_mode = WAL;             -- many readers + one writer
PRAGMA synchronous  = NORMAL;          -- WAL-safe, faster than FULL
PRAGMA fullfsync    = ON;              -- macOS: actually flush to platter
PRAGMA busy_timeout = 5000;            -- ms; retry on lock contention
PRAGMA journal_size_limit = 67108864;  -- 64 MB cap on WAL growth
```

Two app-layer rules go with the pragmas:

- **Every write transaction starts with `BEGIN IMMEDIATE`.** Read-then-upgrade transactions bypass `busy_timeout` and return `SQLITE_BUSY` the moment another writer commits in between. Wrap writes in a retry helper that catches `SQLITE_BUSY` and sleeps a few milliseconds before retrying.
- **Batch the streaming output writes.** A 90-minute Claude Code run can emit tens of thousands of chunks; one autocommit per chunk is the slow path. Buffer for ~50 ms or ~100 chunks per insert, whichever fires first.

Run [Litestream](https://litestream.io) replicating the SQLite file to a local folder and a cloud bucket from day one. Continuous WAL streaming, point-in-time restore, near-zero ops cost.

**Output retention.** `delegation_output` grows unbounded and will dominate DB size — a single chatty run can add hundreds of megabytes. Run a nightly job that prunes raw chunks older than seven days, keeping only the LLM-generated summary stored on the `delegations` row, then `PRAGMA wal_checkpoint(TRUNCATE)` to reclaim WAL space. Keep the policy in code, not in your head.

**When to migrate to Postgres.** SQLite is correct for v0/v1. Remote access from your phone or another laptop is an app-layer concern — AMC plus the FastAPI listener handle it — not a database concern. Migrate when **any one** of these triggers fires:

1. A second long-lived process needs to share DBOS workflow state. DBOS supports both backends, but multi-instance coordination requires Postgres.
2. You commit to vector memory in the same store. `pgvector` with HNSW is cleaner than running `sqlite-vec` as a separate moving part.
3. You move off the Mac mini to a VPS and want managed Postgres (Neon, Supabase, RDS) rather than running SQLite + Litestream there.

Until one fires, SQLite removes a moving part on an always-on personal box. The migration itself is `pgloader sqlite:// postgresql://` plus app-layer cleanup of dialect quirks — half a day if you kept SQL portable in a thin DAL, two days if you leaned on SQLite-isms. Don't pre-migrate.

## Messaging via AMC

Capo does not own messaging. A separate process — [AMC](file:///Users/ada/prod/amc), the Agent Messaging Channel — handles iMessage and Discord (and any future connectors) and exposes a normalized HTTP/webhook API on `127.0.0.1:8080`. AMC owns the platform connectors, the sender allowlist, the quarantine flow, and the canonical message envelope. Capo is just one of its consumers.

This is a deliberate boundary. Connector code (Apple chat.db scraping, Discord gateway, etc.) is fiddly and stateful; keeping it in its own process means it can crash and restart without touching Capo's state, and means new connectors land in AMC without any change to Capo.

**Inbound (webhook push).** AMC POSTs HMAC-SHA256-signed envelopes to a webhook URL of our choosing. The signature is `X-AMC-Signature: sha256=<hex>` over the raw body, and AMC's outbound HTTP client has a **10-second timeout** before it treats the delivery as failed and retries. So the listener has two jobs and only two: verify the signature, and enqueue the envelope to a per-channel worker. The agent run happens in the worker, not in the request handler.

```python
# capo/transport/amc_listener.py (sketch)
import hmac, hashlib
from fastapi import FastAPI, Header, HTTPException, Request, Response
from ..config import settings
from .dispatcher import dispatch  # per-channel ordered queue

app = FastAPI()

@app.post("/amc/webhook")
async def on_amc(
    req: Request,
    x_amc_signature: str  = Header(..., alias="X-AMC-Signature"),
    x_amc_delivery_id: str = Header(..., alias="X-AMC-Delivery-Id"),
):
    body = await req.body()
    expected = "sha256=" + hmac.new(
        settings.amc.webhook_secret.encode(), body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, x_amc_signature):
        raise HTTPException(401, "bad signature")

    # Dedupe on delivery-id (the contract for retries). LRU cache is fine; the
    # window only needs to cover AMC's max retry horizon (~13 min across the
    # 1s/5s/30s/2m/10m schedule).
    if not dispatch.note_delivery(x_amc_delivery_id):
        return Response(status_code=204)

    env = await req.json()
    dispatch.enqueue(env)        # routes to a per-channel asyncio worker
    return Response(status_code=204)
```

The dispatcher runs one worker per `channel_id` so two near-simultaneous messages from the same user process in arrival order. Different channels run in parallel. Each worker pulls an envelope, loads history, runs the agent, persists, sends the reply, and marks read:

```python
# capo/transport/dispatcher.py (sketch)
async def _handle(env: dict) -> None:
    thread_id = f"amc:{env['channel_id']}"
    history   = load_history(thread_id, limit=20)
    result    = await capo.run(env["text"], deps=deps, message_history=history)
    save_message(thread_id, "user",      env["text"])
    save_message(thread_id, "assistant", result.output)
    await amc.send(channel_id=env["channel_id"], text=result.output)
    await amc.mark_read(message_ids=[env["id"]])
```

A few details worth flagging:

- **Signature verification first.** Reject anything that doesn't match `X-AMC-Signature` before parsing or logging the body. AMC's signature is the literal string `sha256=<hex>`, not raw hex — prefix the expected value or strip the header before comparing.
- **Fast-ACK is mandatory.** AMC's HTTP timeout is 10s and a single agent turn easily blows past that. ACKing `204` after dedupe means a retried delivery never piles up behind a slow run. AMC ships a working reference of this exact pattern at [`webhook-receiver/src/amc_receiver/dispatcher.py`](file:///Users/ada/prod/amc/webhook-receiver/src/amc_receiver/dispatcher.py); read it before writing your own.
- **Dedupe on `X-AMC-Delivery-Id`.** AMC retries carry the same delivery-id; that's the canonical retry key. An LRU keyed by delivery-id covers the ~13-minute retry window without growing unbounded. `env["id"]` is the message id and not what AMC retries against.
- **AMC dead-letters after 5 attempts** (backoff `1s, 5s, 30s, 2m, 10m`). If Capo is offline more than ~13 minutes, those deliveries are permanently lost from the webhook channel. On boot, run a one-shot `GET /messages/unread` sweep, enqueue any unacked envelopes through the same dispatcher, and only then start serving the webhook. This makes restart-resilience the listener's problem, not AMC's.
- **`channel_id` is the thread key.** AMC's envelope already namespaces channels (`+15551234567`, `discord:dm:123`, etc.); prefix with `amc:` and use as `thread_id` directly.
- **Outbound is not an agent tool.** `amc.send` and `amc.mark_read` live in `transport/amc_client.py` and are called by the dispatcher worker after the run completes — not exposed as Pydantic AI tools. Capo produces output once per turn; the harness delivers it. Making `send` a tool just invites the model to spam the user.
- **Idempotent sends.** `POST /messages/send` honors `Idempotency-Key`. The `amc_client.send` wrapper should generate a fresh UUIDv4 per call so a crash between the SQLite write and the upstream send completing doesn't double-deliver on retry.
- **`mark_read` takes a list.** The endpoint shape is `{"message_ids": [...]}` — pass a one-element list, not a singular id.
- **Surface AMC error codes.** AMC returns a stable `{error: {code, message, details}}` envelope. `amc_client` should raise typed exceptions per code so Capo can react sensibly: back off on `RATE_LIMITED` (honor `Retry-After`), abandon and notify on `PLATFORM_AUTH` / `PLATFORM_SEND_FAILED` / `CHANNEL_NOT_FOUND`, and reject attachments early on `ATTACHMENT_TOO_LARGE_FOR_PLATFORM`.
- **Per-agent cursor.** Set `X-Agent-ID: capo` on every AMC request so AMC can isolate read cursors if you ever run a second consumer alongside this one.

**MCP wrapper (optional path).** AMC also ships a Python MCP wrapper (`uv run --project mcp amc-mcp` in the AMC repo) exposing the four tools `list_unread_messages`, `send_message`, `mark_read`, `get_message_context`. Capo doesn't need it — it's a long-lived process talking REST — but Claude Code delegations that need to message you back can be launched with `--mcp-config` pointing at the wrapper. The AMC `webhook-receiver` does this today and is a good template.

**Configuration.** AMC's webhook config points at Capo: in AMC's `.env`, `AMC_WEBHOOK_URL=http://127.0.0.1:8090/amc/webhook` and `AMC_WEBHOOK_SECRET=<shared>`. In Capo's `.env`, `AMC_BASE_URL=http://127.0.0.1:8080`, `AMC_BEARER_TOKEN=<same-as-AMC>`, `AMC_WEBHOOK_SECRET=<same-shared>`, `AMC_AGENT_ID=capo`. The two processes share the bearer token and the webhook secret; nothing else.

**Allowlist lives in AMC.** Adding a phone number or Discord user is a TOML edit at `~/.config/messaging-agent/allowlist.toml`, not Capo's concern. Unknown senders quarantine in AMC and webhooks only fire for `allowlist_status='allowed'`, so non-allowlisted traffic never reaches Capo.

## Configuration

Use a single `config.toml` plus environment variables. No magic, no startup wizards.

```toml
# config.toml
[models]
default = "anthropic:claude-sonnet-4-6"
router = "anthropic:claude-haiku-4-5"      # Cheap model for trivial routing
heavy  = "anthropic:claude-opus-4-7"       # Hard reasoning, escalations

[paths]
workspaces_root = "~/.capo/workspaces"
projects_root   = "~/code"
db_path         = "~/.capo/state.db"

[soul]
active = "default"                          # picks souls/default.md
dir    = "souls"

[amc]
base_url    = "http://127.0.0.1:8080"
agent_id    = "capo"
listen_host = "127.0.0.1"
listen_port = 8090                          # AMC's AMC_WEBHOOK_URL points here

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[observability]
logfire_enabled = true
```

API keys and shared secrets (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `LOGFIRE_TOKEN`, `AMC_BEARER_TOKEN`, `AMC_WEBHOOK_SECRET`) live in `.env` and never in `config.toml`.

## Cost caps

Opus 4.7 makes it easy to burn $100 in an afternoon during a long debug session. Cap it.

```toml
# config.toml addition
[budget]
soft_daily_usd = 25       # downgrade `default` → `router`, send heads-up via AMC
hard_daily_usd = 75       # refuse new runs; require explicit override
notify_channel = "amc:default"
```

A small accounting helper reads Logfire's per-run cost (or your own per-call accumulator) at boot and on a pre-tool-call hook. At soft cap, Capo swaps `default` → `router` for the rest of the day and sends a one-line heads-up via AMC. At hard cap, `capo.run` short-circuits with a "budget exceeded — reply 'override' to continue today" message. Resets at local midnight.

Track delegation cost separately. A 90-minute Claude Code run is its own line item and shouldn't be lumped against Capo's daily cap — spawning one is a deliberate user choice, and budgeting it properly needs per-agent caps later. For v0/v1, just log delegation cost and surface it in the completion notification.

## Observability

Pydantic Logfire is the path of least resistance. Two lines in `main.py`:

```python
import logfire
logfire.configure()
logfire.instrument_pydantic_ai()
```

You get a web UI showing every agent run, every tool call, every model request, token costs, and latency. Worth the integration even before production. If you don't want a hosted backend, Pydantic AI's instrumentation is OpenTelemetry-native, so any OTel collector works.

For delegations specifically, log a custom span per delegation lifecycle so you can answer "how long did Claude Code take on the auth refactor?" without digging through SQLite.

## Deployment on Mac mini

Capo is one long-lived Python process that spawns subagent subprocesses on demand. `launchd` is the right tool. Skip `tmux` or `screen`; they don't survive reboots cleanly.

```xml
<!-- ~/Library/LaunchAgents/com.you.capo.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.you.capo</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/you/.capo/.venv/bin/python</string>
        <string>-m</string>
        <string>capo.main</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/you/.capo</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/Users/you/.capo/.venv/bin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/you/.capo/logs/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/you/.capo/logs/stderr.log</string>
</dict>
</plist>
```

Load with `launchctl load ~/Library/LaunchAgents/com.you.capo.plist`. The `KeepAlive` block restarts the process if it crashes, and `RunAtLoad` starts it when you log in.

A few Mac mini specifics worth getting right:

- **AMC is a sibling launchd job.** AMC has its own `com.you.amc.plist`. Capo depends on AMC being up but does not supervise it. If AMC is down, webhook deliveries simply queue inside AMC (per its retry worker) and arrive when Capo's listener is reachable again.
- **Disable App Nap** for the venv's Python, or long-running runs may throttle when the user session is inactive.
- **Caffeinate when delegations are active.** A small helper that runs `caffeinate -i` while any delegation is in `running` state prevents the machine from sleeping mid-task.
- **Filesystem.** Worktrees on the SSD, not on iCloud Drive. Git worktrees in iCloud are a recipe for corrupted repos.

## Why this works for your case

- Capo stays small (~500 lines of Python plus a handful of markdown files in `souls/` and `prompts/`). You can read and modify it.
- Pydantic AI gives you the agent loop, structured tools, dependency injection, durable execution adapters, observability, and model flexibility for free.
- DBOS handles the "long-running task survives restarts" problem that no agent SDK solves cleanly on its own.
- AMC is reused, not reinvented. iMessage, Discord, and any future connectors live behind a single normalized envelope you don't own and don't have to maintain.
- SOUL keeps voice editable as plain markdown, separate from operational rules. Tweaking how Capo sounds doesn't risk breaking how it routes.
- Claude Code and Codex stay in the roles they're best at (coding subagents) and never have to be your orchestrator.
- The whole thing runs on a Mac mini, costs only the API tokens you actually use, and you own every line.

# Capo ↔ Claude Code: Interactive / Persistent Session Integration Options

**Date:** 2026-05-31
**Status:** Design exploration (no code changes proposed yet)
**Scope:** How Capo can talk to a *non-headless* / persistent / interactive Claude Code session, evaluated against Capo's current architecture.

---

## TL;DR

Today Capo drives Claude Code as **one-shot headless subprocesses** (`claude -p … --output-format stream-json`) and recovers them via DBOS + `--resume`. The question is whether Capo can instead work with a **live, full Claude Code session** rather than fire-and-forget headless runs.

There are four meaningfully different mechanisms:

| Option | What it is | Direction | Lives in | Durable / crash-recoverable? | Maturity |
|---|---|---|---|---|---|
| **A. Channels** | An MCP server the *TUI session* spawns; pushes events in + a reply tool out | Bidirectional | Node/Bun MCP server (shim to Capo) | ❌ No DBOS for that session | Research preview (CC ≥ 2.1.80) |
| **B. Streaming JSON input mode** | One persistent `claude -p` process fed JSON turns on stdin | Bidirectional (stdin/stdout) | The `claude` subprocess Capo spawns | ⚠️ Only via re-spawn + `--resume` | Stable CLI |
| **C. Agent SDK (`ClaudeSDKClient`)** | Python SDK holding a live multi-turn session object | Bidirectional (in-process) | Capo's own Python process | ⚠️ Only via re-spawn + `resume=` | Stable SDK |
| **D. Current: one-shot + resume** | `claude -p` per delegation, DBOS monitor, `--resume` on restart | Request/response | The `claude` subprocess Capo spawns | ✅ Full DBOS restart-resume | In production |

**Key finding:** You *cannot* attach to a plain interactive `claude` you already started by hand. But you **can** run a genuinely interactive/persistent session that Capo communicates with bidirectionally — via any of A/B/C — at the cost of giving up (or re-engineering) the DBOS restart-resume durability that option D provides.

**The cross-cutting tension:** Capo's entire reason for existing as a durable system (two SQLite DBs, DBOS workflows, Litestream replication, persistence-before-yield, idempotency keys) assumes the work item is crash-recoverable. A *live* session — held in memory (C), as a long-lived pipe (B), or owned by an external TUI (A) — is not durable the same way. Any of these is a deliberate trade of durability for interactivity/latency.

---

## 1. Background: how Capo calls Claude Code & Codex today

Both delegation tools spawn **headless, non-interactive** CLI subprocesses via `asyncio.create_subprocess_exec`, stream JSONL events back over a piped stdout, capture a resume token, persist a `delegations` row *before* returning, and hand monitoring to the DBOS `monitor_delegation` workflow.

**Claude Code** — `capo/tools/claude_code.py:1004` (spawn at `:1021`):

```
claude -p <rendered_prompt> \
       --output-format stream-json --verbose \
       --permission-mode <mode> \
       [--model <model>]
```

- `-p` = print/headless (one-shot, no TUI).
- `--output-format stream-json --verbose` = line-by-line JSON events the reader consumes.
- First event yields a top-level `session_id` (`claude_code.py:529`), persisted to `delegations.session_id_subagent`; later used for `claude --resume`.
- `SESSION_ID_CAPTURE_TIMEOUT_S = 30.0`.

**Codex** — `capo/tools/codex.py:1106` (spawn at `:1124`):

```
codex exec --json --skip-git-repo-check \
           --sandbox <mode> -C <workspace> \
           [--model <model>] <rendered_prompt>
```

- `exec` = non-interactive subcommand; `--json` = JSONL events; `stdin` closed (`DEVNULL`).
- Resume token is `thread.started.thread_id` (`codex.py:722`).
- **Note:** `delegate_to_codex` is implemented but **not registered** onto the agent in `capo/tools/__init__.py`, so the LLM can't reach Codex via tool-calling in production today.

**Shared lifecycle invariants relevant here:**

1. **Persistence-before-yield** — a `delegations` row (`status='running'`, `pid`) is written before the tool returns.
2. **DBOS owns monitoring + restart-resume** — `monitor_delegation` is launched fire-and-forget; on cold boot, `status='running'` rows are re-monitored and resumed.
3. **Live handles are tracked in memory** — `register_delegation_subprocess` keeps live `(process, reader_handle)` pairs in a `DelegationProcessHandle` registry. (This is the natural home for any "live session" handle in the options below.)
4. **Determinism inside DBOS** — no `datetime.now()`/`uuid4()` in workflow/step bodies; timestamps passed in by callers.
5. **Two-DB separation + Litestream** — `state.db` (app) and `dbos.db` (workflows), both replicated; recovery assumes both restore together.

This is the baseline every option below is measured against.

---

## 2. The core question, disambiguated

> "Can I launch Claude Code, leave a session running, and have Capo use that session?"

This splits into two very different asks:

- **(a) Attach to the interactive TUI session I'm personally typing in.** ❌ Not possible. The Claude Code TUI exposes no IPC/socket/API for an external process to inject prompts into and read responses from the same live REPL. There is no "attach to an already-running session" handle.
- **(b) Run a persistent, full-context session that Capo drives/collaborates with.** ✅ Possible, three ways (A/B/C). "Headless" in Claude Code terminology is *not* synonymous with "one-shot" — persistent multi-turn sessions are a first-class mode.

Channels (Option A) is a partial bridge to (a): it lets an external system push into and receive from a **live, interactive TUI session** — but only if that session was *launched with the channel enabled*. You still can't bolt it onto a session already running without channels.

---

## 3. Option A — Claude Code **Channels** (the closest to "use a live TUI session")

> Source of truth: <https://code.claude.com/docs/en/channels-reference> and <https://code.claude.com/docs/en/channels>. **Research preview**, requires Claude Code **v2.1.80+** (permission relay **v2.1.81+**). Team/Enterprise orgs must explicitly enable channels.

### 3.1 What a channel actually is

A channel is **an MCP server that the Claude Code session spawns as a subprocess and talks to over stdio.** It "pushes events into a Claude Code session so Claude can react to things happening outside the terminal." It is the bridge between external systems and the session.

Crucially for Capo:

- It works with a **standard interactive TUI session** — events arrive in "your running Claude Code session," and for always-on you "run Claude in a background process or persistent terminal."
- It is **bidirectional**:
  - **Inbound (external → session):** the server calls `mcp.notification({ method: 'notifications/claude/channel', params: { content, meta } })`. The event lands in the session's context as a `<channel>` tag.
  - **Outbound (session → external):** expose a standard MCP **reply tool** (`capabilities.tools: {}` + a `CallToolRequestSchema` handler). Claude calls it to send messages back out.
- It can optionally **relay permission prompts** (`claude/channel/permission`): the session's tool-approval prompts (`Bash`, `Write`, `Edit`) are forwarded to the channel so they can be approved/denied remotely; the local terminal dialog stays open in parallel and **first answer wins**.

### 3.2 The inbound notification format

Server emits `notifications/claude/channel` with two params:

| Field | Type | Notes |
|---|---|---|
| `content` | `string` | Becomes the body of the `<channel>` tag. |
| `meta` | `Record<string,string>` | Each entry becomes a `<channel>` tag attribute (routing context: chat id, severity, etc.). **Keys must be identifiers** — letters, digits, underscores only; keys with hyphens/other chars are silently dropped. |

It arrives in Claude's context like this (the `source` attribute is set automatically from the server's configured `name`):

```text
<channel source="capo" severity="high" run_id="1234">
build failed on main: https://ci.example.com/run/1234
</channel>
```

**Delivery semantics (important caveats):**

- **Not acknowledged.** `await mcp.notification()` resolves when the message is written to the transport, **not** when Claude has processed it.
- **Silent drop.** If the session didn't load your server as a channel, or org policy blocks it, events are dropped with no error.
- **Batched.** Events queue and are delivered together on the next turn; Claude handles them as a group.
- **No concurrency within a session.** To process independent event streams concurrently, run **separate sessions**.

### 3.3 The outbound reply tool

Two-way channels register a normal MCP tool (nothing channel-specific about it):

1. `tools: {}` in the `Server` constructor capabilities (enables tool discovery).
2. `ListToolsRequestSchema` + `CallToolRequestSchema` handlers defining the tool schema and the send logic.
3. An `instructions` string telling Claude when/how to call it and which `meta` attribute to echo back (e.g. `chat_id`).

### 3.4 Permission relay (optional, powerful for Capo's approval flow)

Outbound `notifications/claude/channel/permission_request` params:

| Field | Meaning |
|---|---|
| `request_id` | Five lowercase letters from `a`–`z` minus `l` (so it never reads as `1`/`I`). Echo it verbatim in the reply. The local dialog does **not** display this ID — your handler is the only way to learn it. |
| `tool_name` | e.g. `Bash`, `Write`. |
| `description` | Human-readable summary (same text the terminal dialog shows). |
| `input_preview` | Tool args as JSON, truncated to ~200 chars. |

The verdict you send back is `notifications/claude/channel/permission` with `request_id` (echoed) + `behavior` (`'allow'` | `'deny'`). Only verdicts whose ID matches an open request are applied. Relay covers tool-use approvals only — project-trust and MCP-consent dialogs stay local.

### 3.5 How a `capo-channel` would work

```
   you ── launch ──▶  claude  (interactive TUI session = HOST)
                         │ spawns over stdio
                         │ (from .mcp.json + --dangerously-load-development-channels)
                         ▼
                ┌──────────────────────────────┐
                │   capo-channel MCP server     │  ← thin Node/Bun process
                │   (@modelcontextprotocol/sdk) │
                └───────▲───────────────┬───────┘
            reply tool  │               │  notifications/claude/channel
            (Claude ──▶ out)            │  (Capo ──▶ session)
                        │               ▼
                        │   local transport (HTTP localhost / unix socket / AMC)
                        ▼
                     Capo  (long-lived Python process)
```

Two directions:

- **Capo → session:** Capo sends a message to the `capo-channel` server over a local transport; the server calls `mcp.notification(...)`; it lands in the TUI as `<channel source="capo" …>`; the autonomous Claude in the session reads and acts.
- **session → Capo:** Claude calls the `reply` tool; the server's `CallToolRequestSchema` handler forwards the payload back to Capo (HTTP POST, socket write, AMC enqueue, etc.).
- **(optional) approvals:** declare `claude/channel/permission` so the session's tool-approval prompts route to Capo, and let Capo's existing approval surface (`capo/workflows/approval.py`) answer them remotely.

### 3.6 The reframing you must internalize

In the phrasing "**Capo** uses **that session**," ownership is inverted from how Channels actually works:

- **The Claude Code session is the host. It spawns and owns the channel. Capo is just an external system the channel bridges to.** Capo does not *drive* the session like an RPC endpoint — it *injects events*, and the autonomous Claude in that session decides what to do and may reply. It's **messaging/collaboration, not remote-control.**
- **The channel server is Node/Bun, not Python.** The only hard requirement is `@modelcontextprotocol/sdk` on a Node-compatible runtime (Bun/Node/Deno). So the `capo-channel` is a **thin TS/JS shim** whose handlers relay to/from Capo over localhost; Capo itself stays Python. That shim is the seam.

### 3.7 Constraints & gotchas (Channels)

- **Research preview, gated.** CC ≥ 2.1.80 (relay ≥ 2.1.81). Custom channels aren't on the allowlist → start the session with `claude --dangerously-load-development-channels server:capo` (or the plugin form). On Team/Enterprise the `channelsEnabled` org policy must allow it; admins can use `allowedChannelPlugins`.
- **Can't attach post-hoc.** The channel is loaded at session startup from MCP config (`.mcp.json` / `~/.claude.json`). "Attach to the session I left running yesterday" is still not a thing; "launch a session wired to Capo and leave *that* running" is.
- **Events only while the session is open.** For always-on, keep the TUI alive in tmux / a persistent terminal / a launchd-supervised wrapper.
- **Not durable.** No DBOS workflow, no persistence-before-yield, no `--resume` recovery for that session. If the TUI dies mid-task, Capo cannot resume it the way it resumes a headless delegation — you restart the TUI. **This is the core tension with Capo's architecture.**
- **Prompt-injection surface.** Docs are emphatic: an ungated channel is an injection vector — gate on **sender identity** (`message.from.id`, not room/`chat.id`). If Capo pipes AMC inbound (external users) into a session, that's untrusted text reaching Claude; apply the same trust gating Capo already does at its AMC boundary.
- **Best-effort delivery.** No ack, silent drops, batched turns (see §3.2). Messaging-grade, not transactional.

### 3.8 Where it fits in Capo — two honest framings

- **As a parallel "interactive lane"** alongside the existing headless `delegate_to_claude_code`: keep a persistent `claude` TUI (tmux/launchd) wired to a `capo-channel` shim that talks to Capo over a localhost socket (or reuses the AMC wire format). Best when you want a long-running, human-in-the-loop collaborator session. You knowingly forgo DBOS durability for that lane.
- **As a remote-control/approval surface for Capo itself** (the inverse): use the channel's notifications + permission relay to drive *Capo's* approval workflow. This leans on Channels where it's strongest (push notifications + remote approve/deny) without betting the durable delegation path on it.

---

## 4. Option B — `claude -p` **Streaming JSON input mode** (persistent process, CLI level)

> Sources: <https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode>, <https://code.claude.com/docs/en/cli-reference>.

Keep **one** `claude` process alive and feed it multiple turns as JSON lines on stdin, reading JSON events from stdout:

```
claude -p --input-format stream-json --output-format stream-json --verbose
```

- The process is long-lived: it "takes in user input, handles interruptions, surfaces permission requests, and manages session state."
- You write user messages as JSON objects to stdin over time; you read assistant/tool events as JSONL from stdout.
- `--replay-user-messages` re-emits your stdin messages back on stdout for acknowledgement (requires both `--input-format` and `--output-format` = `stream-json`).
- "Headless" but **multi-turn and stateful** — this is the CLI analog of "a session left running that Capo feeds."

### How it maps onto Capo

Capo already spawns `claude` with `asyncio.create_subprocess_exec` and reads JSONL via a reader. The change is:

- Add `--input-format stream-json` and **keep `process.stdin` open** instead of one-shotting the prompt as an argv positional.
- Hold the live process in the existing `DelegationProcessHandle` registry and write subsequent turns to `process.stdin`.
- The reader already parses JSONL events; extend it to correlate per-turn responses.

### Tradeoffs

- ✅ Pure CLI, no new runtime; closest to today's spawn site.
- ✅ Genuine multi-turn context within one process; supports interrupts and mid-stream input.
- ⚠️ **Durability:** a live pipe is not crash-recoverable in itself. On Capo restart, the process is gone; recovery is the spawn-fresh-and-`--resume` path — i.e. back to option D for any in-flight turn. You'd keep DBOS only as the "re-spawn + resume" recovery, not as a monitor of a live pipe.
- ⚠️ Backpressure / framing: you own the stdin write protocol and must handle partial reads, large events (Capo already uses a 2 MiB stream limit), and turn correlation.

---

## 5. Option C — Claude **Agent SDK** (`ClaudeSDKClient`, Python-native)

> Sources: <https://code.claude.com/docs/en/agent-sdk/python>, <https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode>. Package renamed `claude_code_sdk` → **`claude_agent_sdk`** (`ClaudeAgentOptions`, `ClaudeSDKClient`). "Streaming input mode is the preferred way to use the SDK."

Because Capo is a Python asyncio process, this is the **most natural fit**: hold a live `ClaudeSDKClient` per channel/delegation and send turns into it. It retains full context across turns.

```python
from claude_agent_sdk import (
    ClaudeSDKClient, ClaudeAgentOptions, AssistantMessage, TextBlock,
)

options = ClaudeAgentOptions(
    allowed_tools=["Read", "Write", "Bash"],
    permission_mode="acceptEdits",
)

async with ClaudeSDKClient(options) as client:
    await client.query("Create a file called hello.py")
    async for msg in client.receive_response():
        ...  # stream assistant/tool events

    # follow-up in the SAME session — it remembers prior turns
    await client.query("Add a main function to it")
    async for msg in client.receive_response():
        ...
```

Also supports:
- **Streaming input** via async generators (yield multiple `{"type":"user", ...}` messages over time into one `query()`).
- `client.interrupt()` to stop the current task; `client.connect()` / `client.disconnect()` for explicit session lifecycle (disconnect+reconnect = fresh session).

### How it maps onto Capo

- Replace (or add alongside) the subprocess spawn in `delegate_to_claude_code` with a `ClaudeSDKClient` held open for the delegation's lifetime.
- Store the client handle in the in-memory registry (analogous to `DelegationProcessHandle`).
- Capo turns → `client.query(...)`; results stream via `receive_response()` into the same persistence/notification paths.

### Tradeoffs

- ✅ In-process, idiomatic Python — no JSONL framing, no stdin protocol to hand-roll.
- ✅ Cleanest interrupts, structured message objects, native streaming input.
- ✅ Reuses Capo's existing async architecture and DI patterns.
- ⚠️ **Same durability seam as B:** the live client is in-memory; crash recovery is re-spawn + `resume=`/`continue_conversation`, not attach. DBOS would track "should be running + resume token," not the live object.
- ⚠️ New dependency (`claude_agent_sdk`) and its transitive runtime; must respect Capo's lazy-import-of-heavy-deps invariant so `--version`/`--no-serve` boot paths stay fast.

---

## 6. Option D — Current baseline (for contrast): one-shot + `--resume`

What Capo does today (§1). Each delegation is a one-shot `claude -p` whose lifecycle is owned by DBOS `monitor_delegation`; continuity across turns is achieved by **re-spawning with `--resume <session_id>`**, which replays the stored transcript.

- ✅ **Fully crash-recoverable** — the entire point of Capo's two-DB + DBOS + Litestream design. Crash between any steps → cold-boot sweep re-monitors / resumes.
- ✅ Idempotency keys, persistence-before-yield, deterministic workflow bodies all hold.
- ❌ Not a *live* session: no mid-task interaction, higher per-turn latency (cold start + transcript replay), no interrupt of an in-flight turn.

This is "stateful across spawns," not "one live process you talk to."

---

## 7. Decision guidance

Pick by what you're actually optimizing for:

- **"I want a long-running, human-in-the-loop collaborator session, reachable from chat/phone, that Capo can nudge and that can talk back."** → **Channels (A)**. Accept research-preview status and loss of DBOS durability for that lane. Bonus: permission relay can also serve Capo's own approval UX.
- **"I want lower-latency, multi-turn subagent runs that keep context, with minimal change to the spawn site."** → **Streaming JSON input mode (B)**. Closest to today's code; you own the stdin protocol.
- **"I want the cleanest, most idiomatic persistent session inside Capo's Python process."** → **Agent SDK (C)**. Best ergonomics; new dependency; same durability seam as B.
- **"I must not lose crash-recoverability."** → **Stay on D**, or make A/B/C a *parallel opt-in lane* while D remains the durable default. Continuity-across-turns is already achievable via D's `--resume` without giving up durability.

**Recommended posture:** treat A/B/C as **additive lanes**, not replacements for D. Keep the durable one-shot+resume path as the default for anything that must survive a crash; add a live-session lane only where interactivity/latency is worth the durability trade, and `log()`/document that those sessions are not DBOS-recoverable.

---

## 8. Appendix A — `capo-channel` Node/Bun shim sketch

A minimal two-way channel with permission relay that bridges to Capo over localhost. (Illustrative; not production — needs real sender gating, see §3.7.)

```ts
#!/usr/bin/env bun
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'

const CAPO_BASE = process.env.CAPO_BASE ?? 'http://127.0.0.1:8799'  // Capo's local bridge endpoint

const mcp = new Server(
  { name: 'capo', version: '0.0.1' },
  {
    capabilities: {
      experimental: {
        'claude/channel': {},             // registers the notification listener
        'claude/channel/permission': {},  // opt in to permission relay
      },
      tools: {},                          // enables the reply tool
    },
    instructions:
      'Messages from Capo arrive as <channel source="capo" turn_id="...">. ' +
      'Reply to Capo with the capo_reply tool, echoing the turn_id from the tag.',
  },
)

// --- session → Capo: reply tool -------------------------------------------
mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [{
    name: 'capo_reply',
    description: 'Send a message back to Capo over this channel',
    inputSchema: {
      type: 'object',
      properties: {
        turn_id: { type: 'string', description: 'The Capo turn to reply to' },
        text:    { type: 'string', description: 'The message to send to Capo' },
      },
      required: ['turn_id', 'text'],
    },
  }],
}))

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  if (req.params.name === 'capo_reply') {
    const { turn_id, text } = req.params.arguments as { turn_id: string; text: string }
    await fetch(`${CAPO_BASE}/channel/reply`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ turn_id, text }),
    })
    return { content: [{ type: 'text', text: 'sent to capo' }] }
  }
  throw new Error(`unknown tool: ${req.params.name}`)
})

// --- Claude Code → Capo: permission relay ---------------------------------
const PermissionRequestSchema = z.object({
  method: z.literal('notifications/claude/channel/permission_request'),
  params: z.object({
    request_id: z.string(), tool_name: z.string(),
    description: z.string(), input_preview: z.string(),
  }),
})
mcp.setNotificationHandler(PermissionRequestSchema, async ({ params }) => {
  // Forward to Capo's approval surface; Capo POSTs the verdict back to /verdict below.
  await fetch(`${CAPO_BASE}/channel/permission_request`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(params),
  })
})

await mcp.connect(new StdioServerTransport())

// --- Capo → session: local HTTP the Capo process calls --------------------
let nextTurn = 1
Bun.serve({
  port: 8788, hostname: '127.0.0.1', idleTimeout: 0,
  async fetch(req) {
    const url = new URL(req.url)

    // Capo pushes a message INTO the session
    if (req.method === 'POST' && url.pathname === '/push') {
      const { content, meta } = await req.json() as {
        content: string; meta?: Record<string, string>
      }
      const turn_id = String(nextTurn++)
      await mcp.notification({
        method: 'notifications/claude/channel',
        params: { content, meta: { turn_id, ...(meta ?? {}) } },  // keys: [A-Za-z0-9_] only
      })
      return Response.json({ ok: true, turn_id })
    }

    // Capo posts a permission verdict back
    if (req.method === 'POST' && url.pathname === '/verdict') {
      const { request_id, behavior } = await req.json() as {
        request_id: string; behavior: 'allow' | 'deny'
      }
      await mcp.notification({
        method: 'notifications/claude/channel/permission',
        params: { request_id, behavior },
      })
      return Response.json({ ok: true })
    }

    return new Response('not found', { status: 404 })
  },
})
```

Register it and launch the host session:

```jsonc
// .mcp.json  (or ~/.claude.json with absolute paths)
{ "mcpServers": { "capo": { "command": "bun", "args": ["./capo-channel.ts"] } } }
```

```bash
claude --dangerously-load-development-channels server:capo
```

### Capo-side seam (Python)

Capo would need a tiny local bridge (mirrors how it already handles AMC inbound/outbound):

- An outbound call `POST http://127.0.0.1:8788/push` to inject a turn into the session.
- An inbound endpoint `POST /channel/reply` (and `/channel/permission_request`) on a Capo-owned localhost listener to receive the session's replies/approval asks, routed into the dispatcher / approval workflow.
- Supervision of the persistent TUI (tmux/launchd) if you want it always-on — note this would be a **new** boot concern in `capo/main.py`, separate from the DBOS-backed delegation path, and explicitly **not** crash-resumable.

---

## 9. Appendix B — quick reference

**Channel capability keys** (`Server` constructor `capabilities`):
- `experimental['claude/channel'] = {}` — required; registers the listener.
- `experimental['claude/channel/permission'] = {}` — optional; opt in to permission relay.
- `tools = {}` — two-way only; enables the reply tool.

**Methods on the wire (MCP transport, Claude Code extensions):**
- `notifications/claude/channel` — inbound event (`content`, `meta`).
- `notifications/claude/channel/permission_request` — outbound approval ask (`request_id`, `tool_name`, `description`, `input_preview`).
- `notifications/claude/channel/permission` — inbound verdict (`request_id`, `behavior: 'allow'|'deny'`).

**CLI flags:**
- `claude --dangerously-load-development-channels server:<name>` — load an un-allowlisted custom channel.
- `claude -p --input-format stream-json --output-format stream-json --verbose [--replay-user-messages]` — persistent streaming session (Option B).

**Version floors:** Channels ≥ 2.1.80; permission relay ≥ 2.1.81.

---

## 10. Sources

- Claude Code — Channels reference: <https://code.claude.com/docs/en/channels-reference>
- Claude Code — Channels (overview/usage): <https://code.claude.com/docs/en/channels>
- Claude Agent SDK (Python): <https://code.claude.com/docs/en/agent-sdk/python>
- Agent SDK — streaming vs single mode: <https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode>
- Claude Code — CLI reference: <https://code.claude.com/docs/en/cli-reference>
- Working channel implementations (Telegram, Discord, iMessage, fakechat): <https://github.com/anthropics/claude-plugins-official/tree/main/external_plugins>
- Capo source: `capo/tools/claude_code.py`, `capo/tools/codex.py`, `capo/workflows/delegation.py`, `capo/workflows/approval.py`, and `CLAUDE.md`.

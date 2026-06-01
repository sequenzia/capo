# Capo ⇄ AMC ⇄ Claude Code TUI — Channel Bridge Design

**Date:** 2026-05-31
**Status:** Design resolved (decisions recorded in §8); implementation not started.
**Scope:** Letting a human-attended Claude Code **TUI** session participate in the AMC messaging surface, with **Capo as the broker** between AMC and the TUI, using a Claude Code **Channel**.

**Companion doc:** [`claude-code-interactive-session-options-2026-05-31.md`](claude-code-interactive-session-options-2026-05-31.md) compares the four mechanisms for talking to a non-headless Claude Code session (A. Channels / B. streaming-JSON input / C. Agent SDK / D. current one-shot+resume). **This doc records the decision to use Channels (Option A) and specifies the concrete Capo-as-broker topology and code changes.** Read that doc for the full mechanism matrix; read this one for the chosen design.

---

## TL;DR

- **Question asked:** (1) Can AMC expose a custom Claude Code *channel* so you can talk to TUI sessions (not `claude -p`) through the AMC interface? (2) Can **Capo** sit between AMC and the channel — Capo's Pydantic AI agent receives from AMC and communicates with the Claude Code TUI via an `amc-channel`?
- **Answer:** Yes to both. (1) is a thin channel on the AMC adapter. (2) is the better design: Capo becomes a **switchboard** — its agent triages, answers trivial messages directly through AMC (as today), and **escalates real coding work into the one attended TUI session you're watching**, relaying Claude's replies (and optionally its permission prompts) back out through AMC.
- **Decisions that collapse the design (see §8):** human **watches/steers** the TUI → Channels is exactly right; DBOS restart-durability **can be relaxed** for TUI sessions → the main architectural objection disappears; **one session at a time** → no N-terminal orchestration, Capo tracks a single live binding.
- **Key inversion:** a channel is spawned **by the Claude Code session** (stdio child of the TUI), not by Capo. So the `amc-channel` is a thin shim the TUI spawns, which dials back to long-lived Capo. Capo stays the brain on both directions; the shim is dumb transport.
- **Two things that will bite:** (a) Claude must be *instructed to call the reply tool* or nothing reaches the user — it answers in the terminal by default; (b) a TUI session's token usage never crosses the channel, so Capo's per-turn **cost accounting goes blind** on the live lane unless reconciled out-of-band.

---

## 1. Claude Code Channels — reference

> Source of truth: <https://code.claude.com/docs/en/channels-reference> and <https://code.claude.com/docs/en/channels>. **Research preview**, requires Claude Code **v2.1.80+** (permission relay **v2.1.81+**). Team/Enterprise orgs must explicitly enable channels (`channelsEnabled` policy; admins can curate `allowedChannelPlugins`).

### 1.1 What a channel is

A channel is **an MCP server that the Claude Code session spawns as a subprocess and talks to over stdio.** It pushes events *into* a Claude Code session so Claude can react to things happening outside the terminal. It is the bridge between external systems and the session. It works with a **standard interactive TUI session** (not just `-p`); for always-on you keep the TUI alive in a persistent terminal / tmux / launchd wrapper.

Channels can be **one-way** (forward alerts/webhooks for Claude to act on) or **two-way** (chat bridges that also expose a reply tool). A two-way channel with a trusted-sender path can additionally opt in to **relay permission prompts**.

### 1.2 Capability keys (`Server` constructor `capabilities`)

| Key | Required? | Meaning |
|---|---|---|
| `experimental['claude/channel'] = {}` | **Required** | Presence registers the notification listener — this is what makes the MCP server a channel. |
| `experimental['claude/channel/permission'] = {}` | Optional | Declares the channel can receive permission-relay requests (forward tool-approval prompts). |
| `tools = {}` | Two-way only | Standard MCP tool capability; enables the reply tool. |
| `instructions` (string) | Recommended | Added to Claude's system prompt — tell Claude what events to expect, what the `<channel>` tag attributes mean, whether/how to reply, and which attribute to echo back. |

### 1.3 Inbound notification format

Server emits `notifications/claude/channel` with two params:

| Field | Type | Notes |
|---|---|---|
| `content` | `string` | Becomes the body of the `<channel>` tag. |
| `meta` | `Record<string,string>` | Each entry becomes a `<channel>` tag attribute (routing context). **Keys must be identifiers** — letters, digits, underscores only; keys with hyphens/other chars are **silently dropped**. |

Arrives in Claude's context (the `source` attribute is set automatically from the server's configured `name`):

```text
<channel source="capo" thread="42" user_id="u_steve">
Please add retry/backoff to the webhook sender and run the tests.
</channel>
```

**Delivery semantics (caveats):**
- **Not acknowledged** — `await mcp.notification()` resolves when written to the transport, not when Claude processes it.
- **Silent drop** — if the session didn't load your server as a channel, or org policy blocks it, events vanish with no error.
- **Batched** — events queue and are delivered together on the next turn; Claude handles them as a group.
- **No intra-session concurrency** — "to process independent event streams concurrently, run separate sessions."

### 1.4 Outbound reply tool (two-way)

Nothing channel-specific — a normal MCP tool:
1. `tools: {}` in capabilities (enables tool discovery).
2. `ListToolsRequestSchema` + `CallToolRequestSchema` handlers defining schema + send logic.
3. `instructions` telling Claude when/how to call it and which `meta` attribute to echo back (e.g. `thread`).

### 1.5 Permission relay (optional)

Outbound `notifications/claude/channel/permission_request` params:

| Field | Meaning |
|---|---|
| `request_id` | Five lowercase letters from `a`–`z` minus `l` (never reads as `1`/`I`). Echo verbatim in the reply. The **local terminal dialog does not display this ID** — your handler is the only way to learn it. |
| `tool_name` | e.g. `Bash`, `Write`. |
| `description` | Human-readable summary (same text the terminal dialog shows). |
| `input_preview` | Tool args as JSON, truncated to ~200 chars. |

Verdict back: `notifications/claude/channel/permission` with `request_id` (echoed) + `behavior` (`'allow'` | `'deny'`). Only verdicts whose ID matches an open request are applied. The local terminal dialog stays open in parallel — **first answer wins.** Relay covers tool-use approvals only (`Bash`/`Write`/`Edit`); project-trust and MCP-consent dialogs stay local.

### 1.6 Runtime, gating, and the post-hoc-attach limitation

- **Runtime:** the only hard requirement is `@modelcontextprotocol/sdk` on a Node-compatible runtime (Bun/Node/Deno). The Python MCP SDK can express the channel capability (`experimental_capabilities={"claude/channel": {}}` via the low-level `Server`), but emitting the **custom-method** `notifications/claude/channel` is the one piece to verify (see §7).
- **Allowlist:** custom (non-allowlisted) channels need `claude --dangerously-load-development-channels server:<name>` (or the `plugin:<name>@<marketplace>` form). The bypass is per-entry. The official research-preview allowlist already includes **Telegram, Discord, iMessage, and fakechat**.
- **Loaded at startup only:** the channel is read from MCP config (`.mcp.json` / `~/.claude.json`) when the session starts. You **cannot attach a channel to a session already running**; you launch a session wired to the channel and keep *that* alive.
- **Prompt-injection surface:** an ungated channel is an injection vector — gate on **sender identity** (`message.from.id`, not room/`chat.id`).

---

## 2. Two topologies considered

### 2.1 Option (1) — `amc-channel` directly on the AMC adapter

The channel is "just another client of the AMC adapter," the push-based sibling of the existing `mcp/` wrapper. No Capo in the loop.

```
                    ┌────────────────────── AMC ──────────────────────┐
 iMessage/Discord ─► Connectors ─► Adapter HTTP API ─► SQLite
                                        │  ▲
              (outbound webhook OR poll)│  │ POST /messages/send   (reply tool)
                                        ▼  │ POST /messages/mark_read
   TUI session  ◄─stdio─  amc-channel (MCP server, claude/channel cap)
   (Claude Code)          • inbound  → notifications/claude/channel  (<channel> tag)
                          • reply tool → adapter REST
```

| Channel concern | AMC piece |
|---|---|
| Inbound push | AMC outbound webhook → channel's localhost listener → notification (or channel **polls `/messages/unread`**). |
| `meta` routing | `platform`, `channel_id`, `sender`, `message_id` from the normalized envelope (underscore keys only — `chat_id`, not `chat-id`). |
| Reply tool | `send_message` → `POST /messages/send` (lift the call from the `mcp/` wrapper). |
| Mark read | `POST /messages/mark_read`. |
| Sender gating | Reuse AMC sender identity + `identity_links` as the allowlist. |
| Permission relay (bonus) | Approve `Bash`/`Edit` from iMessage/Discord. |

**Tradeoffs / notes:**
- **Multi-agent contention** (already an AMC open decision): one adapter webhook vs. many sessions. Single session → fine; many → use the poll model with a per-session cursor, or a routing key.
- **Overlap:** the research preview already ships official iMessage + Discord channels. Routing through AMC instead buys: one unified cross-platform interface, identity linking, SQLite persistence/audit, the same backend feeding both `-p` and TUI, and independence from Anthropic's allowlist. If you only want "text my TUI," the stock channels are less work.
- No platform code in the shim (same discipline as `mcp/`).

This option is viable but was **not** the chosen design — Capo provides more value in the middle (next).

### 2.2 Option (2, chosen) — Capo as broker / switchboard

Capo's Pydantic AI agent is **already** the AMC receiver and **already** delegates to Claude Code (today via headless `claude -p`). Putting the channel between **Capo and the TUI** (not AMC and the TUI) lets Capo mediate both directions: triage, route to the right session, enrich from memory, apply budget/approval, and post-process Claude's replies before they reach the user.

```
 iMessage/Discord ─► AMC adapter ─webhook─► Capo (Pydantic AI agent: triage)
                                               │        ├─ trivial → reply via AMC (today's path)
                                               │        └─ coding  → escalate ▼
                                               │   ┌─ FastAPI bridge (NEW routes on Capo's app) ─┐
                                  reply/verdict │   │  GET  /live/events  (SSE → push to TUI)     │
                                               └───┤  POST /live/inbound (reply tool + verdicts)  │
                                                   └───────────────▲───────────────┬─────────────┘
                                                                   │ POST          │ SSE
                                                          amc-channel shim ◄─stdio─► Claude Code TUI (you)
                                                          (claude/channel + tools + permission)
```

**Why Capo in the middle is the right call:** with a human at the TUI, Capo becomes the switchboard — trivial messages it answers itself through AMC; real coding work it escalates into the attended session; Claude's results (and permission asks) flow back out through Capo → AMC. The human at the TUI sees only the escalated work.

---

## 3. The inversion to internalize

A channel is spawned **by the Claude Code session** as a stdio child of the TUI — **not** by Capo. Capo is a long-lived process with an independent lifecycle, so it cannot *be* the channel. Therefore:

- The `amc-channel` is a **thin shim the TUI spawns** (from `.mcp.json` + the dev flag), which **dials back to Capo** over a local transport.
- Capo does **not** remote-control the session like an RPC endpoint — it *injects events*; the autonomous Claude in the session decides what to do and may reply via the tool. It is **messaging/collaboration, not remote control.**
- The shim is Node/Bun-or-Python transport; **all brains stay in Capo.**

---

## 4. How it maps onto Capo's code

> Current delegation baseline (see companion doc §1 for detail): `delegate_to_claude_code` (`capo/tools/claude_code.py`) spawns `claude -p … --output-format stream-json`, captures the first-event `session_id` → `delegations.session_id_subagent`, writes a `running` row **before** returning, hands the live subprocess to DBOS `monitor_delegation`, and returns a non-blocking `DelegationHandle`.

What changes for the live lane:

- **`capo/transport/channel_bridge.py` (new)** — two FastAPI routes on Capo's existing app:
  - `GET /live/events` — SSE stream the shim subscribes to (Capo → TUI events).
  - `POST /live/inbound` — receives the shim's `reply` tool payloads + permission verdicts (TUI → Capo).
  - Plus a tiny in-memory registry holding the **single** active live binding (which `thread`/`user_id` the TUI currently serves). This mirrors how Capo already tracks live subprocess handles in its `DelegationProcessHandle` registry.
- **`delegate_to_claude_code` gains a `live` mode** (or a sibling `escalate_to_live_session` tool). It renders the same `ClaudeCodeBrief`/`render_brief`, but instead of `create_subprocess_exec` + DBOS handoff it **emits the brief onto the SSE stream** as the next `notifications/claude/channel` event and returns a non-blocking handle (same philosophy as `DelegationHandle`). Keep the `delegations` row for telemetry; **skip the DBOS monitor** (durability relaxed — see §8). No DBOS determinism concerns on this lane because there is no workflow body.
- **Reply path reuses what exists:** shim `reply` tool → `POST /live/inbound` → dispatcher → `amc_client` → user → `mark_read`. The README's "Capo replies through the same path" holds.
- **Approval convergence (bonus):** declare `claude/channel/permission` on the shim. Claude Code's own `Bash`/`Edit` prompts then relay through Capo → AMC to your phone, alongside Capo's existing gates (`capo/tools/_approval.py`, `capo/workflows/approval.py`). One approval surface for both the Capo agent and the Claude Code agent.

---

## 5. The three flows

1. **Escalate (inbound coding request):** AMC webhook → Capo agent triages → decides "coding" → `live` tool emits `<channel source="capo" thread=… user_id=…>` on `/live/events` → shim forwards via `notifications/claude/channel` → lands in the attended TUI → you watch Claude work.
2. **Reply (TUI → user):** Claude calls the `reply` tool with `thread` + `text` → shim `POST /live/inbound` → Capo dispatcher → `amc_client` `/messages/send` → user → Capo marks the original message read.
3. **Permission relay (optional):** Claude hits a `Bash` prompt → `notifications/claude/channel/permission_request` → shim `POST /live/inbound` (kind=permission) → Capo routes to its approval surface → user approves via AMC → Capo `POST`s a verdict back → shim emits `notifications/claude/channel/permission` → local dialog closes (first-answer-wins; you can also just answer in the terminal).

---

## 6. Two gotchas to plan for

1. **Claude must be told to call `reply`, or the user gets silence.** In a TUI session Claude answers *in the terminal* by default. The shim's `instructions` must say: "messages arrive as `<channel source="capo" thread="…">`; when you've responded to the user, call the `reply` tool with the `thread` from the tag." Without this, you watch Claude work but nothing reaches AMC.
2. **Cost accounting goes blind on the live lane.** Today Capo parses `stream-json` from `claude -p`, so per-turn cost is exact (`capo/costs.py`, caps in `capo/budget.py`). A TUI session's token usage **never crosses the channel** — Capo can't see it. Either exempt live sessions from caps, or reconcile out-of-band from Claude Code's own session usage (the `session_id` Capo already captures / the session JSONL). Decide explicitly.

---

## 7. Shim language

The shim is ~100 lines of pure transport. Two options:

- **TS/Bun** — matches the docs verbatim, lowest risk; adds a Bun toolchain to the Python shop. (A full illustrative shim is in the companion doc §8 — Appendix A.)
- **Python low-level `mcp.server.Server`** — stays in-ecosystem (could live under Capo or as an AMC workspace member). Confirmed: supports `experimental_capabilities={"claude/channel": {}}` in `get_capabilities(...)`. **Open unknown:** emitting the custom-method `notifications/claude/channel` notification — the typed Python SDK may resist an unknown method where the TS SDK's `mcp.notification({method,…})` accepts it freely.

**Recommendation:** spike the Python notification path first (~1–2 hrs). If the SDK fights it, fall back to a tiny Bun shim. Either way, no platform code in the shim.

---

## 8. Decision log (this conversation)

| Decision point | Choice | Consequence |
|---|---|---|
| What "TUI not `-p`" means | **A human watches/steers the TUI** | Channels (Option A) is the right primitive (Agent SDK was only better for *unattended* persistent sessions). |
| Keep DBOS restart-durability on this path? | **Can relax it for TUI sessions** | The "DBOS can't supervise a terminal" objection disappears; live lane skips `monitor_delegation`. Sessions are ephemeral — if the TUI dies, you restart it; Capo does not resume it. |
| Concurrency | **Mostly one at a time** | No N-terminal orchestration; Capo tracks a single live binding. Serialized escalation is acceptable. |

**Posture:** the live channel lane is **additive**, not a replacement. The durable one-shot+resume path (Option D) stays the default for anything that must survive a crash; the channel lane is the human-in-the-loop collaborator surface, explicitly **not** DBOS-recoverable.

---

## 9. Suggested first slice

1. **Spike** the Python channel notification (decides shim language) — a ~30-line server pushing one `<channel>` event into a dev TUI launched with `--dangerously-load-development-channels`.
2. **`channel_bridge.py`** — SSE `/live/events` + `/live/inbound` routes on Capo's FastAPI app, single-binding registry.
3. **`live` mode** in `delegate_to_claude_code` + the `reply`/instructions wiring — end-to-end: text in → escalate → appears in TUI → Claude replies via tool → lands back in iMessage.
4. **Permission relay** + a `capo tui` launcher (writes `.mcp.json`, mints a per-session token + Capo socket URL, registers the binding, execs `claude` with the dev flag) as fast follows.

---

## 10. Sources

- Claude Code — Channels reference: <https://code.claude.com/docs/en/channels-reference>
- Claude Code — Channels (overview/usage): <https://code.claude.com/docs/en/channels>
- Working channel implementations (Telegram, Discord, iMessage, fakechat): <https://github.com/anthropics/claude-plugins-official/tree/main/external_plugins>
- MCP Python SDK (low-level `Server`, `experimental_capabilities`): <https://github.com/modelcontextprotocol/python-sdk>
- Companion: [`claude-code-interactive-session-options-2026-05-31.md`](claude-code-interactive-session-options-2026-05-31.md) (A/B/C/D mechanism matrix, Agent SDK + streaming-JSON alternatives, Node/Bun shim appendix).
- Capo source: `capo/tools/claude_code.py`, `capo/transport/{amc_listener,dispatcher,amc_client}.py`, `capo/workflows/{delegation,approval}.py`, `capo/{budget,costs}.py`, `README.md`.
- AMC: `CLAUDE.md` (adapter REST endpoints, outbound webhook, `mcp/` wrapper, `webhook-receiver/`).

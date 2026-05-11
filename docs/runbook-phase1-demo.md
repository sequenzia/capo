# Phase 1 Manual Demo Runbook

Spec reference: `internal/specs/capo-SPEC.md` §9.1 (Phase 1 Checkpoint Gate).

This runbook is the **manual gate** for the Phase 1 checkpoint. All
automated criteria (§9.1) are exercised by `uv run pytest -q` and are
already green; this procedure proves the same pipeline end-to-end against
the real AMC server and a real LLM provider. Run it once before declaring
Phase 1 done.

## Prerequisites

- Python 3.12+ and [`uv`](https://docs.astral.sh/uv/) installed.
- A reachable AMC server (local or remote) you can configure as a webhook
  destination.
- An AMC bearer token + webhook secret you control.
- An Anthropic API key (Capo's default model id is `anthropic:claude-sonnet-4-6`).
- An AMC sender identifier (phone number, Discord user id, etc.) you can
  text from. You will map this id to a Capo `user_id` in `config.toml`.

## 1. Prepare the secrets file (`.env`)

Place `.env` beside the `config.toml` you intend to pass via `--config`.
Capo's `Settings.load` reads it automatically (spec §6.2). Never commit it
— `.gitignore` already excludes `.env`.

```ini
AMC_WEBHOOK_SECRET=<the HMAC secret AMC will use to sign POST /amc/webhook>
AMC_BEARER_TOKEN=<the bearer token Capo will send to AMC>
ANTHROPIC_API_KEY=sk-ant-...
```

Capo refuses to start if either `AMC_WEBHOOK_SECRET` or `AMC_BEARER_TOKEN`
is missing — that's a checkpoint criterion. Both must be set.

## 2. Prepare `config.toml`

A minimal working `config.toml` for the demo. Adjust paths and sender ids
to match your environment.

```toml
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "~/.capo/workspaces"
projects_root   = "~/code"
db_path         = "~/.capo/state.db"
dbos_db_path    = "~/.capo/dbos.db"

[soul]
# Resolves relative to this config file's directory. Falls back absolute
# paths through unchanged. See souls/default.md in the capo repo for the
# shipping exemplar.
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8765"  # your AMC server's REST URL
agent_id              = "capo"
listen_host           = "127.0.0.1"              # loopback only (TLS-required if not)
listen_port           = 8090
max_boot_wait_seconds = 60

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[retention]
delegation_output_days = 7

[compaction]
threshold_tokens = 100000
preserve_delegation_handles = true

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

# Map the AMC sender id you'll text from to a Capo user_id. The user_id is
# the literal TOML key after ``users.`` — it goes into every domain row.
# Multiple senders can map to the same user_id (intended for shared
# conversation history across devices/platforms).
[users.owner]
amc_senders  = ["+15551234567", "discord:user:123"]
display_name = "Owner"

[observability]
logfire_enabled = true
```

Make sure `souls/default.md` and `prompts/system.md` exist beside the
config file (the repo ships exemplars in `souls/` and `prompts/`). If you
copy them into the same directory as `config.toml`, no path changes are
needed.

## 3. Apply the database migration

The first run needs the §7.3 schema applied to `paths.db_path`. From the
repo root:

```bash
CAPO_STATE_DB=$HOME/.capo/state.db uv run alembic upgrade head
```

(Substitute your `paths.db_path` value for `$HOME/.capo/state.db` if you
chose a different path.) Insert the `user_id` row that matches your
`[users.<id>]` key:

```bash
sqlite3 $HOME/.capo/state.db "INSERT INTO users(user_id) VALUES ('owner')"
```

## 4. Start Capo

```bash
uv run capo --config /absolute/path/to/config.toml
```

Capo will:

1. Validate `config.toml` + `.env` (boot-time Pydantic Settings — §6.2).
2. Build the agent (SOUL + system prompt — §5.1).
3. Open the AMC REST client (§7.5).
4. Start the per-channel dispatcher (§5.2).
5. Run the boot-time unread sweep against AMC (§5.2 invariant) — any
   messages queued at AMC during downtime are delivered now.
6. Bind the FastAPI webhook listener on
   `amc.listen_host:amc.listen_port` (§7.4).

If anything misconfigures, Capo exits non-zero with a single-line error
and no stack trace — that's a checkpoint criterion.

## 5. Wire AMC to Capo

Point your AMC server's webhook destination at
`http://127.0.0.1:8090/amc/webhook` (or whatever host/port you configured)
with the HMAC secret from `.env`. The contract:

- `POST /amc/webhook`
- `X-AMC-Signature: sha256=<lowercase hex of HMAC-SHA256(secret, raw_body)>`
- `X-AMC-Delivery-Id: <uuid v4>`

See spec §7.4 for the full body schema.

## 6. Text "hi" to Capo

From the AMC sender id you mapped to `users.owner.amc_senders`, send the
message `hi` to Capo's channel.

## 7. Observe

- AMC delivers `POST /amc/webhook` with a valid HMAC signature.
- Capo's listener verifies the signature, dedupes, and ACKs `204` within
  ~milliseconds (§6.1 P99 < 1s).
- The dispatcher worker resolves your sender id → `user_id=owner`,
  materializes the conversation history, runs the agent, and sends the
  reply back via `POST /messages/send`.
- You receive Capo's reply on the same AMC channel.

Expected end-to-end latency: ACK in well under 1s + agent latency (model
provider round-trip, typically 1-3s for a sonnet model on a "hi"-sized
prompt).

## 8. Inspect the trace

Phase 1 ships structured logs to stderr (the `capo` and `capo.transport.*`
loggers). Phase 5 will wire these to Logfire; until then the stderr stream
**is** the trace. Useful tags to grep for:

- `capo.listener.signature.fail` — HMAC mismatch (should be absent).
- `capo.listener.dedupe.hit` — duplicate delivery (should be absent on the
  first attempt).
- `capo.listener.accepted` — envelope handed to the dispatcher.
- `capo.dispatcher.turn.failed` — agent or memory error (should be absent
  on a healthy run).
- `capo.dispatcher.amc.*` — outbound AMC failures (should be absent).
- `capo.boot.sweep.complete` — boot sweep finished cleanly.

You can also verify persistence directly:

```bash
sqlite3 $HOME/.capo/state.db "SELECT user_id, thread_id, message_index FROM conversation_history ORDER BY rowid"
sqlite3 $HOME/.capo/state.db "SELECT user_id, thread_id, session_id, ended_at FROM sessions"
```

Both tables should have rows scoped to `user_id=owner` and a `thread_id`
of `amc:<your_channel_id>`. The `sessions.ended_at` column stays `NULL`
while the session is active.

## 9. Stop Capo

`Ctrl+C` triggers a clean shutdown: uvicorn stops accepting requests, the
dispatcher's per-channel workers are cancelled and drained, and the
shared httpx client + AMC REST client close. The process exits 0.

## Troubleshooting

- **Capo exits 2 with `AMC_WEBHOOK_SECRET is required`** — `.env` is
  missing or not in the same directory as `config.toml`. Capo loads
  `.env` next to `--config` automatically; it does NOT read your shell's
  exported variables (that would be a security gap on shared hosts).
- **`401 BAD_SIGNATURE` in the logs** — AMC's signing secret does not
  match `AMC_WEBHOOK_SECRET`. The signature is computed over the raw
  request bytes, BEFORE any JSON parsing.
- **`400 VALIDATION_ERROR` for missing `X-AMC-Delivery-Id`** — AMC must
  send that header; Capo refuses to deduplicate without it (S-4
  recommendation).
- **`capo.dispatcher` warns "unknown sender"** — your AMC sender id is
  not listed in any `[users.<id>].amc_senders` block. Update
  `config.toml` and restart.
- **Reply never arrives** — check `capo.dispatcher.amc.*` logs. Common
  causes: AMC bearer token is wrong (`PLATFORM_AUTH`), channel id is
  malformed (`CHANNEL_NOT_FOUND`).

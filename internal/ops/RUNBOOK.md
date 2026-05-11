# Capo Operator Runbook (V1)

The top-level operator runbook for Capo. Covers initial install, first boot,
day-2 operations, troubleshooting, and the V1 release drills (restart-resume,
cost-cap response, kill).

> **Audience**: An operator new to Capo who has shell access on the Mac mini
> running Capo. You should be able to follow this top-to-bottom without
> needing to read the spec first.

**Spec source**: `internal/specs/capo-SPEC.md` §8 (Scope) + §10 (Testing) + §11
(Deployment & Operations). Per-feature runbooks (Litestream, launchd, alerts,
Phase 3/4 checkpoints) are linked inline and consolidated in §10
(Cross-references).

**Task source**: #61 (this document); aggregates #57 (Litestream), #58
(launchd), #56 (`/healthz`), #59 (caffeinate), #60 (Logfire alerts).

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Initial install](#2-initial-install)
3. [First boot](#3-first-boot)
4. [Day-2 operations](#4-day-2-operations)
5. [Troubleshooting](#5-troubleshooting)
6. [Restart-resume drill (§10.2 90-minute test)](#6-restart-resume-drill-1002-90-minute-test)
7. [Cost cap response](#7-cost-cap-response)
8. [Kill drill](#8-kill-drill)
9. [Routine maintenance](#9-routine-maintenance)
10. [Cross-references](#10-cross-references)

---

## 1. Prerequisites

| Component | Minimum version | Verify | Notes |
|---|---|---|---|
| macOS | 13+ | `sw_vers` | Apple Silicon or Intel. Mac mini is the canonical host. |
| Python | 3.12+ | `python3 --version` | Pinned via `pyproject.toml` `requires-python`. |
| `uv` | recent | `uv --version` | Install: `brew install uv`. Path must match the launchd plist (`/opt/homebrew/bin/uv` on Apple Silicon, `/usr/local/bin/uv` on Intel). |
| `claude` (Claude Code CLI) | **≥ 2.1.138** | `claude --version` | Boot-time SemVer check (`MIN_CLAUDE_CODE_VERSION`). Earlier versions fail closed. |
| `codex` (Codex CLI) | **≥ 0.130.0** | `codex --version` | Required for Codex delegations. Phase 4 feature; may be disabled via `agents.codex.enabled = false`. |
| `litestream` | **0.5.x+** | `litestream version` | Multi-level compaction + LTX files. The shipped config is NOT compatible with 0.3.x. |
| `git` | any recent | `git --version` | Used by the worktree helper. |
| AMC instance | reachable | `curl -fsS $AMC_BASE_URL/healthz` | Sibling launchd job. Capo + AMC live on the same Mac mini. |
| Logfire | account + write token | `LOGFIRE_TOKEN` in launchd env | Optional but recommended. Set `LOGFIRE_IGNORE_NO_CONFIG=1` to run without. |
| Anthropic API key | active | `echo $ANTHROPIC_API_KEY` | Default-model provider. |
| OpenAI API key | active (optional) | `echo $OPENAI_API_KEY` | Required only if Codex / GPT models are in use. |

### 1.1 Happy path

```bash
sw_vers
python3 --version          # → 3.12.x or later
uv --version
claude --version           # → 2.1.138 or later
codex --version            # → 0.130.0 or later
litestream version         # → 0.5.x or later
```

All six commands print a version string. No errors.

### 1.2 What could go wrong

| Symptom | Cause | Grep / probe |
|---|---|---|
| `claude: command not found` under launchd, fine in shell | PATH in launchd doesn't include the CC install location | `tail ~/Library/Logs/capo/stderr.log \| grep "command not found"` |
| `Min Claude Code version 2.1.138` boot error | Old CC pinned in PATH | `grep MIN_CLAUDE_CODE_VERSION ~/Library/Logs/capo/stderr.log` |
| `codex exec resume` rejects flags | Codex < 0.130.0 in PATH | `codex --version` |
| Litestream startup logs `unknown field "compact"` | Litestream 0.3.x — the shipped YAML is 0.5.x-only | `litestream version` |
| AMC webhook 401 / connection refused | AMC not running or `AMC_WEBHOOK_SECRET` mismatch | `curl -v $AMC_BASE_URL/healthz`; `grep BAD_SIGNATURE ~/Library/Logs/capo/stderr.log` |

---

## 2. Initial install

### 2.1 Clone + sync

```bash
mkdir -p ~/dev/repos && cd ~/dev/repos
git clone <capo-repo-url> capo
cd capo

# Install all deps incl. dev. Pinned in uv.lock.
uv sync --group dev
```

`uv sync` builds the venv at `./.venv/` and installs ~80 transitive deps
(DBOS pulls Temporal SDK transitively — first sync is ~30 s).

### 2.2 Apply migrations

Capo owns `state.db`; Alembic manages its schema. DBOS auto-manages `dbos.db`
on its own — do NOT alembic-migrate it.

```bash
mkdir -p ~/.capo
uv run alembic upgrade head
```

Expected output: migrations `001_init` → `002_approvals` → `003_approvals_request_types`
→ `004_costs` apply in order, ending at the head revision. `~/.capo/state.db`
exists with the canonical schema.

> **Critical**: `state.db` and `dbos.db` MUST be **separate** files (spec
> §8.1, §15.3). `init_dbos` will refuse to launch if they collide on path.

### 2.3 `config.toml` + `.env`

#### `config.toml`

Canonical template lives in spec §15.3. Minimum viable copy:

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
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8080"
agent_id              = "capo"
listen_host           = "127.0.0.1"
listen_port           = 8090
max_boot_wait_seconds = 60

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

[users.owner]
amc_senders = ["+15551234567", "discord:user:123"]
display_name = "Owner"
```

The TOML key after `users.` becomes the literal `user_id` written to every
domain row — pick it carefully; it is hard to rename later.

#### `.env`

Secrets ONLY. `chmod 600` and `.gitignore`d.

```bash
cat > ~/dev/repos/capo/.env <<'EOF'
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
AMC_WEBHOOK_SECRET=...
AMC_BEARER_TOKEN=...
LOGFIRE_TOKEN=...
EOF
chmod 600 ~/dev/repos/capo/.env
```

Pydantic Settings validates both files at boot. A missing `AMC_WEBHOOK_SECRET`
fails fast with a clear error and Capo exits non-zero (§9.1 checkpoint row).

### 2.4 Install Litestream

See `internal/ops/litestream-install.md` §2 + §3 for full detail. Minimum:

```bash
brew install benbjohnson/litestream/litestream
litestream version          # confirm 0.5.x+

# Symlink the shipped config so brew services + launchd find it.
# Apple Silicon:
sudo ln -sf "$(pwd)/internal/ops/litestream.yml" /opt/homebrew/etc/litestream.yml
# Intel:
sudo ln -sf "$(pwd)/internal/ops/litestream.yml" /usr/local/etc/litestream.yml

# Configure replica destinations (local file for the drill; S3 for prod).
export CAPO_HOME="$HOME/.capo"
export CAPO_LITESTREAM_STATE_URL="file://${CAPO_HOME}/replicas/state"
export CAPO_LITESTREAM_DBOS_URL="file://${CAPO_HOME}/replicas/dbos"
mkdir -p "${CAPO_HOME}/replicas/state" "${CAPO_HOME}/replicas/dbos"

brew services start litestream
```

### 2.5 Install the launchd plist

See `internal/ops/launchd.md` §1 for full detail. Minimum:

```bash
USER_SHORT=$(whoami)
CAPO_DIR="${HOME}/dev/repos/capo"
DEST="${HOME}/Library/LaunchAgents/com.${USER_SHORT}.capo.plist"

mkdir -p "${HOME}/Library/LaunchAgents"
mkdir -p "${HOME}/Library/Logs/capo"

sed \
  -e "s|{{USER}}|${USER_SHORT}|g" \
  -e "s|{{HOME}}|${HOME}|g" \
  -e "s|{{CAPO_DIR}}|${CAPO_DIR}|g" \
  -e "s|{{LOGFIRE_TOKEN_REF}}|keychain:capo/logfire|g" \
  "${CAPO_DIR}/internal/ops/com.you.capo.plist" > "${DEST}"

plutil -lint "${DEST}"      # must print "OK"
```

Secrets go into the launchd GUI session env, not the plist:

```bash
launchctl setenv LOGFIRE_TOKEN "$(security find-generic-password -s capo -a logfire -w)"
# (Or, to run without Logfire:)
# launchctl setenv LOGFIRE_IGNORE_NO_CONFIG 1
```

### 2.6 Happy path

After §2.1-§2.5, the system is staged but Capo is NOT running yet — that's
§3 (First boot). Verify staging only:

```bash
ls ~/.capo/state.db                         # → exists
plutil -lint ~/Library/LaunchAgents/com.${USER_SHORT}.capo.plist   # → OK
brew services list | grep litestream        # → started
uv run python -c "from capo.config import load_settings; load_settings()"  # exits 0
```

### 2.7 What could go wrong

| Symptom | Cause | Fix |
|---|---|---|
| `alembic.config.MissingConfigFileError` | Wrong CWD (alembic.ini is repo-root-relative) | `cd ~/dev/repos/capo` before `alembic upgrade head` |
| `sqlite3.OperationalError: unable to open database` | `~/.capo/` doesn't exist | `mkdir -p ~/.capo` |
| `ValidationError: env_file '.env' has no AMC_WEBHOOK_SECRET` | Missing secret | Add it; re-run boot |
| Litestream daemon refuses to start | YAML couldn't expand env vars (prints `${CAPO_HOME}` literally in logs) | Export `CAPO_HOME` + `CAPO_LITESTREAM_*_URL` in the shell that starts `brew services` |
| `plutil -lint` reports a syntax error | Stray `<` or `&` in a substituted value | Re-render from a clean template; quote any shell-substituted value |

---

## 3. First boot

### 3.1 Bootstrap Capo via launchd

```bash
UID_NUM=$(id -u)
launchctl bootstrap gui/${UID_NUM} \
  "${HOME}/Library/LaunchAgents/com.${USER_SHORT}.capo.plist"
```

`bootstrap` (launchd v2) is preferred over the deprecated `load`. `RunAtLoad=true`
means Capo starts immediately — no `kickstart` needed on the first boot.

### 3.2 Verify `/healthz`

The local listener binds on `127.0.0.1:8090` (configurable via
`[amc].listen_port`).

```bash
# Wait up to 60s (Capo boot includes Alembic + DBOS launch + unread sweep).
until curl -fsS http://127.0.0.1:8090/healthz >/dev/null 2>&1; do
  sleep 2
done

curl -fsS http://127.0.0.1:8090/healthz | jq .
```

Expected body:

```json
{
  "status": "ok",
  "subsystems": {
    "db": "ok",
    "dbos": "ok",
    "amc": "ok",
    "dispatcher": "ok"
  },
  "uptime_seconds": 12,
  "last_webhook_ts": null
}
```

`last_webhook_ts` is `null` until the first inbound webhook lands.

### 3.3 Send a test AMC message

From your phone or AMC client, text the configured AMC sender (the one
mapped in `config.toml [users.owner].amc_senders`) with:

> hi

Within a few seconds you should see:

1. AMC delivers the webhook to Capo (`POST /amc/webhook`, HMAC-signed).
2. Capo ACKs in `< 1s` (fast-ACK; agent work runs in the dispatcher worker).
3. Capo replies in the same thread with the agent's response (default
   model, default SOUL).
4. `last_webhook_ts` in `/healthz` is now non-null.

### 3.4 Happy path

| Check | Command | Expected |
|---|---|---|
| Process loaded | `launchctl list \| grep capo` | Numeric PID (not `-`) |
| Healthz green | `curl -fsS http://127.0.0.1:8090/healthz \| jq .status` | `"ok"` |
| Stderr clean | `tail -n 50 ~/Library/Logs/capo/stderr.log` | No traceback |
| Round-trip | Text `hi` | Reply within ~5s |

### 3.5 What could go wrong

| Symptom | Grep | Fix |
|---|---|---|
| `launchctl list` shows pid `-`, exit `78` | `ENOEXEC` from `uv` path mismatch | Edit plist `ProgramArguments[0]` to match `which uv` |
| Healthz 200 but `subsystems.amc = "error: ..."` | `tail ~/Library/Logs/capo/stderr.log \| grep "amc:"` | Confirm AMC running (`curl $AMC_BASE_URL/healthz`); confirm bearer token |
| Healthz returns 503 | `curl -s http://127.0.0.1:8090/healthz \| jq .subsystems` | Inspect failing subsystem; see §5 |
| `BAD_SIGNATURE` on the first webhook | `grep BAD_SIGNATURE ~/Library/Logs/capo/stderr.log` | `AMC_WEBHOOK_SECRET` doesn't match AMC's; rotate both |
| Reply never arrives | `grep "dispatcher.error" ~/Library/Logs/capo/stderr.log` | Check Anthropic key / network |
| DBOS warning "SQLite is for development and testing" | Always emitted by DBOS 2.21.0 | Ignore (documented in spike S-2) |

---

## 4. Day-2 operations

### 4.1 Logs

Capo writes stdout/stderr to `~/Library/Logs/capo/`. Litestream writes to
its own log under Homebrew's var dir.

```bash
# Capo
tail -F ~/Library/Logs/capo/stdout.log ~/Library/Logs/capo/stderr.log

# Litestream (Apple Silicon)
tail -F /opt/homebrew/var/log/litestream.log
# (Intel: /usr/local/var/log/litestream.log)
```

Useful greps:

```bash
# All boot-time errors
grep -E "DBOSInitError|Settings.*ValidationError|sqlite3.OperationalError" \
  ~/Library/Logs/capo/stderr.log

# Workflow resume events
grep "capo.workflows.delegation.resume" ~/Library/Logs/capo/stdout.log

# Cost-cap firings
grep -E "capo.budget.(soft|hard)_cap" ~/Library/Logs/capo/stdout.log
```

### 4.2 `/healthz`

The canonical liveness probe. Hit it from anywhere on the loopback:

```bash
curl -fsS http://127.0.0.1:8090/healthz | jq .
```

Status semantics:

| `status` | HTTP | Meaning |
|---|---|---|
| `"ok"` | 200 | All subsystems reporting `"ok"`. |
| `"degraded"` | 200 | Optional subsystem failing (e.g. AMC stale but DB + DBOS + dispatcher OK). Capo still processes messages; outbound may queue. |
| `"error"` | 503 | A required subsystem is down (`db`, `dbos`, or `dispatcher`). |

The subsystem probes (`capo/transport/health.py`):

- `db` — `SELECT 1` against `state.db`.
- `dbos` — Connection to `dbos.db` succeeds.
- `amc` — Last successful AMC `send` (or AMC ping) within the configured
  staleness window (default 5 min).
- `dispatcher` — Each per-channel worker alive, queue depth <
  `concurrency.queue_depth_max` (default 100).

### 4.3 `/status` (in-chat)

`/status` is a zero-token slash command parsed BEFORE the agent loop. Text
it to Capo from your AMC client:

> /status

Capo replies with:

- Current session id + message count + approximate token count.
- Running delegations (one line each: `delegation_id`, agent, started_at,
  status, last activity).
- Last completed delegation summary line (if any).

This is the fastest "what is Capo doing right now?" probe. It does NOT
exercise external networks (no Anthropic call), so it works even when the
agent loop is short-circuited by a hard cost cap.

### 4.4 Cost monitoring

Capo persists daily totals to `state.db.daily_costs`:

```bash
sqlite3 ~/.capo/state.db <<'SQL'
SELECT date, model, ROUND(usd_total, 4) AS usd
FROM daily_costs
WHERE date >= date('now', '-7 days')
ORDER BY date DESC, usd DESC;
SQL
```

Per-delegation costs live on the `delegations` row (`cost_usd` column) — they
do NOT count against the main agent's daily total in V1 (§5.9 acceptance
criteria). Surface them with:

```bash
sqlite3 ~/.capo/state.db <<'SQL'
SELECT id, agent, status, ROUND(cost_usd, 4) AS usd, ended_at
FROM delegations
WHERE date(started_at) = date('now')
ORDER BY started_at DESC;
SQL
```

For the canonical cross-check against Logfire, see §7 (Cost cap response).

### 4.5 Happy path

| Check | Frequency | Command |
|---|---|---|
| Healthz green | Every minute (monitor) | `curl -fsS http://127.0.0.1:8090/healthz \| jq .status` |
| stderr clean | Daily | `wc -l ~/Library/Logs/capo/stderr.log` (compare to yesterday) |
| Litestream alive | Daily | `brew services list \| grep litestream` |
| Daily cost under soft cap | Hourly | `sqlite3 ~/.capo/state.db "SELECT SUM(usd_total) FROM daily_costs WHERE date = date('now');"` |
| No stuck delegations | Daily | `sqlite3 ~/.capo/state.db "SELECT COUNT(*) FROM delegations WHERE status='running' AND started_at < datetime('now','-6 hours');"` |

### 4.6 What could go wrong

| Symptom | Grep | First action |
|---|---|---|
| Healthz `degraded` for > 5 min | `grep "healthz status=degraded" ~/Library/Logs/capo/stderr.log` | Identify failing subsystem; see §5 |
| Daily cost approaching soft cap | Logfire alert "Daily cost approaching soft cap" (75%) | Review costs query; consider switching default model |
| Soft cap fired unexpectedly | `grep capo.budget.soft_cap ~/Library/Logs/capo/stdout.log` | Compare `daily_costs` to recent delegations (see §7) |
| `delegation_output` table swelling | `du -h ~/.capo/state.db` | Retention pruning paused — check `capo.memory.retention` span |
| Litestream log empty for > 10 min | `tail /opt/homebrew/var/log/litestream.log` | `brew services restart litestream` |

---

## 5. Troubleshooting

A symptom-driven catalogue. Each entry: happy-path indicator → degraded
indicator → grep → action.

### 5.1 Degraded `/healthz`

**Happy**: `status = "ok"`, all four subsystems `"ok"`, HTTP 200.

**Degraded / error**:

```bash
curl -s http://127.0.0.1:8090/healthz | jq .
```

| Failing subsystem | Likely cause | Grep | Recovery |
|---|---|---|---|
| `db: error` | `state.db` locked, missing, or corrupted | `grep "sqlite3.OperationalError" ~/Library/Logs/capo/stderr.log` | If migrations partial: `uv run alembic current` + `upgrade head`. If corruption: restore from Litestream (§7 of `internal/ops/litestream-install.md`). |
| `dbos: error` | `dbos.db` locked or migrations stuck | `grep "DBOSInitError" ~/Library/Logs/capo/stderr.log` | Restart Capo (`launchctl kickstart -k gui/${UID_NUM}/com.${USER_SHORT}.capo`). If `operation=db_corrupt`, paired restore via Litestream. |
| `amc: error: ...` | AMC unreachable, bearer expired, or last send stale | `grep -E "(PLATFORM_AUTH\|amc.send.error)" ~/Library/Logs/capo/stderr.log` | See §5.2 below. |
| `dispatcher: error` | Worker crashed or queue depth saturated | `grep "dispatcher.worker.error" ~/Library/Logs/capo/stderr.log` | Restart Capo. Inspect for runaway agent loop in the offending channel. |

### 5.2 AMC unreachable

**Happy**: AMC `/healthz` returns 200; inbound webhooks land in
`~/Library/Logs/capo/stdout.log` as `capo.transport.amc.webhook.received`
spans.

**Degraded**:

```bash
# 1. Is AMC up?
curl -fsS "$(grep ^base_url config.toml | awk '{print $3}' | tr -d '"')/healthz"

# 2. Are recent outbound sends 5xx?
grep "capo.transport.amc_client.send.error" ~/Library/Logs/capo/stderr.log | tail -20

# 3. Are inbound webhooks arriving?
grep "capo.transport.amc.webhook.received" ~/Library/Logs/capo/stdout.log | tail -5
```

Recovery:

- **AMC process down** → start AMC's launchd job. Capo's unread-sweep at next
  boot will catch up any messages queued by AMC while Capo was down (§5.2
  acceptance criteria).
- **Bearer expired (`NotifyError(code="PLATFORM_AUTH")`)** → rotate token in
  keychain, `launchctl setenv` (if injected that way), kickstart Capo.
- **HMAC mismatch (`BAD_SIGNATURE` 401s in AMC's view)** → `AMC_WEBHOOK_SECRET`
  drift; rotate on both sides simultaneously.

### 5.3 DBOS workflow stuck

**Happy**: `dbos.db.workflow_status` rows that match running delegations are
`PENDING` or `SUCCESS`; `recovery_attempts` is low single digits.

**Degraded**: delegation row stuck `running` for > 6 hours with no recent
`delegation_output` rows.

```bash
# 1. Which workflows are pending?
sqlite3 ~/.capo/dbos.db <<'SQL'
SELECT workflow_uuid, status, recovery_attempts, updated_at
FROM workflow_status
WHERE status = 'PENDING'
ORDER BY updated_at;
SQL

# 2. Which delegations look orphaned?
sqlite3 ~/.capo/state.db <<'SQL'
SELECT id, agent, started_at, pid,
       (julianday('now') - julianday(started_at)) * 24 AS hours_running
FROM delegations
WHERE status = 'running'
ORDER BY started_at;
SQL

# 3. Cross-reference: is the subprocess alive?
ps -p <pid_from_state_db>
```

Recovery:

- **Subprocess alive, workflow pending** — DBOS will resume on next restart.
  `launchctl kickstart -k gui/$(id -u)/com.${USER_SHORT}.capo`; watch for
  `capo.workflows.delegation.resume.spawned` in stdout.log.
- **Subprocess dead, workflow pending forever** — DBOS at-least-once is
  retrying. Inspect `recovery_attempts`. If the workflow definition changed
  across the restart (decorator signature drift), manual surgery is needed:
  `UPDATE workflow_status SET status='ERROR' WHERE workflow_uuid='<uuid>';`
  + flip the `delegations` row to `failed`. File an issue documenting the
  schema-evolution path (per S-5 §5).
- **Workflow re-spawn fails** with `is_error=true` on `claude --resume` —
  per S-3 §4.2, row marked `failed` with `summary='session resume failed'`;
  original `session_id_subagent` preserved. Operator re-spawns by texting
  the original task again.

### 5.4 Cost cap reached

See §7 (Cost cap response) for the full playbook. Quick triage:

```bash
grep -E "capo.budget.(soft|hard)_cap" ~/Library/Logs/capo/stdout.log | tail -10
```

- **Soft cap** — Default model swapped to router in-memory until local
  midnight. User saw an AMC heads-up to `budget.notify_channel`. **No
  action required** unless the user wants to keep using `default`; bump
  `soft_daily_usd` in `config.toml` and restart.
- **Hard cap** — Agent loop short-circuited. User can unlock by texting
  `override` (case-insensitive) or `/override`. Confirm a
  `budget_overrides (date, user_id)` row appears for today.

### 5.5 Approval not arriving

**Happy**: `approvals` row inserted with `status='pending'`, AMC outbound
`POST /messages/send` with `Idempotency-Key: notify_approval:<32hex>` lands
in the user's thread within ~1 s.

**Degraded**: user texted a non-allowlisted command, never saw the approval
prompt.

```bash
# 1. Did the approval row land?
sqlite3 ~/.capo/state.db "SELECT approval_id, status, request_type, requested_at FROM approvals ORDER BY requested_at DESC LIMIT 3;"

# 2. Did notify_approval fire?
grep "capo.workflows.approval.notify_approval" ~/Library/Logs/capo/stdout.log | tail -5

# 3. Did AMC accept the outbound send?
grep "capo.transport.amc_client.send" ~/Library/Logs/capo/stderr.log | tail -10
```

Recovery:

- **Row pending but `notify_approval` never fired** — DBOS workflow not
  registered or `dbos.db` locked. Restart Capo.
- **`notify_approval` raised `PLATFORM_AUTH`** — per Task #39: row stays
  `pending`; rotate bearer, restart. Workflow resumes with same `approval_id`
  and re-sends.
- **Timeout fired before user replied** — `[approval].timeout_seconds`
  (default 1800 = 30 min) lapsed. Row flipped to `expired`; no command
  executed. User can re-text the original request to start over.
- **User replied `/approve <id>` but nothing happened** — dispatcher routed
  but `DBOS.send_async` lost the message. `grep
  "capo.transport.dispatcher.approval_decision"` to confirm routing;
  re-text `/approve <id>` (the no-double-fire invariant means a second
  delivery is safe — the workflow returns `APPROVAL_REPLY_ALREADY_RESOLVED`).

---

## 6. Restart-resume drill (§10.2 90-minute test)

This is the V1 success metric and the third row of the §9.3 Phase 3
checkpoint gate. Run it once per release against a real Claude Code and a
legitimately 90-minute task.

**Full operator runbook**: `internal/specs/spikes/S-5-phase3-checkpoint.md`
— the unmodified §10.2 critical path with checkpoint signals per step.

### 6.1 Happy path (summary)

| Step | Action | Signal |
|---|---|---|
| 1 | Text Capo: "spawn a long CC task (90 min)" | `delegations` row inserted; webhook ACK `< 1s`. |
| 2 | At t+5s: `session_id_subagent` captured | `sqlite3 ~/.capo/state.db "SELECT session_id_subagent FROM delegations WHERE id='<id>';"` non-NULL. |
| 3 | At t+45m: `launchctl kill TERM gui/$(id -u)/com.${USER_SHORT}.capo` | Capo exits; CC subprocess survives (orphaned by launchd, reparented to PID 1 — expected). |
| 4 | `launchctl kickstart -k gui/$(id -u)/com.${USER_SHORT}.capo` | New Capo PID; `capo.workflows.delegation.resume.spawned` event for `<id>`. |
| 5 | Resume step spawns `claude --resume <session_id>` | `ps aux \| grep 'claude --resume'` shows the resume invocation. |
| 6 | At t≈90m: CC completes | Row `status='completed'`, exactly **one** AMC `POST /messages/send` for the terminal message (verified via `Idempotency-Key` header). |
| 7 | Text `/status` | No running delegations; last entry is the completed run with cost. |

Heartbeat sub-test: total of **4** inbound messages from Capo (3 heartbeats at
5m / 15m / 60m, plus 1 terminal). The restart at t=45m MUST NOT cause
duplicate heartbeats — per-threshold idempotency keys (§5.13).

### 6.2 What could go wrong

| Failure | Grep / probe | Recovery |
|---|---|---|
| Capo fails to restart at step 4 | `launchctl print gui/$(id -u)/com.${USER_SHORT}.capo \| head -40`; `~/Library/Logs/capo/stderr.log` for `DBOSInitError` | If `operation=db_corrupt`, paired Litestream restore (§7 of `litestream-install.md`). |
| Workflow resumes but `claude --resume` returns `is_error=true` | `grep "capo.workflows.delegation.resume.is_error" ~/Library/Logs/capo/stdout.log` | Row marked `failed`, original `session_id_subagent` preserved. Re-spawn manually. |
| `notify_user` raises `PLATFORM_AUTH` after restart | `grep "notify_user.*PLATFORM_AUTH" ~/Library/Logs/capo/stderr.log` | Row stays `completed`; user notification lost (acceptable per S-5 §5). Rotate AMC bearer. |
| User receives **two** terminal notifications | Inspect both AMC server log entries for the `Idempotency-Key` header | If keys differ, `notify_user.idempotency_key_for(...)` derivation drifted — search recent commits in `capo/workflows/_idempotency.py`. If keys match, AMC-side dedupe bug — file with AMC team. |
| User receives **zero** terminal notifications, row is `completed` | `grep "capo.workflows.delegation.notify_(no_channel\|no_row)" ~/Library/Logs/capo/stdout.log` | Re-send manually via shell or a fresh user message. |
| DBOS recovery loops on unknown step ids | `sqlite3 ~/.capo/dbos.db "SELECT recovery_attempts FROM workflow_status WHERE status='PENDING';"` keeps climbing | Workflow definition changed across restart. `UPDATE workflow_status SET status='ERROR'`, mark row failed by hand, file schema-evolution issue. |

### 6.3 Sign-off

Record the run in spec §15.4 (Change log) per S-5 §6 (Sign-off Checklist).

---

## 7. Cost cap response

§5.9 of the spec defines soft + hard daily caps. This section is the
operator playbook for when one fires.

### 7.1 Happy path (no cap fired)

Daily total is comfortably under `budget.soft_daily_usd`. Periodic spot check:

```bash
TODAY=$(date +%Y-%m-%d)
sqlite3 ~/.capo/state.db <<SQL
SELECT
  '$TODAY' AS day,
  ROUND(SUM(usd_total), 2) AS total_usd,
  (SELECT soft_daily_usd FROM (SELECT 25 AS soft_daily_usd)) AS soft_cap,
  (SELECT hard_daily_usd FROM (SELECT 75 AS hard_daily_usd)) AS hard_cap
FROM daily_costs
WHERE date = '$TODAY';
SQL
```

(Replace the hardcoded 25/75 with whatever you set in `config.toml`.)

### 7.2 Soft cap fired

**Signal**: AMC message to `budget.notify_channel` reading "Soft cap reached.
Default model swapped to router for the rest of the day." Log event
`capo.budget.soft_cap`.

**Behavior**:
- Default model is `models.router` (e.g. `claude-haiku-4-5`) in-memory until
  local midnight.
- Reset at local midnight is automatic.
- All running delegations continue with their previously-frozen model — soft
  cap does NOT retroactively swap in-flight subagents.

**Action**: usually none. If the user wants `default` back today, bump
`budget.soft_daily_usd` in `config.toml` and restart — the in-memory swap
persists until restart, so editing the file alone won't restore the model.

### 7.3 Hard cap fired

**Signal**: AMC reply to the user reading "budget exceeded — reply 'override'
to continue today". Log event `capo.budget.hard_cap`.

**Behavior**:
- `capo.run` short-circuits with the override message — agent loop never runs.
- Dispatcher pre-parses inbound text BEFORE the agent loop (case-insensitive
  `override` or slash command `/override`); on match, inserts
  `budget_overrides (date, user_id)` row for today + current `user_id`.
- Override unlocks the rest of the day for that user; resets at local
  midnight.

### 7.4 Investigating a runaway

```bash
# 1. Today's costs per model
sqlite3 ~/.capo/state.db <<'SQL'
SELECT model, ROUND(usd_total, 4) AS usd
FROM daily_costs
WHERE date = date('now')
ORDER BY usd DESC;
SQL

# 2. Today's delegations sorted by cost (per-delegation costs do NOT roll up
# into the main daily total — but they're often the cause of the bill).
sqlite3 ~/.capo/state.db <<'SQL'
SELECT id, agent, model, status,
       ROUND(cost_usd, 4) AS usd,
       started_at, ended_at
FROM delegations
WHERE date(started_at) = date('now')
ORDER BY cost_usd DESC NULLS LAST
LIMIT 20;
SQL

# 3. Cross-reference Logfire (the canonical source — §5.9 reconciliation)
#    via the Logfire UI or MCP. Look for the nightly capo.budget.reconcile
#    span and check the drift_usd attribute.
```

If you identify a runaway delegation:

```bash
# Kill it. (See §8 for full kill drill.)
# /kill <delegation_id>  from the AMC client
# OR direct:
sqlite3 ~/.capo/state.db "SELECT pid FROM delegations WHERE id='<id>';"
kill -TERM <pid>
```

If you want to keep working through the cap for the rest of the day:

> Text Capo: `/override`
> — or just `override` (case-insensitive).

Verify the unlock:

```bash
sqlite3 ~/.capo/state.db "SELECT * FROM budget_overrides WHERE date = date('now');"
```

### 7.5 What could go wrong

| Symptom | Grep | Action |
|---|---|---|
| Hard cap message keeps re-firing after `override` | `sqlite3 ~/.capo/state.db "SELECT * FROM budget_overrides WHERE date=date('now') AND user_id='<id>';"` empty | Dispatcher missed the override token. Re-text `/override` explicitly. |
| `daily_costs` row much larger than the sum of `delegations.cost_usd` | Main agent burned tokens (long retries, large compaction) | Expected — main agent costs count, delegation costs don't (§5.9 acceptance). |
| `drift_usd` in `capo.budget.reconcile` exceeds ±$1 | Logfire alert "Daily cost approaching soft cap" misfired | §6.4 invariant violated; file an issue against the accountant. |
| Override unlocked but agent still refuses to run | `grep "capo.budget.hard_cap" ~/Library/Logs/capo/stderr.log` after the override | Restart Capo (the in-memory cap state may not have re-read the override row). |

---

## 8. Kill drill

§5.5 + §5.10 define `kill_delegation`. There are two paths:

- **Owner kill** (delegation `user_id` == requester `user_id`) → executes
  immediately, no approval.
- **Non-owner kill** (someone else's delegation) → routes through approval
  workflow with `request_type='kill_delegation'`.

### 8.1 Happy path: kill via `/kill`

```
> /kill <delegation_id>
```

Expected sequence:

1. Dispatcher parses the slash command, calls `kill_delegation(<id>)`.
2. Owner check passes (you're killing your own).
3. `delegations` row flips to `killed` with `summary` populated.
4. Subprocess receives SIGTERM. Reader handle drains remaining stdout.
5. **Cascade**: any `approvals` row with `request_payload.$.delegation_id ==
   <id>` and `status='pending'` is `force_resolve_approval(...,
   status='cancelled', resolved_by='system:killer:<user>', reason='delegation
   <id> killed')`-ed. User gets one AMC "approval cancelled" message per
   tied approval.
6. Capo replies with the kill confirmation.

Verify:

```bash
sqlite3 ~/.capo/state.db <<SQL
SELECT id, status, summary FROM delegations WHERE id='<delegation_id>';
SELECT approval_id, status, resolved_by, reason
FROM approvals
WHERE json_extract(request_payload, '\$.delegation_id') = '<delegation_id>';
SQL

ps -p <pid_from_before>     # → no such process
```

### 8.2 Happy path: manual kill (bypassing the agent)

When `/kill` itself is blocked (e.g. hard cost cap fired AND no override —
slash commands still route, but if the dispatcher is wedged, you can drop
to shell):

```bash
# Find the subprocess
sqlite3 ~/.capo/state.db "SELECT id, pid, agent FROM delegations WHERE status='running';"

# SIGTERM the chosen one. (Capo's reader will detect the exit on next poll.)
kill -TERM <pid>

# If it doesn't die in 30s, escalate
sleep 30
kill -9 <pid>

# Mark the row killed (dispatcher won't do this for you in this path)
sqlite3 ~/.capo/state.db <<SQL
UPDATE delegations
SET status='killed', summary='manual kill via shell', ended_at=datetime('now')
WHERE id='<id>';
SQL
```

> ⚠️ **Caveat**: manual shell-kill SKIPS the cascade-cancel of tied
> approvals (§8.1 step 5). Walk the approvals table by hand:
>
> ```bash
> sqlite3 ~/.capo/state.db <<SQL
> UPDATE approvals
> SET status='cancelled',
>     resolved_by='system:operator',
>     reason='delegation <id> manually killed',
>     decided_at=datetime('now')
> WHERE json_extract(request_payload, '\$.delegation_id') = '<id>'
>   AND status = 'pending';
> SQL
> ```

### 8.3 What could go wrong

| Symptom | Grep | Fix |
|---|---|---|
| `/kill` triggered an approval prompt (you didn't expect it) | `sqlite3 ~/.capo/state.db "SELECT user_id FROM delegations WHERE id='<id>';"` doesn't match requester | Non-owner path (§5.10). Reply `/approve <approval_id>` or `/deny`. |
| Subprocess won't die after SIGTERM | `ps -p <pid>` still present after 30s | Escalate to `kill -9`. Common with CC builds compiling large dep graphs. |
| Cascade missed a tied approval | `sqlite3 ~/.capo/state.db "SELECT approval_id, json_extract(request_payload, '$.delegation_id') FROM approvals WHERE status='pending';"` | Payload shape drift — `kill_delegation` uses `json_extract(request_payload, '$.delegation_id') = ?`. Hand-resolve via SQL (above). File issue. |
| Row flipped to `killed` but subprocess still alive | Reader handle lost the subprocess (rare; happens if PID was recycled) | `ps -p <pid>`; if alive, `kill -9 <pid>` directly. |
| Heartbeat fires AFTER kill | `grep "capo.workflows.delegation.heartbeat" ~/Library/Logs/capo/stdout.log` post-kill | Heartbeat poller hadn't seen the terminal status yet (eventual consistency, ~1s). Will stop on the next tick. |

### 8.4 Full wall-clock variant

The full kill-cascade-against-real-subprocess test is in S-6 §8 (Phase 4
operator runbook). Run once per release.

---

## 9. Routine maintenance

### 9.1 Restart Capo

```bash
UID_NUM=$(id -u)
launchctl kickstart -k gui/${UID_NUM}/com.${USER_SHORT}.capo
```

`-k` first sends SIGTERM (graceful drain) then relaunches.
`ThrottleInterval=10` means launchd refuses restarts more often than 1 per
10 s — expect a ~10 s pause between fast crashloop iterations.

### 9.2 Update Capo (git pull + restart)

```bash
cd ~/dev/repos/capo
git fetch && git pull --ff-only
uv sync --group dev
uv run alembic upgrade head    # idempotent if no new migrations
launchctl kickstart -k gui/$(id -u)/com.${USER_SHORT}.capo
curl -fsS http://127.0.0.1:8090/healthz | jq .status   # → "ok"
```

### 9.3 Rotate SOUL

```bash
# Edit souls/<name>.md or change [soul].active in config.toml
launchctl kickstart -k gui/$(id -u)/com.${USER_SHORT}.capo
```

SOUL takes effect on restart only — there is no runtime swap in V1 (§8.2).

### 9.4 Paired Litestream restore

See `internal/ops/litestream-install.md` §7. **Always** restore BOTH
`state.db` and `dbos.db` from the same timestamp — restoring only one leaves
Capo inconsistent (DBOS step idempotency keys point at delegation rows
across the file boundary).

### 9.5 Retention pruning

Capo prunes `delegation_output` chunks older than
`[retention].delegation_output_days` (default 7) on a nightly schedule
(§5.12 / Task #54). Verify it runs:

```bash
grep "capo.memory.retention" ~/Library/Logs/capo/stdout.log | tail -5
```

If `state.db` is growing unboundedly, the prune job is paused. Restart Capo
and re-check.

---

## 10. Cross-references

### Per-feature runbooks

- **Litestream install + paired restore drill** —
  `internal/ops/litestream-install.md` (task #57). §7 is the
  paired-restore drill required by §8.1 sign-off.
- **launchd plist + operator drill** — `internal/ops/launchd.md` (task #58).
  §6 is the manual supervision-contract drill.
- **`caffeinate` helper** — see task #59 output. Wraps the listener so the
  Mac doesn't sleep while delegations are `running`.
- **Logfire alerts** — see task #60 output. Six alerts from spec §11.3
  configured against the Logfire alerts API (delegation failure rate, AMC
  send 5xx, webhook sig failures, daily cost 75% soft, DBOS workflow
  failures, listener P99 > 2s).

### Per-phase operator gates

- **Phase 3 (restart-resume)**: `internal/specs/spikes/S-5-phase3-checkpoint.md`
  — the 90-minute §10.2 critical path against a real Claude Code. This
  runbook's §6 is the summary; S-5 is the authoritative checklist with
  sign-off rows.
- **Phase 4 (approvals + Codex + kill cascade)**:
  `internal/specs/spikes/S-6-phase4-checkpoint.md` — six operator-driven
  wall-clock variants (approve round-trip, deny, 30-min timeout, codex
  delegation, codex restart-resume, kill cascade). This runbook's §8
  (kill drill) is the summary; S-6 §8 is the full kill cascade against a
  real Codex subprocess.

### Spec sections

- §3.2 row 1 — V1 success metric (90-min restart-resume).
- §5.6 — DBOS Durable Monitoring + Restart Resume.
- §5.8 — Approval Flows.
- §5.9 — Cost caps.
- §5.10 — Session Control + Slash Commands.
- §5.12 — Health Check Endpoint (`/healthz`).
- §5.13 — Live Progress Reporting (heartbeat).
- §6.5 — Span taxonomy + healthz contract.
- §8.1 — Litestream + paired restore.
- §9.3 — Phase 3 Checkpoint Gate.
- §9.4 — Phase 4 Checkpoint Gate.
- §10.2 — Critical Path: Restart-Resilient Long Delegation.
- §11.3 — Monitoring & Alerting (Logfire alerts).
- §11.4 — Original runbook outline (this document is the realization).
- §15.3 — Canonical `config.toml`.
- §15.4 — Change log (record drill outcomes here).

### Demo runbooks (legacy, per-phase)

- `docs/runbook-phase1-demo.md` — Phase 1 echo-path demo.
- `docs/runbook-phase2-demo.md` — Phase 2 CC delegation demo.

These predate this top-level runbook and remain useful as
phase-completion-time smoke tests against a real AMC + real CLIs.

---

*Authored as part of task #61 (operator runbook) on 2026-05-11.*
*Aggregates per-feature runbooks from #57 (Litestream), #58 (launchd), and
adds the missing chapters: initial install, configuration, day-2 ops,
troubleshooting, restart-resume drill, cost-cap response, kill drill.*

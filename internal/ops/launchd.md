# Capo launchd runbook

Operator runbook for running Capo under macOS `launchd` as a per-user agent.

Source spec: `internal/specs/capo-SPEC.md` §8.1, §8.3, §9.5
Source task: #58
Plist template: `internal/ops/com.you.capo.plist`

---

## Overview

Capo runs as a **per-user** `launchd` agent (NOT a system daemon). It lives in
`~/Library/LaunchAgents/` and is owned by the operator account that texts /
DMs Capo. `KeepAlive` restarts Capo on crash; `RunAtLoad` boots Capo on user
login; an explicit `PATH` and `HOME` ensure `claude`, `codex`, and `uv`
resolve before any shell profile has run.

| Property | Value |
|---|---|
| Scope | Per-user (`~/Library/LaunchAgents`) |
| Domain | `gui/<uid>` |
| Restart on crash | Yes (`KeepAlive.SuccessfulExit=false`) |
| Boot on login | Yes (`RunAtLoad=true`) |
| Restart throttle | 10s (`ThrottleInterval`) |
| Logs | `~/Library/Logs/capo/{stdout,stderr}.log` |
| Working directory | `~/dev/repos/capo` (configurable) |

---

## Prerequisites

- macOS (Apple Silicon or Intel).
- `uv` installed at `/opt/homebrew/bin/uv` (Apple Silicon) or
  `/usr/local/bin/uv` (Intel). Verify with `which uv`.
- `claude` and `codex` CLIs installed and on PATH (typically
  `/opt/homebrew/bin/{claude,codex}` or `~/.local/bin/{claude,codex}`).
- Capo checkout at `~/dev/repos/capo` (or set `CAPO_DIR` to your path and
  substitute below).
- A valid `config.toml` adjacent to the checkout root (or at
  `${CAPO_DIR}/config.toml`).
- A Logfire write token (or `LOGFIRE_IGNORE_NO_CONFIG=1` if running without
  Logfire — see `capo/observability.py`).

---

## 1. Install

### 1.1 Render the template

The template ships as `internal/ops/com.you.capo.plist` with placeholders:

| Placeholder | Replace with | Example |
|---|---|---|
| `{{USER}}` | `whoami` | `alice` |
| `{{HOME}}` | `$HOME` | `/Users/alice` |
| `{{CAPO_DIR}}` | absolute path to the capo checkout | `/Users/alice/dev/repos/capo` |
| `{{LOGFIRE_TOKEN_REF}}` | a reference name for the secret (NOT the secret itself) | `keychain:capo/logfire` |

**Rename**: the deployed filename MUST embed the username, e.g.
`com.alice.capo.plist`. This matches the `Label` key inside the plist and
avoids collisions if two users share a Mac.

One-shot render + rename:

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

# Sanity-check it parses
plutil -lint "${DEST}"
```

### 1.2 Secret handling

Do NOT inline the Logfire token in the plist (the plist is world-readable on
some configurations and will end up in shell history if you copy it). Set the
token in the GUI launchd session BEFORE bootstrapping:

```bash
launchctl setenv LOGFIRE_TOKEN "$(security find-generic-password \
  -s capo -a logfire -w)"
```

Anything Capo's `EnvironmentVariables` block reads via `os.environ` will see
this. The plist's `LOGFIRE_TOKEN_REF` is a documentation breadcrumb pointing
to where the real token lives (keychain in the example above).

If you do not use Logfire, set `LOGFIRE_IGNORE_NO_CONFIG=1` instead:

```bash
launchctl setenv LOGFIRE_IGNORE_NO_CONFIG 1
```

### 1.3 Bootstrap

```bash
UID_NUM=$(id -u)
launchctl bootstrap gui/${UID_NUM} "${HOME}/Library/LaunchAgents/com.${USER_SHORT}.capo.plist"
```

`bootstrap` is the modern (`launchd` v2) load command — prefer it over the
deprecated `launchctl load`. `RunAtLoad=true` means Capo starts immediately.

---

## 2. Start / kickstart

`bootstrap` already starts the job, but for explicit restarts:

```bash
launchctl kickstart -k gui/${UID_NUM}/com.${USER_SHORT}.capo
```

`-k` first sends SIGTERM to the running process (giving Capo's shutdown
handler a chance to drain), then relaunches.

---

## 3. Status

```bash
# Is it loaded and running?
launchctl print gui/${UID_NUM}/com.${USER_SHORT}.capo | head -40

# Quick PID + exit-code snapshot
launchctl list | grep "com\.${USER_SHORT}\.capo"
#   <pid>   <last_exit_status>   com.<user>.capo
#   If <pid> is "-" the job is not currently running.
```

Cross-check with the local healthz endpoint (Task #56):

```bash
curl -fsS http://127.0.0.1:8000/healthz | jq .
```

Tail logs:

```bash
tail -F ~/Library/Logs/capo/stdout.log ~/Library/Logs/capo/stderr.log
```

---

## 4. Restart

Two flavors:

```bash
# Soft: SIGTERM then relaunch (preferred during normal ops)
launchctl kickstart -k gui/${UID_NUM}/com.${USER_SHORT}.capo

# Hard: unload then bootstrap (use after editing the plist itself)
launchctl bootout gui/${UID_NUM}/com.${USER_SHORT}.capo
launchctl bootstrap gui/${UID_NUM} \
  "${HOME}/Library/LaunchAgents/com.${USER_SHORT}.capo.plist"
```

`ThrottleInterval=10` means launchd will refuse to restart Capo more often
than once every 10 seconds. If you are debugging a fast crashloop, expect a
~10s pause between exits and re-spawns.

---

## 5. Uninstall

```bash
launchctl bootout gui/${UID_NUM}/com.${USER_SHORT}.capo
rm "${HOME}/Library/LaunchAgents/com.${USER_SHORT}.capo.plist"

# Optional: clear the launchd-session env vars so a future re-install starts clean
launchctl unsetenv LOGFIRE_TOKEN
launchctl unsetenv LOGFIRE_IGNORE_NO_CONFIG
```

Logs and Capo's SQLite state are NOT touched by uninstall; remove
`~/Library/Logs/capo/` and `${CAPO_DIR}/state.db` only if you mean to.

---

## 6. Manual operator drill (acceptance test for this task)

Run this end-to-end after install to validate the supervision contract. All
five steps must pass before marking the plist "deployed".

1. **Load.** Bootstrap per §1.3. `launchctl list | grep capo` shows a numeric
   PID (not `-`).
2. **Start.** `curl http://127.0.0.1:8000/healthz` returns 200 within ~5s of
   bootstrap. Tail `stderr.log` for any boot-time error.
3. **Survive SIGTERM.** Capture the PID, then:
   ```bash
   PID=$(launchctl list | awk -v lbl="com.${USER_SHORT}.capo" '$3==lbl {print $1}')
   kill -TERM "$PID"
   sleep 15      # > ThrottleInterval
   launchctl list | grep "com\.${USER_SHORT}\.capo"
   ```
   The new PID must be different from `$PID` and must not be `-`. `healthz`
   should be green again within ~10s after the throttle.
4. **Survive crash.** Force a crash (the cleanest way is to kill -9 the worker;
   alternatively, push a deliberate `raise SystemExit(1)` into a dev branch
   before the dispatcher starts and observe the relaunch):
   ```bash
   PID=$(launchctl list | awk -v lbl="com.${USER_SHORT}.capo" '$3==lbl {print $1}')
   kill -9 "$PID"
   sleep 15
   launchctl list | grep "com\.${USER_SHORT}\.capo"   # new PID, not "-"
   tail -n 50 ~/Library/Logs/capo/stderr.log         # restart noted
   ```
5. **Boot on login.** Log out and back in (or reboot the Mac mini). After
   login, within ~15s `launchctl list | grep capo` should show a running PID
   without any further intervention.

Record the drill outcome (PIDs observed, healthz timing, log excerpts) in the
operator notebook.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `launchctl list` shows pid `-` and exit code `78` (`ENOEXEC`) | `uv` not at `/opt/homebrew/bin/uv` (Intel Mac) | Edit plist: change first `ProgramArguments` entry to `/usr/local/bin/uv`, then `bootout`+`bootstrap` |
| Repeated relaunches, `stderr.log` shows `claude: command not found` | PATH missing the Homebrew bin dir for this arch | Confirm `/opt/homebrew/bin` vs `/usr/local/bin` ordering matches your arch; rebuild plist and reload |
| Crash-loop pegging CPU | `ThrottleInterval` too low or missing | Verify `<key>ThrottleInterval</key><integer>10</integer>` is in the plist |
| Capo runs but Claude Code config not found | `HOME` not set in plist (`launchd` does NOT inherit it) | Confirm `EnvironmentVariables.HOME` is present and set to the operator's home |
| Logs empty even though `launchctl list` shows a PID | `~/Library/Logs/capo/` does not exist | `mkdir -p ~/Library/Logs/capo` and kickstart |
| Logfire spans not arriving | `LOGFIRE_TOKEN` not in launchd session env | `launchctl setenv LOGFIRE_TOKEN ...` then kickstart |
| Healthz returns 503 (subsystem unhealthy) | A dependency (Litestream / DBOS / AMC) failed | Inspect healthz body for failing subsystem and tail `stderr.log` |

---

## 8. Edge cases (documented per task #58)

- **Intel Mac PATH.** `/opt/homebrew/bin` does not exist on Intel; the
  template's PATH still works because `/usr/local/bin` is next in line.
  However, the `ProgramArguments` literal `/opt/homebrew/bin/uv` MUST be
  edited to `/usr/local/bin/uv` on Intel — `launchd` does NOT do PATH lookup
  for `ProgramArguments[0]`, it `execve()`s it directly. This is the rename
  step called out in §1.1.
- **`HOME` not inherited from the user session.** `launchd` jobs start with
  a near-empty environment. Claude Code reads
  `~/.config/claude/` and Codex writes rollouts to `~/.codex/`; both fail
  silently or pick wrong paths if `HOME` is unset. The plist sets `HOME`
  explicitly in `EnvironmentVariables` to avoid this.
- **`~/.cargo/bin`.** Included in PATH because some Litestream / ripgrep /
  tokio-console installations live there. Harmless if the directory does not
  exist.
- **Multiple operators on one Mac.** Each user must install their OWN plist
  (`com.<their_user>.capo.plist`) — the label includes the username so the
  two jobs do not collide.

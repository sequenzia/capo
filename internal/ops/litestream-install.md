# Litestream Install + Operator Runbook (Capo §8.1)

This runbook covers operator-side setup of [Litestream](https://litestream.io)
for replicating Capo's two SQLite databases (`~/.capo/state.db` and
`~/.capo/dbos.db`) and the **paired restore drill** required by spec §8.1.

> Canonical config: `internal/ops/litestream.yml`. All commands below assume
> the repo is checked out and the working directory is the repo root.
>
> Pin: Litestream **0.5.x** or later (multi-level compaction + LTX files).
> The config in this repo is NOT compatible with the older 0.3.x line.

---

## 1. Concept — why two pipes?

Capo persists state in two **separate** SQLite files:

| File             | Owner            | Holds                                        |
| ---------------- | ---------------- | -------------------------------------------- |
| `~/.capo/state.db` | Capo / Alembic | Delegations, messages, approvals, costs     |
| `~/.capo/dbos.db`  | DBOS            | Workflow + step state, event K/V            |

Litestream replicates each one independently. **Loss of one does not corrupt
the other**, but the two are referentially linked at the application layer:
DBOS step idempotency keys (in `dbos.db`) point at delegation rows (in
`state.db`).

> ⚠️ **Restore implication**: when recovering from disaster you MUST restore
> BOTH files from a matching timestamp window. Restoring only one leaves
> Capo in an inconsistent state (orphaned workflows, missing approvals, etc.).
> The paired-restore drill below makes this explicit.

---

## 2. Install Litestream (macOS, Homebrew)

```bash
# Install
brew install benbjohnson/litestream/litestream

# Sanity check — 0.5.x or later required by the config in this repo
litestream version
```

If `brew` is not on `PATH` for `launchd` (Apple Silicon installs land in
`/opt/homebrew/bin`), update the Capo launchd plist `PATH` env entry too
(see §7.1 of the spec / `internal/ops/launchd-*.plist`).

---

## 3. Configure replica destinations

The config in `internal/ops/litestream.yml` reads its replica URLs from
environment variables so the same file works for both the local-verification
drill and production cloud deployments.

### 3a. Default — local file replica (for the drill)

Stand up a local replica directory; no cloud creds needed:

```bash
export CAPO_HOME="$HOME/.capo"
export CAPO_LITESTREAM_STATE_URL="file://${CAPO_HOME}/replicas/state"
export CAPO_LITESTREAM_DBOS_URL="file://${CAPO_HOME}/replicas/dbos"

mkdir -p "${CAPO_HOME}/replicas/state" "${CAPO_HOME}/replicas/dbos"
```

These are the **defaults assumed by the rest of this runbook**. Operators
verifying replication on a fresh machine should start here.

### 3b. Production — S3 (preferred)

```bash
export CAPO_HOME="$HOME/.capo"
export CAPO_LITESTREAM_STATE_URL="s3://my-capo-bucket/state.db"
export CAPO_LITESTREAM_DBOS_URL="s3://my-capo-bucket/dbos.db"

export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
```

Bucket policy hint: lifecycle-expire `*/snapshots/*` objects older than
`snapshot.retention` (168h by default) on the bucket side; Litestream itself
will only prune what it knows about.

### 3c. Production — filesystem replica (NAS, external drive)

```bash
export CAPO_LITESTREAM_STATE_URL="file:///Volumes/CapoBackup/state"
export CAPO_LITESTREAM_DBOS_URL="file:///Volumes/CapoBackup/dbos"
```

---

## 4. Run as a `brew services` background daemon

`litestream` ships with a Homebrew service definition that runs the daemon
under launchd as the current user.

### 4a. Make the config readable from the canonical service location

Homebrew's plist points at `/usr/local/etc/litestream.yml` (Intel) or
`/opt/homebrew/etc/litestream.yml` (Apple Silicon). Symlink the repo copy:

```bash
# Apple Silicon
sudo ln -sf "$(pwd)/internal/ops/litestream.yml" /opt/homebrew/etc/litestream.yml

# Intel
sudo ln -sf "$(pwd)/internal/ops/litestream.yml" /usr/local/etc/litestream.yml
```

(Or set `LITESTREAM_CONFIG=$(pwd)/internal/ops/litestream.yml` in the launchd
env — see `internal/ops/launchd-*.plist`.)

### 4b. Export the replica env vars so the service inherits them

`brew services` reads env from `~/Library/LaunchAgents/homebrew.mxcl.litestream.plist`.
The simplest path: export the variables in your shell, then start the service
from the same shell so it inherits them. For persistence across reboots,
inline them into the plist's `EnvironmentVariables` block.

### 4c. Start + check status

```bash
brew services start litestream
brew services list                 # litestream should report "started"

# Tail logs
tail -f /opt/homebrew/var/log/litestream.log    # Apple Silicon
tail -f /usr/local/var/log/litestream.log       # Intel
```

A healthy startup log contains, for each db:

```
level=INFO  msg="opened database" path=/Users/.../state.db
level=INFO  msg="initialized replica" name=file
level=INFO  msg="write snapshot" db=...state.db
```

### 4d. Stop / restart

```bash
brew services stop    litestream
brew services restart litestream
```

---

## 5. Run-foreground alternative (for the verify drill)

If you'd rather not register a service while drilling, run the daemon in the
foreground from a terminal. This is also what CI / a smoke test would invoke.

```bash
litestream replicate -config internal/ops/litestream.yml
```

Stop with `^C`. Litestream will flush any pending LTX files before exiting.

---

## 6. Verification drill — write propagates to the replica

Goal: confirm a write to `state.db` appears in the local replica within ~2s
(default `sync-interval: 1s` + one compaction tick).

### 6a. Pre-flight

```bash
# Ensure both DBs exist (created by Capo on first boot).
ls -lh ~/.capo/state.db ~/.capo/dbos.db

# Start Litestream (service or foreground, see §4 or §5).
```

### 6b. Confirm Litestream sees both DBs

```bash
litestream databases -config internal/ops/litestream.yml
```

Expected output: two rows, one for `state.db`, one for `dbos.db`, each with
the replica URL you configured.

### 6c. Trigger an isolated write and watch the replica

```bash
# Issue an idempotent write to state.db using sqlite3 directly.
# (Touches a benign sqlite_schema-adjacent table; safe to roll back.)
sqlite3 ~/.capo/state.db "CREATE TABLE IF NOT EXISTS _capo_replication_probe(ts TEXT); INSERT INTO _capo_replication_probe VALUES (datetime('now'));"

# Wait ~2 seconds (1s sync + safety margin).
sleep 2

# List snapshots / generations on the replica.
litestream snapshots -config internal/ops/litestream.yml ~/.capo/state.db
```

Expected: at least one snapshot row, with a `created_at` newer than your
session start. Optional sanity-check — restore to a scratch path and inspect
the probe row:

```bash
litestream restore -config internal/ops/litestream.yml \
    -o /tmp/state-probe.db ~/.capo/state.db

sqlite3 /tmp/state-probe.db "SELECT * FROM _capo_replication_probe;"
# → expect to see your timestamp

rm /tmp/state-probe.db
```

### 6d. Repeat for `dbos.db`

DBOS owns the `dbos.db` schema — do NOT inject a probe table directly.
Instead, trigger any DBOS workflow step (e.g. by sending a message through
the AMC webhook) and re-run §6c against `dbos.db`. A new snapshot or LTX file
must appear within ~2 s.

### 6e. Clean up the probe (state.db only)

```bash
sqlite3 ~/.capo/state.db "DROP TABLE _capo_replication_probe;"
```

---

## 7. Paired restore drill (§8.1 checkpoint)

This is the **headline drill** for Phase 5 sign-off. It validates that an
operator can recover Capo to a consistent point in time.

### 7a. Pick a target restore time

Choose a wall-clock timestamp `T` (RFC3339, e.g. `2026-05-10T15:30:00Z`).
For a "latest" restore, omit `-timestamp` (defaults to the newest LTX).

### 7b. Stop Capo and Litestream

```bash
# Stop Capo (whatever your supervisor uses — launchd, manual, etc.)
launchctl unload ~/Library/LaunchAgents/com.you.capo.plist

# Stop Litestream so it does not race with the restore.
brew services stop litestream
```

### 7c. Move existing DBs aside (do NOT delete — let the operator inspect)

```bash
mv ~/.capo/state.db  ~/.capo/state.db.pre-restore
mv ~/.capo/dbos.db   ~/.capo/dbos.db.pre-restore
# Also move WAL/SHM siblings if present.
mv ~/.capo/state.db-wal ~/.capo/state.db-wal.pre-restore 2>/dev/null || true
mv ~/.capo/state.db-shm ~/.capo/state.db-shm.pre-restore 2>/dev/null || true
mv ~/.capo/dbos.db-wal  ~/.capo/dbos.db-wal.pre-restore  2>/dev/null || true
mv ~/.capo/dbos.db-shm  ~/.capo/dbos.db-shm.pre-restore  2>/dev/null || true
```

### 7d. Restore BOTH from the same timestamp

```bash
# IMPORTANT: same -timestamp for both, or both with no timestamp (latest).

litestream restore -config internal/ops/litestream.yml \
    -o ~/.capo/state.db \
    -timestamp 2026-05-10T15:30:00Z \
    ~/.capo/state.db

litestream restore -config internal/ops/litestream.yml \
    -o ~/.capo/dbos.db \
    -timestamp 2026-05-10T15:30:00Z \
    ~/.capo/dbos.db
```

For the "latest" variant, drop the `-timestamp` flag from both commands.

### 7e. Integrity-check both DBs

```bash
litestream restore -config internal/ops/litestream.yml \
    -integrity-check full \
    -o /tmp/state-check.db ~/.capo/state.db && rm /tmp/state-check.db

litestream restore -config internal/ops/litestream.yml \
    -integrity-check full \
    -o /tmp/dbos-check.db ~/.capo/dbos.db && rm /tmp/dbos-check.db

# Application-level sanity:
sqlite3 ~/.capo/state.db "PRAGMA integrity_check;"   # → "ok"
sqlite3 ~/.capo/dbos.db  "PRAGMA integrity_check;"   # → "ok"
```

### 7f. Restart Litestream BEFORE Capo

Litestream must take ownership of the new files before Capo opens them, or
the first writes will not be captured.

```bash
brew services start litestream
# Wait for "initialized replica" lines for BOTH dbs in the litestream log.
tail -f /opt/homebrew/var/log/litestream.log | grep -m 2 "initialized replica"

# Now boot Capo.
launchctl load ~/Library/LaunchAgents/com.you.capo.plist
```

### 7g. Smoke-test Capo end-to-end

Send a message through AMC; confirm:
- `/healthz` returns 200 with all subsystems "ok".
- Existing in-flight delegations are visible (`/list_delegations`).
- New writes show up as fresh LTX files in `~/.capo/replicas/...`.

### 7h. Drill sign-off checklist

- [ ] Both DBs restored from the **same** timestamp (or both `latest`).
- [ ] `PRAGMA integrity_check` returned `ok` for both.
- [ ] Litestream restarted **before** Capo.
- [ ] Capo boot logs show no DBOS workflow errors.
- [ ] `~/.capo/state.db.pre-restore` kept for at least 24h post-drill.

---

## 8. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `failed to open database: locked` on Litestream start | Capo opened the DB before Litestream | Stop Capo, start Litestream first, then Capo |
| Replica directory empty after first boot | Env var not exported, or path not writable | Verify `CAPO_LITESTREAM_*_URL`; ensure parent dir exists |
| `litestream databases` lists 0 dbs | YAML couldn't expand env vars (printed `${CAPO_HOME}`) | Run from a shell where `CAPO_HOME` is exported, or hardcode in plist |
| Restored DB missing recent writes | Restore picked an older snapshot/timestamp | Re-run restore without `-timestamp` for "latest" |
| `dbos.db` and `state.db` disagree (workflow refs missing delegations) | Restored from mismatched timestamps | Re-run §7 with the **same** timestamp for both pipes |

---

## 9. Cross-references

- Spec: `internal/specs/capo-SPEC.md` §8.1, §6.2 (replica targets user-owned),
  §11.1 (rollback procedure requires paired restore), §13 (risk table —
  "Litestream restore inconsistency with DBOS state").
- launchd plist: `internal/ops/launchd-*.plist` (Task #58) — must include the
  `CAPO_LITESTREAM_*_URL` env vars so the daemon survives reboot.
- Health check: `capo/transport/health.py` (Task #56) — Phase 5 may expose a
  Litestream subsystem probe (lag in seconds vs. fresh write).

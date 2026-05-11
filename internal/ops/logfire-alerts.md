# Capo — Logfire alerts runbook

This runbook explains how to apply the alert catalogue in
[`logfire-alerts.yml`](./logfire-alerts.yml) against your Logfire project. The
catalogue is **declarative**; Capo does NOT auto-create alerts at boot.

> Source of truth: spec §11.3 (monitoring & alerting table) plus §6.5 (span
> taxonomy enforced in code by Task #51 / `capo/observability.py`). If you
> rename a span or change an attribute, update both the spec and this file.

---

## 1. Prerequisites

You need:

1. A Logfire project with a **write token** OR a session bound via the
   Logfire MCP (the `mcp__logfire__*` tools in your client). The MCP route is
   strongly preferred — it scopes credentials per-session and avoids leaking a
   long-lived control-plane token onto disk.
2. An incoming-webhook URL for your notification destination. V1 prescribes
   Slack:

   - Slack → *Apps* → *Incoming Webhooks* → *Add to Slack* → pick the channel
     (e.g. `#capo-alerts`) → copy the URL of the form
     `https://hooks.slack.com/services/T.../B.../...`.
   - Logfire alerts deliver in Slack message format, so any webhook target
     that accepts that shape (Slack, Mattermost-with-Slack-compat, an AMC
     bridge, etc.) will work — but the operator is responsible for that
     wiring. Discord and Opsgenie are first-class Logfire channel types and
     can be substituted via `channel_type=discord|opsgenie` below.

3. Permission to create alerts in the Logfire project (a project member with
   Admin or Member role).

If pre-launch you have no telemetry ingesting yet, the alerts created from
this file are safe to apply — every query selects against `records` filtered by
a `start_timestamp > now() - INTERVAL ...` predicate, so they all return zero
rows (no notifications) until Capo actually starts emitting spans. There is no
"alerts safely no-op" toggle needed.

---

## 2. One-time channel setup

Create one notification channel per logical destination in the YAML
`channels:` block. Use the Logfire MCP from your client:

```text
# 1. Create the primary on-call channel.
mcp__logfire__channel_create(
    name="capo_ops_primary",
    type="slack",
    config={"webhook_url": "https://hooks.slack.com/services/T.../B.../..."},
)

# 2. (Optional) Create a secondary / informational channel. Re-use the primary
#    if you only have one Slack room.
mcp__logfire__channel_create(
    name="capo_ops_secondary",
    type="slack",
    config={"webhook_url": "https://hooks.slack.com/services/T.../B.../..."},
)
```

If your client exposes a different field name for the type/config (the Logfire
API is the canonical source — channel types include `slack`, `discord`,
`opsgenie`), match the tool's expected shape. The point is **one logical
channel per `channels:` entry in `logfire-alerts.yml`**.

Then list channels to capture the IDs you'll pass to `alert_create`:

```text
mcp__logfire__channel_list()
# → records like {id: "ch_abc123", name: "capo_ops_primary", ...}
```

Record the resolved channel IDs locally — you'll reference them in step 3.

### Slack webhook setup (detailed)

1. Open https://api.slack.com/apps → *Create New App* → *From scratch*.
2. App name: `Capo Alerts`, workspace: your workspace.
3. *Features* → *Incoming Webhooks* → toggle **On**.
4. *Add New Webhook to Workspace* → pick the destination channel.
5. Copy the webhook URL (it includes a secret; treat it as you would an API
   key — store in a password manager and supply only at `channel_create`
   time, never commit to git).

### AMC bridge (if you prefer alerts via your existing AMC notify channel)

Logfire alerts emit Slack-formatted JSON. To route them through AMC, stand up
a tiny HTTP receiver that:

1. Accepts a Slack-format incoming-webhook POST.
2. Re-shapes the payload into an AMC `send` REST call against
   `notify_channel` (channel id is the operator's iMessage/Discord channel).
3. Returns 2xx so the Logfire retry layer doesn't redeliver.

The bridge is out of scope for V1 — `ops_primary = slack` is recommended. The
runbook §11.4 "Capo isn't responding" entry assumes you can see Slack while
Capo is down (loopback FastAPI listener is unreachable to AMC during outage).

---

## 3. Applying alerts

For each entry under `alerts:` in `logfire-alerts.yml`, invoke
`mcp__logfire__alert_create`. There is one MCP call per alert — there is no
bulk `alerts apply` endpoint. A bash helper using `yq` makes this tolerable
when you need to reapply after a config change:

```bash
# Pretty-print all alert names + queries (no API calls):
yq '.alerts[] | {name: .name, time_window: .time_window, query: .query}' \
    internal/ops/logfire-alerts.yml
```

Per alert, the call shape is:

```text
mcp__logfire__alert_create(
    name             = <name from YAML>,
    query            = <query from YAML, multi-line OK>,
    time_window      = <time_window from YAML, e.g. "5m" / "15m" / "1h">,
    notification_mode= <notification_mode from YAML, default any_results>,
    channel_ids      = [<resolved id for each name in `channels:` from YAML>],
)
```

Worked example — applying `capo_healthz_failures_5m` (the §60 critical alert):

```text
mcp__logfire__alert_create(
    name="capo_healthz_failures_5m",
    query="""
        SELECT
            trace_id,
            attributes->>'http.route'        AS route,
            attributes->>'http.status_code'  AS status_code,
            start_timestamp
        FROM records
        WHERE
            attributes->>'http.route' = '/healthz'
            AND CAST(attributes->>'http.status_code' AS BIGINT) >= 500
            AND start_timestamp > now() - INTERVAL '5 minutes'
        ORDER BY start_timestamp DESC
        LIMIT 100
    """,
    time_window="5m",
    notification_mode="any_results",
    channel_ids=["ch_abc123"],  # resolved from channel_list above
)
```

The MCP tool returns the created alert's id; record it if you intend to
update or delete the alert later via `alert_update` / `alert_delete`.

### Severity is operator-side

Logfire alerts do not carry a native `severity` field. Encode severity by:

- routing critical/high alerts to `ops_primary` (channel that pages),
- routing medium/informational alerts to `ops_secondary` (a quieter
  destination, possibly the same Slack room), and
- including a `[CRITICAL]` / `[HIGH]` / `[MEDIUM]` prefix in the alert's
  `name` if your client's Slack rendering benefits from it (Logfire embeds
  the name in the notification body).

### Verifying the queries before applying

If you want to confirm a query is syntactically valid against your project's
ingest, run it once via `query_run` with a short time window. A `SELECT ...`
query that succeeds in `query_run` is identical to what `alert_create`
accepts.

```text
mcp__logfire__query_run(
    query="SELECT count(*) FROM records WHERE span_name = 'capo.amc.send' LIMIT 1"
)
```

---

## 4. The alert catalogue (one-line summary)

| Alert name | Severity | Window | §11.3 row / AC #60 line | Source span |
|---|---|---|---|---|
| `capo_delegation_failures_hourly`        | high     | 1h  | §11.3 "Delegation failure rate" + AC §60 line 2 | `capo.delegation.*.complete` `status='failed'` |
| `capo_amc_send_errors_15m`               | high     | 15m | §11.3 "AMC send 5xx" + AC §60 line 3            | `capo.amc.send` `error_code IN ('PLATFORM_AUTH','INTERNAL_ERROR')` |
| `capo_approval_workflow_exceptions_15m`  | medium   | 15m | AC §60 line 4                                   | `capo.approval.request` `is_exception=true` |
| `capo_dbos_workflow_exceptions_15m`      | high     | 15m | §11.3 "DBOS workflow failures" + AC §60 line 5  | `capo.delegation.*.monitor` ∪ `capo.approval.request` `is_exception=true` |
| `capo_budget_hard_cap_reached`           | critical | 15m | AC §60 line 6 (informational)                   | `capo.budget.cap` `cap_kind='hard'` |
| `capo_budget_soft_cap_early_warning`     | medium   | 1h  | §11.3 "Daily cost approaching soft cap"         | `capo.budget.cap` `cap_kind='soft'` |
| `capo_healthz_failures_5m`               | critical | 5m  | AC §60 line 7                                   | `records` `http.route='/healthz'` `status>=500` |
| `capo_webhook_signature_failures_5m`     | high     | 5m  | §11.3 "Webhook signature failures"              | `capo.amc.webhook.in` `signature_ok='false'` |
| `capo_webhook_p99_latency_5m`            | medium   | 5m  | §11.3 "Listener handler P99 latency"            | `capo.amc.webhook.in` `duration` P99 |

---

## 5. Updating / removing alerts

The Logfire MCP exposes `alert_update` and `alert_delete`. To safely reapply
this catalogue after a YAML change:

1. List existing alerts: `mcp__logfire__alert_list()` (filter by name prefix
   `capo_`).
2. For names that appear in BOTH the existing list and `logfire-alerts.yml` →
   `alert_update` with the new query / time_window.
3. For names that appear ONLY in the existing list (you removed them from
   YAML) → `alert_delete` after confirming with the operator.
4. For names that appear ONLY in YAML → `alert_create` per §3.

A short shell script that drives the MCP through this loop is operator-
maintained and lives in `~/.capo/scripts/` (not in this repo — it would carry
the channel-id mapping which is environment-specific).

---

## 6. Troubleshooting

**Symptom: alert created but never fires even though I know the underlying
event happened.**

- Confirm the underlying span is actually emitted: run the alert's `query`
  with a wider `now() - INTERVAL ...` window via `query_run`. If zero rows,
  the span isn't reaching Logfire (likely cause: `LOGFIRE_TOKEN` unset; see
  `capo/observability.py` and the runbook §11.4 "Capo isn't responding"
  entry).
- Confirm the alert was created against the expected project: the Logfire
  MCP can be scoped to multiple projects; `alert_list` will only show alerts
  for the currently-scoped project.

**Symptom: alert fires every poll even though the count is stable.**

- `notification_mode: any_results` notifies on every run that returns rows.
  Switch to `results_change` or `starts_having_results` for less noise. The
  catalogue defaults to `any_results` because §11.3 alerts are page-worthy;
  switch on a per-alert basis once you've calibrated.

**Symptom: `CAST(... AS BIGINT)` errors on the healthz query.**

- The `http.status_code` attribute is a string in OTel semantic conventions
  but Logfire sometimes ingests it as an integer. Try
  `attributes->>'http.status_code' >= '500'` (lexicographic — works because
  all 5xx codes are 3-digit) as a fallback.

**Symptom: `now() - INTERVAL '5 minutes'` doesn't parse.**

- Apache DataFusion accepts `INTERVAL '5 minutes'` and `INTERVAL '5' MINUTE`
  forms. If one fails, try the other. `time_window: 5m` on the alert itself
  ALSO bounds the query, so the inline `INTERVAL` is belt-and-braces — you
  can drop it if your client's `query_run` rejects it.

---

## 7. Testing

Per Task #60 acceptance criteria, verification is:

- **Operator-side, manual:** apply the catalogue against a test Logfire
  project, intentionally trigger each underlying event (e.g. kill a
  delegation to fire `*_delegation_failures_hourly`, point AMC at a bad
  bearer to fire `*_amc_send_errors_15m`, stop the AMC sidecar to fire
  `*_healthz_failures_5m`), and confirm a Slack message arrives within one
  poll cycle.
- **Pre-application:** `uv run python -c "import yaml;
  yaml.safe_load(open('internal/ops/logfire-alerts.yml'))"` proves the YAML
  parses cleanly. Each `query:` is a multi-line string; YAML treats it as
  opaque text, so a typo inside a `query:` won't fail this check — it will
  fail at `alert_create` time. Run each query via `query_run` first if you
  want pre-flight validation.

There is no Capo-side unit test for this file — it never runs inside the
Capo process.

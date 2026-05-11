# S-4 AMC Webhook E2E Smoke Harness

This directory contains the reusable smoke-test harness for validating Capo's AMC
webhook listener path end-to-end. It is intentionally dependency-light (stdlib +
`httpx`) so it can run from a developer laptop, CI, or a launchd-supervised
Capo box without spinning up the full test framework.

The harness is paired with the findings document at
`internal/specs/spikes/S-4-amc-webhook-e2e.md`. The findings doc records the
*observed* AMC behavior; this harness is the *probe* that produces those
observations. The harness was scaffolded during the S-4 spike before the
Phase 1 listener (Task #10) landed — see the findings doc Section 7 for the
exact items that remain to be re-run once the listener is live.

## Layout

```
S-4-harness/
├── README.md              — this file
├── harness.py             — main runnable harness
├── signer.py              — HMAC signer used by harness + future tests
├── scenarios.py           — declarative scenario table (one row per behavior)
├── fixtures/
│   └── envelope.json      — canonical AMCInboundEnvelope sample body
└── stub_listener.py       — tiny FastAPI/Starlette stand-in for local probes
```

## What it covers

Mapped to S-4 acceptance criteria (§9.0):

| Criterion | Scenario | Pass condition |
|-----------|----------|----------------|
| HMAC verification end-to-end | `signed_envelope_accepted` | listener responds `204` within 1s |
| HMAC mismatch rejected | `tampered_signature_rejected` | listener responds `401`, never enqueues |
| Dedupe on `X-AMC-Delivery-Id` | `duplicate_delivery_id_deduped` | second POST returns `204` but is *not* enqueued |
| Dedupe window | `duplicate_after_window` | after >15 min, the same delivery-id is re-enqueued |
| Missing delivery-id | `missing_delivery_id_rejected` | listener responds `400` (or documented behavior) and does not enqueue |
| `mark_read` idempotency | `mark_read_twice_same_id` | second call returns success; `already_read` contains the id |
| Error-code surface | `error_code_probe` (six sub-cases) | each documented AMC error code reproduces with the documented HTTP shape |
| Fast-ACK SLA | timed wrapper over the above | P99 < 1s on `204` responses |

## Usage

```bash
# 1. Run the local stub listener (mimics Capo's webhook path, useful before Task #10):
python stub_listener.py --secret testsecret --port 8090

# 2. In a second shell, run the harness against the stub:
python harness.py \
    --listener http://127.0.0.1:8090/amc/webhook \
    --secret testsecret \
    --scenarios all

# 3. Or run against a real Capo build once Task #10 ships:
python harness.py \
    --listener http://127.0.0.1:8090/amc/webhook \
    --secret "$AMC_WEBHOOK_SECRET" \
    --scenarios signed_envelope_accepted,tampered_signature_rejected,duplicate_delivery_id_deduped

# 4. To probe AMC error codes against a real AMC instance (requires AMC_BEARER_TOKEN
#    and an AMC base URL):
python harness.py \
    --amc-base https://amc.local \
    --amc-token "$AMC_BEARER_TOKEN" \
    --scenarios error_code_probe
```

## Exit codes

* `0` — all selected scenarios passed
* `1` — at least one scenario failed; the report on stdout enumerates failures
* `2` — harness misconfiguration (missing secret, unreachable listener, etc.)

## Reusability

This harness is intended to be invoked by:

- **Spike S-4** (this task) — produce the findings doc
- **Phase 1 integration tests** (Task #10 and adjacent) — same scenarios run in CI
- **Phase 1 checkpoint gate** (§9.1) — manual demo verification
- **Phase 3 restart-resilience tests** (§10.2) — drive the boot-time unread sweep
  with the same canonical envelope

When integrating into the real test suite, prefer `from scenarios import SCENARIOS`
and drive `harness.run_scenario(name, listener_url, secret)` directly rather than
shelling out — the scenario table is the contract, the CLI is just a wrapper.

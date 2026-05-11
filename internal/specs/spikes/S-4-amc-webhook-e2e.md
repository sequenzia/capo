# Spike S-4: AMC Webhook End-to-End

**Status**: PARTIAL — harness landed; live-AMC items deferred to Phase 1 (Task #10).
**Spec section**: §9.0 (Spikes), feeding §5.2, §7.4, §7.5.
**Output artifact**: this document + `internal/specs/spikes/S-4-harness/` (reusable).

---

## 1. Question

> Validate HMAC verification, dedupe behavior under retry, `mark_read` idempotency,
> and error-code surfacing against a real AMC instance.

## 2. Outcome summary

| Acceptance item | Status | Where verified | Notes |
|-----------------|--------|----------------|-------|
| Smoke-test harness posts signed webhooks to a Capo stand-in | **PASS** | `S-4-harness/stub_listener.py` end-to-end run (this spike) | 5 sub-checks pass: 204 happy path, 401 tamper, 401 wrong secret, 400 missing delivery-id, 204+204 dedupe pair |
| HMAC verification end-to-end | **PASS (stub)**, **PARTIAL (real AMC)** | `signed_envelope_accepted` / `tampered_signature_rejected` scenarios | Stub matches §5.2 spec exactly. Real-instance run is deferred — see §7 below |
| Dedupe on `X-AMC-Delivery-Id` within window | **PARTIAL** | `duplicate_delivery_id_deduped` scenario | The stub honors a 15-minute LRU. The "not enqueued" half of the assertion needs the real Capo listener (Task #10) and an internal probe |
| Dedupe AFTER window | **DEFERRED** | `duplicate_after_window` scenario | Marked `--include-slow`; 15-minute real-time test belongs in Phase 1 CI gate |
| `mark_read` idempotency | **DEFERRED to live AMC** | `mark_read_twice_same_id` scenario | Spec §7.5 documents the contract (`marked_read` then `already_read`); harness ready to probe once Capo has REST auth wired (Task #10) |
| AMC error-code surface | **DEFERRED to live AMC** | `error_code_*` scenarios | Probe scaffolded; needs an AMC fault-injection mechanism (open question in §8) |
| Signature-mismatch behavior | **DEFERRED to live AMC** | n/a | Capo's stub returns `401 BAD_SIGNATURE` per spec; need to verify AMC's outbound behavior matches the documented shape |
| Missing `X-AMC-Delivery-Id` behavior | **DEFERRED to live AMC** | `missing_delivery_id_rejected` scenario | Spec does not name a code for this case; recommendation in §6 |
| Retry-window length & dead-letter timing | **DEFERRED to live AMC** | n/a | Documented values from spec recorded in §5 below; live confirmation requires AMC ops access |

**Headline**: All harness *machinery* exists and self-tests cleanly. Three buckets remain
genuinely unverified against the real AMC instance (real-AMC probes, the 15-minute
re-enqueue window, and the internal "not-enqueued" assertion behind dedupe). Those
move into Task #10 (AMC listener) and the Phase 1 checkpoint gate. The harness
is the reusable artifact that closes them.

## 3. What's in the harness

`internal/specs/spikes/S-4-harness/`:

* `signer.py` — Pure function: build an `AMCInboundEnvelope` and produce a signed
  request matching §7.4 (HMAC-SHA256 over raw body, `sha256=<hex>` header form).
  Supports happy path, single-nibble tamper, and `X-AMC-Delivery-Id` omission.
* `scenarios.py` — Declarative table of every behavior S-4 must observe (13
  scenarios in total), each tagged with whether it needs the real AMC or the
  real Capo listener. Importable by future Phase 1 test code.
* `stub_listener.py` — ~80-line stdlib-only `BaseHTTPRequestHandler` that
  implements *only* the listener-side §5.2 behaviors S-4 needs to probe:
  HMAC verify-before-parse, dedupe LRU keyed on `X-AMC-Delivery-Id` with a
  configurable TTL (default 15 min). Lets a contributor smoke the harness with
  no Capo build and no AMC.
* `harness.py` — CLI driver. Maps scenarios → HTTP calls. Per-scenario JSON or
  plain-text report. Exit code propagates pass/fail so it slots into CI.
* `fixtures/envelope.json` — Canonical envelope sample matching §7.4.

## 4. What the harness proved during this spike

Running `python3 stub_listener.py --secret testsecret --port 18090` and a driver
that posts via `urllib`:

```
happy:                204 OK
tamper:               401 OK  (body: {"error":{"code":"BAD_SIGNATURE",...}})
missing delivery-id:  400 OK  (body: {"error":{"code":"VALIDATION_ERROR",...}})
dedupe pair:          204 + 204 OK
wrong secret:         401 OK
```

This validates the *Capo listener contract* the harness will eventually be
pointed at by Task #10: the HMAC compare-digest path, the
"verify-before-parse-or-log" ordering from §5.2, and the dedupe LRU semantics.

## 5. Behavioral facts captured from the spec (canonical reference)

These are the values the future real-AMC probe must confirm or correct. They
are pulled verbatim from `capo-SPEC.md`:

| Behavior | Documented value | Spec citation |
|----------|------------------|---------------|
| Webhook handler timeout (AMC side) | 10s | §7.6 Technical Constraints |
| AMC retry count | 5 | §7.6 |
| Total retry window | ~13 minutes | §7.6 |
| Dead-letter after | 5 retries / ~13 min | §7.6 |
| Capo dedupe LRU window | ≥ 15 minutes | §5.2 acceptance criteria |
| Webhook fast-ACK SLA | P99 < 1s | §5.2 |
| Boot-time unread sweep wait cap | 60s default (`max_boot_wait_seconds`) | §5.2 edge cases, §15.3 config |
| Signature header form | `X-AMC-Signature: sha256=<lowercase-hex>` | §7.4 |
| Signature compare | `hmac.compare_digest` over raw body | §5.2 |
| Body parse ordering | After signature OK only | §5.2 ("Reject before parsing or logging the body") |
| Outbound `Authorization` | `Bearer $AMC_BEARER_TOKEN` | §7.5 |
| Outbound `X-Agent-ID` | `capo` (every outbound) | §5.2, §7.5 |
| Outbound `Idempotency-Key` | UUIDv4, required on `POST /messages/send` | §7.5 |
| `mark_read` idempotency | First call → `marked_read[]`; repeat → `already_read[]`, no 4xx | §7.5 |
| Error envelope shape | `{ "error": { "code": str, "message": str, "retry_after_seconds": int\|null } }` | §7.5 |
| Known AMC error codes | `RATE_LIMITED`, `PLATFORM_AUTH`, `CHANNEL_NOT_FOUND`, `ATTACHMENT_TOO_LARGE`, `VALIDATION_ERROR`, `INTERNAL_ERROR` | §7.5 |

The dedupe-window value (15 min) being *greater than* AMC's retry window (~13 min)
is intentional headroom — the LRU expires after the last AMC retry would have
fired, so an in-window retry is always caught.

## 6. Capo's behavior on signature mismatch and missing `X-AMC-Delivery-Id`

These are *Capo's* responses (the listener AMC posts to), not AMC's. Both items
in S-4 (Edge Cases) are about the Capo side.

### Signature mismatch

Per §5.2 + §7.4, Capo's listener MUST:

1. Read raw body bytes.
2. Compute `hmac.new(secret, body, sha256).hexdigest()`.
3. Constant-time-compare against the hex in `X-AMC-Signature` (prefix
   `sha256=`).
4. On mismatch, return `401` with body
   `{"error":{"code":"BAD_SIGNATURE","message":"..."}}`.
5. **MUST NOT** parse the body, log the body, or enqueue.

The stub implements this exactly. The harness's
`tampered_signature_rejected` scenario exercises it. Recommendation for
Task #10: emit a `capo.listener.signature_fail` counter metric (no body
content) so signature attacks are observable in Logfire without leaking
payloads.

### Missing `X-AMC-Delivery-Id`

**This is an open question — spec does not pin a code for the bare-missing case.**
Documented expectations:

* `X-AMC-Delivery-Id` is listed as **required** in §7.4 headers.
* Dedupe (§5.2) keys on it, so a missing header would either be impossible to
  dedupe or would have to be treated as always-new.

Three plausible policies:

| Option | Behavior | Pros | Cons |
|--------|----------|------|------|
| A. 400 VALIDATION_ERROR (stub default) | Reject early | Cheap; catches AMC misconfig; harness can assert | If AMC ever changes header name silently, all traffic is rejected. |
| B. 204 with a `missing-delivery-id` metric | Process but log | Defensive; never drop AMC traffic | Silently disables dedupe; one Capo restart during AMC retries → user sees N copies |
| C. 204 + synthesize a delivery-id from `(channel_id, message_id, ts)` hash | Process with synthesized key | Keeps dedupe semantics even if AMC drops the header | Most code; new failure mode if message_id is also missing |

**Recommendation for Task #10**: implement **A (400)** in code, but route it
through a *single* validator that also accounts for malformed signatures, so a
config-side feature flag can flip to **B** or **C** if AMC's observed behavior
demands it. The harness's `missing_delivery_id_rejected` scenario asserts the
*current* policy (A) and serves as the regression guard if Capo flips it.

## 7. Items deferred to Task #10 (Capo AMC listener) and live-AMC access

The S-4 acceptance criteria explicitly call for verification against a "real
AMC instance". This environment does not include a live AMC instance the
harness can target. Each of the following is *scaffolded* in the harness and
will be flipped from skip → pass during Task #10 / Phase 1 checkpoint gate:

1. **Real-AMC HMAC roundtrip.** Point harness at the deployed Capo listener
   with the actual `AMC_WEBHOOK_SECRET` and confirm a real AMC inbound passes
   verify-before-parse. Should be a no-op risk — the math is universal — but
   needs ops-side confirmation that AMC really does sign over raw bytes (and
   not a canonicalized form). **Acceptance**: a real AMC inbound, captured
   with `tcpdump` or AMC's outbound log, hashes to the same digest the
   harness produces when replayed.
2. **Dedupe re-enqueue assertion.** Add a Capo-internal test hook
   (`/internal/test/last-enqueued-id` or a counter exported on `/healthz`)
   that the harness can read to prove the second of two same-delivery-id
   POSTs was NOT enqueued. Currently the harness only proves both return 204.
3. **15-minute window probe.** Drive `duplicate_after_window` with
   `--include-slow` in the Phase 1 nightly. This is the test that confirms
   the window is *long enough* to absorb AMC's full ~13-minute retry budget
   plus a buffer.
4. **`mark_read` idempotency against live AMC.** Seed with a real
   `message_id` (via `GET /messages/unread`), call `POST /messages/mark_read`
   twice, assert the documented `marked_read[]` then `already_read[]`
   transition. **No 4xx on the second call** is the load-bearing assertion;
   the dispatcher worker retries on crash and must not surface a spurious
   error to the user.
5. **AMC error-code surface (six codes).** Requires AMC fault injection. AMC
   must expose either a test-only header (`X-Test-Force-Error: <code>`,
   assumed by the harness scaffold) or a sandbox account where each code can
   be triggered organically:
   * `RATE_LIMITED` — burst above rate limit; assert `retry_after_seconds` is
     present and Capo's `amc_client` honors it.
   * `PLATFORM_AUTH` — rotate bearer token to an invalid value; assert
     non-retryable + Logfire alert path.
   * `CHANNEL_NOT_FOUND` — send to a known-deleted channel; assert
     non-retryable + delegation orphan path.
   * `ATTACHMENT_TOO_LARGE` — attempt outbound with oversize attachment;
     assert the §5.2 fallback (drop attachment, send text only).
   * `VALIDATION_ERROR` — omit a required field; assert the body shape and
     that the code is surfaced through a typed exception.
   * `INTERNAL_ERROR` — assert AMC's 5xx behavior is treated as transient
     by `amc_client` (retry path).

Each is a row in `scenarios.py` and a unit slot in `harness.py`'s
`run_amc_rest_scenario`. The work to flip them on is purely: wire the AMC
endpoint, supply the credentials, decide on the fault-injection mechanism.

## 8. Open questions

1. **Missing `X-AMC-Delivery-Id` policy.** Spec is silent. §6 recommends 400
   for now. Final answer requires either (a) AMC ops confirming the header is
   *always* present and any miss is a bug, or (b) explicit Capo product
   decision on B vs C above. **Owner**: Capo + AMC ops, before Phase 1
   checkpoint gate.
2. **AMC fault-injection mechanism.** The harness assumes
   `X-Test-Force-Error: <code>` as a convention. AMC may use a different
   mechanism (per-account "test mode", a sandbox base URL, etc.). **Owner**:
   AMC ops, before the Phase 1 checkpoint gate. The scenario file's
   `error_code_*` entries are decoupled from the mechanism — only
   `harness.py`'s `run_amc_rest_scenario` needs updating.
3. **Internal "not-enqueued" assertion surface.** The dedupe acceptance is
   currently provable only via a Capo-internal probe. Options: a counter on
   `/healthz`, a `/internal/test/*` endpoint behind an env flag, or a Logfire
   span the test asserts on. **Recommendation**: Logfire span — it's the
   lowest-risk path and aligns with §6.5 observability requirements.

## 9. Recommendations folded back into the spec

These are sized as small edits the spec author can accept verbatim if desired:

* **§5.2 acceptance row, "Edge cases" table**: add a row for "missing
  `X-AMC-Delivery-Id`" with expected behavior `400 VALIDATION_ERROR` (per the
  policy decision in §6 of this doc).
* **§5.2 acceptance criteria**: add a bullet — "Signature-fail responses MUST
  NOT include any of the request body in their response or log line."
* **§6.5 Observability**: add `capo.listener.signature_fail` and
  `capo.listener.dedupe_hit` counter names to the canonical metric list.
* **§9.1 Phase 1 deliverables**: explicitly call out that the AMC listener
  task (Task #10 in this execution plan) integrates
  `internal/specs/spikes/S-4-harness/` as the integration-test driver, not as
  a separate to-be-built test layer.

## 10. Sign-off criteria for re-running S-4 against live AMC

Once Task #10 lands and a live AMC instance is reachable, the following one-line
invocation should be a green build:

```bash
python internal/specs/spikes/S-4-harness/harness.py \
    --listener http://127.0.0.1:8090/amc/webhook \
    --secret "$AMC_WEBHOOK_SECRET" \
    --amc-base "$AMC_BASE_URL" \
    --amc-token "$AMC_BEARER_TOKEN" \
    --scenarios all \
    --include-slow
```

Expected: `13 scenarios — 13 pass, 0 fail, 0 skip`.

At that point S-4 flips from **PARTIAL** to **PASS** and the §9.1 checkpoint
gate row "Spike S-4 (AMC webhook E2E) complete" can be checked off.

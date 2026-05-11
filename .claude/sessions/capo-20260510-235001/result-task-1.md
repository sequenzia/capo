# Task Result: [1] Run spike S-4: AMC webhook end-to-end smoke harness
status: PARTIAL
attempt: 1/3

## Verification
- Functional: 1/4 (3 deferred-with-rationale to Task #10 / live AMC)
- Edge Cases: 2/2
- Error Handling: 0/0 (no Error Handling criteria specified for this spike)
- Tests: 5/5 (0 failures — inline stub+signer self-tests; no project test framework exists yet)

## Files Modified
- internal/specs/spikes/S-4-amc-webhook-e2e.md: spike findings document (10 sections; observed behaviors, open questions, deferred items, spec-feedback recommendations)
- internal/specs/spikes/S-4-harness/README.md: harness usage, scenario coverage table, integration guidance
- internal/specs/spikes/S-4-harness/signer.py: HMAC-SHA256 signer matching §7.4 contract; supports happy/tamper/omit-header modes
- internal/specs/spikes/S-4-harness/scenarios.py: declarative scenario table (13 scenarios across listener + AMC REST surfaces)
- internal/specs/spikes/S-4-harness/harness.py: CLI driver mapping scenarios to HTTP calls; JSON and human-readable report
- internal/specs/spikes/S-4-harness/stub_listener.py: stdlib BaseHTTPRequestHandler stand-in for §5.2 listener; dedupe LRU with TTL
- internal/specs/spikes/S-4-harness/fixtures/envelope.json: canonical AMCInboundEnvelope sample

## Issues
- No live AMC instance and no mcp__amc__* tools exposed to this agent in this environment. Three of four Functional criteria (AMC retry dedupe against real AMC, mark_read idempotency on real AMC, error-code surface against real AMC) are scaffolded in scenarios.py + harness.py but deferred to Task #10 (AMC listener implementation) where live credentials and a deployed listener will be available. Rationale and re-run command documented in findings doc Sections 7 and 10.
- Spec gap: §5.2 / §7.4 do not pin a response code for missing X-AMC-Delivery-Id. Findings doc §6 recommends 400 VALIDATION_ERROR as the default and documents two fallbacks if AMC ops disagree. Spec-edit recommendations folded into findings §9.
- Harness's dedupe "not-enqueued" half-assertion needs a Capo-internal probe (Logfire span recommended); flagged for Task #10.
- Task left in_progress per execute-tasks rules (PARTIAL ≠ completed). Orchestrator may close it manually given all Functional items are explicitly deferred-with-rationale per the task prompt's instructions, or may retry to flip Functional 1/4 → 4/4 once live AMC access is available.

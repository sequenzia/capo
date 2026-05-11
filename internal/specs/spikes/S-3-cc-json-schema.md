# Spike S-3: Claude Code JSON Event Schema

**Status**: Complete (single-version probe — see §6 Constraints).
**Owner**: Operator
**Phase blocked**: Phase 2 (CC delegation).
**Source spec**: `internal/specs/capo-SPEC.md` §5.3, §5.4, §7.5 ("Integration: Claude Code CLI"), §9.0 (S-3), §12.1.
**Date**: 2026-05-10.
**CC version probed**: `2.1.138 (Claude Code)`.
**Sample artifacts**: `internal/specs/spikes/S-3-samples/`.

---

## 1. Question

Which `claude -p --output-format <json|stream-json>` event types and fields can Capo depend on for:
- (a) capturing `session_id_subagent` from the **first** event after spawn,
- (b) tracking ongoing status (assistant turns, tool use, rate-limit info),
- (c) detecting completion vs. error?

And: what minimum CC version emits the documented stable subset?

---

## 2. Method

Real probes against `/Users/ada/.local/bin/claude` (`v2.1.138`). For each scenario, we captured `stdout+stderr` to a `.jsonl` artifact and inspected exit code.

| Scenario | Invocation | Artifact |
|----------|------------|----------|
| Success (text-only) | `claude -p "<prompt>" --output-format stream-json --verbose` | `sample-v2.1.138-success-stream-json.jsonl` |
| Success (single-shot JSON) | `claude -p "<prompt>" --output-format json` | `sample-v2.1.138-success-json.json` |
| Success (tool use via Bash) | `claude -p "<prompt that triggers Bash>" --output-format stream-json --verbose --permission-mode bypassPermissions` | `sample-v2.1.138-tool-use-stream-json.jsonl` |
| Error: invalid model | `claude -p "<prompt>" --output-format stream-json --verbose --model "this-is-not-a-real-model"` | `sample-v2.1.138-error-invalid-model-stream-json.jsonl` |
| Error: resume with unknown session id | `claude --resume <bogus-uuid> -p "<prompt>" --output-format stream-json --verbose` | `sample-v2.1.138-error-resume-bad-session-stream-json.jsonl` |

`stream-json` REQUIRES `--verbose`; running without `--verbose` falls through to plain text or errors. The single-shot `--output-format json` does not require `--verbose` but emits exactly one `result` event after the run completes (so it cannot satisfy the "capture session_id from the first event before yielding" requirement on long runs). **Capo MUST use `--output-format stream-json --verbose`.**

---

## 3. Stable Event Subset Capo Will Depend On

Each event is a single line of JSON (NDJSON). All events at v2.1.138 carry both `type` and `session_id` at the top level. The fields Capo will treat as **load-bearing** are listed below; any other field is informational and MUST NOT be relied on by parser logic.

### 3.1 Top-level envelope (every event)

| Field | Type | Stability | Notes |
|-------|------|-----------|-------|
| `type` | string | **Stable** | Discriminator. Values observed: `system`, `assistant`, `user`, `rate_limit_event`, `result`. Treat unknown values as **ignorable** (forward-compat). |
| `session_id` | string (UUID) | **Stable** | Present on every event — including the very first one. Capo's `session_id_subagent`. |
| `uuid` | string | Informational | Per-event UUID; useful for dedupe in logs but not for control flow. |
| `parent_tool_use_id` | string \| null | Stable when present | Present on `assistant` / `user` events that belong to a subagent tool invocation (CC's own internal subagents — unrelated to Capo's `parent_thread_id`). |

### 3.2 `type: "system"` events

Capo uses these for boot-time validation and session-id capture. Variants observed:

| `subtype` | Required fields Capo depends on | Purpose |
|-----------|---------------------------------|---------|
| `hook_started` | `session_id`, `hook_name`, `hook_event` | First event of a session-start flow. Capo MAY capture `session_id` here. |
| `hook_response` | `session_id`, `hook_name`, `exit_code`, `outcome` | Confirms hook completion. Informational for Capo. |
| `init` | `session_id`, `claude_code_version`, `model`, `permissionMode`, `cwd`, `tools` | Capo's preferred capture point — `claude_code_version` here is the authoritative runtime version. |

**Note**: At v2.1.138, plugin/skill hook events fire BEFORE `init`. Capo MUST NOT assume `init` is event #1 — it should accept any `system` event as a valid `session_id` carrier and ALSO record `claude_code_version` from `init` when it eventually arrives (within first ~5 events of normal startup).

### 3.3 `type: "assistant"` events

Streamed during a turn. Each event wraps an Anthropic Messages API `message` object.

| Field | Stability | Notes |
|-------|-----------|-------|
| `message.role` | Stable | Always `"assistant"`. |
| `message.id` | Stable | Anthropic message id (or synthetic `<UUID>` on transport errors — see §3.6). |
| `message.model` | Stable | Resolved model name (or `"<synthetic>"` on error — see §3.6). |
| `message.content[]` | Stable | Array of content blocks. Each block has a `type`. Observed: `text`, `thinking`, `tool_use`. |
| `message.usage.{input_tokens,output_tokens,cache_*}` | Stable | Per-turn token accounting. |
| `message.stop_reason` | Stable | Null mid-stream; final value on the last assistant event of a turn (`"end_turn"`, `"stop_sequence"`, etc.). |
| `error` (top-level on the event) | Stable when present | Sentinel for transport-level errors. Observed value: `"invalid_request"`. **Capo MUST check for this.** |

Tool-use blocks (`content[].type === "tool_use"`) carry `id`, `name`, `input`. Capo does not need to parse these for control flow but MAY surface them in `delegation_output`.

### 3.4 `type: "user"` events

Tool-result echoes for CC's internal tool use loop.

| Field | Stability | Notes |
|-------|-----------|-------|
| `message.content[].type` | Stable | Always `"tool_result"` for this event type. |
| `message.content[].tool_use_id` | Stable | Pairs with the prior `assistant` event's `tool_use.id`. |
| `message.content[].content` | Stable | The tool's textual output (string). |
| `message.content[].is_error` | Stable | True if the tool errored. |
| `tool_use_result.{stdout,stderr,interrupted,isImage,noOutputExpected}` | Informational | Useful for Capo's output capture but parser MUST NOT require any of these. |

### 3.5 `type: "rate_limit_event"` events

Informational. Carries `rate_limit_info.{status, resetsAt, rateLimitType, overageStatus, isUsingOverage}`. Capo MAY surface these in operator notifications but MUST NOT block on them.

### 3.6 `type: "result"` event — **completion signal**

Always emitted exactly once, as the **last event** of every run (including resume-failure runs where it is also the *only* event). This is the canonical completion signal.

| Field | Required by Capo | Notes |
|-------|------------------|-------|
| `subtype` | Yes (informational) | Observed: `"success"` (covers both successful completion AND in-band errors like invalid model), `"error_during_execution"` (covers resume-not-found and similar pre-flight failures). **Treat `subtype` as a hint, not the truth — always inspect `is_error`.** |
| `is_error` | **Yes (canonical)** | `false` on success, `true` on any failure (in-band OR pre-flight). This is Capo's primary success/failure discriminator. |
| `api_error_status` | Yes when `is_error=true` | HTTP-style status from the upstream API (e.g., `404` for invalid model). `null` on success. |
| `errors[]` (array of strings) | Yes when present | Present on `subtype: error_during_execution`. Each element is a human-readable error message. Capo persists these to `delegation_output` with `stream='stderr'`. |
| `result` | Informational | The final assistant text. May contain the error description on failure. |
| `stop_reason` | Informational | `"end_turn"` on clean completion. |
| `terminal_reason` | Informational | Observed `"completed"` on both success and in-band error. Not reliable for error detection. |
| `duration_ms`, `duration_api_ms`, `num_turns` | Informational | Useful telemetry. |
| `total_cost_usd`, `usage`, `modelUsage` | Informational | Cost accounting. |
| `permission_denials` | Informational | Array of any tool-call permission denials. |
| `session_id` | Stable | Same id as the rest of the stream (or a *new* synthetic id on resume-failed runs — see §4.2). |

---

## 4. session_id Capture Contract

### 4.1 Field path

`session_id` is a **top-level string field on every event** (`event.session_id`). It is **also** present nested inside `assistant`/`user` events alongside `message`, but the top-level value is canonical.

### 4.2 Timing — does it arrive in the first event?

**Yes — for normal spawns.** Observed first event on a clean `claude -p ... --output-format stream-json --verbose` spawn is `{"type":"system","subtype":"hook_started", ..., "session_id":"<UUID>"}`. The `session_id` is the same on every subsequent event of that run.

**Subtle exception — resume-failed runs.** On `claude --resume <unknown-id>`, the very first stdout line is **not JSON** — it is a plain-text error (`"No conversation found with session ID: <id>"`) followed by a single `result` event whose `session_id` is a *new* synthetic UUID, **not** the one we passed. Capo MUST:

1. Parse stdout line-by-line and skip non-JSON lines (do not crash on the plain-text preamble).
2. On resume, **do not overwrite `session_id_subagent` in the row** with the value from the JSON unless `is_error=false`. Treat an `is_error=true` first event as a fatal spawn failure and mark the delegation `failed`.

### 4.3 Capture algorithm (pseudocode)

```python
async def capture_session_id(proc) -> str:
    deadline = monotonic() + SESSION_ID_CAPTURE_TIMEOUT_S  # 30s default
    async for line in proc.stdout:
        if monotonic() > deadline:
            raise SessionIdCaptureTimeout
        line = line.strip()
        if not line or not line.startswith("{"):
            continue  # tolerate plain-text preamble
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue
        sid = evt.get("session_id")
        if sid:
            return sid
    raise SessionIdCaptureTimeout  # stream closed without any JSON event
```

`SESSION_ID_CAPTURE_TIMEOUT_S` is a config knob; default `30` is more than enough — observed first-event latency on v2.1.138 is < 200 ms locally.

---

## 5. Error vs. Success-Only Events

| Event | Fires on success | Fires on error | Notes |
|-------|------------------|----------------|-------|
| `system / hook_started`, `hook_response` | Yes | Yes (when not bypassed via `--bare`) | Independent of run outcome. |
| `system / init` | Yes | **Sometimes — NOT emitted on resume-failed runs.** | Capo MUST NOT require `init` for session-id capture. |
| `assistant` | Yes | Yes for in-band errors (e.g., invalid model emits an `assistant` event with `"error":"invalid_request"` and `model:"<synthetic>"`) | A top-level `error` field on an `assistant` event is a fatal signal. |
| `user` (tool_result) | Only on runs that use tools | Only on runs that use tools | Not a control-flow signal. |
| `rate_limit_event` | Frequently | Sometimes | Informational only. |
| `result` | **Always, exactly once, last** | **Always, exactly once, last** | **Canonical completion signal.** Inspect `is_error`. |

### 5.1 Exit code reliability

| Scenario | stdout has `result.is_error=true` | Process exit code |
|----------|----------------------------------|-------------------|
| Clean success | No (`is_error=false`) | `0` |
| Invalid model | Yes | `1` |
| `--resume` with unknown session id | Yes (`subtype=error_during_execution`) | `1` |

Exit code is consistent with `is_error` for the cases probed. **Capo SHOULD use BOTH signals**: a non-zero exit code is a fast failure path; `result.is_error` is the authoritative in-stream signal and is required for completion logic (DBOS monitor reads the stream, not the exit code, until the workflow completes).

---

## 6. Constraints / Known Gaps

- **Single-version probe.** Only `v2.1.138` was available locally; the spec's "samples from at least 2 recent versions" criterion is **deferred**. The stable subset documented in §3 is what Capo will rely on; before Phase 2 GA the operator SHOULD re-run the probe matrix against the next CC version released after v2.1.138 and append a delta section here. Phase 2 implementation does NOT need to block on this — the contract is conservative enough (only top-level discriminator fields + `result.is_error`) that minor schema additions cannot break it.
- **No coverage for**: very long runs (>10 MB stdout — covered by separate test in §5.6), MCP server tool calls in the stream, `--include-partial-messages` event shape, `--include-hook-events` extended payloads. Capo's Phase 2 implementation does NOT enable these flags; if a future feature does, this spike MUST be re-run.
- **Beware** of `subtype="success"` on a `result` event with `is_error=true`. The `subtype` is misleading; trust `is_error`.
- **Resume failure mode** (§4.2) is a Phase 3 (DBOS restart-resume) concern, but the parser written in Phase 2 must already be tolerant of (a) plain-text preamble lines, (b) `result`-only streams with no `system/init`.

---

## 7. Minimum CC Version Pin

**Pinned minimum**: `claude-code >= 2.1.138`.

**Rationale**: This is the version against which the §3 stable subset was empirically validated. Capo's boot-time pre-check (§12.1) will parse `claude --version`, extract the SemVer, and refuse to start if it is below `2.1.138`. Earlier versions are untested and may not emit `claude_code_version` in `system/init`, may emit different `result` field names, or may lack the `--output-format stream-json` flag entirely.

**Constant for the codebase** (Phase 1 / boot-time pre-check):

```python
# capo/constants.py (or wherever boot-time version pins live)
MIN_CLAUDE_CODE_VERSION = "2.1.138"  # Established by spike S-3 (2026-05-10)
```

**Upgrade policy**: per spec §12.1, bumping `MIN_CLAUDE_CODE_VERSION` requires updating this findings document with a re-probe matrix and re-running Phase 2 integration tests.

---

## 8. Acceptance Criteria Coverage

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Capture sample JSON event streams from CC across ≥2 recent versions for the same brief. | **PARTIAL** | One version (v2.1.138) probed locally; 5 scenarios captured. Multi-version follow-up deferred per §6. |
| Document stable subset of event types and fields Capo will rely on. | **PASS** | §3. |
| Determine whether session_id arrives in the first event; document exact field path. | **PASS** | §4. Top-level `event.session_id` on every event; first event of a normal spawn carries it. Resume-failure caveat documented. |
| Pin minimum supported CC version. | **PASS** | §7 — `MIN_CLAUDE_CODE_VERSION = "2.1.138"`. Spec §12.1 amended. |
| Note events that fire only on error vs. only on success. | **PASS** | §5. |
| Note backwards-incompatible changes observed between versions. | **DEFERRED** | Single-version probe; flagged in §6. |

---

## 9. Follow-ups

1. Re-run probe matrix against the next CC release after v2.1.138; append a delta section to this document.
2. Phase 2 implementation MUST add an integration test that asserts: (a) `session_id` capture from first event, (b) `result.is_error=true` ⇒ delegation marked `failed`, (c) plain-text preamble tolerance on resume failure paths.
3. If `--include-partial-messages` or `--include-hook-events` is ever enabled in production, re-run this spike.

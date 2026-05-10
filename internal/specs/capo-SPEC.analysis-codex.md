# Spec Analysis Report: Capo PRD

**Analyzed**: 2026-05-10
**Spec Path**: internal/specs/capo-SPEC.md
**Detected Depth Level**: Full-Tech
**Overall Score**: 95 - Excellent
**Status**: Updated after review

---

## Summary Dashboard

| Dimension | Score | Critical | Warning | Suggestion |
|-----------|-------|----------|---------|------------|
| Requirements | 96 | 0 | 0 | 0 |
| Risk & Feasibility | 94 | 0 | 0 | 0 |
| Quality | 96 | 0 | 0 | 0 |
| Completeness | 94 | 0 | 0 | 0 |
| **Overall** | **95** | **0** | **0** | **0** |

### Overall Assessment

Capo is a strong Full-Tech spec: it has complete section coverage, measurable success metrics, detailed feature acceptance criteria, clear phasing, explicit risks, and unusually concrete operational requirements. The review resolved the remaining contract-level gaps around AMC outbound calls, Codex parity, multi-user partitioning, restore behavior, dependency versions, heartbeat persistence, and provisional Claude Code session capture.

---

## Completeness Scorecard

| Section | Status | Score | Notes |
|---------|--------|-------|-------|
| Executive Summary | Present | 100% | Clear product purpose, architecture primitives, and ownership constraint. |
| Problem Statement | Present | 100% | Problem, current state, impact, and value are well articulated. |
| Goals & Success Metrics | Present | 100% | Six measurable metrics with targets, measurement methods, and phases. |
| User Personas | Present | 90% | Primary persona is complete; future small-group persona is intentionally lighter but adequate for V1. |
| User Stories | Present | 100% | 13 numbered stories cover the functional surface. |
| Acceptance Criteria | Present | 95% | Criteria are mostly testable; accepted fixes clarified P0 sequencing and storage details. |
| Non-Functional Requirements | Present | 90% | Strong performance, security, reliability, and observability coverage; scalability is intentionally V1-limited. |
| System Architecture | Present | 95% | Component diagram, tech stack, and paired DB restore boundary are clear. |
| API Specifications | Present | 95% | Inbound Capo endpoints and AMC outbound REST contracts are now specified. |
| Data Models | Present | 95% | SQL is detailed and now aligns with multi-user history keys plus heartbeat persistence. |
| Performance SLAs | Present | 100% | Specific P50/P99, output, contention, and storage-growth targets. |
| Testing Strategy | Present | 95% | Unit/integration/smoke/performance plan is clear; concurrency default and stress coverage are now separated. |
| Deployment Plan | Present | 90% | launchd rollout, rollback, and paired DB restore are now explicit. |
| Dependencies | Present | 90% | Dependencies include version-policy and boot-time CLI validation expectations. |
| Risks & Mitigations | Present | 90% | Major technical risks are named with mitigations and spike gates. |
| Open Questions | Present | 90% | Open questions are concrete and mostly have defaults; Codex remains a task-decomposition blocker until S-1. |

**Thresholds**: Min Sections: met (15+) | Min Features: met (13) | Min User Stories: met (13) | Min Metrics: met (6)

---

## Findings

### Critical

No critical findings.

---

### Warnings

#### FIND-001: AMC Outbound REST Contracts Are Incomplete

- **Dimension**: Risk & Feasibility
- **Category**: RISK-02
- **Location**: Section 7.5 "Integration: AMC" (lines 1040-1103)
- **Issue**: Capo depends on `GET /messages/unread`, `POST /messages/send`, and `POST /messages/mark_read`, but the spec only shows sequence diagrams and a retry summary. It does not define request bodies, response bodies, required headers, idempotency behavior for `mark_read`, or the typed error-code response schema used by `amc_client`.
- **Impact**: The AMC client, retry logic, and integration tests cannot be implemented or mocked confidently from this spec alone. This also weakens the Phase 1 checkpoint for error-code surfacing and idempotent unread/mark-read handling.
- **Recommendation**: Add an "AMC REST Contract" subsection under 7.5 covering `/messages/unread`, `/messages/send`, and `/messages/mark_read` with method, auth, headers, request schema, success response, error response, idempotency rules, and retryability per error code.
- **Status**: Resolved

#### FIND-002: Codex Parity Is P0 But Its CLI Contract Is Deferred

- **Dimension**: Risk & Feasibility
- **Category**: RISK-02
- **Location**: Section 5.4 "delegate_to_codex" (lines 293-300), Section 7.5 "Integration: Codex CLI" (line 1126), Section 14 "Open Questions" (line 1486)
- **Issue**: `delegate_to_codex` is marked P0 and promises full lifecycle parity, restart resume, output parsing, and status/output/kill compatibility. The actual Codex spawn invocation, output/event contract, and resume mechanism are explicitly unresolved until spike S-1.
- **Impact**: Phase 4 tasks cannot be fully generated until the spike result amends the spec. If task generation starts before S-1, implementers must guess at the core contract for a P0 feature.
- **Recommendation**: Keep Phase 4 blocked on S-1, and add a clear "Do not create Phase 4 Codex implementation tasks until §5.4 and §7.5 are amended" note. Alternatively, demote Codex parity from P0 until the resume contract is proven.
- **Status**: Resolved

#### FIND-003: Multi-User Partitioning Is Not Reflected In Keys And Indexes

- **Dimension**: Quality
- **Category**: INC-04
- **Location**: Section 5.11 "Multi-User Support" (lines 469-476), Section 7.3 "Data Models" (lines 699-807)
- **Issue**: The spec requires every domain table to be partitioned by `user_id`, but `conversation_history` uses `PRIMARY KEY (thread_id, message_index)` and `sessions` indexes active sessions by `thread_id` only. The ERD also marks `conversation_history.user_id` as an FK rather than part of the key. If `thread_id` is not globally unique across users, this conflicts with the multi-user isolation requirement.
- **Impact**: Future small-group support could hit key collisions or accidental cross-user reads. Even in V1, it leaves implementers unsure whether isolation is guaranteed by schema keys or by an implicit assumption that AMC channel IDs are globally unique.
- **Recommendation**: Either make the guarantee explicit that `thread_id` is globally unique across all users, or update the schema to include `user_id` in conversation/session uniqueness, such as `PRIMARY KEY (user_id, thread_id, message_index)` and an active-session uniqueness/index scoped by `(user_id, thread_id)`.
- **Status**: Resolved

#### FIND-004: Delegation `session_id` Persistence Timing Is Ambiguous

- **Dimension**: Requirements
- **Category**: AMB-02
- **Location**: Section 5.3 "delegate_to_claude_code" (lines 264-267), Section 5.6 "Restart contract" (line 339)
- **Issue**: The acceptance criterion says to insert a `delegations` row with status `running` and `session_id` before the tool returns, while also saying the `session_id` is captured asynchronously and updated when the first JSON event arrives. The restart contract later depends on `session_id` for `claude --resume`.
- **Impact**: It is unclear whether the tool may return before `session_id_subagent` is known, whether the DBOS workflow may start with a null session ID, and how a crash between spawn and first JSON event should be handled.
- **Recommendation**: Split the requirement into explicit states: insert the row with nullable `session_id_subagent`, start output capture, update the row when the first event arrives, and only mark the delegation restart-resumable or hand it to DBOS after the session ID is durable. Add an edge case for crash before session ID capture.
- **Status**: Resolved

#### FIND-005: Concurrent Delegation Targets Conflict

- **Dimension**: Quality
- **Category**: INC-04
- **Location**: Section 6.1 "Performance Requirements" (line 546), Section 10.3 "Performance Test Plan" (lines 1389-1390), Section 14 "Open Questions" (line 1490)
- **Issue**: The performance requirement says concurrent delegations run up to `concurrency.max_delegations` with default 3, while the performance test plan requires 5 simultaneous synthetic CC delegations. The open question then asks whether the cap should be 3 vs 5 and defaults to 3.
- **Impact**: The pass/fail threshold for load testing is unclear. A system configured to the documented default of 3 could fail the test plan's 5-delegation case despite meeting the stated NFR.
- **Recommendation**: Align the target. Either set the default to 5, change the performance test to "configured max, with a default test of 3", or keep the 5-run test but label it as a stress test above the supported default.
- **Status**: Resolved

#### FIND-006: Litestream Restore Scope Conflicts With DBOS Workflow State

- **Dimension**: Risk & Feasibility
- **Category**: RISK-04
- **Location**: Section 5.6 "DBOS state" (line 343), Section 7.5 "Integration Points" (line 1037), Section 11.1 "Rollback" (line 1399), Section 13 "Risks & Mitigations" (line 1479)
- **Issue**: The architecture replicates `state.db` with Litestream, while DBOS workflow state lives separately in `dbos.db`. The risk table says restore inconsistency is mitigated by restoring both together, but the dependencies, Litestream deliverable, and restore exercise only name `state.db`.
- **Impact**: A state-only restore can produce app rows for delegations whose DBOS workflow state is missing or out of sync, directly affecting the restart-resilience goal and completion notification guarantees.
- **Recommendation**: Specify backup/restore for `dbos.db` alongside `state.db`, or explicitly define state-only restore behavior: detect orphaned running delegations, mark them failed/recoverable, and notify the user. Update the Phase 5 Litestream restore exercise to include the DBOS state file or the orphan recovery path.
- **Status**: Resolved

#### FIND-007: Dependency Versions And Minimums Are Not Specified

- **Dimension**: Risk & Feasibility
- **Category**: MISS-04
- **Location**: Section 7.2 "Tech Stack" (lines 668-680), Section 12.1 "Technical Dependencies" (lines 1436-1458)
- **Issue**: The spec depends on APIs and CLIs whose contracts matter to persistence and parsing, including Pydantic AI `ModelMessage`, DBOS workflow behavior, Claude Code JSON events, Codex resume, and Litestream restore behavior. The dependency table lists owner/status but no minimum versions, pinning strategy, or boot-time version validation requirements.
- **Impact**: Implementers can pass the spec with locally available versions that later break history serialization, event parsing, or resume semantics. This undermines the spike outputs that are supposed to pin stable contracts.
- **Recommendation**: Add minimum versions or a version policy for Pydantic AI, DBOS, Claude Code CLI, Codex CLI, and Litestream. Require boot-time version checks for subagent CLIs once S-1/S-3 establish supported contract versions.
- **Status**: Resolved

---

### Suggestions

#### FIND-008: Heartbeat Threshold Freezing Has No Storage Field

- **Dimension**: Requirements
- **Category**: REQ-01
- **Location**: Section 5.13 "Live Progress Reporting" (lines 530-535), Section 7.3 "delegations" schema (lines 722-846)
- **Issue**: The heartbeat requirement says the threshold list must be captured from config at delegation start and frozen on the `delegations` row, but the `delegations` schema has no field for frozen heartbeat intervals or sent heartbeat state.
- **Impact**: Without a storage field or child table, config edits can affect in-flight idempotency keys, and the workflow has no obvious durable record of which milestone notifications were already sent.
- **Recommendation**: Add `heartbeat_intervals_json` to `delegations` and either a `heartbeats_sent_json` field or a normalized `delegation_heartbeats` table keyed by `(delegation_id, threshold_seconds)` with `sent_at` and `idempotency_key`.
- **Status**: Resolved

#### FIND-009: Claude Code Spawn Uses A Placeholder Capture Mechanism

- **Dimension**: Risk & Feasibility
- **Category**: AMB-04
- **Location**: Section 7.5 "Integration: Claude Code CLI" (lines 1105-1123), Section 9.0 "S-3" (line 1196)
- **Issue**: The Claude Code spawn example includes `--session-id-output <some_capture_mechanism>` while the event schema is deferred to S-3. This is acceptable as a spike item, but the placeholder sits in the normative integration section without being labeled as provisional.
- **Impact**: A task generator may treat the placeholder as an actual CLI contract, while the implementation should wait for S-3's event parser spec and minimum version pin.
- **Recommendation**: Mark the spawn invocation as provisional until S-3 completes, or move the placeholder into the spike description and leave §7.5 with only the confirmed current command shape.
- **Status**: Resolved

---

## Resolution Summary

**Review Session**: 2026-05-10
**Resolution Mode**: Interactive Review

| Metric | Count |
|--------|-------|
| Total Findings | 9 |
| Resolved | 9 |
| Skipped | 0 |
| Remaining | 0 |

### Score Change

| Dimension | Before | After | Change |
|-----------|--------|-------|--------|
| Requirements | 88 | 96 | +8 |
| Risk & Feasibility | 80 | 94 | +14 |
| Quality | 84 | 96 | +12 |
| Completeness | 88 | 94 | +6 |
| **Overall** | **85** | **95** | **+10** |

### Resolved Findings

- **FIND-001**: Added an AMC REST contract for unread, send, mark-read, shared error shape, idempotency, and retry behavior.
- **FIND-002**: Added explicit Phase 4 blocker language for Codex implementation tasks until S-1 amends the spec.
- **FIND-003**: Scoped conversation history and active-session uniqueness by `user_id`.
- **FIND-004**: Clarified Claude Code session-ID capture timing and crash-before-session-ID recovery behavior.
- **FIND-005**: Split default 3-delegation concurrency from 5-delegation stress coverage.
- **FIND-006**: Updated Litestream, rollback, runbook, and restore exercise language for paired `state.db` + `dbos.db` backup/restore.
- **FIND-007**: Added dependency version policy and boot-time CLI version validation expectations.
- **FIND-008**: Added heartbeat interval storage and a `delegation_heartbeats` table.
- **FIND-009**: Marked Claude Code session capture in §7.5 as provisional until S-3 confirms the contract.

### Skipped Findings

No skipped findings.

### Recommendations for Future

Before generating implementation tasks for Phase 4, complete spike S-1 and amend the Codex sections with the concrete CLI contract. Before Phase 3 durability work, complete S-2 and confirm DBOS + SQLite behavior under the configured concurrency target.

---

## Analysis Methodology

This analysis was performed using depth-aware criteria for **Full-Tech** specs:

- **Dimensions Analyzed**: Requirements, Risk & Feasibility, Quality, Completeness
- **Sections Checked**: Executive Summary, Problem Statement, Goals & Metrics, User Research, Functional Requirements, Non-Functional Requirements, Technical Architecture, API Specifications, Data Models, Integration Points, Scope, Implementation Plan, Testing Strategy, Deployment & Operations, Dependencies, Risks, Open Questions, Appendix
- **Criteria Applied**: analysis-dimensions.md checklists, common-findings.md pattern library
- **Template Validated Against**: sdd-specs full-tech template
- **Out of Scope**: External validation of current third-party API/CLI behavior; this review evaluates the spec's internal completeness and implementability, not whether the named tools currently support the described contracts.

# Spec Analysis Report: Capo PRD

**Analyzed**: 2026-05-10 12:00
**Spec Path**: /Users/ada/dev/repos/capo/internal/specs/capo-SPEC.md
**Detected Depth Level**: Full-Tech
**Status**: Resolved (all 23 findings applied to spec on 2026-05-10)

---

## Summary

| Category | Critical | Warning | Suggestion | Total |
|----------|----------|---------|------------|-------|
| Inconsistencies | 2 | 4 | 1 | 7 |
| Missing Information | 0 | 5 | 2 | 7 |
| Ambiguities | 0 | 3 | 2 | 5 |
| Structure Issues | 0 | 1 | 3 | 4 |
| **Total** | **2** | **13** | **8** | **23** |

### Overall Assessment

This is a high-quality, mature Full-Tech spec — comprehensive coverage, normative span taxonomy, explicit edge cases, and well-aligned spike strategy. The two critical findings are localized: an ERD/SQL primary-key mismatch and a stated-goal vs. success-metric contradiction (500 vs 800 LOC). Most remaining issues are clarity gaps around mechanisms referenced but not fully specified (the cost-cap "override" reply, the `BEGIN IMMEDIATE` retry helper, the `[users.<key>]` TOML syntax, the queue-depth max).

---

## Findings

### Critical

#### FIND-001: ERD vs SQL primary-key mismatch on `daily_costs`

- **Category**: Inconsistencies
- **Location**: Section 7.3 "Data Models" — ERD lines 750-755 vs SQL lines 838-844
- **Issue**: The Mermaid ERD declares `daily_costs` with `TEXT date PK` and `TEXT model PK` only, while the canonical SQL DDL declares `PRIMARY KEY (date, model, user_id)`. The ERD also marks `user_id` only as `FK` rather than part of the composite PK. The two representations of the same table disagree on the primary key shape.
- **Impact**: A code generator or developer reading the ERD will believe `(date, model)` is unique per user, but the SQL allows the same `(date, model)` to repeat across users. Building queries or migrations from the ERD will produce subtly broken behavior (collisions in single-user mode, or missing rows in multi-user mode).
- **Recommendation**: Update the ERD `DAILY_COSTS` block to mark `user_id` as part of the composite primary key (e.g. `TEXT user_id PK,FK`), matching the SQL `PRIMARY KEY (date, model, user_id)`.
- **Status**: Pending

#### FIND-002: Goal 5 conflicts with the success metric for LOC

- **Category**: Inconsistencies
- **Location**: Section 3.1 "Primary Goals" Goal 5 (line 54) vs Section 3.2 "Success Metrics" final row (line 65)
- **Issue**: Goal 5 states the target is `~500 lines of Python + a handful of markdown files`. The success metric table sets the target as `< ~800 LOC at Phase 5 close`. The two numbers (500 vs 800) measure the same thing but disagree by 60%.
- **Impact**: Implementation reviewers won't know whether to enforce the 500-line ceiling (which Goal 5 makes a primary goal) or the 800-line ceiling (which the measurable success metric uses). The "Readable" goal becomes unverifiable.
- **Recommendation**: Pick one target and align both statements. The size context (`Description` says ~78KB spec describing a complex orchestrator) makes 800 the more realistic ceiling; recommend updating Goal 5 to `~800 lines of Python` so it matches the measurable metric.
- **Status**: Pending

---

### Warnings

#### FIND-003: Cost-cap "override" reply mechanism is undefined

- **Category**: Missing Information
- **Location**: Section 5.9 "Cost Caps + Model Routing" line 430; cross-references §5.10 (lines 448-453) and §11.4 line 1382
- **Issue**: §5.9 says the hard-cap response is `"budget exceeded — reply 'override' to continue today"` and that `Override unlocks the rest of the day`, but no specification exists for how the override reply is parsed, scoped, persisted, or expired. §5.10's slash-command list does not include `/override`, and the natural-language fallback path is undefined for this case. The runbook (§11.4 line 1382) also references `override via 'override' reply` without further detail.
- **Impact**: The hard-cap path is unimplementable as written — case sensitivity, exact match vs substring, whether it must be in the same thread, what table records the override, and how the cap resets all need to be decided. This is a P1 acceptance criterion blocking Phase 5.
- **Recommendation**: Add an acceptance criterion to §5.9 specifying: (a) the trigger phrase grammar (e.g., case-insensitive exact match on `override`), (b) where the override state lives (suggest a `budget_overrides(date, user_id)` row), (c) the override scope (current `user_id` only), (d) expiration (local midnight, same as the cap), and (e) where parsing happens (pre-agent in dispatcher, like slash commands).
- **Status**: Pending

#### FIND-004: `BEGIN IMMEDIATE` retry helper is referenced but never defined

- **Category**: Missing Information
- **Location**: Section 5.6 line 339, §7.6 "Technical Constraints" line 1090, §9.1 "Phase 1" line 1168
- **Issue**: The spec references a `BEGIN IMMEDIATE` retry helper in three places (5.6 idempotency clause, 7.6 mitigation column for SQLite WAL, 9.1 deliverable "SQLite hardening"), but no acceptance criteria or signature define its retry budget, backoff strategy, or exception-translation behavior. §6.1 promises "Zero SQLITE_BUSY returned to user; retries internally" — that requirement is unverifiable without the helper's spec.
- **Impact**: Phase 1 deliverable "SQLite hardening" cannot be checkpoint-gated without knowing what "retries internally" actually means (how many tries, what backoff, what eventual error surface).
- **Recommendation**: Add a small subsection to §7.3 or §9.1 specifying the retry helper contract: e.g., "On `sqlite3.OperationalError` matching `database is locked`, retry up to N times with exponential backoff capped at `busy_timeout` (5s) total wall time, then re-raise as `StoreUnavailable`." Reference the helper consistently from §5.6, §6.1, §7.6.
- **Status**: Pending

#### FIND-005: `[users.user_id_default]` TOML key syntax does not match §5.11 resolution behavior

- **Category**: Inconsistencies
- **Location**: Section 15.3 "Configuration Reference" line 1522 vs §5.11 lines 472-476
- **Issue**: §5.11 says `config.toml [users]` table maps AMC sender IDs to internal `user_id` values, with one mapping in V1. The canonical TOML in §15.3 shows `[users.user_id_default]` as the section header — but it's unclear whether `user_id_default` is meant to be the literal `user_id` string for that row (in which case the value will be `"user_id_default"` in every domain table) or a placeholder meaning "the default user_id slot". Neither §5.11 nor §15.3 specifies which.
- **Impact**: A developer wiring up the dispatcher's sender-resolution code will not know whether to use the TOML sub-table key as the `user_id` directly, or to look up some inner field. The fact that the sub-table contains `display_name = "Owner"` (not `user_id = "owner"`) suggests the key IS the user_id, but this is unstated.
- **Recommendation**: Either (a) rename the example to a realistic value like `[users.owner]` and add a sentence in §5.11 or §15.3 stating "The TOML key after `users.` is used as the `user_id` value in all domain tables", or (b) restructure the TOML to make the user_id an explicit field: `[[users]]\nuser_id = "owner"\namc_senders = [...]`.
- **Status**: Pending

#### FIND-006: Spike findings path `specs/spikes/` does not match the spec's actual location

- **Category**: Inconsistencies
- **Location**: Section 9.0 "Spikes" line 1153
- **Issue**: The spec says spike findings are committed under `specs/spikes/`, but this spec itself lives at `internal/specs/capo-SPEC.md`. There is no top-level `specs/` directory in the repo (per the recent refactor commit `5f4fd5a refactor(spec): move capo spec to internal directory`). A spike author following this instruction will create the wrong directory.
- **Impact**: Spike findings will be misplaced and broken cross-references from this spec (§5.4 mentions S-1, §5.6 implicitly references S-2, §7.3/§7.5 reference S-3 and S-4) will not resolve.
- **Recommendation**: Change `specs/spikes/` to `internal/specs/spikes/` on line 1153.
- **Status**: Pending

#### FIND-007: Health-check `dispatcher` subsystem references a non-existent "configured max" for queue depth

- **Category**: Missing Information
- **Location**: Section 5.12 "Health Check Endpoint" line 511
- **Issue**: The acceptance criterion says `dispatcher — Workers alive, queue depth < configured max`, but the canonical config in §15.3 only contains `concurrency.max_delegations` — there is no `concurrency.queue_depth_max` (or equivalent) defined anywhere. `max_delegations` controls concurrent subagent runs, not the per-channel dispatcher queue depth.
- **Impact**: The health check cannot be implemented as written. A default ceiling and a config key need to exist before §5.12 can be checkpoint-gated in Phase 5.
- **Recommendation**: Add a `queue_depth_max` (or similarly named) field to `[concurrency]` in §15.3 with a sensible default (e.g., 100 per worker), and reference it explicitly from §5.12. Alternatively, change the criterion to "queue depth < `concurrency.max_delegations × 10`" or another formula derived from existing config.
- **Status**: Pending

#### FIND-008: `max_boot_wait` is referenced in §5.2 and §7.5 but absent from canonical config

- **Category**: Missing Information
- **Location**: Section 5.2 "Edge Cases" table line 234, §7.5 "AMC error handling" line 1057, vs §15.3 canonical config (no entry)
- **Issue**: Both §5.2 and §7.5 cite `max_boot_wait` (default 60s) as a configurable boot timeout for the unread sweep, but `[amc]` in §15.3 has no `max_boot_wait` field. The spec promises it is configurable without exposing the configuration surface.
- **Impact**: Boot-time unread sweep behavior cannot be tuned without editing source; checkpoint gate for Phase 1 references this behavior indirectly. The Pydantic Settings validator referenced in §6.2 won't know the field.
- **Recommendation**: Add `max_boot_wait_seconds = 60` to the `[amc]` block in §15.3 (line ~1483-1487) and clarify the field name consistently in §5.2 and §7.5.
- **Status**: Pending

#### FIND-009: ERD `APPROVALS }o--|| DELEGATIONS : "gates (optional)"` has no SQL counterpart

- **Category**: Inconsistencies
- **Location**: Section 7.3 ERD line 692 vs SQL `CREATE TABLE approvals` lines 824-835
- **Issue**: The Mermaid ERD declares an optional gating relationship between `APPROVALS` and `DELEGATIONS`, but the `approvals` SQL table has no `delegation_id` column or foreign key — only an opaque `action_payload_json` text blob and `action_kind`. The declared relationship is therefore not implementable from the schema.
- **Impact**: A reader using the ERD to plan queries (e.g., "show all approvals for delegation X") will find no direct path; they'd have to JSON-extract from `action_payload_json`. The ERD overstates the schema's expressivity.
- **Recommendation**: Either (a) add an explicit `delegation_id TEXT` column to `approvals` (nullable, since not all approvals gate a delegation) with a referenced FK, and add an index for the lookup; or (b) remove the relationship line from the ERD and note in §5.8 that the linkage exists only through `action_payload_json`.
- **Status**: Pending

#### FIND-010: Codex integration §7.5 is materially incomplete for a Full-Tech spec

- **Category**: Missing Information
- **Location**: Section 7.5 "Integration: Codex CLI" line 1079-1081
- **Issue**: The entire Codex integration section reads: `Identical lifecycle; specifics finalized after spike S-1 (session resume mechanism)`. A Full-Tech spec normally provides at least the spawn invocation, event/stream parsing assumptions, and resume contract — even if some are conditional on spike findings. §5.4 has the same caveat. For a Phase-4 feature this is acceptable as a known gap, but the spec should make explicit that §5.4 and §7.5 will be amended as a contract before Phase 4 commits.
- **Impact**: Phase 4 cannot be planned in detail; risk owner (operator) has no formal trigger to update the spec. A reader unfamiliar with the spike model may treat this as a complete spec.
- **Recommendation**: Add a single sentence to §7.5 (and confirm in §5.4): "Spec amendment is a Phase 4 entry criterion. §5.4 and §7.5 must be updated with the resolved Codex spawn invocation, event-stream contract, and resume mechanism (or workaround) before Phase 4 deliverables begin." Already partially present in §5.4 line 297 — make symmetrical in §7.5.
- **Status**: Pending

#### FIND-011: Webhook request body schema in §7.4 does not include attachments, but §5.2 error handling does

- **Category**: Missing Information
- **Location**: Section 7.4 "POST /amc/webhook" request body lines 935-945 vs §5.2 error table line 245
- **Issue**: §5.2 lists `AMC ATTACHMENT_TOO_LARGE` as a handled error with response "Drop attachment, send text only" — implying the inbound envelope can carry attachments. The Pydantic body schema in §7.4 enumerates only `id`, `channel_id`, `sender_id`, `text`, `ts`, `approval_id` and notes `extra="allow"`. The `AMCInboundEnvelope` class diagram (§7.3 lines 906-914) matches the §7.4 list.
- **Impact**: Implementers will not know whether attachments arrive on the inbound webhook (and if so under what field), or whether `ATTACHMENT_TOO_LARGE` is purely an outbound `send` error. The error table in §5.2 conflates inbound and outbound errors.
- **Recommendation**: Either (a) add `attachments: list[Attachment] | None` to the inbound envelope schema in both §7.4 and §7.3, defining `Attachment` with `kind, url, size_bytes`; or (b) clarify in §5.2's error table that `ATTACHMENT_TOO_LARGE` is an outbound-send error only, and move it out of the inbound-context error column or annotate the column name accordingly.
- **Status**: Pending

#### FIND-012: Alembic for `dbos.db` is mentioned but its scope is undefined and possibly redundant

- **Category**: Ambiguities
- **Location**: Section 7.2 line 670 ("supports both SQLite files via two configs"), §9.1 line 1169 (Phase 1 Alembic init only for `state.db`), §9.3 line 1229 ("Alembic config for dbos.db")
- **Issue**: DBOS owns its workflow-state schema internally — its tables are created and migrated by DBOS itself. Running Alembic against `dbos.db` would either fight DBOS's bookkeeping or be a no-op. The spec asserts Alembic supports both SQLite files but doesn't say what app-owned tables would live in `dbos.db` that need Alembic management.
- **Impact**: Phase 3 deliverable "Alembic config for dbos.db" is undefined work; it may be unnecessary or actively harmful. Wasted phase-3 hours, or implementer adds Alembic that conflicts with DBOS.
- **Recommendation**: Decide: (a) if `dbos.db` is entirely DBOS-managed, remove the Alembic config deliverable from §9.3 and update §7.2 to "Alembic for `state.db` only — DBOS manages `dbos.db` schema"; or (b) if there are app-owned tables planned in `dbos.db` (e.g., DBOS-co-located idempotency keys), list them in §7.3 and keep the Alembic deliverable with a defined initial migration.
- **Status**: Pending

#### FIND-013: Cost-cap accuracy promise (`within ±$1`) lacks a defined reconciliation procedure

- **Category**: Ambiguities
- **Location**: Section 6.4 "Reliability Requirements" table line 577, §5.9 line 427
- **Issue**: §6.4 promises "Daily totals within ±$1 of Logfire-reported actual", but §5.9 says the accountant "reads Logfire's per-run cost (or a custom per-call accumulator)" — the OR is the problem. If the accumulator is the source of truth, there is no separate Logfire signal to be "within ±$1 of". §13 risk row (line 1415) calls out "drift > 10%" but doesn't say how drift is computed. No reconciliation cadence, query, or alert is specified.
- **Impact**: Phase 5 checkpoint includes "synthetic cost accumulation crosses soft cap → model swap verified" but the accuracy SLA cannot be measured without a reconciliation step. The §13 risk mitigation ("Reconcile with Logfire daily") is unimplementable as written.
- **Recommendation**: Add an acceptance criterion to §5.9 specifying (a) the canonical cost source (suggest Logfire) and (b) a daily reconciliation step that emits a `capo.budget.reconcile` span with `accumulator_usd`, `logfire_usd`, `drift_usd`. Tie §6.4's ±$1 requirement to this span.
- **Status**: Pending

#### FIND-014: Main agent model override mechanism is referenced but undefined

- **Category**: Missing Information
- **Location**: Section 5.1 line 193, §5.9 line 425
- **Issue**: §5.1 says "configurable per-run override is supported" for the default model, and §5.9 says "Capo's main agent uses `config.models.default` on every run unless explicitly overridden by the user." Neither describes the override surface — there is no `/model` slash command in §5.10, no override field on inbound envelopes in §7.4, and no agent tool for model selection in §7.1's tool list.
- **Impact**: The acceptance criterion is unimplementable as stated. Either drop the claim or expose the mechanism.
- **Recommendation**: Either (a) add a `/model <name>` slash command to §5.10 with scope (single turn vs session) and the same allowlist as `config.models`, or (b) tighten the §5.1 and §5.9 language to "configurable via `config.toml` edit + restart" and remove the per-run-override claim from V1.
- **Status**: Pending

#### FIND-015: §5.5 phase column has redundant wording

- **Category**: Structure Issues
- **Location**: Section 5.5 "Feature: Tools — Status / Output / Kill / List" line 305
- **Issue**: The phase declaration reads `**Phase**: 2 (status/output/kill), 2 (list)`. Both halves resolve to Phase 2, so the qualifier is redundant and confuses the reader into looking for two different phases.
- **Impact**: Minor reading friction; could mislead a planner scanning for Phase 1 vs Phase 2 placement.
- **Recommendation**: Simplify to `**Phase**: 2`. If the intent was to call out that `list_delegations` is the lowest-priority of the four, do so in prose under acceptance criteria rather than as a misleading phase split.
- **Status**: Pending

---

### Suggestions

#### FIND-016: Glossary should include `Litestream`, `WAL`, `HMAC`, `Alembic`, `Pydantic AI`

- **Category**: Missing Information
- **Location**: Section 15.1 "Glossary" lines 1438-1447
- **Issue**: The glossary defines product-specific terms (AMC, DBOS, SOUL, Delegation, Session, Thread, Worktree, Approval workflow) but skips several tech-stack terms used heavily throughout the spec: `Litestream`, `WAL` (used in §6.1, §7.6, §9.1), `HMAC`, `Alembic`, `Pydantic AI`, `Logfire`. For a Full-Tech spec, even well-known terms benefit from one-line glossary entries that pin the role each plays in *this* system.
- **Impact**: Onboarding readers — particularly future small-group members per the persona in §4.1 — will hit unfamiliar acronyms (`WAL`, `HMAC`) without a single-stop reference.
- **Recommendation**: Add ~5 glossary entries to §15.1 covering the missing terms, each ≤ 1 sentence describing its role in Capo specifically.
- **Status**: Pending

#### FIND-017: `mark_read` is not declared idempotent in §5.2 even though `send` is

- **Category**: Missing Information
- **Location**: Section 5.2 line 225, §7.5 "Integration: AMC" lines 1055-1057
- **Issue**: §5.2 says the worker calls `amc.send` then `amc.mark_read`. §7.5 explicitly states every outbound `send` includes a UUIDv4 `Idempotency-Key`. No corresponding idempotency contract is stated for `mark_read`, but Capo's restart-resume scenario (§5.6) and the dispatcher worker crash path (§5.2 edge case) can both result in `mark_read` being attempted twice for the same `message_id`.
- **Impact**: Possible mark-read drift after worker crashes; minor because AMC likely tolerates redundant mark-read, but the contract should be explicit (Phase 1 spike S-4 line 1151 already calls out `mark_read idempotency` as in scope, but it's not reflected as an acceptance criterion).
- **Recommendation**: Add a one-line acceptance criterion under §5.2: "`amc.mark_read` calls are idempotent on `message_id`; safe to retry."
- **Status**: Pending

#### FIND-018: Span name templating for dynamic tools (`capo.tool.<name>`) doesn't specify how `<name>` is normalized

- **Category**: Ambiguities
- **Location**: Section 6.5 "Logfire Span Taxonomy" line 589
- **Issue**: The span name template `capo.tool.<name>` is fine for fixed tools (`web_search`, `fetch_url`, `shell_exec`), but the same taxonomy must cover dynamically-registered NL-fallback tools from §5.10 (`session_new`, `session_status`, `session_clear`). The taxonomy doesn't specify whether `<name>` is the Python function name, the agent-facing tool name, or a slugified label.
- **Impact**: Logfire dashboards built against this taxonomy will be inconsistent if implementations choose different conventions across tool families.
- **Recommendation**: Add a one-line clarification under §6.5 table: "`<name>` is the agent-facing tool name as registered with Pydantic AI (snake_case), e.g., `capo.tool.delegate_to_claude_code`, `capo.tool.session_new`."
- **Status**: Pending

#### FIND-019: Heartbeat milestone identity is loose

- **Category**: Ambiguities
- **Location**: Section 5.13 line 530 (idempotency from `(delegation_id, milestone)`); §15.3 line 1520 (`intervals_seconds = [900, 3600, 14400]`)
- **Issue**: "Milestone" identity is implicit — is it the interval value in seconds (900, 3600, 14400), an ordinal (first, second, third), or the elapsed time at send? If the config is updated mid-run, milestones may shift and idempotency keys may collide or drift.
- **Impact**: Heartbeats could double-send across restart if the milestone key isn't canonicalized.
- **Recommendation**: Specify `milestone = str(threshold_seconds)`, derived from the configured intervals at delegation start time and frozen on the delegation row, so config edits don't break idempotency for in-flight runs.
- **Status**: Pending

#### FIND-020: §5.6 idempotency clause has a hard-to-parse parenthetical

- **Category**: Ambiguities
- **Location**: Section 5.6 line 339
- **Issue**: The clause "DB inserts other than within `BEGIN IMMEDIATE` retry helper" is a double-negative-style exception that takes two reads to parse. The intent appears to be "DB inserts (except those already wrapped by the retry helper) need idempotency keys."
- **Impact**: Readability only — but this is a critical correctness contract.
- **Recommendation**: Rephrase to: "All `@DBOS.step` calls with external side effects — subprocess spawn, `amc.send`, and any DB insert not already wrapped by the `BEGIN IMMEDIATE` retry helper — generate and persist an idempotency key in the workflow's step state so retries are safe."
- **Status**: Pending

#### FIND-021: Session boundaries are unbounded if user never sends `/new`

- **Category**: Structure Issues
- **Location**: Section 5.7 line 368
- **Issue**: §5.7 says "A session ends when the user explicitly starts a new one (slash command `/new` or NL equivalent)." A session that never receives `/new` will accumulate indefinitely — possibly across years. Compaction (line 369) handles tokens, but the unbounded `sessions` lifetime means `compacted_to_message_index` and conversation tail growth have no natural ceiling.
- **Impact**: Edge-case behavior on long-lived sessions is undefined: does compaction repeat? What happens to the original `started_at` field for analytics? Minor for V1 single-user, more relevant for forward-compat multi-user.
- **Recommendation**: Add a note: "Sessions have no implicit timeout in V1; users can `/new` at any time. Future iterations may add an idle-timeout-based session rotation."
- **Status**: Pending

#### FIND-022: §11.3 alert table mentions an MCP tool name, which is an implementation detail in a spec

- **Category**: Structure Issues
- **Location**: Section 11.3 "Monitoring & Alerting" line 1365
- **Issue**: The line reads "Logfire alerts (configured via `mcp__plugin_logfire_logfire__alert_create`)". The MCP tool identifier is a transport detail of the current operator's tooling, not a spec contract. If the operator's MCP setup changes, the spec becomes false without any functional change.
- **Impact**: Cosmetic; locks the spec to a specific MCP namespace.
- **Recommendation**: Drop the parenthetical or replace with "configured against the Logfire alerts API".
- **Status**: Pending

#### FIND-023: Successful "completion notification arrives exactly once" success metric lacks an explicit idempotency anchor

- **Category**: Structure Issues
- **Location**: Section 10.2 "Critical Path: Restart-Resilient Long Delegation" step 6 line 1335
- **Issue**: Step 6 reads "Workflow runs `summarize_run` + `notify_user`; AMC receives completion message exactly once." §5.6 acceptance criteria already promise idempotency keys on side-effecting steps, but the test step does not specify *how* "exactly once" is asserted (e.g., assert AMC `/messages/send` mock invocation count == 1 with a specific idempotency key).
- **Impact**: Test step is descriptive rather than prescriptive; a future implementer could write a test that misses the double-send case.
- **Recommendation**: Tighten step 6 to: "AMC mock records exactly one `/messages/send` call for the completion notification (verify via the `Idempotency-Key` header and call count)."
- **Status**: Pending

---

## Resolution Summary

**Reviewed**: 2026-05-10 via interactive HTML review.
**Outcome**: All 23 findings approved and applied to `internal/specs/capo-SPEC.md`.

| | Critical | Warning | Suggestion | Total |
|---|---|---|---|---|
| Approved | 2 | 13 | 8 | **23** |
| Rejected | 0 | 0 | 0 | 0 |
| Pending | 0 | 0 | 0 | 0 |

### Notes on Option Choices

Four findings offered alternative resolutions; the simpler/safer option was selected for each:

- **FIND-009** (Approvals↔Delegations relationship): **Option A** — removed the ERD relationship line and added a clarifying sentence in §5.8 noting linkage is via `action_payload_json` only (no FK).
- **FIND-011** (Inbound attachments): **Option B** — annotated the §5.2 error table to clarify `ATTACHMENT_TOO_LARGE` is an outbound `send` error; V1 does not handle inbound attachments.
- **FIND-012** (Alembic for `dbos.db`): **Option A** — removed the Alembic-for-`dbos.db` deliverable from Phase 3 and tightened §7.2 to state Alembic manages `state.db` only.
- **FIND-014** (Main-agent model override): **Option B** — tightened §5.1 and §5.9 to state the default model is changed via `config.toml` edit + restart; the per-run override claim was removed from V1.

### Applied Changes (all 23)

1. FIND-001 — daily_costs ERD PK now `(date, model, user_id)`
2. FIND-002 — readability goal updated to "~800 lines of Python"
3. FIND-003 — hard-cap `override` reply mechanism specified (dispatcher pre-parse + `budget_overrides` table)
4. FIND-004 — added "Retry Helper Contract" subsection to §7.3 defining backoff and `StoreUnavailable`
5. FIND-005 — TOML key normalized to `[users.owner]` with explanatory comment
6. FIND-006 — spike findings path updated to `internal/specs/spikes/`
7. FIND-007 — health-check now references `concurrency.queue_depth_max`
8. FIND-008 — `[amc]` config now includes `max_boot_wait_seconds = 60`
9. FIND-009 — ERD relationship removed; §5.8 documents `action_payload_json`-only linkage
10. FIND-010 — Codex §7.5 now states spec-amendment is a Phase 4 entry criterion
11. FIND-011 — §5.2 error table clarifies `ATTACHMENT_TOO_LARGE` is outbound
12. FIND-012 — Alembic for `dbos.db` removed; §7.2 and Phase 3 deliverable updated
13. FIND-013 — §5.9 gained a reconciliation acceptance criterion with `capo.budget.reconcile` span
14. FIND-014 — §5.1 and §5.9 tightened to "config edit + restart"; per-run override removed
15. FIND-015 — §5.5 phase wording simplified to "Phase: 2"
16. FIND-016 — glossary expanded with Litestream, WAL, HMAC, Alembic, Pydantic AI, Logfire
17. FIND-017 — `amc.mark_read` declared idempotent on `message_id`
18. FIND-018 — `capo.tool.<name>` taxonomy clarified to use snake_case agent-facing names
19. FIND-019 — heartbeat idempotency keys now use frozen threshold list per delegation
20. FIND-020 — §5.6 idempotency clause rewritten without confusing parenthetical
21. FIND-021 — §5.7 session boundary explicitly notes no implicit timeout in V1
22. FIND-022 — §11.3 references "the Logfire alerts API" instead of MCP tool name
23. FIND-023 — §10.2 step 6 now asserts exactly-one `POST /messages/send` via Idempotency-Key/call count

---

## Analysis Methodology

This analysis was performed using depth-aware criteria for Full-Tech specs:

- **Sections Checked**: All sections §1–§15 including Mermaid diagrams, SQL DDL, Pydantic class diagrams, API specifications, span taxonomy, configuration TOML, and the 5-phase implementation plan with spikes.
- **Criteria Applied**: Full Technical Documentation checklist (§Architecture, §APIs, §Data Models, §Performance SLAs, §Testing, §Deployment) plus cross-depth consistency (feature naming, priority/phase alignment, goal-metric mapping) and clarity scans (vague quantifiers, ambiguous pronouns, undefined terms).
- **Out of Scope**: Cross-document verification against `internal/blueprints/capo-blueprint.md` (treated as standalone per analysis directive). AMC repo at `/Users/ada/prod/amc` treated as external. SOUL/prompt content correctness not analyzed.

# Financial Truth Engine — Clean Start Next Steps

**Status:** Execution checklist  
**Created:** 2026-06-17  
**Updated:** 2026-06-23
**Purpose:** Start the Financial Truth Engine cleanly while preserving the current EOB project as a reference only.

---

## Guiding Decision

Proceed as a clean, separate initiative.

Do not refactor the existing EOB project into this architecture. The existing EOB project may remain as a reference, but the Financial Truth Engine should start with a clean database, clean schema, clean fixtures, and clean assumptions.

The central design principle remains:

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The PDF is evidence, not truth. AI produces observations. Deterministic reconciliation creates financial truth.

---

## Codex Task Log

### Task 001 — Ledger Schema and Fixtures ✅ Merged

**Delivered:**
- `migrations/001_create_financial_truth_schema.sql` — clean PL/pgSQL schema defining
  all 11 tables (`fte_practices`, `fte_evidence`, `fte_observations`, `fte_claims`,
  `fte_claim_events`, `fte_event_evidence`, `fte_financial_positions`,
  `fte_denial_knowledge`, `fte_contract_terms`, `fte_review_queue`,
  `fte_analysis_runs`), with `practice_id` on all tenant-scoped tables, RLS enabled,
  evidence immutability enforced, and indexes for claim/payer/evidence lookup.
- `fixtures/synthetic_ccdbe216_failure_modes.sql` — synthetic fixture for the
  ccdbe216 practice (`c0000000-0000-4000-8000-0000000000fe`), covering 5 failure
  modes: phantom duplicate, section-delimiter double-count, null-check crossbleed,
  summary-row exclusion, and late-retry/page contradiction.
- `fixtures/synthetic_96c5c357_failure_modes.sql` — synthetic fixture for the
  96c5c357 practice (`96000000-0000-4000-8000-0000000000fe`), covering check-spacing
  fragmentation variants and a short-pay scenario (CLM-APC-1000).
- `tests/validate_schema.sql` — schema validation passed in disposable Supabase
  project; all tables, constraints, and RLS policies confirmed present.
- `fixtures/README.md` — fixture documentation.
- `migrations/README.md` — schema/migration documentation.

**Safety:** all fixtures are synthetic; no PHI, no real patient data, no production
exports, no legacy EOB tables or connection strings.

---

### Task 002 — Deterministic Reconciler Prototype ✅ Merged (PR #3)

**Delivered:**
- `reconciler/fte_reconcile.sql` — 9-phase PL/pgSQL stored procedure
  `fte_reconcile_practice(p_practice_id uuid) RETURNS jsonb`. Classifies
  observations (trusted / suspect / excluded), emits claim events exclusively
  from trusted observations, derives financial positions with schema-valid
  reconciliation statuses, routes anomalies to `fte_review_queue`, and records
  an append-only `fte_analysis_runs` entry. Idempotent: Phase 0 deletes all
  derived rows and re-derives from scratch on every call.
- `tests/validate_reconciler.sql` — 12-check validation suite (wrapped in
  ROLLBACK; nothing persists). All 12 checks passed in disposable Supabase
  project. Four additional spot checks also passed:
  - ccdbe216 payment event is `ambiguous` (Phase 5 late/retry wires this)
  - ccdbe216 financial position is `in_review` with `open_balance_amount = 0.00`
    (math balances, but position is `in_review` because any ambiguous event
    blocks `balanced` — `'ambiguous'` is not a valid position status per schema)
  - 96c5c357 CLM-APC-1000 has `short_pay_detected` with `amount = 1248.11`
  - 96c5c357 CLM-APC-2000 has zero events and an `in_review` position with
    all-NULL monetary fields
- `reconciler/README.md` — phase-by-phase documentation of the 9-phase
  procedure, observation classification rules, and extension guide.
- `CODEX_TASK_002.md` — task spec corrected in 4 places where `'ambiguous'`
  was used as a `fte_financial_positions.reconciliation_status` value
  (schema CHECK forbids it; `'in_review'` is the correct surrogate).

**Safety:** tested exclusively in a disposable Supabase project using synthetic
fixtures; no PHI, no real practice IDs, no production Supabase project accessed,
no legacy EOB code modified.

---

### Task 003 — Review Queue Resolution Spec ✅ Design Spec Only

**Delivered:**
- `CODEX_TASK_003.md` — design specification for append-only review resolutions.
  Defined the `fte_review_resolutions` table concept, 15-action vocabulary grouped
  into three categories (claim-level, payment-event-level, observation-level),
  Phase 0.5 reconciler sub-phase design, snapshot vs. hard FK strategy, and the
  three non-destructive review patterns (confirm, reject/suppress, correct/supersede).

**Implementation:** deferred to Task 004 (approved and implemented separately).

**Safety:** specification only; no schema changes, no fixtures, no data.

---

### Task 004A/B — Append-Only Review Resolutions ✅ Merged

**Delivered:**
- `migrations/002_add_review_resolutions.sql` — adds `fte_review_resolutions` table
  with 15-action CHECK constraint, snapshot FK strategy for Phase-0-volatile IDs
  (`source_review_queue_id`, `source_claim_event_id`, `source_position_id` are plain
  uuid snapshot columns with no `REFERENCES` clause), and hard FKs to stable entity
  tables only (`fte_practices`, `fte_claims`, `fte_observations`, `fte_evidence`).
  Append-only: Phase 0 never deletes it.
- `reconciler/fte_reconcile.sql` (updated) — Phase 0.5 loads all non-superseded
  `fte_review_resolutions` rows into `_fte_active_resolutions` temp table before
  classification begins. `confirm_payment_event` resolution: Phase 5 checks
  `_fte_active_resolutions` and promotes ambiguous payment events to `'reconciled'`
  status, causing Phase 6 to derive `'balanced'` positions instead of `'in_review'`.
  Return JSON reports `review_resolutions_applied` count.
- `tests/validate_review_resolution.sql` — 7-check validation suite (wrapped in
  ROLLBACK). Verifies baseline ambiguous state, `confirm_payment_event` promotion to
  reconciled/balanced, idempotency, and audit trail preservation. All 7 checks passed
  in disposable Supabase project.

**Safety:** tested exclusively in a disposable Supabase project using synthetic
fixtures; no PHI, no real practice IDs, no production Supabase project accessed.

---

### Task 004C — Observation-Level Review Resolutions ✅ Merged (PR #10)

**Delivered:**
- `migrations/003_add_observation_resolution_target.sql` — adds
  `target_observation_id uuid references fte_observations(id) on delete restrict`
  column plus 5 CHECK constraints enforcing valid shape for the three
  observation-level actions: `confirm_observation`, `reject_observation`,
  `mark_duplicate`. Includes partial index for reverse lookup ("which observations are
  marked duplicate of canonical X?").
- `reconciler/fte_reconcile.sql` (updated) — Phase 0.5 now also loads
  `_fte_suppressed_observations` (obs IDs where `action IN ('reject_observation',
  'mark_duplicate')`). Phase 1 excludes suppressed observations via NOT EXISTS
  (NULL-safe; NOT IN fails on NULLs). `confirm_observation` has queue-only
  effect: suppresses the `fte_review_queue` entry in Phase 2 without altering
  Phase 1 classification or any ledger events/positions.
- `tests/validate_observation_resolution.sql` — 12-check validation suite (wrapped
  in ROLLBACK). Verifies: baseline queue=6, confirm b4 queue→5, reject b3 queue→4,
  mark_duplicate b1→a1 queue→3, idempotency, and rejection of trusted obs a3 removes
  `contractual_adjustment_applied` event and recalculates `open_balance_amount` to
  $209.60 while b5 ambiguity keeps position `in_review`. All 12 checks passed in
  disposable Supabase project.
- `tests/README.md` (updated) — documents the four validation suites.
- `reconciler/README.md` (updated) — documents Phase 0.5 additions.

**Safety:** tested exclusively in a disposable Supabase project using synthetic
fixtures; no PHI, no real practice IDs, no production Supabase project accessed,
no legacy EOB code modified. PR #10 merged to main (HEAD `afef369`).

---

### Task 004D — Corrected-Value Resolutions ✅ Merged (PR #13)

**Delivered:**
- `migrations/004_corrected_value_constraints.sql` — 4 CHECK constraints
  enforcing valid shape for `attach_corrected_value` (`observation_id IS NOT NULL`,
  `target_type = 'observation'`, `corrected_value IS NOT NULL`,
  `corrected_value >= 0`) and unique partial index
  `idx_fte_resolutions_single_active_correction` on
  `(practice_id, observation_id, action) WHERE is_superseded = false AND action = 'attach_corrected_value'`.
  Enforces at most one active correction per observation at the DB level.
- `reconciler/fte_reconcile.sql` (Phase 5c updated) — correlated subquery in the
  `FOR v_obs` SELECT list looks up any active `attach_corrected_value` resolution
  from `_fte_active_resolutions` for each trusted payment observation.
  `COALESCE(v_obs.corrected_amount, v_obs.amount)` uses the correction when present
  and falls back to the extracted amount otherwise — no behaviour change when no
  resolution exists.
- `tests/validate_corrected_value.sql` — 11-check validation suite (wrapped in
  ROLLBACK). Verifies: baseline payment=$351.89/open_balance=$1,248.11/queue=5,
  correction applied payment=$1,600.00/balanced/queue=4, idempotency, isolation
  (CLM-APC-2000 unaffected), and unique partial index rejects second active
  correction. All 11 checks passed in disposable Supabase project.

**Safety:** tested exclusively in a disposable Supabase project using synthetic
fixtures (`synthetic_96c5c357_failure_modes.sql`); no PHI, no real practice IDs,
no production Supabase project accessed, no legacy EOB code modified.

---

### Task 004E — Corrected-Value Supersession Validation ✅ Complete

**Delivered:**
- `tests/validate_corrected_value_supersession.sql` — 10-check validation suite
  (wrapped in ROLLBACK) that proves the corrected-value supersession workflow:
  INSERT first correction → reconcile (balanced) → UPDATE is_superseded=true →
  INSERT second correction → reconcile (unbalanced, open_balance=$100.00) →
  audit trail (2 rows: 1 superseded + 1 active at $1,500.00) → index still
  enforces (third active correction raises unique_violation). No migration,
  reconciler, fixture, or README changes required — migration 004's unique
  partial index and Phase 0.5's `WHERE is_superseded = false` filter make
  supersession transparent to the reconciler.

**Safety:** no new schema objects, no reconciler changes, no fixture changes;
validation-only, runs under ROLLBACK in a disposable Supabase project.

---

### Task 004F — Validation Runbook and Ergonomics ✅ Complete

**Delivered:**
- `tests/RUNBOOK.md` — authoritative run-order guide for the validation suite.
  Covers first-time setup (local psql and Supabase SQL Editor sequences), the
  repeatable validation-run command, the suite→fixture dependency table, and a
  troubleshooting section addressing the most common failure modes (stale registered
  reconciler, `\i` metacommand errors in the SQL Editor, duplicate-object errors on
  re-migration, missing NOTICE output in collapsed Messages panel).

**Safety:** documentation-only; no schema, reconciler, fixture, or test changes.

---

### Task 004H — Corrected Billed Amount Support ✅ Complete

**Delivered:**
- `reconciler/fte_reconcile.sql` (Phase 3 updated) — correlated subquery in the
  `FOR v_obs` SELECT list looks up any active `attach_corrected_value` resolution
  from `_fte_active_resolutions` for each trusted billed_amount observation.
  `COALESCE(v_obs.corrected_billed_amount, v_obs.amount)` uses the correction when
  present and falls back to the extracted amount otherwise. Mirrors the Phase 4
  contractual-adjustment and Phase 5c payment-correction pattern exactly. No migration
  required — migration 004's unique partial index already covers any observation type.
- `tests/validate_corrected_billed_amount.sql` — 10-check validation suite (wrapped
  in ROLLBACK) using existing obs a1 (billed_amount $1,600.00) as the correction target.
  Verifies: baseline billed=$1,600.00/payment=$351.89/open=$1,248.11/unbalanced/resolutions=0,
  corrected billed=$1,500.00/payment unchanged/open=$1,148.11/unbalanced/resolutions=1,
  resolution row survives Phase 0, idempotency across a third run, and unique partial
  index rejects a second active correction.

**Safety:** no new schema objects, no new action vocabulary, no fixture file changes;
no migration; Phase 4 contractual-adjustment and Phase 5c payment-correction paths
unchanged; ROLLBACK-wrapped validation in a disposable Supabase project only; no PHI,
no production data, no legacy EOB project accessed.

---

### Task 004G — Corrected Contractual Adjustment Support ✅ Complete

**Delivered:**
- `reconciler/fte_reconcile.sql` (Phase 4 updated) — correlated subquery in the
  `FOR v_obs` SELECT list looks up any active `attach_corrected_value` resolution
  from `_fte_active_resolutions` for each trusted contractual_adjustment observation.
  `COALESCE(v_obs.corrected_adj_amount, v_obs.amount)` uses the correction when present
  and falls back to the extracted amount otherwise. Mirrors the Phase 5c payment-
  correction pattern exactly. No migration required — migration 004's unique partial
  index (`idx_fte_resolutions_single_active_correction`) already covers any observation
  type; no `observation_type` restriction exists in the CHECK constraints.
- `tests/validate_corrected_contractual_adjustment.sql` — 10-check validation suite
  (wrapped in ROLLBACK) using synthetic obs a3 inserted inside the transaction. Verifies:
  baseline adj=$900.00/payment=$351.89/open_balance=$348.11/unbalanced/resolutions=0,
  corrected adj=$800.00/payment unchanged/open_balance=$448.11/unbalanced/resolutions=1,
  resolution row survives Phase 0, idempotency across a third run, and unique partial
  index rejects a second active correction.

**Safety:** no new schema objects, no new action vocabulary, no fixture file changes;
no migration; Phase 5c payment-correction path unchanged; ROLLBACK-wrapped validation
in a disposable Supabase project only; no PHI, no production data, no legacy EOB
project accessed.

---

## Current Capabilities

As of Task 004H complete (2026-06-23), the FTE can:

- **Represent the full claim ledger.** Eleven tables covering practices, evidence,
  observations, claims, claim events, event-evidence audit links, financial positions,
  denial knowledge, contract terms, review queue, and analysis runs. All tenant-scoped
  tables include `practice_id` and RLS.
- **Run a deterministic 9-phase reconciler.** `fte_reconcile_practice(uuid)` is fully
  idempotent: Phase 0 deletes all derived rows, Phase 0.5 loads reviewer decisions,
  Phase 1 classifies observations (trusted / excluded / suspect), Phases 2–8 emit
  claim events, route to review queue, derive financial positions, detect short pays,
  and record an append-only `fte_analysis_runs` entry.
- **Route uncertainty explicitly.** Low-confidence, conflicting, missing-link,
  unbalanced, late-retry, duplicate, and summary-row cases go to `fte_review_queue`
  instead of silently corrupting financial truth.
- **Apply reviewer decisions across reruns.** `fte_review_resolutions` rows survive
  Phase 0 and are loaded in Phase 0.5, so reconciler reruns honor past reviewer
  decisions without manual re-entry.
- **Resolve ambiguous payment events.** `confirm_payment_event` promotes a
  `payment_applied` event from `'ambiguous'` to `'reconciled'`, causing the financial
  position to re-derive as `'balanced'` on the next reconciler run.
- **Suppress invalid or duplicate observations.** `reject_observation` and
  `mark_duplicate` remove an observation from Phase 1 entirely — no events, no queue
  entry — causing financial positions to recalculate without it. `mark_duplicate`
  records the canonical observation via `target_observation_id` FK (migration 003).
- **Confirm correctly-flagged observations.** `confirm_observation` suppresses the
  `fte_review_queue` entry for a correctly-classified suspect/excluded observation
  without promoting it to trusted or altering any ledger events.
- **Replace a corrected value safely.** The supersession workflow (UPDATE old row
  `SET is_superseded = true`, then INSERT a new row) is deterministic: Phase 0.5
  loads only the new active correction; the unique partial index prevents a second
  active row; superseded rows are retained for audit. Proven across 10 validation
  checks (Task 004E).
- **Correct a contractual adjustment amount.** An `attach_corrected_value` resolution
  on a `contractual_adjustment` observation overrides the extracted adjustment amount
  in Phase 4 — same action vocabulary, same correlated-subquery pattern as payment
  corrections, no migration. Phase 6 open-balance math (`GREATEST(0, billed − adj − paid)`)
  picks up the corrected value automatically. Proven across 10 validation checks (Task 004G).
- **Correct a billed amount.** An `attach_corrected_value` resolution on a
  `billed_amount` observation overrides the extracted charge amount in Phase 3 —
  same action vocabulary, same correlated-subquery pattern, no migration. Phase 6
  open-balance math picks up the corrected value automatically. Proven across 10
  validation checks (Task 004H). All three claim-level amounts (billed, contractual
  adjustment, payment) can now be independently corrected by a reviewer.

**Not yet implemented:** extraction layer (AI observations from real PDFs), UI,
API endpoints, Edge Functions, denial/contract intelligence.

---

## Current Validation Suites

All suites run in a disposable Supabase project under `ROLLBACK` (nothing persists).
Apply migrations and register the reconciler before running.

| File | Checks | Covers |
|---|---|---|
| `tests/validate_schema.sql` | structure checks | 11 tables, constraints, RLS policies, indexes |
| `tests/validate_reconciler.sql` | 12 | 9-phase reconciler, event classification, short-pay detection |
| `tests/validate_review_resolution.sql` | 7 | `confirm_payment_event` promotion to balanced/reconciled |
| `tests/validate_observation_resolution.sql` | 12 | confirm/reject/mark_duplicate, Phase 1 suppression, ledger recalculation |
| `tests/validate_corrected_value.sql` | 11 | `attach_corrected_value` — correction applied, balanced, idempotency, isolation, index |
| `tests/validate_corrected_value_supersession.sql` | 10 | corrected-value supersession — replace active correction, audit trail, index enforcement |
| `tests/validate_corrected_contractual_adjustment.sql` | 10 | `attach_corrected_value` on contractual_adjustment obs — Phase 4 corrected amount, payment unchanged, index enforcement |
| `tests/validate_corrected_billed_amount.sql` | 10 | `attach_corrected_value` on billed_amount obs — Phase 3 corrected amount, payment unchanged, index enforcement |

**Total numeric checks: 72** (structure checks in validate_schema.sql not counted)

For the Supabase SQL Editor (which does not support `\i`): load each fixture file
manually before running the test body. The `tests/RUNBOOK.md` documents the run order.

---

## Phase 0 — Archive and Freeze the Legacy EOB Work

Goal: preserve what was learned without carrying forward polluted assumptions or questionable extracted data.

### Actions

- [ ] Tag the current repository state as `eob-legacy-final`.
- [ ] Export current Supabase schema and migrations.
- [ ] Export a small curated set of difficult EOB source documents as test fixtures.
- [ ] Document the known failure modes from the current EOB project.
- [x] Freeze feature development on the old document-first architecture.
- [x] Do not migrate old extracted rows into the new Financial Truth Engine.

### Keep From The Old Project

- Known difficult PDFs / ERAs
- Lessons learned from extraction failures
- Useful CARC/RARC classification logic
- Useful auth/RLS patterns
- Useful UI concepts
- Practice/tenant concepts

### Do Not Carry Forward As Truth

- Old extracted line items
- Old payment rows
- Old reconciliation patches
- Manual one-off fixes
- Old analytics rollups
- Old inferred financial results

### Exit Criteria

The old system is preserved for reference, but no longer drives new design decisions.

---

## Phase 1 — Create a Clean Technical Environment

Goal: build the new system without old database residue.

### Recommended Setup

- [ ] Create a new Supabase project for Financial Truth Engine.
- [ ] Create new storage buckets for source evidence and extracted artifacts.
- [ ] Create new environment variables.
- [ ] Create a separate Vercel project only when UI work begins.
- [x] Keep the current repo folder separate under `financial-truth-engine/` until a dedicated repo is created or approved.

### Suggested Names

- Supabase project: `n2n-financial-truth-engine`
- App/project folder: `financial-truth-engine`
- Short internal name: `n2n-fte`

### Exit Criteria

A clean empty database exists, and no old EOB tables or data are present.

---

## Phase 2 — Build the Ledger Schema First ✅ Complete (Task 001)

Goal: create the financial truth foundation before extraction, analytics, or UI.

### Core Tables

- [x] `fte_practices`
- [x] `fte_evidence`
- [x] `fte_observations`
- [x] `fte_claims`
- [x] `fte_claim_events`
- [x] `fte_event_evidence`
- [x] `fte_financial_positions`
- [x] `fte_denial_knowledge`
- [x] `fte_contract_terms`
- [x] `fte_review_queue`
- [x] `fte_analysis_runs`

### Rules

- [x] Every tenant-scoped table includes `practice_id`.
- [x] RLS is enabled before real data is loaded.
- [x] Evidence is immutable.
- [x] Observations are not treated as financial truth.
- [x] Claim events must link back to evidence or observations.
- [x] Financial positions are derived/materialized, not manually entered as source truth.

### Exit Criteria

The database can represent one claim, its supporting evidence, its events, and its financial position. ✅ Confirmed via schema validation and synthetic fixture tests.

---

## Phase 3 — Build One Claim Prototype (In Progress — Task 002 delivered reconciler layer)

Goal: prove the architecture on one hard example before building a product around it.

### Prototype Flow

```text
One difficult AZHS EOB
  -> evidence records
  -> AI observations
  -> claim identity
  -> claim events
  -> financial position
  -> evidence-backed explanation
```

### Actions

- [x] Select fixture claims representing known-difficult EOB patterns (ccdbe216, 96c5c357 failure modes).
- [x] Define synthetic evidence and observation records.
- [x] Reconcile observations into claim events via deterministic 9-phase reconciler.
- [x] Derive financial positions from claim events.
- [ ] Load a real (de-identified or explicitly approved synthetic) EOB as evidence.
- [ ] Run AI observation extraction against real evidence.
- [ ] Produce a plain-English explanation with evidence references.

### Exit Criteria

A single claim can be reconstructed from messy evidence into a traceable, auditable financial position.

---

## Phase 4 — Add Review Handling

Goal: make uncertainty explicit instead of hiding it.

### Actions

- [x] Route low-confidence / non-trusted observations to `fte_review_queue` (implemented in reconciler Phase 2 and Phase 7).
- [x] Route unbalanced financial positions to `fte_review_queue` (reconciler Phase 7).
- [x] Ambiguous payment events produce `in_review` positions rather than silent mutations (reconciler Phase 5 + Phase 6).
- [x] Store reviewer corrections as new events or reviewed observations, not silent mutations. (implemented via append-only `fte_review_resolutions` — Tasks 004A/B/C)
- [ ] Build reviewer workflow for confirming or correcting ambiguous/unbalanced positions.

### Exit Criteria

The system can say, "I do not know," without corrupting financial truth.

---

## Phase 5 — Add Denial and Contract Intelligence

Goal: reason from ledger truth, not raw extraction.

### Actions

- [ ] Seed `fte_denial_knowledge` with core CARC/RARC rules.
- [ ] Create recoverable denial detection from claim events and financial positions.
- [ ] Add contract variance detection.
- [ ] Detect underpaid CPTs.
- [ ] Detect repeated payer behavior patterns.
- [ ] Create dollar-aware recommendations.

### Exit Criteria

The system can identify recoverable denials and possible payer underpayment patterns with supporting evidence.

---

## Phase 6 — Add UI Last

Goal: avoid building screens around an unproven data model.

### First Screens

- [ ] Claim ledger detail
- [ ] Evidence viewer
- [ ] Observation review queue
- [ ] Financial position summary
- [ ] Denial / contract worklist
- [ ] Recommendation feed

### Exit Criteria

Users can inspect what happened, why the system believes it, and what action is recommended.

---

## Original Task 001 Prompt (Delivered — see Task Log above)

The following prompt was used to initiate Task 001 and is preserved as historical record:

```text
We are starting the separate Financial Truth Engine initiative.

Do not modify the existing EOB extraction implementation.
Do not migrate old extracted EOB rows.
Do not build UI yet.

Create a clean Supabase/Postgres schema for:
- practices
- evidence
- observations
- claims
- claim_events
- event_evidence
- financial_positions
- denial_knowledge
- contract_terms
- review_queue
- analysis_runs

Requirements:
- practice_id on all tenant-scoped tables
- RLS enabled and policy stubs included
- evidence is immutable
- observations are AI-visible facts only, not truth
- claim_events represent auditable financial events
- financial_positions are derived from claim_events
- every event can link back to evidence/observations
- include indexes for claim lookup, payer lookup, evidence lookup, and event reconstruction
- include comments explaining why this is separate from the old EOB architecture

Deliver:
1. migrations
2. table comments
3. RLS policies
4. indexes
5. a short README explaining the schema
```

---

## Non-Negotiables

- Do not wipe anything until the old project has been archived.
- Do not migrate old extracted financial rows into the new system.
- Do not let AI calculate final financial truth.
- Do not build analytics before the ledger works.
- Do not build UI before the one-claim prototype works.
- Do not allow recommendations without evidence links.
- Do not use real patient data, real member IDs, real DOBs, real SSNs, or production exports in fixtures or tests.
- Do not connect FTE tasks to the legacy EOB Supabase project.
- Synthetic fixtures only unless explicitly approved otherwise.

---

## Immediate Next Action

**Tasks 001 through 004E are complete.**

The schema layer (migrations 001–004), deterministic reconciler (9 phases +
Phase 0.5), five reviewer action categories (payment-event confirmation,
observation confirm/reject/mark_duplicate, corrected-value attachment and
supersession), and corrected-value replacement ergonomics are all proven on
synthetic data across 52 numeric validation checks across 5 suites — all PASS
in a disposable Supabase project.

The next slice should come from Phase 3 or Phase 4 of the roadmap above:

- **Phase 3** — real (de-identified or explicitly approved synthetic) EOB as
  evidence; AI observation extraction against real evidence; plain-English
  explanation with evidence references.
- **Phase 4** — reviewer workflow for confirming or correcting ambiguous/
  unbalanced positions (UI-facing, requires Phase 3 evidence first).

Before starting either: write a CODEX task spec, get Keith's approval, then
implement. Do not start without a written spec.

# Financial Truth Engine — Clean Start Next Steps

**Status:** Execution checklist  
**Created:** 2026-06-17  
**Updated:** 2026-06-19  
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

### Task 003 — TBD ⏳ Scope Before Coding

**Status: planning placeholder only.**

Do not implement Task 003 until the scope is deliberately defined in a CODEX
task spec. Do not create SQL, migrations, Edge Functions, UI, or API code based
on this placeholder.

**Candidate directions (choose one before starting):**

- AI observation ingestion layer — how does a real PDF become `fte_observations`
  rows? What is the Gemini prompt contract? How are confidence scores assigned?
- Real evidence intake — load one real (de-identified or synthetic) EOB into
  `fte_evidence` and `fte_observations`, then run the reconciler against it.
- Review queue resolution — how does a human reviewer correct or confirm an
  ambiguous/unbalanced position? What new events or reviewed-observation rows
  does that produce?
- Denial intelligence — seed `fte_denial_knowledge` with CARC/RARC rules;
  detect recoverable denials from `fte_claim_events`.

**Before starting Task 003:**
1. Pick one direction from the list above.
2. Write a CODEX task spec (like `CODEX_TASK_002.md`) with explicit schema
   changes, inputs, outputs, and a validation strategy.
3. Get approval before writing any implementation code.

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
- [ ] Store reviewer corrections as new events or reviewed observations, not silent mutations.
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

**Define Task 003 scope before writing any code.**

Tasks 001 (ledger schema + fixtures) and 002 (deterministic reconciler prototype)
are merged and validated. The schema layer and deterministic reconciler layer are
proven on synthetic data.

The next step is deliberate scoping — pick one direction from the Task 003
candidate list above, write a CODEX task spec, and get approval before any
implementation begins.

# Financial Truth Engine — Clean Start Next Steps

**Status:** Execution checklist  
**Created:** 2026-06-17  
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

## Phase 0 — Archive and Freeze the Legacy EOB Work

Goal: preserve what was learned without carrying forward polluted assumptions or questionable extracted data.

### Actions

- [ ] Tag the current repository state as `eob-legacy-final`.
- [ ] Export current Supabase schema and migrations.
- [ ] Export a small curated set of difficult EOB source documents as test fixtures.
- [ ] Document the known failure modes from the current EOB project.
- [ ] Freeze feature development on the old document-first architecture.
- [ ] Do not migrate old extracted rows into the new Financial Truth Engine.

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
- [ ] Keep the current repo folder separate under `financial-truth-engine/` until a dedicated repo is created or approved.

### Suggested Names

- Supabase project: `n2n-financial-truth-engine`
- App/project folder: `financial-truth-engine`
- Short internal name: `n2n-fte`

### Exit Criteria

A clean empty database exists, and no old EOB tables or data are present.

---

## Phase 2 — Build the Ledger Schema First

Goal: create the financial truth foundation before extraction, analytics, or UI.

### Core Tables

- [ ] `practices`
- [ ] `evidence`
- [ ] `observations`
- [ ] `claims`
- [ ] `claim_events`
- [ ] `event_evidence`
- [ ] `financial_positions`
- [ ] `denial_knowledge`
- [ ] `contract_terms`
- [ ] `review_queue`
- [ ] `analysis_runs`

### Rules

- [ ] Every tenant-scoped table includes `practice_id`.
- [ ] RLS is enabled before real data is loaded.
- [ ] Evidence is immutable.
- [ ] Observations are not treated as financial truth.
- [ ] Claim events must link back to evidence or observations.
- [ ] Financial positions are derived/materialized, not manually entered as source truth.

### Exit Criteria

The database can represent one claim, its supporting evidence, its events, and its financial position.

---

## Phase 3 — Build One Claim Prototype

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

- [ ] Select one known difficult AZHS EOB.
- [ ] Load the PDF as evidence.
- [ ] Split it into page-level evidence records.
- [ ] Extract observations only.
- [ ] Reconcile observations into one claim.
- [ ] Create claim events.
- [ ] Materialize the financial position.
- [ ] Produce a plain-English explanation with evidence references.

### Exit Criteria

A single claim can be reconstructed from messy evidence into a traceable, auditable financial position.

---

## Phase 4 — Add Review Handling

Goal: make uncertainty explicit instead of hiding it.

### Actions

- [ ] Route low-confidence observations to `review_queue`.
- [ ] Route ambiguous claim matches to `review_queue`.
- [ ] Route unbalanced financial positions to `review_queue`.
- [ ] Store reviewer corrections as new events or reviewed observations, not silent mutations.

### Exit Criteria

The system can say, “I do not know,” without corrupting financial truth.

---

## Phase 5 — Add Denial and Contract Intelligence

Goal: reason from ledger truth, not raw extraction.

### Actions

- [ ] Seed `denial_knowledge` with core CARC/RARC rules.
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

## First Claude Code / Codex Task

Use this as the first implementation prompt:

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

---

## Immediate Next Action

Create the clean Supabase project and implement Phase 2: the ledger schema.

After that, run one difficult AZHS EOB through the one-claim prototype.

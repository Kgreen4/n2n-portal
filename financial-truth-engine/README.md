# Financial Truth Engine

**Status:** Active — schema, reconciler, and review resolutions proven on synthetic data (Task 004C merged 2026-06-22)
**Owner:** Keith Green / N2N Analytics  
**Created:** 2026-06-16  
**Important:** This effort is intentionally separate from the current EOB extraction project.

---

## Purpose

This folder captures the separate Financial Truth Engine initiative. It should not be treated as an implementation change to the current EOB project until Keith explicitly decides to merge, migrate, or prototype part of it.

The current EOB project is document/extraction centered:

```text
PDF -> Extract fields -> Reconcile -> Analyze
```

The Financial Truth Engine is ledger centered:

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The difference matters: the PDF is evidence, not truth. AI extracts observations. Deterministic reconciliation builds financial truth. Analytics and recommendations reason from the ledger.

---

## Design Principle

Do not ask AI to decide final financial truth.

AI should identify visible facts:

- payments
- adjustments
- claim identifiers
- check/EFT identifiers
- CARC/RARC codes
- patient responsibility amounts
- service dates
- CPT/modifier observations
- payer/provider observations

The system should then reconcile those observations into auditable claim events and financial positions.

---

## Target Architecture

```text
Layer 1: Evidence
  PDFs, PDF pages, OCR text, ERA/835 segments, check stubs, payer exports

Layer 2: Observations
  AI-extracted visible facts with confidence and evidence references

Layer 3: Claim Ledger
  Claims, claim events, event-evidence links, financial positions

Layer 4: Reasoning Engine
  Denial worklists, contract variance findings, payer behavior intelligence,
  revenue leakage recommendations, executive narratives
```

---

## Core Tables To Prototype

Recommended first-pass model:

- `evidence`
- `observations`
- `claims`
- `claim_events`
- `event_evidence`
- `financial_positions`
- `denial_knowledge`
- `contract_terms`

All tenant-scoped tables should include `practice_id` and RLS.

---

## Build Epics

### Epic 0: Keep This Separate

Goal: protect the current EOB project while exploring the Financial Truth Engine separately.

Tasks:

- Keep this folder isolated from current EOB implementation files.
- Do not modify existing EOB extraction code as part of this effort yet.
- Use this folder as the architecture and planning space.
- Prototype only when Keith explicitly starts the Financial Truth Engine workstream.

Exit criteria:

- Current EOB project remains unchanged.
- Financial Truth Engine has its own clean project area.

### Epic 1: Financial Truth Data Model

Goal: design the ledger/evidence schema.

Tasks:

- Create draft migrations for evidence, observations, claims, claim_events, event_evidence, financial_positions, denial_knowledge, and contract_terms.
- Add tenant isolation via `practice_id`.
- Add RLS policies.
- Add indexes for claim lookup, payer lookup, document lookup, and event reconstruction.

Exit criteria:

- A schema can be reviewed without touching the current production EOB tables.

### Epic 2: Observation-Based Extraction

Goal: make extraction produce observations, not final answers.

Tasks:

- Define an observation schema.
- Create extraction prompts that return visible facts only.
- Preserve source evidence, raw values, normalized values, page references, and confidence scores.

Exit criteria:

- A PDF can be converted into evidence + observations without asserting final financial truth.

### Epic 3: Claim Reconciliation Service

Goal: build the core Financial Truth Engine.

Tasks:

- Prototype `reconcile_claim()`.
- Group observations into claim identity.
- Emit claim events.
- Link every event to supporting evidence.
- Materialize financial positions.
- Flag ambiguous or unbalanced positions for review.

Exit criteria:

- One difficult AZHS claim can be reconstructed from evidence into a traceable financial position.

### Epic 4: Denial and Contract Intelligence

Goal: turn ledger truth into revenue actions.

Tasks:

- Identify recoverable denials.
- Detect underpaid CPTs.
- Detect modifier and fee schedule variance.
- Detect repeated payer behavior patterns.
- Prioritize findings by recoverable dollars and deadline risk.

Exit criteria:

- Recommendations are evidence-backed, dollar-aware, and auditable.

---

## Recommended First Prototype

Do not start with another extraction prompt update.

Start with one difficult AZHS EOB and prove this flow:

```text
PDF page evidence
  -> observations
  -> claim identity
  -> claim events
  -> financial position
  -> evidence-backed explanation
```

This proves whether the ledger architecture reduces fragility before UI, reports, or broader analytics are built.

---

## Current Capabilities

As of Task 004C (2026-06-22):

- 11-table ledger schema with RLS, tenant isolation, and immutable evidence
- Deterministic 9-phase reconciler (`fte_reconcile_practice`) — idempotent, evidence-linked
- Phase 0.5 review resolution loading — reviewer decisions survive reruns
- Three reviewer action categories proven on synthetic data:
  - `confirm_payment_event` — promotes ambiguous payment events to reconciled/balanced
  - `confirm_observation` / `reject_observation` / `mark_duplicate` — observation-level suppression
- 31 validation checks across 4 test suites, all passing in a disposable Supabase project

Not yet implemented: corrected-value attachment, AI extraction layer, UI, API, Edge Functions.

---

## Current Validation Suites

| File | Checks | Task |
|---|---|---|
| `tests/validate_schema.sql` | structure | 001 |
| `tests/validate_reconciler.sql` | 12 | 002 |
| `tests/validate_review_resolution.sql` | 7 | 004A/B |
| `tests/validate_observation_resolution.sql` | 12 | 004C |

All suites wrap in `ROLLBACK` — nothing persists. See `tests/README.md` for run order.

---

## Claude Code / Codex Starting Prompt

```text
We are starting a separate Financial Truth Engine initiative.

Do not modify the existing EOB extraction implementation.

Design a claim-centric ledger prototype with:
- immutable evidence
- AI observations
- claims
- claim events
- event-to-evidence links
- financial positions
- denial knowledge
- contract terms

The PDF is evidence, not truth. AI outputs observations only. Deterministic reconciliation creates financial truth.

Produce the first schema and a one-claim reconciliation prototype plan.
```

# Financial Truth Engine

**Status:** Active — schema, reconciler, corrected-value resolutions, position-level resolutions (dismiss_short_pay + confirm_short_pay), durable evidence-request workflow (request_more_evidence), and durable correction-needed marker (mark_position_needs_correction) proven on synthetic data (Tasks 001–005E merged)
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

As of Task 005D (2026-06-24):

- 11-table ledger schema with RLS, tenant isolation, and immutable evidence
- Deterministic 9-phase reconciler (`fte_reconcile_practice`) — idempotent, evidence-linked
- Phase 0.5 review resolution loading — reviewer decisions survive reruns
- Five reviewer action categories proven on synthetic data:
  - `confirm_payment_event` — promotes ambiguous payment events to reconciled/balanced
  - `confirm_observation` / `reject_observation` / `mark_duplicate` — observation-level suppression
  - `attach_corrected_value` — per-observation amount correction applied to `billed_amount`, `contractual_adjustment`, and `payment` observations; enforced by DB constraints (migration 004); Phases 3, 4, and 5c each use `COALESCE(corrected_value, extracted_amount)` — see `reconciler/README.md §4`
  - `dismiss_short_pay` — position-level dismissal: suppresses Phase 7 queue routing and Phase 8 `short_pay_detected` event; preserves mathematical `unbalanced` position; enforced by DB constraints (migration 005) — see `reconciler/README.md §5`
  - `confirm_short_pay` — position-level confirmation: suppresses Phase 7 queue routing only; preserves `short_pay_detected` event so downstream recovery workflows remain active; preserves mathematical `unbalanced` position; conflict-prevention index prevents simultaneous active `confirm_short_pay` + `dismiss_short_pay` for the same claim (migration 006) — see `reconciler/README.md §5.6–§5.10`
  - `request_more_evidence` — durable reviewer note that a claim cannot be resolved without additional evidence (e.g. a clean 835 remittance, a payer callback); requires non-null/non-blank `notes` and a stable `claim_id` anchor; at most one active evidence request per claim (partial unique index); claim retains its reconciler-derived position status (`in_review` or `unbalanced`) unchanged; Phase 7 queue routing is NOT suppressed; no claim events emitted; supersede the row to close the request — see `reconciler/README.md §5.12`
  - `mark_position_needs_correction` — durable correction-needed marker; requires non-null/non-blank `notes` and a stable `claim_id` anchor; at most one active marker per claim (partial unique index); no Phase 2/6/7/8 change — claim retains its reconciler-derived status unchanged; Phase 7 queue routing is NOT suppressed (contrast: `dismiss_short_pay` suppresses both Phase 7 and Phase 8; `confirm_short_pay` suppresses Phase 7 only); Phase 8 `short_pay_detected` event is NOT suppressed; no claim events emitted — see `reconciler/README.md §5.13`
- 115 numeric checks across 12 test suites, all passing in a disposable Supabase project

Not yet implemented: AI extraction layer, UI, API, Edge Functions.

Position-level reviewer actions currently implemented: `dismiss_short_pay`,
`confirm_short_pay`, `request_more_evidence`. `confirm_position_balanced` is
intentionally deferred — see `reconciler/README.md §5.11` and
`README_SCHEMA.md` Invariant 12 for the deferral rationale. Balanced-by-review
(reviewer asserting `balanced` without event-derived math) is not implemented.

---

## Current Validation Suites

| File | Checks | Task |
|---|---|---|
| `tests/validate_schema.sql` | structure | 001 |
| `tests/validate_reconciler.sql` | 12 | 002 |
| `tests/validate_review_resolution.sql` | 7 | 004A/B |
| `tests/validate_observation_resolution.sql` | 12 | 004C |
| `tests/validate_corrected_value.sql` | 11 | 004D |
| `tests/validate_corrected_value_supersession.sql` | 10 | 004E |
| `tests/validate_corrected_contractual_adjustment.sql` | 10 | 004G |
| `tests/validate_corrected_billed_amount.sql` | 10 | 004H |
| `tests/validate_dismiss_short_pay.sql` | 9 | 005A |
| `tests/validate_confirm_short_pay.sql` | 10 | 005B |
| `tests/validate_request_more_evidence.sql` | 12 | 005D |
| `tests/validate_mark_position_needs_correction.sql` | 12 | 005E |

All suites wrap in `ROLLBACK` — nothing persists. See `tests/RUNBOOK.md` for run order.

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

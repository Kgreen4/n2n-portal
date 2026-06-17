# Codex Task 001 — Clean Ledger Schema + Synthetic Hard-Case Fixtures

**Status:** Ready for Codex  
**Created:** 2026-06-17  
**Scope:** Financial Truth Engine only  
**Do not modify:** Existing EOB extraction project/code/tables

---

## Task Summary

Create the first implementation foundation for the separate Financial Truth Engine initiative.

This task is intentionally limited to:

1. Repository hygiene for this separate effort.
2. Supabase/Postgres ledger schema migrations.
3. RLS, indexes, comments, and constraints.
4. Synthetic fixtures that simulate the known hard EOB failure modes.
5. Basic tests or validation scripts where practical.

Do not build UI.  
Do not refactor existing EOB code.  
Do not migrate old EOB extracted data.  
Do not commit PHI or raw EOB PDFs.

---

## Architecture Principle

The Financial Truth Engine is ledger-centered:

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The PDF is evidence, not truth.

AI outputs observations only.

Deterministic reconciliation creates financial truth.

---

## Required Files To Create

Create these files inside `financial-truth-engine/` or nested subfolders:

```text
financial-truth-engine/
  AGENTS.md
  README_SCHEMA.md
  migrations/
    001_create_financial_truth_schema.sql
  fixtures/
    README.md
    synthetic_ccdbe216_failure_modes.sql
    synthetic_96c5c357_failure_modes.sql
  tests/
    README.md
    validate_schema.sql
```

If the existing repo has a preferred migration/test convention, follow it, but keep this work isolated under `financial-truth-engine/` unless instructed otherwise.

---

## AGENTS.md Rules

Create an `AGENTS.md` file that tells future Codex/agent runs:

- This effort is separate from the current EOB project.
- Work only under `financial-truth-engine/` unless explicitly instructed.
- Do not modify existing EOB extraction code.
- Do not reference or migrate old extracted EOB rows as truth.
- Do not commit PHI, raw PDFs, production exports, screenshots, member IDs, or patient data.
- Use synthetic or redacted fixtures only.
- AI observations are not financial truth.
- Claim events and financial positions must be deterministic and auditable.
- All financial conclusions must link back to evidence and/or observations.

---

## Migration Requirements

Create `migrations/001_create_financial_truth_schema.sql`.

It must define:

- `fte_practices`
- `fte_evidence`
- `fte_observations`
- `fte_claims`
- `fte_claim_events`
- `fte_event_evidence`
- `fte_financial_positions`
- `fte_denial_knowledge`
- `fte_contract_terms`
- `fte_review_queue`
- `fte_analysis_runs`

Use `fte_` prefixes so the schema remains cleanly separated from the old EOB project even if temporarily deployed into the same Supabase project.

---

## Table Design Requirements

### General

- Use UUID primary keys.
- Include `practice_id` on every tenant-scoped table.
- Include `created_at` and `updated_at` where appropriate.
- Add table and column comments.
- Enable RLS on all tenant-scoped tables.
- Add basic RLS policy stubs.
- Add useful indexes.
- Add constraints where practical.

### Evidence

`fte_evidence` stores immutable source artifacts or derived evidence units.

Must support:

- document-level evidence
- page-level evidence
- OCR text evidence
- ERA/835 segment evidence
- check/payment evidence
- payer portal/export evidence

Important:

- Evidence should be append-only.
- Avoid updates to source content.
- Corrections should be represented as new observations or events, not mutations.

### Observations

`fte_observations` stores AI-visible facts extracted from evidence.

Must support:

- observation type
- amount
- amount type
- claim identifier
- payer name
- service date
- CPT/modifier values
- CARC/RARC values
- check/EFT identifier
- confidence score
- raw value
- normalized value
- source evidence reference
- page/bounding metadata where useful

Important:

- Observations are not final truth.
- Observations must not directly update financial positions.

### Claims

`fte_claims` represents claim identity.

Must support:

- internal claim ID
- practice ID
- claim number
- payer claim number
- patient identifier hash or synthetic placeholder
- service date range
- payer name
- status

### Claim Events

`fte_claim_events` represents auditable financial events.

Must support:

- event type
- event date
- amount
- amount type
- payer name
- CARC/RARC
- reason category
- confidence score
- reconciliation status
- metadata

Examples:

- claim submitted
- claim adjudicated
- payment applied
- contractual adjustment applied
- patient responsibility assigned
- denial posted
- short-pay detected
- appeal filed
- recovery received
- write-off approved

### Event Evidence

`fte_event_evidence` links claim events back to evidence and/or observations.

Requirements:

- A claim event must be able to point to one or more evidence/observation records.
- This is the audit trail for every financial conclusion.

### Financial Positions

`fte_financial_positions` stores derived/materialized current state per claim.

Must support:

- billed amount
- allowed amount
- contractual adjustment amount
- paid amount
- denied amount
- patient responsibility amount
- recoverable amount
- open balance amount
- position confidence score
- reconciliation status
- last reconciled timestamp

Important:

- Financial positions are derived from claim events.
- Do not treat AI observations as financial positions.

### Denial Knowledge

`fte_denial_knowledge` stores editable CARC/RARC/payer intelligence.

Must support:

- payer-specific overrides
- CARC/RARC category and subcategory
- recoverable flag
- default action
- default owner
- appeal window days
- evidence requirements

### Contract Terms

`fte_contract_terms` stores expected payer behavior.

Must support:

- payer name
- CPT code
- modifiers
- effective date range
- expected allowed amount or percentage
- source metadata

### Review Queue

`fte_review_queue` stores ambiguity and exception handling.

Must support reasons such as:

- low-confidence observation
- conflicting observations
- missing evidence link
- unbalanced financial position
- suspected duplicate
- suspected summary row
- late/retry page contradiction

### Analysis Runs

`fte_analysis_runs` stores execution/audit metadata.

Must support:

- run type
- fixture/source identifier
- status
- inputs hash
- summary
- error details

---

## Synthetic Fixture Requirements

Do not use live PHI or real PDFs.

Create synthetic SQL fixtures that simulate the two hard cases documented in `FIXTURE_PLAN.md`.

### Fixture 1: `ccdbe216`

Simulate a 112-page BCBS AZ multiple-payment EOB with:

- a large original reconciliation gap scenario
- duplicate/phantom check or reference observation
- section-delimiter double counting
- null-check crossbleed across multiple check sections
- spurious summary row
- late/retry page contradiction on page 63
- conflicting observations that should route to review

Expected behavior:

- Conflicts are represented as observations, not silently overwritten.
- Summary row is not treated as transaction truth.
- Late/retry contradiction is routed to review.
- Financial positions are not directly mutated by observations.

### Fixture 2: `96c5c357`

Simulate an Arizona Priority Care EOB with:

- a large single-check gap scenario
- a second unresolved gap scenario
- retry-pending ambiguity

Expected behavior:

- The unresolved gap is visible in review_queue.
- Ledger output remains explainable and evidence-linked.

---

## Validation Requirements

Create `tests/validate_schema.sql` or equivalent validation scripts that demonstrate:

- RLS is enabled on tenant-scoped tables.
- Observations can be inserted without creating financial positions.
- Claim events can link to evidence/observations.
- Financial positions are claim-scoped and practice-scoped.
- Review queue can capture low-confidence/conflicting/late-retry issues.
- Old EOB tables are not referenced.

---

## Deliverables

Codex should produce:

1. Files listed above.
2. SQL migration.
3. Synthetic fixtures.
4. Validation script or test notes.
5. Brief explanation of design decisions.
6. Any assumptions or limitations.

---

## Out of Scope

Do not do the following in this task:

- No UI.
- No PDF parsing implementation.
- No Gemini/OpenAI extraction prompt implementation.
- No production Supabase deployment.
- No real EOB data.
- No PHI.
- No old EOB data migration.
- No modifications to existing app flows.

---

## Definition of Done

This task is complete when a reviewer can inspect the generated files and see a clean, isolated Financial Truth Engine database foundation with synthetic hard-case fixtures that prove the new architecture is designed to handle legacy failure modes without corrupting financial truth.

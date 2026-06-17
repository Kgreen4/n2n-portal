# Financial Truth Engine v2 Strategy

**Project:** N2N Portal / RCM Financial Intelligence  
**Owner:** Keith Green / N2N Analytics  
**Date:** 2026-06-16  
**Status:** Working architecture direction  
**Source conversation:** `Codex - Strategy.txt`

---

## 1. Executive Decision

The project should pivot from an **EOB extraction application** to an **RCM Financial Intelligence Platform**.

The current approach centers the PDF:

```text
PDF -> Extract fields -> Reconcile -> Analyze
```

The new approach centers financial truth:

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The reason for the pivot is simple: EOB extraction will never be perfect across every payer, layout, OCR artifact, check fragment, or page split. A trustworthy product should not depend on perfect extraction. It should isolate extraction uncertainty as evidence and observations, then build an auditable financial ledger from reconciled claim events.

---

## 2. Product North Star

Build a claim-centric platform that answers the questions billing leaders actually ask:

- What happened to this claim?
- What money was paid, denied, adjusted, shifted to patient responsibility, or still at risk?
- Which evidence supports that conclusion?
- Which payer behavior is creating revenue leakage?
- What should the billing team do next?
- Which fixes are worth the most money?

This is not merely denial management. The larger opportunity is **contract and payer behavior intelligence**: underpaid CPTs, fee-schedule mismatches, bundling inconsistencies, modifier issues, payer-specific variance, and repeat operational failures.

---

## 3. Core Architectural Shift

### Previous Center of Gravity

```text
Document
  -> Payment
      -> Line Item
```

This reflects the PDF structure.

### New Center of Gravity

```text
Claim Ledger
   <-> Evidence
   <-> Observations
   <-> Financial Events
   <-> Payments
   <-> Denials
   <-> Adjustments
   <-> Appeals
   <-> Payers
   <-> Contracts
```

This reflects how RCM teams reason about money.

The PDF becomes evidence, not truth.

---

## 4. Four-Layer Platform Model

### Layer 1: Evidence

Immutable raw source material.

Examples:

- PDF document
- PDF page
- OCR text block
- ERA / 835 segment
- Check stub
- Payment remittance record
- Payer portal export

Evidence should be stored, versioned, and never edited. Corrections should be represented as new observations or events, not mutations to the original evidence.

### Layer 2: Observations

AI-visible facts extracted from evidence.

Examples:

```json
{
  "observation_type": "payment",
  "amount": 250.00,
  "confidence": 94,
  "source_evidence_id": "..."
}
```

```json
{
  "observation_type": "adjustment",
  "carc_code": "45",
  "amount": 50.00,
  "confidence": 97,
  "source_evidence_id": "..."
}
```

The AI should not decide final paid amount, denied amount, allowed amount, or claim state. It should report what is visible.

### Layer 3: Claim Ledger

Deterministic and auditable financial truth.

The ledger converts observations into claim events:

- Claim submitted
- Claim adjudicated
- Payment applied
- Contractual adjustment applied
- Patient responsibility assigned
- Denial posted
- Short-pay detected
- Appeal filed
- Recovery received
- Write-off approved

Each event links back to supporting evidence.

### Layer 4: Reasoning Engine

The intelligence layer reasons from the claim ledger, not raw PDFs.

Outputs:

- Recoverable denial worklist
- Contract variance findings
- Payer behavior intelligence
- Revenue leakage recommendations
- Executive narrative summaries
- Monthly action plans

LLMs may write narratives, but they should not calculate dollars or invent classifications without deterministic support.

---

## 5. Recommended Data Model Direction

### evidence

Immutable source artifacts.

Suggested fields:

```sql
evidence (
  id uuid primary key,
  practice_id uuid not null,
  document_id uuid,
  evidence_type text not null,
  source_uri text,
  page_number integer,
  raw_text text,
  metadata jsonb,
  created_at timestamptz default now()
)
```

### observations

AI-extracted facts, still not trusted as final truth.

```sql
observations (
  id uuid primary key,
  practice_id uuid not null,
  evidence_id uuid not null references evidence(id),
  observation_type text not null,
  claim_identifier text,
  payer_name text,
  patient_identifier_hash text,
  service_date date,
  cpt_code text,
  modifier_codes text[],
  amount numeric(12,2),
  amount_type text,
  carc_code text,
  rarc_code text,
  check_number text,
  confidence_score integer,
  bounding_box jsonb,
  raw_value text,
  normalized_value jsonb,
  created_at timestamptz default now()
)
```

### claims

Permanent claim identity.

```sql
claims (
  id uuid primary key,
  practice_id uuid not null,
  claim_number text,
  payer_claim_number text,
  patient_identifier_hash text,
  primary_payer_name text,
  service_from date,
  service_to date,
  status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
)
```

### claim_events

Auditable financial/legal event stream.

```sql
claim_events (
  id uuid primary key,
  practice_id uuid not null,
  claim_id uuid not null references claims(id),
  event_type text not null,
  event_date date,
  amount numeric(12,2),
  amount_type text,
  payer_name text,
  carc_code text,
  rarc_code text,
  reason_category text,
  confidence_score integer,
  reconciliation_status text,
  metadata jsonb,
  created_at timestamptz default now()
)
```

### event_evidence

Many-to-many support graph between claim events and evidence/observations.

```sql
event_evidence (
  id uuid primary key,
  practice_id uuid not null,
  claim_event_id uuid not null references claim_events(id),
  evidence_id uuid references evidence(id),
  observation_id uuid references observations(id),
  support_type text not null,
  confidence_score integer,
  created_at timestamptz default now()
)
```

### financial_positions

Materialized current state per claim.

```sql
financial_positions (
  id uuid primary key,
  practice_id uuid not null,
  claim_id uuid not null references claims(id),
  billed_amount numeric(12,2),
  allowed_amount numeric(12,2),
  contractual_adjustment_amount numeric(12,2),
  paid_amount numeric(12,2),
  denied_amount numeric(12,2),
  patient_responsibility_amount numeric(12,2),
  recoverable_amount numeric(12,2),
  open_balance_amount numeric(12,2),
  position_confidence_score integer,
  last_reconciled_at timestamptz,
  reconciliation_status text,
  metadata jsonb,
  unique(practice_id, claim_id)
)
```

### denial_knowledge

Seeded and editable knowledge base.

```sql
denial_knowledge (
  id uuid primary key,
  practice_id uuid,
  payer_name text,
  carc_code text,
  rarc_code text,
  category text,
  subcategory text,
  recoverable boolean,
  default_action text,
  default_owner text,
  appeal_window_days integer,
  evidence_requirements jsonb,
  confidence_score integer,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
)
```

### contract_terms / payer_behavior_rules

This is the long-term moat.

```sql
contract_terms (
  id uuid primary key,
  practice_id uuid not null,
  payer_name text not null,
  cpt_code text,
  modifier_codes text[],
  effective_from date,
  effective_to date,
  expected_allowed_amount numeric(12,2),
  expected_allowed_percent numeric(6,3),
  source text,
  metadata jsonb
)
```

---

## 6. AI Responsibilities

### AI #1: Evidence Extraction Agent

Purpose: identify visible observations.

Should produce:

- payment observations
- adjustment observations
- denial-code observations
- claim-number observations
- service-line observations
- check/EFT observations
- patient-responsibility observations

Should not produce final financial truth.

### AI #2: Reconciliation Assistant

Purpose: help suggest which observations may belong together when deterministic matching is ambiguous.

The deterministic reconciliation engine remains authoritative. AI suggestions should be stored with confidence and reviewed when low confidence.

### AI #3: Analyst / Narrative Agent

Purpose: explain structured findings in plain English.

Allowed:

- summarize calculated findings
- explain likely root causes
- draft recommended next actions
- generate executive narratives

Not allowed:

- calculate financial totals
- invent appeal deadlines
- classify payer behavior without evidence
- issue autonomous billing actions

---

## 7. Build Epics

### Epic 0: Protect the Current System

Goal: preserve existing extraction/reconciliation work while introducing the new architecture beside it.

Tasks:

- Create a feature branch for Financial Truth Engine work.
- Add `docs/strategy/financial-truth-engine-v2.md` as the architecture source of truth.
- Identify current tables/functions that map into the new model.
- Keep existing EOB pipeline working during migration.
- Add test fixtures from known difficult EOBs.

Exit criteria:

- Current production path still works.
- New architecture is documented and implementation can begin safely.

### Epic 1: Financial Truth Data Model

Goal: add ledger/evidence tables without breaking existing EOB tables.

Tasks:

- Create Supabase migrations for evidence, observations, claims, claim_events, event_evidence, financial_positions, denial_knowledge, contract_terms.
- Add RLS policies using `practice_id`.
- Add indexes for claim lookup, payer lookup, document lookup, and event reconstruction.
- Add seed data for core CARC/RARC knowledge.

Exit criteria:

- New tables deploy cleanly.
- Existing tables remain untouched.
- Practice isolation is preserved.

### Epic 2: Observation-Based Extraction

Goal: refactor extraction so Gemini produces observations, not final answers.

Tasks:

- Add an observation schema.
- Modify or wrap `eob-worker` to write observations.
- Store raw extracted snippets and bounding/page references.
- Preserve confidence scores.
- Create a review view for low-confidence observations.

Exit criteria:

- A PDF can be converted into evidence + observations.
- No financial truth is calculated in the extraction step.

### Epic 3: Claim Reconciliation Service

Goal: build the core Financial Truth Engine.

Tasks:

- Implement `reconcile_claim()`.
- Match observations into claim identities.
- Emit claim_events.
- Link every event to supporting evidence.
- Materialize financial_positions.
- Flag ambiguous or unbalanced positions for review.

Exit criteria:

- A claim can be reconstructed from evidence.
- Every financial position has traceable evidence.
- Reconciliation confidence is visible and auditable.

### Epic 4: Denial and Contract Intelligence

Goal: turn financial positions into money-focused actions.

Tasks:

- Create denial worklist from claim_events and financial_positions.
- Add denial_knowledge rules by CARC/RARC/payer.
- Add contract variance detection using contract_terms.
- Detect underpayments, repeat payer behavior, fee schedule mismatches, and payer-specific variance.
- Prioritize recommendations by recoverable dollars and deadline risk.

Exit criteria:

- The system identifies recoverable denials and likely underpayments.
- Recommendations are tied to dollars and evidence.

### Epic 5: User-Facing Workflow

Goal: make the ledger useful to Keith and billing users.

Tasks:

- Add claim ledger view.
- Add evidence drill-down.
- Add denial/contract worklist.
- Add recommendation feed.
- Add executive summary narrative generated only from structured findings.

Exit criteria:

- A user can open a claim and see what happened, why the system believes it, and what action is recommended.

---

## 8. Suggested First Implementation Sequence

1. Add the new tables and RLS policies.
2. Add `observations` output beside the current `eob_line_items` output.
3. Build a one-claim reconciliation prototype.
4. Test against one known hard AZHS EOB.
5. Compare old output vs. new ledger output.
6. Add review handling for ambiguous observations.
7. Expand to full document-level reconciliation.
8. Add denial worklist from claim_events.
9. Add contract variance detection.
10. Add narrative summaries last.

---

## 9. Claude Code / Codex Prompt Pack

### Prompt 1: Ledger Migrations

```text
Design and implement Supabase/Postgres migrations for a claim-centric Financial Truth Engine.

Requirements:
- Immutable evidence table
- AI observations table
- claims table
- claim_events table
- event_evidence join table
- financial_positions materialized state table
- denial_knowledge table
- contract_terms table
- practice_id on every tenant-scoped table
- RLS policies consistent with existing project conventions
- indexes for claim number, payer, document, practice, and event reconstruction

Do not remove or modify the existing EOB extraction tables yet.
```

### Prompt 2: Observation Schema

```text
Refactor the EOB extraction layer so the AI outputs observations only.

Do not ask the AI to calculate final paid amount, denied amount, adjustment totals, or claim status.

The output should include visible facts such as:
- claim identifiers
- check/EFT identifiers
- service dates
- CPT/modifier observations
- payment amount observations
- adjustment amount observations
- CARC/RARC observations
- patient responsibility observations
- payer and provider observations
- confidence score
- source page/evidence reference

Store observations in the new observations table and preserve raw values.
```

### Prompt 3: Claim Reconciliation Engine

```text
Build a deterministic reconciliation service named reconcile_claim.

Input:
- claim_id or unresolved observation group

Responsibilities:
- group observations into claim identity
- create claim_events
- link events to evidence through event_evidence
- calculate financial_positions
- assign reconciliation_status and confidence score
- flag ambiguous or unbalanced claims for review

The service must be auditable and testable. LLMs may suggest matches, but deterministic logic owns final state.
```

### Prompt 4: Contract Intelligence

```text
Build a contract and payer behavior intelligence layer from financial_positions, claim_events, denial_knowledge, and contract_terms.

Detect:
- recoverable denials
- underpaid CPTs
- missing/incorrect modifiers
- fee schedule mismatches
- payer-specific variance
- repeated CARC/RARC patterns
- aging recoverables approaching appeal deadlines

Recommendations must include estimated dollar impact and evidence references.
No LLM may calculate dollar totals.
```

---

## 10. Immediate Next Decision

The first engineering move should not be another extraction prompt update. It should be the ledger foundation.

Recommended next action:

> Create the Financial Truth Engine schema and run one difficult AZHS EOB through an observation -> claim_events -> financial_positions prototype.

That will prove whether the new architecture actually reduces fragility before larger UI or analytics work begins.

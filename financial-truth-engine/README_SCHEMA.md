# Financial Truth Engine — Schema (README_SCHEMA)

**Migrations:** `migrations/001_create_financial_truth_schema.sql`, `migrations/002_add_review_resolutions.sql`
**Status:** Ledger foundation + review resolutions (Phases 2 and 4 of `NEXT_STEPS.md`)
**Scope:** Schema, RLS, indexes, comments, constraints. No UI, no extraction code, no PDF parsing.

---

## Design Principle

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The PDF is **evidence, not truth**. AI produces **observations** (visible facts with confidence
and evidence references). **Deterministic reconciliation** turns observations into auditable
**claim events**, from which **financial positions** are materialized. Analytics and
recommendations reason from the ledger — never directly from raw extraction.

Everything is prefixed `fte_` so it stays cleanly separated from the frozen legacy `eob_*`
tables even if temporarily deployed into the same Supabase project. The migration references
**no** legacy table.

---

## Tables

| Layer | Table | Role |
|---|---|---|
| Tenant | `fte_practices` | Tenant root; every tenant-scoped table FKs here. |
| 1 Evidence | `fte_evidence` | **Immutable, append-only** source artifacts: document / page / OCR text / ERA-835 segment / check-payment / payer-export. Self-references (page → document). No `updated_at`, no UPDATE policy. |
| 2 Observations | `fte_observations` | AI-visible facts: amounts, claim/check identifiers, CARC/RARC, CPT/modifiers, dates, confidence, raw + normalized values, page/bbox. `is_summary_row` and `is_superseded` flags. **Not truth.** |
| 3 Ledger | `fte_claims` | Claim identity. Patient identity stored only as hash/synthetic placeholder. |
| 3 Ledger | `fte_claim_events` | Deterministic, auditable financial events (submitted, adjudicated, payment_applied, contractual_adjustment_applied, patient_responsibility_assigned, denial_posted, short_pay_detected, appeal_filed, recovery_received, write_off_approved). |
| 3 Ledger | `fte_event_evidence` | Audit spine: links each event to evidence and/or observations (`supports` / `derived_from` / `contradicts` / `contextual`). Constraint forbids a link pointing at neither. |
| 3 Ledger | `fte_financial_positions` | **Derived/materialized** per-claim state (billed/allowed/contractual/paid/denied/patient-resp/recoverable/open-balance + confidence + reconciliation status). One per claim. |
| 4 Intelligence | `fte_denial_knowledge` | Editable CARC/RARC/payer rules. `practice_id IS NULL` = global default; non-null = override. |
| 4 Intelligence | `fte_contract_terms` | Expected payer behavior per CPT/modifier and effective window. |
| Review | `fte_review_queue` | Makes uncertainty explicit (low-confidence / conflicting / missing-link / unbalanced / suspected-duplicate / suspected-summary-row / late-retry-page-contradiction). |
| Review | `fte_review_resolutions` | **Append-only** typed reviewer decisions (15-action vocabulary across 3 categories). Survives Phase 0 DELETE — hard FKs to stable entity tables only (`fte_practices`, `fte_claims`, `fte_observations`, `fte_evidence`). Volatile derived-row IDs are snapshot fields with no `REFERENCES` clause; they become stale after a reprocess — that is expected. Phase 0.5 loads non-superseded rows before reconciliation begins. |
| Audit | `fte_analysis_runs` | Execution/audit metadata for reconciliation and ingestion runs. |

---

## Invariants The Schema Enforces (or Strongly Encodes)

1. **Evidence is immutable.** `fte_evidence` has no `updated_at` and only SELECT/INSERT RLS
   policies — no UPDATE/DELETE policy. Corrections are new observations/events, not mutations.
2. **Observations never auto-mutate truth.** There is no trigger from `fte_observations` to
   `fte_financial_positions`. Positions are written only by the (future) deterministic
   reconciler. (Validation check 5 proves an observation insert creates 0 positions.)
3. **Every financial conclusion is auditable.** `fte_event_evidence` links events back to
   evidence/observations, and a constraint rejects a link that references neither.
4. **Positions are claim- and practice-scoped.** `fte_financial_positions.claim_id` is unique
   and `practice_id` is `NOT NULL`.
5. **Ambiguity is explicit, not silent.** Conflicts/duplicates/summary/late-retry/unbalanced
   cases live in `fte_review_queue` instead of overwriting prior records.
6. **Tenant isolation.** RLS is enabled on all `fte_` tables before any real data; policies are
   keyed on `fte_accessible_practice_ids()`.
7. **Reviewer decisions survive Phase 0.** `fte_review_resolutions` carries hard FKs only to
   stable entity tables that Phase 0 never deletes. Volatile derived-row IDs
   (`source_review_queue_id`, `source_claim_event_id`, `source_position_id`) are plain `uuid`
   snapshot fields — no `REFERENCES` clause — and become stale after a reprocess without
   disrupting referential integrity. `ON DELETE CASCADE` would destroy reviewer history;
   `ON DELETE RESTRICT` would block Phase 0's DELETE. Both outcomes are prevented by design.

---

## Row Level Security

RLS is enabled on every `fte_` table. Policies are **deny-by-default stubs** keyed on
`fte_accessible_practice_ids()`, which currently returns no rows. Wire that function to a real
membership lookup (e.g. `select practice_id from practice_members where user_id = auth.uid()`)
before exposing the schema to `anon`/`authenticated` traffic.

- `fte_evidence`: SELECT + INSERT only (append-only; no UPDATE/DELETE policy).
- `fte_denial_knowledge`: global rows (`practice_id IS NULL`) are readable by all; writes and
  tenant rows follow membership.
- `fte_review_resolutions`: `FOR ALL` keyed on practice membership. `is_superseded` is the
  only column mutated after INSERT; superseded rows are retained, never deleted.
- All other tenant tables: `FOR ALL` read/write keyed on practice membership.

Migrations, fixtures, and validation run under Supabase `service_role` / `postgres`
(`BYPASSRLS`), so the deny-by-default stubs don't block setup.

---

## Indexes (by purpose)

- **Claim lookup:** `fte_claims (practice_id, claim_number)`, `(practice_id, payer_claim_number)`, `(practice_id, status)`.
- **Payer lookup:** `fte_claims (practice_id, payer_name)`, `fte_observations (practice_id, payer_name)`, `fte_claim_events (practice_id, payer_name)`, `fte_denial_knowledge (payer_name)`.
- **Evidence lookup:** `fte_observations (evidence_id)`, `fte_evidence (practice_id, evidence_type)`, `(parent_evidence_id)`, `(fixture_id)`, `fte_event_evidence (evidence_id)` / `(observation_id)`.
- **Event reconstruction:** `fte_claim_events (claim_id, event_date)`, `fte_event_evidence (claim_event_id)`.

---

## How To Apply

```bash
psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql
psql "$DATABASE_URL" -f fixtures/synthetic_ccdbe216_failure_modes.sql   # optional
psql "$DATABASE_URL" -f fixtures/synthetic_96c5c357_failure_modes.sql   # optional
psql "$DATABASE_URL" -f tests/validate_schema.sql
```

Use the Supabase `service_role` / `postgres` connection.

---

## Design Decisions, Assumptions, Limitations

**Decisions**
- `fte_` prefix on every object for clean coexistence with legacy `eob_*` during transition.
- Evidence immutability encoded structurally (no `updated_at`, restricted policies) rather than
  by application convention alone.
- `fte_event_evidence` is a single audit spine for both evidence and observation provenance,
  with a `CHECK` guaranteeing every link is grounded.
- `fte_denial_knowledge.practice_id` nullable to allow shared global CARC/RARC defaults.
- Soft `is_superseded` / `is_summary_row` flags so contradictory and aggregate observations are
  retained for audit rather than deleted.

**Assumptions**
- Postgres 13+ (uses `gen_random_uuid()`; `pgcrypto` enabled defensively).
- A future membership table/JWT claim will back `fte_accessible_practice_ids()`. Until then RLS
  denies non-superuser access by default (intentionally safe).
- Reconciliation logic (`reconcile_claim()`), extraction prompts, and PDF/PII handling are out
  of scope for this task and arrive in later phases (see `NEXT_STEPS.md` Phases 3–5).

**Limitations**
- No reconciliation engine yet — positions in fixtures are hand-authored to illustrate the
  *target* derived state, not produced by code.
- RLS policies are stubs; they are correct in shape but not yet wired to real auth.
- Patient identity is modeled only as a hash/placeholder column; no hashing function is provided
  here (kept out of scope to avoid implying a particular PHI-handling approach).

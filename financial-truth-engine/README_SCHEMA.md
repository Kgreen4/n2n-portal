# Financial Truth Engine — Schema (README_SCHEMA)

**Migrations:** `migrations/001_create_financial_truth_schema.sql`, `migrations/002_add_review_resolutions.sql`, `migrations/003_add_observation_resolution_target.sql`, `migrations/004_corrected_value_constraints.sql`, `migrations/005_dismiss_short_pay_constraints.sql`, `migrations/006_confirm_short_pay_constraints.sql`, `migrations/007_request_more_evidence_constraints.sql`, `migrations/008_mark_position_needs_correction_constraints.sql`, `migrations/009_mark_position_resolved_constraints.sql`, `migrations/010_reject_payment_event_constraints.sql`, `migrations/011_assert_check_identity_constraints.sql`
**Status:** Ledger foundation + review resolutions + observation-level resolution constraints + corrected-value enforcement + position-level resolutions (dismiss_short_pay + confirm_short_pay + request_more_evidence + mark_position_needs_correction + mark_position_resolved) + payment-event-level suppression (reject_payment_event) + durable check-identity assertion (assert_check_identity) (Tasks 001–005H)
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
| Review | `fte_review_resolutions` | **Append-only** typed reviewer decisions (15-action vocabulary across 3 categories). Survives Phase 0 DELETE — hard FKs to stable entity tables only (`fte_practices`, `fte_claims`, `fte_observations`, `fte_evidence`). Volatile derived-row IDs are snapshot fields with no `REFERENCES` clause; they become stale after a reprocess — that is expected. Phase 0.5 loads non-superseded rows before reconciliation begins. Migration 003 adds `target_observation_id uuid references fte_observations(id) on delete restrict` plus 5 CHECK constraints for the three observation-level actions (`confirm_observation`, `reject_observation`, `mark_duplicate`) and a partial index for reverse lookup. Migration 004 adds 4 CHECK constraints for `attach_corrected_value` (requires `observation_id IS NOT NULL`, `target_type = 'observation'`, `corrected_value IS NOT NULL`, `corrected_value >= 0`) and `idx_fte_resolutions_single_active_correction` — `UNIQUE (practice_id, observation_id, action) WHERE is_superseded = false AND action = 'attach_corrected_value'` — enforcing at most one active corrected-value resolution per observation. Migration 005 adds 2 CHECK constraints for `dismiss_short_pay` (`claim_id IS NOT NULL`, `target_type = 'position'`). Migration 006 adds 2 CHECK constraints for `confirm_short_pay` (`claim_id IS NOT NULL`, `target_type = 'position'`) and `idx_fte_resolutions_single_active_position_short_pay` — `UNIQUE (practice_id, claim_id) WHERE is_superseded = false AND action IN ('confirm_short_pay', 'dismiss_short_pay')` — preventing simultaneous active rows of both actions for the same claim. To supersede: set `is_superseded = true` on the old row, then insert a new one. |
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
8. **Observation-level resolution actions are shape-constrained.** Migration 003 adds 5 CHECK
   constraints to `fte_review_resolutions`: the three observation-level actions require
   `observation_id IS NOT NULL` and `target_type = 'observation'`; `mark_duplicate` requires
   `target_observation_id IS NOT NULL` and `<> observation_id`; all other actions require
   `target_observation_id IS NULL`. Phase 1 excludes suppressed observations via NOT EXISTS
   (NULL-safe; NOT IN fails on NULLs).
9. **Corrected-value resolutions are single-active-per-observation.** Migration 004 adds 4 CHECK
   constraints enforcing valid shape for `attach_corrected_value` (non-null `observation_id`,
   `target_type = 'observation'`, non-null non-negative `corrected_value`) and a unique partial
   index `idx_fte_resolutions_single_active_correction` on
   `(practice_id, observation_id, action) WHERE is_superseded = false AND action = 'attach_corrected_value'`.
   This makes the `LIMIT 1` in Phases 3, 4, and 5c's correlated subqueries deterministic
   rather than advisory. `fte_observations` rows are never mutated — the correction lives
   exclusively in `fte_review_resolutions`. See `reconciler/README.md §4` for the full
   correction model.
10. **dismiss_short_pay resolutions require a stable claim anchor and position target.**
   Migration 005 adds two CHECK constraints: `fte_review_resolutions_dismiss_shortpay_needs_claim_id`
   (`claim_id IS NOT NULL` when `action = 'dismiss_short_pay'`) and
   `fte_review_resolutions_dismiss_shortpay_needs_position_type` (`target_type = 'position'` when
   `action = 'dismiss_short_pay'`). The `claim_id` anchor is required because `fte_financial_positions`
   rows are deleted by Phase 0 — `source_position_id` becomes stale after each reprocess, while
   `claim_id` is a hard FK to `fte_claims` (never deleted). When a non-superseded `dismiss_short_pay`
   row exists for a claim, the reconciler suppresses Phase 7 queue routing and Phase 8
   `short_pay_detected` event emission for that claim only. The `fte_financial_positions` row
   retains `reconciliation_status = 'unbalanced'` and the correct `open_balance_amount` —
   the dismissal is operational, not mathematical; financial truth is preserved.
   See `reconciler/README.md §5`.
11. **confirm_short_pay resolutions require a stable claim anchor, position target, and
   conflict prevention.** Migration 006 adds two CHECK constraints —
   `fte_review_resolutions_confirm_shortpay_needs_claim_id` (`claim_id IS NOT NULL` when
   `action = 'confirm_short_pay'`) and `fte_review_resolutions_confirm_shortpay_needs_position_type`
   (`target_type = 'position'` when `action = 'confirm_short_pay'`) — and a partial unique index
   `idx_fte_resolutions_single_active_position_short_pay` on `(practice_id, claim_id) WHERE
   is_superseded = false AND action IN ('confirm_short_pay', 'dismiss_short_pay')`. The partial
   unique index prevents simultaneous active rows of both actions for the same claim: once one
   is active, inserting the other raises `unique_violation`. To switch: set `is_superseded = true`
   on the current row, then insert the new one. When a non-superseded `confirm_short_pay` row
   exists for a claim, the reconciler suppresses Phase 7 queue routing only — the
   `short_pay_detected` event (Phase 8) remains emitted so downstream recovery workflows can act
   on it. The `fte_financial_positions` row retains `reconciliation_status = 'unbalanced'` and
   the correct `open_balance_amount` — financial truth is preserved.
   See `reconciler/README.md §5.6–§5.10`.
13. **`request_more_evidence` resolutions enforce shape and uniqueness.** Migration 007
   adds three CHECK constraints to `fte_review_resolutions` for `action = 'request_more_evidence'`:
   `fte_review_resolutions_rme_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
   whitespace-only notes are rejected because they are not actionable), `fte_review_resolutions_rme_needs_claim_id`
   (`claim_id IS NOT NULL` — required because `fte_financial_positions` rows are deleted by
   Phase 0; only `claim_id`, a hard FK to `fte_claims`, is a stable anchor), and
   `fte_review_resolutions_rme_needs_position_type` (`target_type = 'position'`). A partial
   unique index `idx_fte_resolutions_single_active_evidence_request` on `(practice_id, claim_id)
   WHERE is_superseded = false AND action = 'request_more_evidence'` prevents multiple
   simultaneous active evidence requests for the same claim. Supersede the existing row and
   insert a new one to update an active request. **The reconciler is entirely unaffected:**
   no phase reads or acts on `request_more_evidence`; the row is loaded into
   `_fte_active_resolutions` by Phase 0.5 (all non-superseded rows are loaded
   unconditionally) but no downstream phase changes derived position status, emits a claim
   event, or suppresses queue routing as a result. The claim retains its reconciler-derived
   `reconciliation_status` (`in_review` or `unbalanced`) across reruns. This differs from
   `dismiss_short_pay` (suppresses Phase 7 routing and Phase 8 event) and `confirm_short_pay`
   (suppresses Phase 7 routing) — see `reconciler/README.md §5.12`.
12. **`balanced` remains event-derived.** `reconciliation_status = 'balanced'`
   means the reconciler derived a zero open balance from claim events (Phase 6
   rule 4). Every `balanced` position is traceable to events and verifiable from
   the event ledger alone. Reviewer decisions may suppress workflow routing
   (`dismiss_short_pay`, `confirm_short_pay`) or promote an ambiguous payment
   event to `reconciled` (`confirm_payment_event`) — but no reviewer action
   should silently rewrite a position's status to `balanced` without the
   reconciler deriving it from events. `confirm_position_balanced` (present in
   the migration 002 action vocabulary) is intentionally deferred: zero-event
   claims have unknown math (NULL monetary fields), not zero math, and
   ambiguous-event balanced claims are correctly handled by `confirm_payment_event`.
   See `reconciler/README.md §5.11` for the deferral rationale and future
   implementation options.
14. **`mark_position_needs_correction` resolutions enforce shape and uniqueness,
   with no reconciler change.** Migration 008 adds three CHECK constraints to
   `fte_review_resolutions` for `action = 'mark_position_needs_correction'`:
   `fte_review_resolutions_mpnc_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
   whitespace-only notes are rejected; a correction-needed marker without an
   actionable explanation is not useful), `fte_review_resolutions_mpnc_needs_claim_id`
   (`claim_id IS NOT NULL` — `fte_financial_positions` rows are deleted by Phase 0;
   only `claim_id`, a hard FK to `fte_claims`, is a stable anchor), and
   `fte_review_resolutions_mpnc_needs_position_type` (`target_type = 'position'`).
   A partial unique index `idx_fte_resolutions_single_active_correction_needed` on
   `(practice_id, claim_id) WHERE is_superseded = false AND action = 'mark_position_needs_correction'`
   prevents multiple simultaneous active markers for the same claim. **The reconciler is
   entirely unaffected:** no phase reads or acts on `mark_position_needs_correction`; the
   row is loaded into `_fte_active_resolutions` by Phase 0.5 (all non-superseded rows are
   loaded unconditionally) but no downstream phase changes derived position status, emits a
   claim event, or suppresses queue routing as a result. The claim retains its
   reconciler-derived `reconciliation_status` (`in_review` or `unbalanced`) and its review
   queue entry across reruns regardless of any active correction-needed marker. Phase 7
   queue routing is NOT suppressed (contrast: `dismiss_short_pay` and `confirm_short_pay`
   both suppress Phase 7). Phase 8 `short_pay_detected` event emission is NOT suppressed
   (contrast: `dismiss_short_pay` suppresses Phase 8; `confirm_short_pay` does not). This
   differs from `request_more_evidence` only in intent — both are durable workflow notes
   with identical reconciler impact (none). See `reconciler/README.md §5.13`.
15. **`mark_position_resolved` resolutions enforce shape and uniqueness and suppress Phase 7
   queue routing for `unbalanced` positions only.** Migration 009 adds three CHECK constraints
   to `fte_review_resolutions` for `action = 'mark_position_resolved'`:
   `fte_review_resolutions_mpr_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
   the reviewer's rationale is required; a resolved marker without an explanation is not
   auditable), `fte_review_resolutions_mpr_needs_claim_id` (`claim_id IS NOT NULL` —
   `fte_financial_positions` rows are deleted by Phase 0; only `claim_id`, a hard FK to
   `fte_claims`, is a stable anchor), and `fte_review_resolutions_mpr_needs_position_type`
   (`target_type = 'position'`). A partial unique index
   `idx_fte_resolutions_single_active_position_resolved` on `(practice_id, claim_id)
   WHERE is_superseded = false AND action = 'mark_position_resolved'` prevents multiple
   simultaneous active resolved markers for the same claim. **Phase 7 only:** an active
   `mark_position_resolved` for a claim whose position has
   `reconciliation_status = 'unbalanced'` suppresses the `unbalanced_financial_position`
   review queue row in Phase 7 (added to the existing IN-list alongside `dismiss_short_pay`
   and `confirm_short_pay`). `in_review` positions are never suppressed — the Phase 7
   suppression guard explicitly checks `reconciliation_status = 'unbalanced'`; unknown or
   ambiguous financial state must remain visible. **Phase 8 preserved:** the
   `short_pay_detected` event is not suppressed by `mark_position_resolved` (contrast:
   `dismiss_short_pay` suppresses Phase 8; `confirm_short_pay` and `mark_position_resolved`
   both preserve it). `reconciliation_status` and `open_balance_amount` are unchanged —
   financial truth is preserved; only queue visibility is affected. To replace an active
   resolved marker: UPDATE SET `is_superseded = true`, then INSERT a new row.
   See `reconciler/README.md §5.14`.
16. **`reject_payment_event` resolutions enforce shape, require an actionable note, and
   suppress Phase 5c payment-event emission only.** Migration 010 adds three CHECK
   constraints and one cross-action partial unique index to `fte_review_resolutions` for
   `action = 'reject_payment_event'`:
   `fte_review_resolutions_rpe_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
   whitespace-only notes rejected; rejecting a payment event is a financially significant
   decision that requires an actionable explanation),
   `fte_review_resolutions_rpe_needs_claim_id` (`claim_id IS NOT NULL` — `fte_claim_events`
   rows are deleted and re-derived on every Phase 0 reset; `source_claim_event_id` becomes
   stale after each reprocess; `claim_id` is a hard FK to `fte_claims`, which Phase 0 never
   deletes, and is the only stable anchor for payment-event-level decisions — mirrors the
   `claim_id` anchor rationale for position-level actions in migrations 005–009), and
   `fte_review_resolutions_rpe_needs_payment_event_type` (`target_type = 'payment_event'` —
   targets a claim event, not an observation or position row). A cross-action partial unique
   index `idx_fte_resolutions_single_active_payment_event_decision` on `(practice_id, claim_id)
   WHERE is_superseded = false AND action IN ('confirm_payment_event', 'reject_payment_event')`
   prevents simultaneous active rows of both actions for the same claim — they are logically
   contradictory (confirming a payment while also rejecting it). This mirrors the cross-action
   index pattern from migration 006 (`confirm_short_pay` + `dismiss_short_pay`). To switch:
   UPDATE the current active row `SET is_superseded = true`, then INSERT the new action.
   **Reconciler behavior per phase:** Phase 1 is unchanged — the payment observation retains
   `classification = 'trusted'`; the observation is kept in the ledger as evidence; only
   Phase 5c event emission is suppressed. Phase 0.5 builds a second temp table
   `_fte_rejected_payment_event_claims ON COMMIT DROP` (set of `DISTINCT claim_id` from active
   `reject_payment_event` rows); Phase 5c checks this table before each `payment_applied`
   INSERT and `CONTINUE`s (skips the INSERT) when the claim matches. Phase 6 open_balance
   recalculates as `GREATEST(0, billed − adj − 0)` — the full billed amount — because no
   paid amount was applied. Phase 7 is NOT suppressed — the claim remains `unbalanced` and
   the `unbalanced_financial_position` queue row still emits. Phase 8 is NOT suppressed —
   `short_pay_detected` still emits, with `open_balance_amount` equal to the full billed
   amount. Supersede the row to restore payment-event emission on the next reconciler run.
   See `reconciler/README.md §5.15`.
17. **`assert_check_identity` resolutions enforce shape and store a canonical check number,
   but do not change reconciler behavior.** Migration 011 adds five CHECK constraints and one
   single-action partial unique index to `fte_review_resolutions` for
   `action = 'assert_check_identity'`:
   `fte_review_resolutions_aci_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
   asserting a canonical check identity is a financially significant decision that requires
   an auditable explanation; whitespace-only notes are rejected),
   `fte_review_resolutions_aci_needs_claim_id` (`claim_id IS NOT NULL` — `fte_claim_events`
   rows are deleted and re-derived on every Phase 0 reset; `source_claim_event_id` becomes
   stale after each reprocess; `claim_id` is a hard FK to `fte_claims`, which Phase 0 never
   deletes, and provides claim context for the assertion — mirrors the `claim_id` anchor
   rationale for position-level actions in migrations 005–009 and the other payment-event-level
   action in migration 010),
   `fte_review_resolutions_aci_needs_observation_id` (`observation_id IS NOT NULL` — a claim
   may later have multiple payment observations, so the reviewer must anchor to a specific
   observed payment row; `observation_id` is a FK to `fte_observations`, the evidence layer
   that Phase 0 never deletes, making it the stable, fine-grained payment-observation anchor;
   `claim_id` gives claim context, `observation_id` gives the specific payment anchor),
   `fte_review_resolutions_aci_needs_corrected_identifier`
   (`corrected_identifier IS NOT NULL AND btrim(corrected_identifier) <> ''` — the assertion
   without a canonical value is meaningless; the `corrected_identifier` column, added in
   migration 002, exists specifically to hold the reviewer-supplied canonical check number;
   asserting identity without providing the corrected value is not a complete resolution), and
   `fte_review_resolutions_aci_needs_payment_event_type` (`target_type = 'payment_event'` —
   targets a payment event check/EFT identifier, not an observation or position row; consistent
   with the other two payment-event-level actions). A single-action partial unique index
   `idx_fte_resolutions_single_active_check_identity_observation` on `(practice_id, observation_id)
   WHERE is_superseded = false AND action = 'assert_check_identity'` prevents duplicate
   simultaneous active assertions for the same payment observation. Per-observation (not
   per-claim) uniqueness is correct because a claim may have multiple payment observations —
   each may independently need an identity assertion. Unlike the cross-action index in
   migration 010 (`confirm_payment_event` + `reject_payment_event`), this is a single-action
   index because `assert_check_identity` has no logically contradictory counterpart — an
   active assertion coexists correctly with an active `confirm_payment_event` or
   `reject_payment_event`. **Reconciler behavior: UNCHANGED.** Phase 0.5 loads the row into
   `_fte_active_resolutions` (all non-superseded rows are loaded unconditionally) but no
   downstream phase acts on it. The `payment_applied` event still emits; `open_balance_amount`
   and `reconciliation_status` are not modified; Phase 7 queue routing is not suppressed;
   Phase 8 `short_pay_detected` event is not suppressed. `review_resolutions_applied`
   increments by 1 (reporting counter only). The corrected_identifier is stored in
   `fte_review_resolutions` as a durable reviewer note for human review and future phase
   integration (e.g., a future Phase 5c variant that substitutes the canonical check number
   when grouping payment events). To replace an active assertion: UPDATE SET
   `is_superseded = true`, then INSERT a new row with the revised `corrected_identifier`.
   See `reconciler/README.md §5.16`.

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
# Apply schema migrations in order
psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql
psql "$DATABASE_URL" -f migrations/003_add_observation_resolution_target.sql
psql "$DATABASE_URL" -f migrations/004_corrected_value_constraints.sql
psql "$DATABASE_URL" -f migrations/005_dismiss_short_pay_constraints.sql
psql "$DATABASE_URL" -f migrations/006_confirm_short_pay_constraints.sql
psql "$DATABASE_URL" -f migrations/007_request_more_evidence_constraints.sql
psql "$DATABASE_URL" -f migrations/008_mark_position_needs_correction_constraints.sql
psql "$DATABASE_URL" -f migrations/009_mark_position_resolved_constraints.sql
psql "$DATABASE_URL" -f migrations/010_reject_payment_event_constraints.sql

# Register the reconciler
psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql

# Load optional synthetic fixtures (both practices, or just one)
psql "$DATABASE_URL" -f fixtures/synthetic_ccdbe216_failure_modes.sql
psql "$DATABASE_URL" -f fixtures/synthetic_96c5c357_failure_modes.sql

# Run validation suites (each wraps in ROLLBACK; nothing persists)
psql "$DATABASE_URL" -f tests/validate_schema.sql
psql "$DATABASE_URL" -f tests/validate_reconciler.sql
psql "$DATABASE_URL" -f tests/validate_review_resolution.sql       # ccdbe216 fixture required
psql "$DATABASE_URL" -f tests/validate_observation_resolution.sql  # ccdbe216 fixture required
psql "$DATABASE_URL" -f tests/validate_corrected_value.sql                  # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_corrected_value_supersession.sql      # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_corrected_contractual_adjustment.sql  # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_corrected_billed_amount.sql           # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_dismiss_short_pay.sql                 # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_confirm_short_pay.sql                 # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_request_more_evidence.sql             # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_mark_position_needs_correction.sql    # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_mark_position_resolved.sql            # 96c5c357 fixture required
psql "$DATABASE_URL" -f tests/validate_reject_payment_event.sql              # 96c5c357 fixture required
```

Use the Supabase `service_role` / `postgres` connection. For the Supabase SQL Editor,
load fixture files manually before running test bodies (the SQL Editor does not support
`\i`). See `tests/RUNBOOK.md` for the complete run order.

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

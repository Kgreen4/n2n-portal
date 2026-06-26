# Financial Truth Engine — Clean Start Next Steps

**Status:** Execution checklist  
**Created:** 2026-06-17  
**Updated:** 2026-06-24
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

### Task 005B — confirm_short_pay Position-Level Review Resolution ✅ Complete

**Delivered:**
- `migrations/006_confirm_short_pay_constraints.sql` — two CHECK constraints
  (`fte_review_resolutions_confirm_shortpay_needs_claim_id`: `claim_id IS NOT NULL`
  and `fte_review_resolutions_confirm_shortpay_needs_position_type`: `target_type = 'position'`)
  and a partial unique index `idx_fte_resolutions_single_active_position_short_pay` on
  `(practice_id, claim_id) WHERE is_superseded = false AND action IN ('confirm_short_pay',
  'dismiss_short_pay')` — prevents simultaneous active rows of both actions for the same claim.
- `reconciler/fte_reconcile.sql` (Phase 7 updated) — Phase 7 guard extended from
  `ar.action = 'dismiss_short_pay'` to `ar.action IN ('dismiss_short_pay', 'confirm_short_pay')`:
  both actions suppress the `unbalanced_financial_position` queue row. Phase 8 guard is
  **unchanged** — `short_pay_detected` event is still suppressed for `dismiss_short_pay` only;
  `confirm_short_pay` leaves the event in place so downstream recovery workflows remain active.
  Phase 6 math and `fte_financial_positions` row (`reconciliation_status = 'unbalanced'`,
  correct `open_balance_amount`) are preserved — the confirmation is operational, not mathematical.
- `tests/validate_confirm_short_pay.sql` — 10-check validation suite (wrapped in ROLLBACK).
  Verifies: baseline `short_pay_detected` emitted and queue row present (checks 1–3);
  after confirm: `review_resolutions_applied=1`, queue row absent, `short_pay_detected`
  **preserved**, position `unbalanced`/`open_balance_amount=1248.11` (checks 4–7);
  conflict-prevention — `dismiss_short_pay` insert raises `unique_violation` while
  `confirm_short_pay` is active (check 8); CLM-APC-2000 unaffected (check 9);
  supersession restores the queue row while `short_pay_detected` remains (check 10).
- `tests/RUNBOOK.md` (updated) — added `validate_confirm_short_pay.sql` to suite table
  (10 checks), fixture dependency table (96c5c357), and Supabase manual run sequence
  (step 12); migration 006 added to first-time setup; updated total 81→91.
- `tests/run_all_validations.sql` (updated) — added `\i tests/validate_confirm_short_pay.sql`
  after dismiss suite; updated expected count 81→91.
- `reconciler/README.md` (updated) — Phase 7/8 table rows document both actions; §5.6–§5.10
  added (confirm_short_pay overview, stable anchor rationale, supersession, migration 006
  constraints table, validation suite table).
- `README.md` (updated) — status line, capabilities bullet, suite table (10 suites),
  numeric check count 81→91.
- `README_SCHEMA.md` (updated) — migration header, status line, Invariant #11
  (confirm_short_pay shape constraints, conflict-prevention index, Phase 7/8 behavior,
  math preservation), How To Apply psql block (migration 006 + validate_confirm_short_pay.sql).
- `NEXT_STEPS.md` (this file) — Task 005B entry and Current Capabilities/Suites updates.

**Safety:** no PHI, no real patient data, no production data, no legacy EOB DB or
code accessed. No new `reconciliation_status` values. No new claim event types.
No fixture files modified. No forbidden files touched. Phase 8 `short_pay_detected`
event remains emitted — financial truth and downstream recovery workflows preserved.
Conflict-prevention index enforces at-most-one active short-pay decision per claim at
the DB level. Tested via ROLLBACK-wrapped suite against synthetic 96c5c357 fixture in a
disposable Supabase project.

---

### Task 005D — `request_more_evidence` Durable Evidence-Needed Workflow ✅ Complete

**Delivered:**
- `migrations/007_request_more_evidence_constraints.sql` — 3 CHECK constraints and 1
  partial unique index enforcing valid shape for `request_more_evidence`:
  `fte_review_resolutions_rme_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
  whitespace-only notes are rejected, an actionable explanation is required);
  `fte_review_resolutions_rme_needs_claim_id` (`claim_id IS NOT NULL` — `fte_financial_positions`
  rows are deleted by Phase 0; only `claim_id`, a hard FK to `fte_claims`, is a stable anchor);
  `fte_review_resolutions_rme_needs_position_type` (`target_type = 'position'`); and
  `idx_fte_resolutions_single_active_evidence_request` on `(practice_id, claim_id)
  WHERE is_superseded = false AND action = 'request_more_evidence'` — at most one active
  evidence request per claim.
- `tests/validate_request_more_evidence.sql` — 12-check validation suite (wrapped in
  ROLLBACK). Verifies: baseline CLM-APC-2000 `in_review` and queued (checks 1–3); after
  insert of valid evidence request: `review_resolutions_applied=1`, CLM-APC-2000 still
  `in_review`, still queued, zero events, all monetary fields NULL (checks 4–8); duplicate
  active evidence request raises `unique_violation` (check 9); NULL notes and blank notes
  raise `check_violation` (check 10); NULL `claim_id` and `target_type='observation'` both
  raise `check_violation` (check 11); CLM-APC-1000 isolation — unbalanced, `short_pay_detected`
  event preserved, queued with `unbalanced_financial_position` (check 12). All 12 checks
  target the 96c5c357 fixture.
- `tests/run_all_validations.sql` (updated) — `\i tests/validate_request_more_evidence.sql`
  added after confirm suite; expected check count 91 → 103.
- `tests/RUNBOOK.md` (updated) — `validate_request_more_evidence.sql` added to suite table
  (12 checks), fixture dependency table (96c5c357), first-time-setup psql and Supabase
  sequences (migration 007 + validation step 13); total 91 → 103; "ten suites" → "eleven suites".
- `README.md` (updated) — status line, capabilities bullet (`request_more_evidence`),
  suite table (11 suites), numeric check count 91 → 103.
- `README_SCHEMA.md` (updated) — migration header, status line, Invariant 13
  (migration 007 constraints, stable-anchor rationale, reconciler-unchanged invariant,
  difference from dismiss/confirm), How To Apply psql block (migration 007 +
  validate_request_more_evidence.sql).
- `reconciler/README.md` (updated) — §5.12 `request_more_evidence` added: durable-note
  behavior, reconciler-unchanged detail per phase, shape constraints table,
  `claim_id`-anchor rationale, partial-unique-index rationale (no cross-action conflict),
  supersession workflow with annotated SQL.
- `NEXT_STEPS.md` (this file) — Task 005D entry and Current Capabilities/Suites updates.

**Reconciler behavior: NONE.** No reconciler phase was modified. No frozen file was
touched. `request_more_evidence` is a durable workflow note only: the row is loaded by
Phase 0.5 (incrementing `review_resolutions_applied`) but no downstream phase acts on it.
Phase 7 queue routing is NOT suppressed (contrast with `dismiss_short_pay` + `confirm_short_pay`).
Phase 8 `short_pay_detected` event is NOT suppressed. Position `reconciliation_status`
is unchanged. Financial math is unchanged.

**Safety:** no PHI, no real patient data, no production data, no legacy EOB DB or code
accessed. No new `reconciliation_status` values. No new claim event types. No fixture
files modified. No forbidden files touched (migrations 001–006, reconciler, fixtures,
all existing test files, and all CODEX_TASK_*.md files are unchanged). All shape
enforcement is DB-level (migration 007 constraints + partial unique index). Tested via
ROLLBACK-wrapped 12-check suite against synthetic 96c5c357 fixture in a disposable
Supabase project.

---

### Task 005E — `mark_position_needs_correction` Shape Constraints and Validation ✅ Complete

**Delivered:**
- `migrations/008_mark_position_needs_correction_constraints.sql` — 3 CHECK constraints
  and 1 partial unique index enforcing valid shape for `mark_position_needs_correction`:
  `fte_review_resolutions_mpnc_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
  whitespace-only notes rejected), `fte_review_resolutions_mpnc_needs_claim_id` (`claim_id
  IS NOT NULL` — stable anchor, `source_position_id` goes stale on Phase 0), and
  `fte_review_resolutions_mpnc_needs_position_type` (`target_type = 'position'`). Partial
  unique index `idx_fte_resolutions_single_active_correction_needed` on
  `(practice_id, claim_id) WHERE is_superseded = false AND action =
  'mark_position_needs_correction'` prevents simultaneous duplicate active markers for the
  same claim.
- `tests/validate_mark_position_needs_correction.sql` — 12-check ROLLBACK-wrapped suite.
  CLM-APC-1000 (unbalanced, `short_pay_detected`, queued) as primary vehicle — proves Phase
  7 and Phase 8 are NOT suppressed (checks 7 and 8). CLM-APC-2000 as shape-violation and
  isolation target. Checks: baseline state (1–4), valid insert + reconciler rerun (5–8),
  unique-violation on second active marker (9), notes/claim_id/target_type constraint
  violations (10–11), CLM-APC-2000 isolation after all attempts (12). Brings total to 115
  numeric checks across 12 suites.
- `tests/run_all_validations.sql` (updated) — added `\i
  tests/validate_mark_position_needs_correction.sql` block; updated header prerequisites
  to include migrations 005–008; updated expected count from 103 to 115 and from eleven to
  twelve suites.
- `tests/RUNBOOK.md` (updated) — added suite table row, fixture dependency row, migration
  008 to first-time setup (psql + Supabase SQL Editor), step 14 to Supabase manual
  sequence, updated totals to 115 / twelve suites.
- `README.md` (updated) — status line Tasks 001–005E, seventh action category bullet
  (`mark_position_needs_correction`), suite table row, check count 103→115 / 11→12 suites.
- `README_SCHEMA.md` (updated) — migrations frontmatter (migration 008 appended), status
  line Tasks 001–005E, Invariant 14 (`mark_position_needs_correction` shape/uniqueness
  constraints and reconciler-unchanged behavior — Phase 7 NOT suppressed, Phase 8 NOT
  suppressed, explicit contrast with `dismiss_short_pay` and `confirm_short_pay`),
  How-To-Apply psql block (migration 008 + `validate_mark_position_needs_correction.sql`).
- `reconciler/README.md` (updated) — §5.13 `mark_position_needs_correction` added: durable
  correction-needed marker behavior, reconciler-unchanged detail per phase, DB constraints
  table (4 rows for migration 008), `claim_id`-anchor rationale, partial-unique-index
  rationale (no cross-action conflict partner), supersession workflow with annotated SQL,
  coexistence note (coexists with `request_more_evidence`, `dismiss_short_pay`,
  `confirm_short_pay`), validation suite reference.
- `NEXT_STEPS.md` (this file) — Task 005E entry and Current Capabilities/Suites updates.

**Reconciler behavior: NONE.** No reconciler phase was modified. No frozen file was
touched. `mark_position_needs_correction` is a durable workflow note only: the row is
loaded by Phase 0.5 (incrementing `review_resolutions_applied`) but no downstream phase
acts on it. Phase 7 queue routing is NOT suppressed (contrast with `dismiss_short_pay` +
`confirm_short_pay` which both suppress Phase 7). Phase 8 `short_pay_detected` event is
NOT suppressed (contrast with `dismiss_short_pay` which suppresses Phase 8). Position
`reconciliation_status` is unchanged. Financial math is unchanged.

**Safety:** no PHI, no real patient data, no production data, no legacy EOB DB or code
accessed. No new `reconciliation_status` values. No new queue reason values. No new claim
event types. No fixture files modified. No frozen files touched (migrations 001–007,
reconciler, fixtures, all existing test files, and all CODEX_TASK_*.md files are
unchanged). All shape enforcement is DB-level (migration 008 constraints + partial unique
index). Tested via ROLLBACK-wrapped 12-check suite against synthetic 96c5c357 fixture in a
disposable Supabase project.

---

### Task 005F — `mark_position_resolved` Phase 7 Queue Suppression ✅ Complete

**Delivered:**
- `migrations/009_mark_position_resolved_constraints.sql` — 3 CHECK constraints and 1
  partial unique index enforcing valid shape for `mark_position_resolved`:
  `fte_review_resolutions_mpr_needs_notes` (`notes IS NOT NULL AND btrim(notes) <> ''` —
  whitespace-only notes rejected), `fte_review_resolutions_mpr_needs_claim_id` (`claim_id
  IS NOT NULL` — stable anchor, `source_position_id` goes stale on Phase 0), and
  `fte_review_resolutions_mpr_needs_position_type` (`target_type = 'position'`). Partial
  unique index `idx_fte_resolutions_single_active_position_resolved` on
  `(practice_id, claim_id) WHERE is_superseded = false AND action =
  'mark_position_resolved'` prevents simultaneous duplicate active resolved markers for
  the same claim.
- `reconciler/fte_reconcile.sql` (Phase 7 only modified) — `'mark_position_resolved'`
  added to the suppression IN-list alongside `'dismiss_short_pay'` and
  `'confirm_short_pay'` in the `unbalanced` position guard. No other phase touched.
- `tests/validate_mark_position_resolved.sql` — 14-check ROLLBACK-wrapped suite. CLM-APC-1000
  (unbalanced, `short_pay_detected`, queued) as primary vehicle — proves Phase 7 IS
  suppressed (check 9) and Phase 8 is preserved (check 8) when `mark_position_resolved`
  is active. CLM-APC-2000 as `in_review` invariant and shape-violation target. Checks:
  baseline state (1–5), valid insert + reconciler rerun (6–10), unique-violation on second
  active marker (11), notes/claim_id/target_type constraint violations (12–13), supersession
  + in_review invariant (14). Brings total to 129 numeric checks across 13 suites.
- `tests/run_all_validations.sql` (updated) — added `\i tests/validate_mark_position_resolved.sql`
  block; updated header prerequisites to include migration 009; updated expected count
  from 115 to 129 and from twelve to thirteen suites.
- `tests/RUNBOOK.md` (updated) — added suite table row (14 checks), fixture dependency row
  (96c5c357), migration 009 to first-time setup (psql + Supabase SQL Editor step 9,
  reconciler shifted to step 10), step 15 to Supabase manual sequence, updated totals to
  129 / thirteen suites.
- `README.md` (updated) — status line Tasks 001–005F, eighth action category bullet
  (`mark_position_resolved`), suite table row, check count 115→129 / 12→13 suites.
- `README_SCHEMA.md` (updated) — migrations frontmatter (migration 009 appended), status
  line Tasks 001–005F, Invariant 15 (`mark_position_resolved` Phase 7 suppression for
  `unbalanced` only, Phase 8 preserved, `in_review` invariant, notes/claim_id/target_type
  constraints, uniqueness, supersession, no financial math changes, migration 009),
  How-To-Apply psql block (migration 009 + `validate_mark_position_resolved.sql`).
- `reconciler/README.md` (updated) — Phase 7 table row extended to mention
  `mark_position_resolved` in the suppression IN-list; §5.14 `mark_position_resolved`
  added: Phase 7-only behavior per phase, DB constraints table (migration 009), `claim_id`
  anchor rationale, partial-unique-index rationale (no cross-action conflict partner),
  supersession workflow SQL, contrast table with all other position-level actions,
  validation suite reference.
- `NEXT_STEPS.md` (this file) — Task 005F entry and Current Capabilities/Suites updates.

**Reconciler behavior: Phase 7 only.** `reconciler/fte_reconcile.sql` was modified in
Phase 7 only — `'mark_position_resolved'` added to the `IN('dismiss_short_pay',
'confirm_short_pay', 'mark_position_resolved')` IN-list in the `unbalanced` position
suppression guard. No other phase was touched. Phase 6 math (`reconciliation_status`,
`open_balance_amount`) is unchanged. Phase 8 `short_pay_detected` event emission is
unchanged (not suppressed). `in_review` positions are never suppressed — the guard checks
`fp.reconciliation_status <> 'unbalanced'` first. Financial truth is preserved.

**Safety:** no PHI, no real patient data, no production data, no legacy EOB DB or code
accessed. No new `reconciliation_status` values. No new queue reason values. No new claim
event types. No fixture files modified. No frozen files touched (migrations 001–008,
fixtures, all existing test files, and all CODEX_TASK_*.md files are unchanged). All
shape enforcement is DB-level (migration 009 constraints + partial unique index). Tested
via ROLLBACK-wrapped 14-check suite against synthetic 96c5c357 fixture in a disposable
Supabase project.

---

### Task 005C — `confirm_position_balanced` — Planning/Docs Only ✅ Decision Recorded

**Decision: deferred. `confirm_position_balanced` is not implemented.**

**Rationale:**

`reconciliation_status = 'balanced'` currently has exactly one meaning:
the reconciler derived a zero open balance from claim events (Phase 6 rule 4).
Implementing `confirm_position_balanced` as a reviewer position-level assertion
would make `balanced` mean either (a) event-derived math proves zero or (b)
reviewer asserted balanced without event math. Conflating the two weakens the
financial-truth invariant and makes positions no longer self-verifying from
the event ledger.

Two representative cases illustrate why the deferral is correct:

- **CLM-APC-2000 (96c5c357 fixture):** zero events, all monetary fields NULL.
  The claim is `in_review` because no trusted observations exist (all
  SUSPECT / retry-pending). The open balance is unknown, not zero. Marking
  it `balanced` via reviewer assertion would assert financial truth that cannot
  be verified from events.
- **CLM-AZ-0001 (ccdbe216 fixture):** ambiguous `payment_applied` event;
  math balances (720.00 − 209.60 − 510.40 = 0.00). Already resolvable by
  `confirm_payment_event`, which promotes the event to `reconciled` and lets
  the reconciler derive `balanced` from events on the next run. No
  position-level assertion needed.

**What was not changed:**

- No SQL modified.
- No migrations added.
- No tests added or modified.
- No fixtures modified.
- Validation total unchanged at 91 numeric checks across 10 suites.
- `confirm_position_balanced` remains in the migration 002 action vocabulary
  CHECK constraint — no constraint is needed for an unimplemented action.

**Documentation added (this task):**

- `reconciler/README.md §5.11` — deferral rationale, correct resolution paths,
  future implementation options (new status value, separate workflow state,
  new event/evidence model).
- `README_SCHEMA.md` Invariant 12 — "balanced remains event-derived" invariant.
- `README.md` — not-yet-implemented note for `confirm_position_balanced`.
- `NEXT_STEPS.md` (this entry) — decision record.

---

### Task 005A — dismiss_short_pay Position-Level Review Resolution ✅ Complete

**Delivered:**
- `migrations/005_dismiss_short_pay_constraints.sql` — two CHECK constraints
  enforcing valid shape for `dismiss_short_pay`:
  `fte_review_resolutions_dismiss_shortpay_needs_claim_id` (`claim_id IS NOT NULL`)
  and `fte_review_resolutions_dismiss_shortpay_needs_position_type`
  (`target_type = 'position'`). No new columns, no new indexes, no new action
  vocabulary — `dismiss_short_pay` was already in the migration 002 CHECK constraint.
- `reconciler/fte_reconcile.sql` (Phases 7 and 8 updated) — Phase 7 INSERT adds
  `AND (fp.reconciliation_status <> 'unbalanced' OR NOT EXISTS (SELECT 1 FROM
  _fte_active_resolutions ar WHERE ar.claim_id = fp.claim_id AND ar.action =
  'dismiss_short_pay'))` — suppresses queue entry for dismissed unbalanced claims;
  `in_review` positions are always routed. Phase 8 FOR loop adds `AND NOT EXISTS
  (SELECT 1 FROM _fte_active_resolutions ar WHERE ar.claim_id = fp.claim_id AND
  ar.action = 'dismiss_short_pay')` — suppresses `short_pay_detected` event for
  dismissed claims. `fte_financial_positions` rows retain `reconciliation_status =
  'unbalanced'` and correct `open_balance_amount` — the suppression is operational,
  not mathematical; financial truth is preserved.
- `tests/validate_dismiss_short_pay.sql` — 9-check validation suite (wrapped in
  ROLLBACK). Verifies: baseline short_pay emitted and queue row exists (checks 1–3);
  after dismiss: `review_resolutions_applied=1`, event suppressed, queue row absent,
  position math preserved at `open_balance_amount=1248.11` (checks 4–7); CLM-APC-2000
  unaffected (check 8); supersession restores both the event and queue row (check 9).
- `tests/RUNBOOK.md` (updated) — added `validate_dismiss_short_pay.sql` to suite
  table (9 checks), fixture dependency table (96c5c357), and Supabase manual run
  sequence (step 11); updated total 72→81.
- `tests/run_all_validations.sql` (updated) — added `\i tests/validate_dismiss_short_pay.sql`
  after corrected-billed-amount suite; updated expected count 72→81.
- `reconciler/README.md` (updated) — Phase 7/8 table entries describe dismiss_short_pay
  suppression behavior; new §5 "Position-level resolution model" documents the
  dismiss_short_pay design (overview, stable claim_id anchor rationale, supersession
  workflow, migration 005 constraints, validation suite); old §5–7 renumbered to §7–9.
- `README.md` (updated) — status line, capabilities bullet (5 categories), suite table
  (9 suites), numeric check count 72→81.
- `README_SCHEMA.md` (updated) — migration header, status line, new invariant #10
  (position-level dismissal shape constraints and math-preservation guarantee).
- `NEXT_STEPS.md` (this file) — Task 005A entry and Current Capabilities/Suites updates.

**Safety:** no PHI, no real patient data, no production data, no legacy EOB DB or
code accessed. No new `reconciliation_status` values. No new claim event types.
No fixture files modified. No forbidden files touched. Position math is preserved
as financial truth — the dismissal is entirely operational (queue + event suppression
only). Tested via ROLLBACK-wrapped suite against synthetic 96c5c357 fixture in a
disposable Supabase project.

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

As of Task 005F complete (2026-06-25), the FTE can:

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
- **Dismiss a short pay.** `dismiss_short_pay` suppresses both the Phase 7 queue row
  and the Phase 8 `short_pay_detected` event for the claim. Position math preserved
  (`unbalanced`, correct `open_balance_amount`). Enforced by migration 005 constraints.
  Proven across 9 validation checks (Task 005A).
- **Confirm a short pay.** `confirm_short_pay` suppresses the Phase 7 queue row only —
  the `short_pay_detected` event remains emitted so recovery workflows stay active.
  A conflict-prevention partial unique index (migration 006) prevents simultaneous
  active `confirm_short_pay` + `dismiss_short_pay` for the same claim. Position math
  preserved. Proven across 10 validation checks (Task 005B).
- **Request more evidence.** `request_more_evidence` records a durable reviewer note
  that a claim cannot be resolved without additional external evidence. Requires a
  non-null, non-blank `notes` field (whitespace-only rejected) and a stable `claim_id`
  anchor. At most one active evidence request per claim (partial unique index, migration
  007). No reconciler phase is modified: claim retains its derived `reconciliation_status`
  (`in_review` or `unbalanced`); Phase 7 queue routing is NOT suppressed; no claim
  events emitted; financial math unchanged. Supersede the row to close the request and
  insert a substantive resolution. Proven across 12 validation checks (Task 005D).
- **Mark a position as needing correction.** `mark_position_needs_correction` records a
  durable reviewer note that a financial position contains an extraction or attribution
  error that must be corrected before the claim can be resolved. Requires a non-null,
  non-blank `notes` field and a stable `claim_id` anchor. At most one active correction
  marker per claim (partial unique index, migration 008). No reconciler phase is
  modified: claim retains its derived `reconciliation_status`; Phase 7 queue routing is
  NOT suppressed (contrast: `dismiss_short_pay` + `confirm_short_pay` both suppress Phase
  7); Phase 8 `short_pay_detected` event is NOT suppressed (contrast: `dismiss_short_pay`
  suppresses Phase 8); no claim events emitted; financial math unchanged. Supersede the
  row once the underlying correction is applied (e.g. via `attach_corrected_value` on the
  affected observation). Proven across 12 validation checks (Task 005E).
- **Mark a position as resolved.** `mark_position_resolved` records a durable reviewer
  decision that a position no longer needs generic queue routing — without committing to
  write it off (`dismiss_short_pay`) or actively pursue recovery (`confirm_short_pay`).
  Requires a non-null, non-blank `notes` field (the reviewer's rationale) and a stable
  `claim_id` anchor. At most one active resolved marker per claim (partial unique index,
  migration 009). Phase 7 queue routing is suppressed for `unbalanced` positions only
  (added to the `dismiss_short_pay` + `confirm_short_pay` IN-list); `in_review` positions
  are never suppressed — the guard is `unbalanced`-only. Phase 8 `short_pay_detected`
  event is preserved (contrast: `dismiss_short_pay` suppresses Phase 8). Position
  `reconciliation_status` and `open_balance_amount` are unchanged. Financial math is
  unchanged. Supersede the row to restore queue visibility. Proven across 14 validation
  checks (Task 005F).

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
| `tests/validate_dismiss_short_pay.sql` | 9 | `dismiss_short_pay` — Phase 7/8 suppression, math preserved, CLM-APC-2000 isolation, supersession |
| `tests/validate_confirm_short_pay.sql` | 10 | `confirm_short_pay` — Phase 7 suppression only, short_pay_detected preserved, conflict-prevention index, CLM-APC-2000 isolation, supersession |
| `tests/validate_request_more_evidence.sql` | 12 | `request_more_evidence` — durable note only, no reconciler/queue/event change, notes/claim_id/target_type shape constraints, uniqueness, CLM-APC-1000 isolation |
| `tests/validate_mark_position_needs_correction.sql` | 12 | `mark_position_needs_correction` — durable correction-needed marker, no reconciler/queue/event change, notes/claim_id/target_type shape constraints, uniqueness, CLM-APC-2000 isolation |
| `tests/validate_mark_position_resolved.sql` | 14 | `mark_position_resolved` — Phase 7 queue suppression for unbalanced only, Phase 8 preserved, in_review invariant, notes/claim_id/target_type shape constraints, uniqueness, supersession |

**Total numeric checks: 129** (structure checks in validate_schema.sql not counted)

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

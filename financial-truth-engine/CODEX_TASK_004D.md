# CODEX TASK 004D — Corrected-Value Review Resolutions

```
Task     : 004D
Title    : Corrected-value review resolutions
Branch   : feature/fte-task-004d-corrected-value-resolutions
Repo     : github.com/Kgreen4/n2n-portal  (financial-truth-engine/)
Depends  : Tasks 001 + 002 + 003 + 004B + 004C merged to main
Status   : APPROVED FOR IMPLEMENTATION
```

---

## §1  Scope

### What this task does

Adds DB-level constraints and a unique partial index for the
`attach_corrected_value` action (migration 004), then wires a correlated
subquery into Phase 5c of the reconciler so that a reviewer-supplied
`corrected_value` replaces the extracted `amount` when deriving
`payment_applied` events.

**Single action, single phase.**  Only `attach_corrected_value` is modified.
Only Phase 5c is modified.  No other reconciler logic changes.

### Supported resolution actions in scope

| Action | Target type | Where used |
|---|---|---|
| `attach_corrected_value` | `observation` | Phase 5c — `payment_applied` amount |

### Out of scope

- `confirm_observation`, `reject_observation`, `mark_duplicate`,
  `confirm_payment_event` — no changes to these paths
- `billed_amount` observations — correction applies to `payment` type only
- `contractual_adjustment` observations — no changes to Phase 4
- New claim event type — `payment_applied` is reused with corrected amount
- UI, API, or Edge Functions — SQL layer only
- Any change to `fte_observations` rows — source observations are immutable
- Any change to `migrations/001`, `002`, or `003`
- Any change to `tests/validate_reconciler.sql`,
  `tests/validate_review_resolution.sql`, or
  `tests/validate_observation_resolution.sql`
- Any change to fixture files other than the one cited in §3

---

## §2  Security constraints

The following rules apply to every file created or modified in this task.
They are non-negotiable and override any other convenience.

1. **No PHI** — synthetic fixtures only.  No real member IDs, real DOBs,
   real claim numbers, or production exports anywhere in this task.
2. **No raw PDFs** — evidence records may reference synthetic source
   documents only.  No PDF paths or content.
3. **No legacy EOB repo** — do not import, copy, or adapt code from
   `kgreen41-eob/cardio-metrics-saas`.
4. **No legacy Supabase project** — the FTE test environment is a
   disposable Supabase project; no connection to the production EOB project
   (`jdmyjdvricpyrsfchakk`).
5. **No direct production database access** — all testing is done by the
   human reviewer running SQL manually in the Supabase SQL editor; no
   credentials embedded in code or docs.
6. **Do not modify** during implementation:
   - `migrations/001_create_financial_truth_schema.sql`
   - `migrations/002_add_review_resolutions.sql`
   - `migrations/003_add_observation_resolution_target.sql`
   - `reconciler/fte_reconcile.sql` — Phase 0, 0.5, 2, 3, 4, 5 (late/retry),
     6, 7, 8 (only Phase 5c changes)
   - `tests/validate_reconciler.sql`
   - `tests/validate_review_resolution.sql`
   - `tests/validate_observation_resolution.sql`
   - `CODEX_TASK_001.md` through `CODEX_TASK_004C.md`
   - `AGENTS.md`

---

## §3  Fixture inventory — synthetic_96c5c357_failure_modes

Fixture file: `fixtures/synthetic_96c5c357_failure_modes.sql`
(already committed; do not modify)

Practice UUID: `96000000-0000-4000-8000-0000000000fe`

### Claims

| claim_number | claim_id |
|---|---|
| CLM-APC-1000 | `c1a90000-0000-4000-8000-000000001000` |
| CLM-APC-2000 | `c1a90000-0000-4000-8000-000000002000` |

### Observations

| short | full observation_id | type | amount | claim | classification | failure_mode | notes |
|---|---|---|---|---|---|---|---|
| a1 | `0b590000-0000-4000-8000-0000000000a1` | billed_amount | $1,600.00 | CLM-APC-1000 | trusted | — | source-of-truth billed |
| a2 | `0b590000-0000-4000-8000-0000000000a2` | payment | $351.89 | CLM-APC-1000 | trusted | — | CK#2-1835212; **correction target** |
| b1 | `0b590000-0000-4000-8000-0000000000b1` | payment | $982.34 | CLM-APC-2000 | suspect | check_spacing_variant_fragmentation | retry_pending=false |
| b2 | `0b590000-0000-4000-8000-0000000000b2` | payment | $39.84 | CLM-APC-2000 | suspect | check_spacing_variant_fragmentation | retry_pending=false |
| b3 | `0b590000-0000-4000-8000-0000000000b3` | check_eft_identifier | null | CLM-APC-2000 | excluded | check_spacing_variant_fragmentation | is_superseded=true, retry_pending=true |

### Evidence

| short | evidence_id | type | notes |
|---|---|---|---|
| ck-stub | `dddd9600-0000-4000-8000-0000000000ca` | check_payment | check #2-1835212 for CLM-APC-1000 |

### Fixture correction scenario

The reconciler extracted `$351.89` for obs a2 (check #2-1835212).  The
human reviewer determines this is incorrect: the claim was adjudicated at
100% of billed ($1,600.00) and the full payment was received.  The
reviewer inserts an `attach_corrected_value` resolution with
`corrected_value = 1600.00`.

---

## §4  Financial impact tracing

### Baseline run (no resolution)

| Phase | Action | Output |
|---|---|---|
| Phase 2 | b1, b2 → suspected_duplicate; b3 → late_retry_page_contradiction | 3 review queue entries |
| Phase 3 | a1 → claim_adjudicated, CLM-APC-1000, $1,600.00 | 1 event |
| Phase 5c | a2 → payment_applied, CLM-APC-1000, `COALESCE(NULL, $351.89)` = **$351.89** | 1 event |
| Phase 6 | CLM-APC-1000: billed=$1,600, paid=$351.89, open_balance=**$1,248.11**, status='**unbalanced**' | 1 position |
| Phase 6 | CLM-APC-2000: no events, open_balance=NULL, status='**in_review**' | 1 position |
| Phase 7 | CLM-APC-1000 (unbalanced) → unbalanced_financial_position | +1 queue entry |
| Phase 7 | CLM-APC-2000 (in_review) → unbalanced_financial_position | +1 queue entry |
| **Total** | | **review_queue = 5; review_resolutions_applied = 0** |

### After correction (corrected_value = $1,600.00 on obs a2)

| Phase | Action | Output |
|---|---|---|
| Phase 0.5 | Loads 1 active resolution: obs a2 / attach_corrected_value / $1,600.00 | review_resolutions_applied = 1 |
| Phase 2 | Same as baseline (b1, b2, b3 unaffected by correction) | 3 queue entries |
| Phase 3 | Same as baseline | 1 event |
| Phase 5c | a2 → correlated lookup finds corrected_value=$1,600.00; payment_applied = `COALESCE($1,600.00, $351.89)` = **$1,600.00** | 1 event |
| Phase 6 | CLM-APC-1000: billed=$1,600, paid=$1,600, open_balance=**$0.00**, status='**balanced**' | 1 position |
| Phase 6 | CLM-APC-2000: no events, open_balance=NULL, status='in_review' (unchanged) | 1 position |
| Phase 7 | CLM-APC-1000 is balanced → **not routed** | 0 entries for CLM-APC-1000 |
| Phase 7 | CLM-APC-2000 (in_review) → unbalanced_financial_position | +1 queue entry |
| **Total** | | **review_queue = 4; review_resolutions_applied = 1** |

### Queue delta

| Condition | Queue count | CLM-APC-1000 unbalanced_financial_position |
|---|---|---|
| No correction | 5 | present |
| corrected_value = $1,600.00 | 4 | absent |

Phase 6 recalculates `open_balance_amount` automatically from the corrected
`payment_applied` event — no additional logic required.

---

## §5  Files

| Operation | Path | Notes |
|---|---|---|
| **Create** | `migrations/004_corrected_value_constraints.sql` | 4 CHECK constraints + 1 unique partial index |
| **Modify** | `reconciler/fte_reconcile.sql` | Phase 5c only — correlated subquery + COALESCE |
| **Create** | `tests/validate_corrected_value.sql` | 11-check validation; uses 96c5c357 fixture |
| **Modify** | `README.md` | Add 004D row to task table; note corrected-value path |
| **Modify** | `README_SCHEMA.md` | Add unique partial index to fte_review_resolutions section |
| **Modify** | `NEXT_STEPS.md` | Mark 004D complete; advance next milestone |

---

## §6  Migration 004

File: `migrations/004_corrected_value_constraints.sql`

```sql
-- =============================================================================
-- Financial Truth Engine (FTE) — Corrected-Value Constraints
-- Migration: 004_corrected_value_constraints.sql
-- Depends on: 003_add_observation_resolution_target.sql
-- Created: 2026-06-23
--
-- WHAT THIS MIGRATION ADDS
-- -----------------------------------------------------------------------------
-- 1. Four CHECK constraints that enforce valid shape for the
--    attach_corrected_value action:
--
--    #1  observation_id IS NOT NULL   (the observation being corrected)
--    #2  target_type = 'observation'  (semantic type guard)
--    #3  corrected_value IS NOT NULL  (the replacement amount must be supplied)
--    #4  corrected_value >= 0         (negative corrections are invalid)
--
-- 2. Unique partial index on (practice_id, observation_id, action) filtered
--    to is_superseded = false AND action = 'attach_corrected_value'.
--    Enforces at most one active correction per observation at the DB level.
--    To supersede a correction: set is_superseded = true on the old row,
--    then insert a new row.  The index permits multiple historical corrections
--    (is_superseded = true) but rejects a second is_superseded = false row.
-- =============================================================================

begin;

-- CONSTRAINT #1: attach_corrected_value requires observation_id to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_obs_id
    check (
      action <> 'attach_corrected_value'
      OR observation_id IS NOT NULL
    );

-- CONSTRAINT #2: attach_corrected_value requires target_type = 'observation'.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_observation_type
    check (
      action <> 'attach_corrected_value'
      OR target_type = 'observation'
    );

-- CONSTRAINT #3: attach_corrected_value requires corrected_value to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_corrected_value
    check (
      action <> 'attach_corrected_value'
      OR corrected_value IS NOT NULL
    );

-- CONSTRAINT #4: attach_corrected_value corrected_value must be non-negative.
-- A zero correction is permitted (full write-off scenario).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_value_nonnegative
    check (
      action <> 'attach_corrected_value'
      OR corrected_value >= 0
    );

-- UNIQUE PARTIAL INDEX: at most one active correction per observation.
-- Permits any number of is_superseded = true historical rows for the same
-- observation; rejects a second is_superseded = false row.
create unique index idx_fte_resolutions_single_active_correction
  on fte_review_resolutions (practice_id, observation_id, action)
  where is_superseded = false
    and action = 'attach_corrected_value';

-- =============================================================================
-- End of migration 004.
-- =============================================================================

commit;
```

---

## §7  Reconciler changes

File: `reconciler/fte_reconcile.sql`
**Only Phase 5c changes.**  All other phases are untouched.

### Targeted edit — Phase 5c SELECT list and payment amount

#### BEFORE

```sql
  -- =========================================================================
  -- PHASE 5c: Emit payment_applied events from trusted payment observations.
  --
  -- Each payment event gets two fte_event_evidence links (both link_role=
  -- 'supports'):
  --   (1) the page observation that reported the payment
  --   (2) the check_payment evidence stub matched by check_eft_identifier
  --       (if a matching stub exists; the INSERT is a no-op if not found)
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'payment'
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'payment_applied', v_obs.service_date,
       v_obs.amount, 'paid', v_obs.payer_name,
       'payment', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;
```

#### AFTER

```sql
  -- =========================================================================
  -- PHASE 5c: Emit payment_applied events from trusted payment observations.
  --
  -- Each payment event gets two fte_event_evidence links (both link_role=
  -- 'supports'):
  --   (1) the page observation that reported the payment
  --   (2) the check_payment evidence stub matched by check_eft_identifier
  --       (if a matching stub exists; the INSERT is a no-op if not found)
  --
  -- corrected_amount: correlated subquery looks up any active
  -- attach_corrected_value resolution for this observation.  The unique
  -- partial index on fte_review_resolutions (migration 004) guarantees at
  -- most one active row, making LIMIT 1 deterministic rather than advisory.
  -- COALESCE falls back to the extracted amount when no correction exists,
  -- so existing behaviour is unchanged when no resolution is present.
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid,
      (SELECT ar.corrected_value
       FROM _fte_active_resolutions ar
       WHERE ar.observation_id = cl.id
         AND ar.action         = 'attach_corrected_value'
       LIMIT 1) AS corrected_amount
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'payment'
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'payment_applied', v_obs.service_date,
       COALESCE(v_obs.corrected_amount, v_obs.amount), 'paid', v_obs.payer_name,
       'payment', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;
```

No other lines in `fte_reconcile.sql` change.

---

## §8  Validation test — validate_corrected_value.sql

File: `tests/validate_corrected_value.sql`

11 PASS checks across 6 steps.

```
STEP 1  Baseline run (no correction) — payment = $351.89,
        open_balance = $1,248.11, queue_count = 5              [1–3/11]
STEP 2  Insert attach_corrected_value for obs a2 ($1,600.00)
STEP 3  Second run (correction active) — payment = $1,600.00,
        open_balance = $0.00, queue_count = 4                  [4–7/11]
STEP 4  Idempotency — third run, corrected state unchanged     [8–9/11]
STEP 5  Isolation — CLM-APC-2000 suspect obs b1 unaffected     [10/11]
STEP 6  Index enforcement — second active correction rejected   [11/11]
STEP 7  rollback — all reconciler output and the resolution row discarded
```

Baseline temp table: `_vcv_baseline` (distinct from `_vrr_baseline` used in
`validate_review_resolution.sql` and `_vor_baseline` used in
`validate_observation_resolution.sql`).

```sql
-- =============================================================================
-- Financial Truth Engine — Corrected-Value Resolution Validation
-- tests/validate_corrected_value.sql
--
-- 11 PASS checks across 6 steps that verify the attach_corrected_value
-- resolution path introduced in Task 004D.
--
-- STEP 1  Baseline run (no correction) → payment_applied = $351.89,
--         open_balance = $1,248.11, queue_count = 5,
--         review_resolutions_applied = 0                      [1–3/11]
-- STEP 2  Insert attach_corrected_value for obs a2, corrected_value = $1,600.00
-- STEP 3  Second run (correction active) → payment_applied = $1,600.00,
--         open_balance = $0.00, queue_count = 4,
--         review_resolutions_applied = 1                      [4–7/11]
-- STEP 4  Idempotency — third run, corrected state unchanged  [8–9/11]
-- STEP 5  Isolation — CLM-APC-2000 obs b1 unaffected         [10/11]
-- STEP 6  Index enforcement — second active correction rejected [11/11]
-- STEP 7  rollback
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. migration 004_corrected_value_constraints.sql applied
--   5. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_corrected_value.sql
--
-- All 11 checks emit RAISE NOTICE 'PASS [N/11] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only.  When running in the Supabase SQL
--   editor, paste the contents of fixtures/synthetic_96c5c357_failure_modes.sql
--   first (execute separately so it commits), then paste this file's body
--   starting from the BEGIN block.
-- =============================================================================

\i fixtures/synthetic_96c5c357_failure_modes.sql

begin;

-- Ensure no stale resolutions from a previous aborted run bleed in.
-- Phase 0 (inside the reconciler) handles derived tables; only
-- fte_review_resolutions survives Phase 0 and must be wiped manually.
delete from fte_review_resolutions
where practice_id = '96000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count.  fte_analysis_runs is append-only;
-- a disposable DB may hold prior reconciler runs from earlier manual validation.
-- All run-count assertions below test deltas (advance >= N), not absolute totals.
create temp table _vcv_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no correction exists.
--
-- Expected:
--   payment_applied for CLM-APC-1000 → amount = $351.89
--   financial position for CLM-APC-1000 → open_balance = $1,248.11,
--                                          reconciliation_status = 'unbalanced'
--   total review queue count → 5
--   return JSON: review_resolutions_applied = 0
-- =============================================================================
do $$
declare
  v_result      jsonb;
  v_count       int;
  v_amount      numeric;
  v_balance     numeric;
  v_pos_status  text;
  v_queue_count int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/11: return JSON reports zero active resolutions loaded
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/11] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/11] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/11: payment_applied = $351.89 (extracted amount; no correction applied)
  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 351.89,
    format('FAIL [2/11] baseline: payment_applied expected 351.89, got %s', v_amount);
  raise notice 'PASS [2/11] baseline: payment_applied = $351.89 (extracted amount, no correction)';

  -- CHECK 3/11: position is unbalanced; open_balance = $1,248.11; queue_count = 5
  -- (b1 suspected_duplicate + b2 suspected_duplicate + b3 late_retry_page_contradiction
  --  + CLM-APC-1000 unbalanced_financial_position + CLM-APC-2000 unbalanced_financial_position)
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [3/11] baseline: position expected unbalanced, got %s', v_pos_status);
  assert v_balance = 1248.11,
    format('FAIL [3/11] baseline: open_balance expected 1248.11, got %s', v_balance);

  select count(*) into v_queue_count
  from fte_review_queue
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

  assert v_queue_count = 5,
    format('FAIL [3/11] baseline: queue_count expected 5, got %s', v_queue_count);
  raise notice 'PASS [3/11] baseline: position = unbalanced, open_balance = $1,248.11, queue_count = 5';
end $$;


-- =============================================================================
-- STEP 2: Insert attach_corrected_value resolution for obs a2.
--
-- Reviewer has determined that the extracted payment of $351.89 is incorrect.
-- The claim was adjudicated at 100% of billed; correct payment is $1,600.00.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  claim_id,
  observation_id,
  action,
  target_type,
  corrected_value,
  resolved_by,
  notes
) values (
  '96000000-0000-4000-8000-0000000000fe',
  'c1a90000-0000-4000-8000-000000001000',
  '0b590000-0000-4000-8000-0000000000a2',
  'attach_corrected_value',
  'observation',
  1600.00,
  'test_runner',
  'Synthetic: extracted $351.89 is incorrect; correct payment is $1,600.00 (claim adjudicated at 100% of billed)'
);


-- =============================================================================
-- STEP 3: Second run — correction now active.
--
-- Expected:
--   payment_applied for CLM-APC-1000 → amount = $1,600.00
--   financial position for CLM-APC-1000 → open_balance = $0.00,
--                                          reconciliation_status = 'balanced'
--   CLM-APC-1000 not in queue as unbalanced_financial_position
--   total review queue count → 4
--   return JSON: review_resolutions_applied = 1
--   resolution row present (Phase 0 does not delete fte_review_resolutions)
-- =============================================================================
do $$
declare
  v_result      jsonb;
  v_count       int;
  v_amount      numeric;
  v_balance     numeric;
  v_pos_status  text;
  v_queue_count int;
  v_res_count   int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/11: return JSON reports 1 resolution loaded; payment = $1,600.00
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/11] resolved: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 1600.00,
    format('FAIL [4/11] resolved: payment_applied expected 1600.00, got %s', v_amount);
  raise notice 'PASS [4/11] resolved: review_resolutions_applied = 1; payment_applied = $1,600.00';

  -- CHECK 5/11: position is balanced; open_balance = $0.00
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'balanced',
    format('FAIL [5/11] resolved: position expected balanced, got %s', v_pos_status);
  assert v_balance = 0.00,
    format('FAIL [5/11] resolved: open_balance expected 0.00, got %s', v_balance);
  raise notice 'PASS [5/11] resolved: position = balanced, open_balance = $0.00';

  -- CHECK 6/11: CLM-APC-1000 cleared from unbalanced queue; total queue = 4
  select count(*) into v_queue_count
  from fte_review_queue rq
  join fte_claims c on c.id = rq.claim_id
  where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and rq.reason       = 'unbalanced_financial_position'
    and c.claim_number  = 'CLM-APC-1000';

  assert v_queue_count = 0,
    format('FAIL [6/11] resolved: CLM-APC-1000 unbalanced entries expected 0, got %s', v_queue_count);

  select count(*) into v_queue_count
  from fte_review_queue
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

  assert v_queue_count = 4,
    format('FAIL [6/11] resolved: total queue_count expected 4, got %s', v_queue_count);
  raise notice 'PASS [6/11] resolved: CLM-APC-1000 cleared from unbalanced queue; total queue_count = 4';

  -- CHECK 7/11: resolution row survives Phase 0
  select count(*) into v_res_count
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and action         = 'attach_corrected_value'
    and is_superseded  = false;

  assert v_res_count = 1,
    format('FAIL [7/11] resolved: resolution row count expected 1, got %s', v_res_count);
  raise notice 'PASS [7/11] resolved: resolution row (attach_corrected_value) survived Phase 0';
end $$;


-- =============================================================================
-- STEP 4: Idempotency — third run, corrected state persists unchanged.
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_amount     numeric;
  v_pos_status text;
  v_run_count  int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 8/11: same payment, resolutions, and position as step 3
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [8/11] idempotency: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 1600.00,
    format('FAIL [8/11] idempotency: payment_applied expected 1600.00, got %s', v_amount);

  select fp.reconciliation_status into v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'balanced',
    format('FAIL [8/11] idempotency: position expected balanced, got %s', v_pos_status);
  raise notice 'PASS [8/11] idempotency: third run — resolutions = 1, payment = $1,600.00, position = balanced';

  -- CHECK 9/11: analysis_runs advanced by >= 3
  select count(*) - (select run_count from _vcv_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 3,
    format('FAIL [9/11] idempotency: analysis_runs expected advance of >= 3, got %s', v_run_count);
  raise notice 'PASS [9/11] idempotency: analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 5: Isolation — correction for obs a2 (CLM-APC-1000) must not affect
-- the suspect observations on CLM-APC-2000.
-- =============================================================================
do $$
declare
  v_count int;
begin
  -- CHECK 10/11: obs b1 still in queue as suspected_duplicate
  select count(*) into v_count
  from fte_review_queue rq
  where rq.practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and rq.observation_id = '0b590000-0000-4000-8000-0000000000b1'
    and rq.reason         = 'suspected_duplicate';

  assert v_count = 1,
    format('FAIL [10/11] isolation: b1 suspected_duplicate entries expected 1, got %s', v_count);
  raise notice 'PASS [10/11] isolation: CLM-APC-2000 obs b1 unaffected by CLM-APC-1000 correction';
end $$;


-- =============================================================================
-- STEP 6: Index enforcement — the unique partial index must reject a second
-- active attach_corrected_value resolution for the same observation.
-- =============================================================================
do $$
declare
  v_caught boolean := false;
begin
  begin
    insert into fte_review_resolutions (
      practice_id,
      claim_id,
      observation_id,
      action,
      target_type,
      corrected_value,
      resolved_by,
      notes
    ) values (
      '96000000-0000-4000-8000-0000000000fe',
      'c1a90000-0000-4000-8000-000000001000',
      '0b590000-0000-4000-8000-0000000000a2',
      'attach_corrected_value',
      'observation',
      999.00,
      'test_runner',
      'Synthetic: this second active correction must be rejected by the unique partial index'
    );
  exception when unique_violation then
    v_caught := true;
  end;

  -- CHECK 11/11: unique constraint violation was raised
  assert v_caught,
    'FAIL [11/11] index: second active attach_corrected_value for obs a2 should have raised unique_violation';
  raise notice 'PASS [11/11] index: unique partial index blocked second active correction for obs a2';
end $$;


-- =============================================================================
-- STEP 7: Rollback — all reconciler output and the resolution row discarded.
-- The fixture data (fte_observations, fte_claims, fte_evidence) and the
-- seed_fixture fte_analysis_runs row were committed by the fixture''s own
-- begin/commit and are NOT rolled back here.
-- =============================================================================
rollback;
```

---

## §9  README updates

### README.md — task table row

Add after the 004C row:

```markdown
| 004D | Corrected-value resolutions | `attach_corrected_value` enforced by DB constraints (migration 004); Phase 5c uses `COALESCE(corrected_value, extracted_amount)` | `migrations/004_corrected_value_constraints.sql`, `reconciler/fte_reconcile.sql` (Phase 5c), `tests/validate_corrected_value.sql` |
```

### README_SCHEMA.md — fte_review_resolutions section

Under the `fte_review_resolutions` table description, add:

```markdown
**Unique partial index (migration 004):**
`idx_fte_resolutions_single_active_correction` —
`UNIQUE (practice_id, observation_id, action) WHERE is_superseded = false AND action = 'attach_corrected_value'`
Enforces at most one active corrected-value resolution per observation at the DB level.
To supersede: set `is_superseded = true` on the old row, then insert a new one.
```

### NEXT_STEPS.md

- Mark Task 004D complete
- Update the "Next immediate task" pointer to Task 005 (or whatever follows 004D in the roadmap)

---

## §10  Invariants to preserve

The following must remain true after this task is applied:

1. **Existing tests pass unchanged** — `validate_reconciler.sql`,
   `validate_review_resolution.sql`, and `validate_observation_resolution.sql`
   must produce all PASS checks without modification.

2. **No-correction fallback** — when no `attach_corrected_value` resolution
   exists for an observation, `COALESCE(NULL, v_obs.amount)` = `v_obs.amount`,
   so existing Phase 5c behaviour is bitwise identical to the pre-task baseline.

3. **Observation immutability** — `fte_observations` rows are never mutated.
   The correction lives exclusively in `fte_review_resolutions`.

4. **Append-only resolution table** — `fte_review_resolutions` rows are never
   updated or deleted by the reconciler.  Phase 0 explicitly does NOT delete
   from this table.  The resolution row must survive all three reconciler runs
   in the validation test (CHECK 7/11 and CHECK 8/11 confirm this).

5. **Single active correction enforced at DB level** — the unique partial index
   prevents two `is_superseded = false` rows for the same
   `(practice_id, observation_id, 'attach_corrected_value')` tuple.  The LIMIT 1
   in the correlated subquery is therefore deterministic, not a tie-breaker.

6. **Phase 6 auto-recalculates** — open_balance and reconciliation_status are
   fully derived from emitted events each run; no correction-specific logic is
   added to Phase 6.

7. **CLM-APC-2000 isolation** — the correction for obs a2 (CLM-APC-1000) has
   no effect on CLM-APC-2000's suspect observations or queue entries.

8. **Migration 004 is additive** — only adds constraints and an index; does not
   alter columns, rename objects, or modify existing constraints.

9. **corrected_value scope** — the `corrected_value` column already exists
   (migration 002).  Migration 004 adds enforcement constraints; it does not
   add a new column.

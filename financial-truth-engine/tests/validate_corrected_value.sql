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
-- seed_fixture fte_analysis_runs row were committed by the fixture's own
-- begin/commit and are NOT rolled back here.
-- =============================================================================
rollback;

-- =============================================================================
-- Financial Truth Engine — Corrected-Value Supersession Validation
-- tests/validate_corrected_value_supersession.sql
--
-- 10 PASS checks across 6 steps that verify the corrected-value supersession
-- workflow: old active correction → supersede → new active correction.
--
-- STEP 1  Baseline run (no correction) → payment_applied = $351.89,
--         open_balance = $1,248.11, status = unbalanced, queue_count = 5,
--         review_resolutions_applied = 0                          [1/10]
-- STEP 2  Insert first correction ($1,600.00) → reconcile
--         → payment = $1,600.00, balanced, queue_count = 4       [2–3/10]
-- STEP 3  Supersede first correction (UPDATE is_superseded = true)
--         → old row archived, zero active corrections for obs a2  [4/10]
-- STEP 4  Insert second correction ($1,500.00) → reconcile
--         → payment = $1,500.00, unbalanced, open_balance = $100.00,
--           queue_count = 5                                        [5–7/10]
-- STEP 5  Audit trail — 2 correction rows total, 1 superseded + 1 active,
--         active row has corrected_value = $1,500.00              [8–9/10]
-- STEP 6  Index enforcement — third active correction rejected     [10/10]
-- STEP 7  rollback
--
-- Fixture:  fixtures/synthetic_96c5c357_failure_modes.sql
--   Practice:    96000000-0000-4000-8000-0000000000fe
--   Observation: 0b590000-0000-4000-8000-0000000000a2 (obs a2)
--                payment, CLM-APC-1000, extracted amount = $351.89
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. migration 004_corrected_value_constraints.sql applied
--   5. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f fixtures/synthetic_96c5c357_failure_modes.sql
--   psql "$DATABASE_URL" -f tests/validate_corrected_value_supersession.sql
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only.  When running in the Supabase SQL
--   editor, paste and execute fixtures/synthetic_96c5c357_failure_modes.sql
--   first in a separate tab/execution (it has its own BEGIN/COMMIT and must
--   commit before this suite runs).  Then paste this file's body starting from
--   the BEGIN block below.
--
-- All 10 checks emit RAISE NOTICE 'PASS [N/10] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Note on supersede-without-replace:
--   If a caller sets is_superseded = true on the only active correction but
--   never inserts a new one, the next reconciler run falls back to the
--   extracted amount via COALESCE(NULL, v_obs.amount).  This is correct
--   behaviour but silent — no error, no queue entry for the observation.
--   That path is intentionally excluded from 004E scope.
-- =============================================================================

\i fixtures/synthetic_96c5c357_failure_modes.sql

begin;

-- Wipe any stale resolutions left by a previous aborted run.
-- Phase 0 (inside the reconciler) handles all derived tables; only
-- fte_review_resolutions survives Phase 0 and must be wiped manually.
delete from fte_review_resolutions
where practice_id = '96000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count.  fte_analysis_runs is
-- append-only; a disposable DB may hold prior runs from earlier validation
-- sessions.  All run-count assertions test deltas (advance >= N), not totals.
create temp table _vcvs_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no correction exists.
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

  -- CHECK 1/10: no resolutions loaded; payment = extracted amount;
  --             position unbalanced; open_balance = $1,248.11; queue_count = 5
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/10] baseline: review_resolutions_applied expected 0, got %s', v_count);

  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 351.89,
    format('FAIL [1/10] baseline: payment_applied expected 351.89, got %s', v_amount);

  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [1/10] baseline: status expected unbalanced, got %s', v_pos_status);
  assert v_balance = 1248.11,
    format('FAIL [1/10] baseline: open_balance expected 1248.11, got %s', v_balance);

  select count(*) into v_queue_count
  from fte_review_queue
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

  assert v_queue_count = 5,
    format('FAIL [1/10] baseline: queue_count expected 5, got %s', v_queue_count);

  raise notice 'PASS [1/10] baseline: resolutions=0, payment=$351.89, open_balance=$1,248.11, status=unbalanced, queue=5';
end $$;


-- =============================================================================
-- STEP 2: Insert first correction ($1,600.00) and reconcile.
--
-- Reviewer has determined the extracted $351.89 is wrong; the claim was
-- adjudicated at 100% of billed.  Correct payment is $1,600.00.
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
  'Synthetic: extracted $351.89 incorrect; correct payment is $1,600.00 (100% of billed)'
);

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

  -- CHECK 2/10: first correction loaded; payment = $1,600.00
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [2/10] first correction: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 1600.00,
    format('FAIL [2/10] first correction: payment_applied expected 1600.00, got %s', v_amount);

  raise notice 'PASS [2/10] first correction: resolutions=1, payment=$1,600.00';

  -- CHECK 3/10: position balanced; open_balance = $0.00; queue_count = 4
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'balanced',
    format('FAIL [3/10] first correction: status expected balanced, got %s', v_pos_status);
  assert v_balance = 0.00,
    format('FAIL [3/10] first correction: open_balance expected 0.00, got %s', v_balance);

  select count(*) into v_queue_count
  from fte_review_queue
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

  assert v_queue_count = 4,
    format('FAIL [3/10] first correction: queue_count expected 4, got %s', v_queue_count);

  raise notice 'PASS [3/10] first correction: status=balanced, open_balance=$0.00, queue=4';
end $$;


-- =============================================================================
-- STEP 3: Supersede the first correction.
--
-- The reviewer has a revised figure.  The supersession pattern is:
--   UPDATE old row SET is_superseded = true
--   then INSERT a new row with the corrected figure.
-- This step only performs the UPDATE.  Phase 0.5 on the next reconciler run
-- will load zero active corrections for obs a2, reverting to extracted amount
-- until the new correction is inserted in STEP 4.
-- =============================================================================
update fte_review_resolutions
set    is_superseded = true
where  practice_id    = '96000000-0000-4000-8000-0000000000fe'
  and  observation_id = '0b590000-0000-4000-8000-0000000000a2'
  and  action         = 'attach_corrected_value'
  and  is_superseded  = false;

do $$
declare
  v_superseded_count int;
  v_active_count     int;
begin
  -- CHECK 4/10: old row is now superseded; zero active corrections remain
  select count(*) into v_superseded_count
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b590000-0000-4000-8000-0000000000a2'
    and action         = 'attach_corrected_value'
    and is_superseded  = true;

  assert v_superseded_count = 1,
    format('FAIL [4/10] supersession: expected 1 superseded row, got %s', v_superseded_count);

  select count(*) into v_active_count
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b590000-0000-4000-8000-0000000000a2'
    and action         = 'attach_corrected_value'
    and is_superseded  = false;

  assert v_active_count = 0,
    format('FAIL [4/10] supersession: expected 0 active rows after supersession, got %s', v_active_count);

  raise notice 'PASS [4/10] supersession: old row is_superseded=true; 0 active corrections remain for obs a2';
end $$;


-- =============================================================================
-- STEP 4: Insert second correction ($1,500.00) and reconcile.
--
-- The revised figure is $1,500.00.  The unique partial index now allows this
-- INSERT because the previous row has is_superseded = true.
-- Expected: payment = $1,500.00; open_balance = $100.00 ($1,600.00 − $1,500.00);
-- position returns to unbalanced; CLM-APC-1000 re-enters review queue.
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
  1500.00,
  'test_runner',
  'Synthetic: revised correction — $1,600.00 figure was wrong; correct payment is $1,500.00'
);

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

  -- CHECK 5/10: exactly 1 resolution loaded (the new active row only)
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [5/10] second correction: review_resolutions_applied expected 1, got %s', v_count);

  raise notice 'PASS [5/10] second correction: resolutions=1 (superseded row excluded by Phase 0.5)';

  -- CHECK 6/10: payment = $1,500.00
  select ce.amount into v_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_amount = 1500.00,
    format('FAIL [6/10] second correction: payment_applied expected 1500.00, got %s', v_amount);

  raise notice 'PASS [6/10] second correction: payment=$1,500.00';

  -- CHECK 7/10: position unbalanced; open_balance = $100.00; queue_count = 5
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [7/10] second correction: status expected unbalanced, got %s', v_pos_status);
  assert v_balance = 100.00,
    format('FAIL [7/10] second correction: open_balance expected 100.00, got %s', v_balance);

  select count(*) into v_queue_count
  from fte_review_queue
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

  assert v_queue_count = 5,
    format('FAIL [7/10] second correction: queue_count expected 5, got %s', v_queue_count);

  raise notice 'PASS [7/10] second correction: status=unbalanced, open_balance=$100.00, queue=5 (CLM-APC-1000 re-entered)';
end $$;


-- =============================================================================
-- STEP 5: Audit trail — both correction rows must be retained.
-- =============================================================================
do $$
declare
  v_total_count      int;
  v_superseded_count int;
  v_active_count     int;
  v_active_value     numeric;
begin
  select count(*) into v_total_count
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b590000-0000-4000-8000-0000000000a2'
    and action         = 'attach_corrected_value';

  -- CHECK 8/10: exactly 2 correction rows (historical + current)
  assert v_total_count = 2,
    format('FAIL [8/10] audit trail: expected 2 total correction rows, got %s', v_total_count);

  select count(*) filter (where is_superseded = true),
         count(*) filter (where is_superseded = false)
    into v_superseded_count, v_active_count
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b590000-0000-4000-8000-0000000000a2'
    and action         = 'attach_corrected_value';

  assert v_superseded_count = 1,
    format('FAIL [8/10] audit trail: expected 1 superseded row, got %s', v_superseded_count);
  assert v_active_count = 1,
    format('FAIL [8/10] audit trail: expected 1 active row, got %s', v_active_count);

  raise notice 'PASS [8/10] audit trail: 2 total correction rows (1 superseded, 1 active)';

  -- CHECK 9/10: active row carries the second correction value ($1,500.00)
  select corrected_value into v_active_value
  from fte_review_resolutions
  where practice_id    = '96000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b590000-0000-4000-8000-0000000000a2'
    and action         = 'attach_corrected_value'
    and is_superseded  = false;

  assert v_active_value = 1500.00,
    format('FAIL [9/10] audit trail: active corrected_value expected 1500.00, got %s', v_active_value);

  raise notice 'PASS [9/10] audit trail: active correction row has corrected_value=$1,500.00';
end $$;


-- =============================================================================
-- STEP 6: Index enforcement — unique partial index must reject a third active
-- correction for the same observation, even after supersession.
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
      'Synthetic: third active correction must be rejected by unique partial index'
    );
  exception when unique_violation then
    v_caught := true;
  end;

  -- CHECK 10/10: unique_violation was caught
  assert v_caught,
    'FAIL [10/10] index: third active attach_corrected_value for obs a2 should have raised unique_violation';

  raise notice 'PASS [10/10] index: unique partial index blocked third active correction for obs a2';
end $$;


-- =============================================================================
-- STEP 7: Rollback — all reconciler output and both resolution rows discarded.
-- The fixture data (fte_observations, fte_claims, fte_evidence) and the
-- fte_analysis_runs entries written by the three reconciler calls are NOT
-- rolled back (fte_analysis_runs is append-only by design).
-- =============================================================================
rollback;

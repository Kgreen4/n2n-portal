-- =============================================================================
-- Financial Truth Engine — Corrected Billed Amount Validation
-- tests/validate_corrected_billed_amount.sql
--
-- 10 PASS checks across 5 steps that verify the attach_corrected_value
-- resolution path for billed_amount observations (Task 004H).
--
-- STEP 1  Baseline run (no correction) →
--           claim_adjudicated = $1,600.00 (extracted amount),
--           payment_applied = $351.89 (unchanged),
--           open_balance = $1,248.11, status = unbalanced,
--           review_resolutions_applied = 0                      [1–3/10]
-- STEP 2  Insert attach_corrected_value for obs a1, corrected_value = $1,500.00
-- STEP 3  Second run (correction active) →
--           claim_adjudicated = $1,500.00 (corrected),
--           payment_applied = $351.89 (unchanged — Phase 3 only),
--           open_balance = $1,148.11, status = unbalanced,
--           review_resolutions_applied = 1                      [4–7/10]
-- STEP 4  Idempotency — third run, corrected state unchanged    [8–9/10]
-- STEP 5  Index enforcement — second active correction rejected  [10/10]
-- STEP 6  rollback
--
-- Fixture design (CLM-APC-1000):
--   billed_amount obs a1         = $1,600.00  (extracted / corrected to $1,500.00)
--   payment obs a2               = $351.89
--   no contractual_adjustment obs in this suite
--   baseline open_balance  = GREATEST(0, 1600 - 0 - 351.89) = $1,248.11
--   corrected open_balance = GREATEST(0, 1500 - 0 - 351.89) = $1,148.11
--   Both remain unbalanced (open > 0); correction shifts balance but
--   does not close it — intentional, proving Phase 3 was affected and
--   Phase 5c (payment) was not.
--   Direction: lower billed → lower open_balance (opposite sign to
--   correcting contractual adjustment, where lower adj → higher open_balance).
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. migration 004_corrected_value_constraints.sql applied
--   5. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_corrected_billed_amount.sql
--
-- All 10 checks emit RAISE NOTICE 'PASS [N/10] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only.  When running in the Supabase SQL
--   editor, paste the contents of fixtures/synthetic_96c5c357_failure_modes.sql
--   first (execute separately so it commits), then comment out or remove the \i
--   line below and paste this file's body starting from the BEGIN block.
-- =============================================================================

\i fixtures/synthetic_96c5c357_failure_modes.sql

begin;

-- Ensure no stale resolutions from a previous aborted run bleed in.
delete from fte_review_resolutions
where practice_id = '96000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count.  fte_analysis_runs is append-only;
-- a disposable DB may hold prior reconciler runs from earlier manual validation.
-- All run-count assertions below test deltas (advance >= N), not absolute totals.
create temp table _vcba_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no correction active.
--
-- obs a1 (billed_amount $1,600.00) and obs a2 (payment $351.89) exist in
-- the fixture.  No contractual_adjustment obs is inserted here.
--
-- Expected:
--   claim_adjudicated for CLM-APC-1000   → amount = $1,600.00 (extracted)
--   payment_applied for CLM-APC-1000     → amount = $351.89
--   financial position for CLM-APC-1000  → open_balance = $1,248.11,
--                                          reconciliation_status = 'unbalanced'
--   return JSON: review_resolutions_applied = 0
-- =============================================================================
do $$
declare
  v_result       jsonb;
  v_count        int;
  v_billed       numeric;
  v_pay_amount   numeric;
  v_balance      numeric;
  v_pos_status   text;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/10: return JSON reports zero active resolutions
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/10] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/10] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/10: claim_adjudicated = $1,600.00 (extracted billed amount)
  select ce.amount into v_billed
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'claim_adjudicated'
    and c.claim_number = 'CLM-APC-1000';

  assert v_billed = 1600.00,
    format('FAIL [2/10] baseline: claim_adjudicated expected 1600.00, got %s', v_billed);
  raise notice 'PASS [2/10] baseline: claim_adjudicated = $1,600.00 (extracted amount, no correction)';

  -- CHECK 3/10: payment_applied = $351.89; open_balance = $1,248.11; unbalanced
  select ce.amount into v_pay_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pay_amount = 351.89,
    format('FAIL [3/10] baseline: payment_applied expected 351.89, got %s', v_pay_amount);

  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [3/10] baseline: position expected unbalanced, got %s', v_pos_status);
  assert v_balance = 1248.11,
    format('FAIL [3/10] baseline: open_balance expected 1248.11, got %s', v_balance);
  raise notice 'PASS [3/10] baseline: payment_applied = $351.89; open_balance = $1,248.11; status = unbalanced';
end $$;


-- =============================================================================
-- STEP 2: Insert attach_corrected_value resolution for obs a1 (billed_amount).
--
-- Reviewer has determined that the extracted billed amount of $1,600.00
-- is incorrect.  The correct charge per the source document is $1,500.00.
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
  '0b590000-0000-4000-8000-0000000000a1',
  'attach_corrected_value',
  'observation',
  1500.00,
  'test_runner',
  'Synthetic: extracted billed amount $1,600.00 is incorrect; correct charge per source document is $1,500.00'
);


-- =============================================================================
-- STEP 3: Second run — correction now active.
--
-- Expected:
--   claim_adjudicated → $1,500.00 (corrected)
--   payment_applied → $351.89 (unchanged — correction scoped to Phase 3 only)
--   financial position → open_balance = $1,148.11, status = unbalanced
--   return JSON: review_resolutions_applied = 1
--   resolution row survives Phase 0
-- =============================================================================
do $$
declare
  v_result       jsonb;
  v_count        int;
  v_billed       numeric;
  v_pay_amount   numeric;
  v_balance      numeric;
  v_pos_status   text;
  v_res_count    int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/10: return JSON reports 1 resolution loaded; billed = $1,500.00
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/10] corrected: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_billed
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'claim_adjudicated'
    and c.claim_number = 'CLM-APC-1000';

  assert v_billed = 1500.00,
    format('FAIL [4/10] corrected: claim_adjudicated expected 1500.00, got %s', v_billed);
  raise notice 'PASS [4/10] corrected: review_resolutions_applied = 1; claim_adjudicated = $1,500.00';

  -- CHECK 5/10: payment_applied unchanged at $351.89 — correction scoped to Phase 3
  select ce.amount into v_pay_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pay_amount = 351.89,
    format('FAIL [5/10] corrected: payment_applied expected 351.89, got %s', v_pay_amount);
  raise notice 'PASS [5/10] corrected: payment_applied = $351.89 (unchanged — correction scoped to Phase 3 only)';

  -- CHECK 6/10: open_balance = $1,148.11; still unbalanced
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [6/10] corrected: position expected unbalanced, got %s', v_pos_status);
  assert v_balance = 1148.11,
    format('FAIL [6/10] corrected: open_balance expected 1148.11, got %s', v_balance);
  raise notice 'PASS [6/10] corrected: open_balance = $1,148.11; status = unbalanced';

  -- CHECK 7/10: resolution row survives Phase 0
  select count(*) into v_res_count
  from fte_review_resolutions
  where practice_id   = '96000000-0000-4000-8000-0000000000fe'
    and action        = 'attach_corrected_value'
    and is_superseded = false;

  assert v_res_count = 1,
    format('FAIL [7/10] corrected: resolution row count expected 1, got %s', v_res_count);
  raise notice 'PASS [7/10] corrected: resolution row (attach_corrected_value on obs a1) survived Phase 0';
end $$;


-- =============================================================================
-- STEP 4: Idempotency — third run, corrected state persists unchanged.
-- =============================================================================
do $$
declare
  v_result      jsonb;
  v_count       int;
  v_billed      numeric;
  v_pos_status  text;
  v_run_count   int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 8/10: same billed amount, resolution count, and status as step 3
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [8/10] idempotency: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_billed
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'claim_adjudicated'
    and c.claim_number = 'CLM-APC-1000';

  assert v_billed = 1500.00,
    format('FAIL [8/10] idempotency: claim_adjudicated expected 1500.00, got %s', v_billed);

  select fp.reconciliation_status into v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [8/10] idempotency: position expected unbalanced, got %s', v_pos_status);
  raise notice 'PASS [8/10] idempotency: third run — resolutions = 1, claim_adjudicated = $1,500.00, status = unbalanced';

  -- CHECK 9/10: analysis_runs advanced by >= 3
  select count(*) - (select run_count from _vcba_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 3,
    format('FAIL [9/10] idempotency: analysis_runs expected advance of >= 3, got %s', v_run_count);
  raise notice 'PASS [9/10] idempotency: analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 5: Index enforcement — the unique partial index must reject a second
-- active attach_corrected_value resolution for the same billed_amount
-- observation (obs a1).
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
      '0b590000-0000-4000-8000-0000000000a1',
      'attach_corrected_value',
      'observation',
      9999.00,
      'test_runner',
      'Synthetic: this second active correction on obs a1 must be rejected by the unique partial index'
    );
  exception when unique_violation then
    v_caught := true;
  end;

  -- CHECK 10/10: unique constraint violation was raised
  assert v_caught,
    'FAIL [10/10] index: second active attach_corrected_value for obs a1 should have raised unique_violation';
  raise notice 'PASS [10/10] index: unique partial index blocked second active correction for obs a1';
end $$;


-- =============================================================================
-- STEP 6: Rollback — all reconciler output and the resolution row are
-- discarded.  The fixture data committed by the fixture's own begin/commit
-- (obs a1, a2, claims, evidence) is NOT rolled back here.
-- =============================================================================
rollback;

-- =============================================================================
-- Financial Truth Engine — Corrected Contractual Adjustment Validation
-- tests/validate_corrected_contractual_adjustment.sql
--
-- 10 PASS checks across 5 steps that verify the attach_corrected_value
-- resolution path for contractual_adjustment observations (Task 004G).
--
-- STEP 1  Insert synthetic obs a3 (trusted contractual_adjustment, $900.00).
--         Baseline run (no correction) →
--           contractual_adjustment_applied = $900.00,
--           payment_applied = $351.89 (unchanged),
--           open_balance = $348.11, status = unbalanced,
--           review_resolutions_applied = 0                      [1–3/10]
-- STEP 2  Insert attach_corrected_value for obs a3, corrected_value = $800.00
-- STEP 3  Second run (correction active) →
--           contractual_adjustment_applied = $800.00,
--           payment_applied = $351.89 (unchanged — Phase 4 only),
--           open_balance = $448.11, status = unbalanced,
--           review_resolutions_applied = 1                      [4–7/10]
-- STEP 4  Idempotency — third run, corrected state unchanged    [8–9/10]
-- STEP 5  Index enforcement — second active correction rejected  [10/10]
-- STEP 6  rollback
--
-- Fixture design (CLM-APC-1000):
--   billed_amount obs a1         = $1,600.00
--   payment obs a2               = $351.89
--   contractual_adjustment obs a3 = $900.00 extracted / $800.00 corrected
--   baseline open_balance  = GREATEST(0, 1600 - 900 - 351.89) = $348.11
--   corrected open_balance = GREATEST(0, 1600 - 800 - 351.89) = $448.11
--   Both baselines are unbalanced (open > 0); correction shifts balance but
--   does not close it — intentional, proving Phase 4 was affected, not Phase 5c.
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. migration 004_corrected_value_constraints.sql applied
--   5. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_corrected_contractual_adjustment.sql
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
create temp table _vcca_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Insert synthetic obs a3, then run baseline (no correction).
--
-- obs a3 is a trusted contractual_adjustment observation for CLM-APC-1000.
-- It is NOT in the fixture file — it is inserted here inside the ROLLBACK
-- transaction so that no fixture file is modified.
--
-- Expected:
--   contractual_adjustment_applied for CLM-APC-1000 → amount = $900.00
--   payment_applied for CLM-APC-1000 → amount = $351.89 (extracted, unchanged)
--   financial position for CLM-APC-1000 → open_balance = $348.11,
--                                          reconciliation_status = 'unbalanced'
--   return JSON: review_resolutions_applied = 0
-- =============================================================================
insert into fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type,
   claim_identifier, payer_name, service_date, cpt_code, check_eft_identifier,
   carc_code, confidence_score, raw_value, normalized_value, page_number,
   is_summary_row, is_superseded, metadata)
values
  ('0b590000-0000-4000-8000-0000000000a3',
   '96000000-0000-4000-8000-0000000000fe',
   'dddd9600-0000-4000-8000-000000000d01',
   'contractual_adjustment', 900.00, 'contractual_adjustment',
   'CLM-APC-1000', 'Arizona Priority Care', '2026-05-02', '93000',
   null,
   '45',
   0.92, '$900.00', '900.00', 1, false, false,
   '{"note":"synthetic contractual adjustment — CARC 45 write-down"}');

do $$
declare
  v_result      jsonb;
  v_count       int;
  v_adj_amount  numeric;
  v_pay_amount  numeric;
  v_balance     numeric;
  v_pos_status  text;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/10: return JSON reports zero active resolutions
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/10] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/10] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/10: contractual_adjustment_applied = $900.00 (extracted amount)
  select ce.amount into v_adj_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'contractual_adjustment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_adj_amount = 900.00,
    format('FAIL [2/10] baseline: contractual_adjustment_applied expected 900.00, got %s', v_adj_amount);
  raise notice 'PASS [2/10] baseline: contractual_adjustment_applied = $900.00 (extracted amount, no correction)';

  -- CHECK 3/10: payment_applied unchanged; open_balance = $348.11; unbalanced
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
  assert v_balance = 348.11,
    format('FAIL [3/10] baseline: open_balance expected 348.11, got %s', v_balance);
  raise notice 'PASS [3/10] baseline: payment_applied = $351.89; open_balance = $348.11; status = unbalanced';
end $$;


-- =============================================================================
-- STEP 2: Insert attach_corrected_value resolution for obs a3.
--
-- Reviewer has determined that the extracted contractual adjustment of $900.00
-- is incorrect.  The correct write-down per contract terms is $800.00.
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
  '0b590000-0000-4000-8000-0000000000a3',
  'attach_corrected_value',
  'observation',
  800.00,
  'test_runner',
  'Synthetic: extracted adj $900.00 is incorrect; correct contractual write-down per contract terms is $800.00'
);


-- =============================================================================
-- STEP 3: Second run — correction now active.
--
-- Expected:
--   contractual_adjustment_applied → $800.00 (corrected)
--   payment_applied → $351.89 (unchanged — correction scoped to Phase 4 only)
--   financial position → open_balance = $448.11, status = unbalanced
--   return JSON: review_resolutions_applied = 1
--   resolution row survives Phase 0
-- =============================================================================
do $$
declare
  v_result      jsonb;
  v_count       int;
  v_adj_amount  numeric;
  v_pay_amount  numeric;
  v_balance     numeric;
  v_pos_status  text;
  v_res_count   int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/10: return JSON reports 1 resolution loaded; adj = $800.00
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/10] corrected: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_adj_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'contractual_adjustment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_adj_amount = 800.00,
    format('FAIL [4/10] corrected: contractual_adjustment_applied expected 800.00, got %s', v_adj_amount);
  raise notice 'PASS [4/10] corrected: review_resolutions_applied = 1; contractual_adjustment_applied = $800.00';

  -- CHECK 5/10: payment_applied unchanged at $351.89 — correction scoped to Phase 4
  select ce.amount into v_pay_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pay_amount = 351.89,
    format('FAIL [5/10] corrected: payment_applied expected 351.89, got %s', v_pay_amount);
  raise notice 'PASS [5/10] corrected: payment_applied = $351.89 (unchanged — correction scoped to Phase 4 only)';

  -- CHECK 6/10: open_balance = $448.11; still unbalanced
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [6/10] corrected: position expected unbalanced, got %s', v_pos_status);
  assert v_balance = 448.11,
    format('FAIL [6/10] corrected: open_balance expected 448.11, got %s', v_balance);
  raise notice 'PASS [6/10] corrected: open_balance = $448.11; status = unbalanced';

  -- CHECK 7/10: resolution row survives Phase 0
  select count(*) into v_res_count
  from fte_review_resolutions
  where practice_id   = '96000000-0000-4000-8000-0000000000fe'
    and action        = 'attach_corrected_value'
    and is_superseded = false;

  assert v_res_count = 1,
    format('FAIL [7/10] corrected: resolution row count expected 1, got %s', v_res_count);
  raise notice 'PASS [7/10] corrected: resolution row (attach_corrected_value on obs a3) survived Phase 0';
end $$;


-- =============================================================================
-- STEP 4: Idempotency — third run, corrected state persists unchanged.
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_adj_amount numeric;
  v_pos_status text;
  v_run_count  int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 8/10: same adj amount, resolution count, and status as step 3
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [8/10] idempotency: review_resolutions_applied expected 1, got %s', v_count);

  select ce.amount into v_adj_amount
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'contractual_adjustment_applied'
    and c.claim_number = 'CLM-APC-1000';

  assert v_adj_amount = 800.00,
    format('FAIL [8/10] idempotency: contractual_adjustment_applied expected 800.00, got %s', v_adj_amount);

  select fp.reconciliation_status into v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [8/10] idempotency: position expected unbalanced, got %s', v_pos_status);
  raise notice 'PASS [8/10] idempotency: third run — resolutions = 1, adj = $800.00, status = unbalanced';

  -- CHECK 9/10: analysis_runs advanced by >= 3
  select count(*) - (select run_count from _vcca_baseline)
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
-- active attach_corrected_value resolution for the same contractual adjustment
-- observation (obs a3).
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
      '0b590000-0000-4000-8000-0000000000a3',
      'attach_corrected_value',
      'observation',
      999.00,
      'test_runner',
      'Synthetic: this second active correction on obs a3 must be rejected by the unique partial index'
    );
  exception when unique_violation then
    v_caught := true;
  end;

  -- CHECK 10/10: unique constraint violation was raised
  assert v_caught,
    'FAIL [10/10] index: second active attach_corrected_value for obs a3 should have raised unique_violation';
  raise notice 'PASS [10/10] index: unique partial index blocked second active correction for obs a3';
end $$;


-- =============================================================================
-- STEP 6: Rollback — all reconciler output, obs a3, and the resolution row
-- are discarded.  The fixture data committed by the fixture's own begin/commit
-- (obs a1, a2, b1, b2, b3, claims, evidence) is NOT rolled back here.
-- =============================================================================
rollback;

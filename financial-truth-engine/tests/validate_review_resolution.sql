-- =============================================================================
-- Financial Truth Engine — Review Resolution Validation
-- tests/validate_review_resolution.sql
--
-- 7 PASS checks across 5 steps that verify the confirm_payment_event
-- resolution path introduced in Task 004B.
--
-- STEP 1  Baseline run (no resolution) → payment_applied = 'ambiguous',
--         position = 'in_review', review_resolutions_applied = 0         [1-3/7]
-- STEP 2  Insert confirm_payment_event resolution for CLM-AZ-0001
-- STEP 3  Second run (resolution active) → payment_applied = 'reconciled',
--         position = 'balanced', review_resolutions_applied = 1          [4-6/7]
-- STEP 4  Idempotency — third run, resolved state unchanged              [7/7]
-- STEP 5  rollback — all reconciler output and the resolution row discarded
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_review_resolution.sql
--
-- All 7 checks emit RAISE NOTICE 'PASS [N/7] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only. When running in the Supabase SQL
--   editor, paste the contents of fixtures/synthetic_ccdbe216_failure_modes.sql
--   first (execute separately so it commits), then paste this file's body
--   starting from the BEGIN block.
-- =============================================================================

\i fixtures/synthetic_ccdbe216_failure_modes.sql

begin;

-- Ensure no stale resolutions from a previous aborted run bleed in.
-- Phase 0 (inside the reconciler) handles derived tables; only
-- fte_review_resolutions survives Phase 0 and must be wiped manually.
delete from fte_review_resolutions
where practice_id = 'c0000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count. fte_analysis_runs is append-only;
-- a disposable DB may hold prior reconciler runs from earlier manual validation.
-- All run-count assertions below test deltas (advance >= N), not absolute totals.
create temp table _vrr_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no resolutions exist.
--
-- Expected:
--   payment_applied event for CLM-AZ-0001 → reconciliation_status = 'ambiguous'
--   financial position for CLM-AZ-0001    → reconciliation_status = 'in_review'
--   open_balance_amount                   → 0.00  (math balances; human review needed)
--   contradicts link for obs b5           → exists
--   return JSON: review_resolutions_applied = 0
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
  v_ev_status text;
  v_pos_status text;
  v_balance   numeric;
  v_links     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/7: return JSON reports zero active resolutions loaded
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/7] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/7] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/7: payment event is ambiguous; position is in_review with zero balance
  select ce.reconciliation_status into v_ev_status
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_ev_status = 'ambiguous',
    format('FAIL [2/7] baseline: payment_applied expected ambiguous, got %s', v_ev_status);

  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_pos_status = 'in_review',
    format('FAIL [2/7] baseline: position expected in_review, got %s', v_pos_status);
  assert v_balance = 0.00,
    format('FAIL [2/7] baseline: open_balance expected 0.00, got %s', v_balance);
  raise notice 'PASS [2/7] baseline: payment_applied = ambiguous, position = in_review, open_balance = 0.00';

  -- CHECK 3/7: contradicts evidence link exists (step c is unconditional)
  select count(*) into v_links
  from fte_event_evidence ee
  join fte_claim_events ce on ce.id = ee.claim_event_id
  join fte_claims c on c.id = ce.claim_id
  where ee.practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and ee.observation_id = '0b500000-0000-4000-8000-0000000000b5'
    and ee.link_role      = 'contradicts'
    and ce.event_type     = 'payment_applied'
    and c.claim_number    = 'CLM-AZ-0001';

  assert v_links = 1,
    format('FAIL [3/7] baseline: contradicts link count expected 1, got %s', v_links);
  raise notice 'PASS [3/7] baseline: contradicts evidence link exists for obs b5 → payment_applied';
end $$;


-- =============================================================================
-- STEP 2: Insert confirm_payment_event resolution for CLM-AZ-0001.
--
-- Reviewer has confirmed that the page-63 retry render is an OCR artifact and
-- the original page-1 payment of $510.40 is authoritative.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  claim_id,
  observation_id,
  action,
  target_type,
  target_event_type,
  target_review_reason,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  'c1a10000-0000-4000-8000-000000000001',
  '0b500000-0000-4000-8000-0000000000b5',
  'confirm_payment_event',
  'payment_event',
  'payment_applied',
  'late_retry_page_contradiction',
  'test_runner',
  'Synthetic: reviewer confirms page-63 retry is an OCR re-render artifact; original $510.40 payment is correct'
);


-- =============================================================================
-- STEP 3: Second run — resolution now active.
--
-- Expected:
--   payment_applied event for CLM-AZ-0001 → reconciliation_status = 'reconciled'
--   financial position for CLM-AZ-0001    → reconciliation_status = 'balanced'
--   open_balance_amount                   → 0.00
--   contradicts link for obs b5           → still exists (step c is unconditional)
--   resolution row                        → still present (Phase 0 does not delete it)
--   return JSON: review_resolutions_applied = 1
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_ev_status  text;
  v_pos_status text;
  v_balance    numeric;
  v_links      int;
  v_res_count  int;
  v_run_count  int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/7: return JSON reports 1 resolution loaded; event is now reconciled
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/7] resolved: review_resolutions_applied expected 1, got %s', v_count);

  select ce.reconciliation_status into v_ev_status
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_ev_status = 'reconciled',
    format('FAIL [4/7] resolved: payment_applied expected reconciled, got %s', v_ev_status);
  raise notice 'PASS [4/7] resolved: review_resolutions_applied = 1; payment_applied = reconciled';

  -- CHECK 5/7: position is balanced with zero open balance
  select fp.reconciliation_status, fp.open_balance_amount
    into v_pos_status, v_balance
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_pos_status = 'balanced',
    format('FAIL [5/7] resolved: position expected balanced, got %s', v_pos_status);
  assert v_balance = 0.00,
    format('FAIL [5/7] resolved: open_balance expected 0.00, got %s', v_balance);
  raise notice 'PASS [5/7] resolved: position = balanced, open_balance = 0.00';

  -- CHECK 6/7: contradicts link preserved; resolution row intact; 2 reconciler runs recorded
  select count(*) into v_links
  from fte_event_evidence ee
  join fte_claim_events ce on ce.id = ee.claim_event_id
  join fte_claims c on c.id = ce.claim_id
  where ee.practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and ee.observation_id = '0b500000-0000-4000-8000-0000000000b5'
    and ee.link_role      = 'contradicts'
    and ce.event_type     = 'payment_applied'
    and c.claim_number    = 'CLM-AZ-0001';

  assert v_links = 1,
    format('FAIL [6/7] resolved: contradicts link expected 1, got %s', v_links);

  select count(*) into v_res_count
  from fte_review_resolutions
  where practice_id  = 'c0000000-0000-4000-8000-0000000000fe'
    and action       = 'confirm_payment_event'
    and is_superseded = false;

  assert v_res_count = 1,
    format('FAIL [6/7] resolved: resolution row expected 1, got %s', v_res_count);

  -- fte_analysis_runs is append-only; assert delta from baseline, not absolute count.
  select count(*) - (select run_count from _vrr_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 2,
    format('FAIL [6/7] resolved: analysis_runs expected advance of >= 2, got %s', v_run_count);

  raise notice 'PASS [6/7] resolved: contradicts link preserved; resolution row intact; analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 4: Idempotency — third run, resolved state persists unchanged.
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_ev_status  text;
  v_pos_status text;
  v_run_count  int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [7/7] idempotency: review_resolutions_applied expected 1, got %s', v_count);

  select ce.reconciliation_status into v_ev_status
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'payment_applied'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_ev_status = 'reconciled',
    format('FAIL [7/7] idempotency: payment_applied expected reconciled, got %s', v_ev_status);

  select fp.reconciliation_status into v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_pos_status = 'balanced',
    format('FAIL [7/7] idempotency: position expected balanced, got %s', v_pos_status);

  select count(*) - (select run_count from _vrr_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 3,
    format('FAIL [7/7] idempotency: analysis_runs expected advance of >= 3, got %s', v_run_count);

  raise notice 'PASS [7/7] idempotency: third run — reconciled/balanced state unchanged; analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 5: Rollback — all reconciler output and the resolution row discarded.
-- The fixture data (fte_observations, fte_claims, fte_evidence) and the
-- seed_fixture fte_analysis_runs row were committed by the fixture''s own
-- begin/commit and are NOT rolled back here.
-- =============================================================================
rollback;

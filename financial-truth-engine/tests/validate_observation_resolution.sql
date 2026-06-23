-- =============================================================================
-- Financial Truth Engine — Observation Resolution Validation
-- tests/validate_observation_resolution.sql
--
-- 12 PASS checks across 10 steps that verify the confirm_observation,
-- reject_observation (queue-suppression and ledger-impact), and mark_duplicate
-- resolution paths introduced in Task 004C.
--
-- STEP 1   Baseline run (no resolutions) → queue = 6, resolutions_applied = 0     [1-3/12]
-- STEP 2   Insert confirm_observation resolution for obs b4
-- STEP 3   Second run (confirm active) → queue = 5                                [4-5/12]
-- STEP 4   Insert reject_observation resolution for obs b3 (queue-suppression)
-- STEP 5   Third run (confirm + reject b3) → queue = 4                            [6-7/12]
-- STEP 6   Insert mark_duplicate resolution for obs b1 → canonical a1
-- STEP 7   Fourth run (all three resolutions active) → queue = 3                  [8-9/12]
-- STEP 8   Idempotency — fifth run → queue still = 3                              [10/12]
-- STEP 9   Insert reject_observation for obs a3 (trusted, ledger-impact)
-- STEP 10  Sixth run → contractual_adjustment_applied event absent;               [11-12/12]
--           open_balance recalculates to $209.60
-- STEP 11  rollback — all reconciler output and resolution rows discarded
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_observation_resolution.sql
--
-- All 12 checks emit RAISE NOTICE 'PASS [N/12] ...' and exit without exception.
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
create temp table _vor_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no resolutions exist.
--
-- Expected:
--   review_resolutions_applied = 0
--   fte_review_queue count     = 6 (5 observation entries + 1 position entry)
--   b4 (suspected_summary_row) in queue
--   b1 (suspected_duplicate)   in queue
--   b3 (missing_evidence_link) in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/12: return JSON reports zero active resolutions loaded
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/12] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/12] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/12: queue has 6 entries (5 obs + 1 position)
  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 6,
    format('FAIL [2/12] baseline: queue count expected 6, got %s', v_count);
  raise notice 'PASS [2/12] baseline: queue count = 6';

  -- CHECK 3/12: key observations present in queue with correct reasons
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4'
    and reason         = 'suspected_summary_row';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b4 suspected_summary_row expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1'
    and reason         = 'suspected_duplicate';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b1 suspected_duplicate expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3'
    and reason         = 'missing_evidence_link';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b3 missing_evidence_link expected 1, got %s', v_count);

  raise notice 'PASS [3/12] baseline: b4 (suspected_summary_row), b1 (suspected_duplicate), b3 (missing_evidence_link) all in queue';
end $$;


-- =============================================================================
-- STEP 2: Insert confirm_observation resolution for obs b4.
--
-- Reviewer acknowledges that the b4 summary row ($1,479.08) is correctly
-- classified as a summary aggregate and should be suppressed from the queue.
-- Queue-only effect: the observation still classifies as suspected_summary_row
-- and does not change ledger events or positions.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b4',
  'confirm_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer confirms b4 ($1,479.08 summary_total) is correctly classified as a suspected_summary_row — no queue entry needed'
);


-- =============================================================================
-- STEP 3: Second run — confirm_observation active.
--
-- Expected:
--   review_resolutions_applied = 1
--   queue count                = 5 (b4 suppressed from queue; b1/b2/b3/b5 + position)
--   b4 NOT in queue
--   b1 still in queue
--   b3 still in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/12: 1 resolution loaded; queue drops to 5
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/12] confirm: review_resolutions_applied expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 5,
    format('FAIL [4/12] confirm: queue count expected 5, got %s', v_count);
  raise notice 'PASS [4/12] confirm: review_resolutions_applied = 1; queue count = 5';

  -- CHECK 5/12: b4 absent from queue; b1 and b3 still present
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4';
  assert v_count = 0,
    format('FAIL [5/12] confirm: b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1';
  assert v_count = 1,
    format('FAIL [5/12] confirm: b1 expected still in queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3';
  assert v_count = 1,
    format('FAIL [5/12] confirm: b3 expected still in queue, got count=%s', v_count);

  raise notice 'PASS [5/12] confirm: b4 absent from queue; b1 and b3 still present';
end $$;


-- =============================================================================
-- STEP 4: Insert reject_observation resolution for obs b3 (queue-suppression case).
--
-- Reviewer marks b3 ($265.36 null_check_crossbleed) as entirely invalid —
-- it has no check identifier and cannot be traced to a real disbursement.
-- b3 was already excluded by Phase 1 Rule 1 (is_superseded=true). This proves
-- that reject suppresses Phase 1 classification and queue routing for an
-- already-problematic observation.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b3',
  'reject_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer marks b3 ($265.36 null_check_crossbleed) as invalid — no check identifier, cannot trace to a real disbursement; exclude from reconciliation'
);


-- =============================================================================
-- STEP 5: Third run — confirm + reject(b3) active.
--
-- Expected:
--   review_resolutions_applied = 2
--   queue count                = 4 (b3 and b4 gone; b1/b2/b5 + position)
--   b3 NOT in queue
--   b4 NOT in queue
--   b1 still in queue
--   b2 still in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 6/12: 2 resolutions loaded; queue drops to 4
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 2,
    format('FAIL [6/12] reject-b3: review_resolutions_applied expected 2, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 4,
    format('FAIL [6/12] reject-b3: queue count expected 4, got %s', v_count);
  raise notice 'PASS [6/12] reject-b3: review_resolutions_applied = 2; queue count = 4';

  -- CHECK 7/12: b3 and b4 absent; b1 and b2 still present
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3';
  assert v_count = 0,
    format('FAIL [7/12] reject-b3: b3 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4';
  assert v_count = 0,
    format('FAIL [7/12] reject-b3: b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1';
  assert v_count = 1,
    format('FAIL [7/12] reject-b3: b1 expected still in queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b2';
  assert v_count = 1,
    format('FAIL [7/12] reject-b3: b2 expected still in queue, got count=%s', v_count);

  raise notice 'PASS [7/12] reject-b3: b3 and b4 absent; b1 and b2 still present';
end $$;


-- =============================================================================
-- STEP 6: Insert mark_duplicate resolution for obs b1 → canonical a1.
--
-- Reviewer identifies b1 (OCR variant check ref "O000447412") as a duplicate
-- of canonical observation a1 (check "0000447412", $510.40). On the next run
-- Phase 1 suppresses b1 entirely (same mechanism as reject_observation).
-- target_observation_id records the canonical for audit.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  target_observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b1',
  '0b500000-0000-4000-8000-0000000000a1',
  'mark_duplicate',
  'observation',
  'test_runner',
  'Synthetic: b1 (OCR variant "O000447412") is a duplicate of canonical obs a1 (check "0000447412", $510.40) — suppress b1 from reconciliation'
);


-- =============================================================================
-- STEP 7: Fourth run — three resolutions active (confirm b4, reject b3, dup b1).
--
-- Expected:
--   review_resolutions_applied = 3
--   queue count                = 3 (b1/b3/b4 gone; b2/b5 + position remain)
--   b1 NOT in queue
--   b3 NOT in queue
--   b4 NOT in queue
--   mark_duplicate resolution row has target_observation_id = a1
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 8/12: 3 resolutions loaded; queue drops to 3
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 3,
    format('FAIL [8/12] duplicate: review_resolutions_applied expected 3, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [8/12] duplicate: queue count expected 3, got %s', v_count);
  raise notice 'PASS [8/12] duplicate: review_resolutions_applied = 3; queue count = 3';

  -- CHECK 9/12: b1/b3/b4 absent; target_observation_id wired to a1
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id in (
      '0b500000-0000-4000-8000-0000000000b1',
      '0b500000-0000-4000-8000-0000000000b3',
      '0b500000-0000-4000-8000-0000000000b4'
    );
  assert v_count = 0,
    format('FAIL [9/12] duplicate: b1/b3/b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_resolutions
  where practice_id           = 'c0000000-0000-4000-8000-0000000000fe'
    and action                = 'mark_duplicate'
    and observation_id        = '0b500000-0000-4000-8000-0000000000b1'
    and target_observation_id = '0b500000-0000-4000-8000-0000000000a1'
    and is_superseded         = false;
  assert v_count = 1,
    format('FAIL [9/12] duplicate: mark_duplicate row with target=a1 expected 1, got %s', v_count);

  raise notice 'PASS [9/12] duplicate: b1/b3/b4 absent from queue; mark_duplicate target_observation_id = a1';
end $$;


-- =============================================================================
-- STEP 8: Idempotency — fifth run, all-resolved state persists unchanged.
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
  v_run_count int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 3,
    format('FAIL [10/12] idempotency: review_resolutions_applied expected 3, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [10/12] idempotency: queue count expected 3, got %s', v_count);

  -- fte_analysis_runs is append-only; assert delta from baseline (5 reconciler
  -- calls so far in this transaction — steps 1, 3, 5, 7, 8).
  select count(*) - (select run_count from _vor_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 5,
    format('FAIL [10/12] idempotency: analysis_runs expected advance of >= 5, got %s', v_run_count);

  raise notice 'PASS [10/12] idempotency: fifth run — resolutions_applied = 3; queue = 3; analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 9: Insert reject_observation for obs a3 (ledger-impact case).
--
-- a3 is a TRUSTED contractual_adjustment observation ($209.60). In the baseline
-- it emits a contractual_adjustment_applied event and brings the claim's
-- open_balance to $0.00 (billed $720 − adj $209.60 − paid $510.40 = $0.00).
-- Rejecting a3 removes it from Phase 1 entirely: no event is emitted and the
-- position recalculates without the adjustment.
--
-- Unlike b3 (already excluded), this proves reject_observation has financial
-- consequences when applied to a trusted observation.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000a3',
  'reject_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer rejects a3 ($209.60 contractual_adjustment) — proves ledger-impact suppression of a trusted observation'
);


-- =============================================================================
-- STEP 10: Sixth run — all four resolutions active; ledger-impact of a3 reject.
--
-- Expected:
--   review_resolutions_applied = 4
--   queue count                = 3 (unchanged — a3 was trusted, never queued)
--   contractual_adjustment_applied event for CLM-AZ-0001: count = 0
--   financial position open_balance for CLM-AZ-0001: $209.60
--     (billed $720.00 − paid $510.40; adjustment no longer applied)
--   position reconciliation_status: still 'in_review'
--     (b5 late_retry ambiguity is unresolved; payment_applied remains 'ambiguous')
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_balance    numeric;
  v_pos_status text;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 11/12: contractual_adjustment_applied event absent; queue count unchanged
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 4,
    format('FAIL [11/12] reject-a3: review_resolutions_applied expected 4, got %s', v_count);

  select count(*) into v_count
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'contractual_adjustment_applied'
    and c.claim_number = 'CLM-AZ-0001';
  assert v_count = 0,
    format('FAIL [11/12] reject-a3: contractual_adjustment_applied expected 0, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [11/12] reject-a3: queue count expected still 3, got %s', v_count);

  raise notice 'PASS [11/12] reject-a3: resolutions_applied = 4; contractual_adjustment_applied absent; queue = 3';

  -- CHECK 12/12: financial position recalculates without the adjustment
  select fp.open_balance_amount, fp.reconciliation_status
    into v_balance, v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_balance = 209.60,
    format('FAIL [12/12] reject-a3: open_balance expected 209.60, got %s', v_balance);
  assert v_pos_status = 'in_review',
    format('FAIL [12/12] reject-a3: position expected in_review (b5 ambiguity persists), got %s', v_pos_status);

  raise notice 'PASS [12/12] reject-a3: open_balance = $209.60 (adj removed); position = in_review (b5 ambiguity persists)';
end $$;


-- =============================================================================
-- STEP 11: Rollback — all reconciler output and resolution rows discarded.
-- The fixture data (fte_observations, fte_claims, fte_evidence) and any
-- seed_fixture fte_analysis_runs rows were committed by the fixture's own
-- begin/commit and are NOT rolled back here.
-- =============================================================================
rollback;

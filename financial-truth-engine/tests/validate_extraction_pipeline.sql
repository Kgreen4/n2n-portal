-- =============================================================================
-- Financial Truth Engine — Phase 3A Extraction Pipeline Validation
-- tests/validate_extraction_pipeline.sql
--
-- 18 PASS checks verifying the end-to-end extraction baseline introduced in
-- Task 006B: fixture loading, reconciler output, financial positions, short-pay
-- detection, review-queue routing, two-link event evidence, and [SYNTHETIC]
-- raw_text prefix invariant.
--
-- STEP 1  Fixture verification (no reconciler call needed)
--           → evidence count = 6                                          [1/18]
--           → observation count = 10                                      [2/18]
-- STEP 2  Reconciler run
--           → base event count (3 types) = 9                              [3/18]
--           → total event count (all types, incl. short_pay_detected) = 10 [4/18]
-- STEP 3  Payment amounts per claim
--           → CLM-P3A-0001 payment_applied = 150.00                       [5/18]
--           → CLM-P3A-0002 payment_applied = 104.00                       [6/18]
--           → CLM-P3A-0003 payment_applied = 100.00                       [7/18]
-- STEP 4  CLM-P3A-0001 balanced position
--           → reconciliation_status = 'balanced'                          [8/18]
--           → open_balance_amount   = 0.00                                [9/18]
-- STEP 5  CLM-P3A-0002 balanced position
--           → reconciliation_status = 'balanced'                         [10/18]
--           → open_balance_amount   = 0.00                               [11/18]
-- STEP 6  CLM-P3A-0003 unbalanced position + short-pay detection
--           → reconciliation_status = 'unbalanced'                       [12/18]
--           → open_balance_amount   = 180.00  (350 − 70 − 100)           [13/18]
--           → short_pay_detected count = 1, amount = 180.00              [14/18]
-- STEP 7  Review-queue routing
--           → reason = 'suspected_summary_row' count = 1                 [15/18]
--           → reason = 'unbalanced_financial_position' for CLM-P3A-0003  [16/18]
-- STEP 8  Event-evidence links + [SYNTHETIC] prefix invariant
--           → CLM-P3A-0001 payment_applied has 2 fte_event_evidence links [17/18]
--           → non-null raw_text rows starting with '[SYNTHETIC]' = 4     [18/18]
-- STEP 9  Rollback
--
-- Test vehicle: fixtures/synthetic_phase3a_extraction_fixture.sql
--   Practice:       a3000000-0000-4000-8000-0000000000fe
--   Evidence rows:  6  (1 document + 4 page + 1 check_payment stub)
--   Observation rows: 10 (9 claim-level + 1 summary)
--   Claims:         CLM-P3A-0001 / CLM-P3A-0002 / CLM-P3A-0003
--   Check stub:     SYN-4001 ($354.00)
--
-- Prerequisites:
--   1. migrations 001–011 applied
--   2. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--   3. fixtures/synthetic_phase3a_extraction_fixture.sql loaded (or \i below)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_extraction_pipeline.sql
--
-- All 18 checks emit RAISE NOTICE 'PASS [N/18] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only.  When running in the Supabase SQL
--   editor, paste fixtures/synthetic_phase3a_extraction_fixture.sql first
--   (execute separately so it commits), then paste this file's body starting
--   from the BEGIN block.
-- =============================================================================

\i fixtures/synthetic_phase3a_extraction_fixture.sql

begin;

-- Ensure no stale resolutions from a previous aborted run bleed in.
-- fte_review_resolutions survives Phase 0 and must be wiped manually here.
delete from fte_review_resolutions
where practice_id = 'a3000000-0000-4000-8000-0000000000fe';

-- Capture starting run count.  fte_analysis_runs is append-only; use deltas.
create temp table _vep_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Fixture verification — evidence and observation counts.
-- Reconciler not yet called; these rows were committed by the fixture.
-- =============================================================================
do $$
declare
  v_evid_count int;
  v_obs_count  int;
begin
  -- CHECK 1/18: evidence count = 6
  -- (1 document + 4 page + 1 check_payment stub)
  select count(*) into v_evid_count
  from   fte_evidence
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe';

  assert v_evid_count = 6,
    format('FAIL [1/18] expected 6 evidence rows, got %s', v_evid_count);
  raise notice 'PASS [1/18] evidence count = 6';

  -- CHECK 2/18: observation count = 10
  -- (9 claim-level: 3 per claim × 3 claims; 1 summary row)
  select count(*) into v_obs_count
  from   fte_observations
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe';

  assert v_obs_count = 10,
    format('FAIL [2/18] expected 10 observation rows, got %s', v_obs_count);
  raise notice 'PASS [2/18] observation count = 10';
end;
$$;


-- =============================================================================
-- STEP 2: Reconciler run — base and total claim-event counts.
-- =============================================================================
do $$
declare
  v_result       jsonb;
  v_base_events  int;
  v_total_events int;
begin
  select fte_reconcile_practice('a3000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 3/18: base event types = 9
  -- 3 claim_adjudicated + 3 contractual_adjustment_applied + 3 payment_applied
  select count(*) into v_base_events
  from   fte_claim_events
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  event_type in ('claim_adjudicated',
                        'contractual_adjustment_applied',
                        'payment_applied');

  assert v_base_events = 9,
    format('FAIL [3/18] expected 9 base claim events, got %s', v_base_events);
  raise notice 'PASS [3/18] base claim events (claim_adjudicated + contractual_adjustment_applied + payment_applied) = 9';

  -- CHECK 4/18: total event count = 10 (base 9 + 1 short_pay_detected for CLM-P3A-0003)
  select count(*) into v_total_events
  from   fte_claim_events
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe';

  assert v_total_events = 10,
    format('FAIL [4/18] expected 10 total claim events, got %s', v_total_events);
  raise notice 'PASS [4/18] total claim events (all types including short_pay_detected) = 10';
end;
$$;


-- =============================================================================
-- STEP 3: Payment amounts per claim.
-- =============================================================================
do $$
declare
  v_amt_0001 numeric;
  v_amt_0002 numeric;
  v_amt_0003 numeric;
begin
  -- CHECK 5/18: CLM-P3A-0001 payment_applied = 150.00
  select ce.amount into v_amt_0001
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'payment_applied'
    and  c.claim_number = 'CLM-P3A-0001';

  assert v_amt_0001 = 150.00,
    format('FAIL [5/18] expected CLM-P3A-0001 payment_applied=150.00, got %s', v_amt_0001);
  raise notice 'PASS [5/18] CLM-P3A-0001 payment_applied = 150.00';

  -- CHECK 6/18: CLM-P3A-0002 payment_applied = 104.00
  select ce.amount into v_amt_0002
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'payment_applied'
    and  c.claim_number = 'CLM-P3A-0002';

  assert v_amt_0002 = 104.00,
    format('FAIL [6/18] expected CLM-P3A-0002 payment_applied=104.00, got %s', v_amt_0002);
  raise notice 'PASS [6/18] CLM-P3A-0002 payment_applied = 104.00';

  -- CHECK 7/18: CLM-P3A-0003 payment_applied = 100.00
  select ce.amount into v_amt_0003
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'payment_applied'
    and  c.claim_number = 'CLM-P3A-0003';

  assert v_amt_0003 = 100.00,
    format('FAIL [7/18] expected CLM-P3A-0003 payment_applied=100.00, got %s', v_amt_0003);
  raise notice 'PASS [7/18] CLM-P3A-0003 payment_applied = 100.00';
end;
$$;


-- =============================================================================
-- STEP 4: CLM-P3A-0001 balanced financial position.
-- Expected: billed 250 − adj 100 − paid 150 = 0.00 open.
-- =============================================================================
do $$
declare
  v_status  text;
  v_balance numeric;
begin
  select fp.reconciliation_status, fp.open_balance_amount
  into   v_status, v_balance
  from   fte_financial_positions fp
  join   fte_claims c on c.id = fp.claim_id
  where  fp.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-P3A-0001';

  -- CHECK 8/18: reconciliation_status = 'balanced'
  assert v_status = 'balanced',
    format('FAIL [8/18] expected CLM-P3A-0001 reconciliation_status=balanced, got %s', v_status);
  raise notice 'PASS [8/18] CLM-P3A-0001 reconciliation_status = balanced';

  -- CHECK 9/18: open_balance_amount = 0.00
  assert v_balance = 0.00,
    format('FAIL [9/18] expected CLM-P3A-0001 open_balance_amount=0.00, got %s', v_balance);
  raise notice 'PASS [9/18] CLM-P3A-0001 open_balance_amount = 0.00';
end;
$$;


-- =============================================================================
-- STEP 5: CLM-P3A-0002 balanced financial position.
-- Expected: billed 180 − adj 76 − paid 104 = 0.00 open.
-- =============================================================================
do $$
declare
  v_status  text;
  v_balance numeric;
begin
  select fp.reconciliation_status, fp.open_balance_amount
  into   v_status, v_balance
  from   fte_financial_positions fp
  join   fte_claims c on c.id = fp.claim_id
  where  fp.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-P3A-0002';

  -- CHECK 10/18: reconciliation_status = 'balanced'
  assert v_status = 'balanced',
    format('FAIL [10/18] expected CLM-P3A-0002 reconciliation_status=balanced, got %s', v_status);
  raise notice 'PASS [10/18] CLM-P3A-0002 reconciliation_status = balanced';

  -- CHECK 11/18: open_balance_amount = 0.00
  assert v_balance = 0.00,
    format('FAIL [11/18] expected CLM-P3A-0002 open_balance_amount=0.00, got %s', v_balance);
  raise notice 'PASS [11/18] CLM-P3A-0002 open_balance_amount = 0.00';
end;
$$;


-- =============================================================================
-- STEP 6: CLM-P3A-0003 unbalanced position + short_pay_detected.
-- Expected: billed 350 − adj 70 − paid 100 = 180.00 open.
-- No CARC codes, no patient-responsibility language — clean synthetic short pay.
-- =============================================================================
do $$
declare
  v_status    text;
  v_balance   numeric;
  v_sp_count  int;
  v_sp_amount numeric;
begin
  select fp.reconciliation_status, fp.open_balance_amount
  into   v_status, v_balance
  from   fte_financial_positions fp
  join   fte_claims c on c.id = fp.claim_id
  where  fp.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-P3A-0003';

  -- CHECK 12/18: reconciliation_status = 'unbalanced'
  assert v_status = 'unbalanced',
    format('FAIL [12/18] expected CLM-P3A-0003 reconciliation_status=unbalanced, got %s',
           v_status);
  raise notice 'PASS [12/18] CLM-P3A-0003 reconciliation_status = unbalanced';

  -- CHECK 13/18: open_balance_amount = 180.00
  assert v_balance = 180.00,
    format('FAIL [13/18] expected CLM-P3A-0003 open_balance_amount=180.00, got %s', v_balance);
  raise notice 'PASS [13/18] CLM-P3A-0003 open_balance_amount = 180.00';

  -- CHECK 14/18: short_pay_detected count = 1, amount = 180.00 for CLM-P3A-0003
  select count(*), max(ce.amount)
  into   v_sp_count, v_sp_amount
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-P3A-0003';

  assert v_sp_count = 1,
    format('FAIL [14/18] expected 1 short_pay_detected for CLM-P3A-0003, got %s', v_sp_count);
  assert v_sp_amount = 180.00,
    format('FAIL [14/18] expected short_pay_detected amount=180.00, got %s', v_sp_amount);
  raise notice 'PASS [14/18] CLM-P3A-0003 short_pay_detected count = 1, amount = 180.00';
end;
$$;


-- =============================================================================
-- STEP 7: Review-queue routing.
-- Phase 2 routes is_summary_row observations → 'suspected_summary_row'.
-- Phase 7 routes unbalanced positions   → 'unbalanced_financial_position'.
-- =============================================================================
do $$
declare
  v_summary_count    int;
  v_unbalanced_count int;
begin
  -- CHECK 15/18: suspected_summary_row count = 1
  -- (the page-4 observation with is_summary_row=true, no claim_identifier)
  select count(*) into v_summary_count
  from   fte_review_queue
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  reason      = 'suspected_summary_row';

  assert v_summary_count = 1,
    format('FAIL [15/18] expected 1 suspected_summary_row queue row, got %s', v_summary_count);
  raise notice 'PASS [15/18] review_queue suspected_summary_row count = 1';

  -- CHECK 16/18: unbalanced_financial_position for CLM-P3A-0003 count = 1
  select count(*) into v_unbalanced_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-P3A-0003';

  assert v_unbalanced_count = 1,
    format('FAIL [16/18] expected 1 unbalanced_financial_position queue row for CLM-P3A-0003, got %s',
           v_unbalanced_count);
  raise notice 'PASS [16/18] CLM-P3A-0003 review_queue unbalanced_financial_position count = 1';
end;
$$;


-- =============================================================================
-- STEP 8: Event-evidence links + [SYNTHETIC] prefix invariant.
--
-- Phase 5c creates two fte_event_evidence rows for each payment_applied event
-- whose observation has a check_eft_identifier:
--   Link 1: the page evidence (observation.evidence_id)
--   Link 2: the check_payment stub matched by metadata->>'check_number'
-- CLM-P3A-0001 payment observation has check_eft_identifier = 'SYN-4001',
-- and the fixture includes a check_payment stub with that check_number, so
-- exactly 2 'supports' links are expected.
-- =============================================================================
do $$
declare
  v_link_count int;
  v_syn_count  int;
begin
  -- CHECK 17/18: CLM-P3A-0001 payment_applied has exactly 2 event_evidence links
  select count(*) into v_link_count
  from   fte_event_evidence ee
  join   fte_claim_events ce on ce.id = ee.claim_event_id
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-P3A-0001'
    and  ce.event_type  = 'payment_applied'
    and  ee.link_role   = 'supports';

  assert v_link_count = 2,
    format('FAIL [17/18] expected 2 event_evidence links for CLM-P3A-0001 payment_applied, got %s',
           v_link_count);
  raise notice 'PASS [17/18] CLM-P3A-0001 payment_applied has 2 fte_event_evidence links (page + check stub)';

  -- CHECK 18/18: all non-null raw_text evidence rows start with '[SYNTHETIC]'
  -- Expected count = 4 (pages 1–4); document and check_payment stub have null raw_text.
  select count(*) into v_syn_count
  from   fte_evidence
  where  practice_id = 'a3000000-0000-4000-8000-0000000000fe'
    and  raw_text    is not null
    and  raw_text    like '[SYNTHETIC]%';

  assert v_syn_count = 4,
    format('FAIL [18/18] expected 4 non-null raw_text rows starting with [SYNTHETIC], got %s',
           v_syn_count);
  raise notice 'PASS [18/18] all non-null raw_text evidence rows prefixed [SYNTHETIC] (count = 4)';
end;
$$;


-- =============================================================================
-- STEP 9: Rollback — nothing persists.
-- =============================================================================
rollback;

\echo ''
\echo '=== validate_extraction_pipeline: 18/18 PASS ==='
\echo ''

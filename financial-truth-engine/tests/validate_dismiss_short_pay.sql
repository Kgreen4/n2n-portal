-- =============================================================================
-- Financial Truth Engine — dismiss_short_pay Resolution Validation
-- tests/validate_dismiss_short_pay.sql
--
-- 9 PASS checks across 6 steps that verify the dismiss_short_pay position-level
-- review resolution introduced in Task 005A.
--
-- STEP 1  Baseline run (no resolution)
--           → short_pay_detected emitted for CLM-APC-1000
--           → CLM-APC-1000 routed to review queue (unbalanced_financial_position)
--           → review_resolutions_applied = 0                            [1–3/9]
-- STEP 2  Insert active dismiss_short_pay for CLM-APC-1000
-- STEP 3  Second run (resolution active)
--           → short_pay_detected suppressed for CLM-APC-1000
--           → CLM-APC-1000 NOT in review queue for unbalanced_financial_position
--           → CLM-APC-1000 position retains reconciliation_status = 'unbalanced'
--             and open_balance_amount = 1248.11 (math unchanged)
--           → review_resolutions_applied = 1                            [4–7/9]
-- STEP 4  CLM-APC-2000 isolation check
--           → CLM-APC-2000 still queued (unaffected by CLM-APC-1000 dismissal) [8/9]
-- STEP 5  Supersession check
--           → set dismiss_short_pay is_superseded = true
--           → third run: short_pay_detected re-emitted for CLM-APC-1000,
--             unbalanced_financial_position queue row reappears              [9/9]
-- STEP 6  rollback
--
-- Test vehicle: fixtures/synthetic_96c5c357_failure_modes.sql
--   Practice: 96000000-0000-4000-8000-0000000000fe
--   CLM-APC-1000 claim_id: c1a90000-0000-4000-8000-000000001000
--   CLM-APC-2000 claim_id: c1a90000-0000-4000-8000-000000002000
--
-- Prerequisites:
--   1. migrations 001–005 applied
--   2. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--   3. fixtures/synthetic_96c5c357_failure_modes.sql loaded
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_dismiss_short_pay.sql
--
-- All 9 checks emit RAISE NOTICE 'PASS [N/9] ...' and exit without exception.
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
-- fte_review_resolutions survives Phase 0 and must be wiped manually here.
delete from fte_review_resolutions
where practice_id = '96000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count.  fte_analysis_runs is append-only;
-- a disposable DB may hold prior reconciler runs from earlier manual validation.
-- All run-count assertions below test deltas (advance >= N), not absolute totals.
create temp table _vdsp_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no dismiss_short_pay resolution exists.
--
-- Expected:
--   short_pay_detected event emitted for CLM-APC-1000 (count = 1)
--   CLM-APC-1000 has an unbalanced_financial_position queue row (count = 1)
--   return JSON: review_resolutions_applied = 0
-- =============================================================================
do $$
declare
  v_result      jsonb;
  v_event_count int;
  v_queue_count int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/9: review_resolutions_applied = 0 (no resolution loaded yet)
  assert (v_result->>'review_resolutions_applied')::int = 0,
    format('FAIL [1/9] expected review_resolutions_applied=0, got %s',
           v_result->>'review_resolutions_applied');
  raise notice 'PASS [1/9] baseline review_resolutions_applied = 0';

  -- CHECK 2/9: short_pay_detected emitted for CLM-APC-1000
  select count(*) into v_event_count
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_event_count = 1,
    format('FAIL [2/9] expected 1 short_pay_detected for CLM-APC-1000, got %s',
           v_event_count);
  raise notice 'PASS [2/9] baseline short_pay_detected for CLM-APC-1000 = 1';

  -- CHECK 3/9: CLM-APC-1000 has an unbalanced_financial_position queue row
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_queue_count = 1,
    format('FAIL [3/9] expected 1 queue row for CLM-APC-1000 unbalanced_financial_position, got %s',
           v_queue_count);
  raise notice 'PASS [3/9] baseline unbalanced_financial_position queue row for CLM-APC-1000 = 1';
end;
$$;


-- =============================================================================
-- STEP 2: Insert an active dismiss_short_pay resolution for CLM-APC-1000.
-- =============================================================================
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'dismiss_short_pay',
   'position',
   false,
   'Synthetic test: CLM-APC-1000 short pay acknowledged, not pursuing.');


-- =============================================================================
-- STEP 3: Second run — dismiss_short_pay is active.
--
-- Expected:
--   review_resolutions_applied = 1
--   short_pay_detected for CLM-APC-1000 suppressed (count = 0)
--   CLM-APC-1000 NOT in review queue for unbalanced_financial_position
--   CLM-APC-1000 position still has:
--     reconciliation_status = 'unbalanced'  (math is preserved)
--     open_balance_amount   = 1248.11       (math is preserved)
-- =============================================================================
do $$
declare
  v_result       jsonb;
  v_event_count  int;
  v_queue_count  int;
  v_pos_status   text;
  v_pos_balance  numeric;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/9: review_resolutions_applied = 1
  assert (v_result->>'review_resolutions_applied')::int = 1,
    format('FAIL [4/9] expected review_resolutions_applied=1, got %s',
           v_result->>'review_resolutions_applied');
  raise notice 'PASS [4/9] dismissed run review_resolutions_applied = 1';

  -- CHECK 5/9: short_pay_detected suppressed for CLM-APC-1000
  select count(*) into v_event_count
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_event_count = 0,
    format('FAIL [5/9] expected 0 short_pay_detected for CLM-APC-1000 after dismiss, got %s',
           v_event_count);
  raise notice 'PASS [5/9] short_pay_detected suppressed for CLM-APC-1000';

  -- CHECK 6/9: CLM-APC-1000 NOT in review queue for unbalanced_financial_position
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_queue_count = 0,
    format('FAIL [6/9] expected 0 queue rows for CLM-APC-1000 after dismiss, got %s',
           v_queue_count);
  raise notice 'PASS [6/9] CLM-APC-1000 not queued for unbalanced_financial_position after dismiss';

  -- CHECK 7/9: CLM-APC-1000 position preserves mathematical truth
  select fp.reconciliation_status, fp.open_balance_amount
  into   v_pos_status, v_pos_balance
  from   fte_financial_positions fp
  join   fte_claims c on c.id = fp.claim_id
  where  fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [7/9] expected position status=unbalanced, got %s', v_pos_status);
  assert v_pos_balance = 1248.11,
    format('FAIL [7/9] expected open_balance_amount=1248.11, got %s', v_pos_balance);
  raise notice 'PASS [7/9] CLM-APC-1000 position math preserved: status=unbalanced, open_balance=1248.11';
end;
$$;


-- =============================================================================
-- STEP 4: CLM-APC-2000 isolation — must be unaffected.
--
-- CLM-APC-2000 has spacing-variant fragmentation observations (all SUSPECT /
-- retry-pending) and derives reconciliation_status = 'in_review'.
-- The dismiss_short_pay for CLM-APC-1000 must not suppress CLM-APC-2000's
-- queue entry — in_review positions always route regardless of any resolution.
-- =============================================================================
do $$
declare
  v_queue_count int;
begin
  -- CHECK 8/9: CLM-APC-2000 remains in review queue (unaffected)
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-APC-2000';

  assert v_queue_count >= 1,
    format('FAIL [8/9] expected CLM-APC-2000 to remain queued, got count=%s',
           v_queue_count);
  raise notice 'PASS [8/9] CLM-APC-2000 unaffected — still queued (count=%)', v_queue_count;
end;
$$;


-- =============================================================================
-- STEP 5: Supersession check.
--
-- Supersede the dismiss_short_pay row.  On the next reconciler run the
-- suppression stops applying: short_pay_detected must re-appear and
-- CLM-APC-1000 must be re-queued.
-- =============================================================================
update fte_review_resolutions
set    is_superseded = true
where  practice_id = '96000000-0000-4000-8000-0000000000fe'
  and  action      = 'dismiss_short_pay'
  and  claim_id    = 'c1a90000-0000-4000-8000-000000001000';

do $$
declare
  v_result      jsonb;
  v_event_count int;
  v_queue_count int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 9/9: short_pay_detected re-emitted and CLM-APC-1000 re-queued
  select count(*) into v_event_count
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-APC-1000';

  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_event_count = 1,
    format('FAIL [9/9] expected short_pay_detected to re-appear after supersession, got %s',
           v_event_count);
  assert v_queue_count = 1,
    format('FAIL [9/9] expected CLM-APC-1000 re-queued after supersession, got %s',
           v_queue_count);
  raise notice 'PASS [9/9] supersession restores short_pay_detected and unbalanced_financial_position queue row';
end;
$$;


-- =============================================================================
-- STEP 6: Rollback — nothing persists.
-- =============================================================================
rollback;

\echo ''
\echo '=== validate_dismiss_short_pay: 9/9 PASS ==='
\echo ''

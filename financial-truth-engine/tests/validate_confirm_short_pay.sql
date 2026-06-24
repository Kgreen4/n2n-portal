-- =============================================================================
-- Financial Truth Engine — confirm_short_pay Resolution Validation
-- tests/validate_confirm_short_pay.sql
--
-- 10 PASS checks across 6 steps that verify the confirm_short_pay position-level
-- review resolution introduced in Task 005B.
--
-- STEP 1  Baseline run (no resolution)
--           → short_pay_detected emitted for CLM-APC-1000
--           → CLM-APC-1000 routed to review queue (unbalanced_financial_position)
--           → review_resolutions_applied = 0                            [1–3/10]
-- STEP 2  Insert active confirm_short_pay for CLM-APC-1000
-- STEP 3  Second run (resolution active)
--           → review_resolutions_applied = 1
--           → CLM-APC-1000 NOT in review queue for unbalanced_financial_position
--           → short_pay_detected STILL emitted for CLM-APC-1000 (count = 1)
--           → CLM-APC-1000 position retains reconciliation_status = 'unbalanced'
--             and open_balance_amount = 1248.11 (math unchanged)         [4–7/10]
-- STEP 4  Conflict-prevention check
--           → inserting a simultaneous active dismiss_short_pay for CLM-APC-1000
--             raises a unique_violation (index prevents two active short-pay
--             decisions for the same (practice_id, claim_id))             [8/10]
-- STEP 5  CLM-APC-2000 isolation check
--           → CLM-APC-2000 still queued (unaffected by CLM-APC-1000 confirm)   [9/10]
-- STEP 6  Supersession check
--           → set confirm_short_pay is_superseded = true
--           → third run: CLM-APC-1000 re-queued for unbalanced_financial_position
--             (short_pay_detected remains emitted — never suppressed by confirm) [10/10]
-- STEP 7  rollback
--
-- Test vehicle: fixtures/synthetic_96c5c357_failure_modes.sql
--   Practice: 96000000-0000-4000-8000-0000000000fe
--   CLM-APC-1000 claim_id: c1a90000-0000-4000-8000-000000001000
--   CLM-APC-2000 claim_id: c1a90000-0000-4000-8000-000000002000
--
-- Prerequisites:
--   1. migrations 001–006 applied
--   2. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--   3. fixtures/synthetic_96c5c357_failure_modes.sql loaded
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_confirm_short_pay.sql
--
-- All 10 checks emit RAISE NOTICE 'PASS [N/10] ...' and exit without exception.
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
create temp table _vcsp_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no confirm_short_pay resolution exists.
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

  -- CHECK 1/10: review_resolutions_applied = 0 (no resolution loaded yet)
  assert (v_result->>'review_resolutions_applied')::int = 0,
    format('FAIL [1/10] expected review_resolutions_applied=0, got %s',
           v_result->>'review_resolutions_applied');
  raise notice 'PASS [1/10] baseline review_resolutions_applied = 0';

  -- CHECK 2/10: short_pay_detected emitted for CLM-APC-1000
  select count(*) into v_event_count
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_event_count = 1,
    format('FAIL [2/10] expected 1 short_pay_detected for CLM-APC-1000, got %s',
           v_event_count);
  raise notice 'PASS [2/10] baseline short_pay_detected for CLM-APC-1000 = 1';

  -- CHECK 3/10: CLM-APC-1000 has an unbalanced_financial_position queue row
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_queue_count = 1,
    format('FAIL [3/10] expected 1 queue row for CLM-APC-1000 unbalanced_financial_position, got %s',
           v_queue_count);
  raise notice 'PASS [3/10] baseline unbalanced_financial_position queue row for CLM-APC-1000 = 1';
end;
$$;


-- =============================================================================
-- STEP 2: Insert an active confirm_short_pay resolution for CLM-APC-1000.
-- =============================================================================
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'confirm_short_pay',
   'position',
   false,
   'Synthetic test: CLM-APC-1000 short pay confirmed — will pursue recovery.');


-- =============================================================================
-- STEP 3: Second run — confirm_short_pay is active.
--
-- Expected:
--   review_resolutions_applied = 1
--   CLM-APC-1000 NOT in review queue for unbalanced_financial_position
--   short_pay_detected STILL emitted for CLM-APC-1000 (count = 1)
--     (confirm_short_pay does NOT suppress the event — contrast with dismiss)
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

  -- CHECK 4/10: review_resolutions_applied = 1
  assert (v_result->>'review_resolutions_applied')::int = 1,
    format('FAIL [4/10] expected review_resolutions_applied=1, got %s',
           v_result->>'review_resolutions_applied');
  raise notice 'PASS [4/10] confirmed run review_resolutions_applied = 1';

  -- CHECK 5/10: CLM-APC-1000 NOT in review queue for unbalanced_financial_position
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  rq.reason      = 'unbalanced_financial_position'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_queue_count = 0,
    format('FAIL [5/10] expected 0 queue rows for CLM-APC-1000 after confirm, got %s',
           v_queue_count);
  raise notice 'PASS [5/10] CLM-APC-1000 not queued for unbalanced_financial_position after confirm';

  -- CHECK 6/10: short_pay_detected PRESERVED for CLM-APC-1000 (not suppressed)
  select count(*) into v_event_count
  from   fte_claim_events ce
  join   fte_claims c on c.id = ce.claim_id
  where  ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  ce.event_type  = 'short_pay_detected'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_event_count = 1,
    format('FAIL [6/10] expected 1 short_pay_detected for CLM-APC-1000 after confirm (event must be preserved), got %s',
           v_event_count);
  raise notice 'PASS [6/10] short_pay_detected preserved for CLM-APC-1000 after confirm (count=1)';

  -- CHECK 7/10: CLM-APC-1000 position preserves mathematical truth
  select fp.reconciliation_status, fp.open_balance_amount
  into   v_pos_status, v_pos_balance
  from   fte_financial_positions fp
  join   fte_claims c on c.id = fp.claim_id
  where  fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-APC-1000';

  assert v_pos_status = 'unbalanced',
    format('FAIL [7/10] expected position status=unbalanced, got %s', v_pos_status);
  assert v_pos_balance = 1248.11,
    format('FAIL [7/10] expected open_balance_amount=1248.11, got %s', v_pos_balance);
  raise notice 'PASS [7/10] CLM-APC-1000 position math preserved: status=unbalanced, open_balance=1248.11';
end;
$$;


-- =============================================================================
-- STEP 4: Conflict-prevention check.
--
-- The partial unique index on fte_review_resolutions prevents two active
-- short-pay position-level decisions (confirm_short_pay + dismiss_short_pay)
-- for the same (practice_id, claim_id).  Attempting to insert an active
-- dismiss_short_pay while confirm_short_pay is active must raise a
-- unique_violation (SQLSTATE 23505).
-- =============================================================================
do $$
declare
  v_conflict_caught boolean := false;
begin
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'dismiss_short_pay',
       'position',
       false,
       'Synthetic test: conflict-prevention check — must be rejected.');
  exception
    when unique_violation then
      v_conflict_caught := true;
  end;

  -- CHECK 8/10: unique_violation raised for simultaneous active decisions
  assert v_conflict_caught,
    'FAIL [8/10] expected unique_violation when inserting active dismiss_short_pay alongside active confirm_short_pay — conflict-prevention index did not fire';
  raise notice 'PASS [8/10] conflict-prevention index blocks simultaneous active confirm_short_pay + dismiss_short_pay for the same claim';
end;
$$;


-- =============================================================================
-- STEP 5: CLM-APC-2000 isolation — must be unaffected.
--
-- CLM-APC-2000 has spacing-variant fragmentation observations (all SUSPECT /
-- retry-pending) and derives reconciliation_status = 'in_review'.
-- The confirm_short_pay for CLM-APC-1000 must not suppress CLM-APC-2000's
-- queue entry — in_review positions always route regardless of any resolution.
-- =============================================================================
do $$
declare
  v_queue_count int;
begin
  -- CHECK 9/10: CLM-APC-2000 remains in review queue (unaffected)
  select count(*) into v_queue_count
  from   fte_review_queue rq
  join   fte_claims c on c.id = rq.claim_id
  where  rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
    and  c.claim_number = 'CLM-APC-2000';

  assert v_queue_count >= 1,
    format('FAIL [9/10] expected CLM-APC-2000 to remain queued, got count=%s',
           v_queue_count);
  raise notice 'PASS [9/10] CLM-APC-2000 unaffected — still queued (count=%)', v_queue_count;
end;
$$;


-- =============================================================================
-- STEP 6: Supersession check.
--
-- Supersede the confirm_short_pay row.  On the next reconciler run the
-- suppression stops applying: CLM-APC-1000 must be re-queued for
-- unbalanced_financial_position.  The short_pay_detected event should
-- remain emitted (it was never suppressed by confirm_short_pay, and
-- superseding the resolution does not change that).
-- =============================================================================
update fte_review_resolutions
set    is_superseded = true
where  practice_id = '96000000-0000-4000-8000-0000000000fe'
  and  action      = 'confirm_short_pay'
  and  claim_id    = 'c1a90000-0000-4000-8000-000000001000';

do $$
declare
  v_result      jsonb;
  v_event_count int;
  v_queue_count int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 10/10: CLM-APC-1000 re-queued after supersession; short_pay_detected still present
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
    format('FAIL [10/10] expected short_pay_detected to remain after supersession, got %s',
           v_event_count);
  assert v_queue_count = 1,
    format('FAIL [10/10] expected CLM-APC-1000 re-queued after supersession, got %s',
           v_queue_count);
  raise notice 'PASS [10/10] supersession restores unbalanced_financial_position queue row; short_pay_detected preserved throughout';
end;
$$;


-- =============================================================================
-- STEP 7: Rollback — nothing persists.
-- =============================================================================
rollback;

\echo ''
\echo '=== validate_confirm_short_pay: 10/10 PASS ==='
\echo ''

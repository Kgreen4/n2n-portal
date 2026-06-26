-- =============================================================================
-- Financial Truth Engine — Validation Suite: validate_reject_payment_event
-- tests/validate_reject_payment_event.sql
--
-- Covers: Task 005G (reject_payment_event) — migration 010 constraints +
--         Phase 0.5 temp table + Phase 5c CONTINUE guard in fte_reconcile.sql.
-- 18 numeric checks.  Fixture: synthetic_96c5c357_failure_modes.sql.
--
-- Primary vehicle: CLM-APC-1000 (c1a90000-0000-4000-8000-000000001000)
--   Baseline: billed=$1,600.00, paid=$351.89, open_balance=$1,248.11,
--   status=unbalanced, short_pay_detected emitted, queue row present.
--   With active reject_payment_event: payment_applied event suppressed,
--   paid_amount=NULL, open_balance=$1,600.00 (full billed),
--   short_pay_detected amount=$1,600.00, status remains unbalanced,
--   queue row remains present.
--   _fte_classified.classification remains 'trusted' during the reconciler run.
--   Supersession: restores all baseline values.
--
-- Isolation vehicle: CLM-APC-2000 (c1a90000-0000-4000-8000-000000002000)
--   reject_payment_event on CLM-APC-1000 must not affect CLM-APC-2000's
--   position, events, or queue routing.
--
-- PSQL ONLY.  The \i metacommand at the top is not supported by the Supabase
-- SQL Editor.  Supabase users: paste and run the fixture body first (execute
-- separately so it commits), then paste and run this file's body starting at
-- the `begin;` line below.  Remove or comment out the \i line and the \echo
-- line at the bottom before pasting into the SQL Editor.
--
-- Expected output: 18 PASS NOTICE lines.
-- A FAIL raises an EXCEPTION that aborts the transaction.
-- All changes are rolled back at the end — nothing persists.
-- =============================================================================

\i fixtures/synthetic_96c5c357_failure_modes.sql

begin;

-- Clear any leftover review resolutions from prior runs so all checks start
-- from a known-clean baseline.
delete from fte_review_resolutions
  where practice_id = '96000000-0000-4000-8000-0000000000fe';

-- -------------------------------------------------------------------------
-- STEP 1: Baseline reconciler run — no review resolutions active.
-- Checks 1/18 through 5/18.
-- -------------------------------------------------------------------------
do $$
declare
  v_result           jsonb;
  v_paid_amount      numeric;
  v_open_balance     numeric;
  v_status           text;
  v_event_count      int;
  v_short_pay_amount numeric;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 1/18 — baseline: review_resolutions_applied = 0.
  assert (v_result->>'review_resolutions_applied')::int = 0,
    'FAIL [1/18] expected review_resolutions_applied=0, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [1/18] baseline: review_resolutions_applied = 0';

  -- 2/18 — CLM-APC-1000 baseline: payment_applied event exists (1 event).
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and event_type  = 'payment_applied';
  assert v_event_count = 1,
    'FAIL [2/18] expected 1 payment_applied event at baseline, got: '
    || v_event_count::text;
  raise notice 'PASS [2/18] baseline: payment_applied event present (count=1)';

  -- 3/18 — CLM-APC-1000 baseline: paid_amount = $351.89 and
  --         open_balance_amount = $1,248.11 (both derived in Phase 6 from
  --         the same SUM of claim events).
  select fp.paid_amount, fp.open_balance_amount, fp.reconciliation_status
    into v_paid_amount, v_open_balance, v_status
    from fte_financial_positions fp
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and fp.claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_paid_amount = 351.89 and v_open_balance = 1248.11,
    'FAIL [3/18] expected paid_amount=351.89 open_balance=1248.11 at baseline, got: '
    || 'paid=' || coalesce(v_paid_amount::text, 'NULL')
    || ' open=' || coalesce(v_open_balance::text, 'NULL');
  raise notice 'PASS [3/18] baseline: paid_amount=$351.89, open_balance=$1,248.11';

  -- 4/18 — CLM-APC-1000 baseline: status = unbalanced.
  assert v_status = 'unbalanced',
    'FAIL [4/18] expected status=unbalanced at baseline, got: '
    || coalesce(v_status, 'NULL');
  raise notice 'PASS [4/18] baseline: status = unbalanced';

  -- 5/18 — CLM-APC-1000 baseline: short_pay_detected.amount = $1,248.11.
  select ce.amount into v_short_pay_amount
    from fte_claim_events ce
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and ce.event_type  = 'short_pay_detected'
   order by ce.created_at desc
   limit 1;
  assert v_short_pay_amount = 1248.11,
    'FAIL [5/18] expected short_pay_detected amount=1248.11 at baseline, got: '
    || coalesce(v_short_pay_amount::text, 'NULL');
  raise notice 'PASS [5/18] baseline: short_pay_detected amount = $1,248.11';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 2: Insert active reject_payment_event for CLM-APC-1000.
-- -------------------------------------------------------------------------
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes,
   resolved_by, resolved_at)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'reject_payment_event',
   'payment_event',
   false,
   'Payment disputed — check #2-1835212 was applied in error per payer callback 2026-06-25.',
   'test-reviewer',
   now());

-- -------------------------------------------------------------------------
-- STEP 3: Reconciler run with active reject_payment_event.
-- Checks 6/18 through 14/18.
-- -------------------------------------------------------------------------
do $$
declare
  v_result           jsonb;
  v_paid_amount      numeric;
  v_open_balance     numeric;
  v_status           text;
  v_event_count      int;
  v_queue_count      int;
  v_short_pay_amount numeric;
  v_obs_count        int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 6/18 — review_resolutions_applied = 1 (the reject_payment_event row).
  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [6/18] expected review_resolutions_applied=1, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [6/18] reject active: review_resolutions_applied = 1';

  -- 7/18 — payment_applied event is suppressed (0 events for CLM-APC-1000).
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and event_type  = 'payment_applied';
  assert v_event_count = 0,
    'FAIL [7/18] expected 0 payment_applied events after reject, got: '
    || v_event_count::text;
  raise notice 'PASS [7/18] reject active: payment_applied event suppressed';

  -- 8/18 — paid_amount IS NULL (Phase 6 SUM of zero payment_applied events
  --         returns NULL, not 0 — confirming the suppression reaches the
  --         position row, not just the event count).
  select fp.paid_amount, fp.open_balance_amount, fp.reconciliation_status
    into v_paid_amount, v_open_balance, v_status
    from fte_financial_positions fp
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and fp.claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_paid_amount is null,
    'FAIL [8/18] expected paid_amount IS NULL after reject (no payment events), got: '
    || coalesce(v_paid_amount::text, 'NULL');
  raise notice 'PASS [8/18] reject active: paid_amount IS NULL (no payment event emitted)';

  -- 9/18 — open_balance recalculates to full billed amount = $1,600.00.
  assert v_open_balance = 1600.00,
    'FAIL [9/18] expected open_balance=1600.00 after reject, got: '
    || coalesce(v_open_balance::text, 'NULL');
  raise notice 'PASS [9/18] reject active: open_balance = $1,600.00 (full billed)';

  -- 10/18 — reconciliation_status remains unbalanced (Phase 7 NOT suppressed).
  assert v_status = 'unbalanced',
    'FAIL [10/18] expected status=unbalanced after reject, got: '
    || coalesce(v_status, 'NULL');
  raise notice 'PASS [10/18] reject active: status = unbalanced (Phase 7 not suppressed)';

  -- 11/18 — Phase 7 queue row still present for CLM-APC-1000.
  select count(*) into v_queue_count
    from fte_review_queue
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_queue_count >= 1,
    'FAIL [11/18] expected queue row present after reject, got count: '
    || v_queue_count::text;
  raise notice 'PASS [11/18] reject active: unbalanced queue row preserved';

  -- 12/18 — short_pay_detected amount = $1,600.00 (Phase 8 NOT suppressed;
  --          amount recalculates from the rejected-payment open balance).
  select ce.amount into v_short_pay_amount
    from fte_claim_events ce
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and ce.event_type  = 'short_pay_detected'
   order by ce.created_at desc
   limit 1;
  assert v_short_pay_amount = 1600.00,
    'FAIL [12/18] expected short_pay_detected amount=1600.00, got: '
    || coalesce(v_short_pay_amount::text, 'NULL');
  raise notice 'PASS [12/18] reject active: short_pay_detected amount = $1,600.00 (Phase 8 not suppressed)';

  -- 13/18 — payment observation remains classified trusted in Phase 1.
  -- reject_payment_event must suppress only Phase 5c payment_applied event
  -- emission; it must not add the observation to _fte_suppressed_observations
  -- or otherwise prevent trusted classification.
  select count(*) into v_obs_count
    from _fte_classified
   where practice_id       = '96000000-0000-4000-8000-0000000000fe'
     and claim_identifier  = 'CLM-APC-1000'
     and observation_type  = 'payment'
     and classification    = 'trusted';

  assert v_obs_count >= 1,
    'FAIL [13/18] expected payment observation to remain classified trusted after reject, got count: '
    || v_obs_count::text;

  raise notice 'PASS [13/18] reject active: payment observation remains classified trusted (Phase 1 unchanged)';

  -- 14/18 — CLM-APC-2000 isolation: reject on CLM-APC-1000 does not affect CLM-APC-2000.
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000002000'
     and event_type  = 'payment_applied';
  assert v_event_count = 0,
    'FAIL [14/18] CLM-APC-2000 isolation: unexpected payment_applied event, got: '
    || v_event_count::text;
  raise notice 'PASS [14/18] CLM-APC-2000 isolation: reject does not bleed across claims';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 4: Migration 010 constraint checks.
-- Check 15/18 — notes required.
-- Check 16/18 — claim_id required; wrong target_type rejected (bundled).
-- Check 17/18 — cross-action conflict: confirm + reject cannot coexist.
-- -------------------------------------------------------------------------
do $$
declare
  v_caught boolean;
begin
  -- 15/18 — reject_payment_event requires non-blank notes.
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'reject_payment_event',
       'payment_event',
       false,
       '',   -- blank notes — should violate constraint
       'test-reviewer',
       now());
  exception when check_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [15/18] expected check_violation for blank notes on reject_payment_event';
  raise notice 'PASS [15/18] constraint: blank notes rejected on reject_payment_event';

  -- 16/18 — (a) reject_payment_event requires claim_id IS NOT NULL;
  --          (b) reject_payment_event requires target_type = payment_event
  --              (wrong target_type = observation must be rejected).
  --          Both are bundled under one check number since they test the same
  --          migration 010 constraint family (shape constraints on the action).
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       null,   -- missing claim_id — should violate constraint
       'reject_payment_event',
       'payment_event',
       false,
       'Valid note for constraint test.',
       'test-reviewer',
       now());
  exception when check_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [16/18] expected check_violation for null claim_id on reject_payment_event';

  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'reject_payment_event',
       'observation',   -- wrong target_type — should violate constraint
       false,
       'Valid note for constraint test.',
       'test-reviewer',
       now());
  exception when check_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [16/18] expected check_violation for target_type=observation on reject_payment_event';
  raise notice 'PASS [16/18] constraint: null claim_id and wrong target_type both rejected';

  -- 17/18 — Cross-action conflict: idx_fte_resolutions_single_active_payment_event_decision
  --          prevents simultaneous active confirm_payment_event + reject_payment_event
  --          for the same (practice_id, claim_id).
  --          An active reject_payment_event for CLM-APC-1000 is already present
  --          (inserted in STEP 2).  Attempting to insert a non-superseded
  --          confirm_payment_event for the same claim must raise unique_violation.
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'confirm_payment_event',
       'payment_event',
       false,
       'Conflict test — confirm while reject is active.',
       'test-reviewer',
       now());
  exception when unique_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [17/18] expected unique_violation for simultaneous confirm + reject on same claim';
  raise notice 'PASS [17/18] cross-action conflict: confirm_payment_event blocked by active reject_payment_event';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 5: Supersession test — supersede the reject resolution and verify
-- that baseline behavior is fully restored.
-- Check 18/18.
-- -------------------------------------------------------------------------
update fte_review_resolutions
   set is_superseded = true
 where practice_id = '96000000-0000-4000-8000-0000000000fe'
   and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
   and action      = 'reject_payment_event'
   and is_superseded = false;

do $$
declare
  v_result           jsonb;
  v_paid_amount      numeric;
  v_open_balance     numeric;
  v_event_count      int;
  v_queue_count      int;
  v_short_pay_amount numeric;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 18/18 — Supersession restores all baseline values:
  --   (a) payment_applied event present (count=1)
  --   (b) paid_amount = $351.89
  --   (c) open_balance_amount = $1,248.11
  --   (d) short_pay_detected.amount = $1,248.11
  --   (e) queue row still present (unbalanced position routes to queue)
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and event_type  = 'payment_applied';
  assert v_event_count = 1,
    'FAIL [18/18] supersession: expected payment_applied count=1 restored, got: '
    || v_event_count::text;

  select fp.paid_amount, fp.open_balance_amount
    into v_paid_amount, v_open_balance
    from fte_financial_positions fp
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and fp.claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_paid_amount = 351.89,
    'FAIL [18/18] supersession: expected paid_amount=351.89 restored, got: '
    || coalesce(v_paid_amount::text, 'NULL');
  assert v_open_balance = 1248.11,
    'FAIL [18/18] supersession: expected open_balance=1248.11 restored, got: '
    || coalesce(v_open_balance::text, 'NULL');

  select ce.amount into v_short_pay_amount
    from fte_claim_events ce
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and ce.event_type  = 'short_pay_detected'
   order by ce.created_at desc
   limit 1;
  assert v_short_pay_amount = 1248.11,
    'FAIL [18/18] supersession: expected short_pay_detected amount=1248.11 restored, got: '
    || coalesce(v_short_pay_amount::text, 'NULL');

  select count(*) into v_queue_count
    from fte_review_queue
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_queue_count >= 1,
    'FAIL [18/18] supersession: expected queue row present after supersession, got count: '
    || v_queue_count::text;

  raise notice 'PASS [18/18] supersession: baseline fully restored (payment_applied=1, paid=$351.89, open=$1,248.11, short_pay=$1,248.11, queue present)';
end;
$$;

rollback;

\echo '--- validate_reject_payment_event: all 18 checks passed ---'

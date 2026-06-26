-- =============================================================================
-- Financial Truth Engine — Validation Suite: validate_assert_check_identity
-- tests/validate_assert_check_identity.sql
--
-- Covers: Task 005H (assert_check_identity) — migration 011 constraints.
-- 12 numeric checks.  Fixture: synthetic_96c5c357_failure_modes.sql.
--
-- Primary vehicle: CLM-APC-1000 (c1a90000-0000-4000-8000-000000001000)
--   Baseline: billed=$1,600.00, paid=$351.89, open_balance=$1,248.11,
--   status=unbalanced.
--   With active assert_check_identity (corrected_identifier='CK-0001'):
--   review_resolutions_applied=1; payment_applied event still emits;
--   position still unbalanced at open_balance=$1,248.11 (durable note only —
--   no reconciler phase changes); corrected_identifier stored correctly.
--   Supersession: UPDATE is_superseded=true, INSERT new assertion with
--   corrected_identifier='CK-0002'; reconcile; count=1; new identifier stored.
--
-- Isolation vehicle: CLM-APC-2000 (c1a90000-0000-4000-8000-000000002000)
--   assert_check_identity on CLM-APC-1000 must not affect CLM-APC-2000.
--
-- PSQL ONLY.  The \i metacommand at the top is not supported by the Supabase
-- SQL Editor.  Supabase users: paste and run the fixture body first (execute
-- separately so it commits), then paste and run this file's body starting at
-- the `begin;` line below.  Remove or comment out the \i line and the \echo
-- line at the bottom before pasting into the SQL Editor.
--
-- Expected output: 12 PASS NOTICE lines.
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
-- Checks 1/12 through 3/12.
-- -------------------------------------------------------------------------
do $$
declare
  v_result       jsonb;
  v_event_count  int;
  v_open_balance numeric;
  v_status       text;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 1/12 — baseline: CLM-APC-1000 has at least one payment_applied event.
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and event_type  = 'payment_applied';
  assert v_event_count >= 1,
    'FAIL [1/12] expected payment_applied event at baseline, got count: '
    || v_event_count::text;
  raise notice 'PASS [1/12] baseline: payment_applied event present (count >= 1)';

  -- 2/12 — baseline: position unbalanced, open_balance_amount = $1,248.11.
  select fp.open_balance_amount, fp.reconciliation_status
    into v_open_balance, v_status
    from fte_financial_positions fp
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and fp.claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_status = 'unbalanced' and v_open_balance = 1248.11,
    'FAIL [2/12] expected status=unbalanced open_balance=1248.11 at baseline, got: '
    || 'status=' || coalesce(v_status, 'NULL')
    || ' open=' || coalesce(v_open_balance::text, 'NULL');
  raise notice 'PASS [2/12] baseline: status=unbalanced, open_balance=$1,248.11';

  -- 3/12 — baseline: review_resolutions_applied = 0.
  assert (v_result->>'review_resolutions_applied')::int = 0,
    'FAIL [3/12] expected review_resolutions_applied=0 at baseline, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [3/12] baseline: review_resolutions_applied = 0';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 2: Insert valid assert_check_identity for CLM-APC-1000.
-- -------------------------------------------------------------------------
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes,
   corrected_identifier, resolved_by, resolved_at)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'assert_check_identity',
   'payment_event',
   false,
   'Check #CK-0001 is the canonical identifier; pages 2-3 display a per-page reference number that is not the physical check number.',
   'CK-0001',
   'test-reviewer',
   now());

-- -------------------------------------------------------------------------
-- STEP 3: Reconciler run with active assert_check_identity.
-- Checks 4/12 through 7/12.
-- -------------------------------------------------------------------------
do $$
declare
  v_result              jsonb;
  v_event_count         int;
  v_open_balance        numeric;
  v_status              text;
  v_corrected_id        text;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 4/12 — review_resolutions_applied = 1 (the assert_check_identity row is
  --         loaded into _fte_active_resolutions by Phase 0.5).
  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [4/12] expected review_resolutions_applied=1 after assert, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [4/12] assert active: review_resolutions_applied = 1';

  -- 5/12 — payment_applied event still emits (durable note — Phase 5c unchanged).
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and event_type  = 'payment_applied';
  assert v_event_count >= 1,
    'FAIL [5/12] expected payment_applied event still present after assert, got count: '
    || v_event_count::text;
  raise notice 'PASS [5/12] assert active: payment_applied event not suppressed (Phase 5c unchanged)';

  -- 6/12 — position still unbalanced at open_balance=$1,248.11 (no financial change).
  select fp.open_balance_amount, fp.reconciliation_status
    into v_open_balance, v_status
    from fte_financial_positions fp
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and fp.claim_id    = 'c1a90000-0000-4000-8000-000000001000';
  assert v_status = 'unbalanced' and v_open_balance = 1248.11,
    'FAIL [6/12] expected status=unbalanced open_balance=1248.11 after assert, got: '
    || 'status=' || coalesce(v_status, 'NULL')
    || ' open=' || coalesce(v_open_balance::text, 'NULL');
  raise notice 'PASS [6/12] assert active: position unchanged (status=unbalanced, open_balance=$1,248.11)';

  -- 7/12 — corrected_identifier is stored correctly in fte_review_resolutions.
  select corrected_identifier into v_corrected_id
    from fte_review_resolutions
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
     and action      = 'assert_check_identity'
     and is_superseded = false;
  assert v_corrected_id = 'CK-0001',
    'FAIL [7/12] expected corrected_identifier=''CK-0001'', got: '
    || coalesce(v_corrected_id, 'NULL');
  raise notice 'PASS [7/12] assert active: corrected_identifier = ''CK-0001'' stored correctly';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 4: Migration 011 constraint checks.
-- Check 8/12 — duplicate active assertion raises unique_violation.
-- Check 9/12 — blank corrected_identifier raises check_violation.
-- Check 10/12 — blank notes raises check_violation.
-- -------------------------------------------------------------------------
do $$
declare
  v_caught boolean;
begin
  -- 8/12 — Duplicate active assert_check_identity for same (practice_id, claim_id)
  --         must raise unique_violation (single-action partial unique index
  --         idx_fte_resolutions_single_active_check_identity).
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       corrected_identifier, resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'assert_check_identity',
       'payment_event',
       false,
       'Second active assertion — should be rejected by unique index.',
       'CK-0001-DUPE',
       'test-reviewer',
       now());
  exception when unique_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [8/12] expected unique_violation for duplicate active assert_check_identity';
  raise notice 'PASS [8/12] constraint: duplicate active assertion rejected (unique_violation)';

  -- 9/12 — blank corrected_identifier must raise check_violation
  --         (fte_review_resolutions_aci_needs_corrected_identifier).
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       corrected_identifier, resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000002000',  -- different claim to avoid unique conflict
       'assert_check_identity',
       'payment_event',
       false,
       'Valid note but blank corrected_identifier.',
       '',   -- blank corrected_identifier — should violate constraint
       'test-reviewer',
       now());
  exception when check_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [9/12] expected check_violation for blank corrected_identifier on assert_check_identity';
  raise notice 'PASS [9/12] constraint: blank corrected_identifier rejected (check_violation)';

  -- 10/12 — blank notes must raise check_violation
  --          (fte_review_resolutions_aci_needs_notes).
  v_caught := false;
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes,
       corrected_identifier, resolved_by, resolved_at)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000002000',  -- different claim to avoid unique conflict
       'assert_check_identity',
       'payment_event',
       false,
       '',   -- blank notes — should violate constraint
       'CK-VALID',
       'test-reviewer',
       now());
  exception when check_violation then
    v_caught := true;
  end;
  assert v_caught,
    'FAIL [10/12] expected check_violation for blank notes on assert_check_identity';
  raise notice 'PASS [10/12] constraint: blank notes rejected (check_violation)';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 5: CLM-APC-2000 isolation.
-- Check 11/12 — assert on CLM-APC-1000 does not affect CLM-APC-2000.
-- -------------------------------------------------------------------------
do $$
declare
  v_event_count  int;
  v_queue_count  int;
begin
  -- 11/12 — CLM-APC-2000 has no payment_applied event (baseline for this
  --          claim in the 96c5c357 fixture is zero-payment / suspect
  --          observations only).  An active assert on CLM-APC-1000 must not
  --          bleed across to CLM-APC-2000's event or queue state.
  select count(*) into v_event_count
    from fte_claim_events
   where practice_id = '96000000-0000-4000-8000-0000000000fe'
     and claim_id    = 'c1a90000-0000-4000-8000-000000002000'
     and event_type  = 'payment_applied';
  assert v_event_count = 0,
    'FAIL [11/12] CLM-APC-2000 isolation: unexpected payment_applied event, got count: '
    || v_event_count::text;
  raise notice 'PASS [11/12] CLM-APC-2000 isolation: assert on CLM-APC-1000 does not bleed across claims';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 6: Supersession — UPDATE is_superseded = true, INSERT new assertion
-- with corrected_identifier = 'CK-0002', reconcile.
-- Check 12/12 — review_resolutions_applied = 1 (one active row); new
-- corrected_identifier stored correctly.
-- -------------------------------------------------------------------------
update fte_review_resolutions
   set is_superseded = true
 where practice_id = '96000000-0000-4000-8000-0000000000fe'
   and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
   and action      = 'assert_check_identity'
   and is_superseded = false;

insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes,
   corrected_identifier, resolved_by, resolved_at)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'assert_check_identity',
   'payment_event',
   false,
   'Revised canonical identifier after payer confirmation — CK-0002 is correct.',
   'CK-0002',
   'test-reviewer',
   now());

do $$
declare
  v_result       jsonb;
  v_corrected_id text;
  v_active_count int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 12/12 — Supersession: exactly one active assert_check_identity row remains
  --          (the superseded row is retained but does not load into
  --          _fte_active_resolutions); review_resolutions_applied = 1;
  --          corrected_identifier on the active row = 'CK-0002'.
  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [12/12] supersession: expected review_resolutions_applied=1, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');

  select count(*) into v_active_count
    from fte_review_resolutions
   where practice_id   = '96000000-0000-4000-8000-0000000000fe'
     and claim_id      = 'c1a90000-0000-4000-8000-000000001000'
     and action        = 'assert_check_identity'
     and is_superseded = false;
  assert v_active_count = 1,
    'FAIL [12/12] supersession: expected 1 active assert row, got: '
    || v_active_count::text;

  select corrected_identifier into v_corrected_id
    from fte_review_resolutions
   where practice_id   = '96000000-0000-4000-8000-0000000000fe'
     and claim_id      = 'c1a90000-0000-4000-8000-000000001000'
     and action        = 'assert_check_identity'
     and is_superseded = false;
  assert v_corrected_id = 'CK-0002',
    'FAIL [12/12] supersession: expected corrected_identifier=''CK-0002'', got: '
    || coalesce(v_corrected_id, 'NULL');

  raise notice 'PASS [12/12] supersession: review_resolutions_applied=1, active count=1, corrected_identifier=''CK-0002''';
end;
$$;

rollback;

\echo '--- validate_assert_check_identity: all 12 checks passed ---'

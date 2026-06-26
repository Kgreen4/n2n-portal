-- =============================================================================
-- Financial Truth Engine — Validation Suite: validate_mark_position_resolved
-- tests/validate_mark_position_resolved.sql
--
-- Covers: Task 005F (mark_position_resolved) — migration 009 constraints +
--         Phase 7 queue suppression in fte_reconcile.sql.
-- 14 numeric checks.  Fixture: synthetic_96c5c357_failure_modes.sql.
--
-- Primary vehicle:  CLM-APC-1000 (c1a90000-0000-4000-8000-000000001000)
--   Has payment_applied + short_pay_detected events, status = unbalanced,
--   open_balance = $1,248.11.  An active mark_position_resolved suppresses the
--   Phase 7 unbalanced_financial_position queue row but preserves Phase 8
--   (short_pay_detected event is NOT suppressed).
--
-- In-review invariant vehicle: CLM-APC-2000 (c1a90000-0000-4000-8000-000000002000)
--   All-SUSPECT observations, no events emitted, reconciler status = in_review.
--   An active mark_position_resolved for CLM-APC-2000 must NOT suppress its
--   Phase 7 queue row — in_review positions always route regardless of any
--   resolution.
--
-- PSQL ONLY.  The \i metacommand at the top is not supported by the Supabase
-- SQL Editor.  Supabase users: paste and run the fixture body first (execute
-- separately so it commits), then paste and run this file's body starting at
-- the `begin;` line below.  Remove or comment out the \i line and the \echo
-- line at the bottom before pasting into the SQL Editor.
--
-- Expected output: 14 PASS NOTICE lines.
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
-- Checks 1/14 through 5/14.
-- -------------------------------------------------------------------------
do $$
declare
  v_result      jsonb;
  v_status      text;
  v_event_count int;
  v_queue_count int;
begin
  -- Baseline: zero resolutions in fte_review_resolutions.
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 1/14 — review_resolutions_applied is zero (no resolutions exist yet).
  assert (v_result->>'review_resolutions_applied')::int = 0,
    'FAIL [1/14] expected review_resolutions_applied=0, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [1/14] baseline: review_resolutions_applied = 0';

  -- 2/14 — CLM-APC-1000 position status is unbalanced.
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-1000';
  assert v_status = 'unbalanced',
    'FAIL [2/14] expected CLM-APC-1000 reconciliation_status=unbalanced, got: '
    || coalesce(v_status, 'NULL');
  raise notice 'PASS [2/14] baseline: CLM-APC-1000 reconciliation_status = unbalanced';

  -- 3/14 — CLM-APC-1000 has exactly one short_pay_detected event.
  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.event_type  = 'short_pay_detected'
     and c.claim_number = 'CLM-APC-1000';
  assert v_event_count = 1,
    'FAIL [3/14] expected CLM-APC-1000 short_pay_detected count = 1, got: '
    || v_event_count::text;
  raise notice 'PASS [3/14] baseline: CLM-APC-1000 has exactly 1 short_pay_detected event';

  -- 4/14 — CLM-APC-1000 has exactly one unbalanced_financial_position queue row.
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and rq.reason      = 'unbalanced_financial_position'
     and c.claim_number = 'CLM-APC-1000';
  assert v_queue_count = 1,
    'FAIL [4/14] expected CLM-APC-1000 unbalanced_financial_position queue count = 1, got: '
    || v_queue_count::text;
  raise notice 'PASS [4/14] baseline: CLM-APC-1000 has exactly 1 unbalanced_financial_position queue row';

  -- 5/14 — CLM-APC-2000 is in_review and has at least one queue entry.
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_status = 'in_review',
    'FAIL [5/14] expected CLM-APC-2000 reconciliation_status=in_review, got: '
    || coalesce(v_status, 'NULL');
  assert v_queue_count >= 1,
    'FAIL [5/14] expected CLM-APC-2000 queue count >= 1, got: ' || v_queue_count::text;
  raise notice 'PASS [5/14] baseline: CLM-APC-2000 is in_review and queued (queue count = %)', v_queue_count;
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 2: Insert a valid mark_position_resolved for CLM-APC-1000.
--
-- Shape requirements (migration 009):
--   action = 'mark_position_resolved'
--   target_type = 'position'
--   claim_id IS NOT NULL (stable anchor; source_position_id goes stale)
--   notes IS NOT NULL AND btrim(notes) <> '' (required explanation)
--   is_superseded = false (active resolved marker)
-- -------------------------------------------------------------------------
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000001000',
   'mark_position_resolved',
   'position',
   false,
   'SYNTHETIC TEST NOTE — CLM-APC-1000 reviewed 2026-06-25.  Short pay of '
   '$1,248.11 accepted as a contractual write-off per billing director '
   'approval.  No further queue routing needed.  short_pay_detected event '
   'preserved for downstream audit trail.');


-- -------------------------------------------------------------------------
-- STEP 3: Second reconciler run — mark_position_resolved active for CLM-APC-1000.
-- Checks 6/14 through 10/14.
--
-- Phase 7: queue row for CLM-APC-1000 must be SUPPRESSED.
-- Phase 8: short_pay_detected event must be PRESERVED.
-- Phase 6: reconciliation_status and open_balance_amount must be UNCHANGED.
-- CLM-APC-2000: in_review position must still route (invariant preserved).
-- -------------------------------------------------------------------------
do $$
declare
  v_result       jsonb;
  v_status       text;
  v_balance      numeric;
  v_event_count  int;
  v_queue_count  int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 6/14 — review_resolutions_applied = 1 (resolution counted by Phase 0.5).
  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [6/14] expected review_resolutions_applied=1, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [6/14] after resolution: review_resolutions_applied = 1';

  -- 7/14 — CLM-APC-1000 position math is preserved: still unbalanced, still
  --          open_balance_amount = $1,248.11.  mark_position_resolved never
  --          changes reconciliation_status or financial math.
  select fp.reconciliation_status, fp.open_balance_amount
    into v_status, v_balance
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-1000';
  assert v_status = 'unbalanced',
    'FAIL [7/14] expected CLM-APC-1000 still unbalanced after mark_position_resolved, got: '
    || coalesce(v_status, 'NULL');
  assert v_balance = 1248.11,
    'FAIL [7/14] expected CLM-APC-1000 open_balance_amount=1248.11, got: '
    || coalesce(v_balance::text, 'NULL');
  raise notice 'PASS [7/14] after resolution: CLM-APC-1000 math preserved (unbalanced, open_balance=$1,248.11)';

  -- 8/14 — CLM-APC-1000 still has exactly one short_pay_detected event.
  --          Phase 8 is NOT suppressed by mark_position_resolved — contrast
  --          with dismiss_short_pay, which suppresses Phase 8.
  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.event_type  = 'short_pay_detected'
     and c.claim_number = 'CLM-APC-1000';
  assert v_event_count = 1,
    'FAIL [8/14] expected CLM-APC-1000 short_pay_detected count still = 1 (Phase 8 not suppressed), got: '
    || v_event_count::text;
  raise notice 'PASS [8/14] after resolution: CLM-APC-1000 short_pay_detected preserved (Phase 8 not suppressed)';

  -- 9/14 — CLM-APC-1000 has ZERO unbalanced_financial_position queue rows.
  --          Phase 7 suppression is active: mark_position_resolved is now in
  --          the IN-list alongside dismiss_short_pay and confirm_short_pay.
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and rq.reason      = 'unbalanced_financial_position'
     and c.claim_number = 'CLM-APC-1000';
  assert v_queue_count = 0,
    'FAIL [9/14] expected CLM-APC-1000 unbalanced_financial_position queue count = 0 (Phase 7 suppressed), got: '
    || v_queue_count::text;
  raise notice 'PASS [9/14] after resolution: CLM-APC-1000 queue row suppressed (Phase 7 suppression active)';

  -- 10/14 — CLM-APC-2000 is unaffected: still in_review, event count = 0,
  --           still queued.  The in_review invariant must hold: Phase 7 suppresses
  --           only when reconciliation_status = 'unbalanced', so in_review
  --           positions always route regardless of any active resolution.
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_status = 'in_review',
    'FAIL [10/14] isolation: expected CLM-APC-2000 still in_review, got: '
    || coalesce(v_status, 'NULL');
  assert v_event_count = 0,
    'FAIL [10/14] isolation: expected CLM-APC-2000 event count = 0, got: '
    || v_event_count::text;
  assert v_queue_count >= 1,
    'FAIL [10/14] isolation: expected CLM-APC-2000 queue count >= 1, got: '
    || v_queue_count::text;
  raise notice 'PASS [10/14] isolation: CLM-APC-2000 unaffected (in_review, 0 events, queued)';
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 4: Uniqueness constraint — check 11/14.
--
-- Inserting a second active mark_position_resolved for the same
-- (practice_id, claim_id) must raise unique_violation.
-- idx_fte_resolutions_single_active_position_resolved enforces this.
-- -------------------------------------------------------------------------
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
       'mark_position_resolved',
       'position',
       false,
       'SYNTHETIC TEST — second simultaneous active resolved marker; '
       'must be rejected by idx_fte_resolutions_single_active_position_resolved.');
  exception
    when unique_violation then
      v_conflict_caught := true;
  end;
  assert v_conflict_caught,
    'FAIL [11/14] expected unique_violation when inserting second active '
    'mark_position_resolved for same (practice_id, claim_id)';
  raise notice 'PASS [11/14] uniqueness: second active mark_position_resolved for CLM-APC-1000 raises unique_violation';
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 5: Notes constraint — check 12/14.
--
-- NULL notes and blank notes must both raise check_violation for
-- fte_review_resolutions_mpr_needs_notes.
--
-- Inserts target CLM-APC-2000 to avoid simultaneously triggering the unique
-- index for CLM-APC-1000 — only the CHECK constraint should fire.
-- -------------------------------------------------------------------------
do $$
declare
  v_null_caught  boolean := false;
  v_blank_caught boolean := false;
begin
  -- NULL notes must be rejected.
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000002000',
       'mark_position_resolved',
       'position',
       false,
       null);
  exception
    when check_violation then
      v_null_caught := true;
  end;

  -- Blank (whitespace-only) notes must also be rejected.
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000002000',
       'mark_position_resolved',
       'position',
       false,
       '   ');
  exception
    when check_violation then
      v_blank_caught := true;
  end;

  assert v_null_caught and v_blank_caught,
    'FAIL [12/14] expected check_violation for both NULL notes and blank notes; '
    || 'null_caught=' || v_null_caught::text
    || ' blank_caught=' || v_blank_caught::text;
  raise notice 'PASS [12/14] notes constraint: NULL notes and blank notes both raise check_violation';
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 6: claim_id and target_type constraints — check 13/14.
--
-- NULL claim_id must raise check_violation for
-- fte_review_resolutions_mpr_needs_claim_id.
-- Wrong target_type (e.g. 'observation') must raise check_violation for
-- fte_review_resolutions_mpr_needs_position_type.
-- -------------------------------------------------------------------------
do $$
declare
  v_null_claim_caught boolean := false;
  v_wrong_type_caught boolean := false;
begin
  -- NULL claim_id must be rejected.
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       null,
       'mark_position_resolved',
       'position',
       false,
       'SYNTHETIC TEST — missing claim_id; must be rejected by mpr_needs_claim_id.');
  exception
    when check_violation then
      v_null_claim_caught := true;
  end;

  -- target_type = 'observation' must be rejected (only 'position' allowed).
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000002000',
       'mark_position_resolved',
       'observation',
       false,
       'SYNTHETIC TEST — wrong target_type; must be rejected by mpr_needs_position_type.');
  exception
    when check_violation then
      v_wrong_type_caught := true;
  end;

  assert v_null_claim_caught and v_wrong_type_caught,
    'FAIL [13/14] expected check_violation for NULL claim_id and wrong target_type; '
    || 'null_claim_caught=' || v_null_claim_caught::text
    || ' wrong_type_caught=' || v_wrong_type_caught::text;
  raise notice 'PASS [13/14] shape constraints: NULL claim_id and target_type=observation both raise check_violation';
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 7: Supersession + in_review invariant — check 14/14.
--
-- Part A: supersede the CLM-APC-1000 resolved marker.
--   Third reconciler run must restore the unbalanced_financial_position queue
--   row for CLM-APC-1000 (suppression no longer active).
--   short_pay_detected must remain emitted (it was never suppressed).
--   review_resolutions_applied = 0 (superseded row excluded by Phase 0.5).
--
-- Part B: insert an active mark_position_resolved for CLM-APC-2000 (in_review).
--   Fourth reconciler run must NOT suppress CLM-APC-2000's queue row.
--   in_review positions always route regardless of any active resolution.
--   This is the core in_review invariant check for mark_position_resolved.
-- -------------------------------------------------------------------------

-- Part A: supersede the CLM-APC-1000 resolved marker.
update fte_review_resolutions
   set is_superseded = true
 where practice_id = '96000000-0000-4000-8000-0000000000fe'
   and action      = 'mark_position_resolved'
   and claim_id    = 'c1a90000-0000-4000-8000-000000001000'
   and is_superseded = false;

-- Part B: insert an active mark_position_resolved for CLM-APC-2000.
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000002000',
   'mark_position_resolved',
   'position',
   false,
   'SYNTHETIC TEST NOTE — CLM-APC-2000 reviewed 2026-06-25.  Fragmentation '
   'observations are known; claim is in_review pending retry.  This resolved '
   'marker must NOT suppress the CLM-APC-2000 queue row — in_review positions '
   'always route regardless of any resolution.');

do $$
declare
  v_result       jsonb;
  v_event_count  int;
  v_queue_count  int;
begin
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 14/14 — Four sub-checks in one combined check:
  --   (a) review_resolutions_applied = 1 (CLM-APC-1000 row superseded and
  --       excluded; CLM-APC-2000 row active and counted).
  --   (b) CLM-APC-1000 unbalanced_financial_position queue row RESTORED
  --       (supersession removes suppression).
  --   (c) CLM-APC-1000 short_pay_detected still present.
  --   (d) CLM-APC-2000 still queued despite active mark_position_resolved
  --       (in_review invariant: suppression applies only to 'unbalanced').

  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [14/14] expected review_resolutions_applied=1 (CLM-APC-2000 row active), got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');

  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and rq.reason      = 'unbalanced_financial_position'
     and c.claim_number = 'CLM-APC-1000';
  assert v_queue_count = 1,
    'FAIL [14/14] expected CLM-APC-1000 queue row RESTORED after supersession, got: '
    || v_queue_count::text;

  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.event_type  = 'short_pay_detected'
     and c.claim_number = 'CLM-APC-1000';
  assert v_event_count = 1,
    'FAIL [14/14] expected CLM-APC-1000 short_pay_detected still present after supersession, got: '
    || v_event_count::text;

  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_queue_count >= 1,
    'FAIL [14/14] in_review invariant violated: CLM-APC-2000 queue row suppressed despite '
    'active mark_position_resolved — in_review positions must always route; got count: '
    || v_queue_count::text;

  raise notice 'PASS [14/14] supersession + in_review invariant: CLM-APC-1000 queue restored; '
               'short_pay_detected preserved; CLM-APC-2000 still queued despite active '
               'mark_position_resolved (in_review invariant holds)';
end;
$$;


-- -------------------------------------------------------------------------
-- STEP 8: Rollback — nothing persists.
-- -------------------------------------------------------------------------
rollback;

\echo '=== validate_mark_position_resolved: 14/14 PASS ==='

-- =============================================================================
-- Financial Truth Engine — Validation Suite: validate_request_more_evidence
-- tests/validate_request_more_evidence.sql
--
-- Covers: Task 005D (request_more_evidence) — migration 007 constraints.
-- 12 numeric checks.  Fixture: synthetic_96c5c357_failure_modes.sql.
--
-- Primary vehicle:  CLM-APC-2000 (c1a90000-0000-4000-8000-000000002000)
--   All-SUSPECT observations, no events emitted, reconciler status = in_review.
--   An active evidence request leaves position status unchanged.
--
-- Isolation vehicle: CLM-APC-1000 (c1a90000-0000-4000-8000-000000001000)
--   Has payment_applied + short_pay_detected events, status = unbalanced.
--   Inserting a request_more_evidence for CLM-APC-2000 must not perturb CLM-APC-1000.
--
-- Shape violation inserts in checks 10 and 11 target CLM-APC-1000 rather than
-- CLM-APC-2000 so that only the CHECK constraint fires (no simultaneous
-- unique_violation from the active evidence request for CLM-APC-2000).
--
-- PSQL ONLY.  The \i metacommand at the top is not supported by the Supabase
-- SQL Editor.  Supabase users: paste and run the fixture body first, then
-- paste and run this file's body starting at the `begin;` line below.
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
-- Checks 1/12, 2/12, 3/12.
-- -------------------------------------------------------------------------
do $$
declare
  v_result      jsonb;
  v_status      text;
  v_queue_count int;
begin
  -- First reconciler run with zero resolutions.
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 1/12 — review_resolutions_applied is zero (no resolutions exist yet).
  assert (v_result->>'review_resolutions_applied')::int = 0,
    'FAIL [1/12] expected review_resolutions_applied=0, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [1/12] baseline: review_resolutions_applied = 0';

  -- 2/12 — CLM-APC-2000 position status is in_review (no events emitted for
  --          all-SUSPECT observations).
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_status = 'in_review',
    'FAIL [2/12] expected CLM-APC-2000 reconciliation_status=in_review, got: '
    || coalesce(v_status, 'NULL');
  raise notice 'PASS [2/12] baseline: CLM-APC-2000 reconciliation_status = in_review';

  -- 3/12 — CLM-APC-2000 is present in the review queue (uncertainty is
  --          explicit, not silent — Phase 7 routes in_review claims).
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_queue_count >= 1,
    'FAIL [3/12] expected CLM-APC-2000 queue entry count >= 1, got: '
    || v_queue_count::text;
  raise notice 'PASS [3/12] baseline: CLM-APC-2000 has >= 1 review queue entry';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 2: Insert a valid request_more_evidence for CLM-APC-2000.
--
-- Shape requirements (migration 007):
--   action = 'request_more_evidence'
--   target_type = 'position'
--   claim_id IS NOT NULL (stable anchor; source_position_id would go stale)
--   notes IS NOT NULL AND btrim(notes) <> '' (actionable explanation required)
--   is_superseded = false (active evidence request)
-- -------------------------------------------------------------------------
insert into fte_review_resolutions
  (practice_id, claim_id, action, target_type, is_superseded, notes)
values
  ('96000000-0000-4000-8000-0000000000fe',
   'c1a90000-0000-4000-8000-000000002000',
   'request_more_evidence',
   'position',
   false,
   'SYNTHETIC TEST NOTE — CLM-APC-2000 cannot be resolved without a clean 835 '
   'remittance: check #2-1835642 is fragmented across three spacing variants '
   '(2-1835642 / 2 - 1835642 / 2- 1835642) on pages 8 and 12.  '
   'Please obtain an authoritative 835 from Arizona Priority Care before '
   'this claim can be confirmed or dismissed.');

-- -------------------------------------------------------------------------
-- STEP 3: Second reconciler run — one resolution active.
-- Checks 4/12 through 8/12.
-- -------------------------------------------------------------------------
do $$
declare
  v_result      jsonb;
  v_status      text;
  v_queue_count int;
  v_event_count int;
  v_billed      numeric;
  v_paid        numeric;
  v_balance     numeric;
begin
  -- Second reconciler run with one active request_more_evidence resolution.
  select fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe')
    into v_result;

  -- 4/12 — review_resolutions_applied = 1 (the evidence request was loaded
  --          and counted by Phase 0.5, even though no phase acts on it).
  assert (v_result->>'review_resolutions_applied')::int = 1,
    'FAIL [4/12] expected review_resolutions_applied=1, got: '
    || coalesce(v_result->>'review_resolutions_applied', 'NULL');
  raise notice 'PASS [4/12] after resolution: review_resolutions_applied = 1';

  -- 5/12 — CLM-APC-2000 position status is STILL in_review after the evidence
  --          request.  request_more_evidence is a durable note only — it does
  --          not alter reconciler-derived status.
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_status = 'in_review',
    'FAIL [5/12] expected CLM-APC-2000 still in_review after evidence request, got: '
    || coalesce(v_status, 'NULL');
  raise notice 'PASS [5/12] after resolution: CLM-APC-2000 reconciliation_status unchanged (in_review)';

  -- 6/12 — CLM-APC-2000 is STILL in the review queue.  request_more_evidence
  --          does not suppress Phase 7 queue routing (unlike dismiss_short_pay /
  --          confirm_short_pay).
  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_queue_count >= 1,
    'FAIL [6/12] expected CLM-APC-2000 still queued after evidence request, got count: '
    || v_queue_count::text;
  raise notice 'PASS [6/12] after resolution: CLM-APC-2000 still has >= 1 review queue entry (routing not suppressed)';

  -- 7/12 — CLM-APC-2000 claim event count = 0.  All observations are SUSPECT;
  --          the evidence request does not create or modify any claim events.
  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_event_count = 0,
    'FAIL [7/12] expected CLM-APC-2000 claim event count = 0, got: '
    || v_event_count::text;
  raise notice 'PASS [7/12] after resolution: CLM-APC-2000 claim event count = 0 (no events emitted)';

  -- 8/12 — CLM-APC-2000 monetary fields are all NULL.  Phase 0 deletes the
  --          hand-authored position on each run; Phase 6 re-derives with no
  --          events → all monetary fields NULL.  The evidence request does not
  --          inject any financial math.
  select fp.billed_amount, fp.paid_amount, fp.open_balance_amount
    into v_billed, v_paid, v_balance
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-2000';
  assert v_billed is null and v_paid is null and v_balance is null,
    'FAIL [8/12] expected CLM-APC-2000 monetary fields all NULL, got: '
    || 'billed=' || coalesce(v_billed::text, 'NULL')
    || ' paid='  || coalesce(v_paid::text,   'NULL')
    || ' balance='|| coalesce(v_balance::text,'NULL');
  raise notice 'PASS [8/12] after resolution: CLM-APC-2000 monetary fields all NULL (no financial math from evidence request)';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 4: Uniqueness conflict — check 9/12.
--
-- Inserting a second active request_more_evidence for the same
-- (practice_id, claim_id) must raise unique_violation.
-- idx_fte_resolutions_single_active_evidence_request enforces this.
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
       'c1a90000-0000-4000-8000-000000002000',
       'request_more_evidence',
       'position',
       false,
       'SYNTHETIC TEST — second simultaneous active evidence request; must be rejected '
       'by idx_fte_resolutions_single_active_evidence_request.');
  exception
    when unique_violation then
      v_conflict_caught := true;
  end;
  assert v_conflict_caught,
    'FAIL [9/12] expected unique_violation when inserting second active '
    'request_more_evidence for same (practice_id, claim_id)';
  raise notice 'PASS [9/12] uniqueness: second active evidence request for CLM-APC-2000 raises unique_violation';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 5: Notes constraint — check 10/12.
--
-- Both NULL notes and blank-string notes must raise check_violation for
-- fte_review_resolutions_rme_needs_notes.
--
-- Shape violation inserts target CLM-APC-1000 (c1a90000-...-000000001000) to
-- avoid triggering the unique index for CLM-APC-2000 simultaneously —
-- only the CHECK constraint should fire for these tests.
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
       'c1a90000-0000-4000-8000-000000001000',
       'request_more_evidence',
       'position',
       false,
       null);
  exception
    when check_violation then
      v_null_caught := true;
  end;

  -- Blank (whitespace-only) notes must also be rejected.
  -- btrim('   ') = '' — the constraint checks btrim(notes) <> ''.
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'request_more_evidence',
       'position',
       false,
       '   ');
  exception
    when check_violation then
      v_blank_caught := true;
  end;

  assert v_null_caught and v_blank_caught,
    'FAIL [10/12] expected check_violation for both NULL notes and blank notes; '
    || 'null_caught=' || v_null_caught::text
    || ' blank_caught=' || v_blank_caught::text;
  raise notice 'PASS [10/12] notes constraint: NULL notes and blank notes both raise check_violation';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 6: claim_id and target_type constraints — check 11/12.
--
-- NULL claim_id must raise check_violation for
-- fte_review_resolutions_rme_needs_claim_id.
-- Wrong target_type (e.g. 'observation') must raise check_violation for
-- fte_review_resolutions_rme_needs_position_type.
-- -------------------------------------------------------------------------
do $$
declare
  v_null_claim_caught  boolean := false;
  v_wrong_type_caught  boolean := false;
begin
  -- NULL claim_id must be rejected.
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       null,
       'request_more_evidence',
       'position',
       false,
       'SYNTHETIC TEST — missing claim_id; must be rejected by rme_needs_claim_id.');
  exception
    when check_violation then
      v_null_claim_caught := true;
  end;

  -- target_type = 'observation' must be rejected (only 'position' is allowed).
  begin
    insert into fte_review_resolutions
      (practice_id, claim_id, action, target_type, is_superseded, notes)
    values
      ('96000000-0000-4000-8000-0000000000fe',
       'c1a90000-0000-4000-8000-000000001000',
       'request_more_evidence',
       'observation',
       false,
       'SYNTHETIC TEST — wrong target_type; must be rejected by rme_needs_position_type.');
  exception
    when check_violation then
      v_wrong_type_caught := true;
  end;

  assert v_null_claim_caught and v_wrong_type_caught,
    'FAIL [11/12] expected check_violation for both NULL claim_id and wrong target_type; '
    || 'null_claim_caught=' || v_null_claim_caught::text
    || ' wrong_type_caught=' || v_wrong_type_caught::text;
  raise notice 'PASS [11/12] shape constraints: NULL claim_id and target_type=observation both raise check_violation';
end;
$$;

-- -------------------------------------------------------------------------
-- STEP 7: CLM-APC-1000 isolation — check 12/12.
--
-- Inserting a request_more_evidence for CLM-APC-2000 and re-running the
-- reconciler must NOT change CLM-APC-1000's status, event, or queue state.
-- CLM-APC-1000 must still be unbalanced with one short_pay_detected event
-- and one unbalanced_financial_position queue entry.
-- -------------------------------------------------------------------------
do $$
declare
  v_status        text;
  v_event_count   int;
  v_queue_count   int;
begin
  select fp.reconciliation_status
    into v_status
    from fte_financial_positions fp
    join fte_claims c on c.id = fp.claim_id
   where fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and c.claim_number = 'CLM-APC-1000';

  select count(*)
    into v_event_count
    from fte_claim_events ce
    join fte_claims c on c.id = ce.claim_id
   where ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and ce.event_type = 'short_pay_detected'
     and c.claim_number = 'CLM-APC-1000';

  select count(*)
    into v_queue_count
    from fte_review_queue rq
    join fte_claims c on c.id = rq.claim_id
   where rq.practice_id = '96000000-0000-4000-8000-0000000000fe'
     and rq.reason = 'unbalanced_financial_position'
     and c.claim_number = 'CLM-APC-1000';

  assert v_status = 'unbalanced',
    'FAIL [12/12] isolation: expected CLM-APC-1000 still unbalanced, got: '
    || coalesce(v_status, 'NULL');
  assert v_event_count = 1,
    'FAIL [12/12] isolation: expected CLM-APC-1000 short_pay_detected event count = 1, got: '
    || v_event_count::text;
  assert v_queue_count = 1,
    'FAIL [12/12] isolation: expected CLM-APC-1000 unbalanced_financial_position queue count = 1, got: '
    || v_queue_count::text;

  raise notice 'PASS [12/12] isolation: CLM-APC-1000 unaffected (still unbalanced, 1 short_pay_detected event, 1 queue entry)';
end;
$$;

rollback;

\echo '=== validate_request_more_evidence: 12/12 PASS ==='

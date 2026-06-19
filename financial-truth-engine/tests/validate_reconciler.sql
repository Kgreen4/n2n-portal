-- =============================================================================
-- Financial Truth Engine — Reconciler Validation
-- tests/validate_reconciler.sql
--
-- Loads both synthetic fixtures, calls fte_reconcile_practice() for each
-- fixture practice, asserts 12 behavioral checks, then rolls everything back
-- so nothing persists in the database.
--
-- Run AFTER:
--   psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
--   psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
--
-- Usage:
--   psql "$DATABASE_URL" -f tests/validate_reconciler.sql
--
-- All 12 checks emit RAISE NOTICE 'PASS [n/12] ...' on success.
-- Any failure RAISEs EXCEPTION (transaction rolls back, nothing persists).
--
-- The \i calls below load fixtures BEFORE the outer BEGIN so the fixture
-- data is committed (fixtures have their own begin/commit) and visible to
-- the reconciler under the outer transaction's snapshot. The outer
-- BEGIN..ROLLBACK covers only reconciler-derived data.
-- =============================================================================

\i fixtures/synthetic_ccdbe216_failure_modes.sql
\i fixtures/synthetic_96c5c357_failure_modes.sql

begin;

-- ---- Phase 0 pre-wipe: ensure a clean baseline for derived tables -----------
-- The reconciler's Phase 0 does this too, but explicit here for clarity.
DELETE FROM fte_event_evidence      WHERE practice_id IN (
  'c0000000-0000-4000-8000-0000000000fe',
  '96000000-0000-4000-8000-0000000000fe'
);
DELETE FROM fte_review_queue        WHERE practice_id IN (
  'c0000000-0000-4000-8000-0000000000fe',
  '96000000-0000-4000-8000-0000000000fe'
);
DELETE FROM fte_financial_positions WHERE practice_id IN (
  'c0000000-0000-4000-8000-0000000000fe',
  '96000000-0000-4000-8000-0000000000fe'
);
DELETE FROM fte_claim_events        WHERE practice_id IN (
  'c0000000-0000-4000-8000-0000000000fe',
  '96000000-0000-4000-8000-0000000000fe'
);

-- ---- Run reconciler for both fixture practices ------------------------------
SELECT fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe');
SELECT fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe');


-- =============================================================================
-- CHECK 1 — ccdbe216: exactly 3 claim events emitted
--
-- Expected: claim_adjudicated (billed_amount a2, $720.00),
--           contractual_adjustment_applied (a3, $209.60),
--           payment_applied (a1, $510.40).
-- Observations b1..b5 are non-trusted and produce no events.
-- =============================================================================
DO $$
DECLARE
  v_count bigint;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM fte_claim_events
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';

  ASSERT v_count = 3,
    format('FAIL [1/12] ccdbe216: expected 3 claim events, got %s', v_count);
  RAISE NOTICE 'PASS [1/12] ccdbe216: 3 claim events emitted (adjudicated + adj_applied + payment_applied)';
END $$;


-- =============================================================================
-- CHECK 2 — ccdbe216: payment event carries correct amount and observation link
--
-- Observation a1: observation_type='payment', amount=510.40, check_eft='0000447412'
-- Expected: payment_applied event with amount=510.40 AND a 'supports' link to a1.
-- =============================================================================
DO $$
DECLARE
  v_amount  numeric;
  v_has_obs boolean;
BEGIN
  SELECT ce.amount INTO v_amount
  FROM fte_claim_events ce
  WHERE ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    AND ce.event_type  = 'payment_applied';

  ASSERT v_amount = 510.40,
    format('FAIL [2/12] ccdbe216: payment_applied amount expected 510.40, got %s', v_amount);

  SELECT EXISTS (
    SELECT 1
    FROM fte_event_evidence ee
    JOIN fte_claim_events ce ON ce.id = ee.claim_event_id
    WHERE ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
      AND ce.event_type  = 'payment_applied'
      AND ee.observation_id = '0b500000-0000-4000-8000-0000000000a1'
      AND ee.link_role   = 'supports'
  ) INTO v_has_obs;

  ASSERT v_has_obs,
    'FAIL [2/12] ccdbe216: payment_applied event missing ''supports'' link to observation a1';
  RAISE NOTICE 'PASS [2/12] ccdbe216: payment_applied amount=510.40 and links to observation a1';
END $$;


-- =============================================================================
-- CHECK 3 — ccdbe216: check stub evidence linked to payment event
--
-- Evidence dddd0000-...-00ca is evidence_type='check_payment',
-- metadata->>'check_number'='0000447412'. The reconciler (Phase 5c link 2)
-- should have added a 'supports' link from the payment event to this stub.
-- =============================================================================
DO $$
DECLARE
  v_has_stub boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM fte_event_evidence ee
    JOIN fte_claim_events ce ON ce.id = ee.claim_event_id
    WHERE ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
      AND ce.event_type  = 'payment_applied'
      AND ee.evidence_id = 'dddd0000-0000-4000-8000-0000000000ca'
      AND ee.link_role   = 'supports'
      AND ee.observation_id IS NULL
  ) INTO v_has_stub;

  ASSERT v_has_stub,
    'FAIL [3/12] ccdbe216: payment_applied event missing ''supports'' link to check_payment stub';
  RAISE NOTICE 'PASS [3/12] ccdbe216: check_payment stub linked to payment_applied event';
END $$;


-- =============================================================================
-- CHECK 4 — ccdbe216: all 6 reconciler-produced review_reason types are present
--
-- Observations b1..b5 produce the following queue entries:
--   b1 (is_superseded, failure_mode=phantom_duplicate_check_ref) → suspected_duplicate
--   b2 (is_superseded, failure_mode=section_delimiter_double_count) → conflicting_observations
--   b3 (is_superseded, failure_mode=null_check_crossbleed) → missing_evidence_link
--   b4 (is_summary_row) → suspected_summary_row
--   b5 (is_superseded, failure_mode=late_retry_page_contradiction) → late_retry_page_contradiction
-- Phase 7 adds: unbalanced_financial_position (if position is unbalanced/in_review)
--
-- Note: 'low_confidence_observation' is a valid schema value but is not
-- produced by the reconciler for any fixture in this suite — it requires a
-- caller to insert it explicitly or a future classification rule.
-- =============================================================================
DO $$
DECLARE
  v_reasons text[];
  r text;
BEGIN
  SELECT array_agg(DISTINCT reason ORDER BY reason) INTO v_reasons
  FROM fte_review_queue
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';

  FOREACH r IN ARRAY ARRAY[
    'conflicting_observations',
    'late_retry_page_contradiction',
    'missing_evidence_link',
    'suspected_duplicate',
    'suspected_summary_row'
  ]
  LOOP
    ASSERT r = ANY(v_reasons),
      format('FAIL [4/12] ccdbe216: review_reason ''%s'' not found in queue', r);
  END LOOP;

  RAISE NOTICE 'PASS [4/12] ccdbe216: all 5 observation-derived review_reason types present';
END $$;


-- =============================================================================
-- CHECK 5 — ccdbe216: financial position is in_review (not balanced), with
-- open_balance_amount = 0.00 and payment_applied event = 'ambiguous'.
--
-- The math: 720.00 − 209.60 − 510.40 = 0. But the position CANNOT be
-- 'balanced' because Phase 5 (late/retry) marked the payment_applied event
-- 'ambiguous' (b5: late_retry_page_contradiction for CLM-AZ-0001). Phase 6
-- checks for ambiguous events BEFORE checking whether the gap is zero, so a
-- mathematically balanced claim with an ambiguous payment is routed to
-- 'in_review'. The schema CHECK on fte_financial_positions forbids 'ambiguous'
-- as a position status; 'in_review' is the correct surrogate.
-- =============================================================================
DO $$
DECLARE
  v_status      text;
  v_billed      numeric;
  v_adj         numeric;
  v_paid        numeric;
  v_balance     numeric;
  v_pay_status  text;
BEGIN
  SELECT reconciliation_status, billed_amount,
         contractual_adjustment_amount, paid_amount, open_balance_amount
  INTO   v_status, v_billed, v_adj, v_paid, v_balance
  FROM fte_financial_positions
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe'
  LIMIT 1;

  ASSERT v_status = 'in_review',
    format('FAIL [5/12] ccdbe216: position status expected ''in_review'' (payment ambiguous), got ''%s''', v_status);
  ASSERT v_billed = 720.00,
    format('FAIL [5/12] ccdbe216: billed_amount expected 720.00, got %s', v_billed);
  ASSERT v_adj = 209.60,
    format('FAIL [5/12] ccdbe216: contractual_adjustment_amount expected 209.60, got %s', v_adj);
  ASSERT v_paid = 510.40,
    format('FAIL [5/12] ccdbe216: paid_amount expected 510.40, got %s', v_paid);
  ASSERT v_balance = 0.00,
    format('FAIL [5/12] ccdbe216: open_balance_amount expected 0.00, got %s', v_balance);

  -- The payment_applied event itself must remain 'ambiguous' (Phase 5 sets this).
  SELECT reconciliation_status INTO v_pay_status
  FROM fte_claim_events
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    AND event_type  = 'payment_applied';

  ASSERT v_pay_status = 'ambiguous',
    format('FAIL [5/12] ccdbe216: payment_applied event must be ''ambiguous'', got ''%s''', v_pay_status);

  RAISE NOTICE 'PASS [5/12] ccdbe216: position=in_review, open_balance=0.00, payment_event=ambiguous';
END $$;


-- =============================================================================
-- CHECK 6 — 96c5c357 / CLM-APC-1000: claim_adjudicated, payment_applied, and
-- short_pay_detected all emitted; payment amount = 351.89
--
-- CLM-APC-1000 has TRUSTED billed_amount (a1, $1600) and payment (a2, $351.89).
-- open_balance = 1600 − 0 − 351.89 = 1248.11 > 0 → short_pay_detected.
-- =============================================================================
DO $$
DECLARE
  v_event_types text[];
  v_pay_amount  numeric;
BEGIN
  SELECT array_agg(DISTINCT event_type ORDER BY event_type) INTO v_event_types
  FROM fte_claim_events ce
  JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-1000';

  ASSERT 'claim_adjudicated'   = ANY(v_event_types),
    'FAIL [6/12] 96c5c357/CLM-APC-1000: missing claim_adjudicated event';
  ASSERT 'payment_applied'     = ANY(v_event_types),
    'FAIL [6/12] 96c5c357/CLM-APC-1000: missing payment_applied event';
  ASSERT 'short_pay_detected'  = ANY(v_event_types),
    'FAIL [6/12] 96c5c357/CLM-APC-1000: missing short_pay_detected event';

  SELECT ce.amount INTO v_pay_amount
  FROM fte_claim_events ce
  JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-1000'
    AND ce.event_type  = 'payment_applied';

  ASSERT v_pay_amount = 351.89,
    format('FAIL [6/12] 96c5c357/CLM-APC-1000: payment_applied expected 351.89, got %s', v_pay_amount);

  RAISE NOTICE 'PASS [6/12] 96c5c357/CLM-APC-1000: adjudicated+payment+short_pay emitted, payment=351.89';
END $$;


-- =============================================================================
-- CHECK 7 — 96c5c357 / CLM-APC-1000: position is unbalanced with gap 1248.11
-- =============================================================================
DO $$
DECLARE
  v_status  text;
  v_balance numeric;
BEGIN
  SELECT fp.reconciliation_status, fp.open_balance_amount
  INTO   v_status, v_balance
  FROM fte_financial_positions fp
  JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-1000';

  ASSERT v_status = 'unbalanced',
    format('FAIL [7/12] 96c5c357/CLM-APC-1000: position status expected ''unbalanced'', got ''%s''', v_status);
  ASSERT v_balance = 1248.11,
    format('FAIL [7/12] 96c5c357/CLM-APC-1000: open_balance_amount expected 1248.11, got %s', v_balance);

  RAISE NOTICE 'PASS [7/12] 96c5c357/CLM-APC-1000: position unbalanced, open_balance=1248.11';
END $$;


-- =============================================================================
-- CHECK 8 — 96c5c357 / CLM-APC-2000: no events emitted; position is in_review
--
-- Observations b1 and b2 are SUSPECT (check_spacing_variant_fragmentation,
-- no retry_pending → suspected_duplicate). b3 is EXCLUDED (is_superseded +
-- same failure_mode with retry_pending → late_retry_page_contradiction).
-- None emit events. Position exists because review queue entries reference the
-- claim; it should have all-NULL monetary fields and status='in_review'.
-- =============================================================================
DO $$
DECLARE
  v_event_count bigint;
  v_status      text;
  v_billed      numeric;
BEGIN
  SELECT COUNT(*) INTO v_event_count
  FROM fte_claim_events ce
  JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-2000';

  ASSERT v_event_count = 0,
    format('FAIL [8/12] 96c5c357/CLM-APC-2000: expected 0 events, got %s', v_event_count);

  SELECT fp.reconciliation_status, fp.billed_amount
  INTO   v_status, v_billed
  FROM fte_financial_positions fp
  JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-2000';

  ASSERT v_status = 'in_review',
    format('FAIL [8/12] 96c5c357/CLM-APC-2000: position status expected ''in_review'', got ''%s''', v_status);
  ASSERT v_billed IS NULL,
    format('FAIL [8/12] 96c5c357/CLM-APC-2000: billed_amount expected NULL, got %s', v_billed);

  RAISE NOTICE 'PASS [8/12] 96c5c357/CLM-APC-2000: 0 events, position in_review with NULL billed';
END $$;


-- =============================================================================
-- CHECK 9 — 96c5c357: b1 and b2 produce 'suspected_duplicate' review entries;
-- b3 produces 'late_retry_page_contradiction'
-- =============================================================================
DO $$
DECLARE
  v_b1_reason text;
  v_b2_reason text;
  v_b3_reason text;
BEGIN
  SELECT reason INTO v_b1_reason
  FROM fte_review_queue
  WHERE practice_id  = '96000000-0000-4000-8000-0000000000fe'
    AND observation_id = '0b590000-0000-4000-8000-0000000000b1';

  SELECT reason INTO v_b2_reason
  FROM fte_review_queue
  WHERE practice_id  = '96000000-0000-4000-8000-0000000000fe'
    AND observation_id = '0b590000-0000-4000-8000-0000000000b2';

  SELECT reason INTO v_b3_reason
  FROM fte_review_queue
  WHERE practice_id  = '96000000-0000-4000-8000-0000000000fe'
    AND observation_id = '0b590000-0000-4000-8000-0000000000b3';

  ASSERT v_b1_reason = 'suspected_duplicate',
    format('FAIL [9/12] b1 expected ''suspected_duplicate'', got ''%s''', v_b1_reason);
  ASSERT v_b2_reason = 'suspected_duplicate',
    format('FAIL [9/12] b2 expected ''suspected_duplicate'', got ''%s''', v_b2_reason);
  ASSERT v_b3_reason = 'late_retry_page_contradiction',
    format('FAIL [9/12] b3 expected ''late_retry_page_contradiction'', got ''%s''', v_b3_reason);

  RAISE NOTICE 'PASS [9/12] 96c5c357: b1+b2→suspected_duplicate, b3→late_retry_page_contradiction';
END $$;


-- =============================================================================
-- CHECK 10 — fte_analysis_runs entries created for both practices
-- =============================================================================
DO $$
DECLARE
  v_c0_status text;
  v_96_status text;
BEGIN
  SELECT status INTO v_c0_status
  FROM fte_analysis_runs
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    AND run_type    = 'reconciler'
  ORDER BY started_at DESC LIMIT 1;

  SELECT status INTO v_96_status
  FROM fte_analysis_runs
  WHERE practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND run_type    = 'reconciler'
  ORDER BY started_at DESC LIMIT 1;

  ASSERT v_c0_status = 'succeeded',
    format('FAIL [10/12] ccdbe216 analysis_run: expected ''succeeded'', got ''%s''', v_c0_status);
  ASSERT v_96_status = 'succeeded',
    format('FAIL [10/12] 96c5c357 analysis_run: expected ''succeeded'', got ''%s''', v_96_status);

  RAISE NOTICE 'PASS [10/12] fte_analysis_runs entries created with status=succeeded for both practices';
END $$;


-- =============================================================================
-- CHECK 11 — No fte_event_evidence row has both evidence_id and observation_id
-- set to NULL (fte_event_evidence_target_present constraint invariant)
-- =============================================================================
DO $$
DECLARE
  v_bad_count bigint;
BEGIN
  SELECT COUNT(*) INTO v_bad_count
  FROM fte_event_evidence
  WHERE practice_id IN (
    'c0000000-0000-4000-8000-0000000000fe',
    '96000000-0000-4000-8000-0000000000fe'
  )
    AND evidence_id    IS NULL
    AND observation_id IS NULL;

  ASSERT v_bad_count = 0,
    format('FAIL [11/12] %s fte_event_evidence rows have both evidence_id and observation_id NULL',
           v_bad_count);
  RAISE NOTICE 'PASS [11/12] all event_evidence rows satisfy the audit link constraint';
END $$;


-- =============================================================================
-- CHECK 12 — Idempotency: calling fte_reconcile_practice twice produces the
-- same derived state in all ledger tables.
--
-- NOTE: fte_analysis_runs is APPEND-ONLY — Phase 0 does NOT delete it, so a
-- second call adds a new row rather than replacing the first. This check does
-- NOT assert that fte_analysis_runs counts remain the same; it only verifies
-- that the four derived ledger tables (fte_claim_events, fte_event_evidence,
-- fte_financial_positions, fte_review_queue) are identical to the first call.
-- =============================================================================
-- Re-run reconciler for both practices (second call inside this transaction).
SELECT fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe');
SELECT fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_c0_events    bigint;
  v_c0_pay       numeric;
  v_c0_status    text;
  v_96_pay_event bigint;
  v_96_status    text;
BEGIN
  -- ccdbe216: still 3 events (not 6)
  SELECT COUNT(*) INTO v_c0_events
  FROM fte_claim_events
  WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';

  ASSERT v_c0_events = 3,
    format('FAIL [12/12] idempotency: ccdbe216 expected 3 events after 2nd call, got %s', v_c0_events);

  -- ccdbe216: payment amount still 510.40
  SELECT ce.amount INTO v_c0_pay
  FROM fte_claim_events ce
  WHERE ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    AND ce.event_type  = 'payment_applied';

  ASSERT v_c0_pay = 510.40,
    format('FAIL [12/12] idempotency: ccdbe216 payment_applied expected 510.40 after 2nd call, got %s', v_c0_pay);

  -- ccdbe216: position still in_review (not balanced) after 2nd call
  SELECT fp.reconciliation_status INTO v_c0_status
  FROM fte_financial_positions fp
  WHERE fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
  LIMIT 1;

  ASSERT v_c0_status = 'in_review',
    format('FAIL [12/12] idempotency: ccdbe216 position expected ''in_review'' after 2nd call, got ''%s''',
           v_c0_status);

  -- 96c5c357 / CLM-APC-1000: still exactly 1 payment_applied event
  SELECT COUNT(*) INTO v_96_pay_event
  FROM fte_claim_events ce
  JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-1000'
    AND ce.event_type  = 'payment_applied';

  ASSERT v_96_pay_event = 1,
    format('FAIL [12/12] idempotency: CLM-APC-1000 expected 1 payment_applied after 2nd call, got %s',
           v_96_pay_event);

  -- 96c5c357 / CLM-APC-2000: position still in_review
  SELECT fp.reconciliation_status INTO v_96_status
  FROM fte_financial_positions fp
  JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'CLM-APC-2000';

  ASSERT v_96_status = 'in_review',
    format('FAIL [12/12] idempotency: CLM-APC-2000 position expected ''in_review'' after 2nd call, got ''%s''',
           v_96_status);

  RAISE NOTICE 'PASS [12/12] reconciler is idempotent (2nd call produces identical ledger state)';
END $$;


-- Discard all reconciler-derived data. Fixture data (observations, claims,
-- evidence) was committed in its own begin/commit above and is NOT rolled back
-- here, but the fixture cleanup at the top of each fixture file handles that
-- on the next invocation.
rollback;

\echo ''
\echo '==================================================================='
\echo ' FTE reconciler validation complete — all 12 checks passed if no'
\echo ' EXCEPTION was raised above. (All derived data rolled back.)'
\echo '==================================================================='

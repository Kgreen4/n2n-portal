-- =============================================================================
-- Financial Truth Engine — Reconciler Denial Accounting Validation
-- tests/validate_reconciler_denial.sql
--
-- Covers Task 014B (core denied_amount accounting):
--   * Phase 5e emits one denial_posted per amount-bearing canonical denial
--     observation (multiple denials aggregate; carc/rarc propagated; 1 evidence
--     link each). No-amount CARC/RARC-only denial signals derive NO amount.
--   * Over-denial (denied > billed - contractual - paid - patient_responsibility)
--     routes the claim to in_review via a NULL-amount ambiguous marker.
--   * Phase 6 adds denied_amount and subtracts denied from open_balance; denial
--     is explained accounting (not itself a short pay).
--   * Residual unexplained balance after denial still yields short_pay.
--   * E1 (incomplete) and E2 (balanced with contractual + patient responsibility)
--     behavior preserved. Idempotent on rerun.
--
-- Self-contained: creates its own synthetic fixture INSIDE the transaction and
-- ROLLs BACK at the end. All identifiers and amounts are synthetic. No PHI, no
-- production data, no real claim/check numbers. (CO-45 / CO-97 / N130 are
-- synthetic placeholders standing in for CARC/RARC codes.)
--
-- Run AFTER migrations 001..011 and reconciler/fte_reconcile.sql.
-- Emits RAISE NOTICE 'PASS [n/14] ...'; any failure RAISEs EXCEPTION and rolls back.
-- =============================================================================

begin;

INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e3000000-0000-4000-8000-0000000000fe', 'Denial Accounting Test Practice', 'SYN-DEN-PRACTICE');

INSERT INTO fte_evidence
  (id, practice_id, evidence_type, fixture_id, source_uri, page_number, metadata)
VALUES
  ('e3e00000-0000-4000-8000-00000000000a',
   'e3000000-0000-4000-8000-0000000000fe',
   'ocr_text', 'SYN_DEN_PAGE_TEXT',
   'private://fte/de-identified/SYN_DEN/page_001', 1, '{}'::jsonb);

INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e3c00000-0000-4000-8000-00000000000a','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-FULL','SYN-DEN-FULL','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-00000000000b','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-MULTI','SYN-DEN-MULTI','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-00000000000c','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-PART','SYN-DEN-PART','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-00000000000d','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-PR','SYN-DEN-PR','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-00000000000e','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-NOBILL','SYN-DEN-NOBILL','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-00000000000f','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-CARCNOAMT','SYN-DEN-CARCNOAMT','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-000000000010','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-CARCPROP','SYN-DEN-CARCPROP','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-000000000011','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-OVER','SYN-DEN-OVER','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-000000000012','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-E1','SYN-DEN-E1','Synthetic Payer','open'),
  ('e3c00000-0000-4000-8000-000000000013','e3000000-0000-4000-8000-0000000000fe','SYN-DEN-E2','SYN-DEN-E2','Synthetic Payer','open');

-- Column-explicit INSERT ... SELECT: invariant columns (practice_id,
-- evidence_id, payer_name, confidence_score, page_number, flags, metadata) are
-- set ONCE as constants; per-row tuples carry only the varying fields. This
-- removes practice_id/evidence_id from the wide per-row VALUES, preventing the
-- positional-drift typo class.
INSERT INTO fte_observations
  (practice_id, evidence_id, payer_name, confidence_score, page_number,
   is_summary_row, is_superseded, metadata,
   id, observation_type, amount, amount_type, claim_identifier,
   check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value)
SELECT
  'e3000000-0000-4000-8000-0000000000fe'::uuid,
  'e3e00000-0000-4000-8000-00000000000a'::uuid,
  'Synthetic Payer', 0.9000, 1,
  false, false, '{}'::jsonb,
  v.id, v.observation_type, v.amount, v.amount_type, v.claim_identifier,
  v.check_eft_identifier, v.carc_code, v.rarc_code, v.raw_value, v.normalized_value
FROM (VALUES
  -- id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value
  -- FULLDEN: billed 100, denial 100 (no allowed/paid/pr) -> balanced, open 0.
  ('e3b00000-0000-4000-8000-000000000001'::uuid, 'billed_amount'::text, 100.00::numeric, 'billed'::text, 'SYN-DEN-FULL'::text, NULL::text, NULL::text, NULL::text, '100.00'::text, '100.00'::text),
  ('e3b00000-0000-4000-8000-000000000002', 'denial', 100.00, 'denied', 'SYN-DEN-FULL', NULL, NULL, NULL, '100.00', '100.00'),
  -- MULTIDEN: billed 100, denials 60 + 40 -> aggregate 100 -> balanced.
  ('e3b00000-0000-4000-8000-000000000003', 'billed_amount', 100.00, 'billed', 'SYN-DEN-MULTI', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-000000000004', 'denial', 60.00, 'denied', 'SYN-DEN-MULTI', NULL, NULL, NULL, '60.00', '60.00'),
  ('e3b00000-0000-4000-8000-000000000005', 'denial', 40.00, 'denied', 'SYN-DEN-MULTI', NULL, NULL, NULL, '40.00', '40.00'),
  -- PARTDEN: billed 100, denial 30, paid 20 -> open 50 unbalanced + short_pay.
  ('e3b00000-0000-4000-8000-000000000006', 'billed_amount', 100.00, 'billed', 'SYN-DEN-PART', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-000000000007', 'denial', 30.00, 'denied', 'SYN-DEN-PART', NULL, NULL, NULL, '30.00', '30.00'),
  ('e3b00000-0000-4000-8000-000000000008', 'payment', 20.00, 'paid', 'SYN-DEN-PART', 'SYN-DEN-CHK-PART', NULL, NULL, '20.00', '20.00'),
  -- DENPR: billed 100, denial 40, patient_resp 60 -> balanced, no false short_pay.
  ('e3b00000-0000-4000-8000-000000000009', 'billed_amount', 100.00, 'billed', 'SYN-DEN-PR', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-00000000000a', 'denial', 40.00, 'denied', 'SYN-DEN-PR', NULL, NULL, NULL, '40.00', '40.00'),
  ('e3b00000-0000-4000-8000-00000000000b', 'patient_responsibility', 60.00, 'patient_responsibility', 'SYN-DEN-PR', NULL, NULL, NULL, '60.00', '60.00'),
  -- DENNOBILL: denial 50 only, no billed -> incomplete (E1).
  ('e3b00000-0000-4000-8000-00000000000c', 'denial', 50.00, 'denied', 'SYN-DEN-NOBILL', NULL, NULL, NULL, '50.00', '50.00'),
  -- CARCNOAMT: billed 100 + CARC-only denial (NULL amount) -> no denied amount derived.
  ('e3b00000-0000-4000-8000-00000000000d', 'billed_amount', 100.00, 'billed', 'SYN-DEN-CARCNOAMT', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-00000000000e', 'denial', NULL, 'denied', 'SYN-DEN-CARCNOAMT', NULL, 'CO-97', NULL, 'CO-97', NULL),
  -- CARCPROP: billed 100, denial 100 with carc/rarc -> propagated onto event.
  ('e3b00000-0000-4000-8000-00000000000f', 'billed_amount', 100.00, 'billed', 'SYN-DEN-CARCPROP', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-000000000010', 'denial', 100.00, 'denied', 'SYN-DEN-CARCPROP', NULL, 'CO-45', 'N130', '100.00', '100.00'),
  -- OVERDEN: billed 100, denial 150 -> over-denial -> in_review.
  ('e3b00000-0000-4000-8000-000000000011', 'billed_amount', 100.00, 'billed', 'SYN-DEN-OVER', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-000000000012', 'denial', 150.00, 'denied', 'SYN-DEN-OVER', NULL, NULL, NULL, '150.00', '150.00'),
  -- E1INC: payment only (trusted via check), no billed -> incomplete.
  ('e3b00000-0000-4000-8000-000000000013', 'payment', 25.00, 'paid', 'SYN-DEN-E1', 'SYN-DEN-CHK-E1', NULL, NULL, '25.00', '25.00'),
  -- E2BAL: billed 100, allowed 80, paid 20, patient_resp 60 (no denial) -> balanced.
  ('e3b00000-0000-4000-8000-000000000014', 'billed_amount', 100.00, 'billed', 'SYN-DEN-E2', NULL, NULL, NULL, '100.00', '100.00'),
  ('e3b00000-0000-4000-8000-000000000015', 'allowed_amount', 80.00, 'allowed', 'SYN-DEN-E2', NULL, NULL, NULL, '80.00', '80.00'),
  ('e3b00000-0000-4000-8000-000000000016', 'payment', 20.00, 'paid', 'SYN-DEN-E2', 'SYN-DEN-CHK-E2', NULL, NULL, '20.00', '20.00'),
  ('e3b00000-0000-4000-8000-000000000017', 'patient_responsibility', 60.00, 'patient_responsibility', 'SYN-DEN-E2', NULL, NULL, NULL, '60.00', '60.00')
) AS v(id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value);

SELECT fte_reconcile_practice('e3000000-0000-4000-8000-0000000000fe');


-- CHECK 1 — FULLDEN: full denial balances the claim.
DO $$
DECLARE v_status text; v_ob numeric; v_den numeric; v_dp bigint; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount, denied_amount
    INTO v_status, v_ob, v_den
  FROM fte_financial_positions WHERE claim_id = 'e3c00000-0000-4000-8000-00000000000a';
  SELECT COUNT(*) INTO v_dp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000a' AND event_type='denial_posted';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000a' AND event_type='short_pay_detected';
  ASSERT v_status='balanced', format('CHECK 1 FAIL: FULLDEN status expected balanced, got %L', v_status);
  ASSERT v_ob=0, format('CHECK 1 FAIL: FULLDEN open_balance expected 0, got %s', v_ob);
  ASSERT v_den=100.00, format('CHECK 1 FAIL: FULLDEN denied_amount expected 100.00, got %s', v_den);
  ASSERT v_dp=1, format('CHECK 1 FAIL: FULLDEN denial_posted expected 1, got %s', v_dp);
  ASSERT v_sp=0, format('CHECK 1 FAIL: FULLDEN short_pay expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [1/14] full denial balances the claim (denied_amount tracked, no short_pay)';
END $$;

-- CHECK 2 — MULTIDEN: multiple denials aggregate; not ambiguous.
DO $$
DECLARE v_dp bigint; v_den numeric; v_status text;
BEGIN
  SELECT COUNT(*) INTO v_dp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000b' AND event_type='denial_posted';
  SELECT denied_amount, reconciliation_status INTO v_den, v_status FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000b';
  ASSERT v_dp=2, format('CHECK 2 FAIL: MULTIDEN denial_posted expected 2, got %s', v_dp);
  ASSERT v_den=100.00, format('CHECK 2 FAIL: MULTIDEN denied_amount expected 100.00, got %s', v_den);
  ASSERT v_status='balanced', format('CHECK 2 FAIL: MULTIDEN status expected balanced (not ambiguous), got %L', v_status);
  RAISE NOTICE 'PASS [2/14] multiple denials aggregate into denied_amount; not ambiguous';
END $$;

-- CHECK 3 — PARTDEN: partial denial leaves unexplained residual -> short_pay.
DO $$
DECLARE v_status text; v_ob numeric; v_den numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount, denied_amount INTO v_status, v_ob, v_den
  FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000c';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000c' AND event_type='short_pay_detected';
  ASSERT v_status='unbalanced', format('CHECK 3 FAIL: PARTDEN status expected unbalanced, got %L', v_status);
  ASSERT v_ob=50.00, format('CHECK 3 FAIL: PARTDEN open_balance expected 50.00, got %s', v_ob);
  ASSERT v_den=30.00, format('CHECK 3 FAIL: PARTDEN denied_amount expected 30.00, got %s', v_den);
  ASSERT v_sp=1, format('CHECK 3 FAIL: PARTDEN short_pay expected 1, got %s', v_sp);
  RAISE NOTICE 'PASS [3/14] partial denial: residual unexplained balance still yields short_pay';
END $$;

-- CHECK 4 — DENPR: denial + patient responsibility close to zero; no false short_pay.
DO $$
DECLARE v_status text; v_den numeric; v_pr numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, denied_amount, patient_responsibility_amount INTO v_status, v_den, v_pr
  FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000d';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000d' AND event_type='short_pay_detected';
  ASSERT v_status='balanced', format('CHECK 4 FAIL: DENPR status expected balanced, got %L', v_status);
  ASSERT v_den=40.00, format('CHECK 4 FAIL: DENPR denied_amount expected 40.00, got %s', v_den);
  ASSERT v_pr=60.00, format('CHECK 4 FAIL: DENPR patient_responsibility_amount expected 60.00, got %s', v_pr);
  ASSERT v_sp=0, format('CHECK 4 FAIL: DENPR short_pay expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [4/14] denial + patient responsibility both explained; no false short_pay';
END $$;

-- CHECK 5 — DENNOBILL: denial without billed -> incomplete (E1 preserved).
DO $$
DECLARE v_status text; v_ob numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount INTO v_status, v_ob
  FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000e';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000e' AND event_type='short_pay_detected';
  ASSERT v_status='incomplete', format('CHECK 5 FAIL: DENNOBILL status expected incomplete, got %L', v_status);
  ASSERT v_ob IS NULL, format('CHECK 5 FAIL: DENNOBILL open_balance expected NULL, got %L', v_ob);
  ASSERT v_sp=0, format('CHECK 5 FAIL: DENNOBILL short_pay expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [5/14] denial without billed stays incomplete (E1); no short_pay';
END $$;

-- CHECK 6 — FULLDEN: denial without allowed still reconciles (allowed_amount NULL).
DO $$
DECLARE v_allowed numeric; v_status text;
BEGIN
  SELECT allowed_amount, reconciliation_status INTO v_allowed, v_status
  FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000a';
  ASSERT v_allowed IS NULL, format('CHECK 6 FAIL: FULLDEN allowed_amount expected NULL, got %s', v_allowed);
  ASSERT v_status='balanced', format('CHECK 6 FAIL: FULLDEN status expected balanced, got %L', v_status);
  RAISE NOTICE 'PASS [6/14] denial without allowed reconciles (allowed not required)';
END $$;

-- CHECK 7 — CARCPROP: carc/rarc propagated onto the denial_posted event.
DO $$
DECLARE v_carc text; v_links bigint;
BEGIN
  SELECT carc_code INTO v_carc FROM fte_claim_events
  WHERE claim_id='e3c00000-0000-4000-8000-000000000010' AND event_type='denial_posted';
  SELECT COUNT(*) INTO v_links FROM fte_event_evidence ee JOIN fte_claim_events ce ON ce.id=ee.claim_event_id
  WHERE ce.claim_id='e3c00000-0000-4000-8000-000000000010' AND ce.event_type='denial_posted';
  ASSERT v_carc='CO-45', format('CHECK 7 FAIL: CARCPROP carc_code expected CO-45, got %L', v_carc);
  ASSERT v_links>=1, format('CHECK 7 FAIL: CARCPROP denial evidence links expected >=1, got %s', v_links);
  RAISE NOTICE 'PASS [7/14] CARC/RARC propagated onto denial_posted; evidence linked';
END $$;

-- CHECK 8 — CARCNOAMT: CARC-only (no amount) denial derives no amount, no silent balance.
DO $$
DECLARE v_dp bigint; v_den numeric; v_status text;
BEGIN
  SELECT COUNT(*) INTO v_dp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-00000000000f' AND event_type='denial_posted';
  SELECT denied_amount, reconciliation_status INTO v_den, v_status FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000f';
  ASSERT v_dp=0, format('CHECK 8 FAIL: CARCNOAMT denial_posted expected 0, got %s', v_dp);
  ASSERT v_den IS NULL, format('CHECK 8 FAIL: CARCNOAMT denied_amount expected NULL, got %s', v_den);
  ASSERT v_status='unbalanced', format('CHECK 8 FAIL: CARCNOAMT status expected unbalanced (not silently balanced), got %L', v_status);
  RAISE NOTICE 'PASS [8/14] no-amount CARC-only denial derives no amount and does not silently balance';
END $$;

-- CHECK 9 — OVERDEN: over-denial routes to in_review via ambiguous marker.
DO $$
DECLARE v_status text; v_amb bigint;
BEGIN
  SELECT reconciliation_status INTO v_status FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-000000000011';
  SELECT COUNT(*) INTO v_amb FROM fte_claim_events
  WHERE claim_id='e3c00000-0000-4000-8000-000000000011' AND event_type='denial_posted' AND reconciliation_status='ambiguous';
  ASSERT v_status='in_review', format('CHECK 9 FAIL: OVERDEN status expected in_review, got %L', v_status);
  ASSERT v_amb=1, format('CHECK 9 FAIL: OVERDEN ambiguous denial marker expected 1, got %s', v_amb);
  RAISE NOTICE 'PASS [9/14] over-denial routes to in_review via ambiguous marker';
END $$;

-- CHECK 10 — Idempotency: rerun stable event count; FULLDEN still balanced.
DO $$
DECLARE v_before bigint; v_after bigint; v_status text;
BEGIN
  SELECT COUNT(*) INTO v_before FROM fte_claim_events WHERE practice_id='e3000000-0000-4000-8000-0000000000fe';
  PERFORM fte_reconcile_practice('e3000000-0000-4000-8000-0000000000fe');
  SELECT COUNT(*) INTO v_after FROM fte_claim_events WHERE practice_id='e3000000-0000-4000-8000-0000000000fe';
  ASSERT v_after=v_before, format('CHECK 10 FAIL: event count changed on rerun (before %s, after %s)', v_before, v_after);
  SELECT reconciliation_status INTO v_status FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-00000000000a';
  ASSERT v_status='balanced', format('CHECK 10 FAIL: FULLDEN status changed on rerun, got %L', v_status);
  RAISE NOTICE 'PASS [10/14] reconciler idempotent on rerun (stable events, stable status)';
END $$;

-- CHECK 11 — E1 preserved: payment-without-billed stays incomplete, no short_pay.
DO $$
DECLARE v_status text; v_ob numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount INTO v_status, v_ob FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-000000000012';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e3c00000-0000-4000-8000-000000000012' AND event_type='short_pay_detected';
  ASSERT v_status='incomplete', format('CHECK 11 FAIL: E1 status expected incomplete, got %L', v_status);
  ASSERT v_ob IS NULL, format('CHECK 11 FAIL: E1 open_balance expected NULL, got %L', v_ob);
  ASSERT v_sp=0, format('CHECK 11 FAIL: E1 short_pay expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [11/14] E1 preserved: payment-without-billed incomplete, no short_pay';
END $$;

-- CHECK 12 — E2 preserved: balanced billed/allowed/paid/patient_resp; denied_amount NULL.
DO $$
DECLARE v_status text; v_den numeric;
BEGIN
  SELECT reconciliation_status, denied_amount INTO v_status, v_den FROM fte_financial_positions WHERE claim_id='e3c00000-0000-4000-8000-000000000013';
  ASSERT v_status='balanced', format('CHECK 12 FAIL: E2 status expected balanced, got %L', v_status);
  ASSERT v_den IS NULL, format('CHECK 12 FAIL: E2 denied_amount expected NULL (no denial), got %s', v_den);
  RAISE NOTICE 'PASS [12/14] E2 preserved: balanced with no denial; denied_amount NULL';
END $$;

-- CHECK 13 — Review queue: OVERDEN routed; FULLDEN not routed.
DO $$
DECLARE v_over bigint; v_full bigint;
BEGIN
  SELECT COUNT(*) INTO v_over FROM fte_review_queue WHERE claim_id='e3c00000-0000-4000-8000-000000000011';
  SELECT COUNT(*) INTO v_full FROM fte_review_queue WHERE claim_id='e3c00000-0000-4000-8000-00000000000a';
  ASSERT v_over>=1, format('CHECK 13 FAIL: OVERDEN expected in review_queue, got %s', v_over);
  ASSERT v_full=0, format('CHECK 13 FAIL: FULLDEN expected NOT in review_queue, got %s', v_full);
  RAISE NOTICE 'PASS [13/14] review queue routes over-denial only; balanced denial not routed';
END $$;

-- CHECK 14 — Event evidence: FULLDEN denial_posted has exactly one evidence link.
DO $$
DECLARE v_links bigint;
BEGIN
  SELECT COUNT(*) INTO v_links FROM fte_event_evidence ee JOIN fte_claim_events ce ON ce.id=ee.claim_event_id
  WHERE ce.claim_id='e3c00000-0000-4000-8000-00000000000a' AND ce.event_type='denial_posted';
  ASSERT v_links=1, format('CHECK 14 FAIL: FULLDEN denial evidence links expected 1, got %s', v_links);
  RAISE NOTICE 'PASS [14/14] denial_posted event has exactly one evidence link';
END $$;

rollback;

-- =============================================================================
-- All 14 checks passed if you see PASS [1/14]..[14/14] above and no EXCEPTION.
-- ROLLBACK ensures no synthetic fixture or derived row persists.
-- =============================================================================

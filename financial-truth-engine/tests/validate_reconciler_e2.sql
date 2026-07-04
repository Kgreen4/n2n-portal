-- =============================================================================
-- Financial Truth Engine — Reconciler E2 Accounting Validation
-- tests/validate_reconciler_e2.sql
--
-- Covers Task 010B (E2):
--   * Phase 4b: DERIVED contractual_adjustment_applied = billed - allowed
--     (no contractual_adjustment observation is ever stored), with two evidence
--     links; zero-amount event when billed == allowed.
--   * Phase 5d: patient_responsibility_assigned from a canonical
--     patient_responsibility observation, with an evidence link.
--   * Phase 6: allowed_amount + patient_responsibility_amount columns; enhanced
--     open_balance = billed - contractual_adjustment - paid - patient_responsibility.
--   * A nonzero patient responsibility is NOT a short pay; a claim can be
--     balanced with nonzero patient responsibility.
--   * E1 preserved: payment-without-billed stays 'incomplete', open_balance NULL,
--     no short_pay.
--   * mark_duplicate suppression prevents double counting original + corrected.
--   * Idempotency on rerun.
--
-- Self-contained: creates its own synthetic fixture INSIDE the transaction and
-- ROLLs BACK at the end. All identifiers and amounts are synthetic. No PHI, no
-- production data, no real claim/check numbers.
--
-- Run AFTER:
--   psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql .. 011
--   psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
--
-- Usage:
--   psql "$DATABASE_URL" -f tests/validate_reconciler_e2.sql
--
-- Emits RAISE NOTICE 'PASS [n/15] ...' on success; any failure RAISEs EXCEPTION
-- and the transaction rolls back.
-- =============================================================================

begin;

-- ---- Synthetic fixture (rolled back at end) ---------------------------------
INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e2000000-0000-4000-8000-0000000000fe', 'E2 Accounting Test Practice', 'SYN-E2-PRACTICE');

INSERT INTO fte_evidence
  (id, practice_id, evidence_type, fixture_id, source_uri, page_number, metadata)
VALUES
  ('e2e00000-0000-4000-8000-00000000000a',
   'e2000000-0000-4000-8000-0000000000fe',
   'ocr_text', 'SYN_E2_PAGE_TEXT',
   'private://fte/de-identified/SYN_E2/page_001', 1, '{}'::jsonb);

INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e2c00000-0000-4000-8000-00000000000a','e2000000-0000-4000-8000-0000000000fe','SYN-E2-BAL','SYN-E2-BAL','Synthetic Payer','open'),
  ('e2c00000-0000-4000-8000-00000000000b','e2000000-0000-4000-8000-0000000000fe','SYN-E2-SHORT','SYN-E2-SHORT','Synthetic Payer','open'),
  ('e2c00000-0000-4000-8000-00000000000c','e2000000-0000-4000-8000-0000000000fe','SYN-E2-INC','SYN-E2-INC','Synthetic Payer','open'),
  ('e2c00000-0000-4000-8000-00000000000d','e2000000-0000-4000-8000-0000000000fe','SYN-E2-ALLOWONLY','SYN-E2-ALLOWONLY','Synthetic Payer','open'),
  ('e2c00000-0000-4000-8000-00000000000e','e2000000-0000-4000-8000-0000000000fe','SYN-E2-ZERO','SYN-E2-ZERO','Synthetic Payer','open'),
  ('e2c00000-0000-4000-8000-00000000000f','e2000000-0000-4000-8000-0000000000fe','SYN-E2-DUP','SYN-E2-DUP','Synthetic Payer','open');

-- Helper column list is the same for every observation insert below.
-- BAL: billed 100, allowed 80 (adj 20), paid 20, patient_resp 60 -> open 0, balanced.
-- Column-explicit INSERT ... SELECT: the invariant columns (practice_id,
-- evidence_id, payer_name, confidence_score, page_number, flags, metadata) are
-- set ONCE as constants in the SELECT, so each per-row tuple carries only the
-- fields that vary. This removes practice_id/evidence_id from the wide per-row
-- VALUES entirely, preventing the positional-drift typo class.
INSERT INTO fte_observations
  (practice_id, evidence_id, payer_name, confidence_score, page_number,
   is_summary_row, is_superseded, metadata,
   id, observation_type, amount, amount_type, claim_identifier,
   check_eft_identifier, raw_value, normalized_value)
SELECT
  'e2000000-0000-4000-8000-0000000000fe'::uuid,
  'e2e00000-0000-4000-8000-00000000000a'::uuid,
  'Synthetic Payer', 0.9000, 1,
  false, false, '{}'::jsonb,
  v.id, v.observation_type, v.amount, v.amount_type, v.claim_identifier,
  v.check_eft_identifier, v.raw_value, v.normalized_value
FROM (VALUES
  -- id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, raw_value, normalized_value
  -- BAL: billed 100, allowed 80 (adj 20), paid 20, patient_resp 60 -> open 0, balanced.
  ('e2b00000-0000-4000-8000-00000000000a'::uuid, 'billed_amount'::text, 100.00::numeric, 'billed'::text, 'SYN-E2-BAL'::text, NULL::text, '100.00'::text, '100.00'::text),
  ('e2b00000-0000-4000-8000-00000000000b', 'allowed_amount', 80.00, 'allowed', 'SYN-E2-BAL', NULL, '80.00', '80.00'),
  ('e2b00000-0000-4000-8000-00000000000c', 'payment', 20.00, 'paid', 'SYN-E2-BAL', 'SYN-E2-CHK-BAL', '20.00', '20.00'),
  ('e2b00000-0000-4000-8000-00000000000d', 'patient_responsibility', 60.00, 'patient_responsibility', 'SYN-E2-BAL', NULL, '60.00', '60.00'),
  -- SHORT: billed 100, allowed 80 (adj 20), paid 10, patient_resp 30 -> open 40, unbalanced + short_pay.
  ('e2b00000-0000-4000-8000-00000000000e', 'billed_amount', 100.00, 'billed', 'SYN-E2-SHORT', NULL, '100.00', '100.00'),
  ('e2b00000-0000-4000-8000-00000000000f', 'allowed_amount', 80.00, 'allowed', 'SYN-E2-SHORT', NULL, '80.00', '80.00'),
  ('e2b00000-0000-4000-8000-000000000010', 'payment', 10.00, 'paid', 'SYN-E2-SHORT', 'SYN-E2-CHK-SHORT', '10.00', '10.00'),
  ('e2b00000-0000-4000-8000-000000000011', 'patient_responsibility', 30.00, 'patient_responsibility', 'SYN-E2-SHORT', NULL, '30.00', '30.00'),
  -- INC: payment only (trusted via check), no billed -> incomplete (E1).
  ('e2b00000-0000-4000-8000-000000000012', 'payment', 25.00, 'paid', 'SYN-E2-INC', 'SYN-E2-CHK-INC', '25.00', '25.00'),
  -- ALLOWONLY: allowed only, no billed -> no contractual derived, no claim_adjudicated.
  ('e2b00000-0000-4000-8000-000000000013', 'allowed_amount', 70.00, 'allowed', 'SYN-E2-ALLOWONLY', NULL, '70.00', '70.00'),
  -- ZERO: billed 50, allowed 50 (adj 0), paid 50 -> open 0, balanced; zero-amount contractual event.
  ('e2b00000-0000-4000-8000-000000000014', 'billed_amount', 50.00, 'billed', 'SYN-E2-ZERO', NULL, '50.00', '50.00'),
  ('e2b00000-0000-4000-8000-000000000015', 'allowed_amount', 50.00, 'allowed', 'SYN-E2-ZERO', NULL, '50.00', '50.00'),
  ('e2b00000-0000-4000-8000-000000000016', 'payment', 50.00, 'paid', 'SYN-E2-ZERO', 'SYN-E2-CHK-ZERO', '50.00', '50.00'),
  -- DUP: original billed (suppressed) + corrected billed (canonical) + allowed -> no double count.
  ('e2b00000-0000-4000-8000-000000000017', 'billed_amount', 100.00, 'billed', 'SYN-E2-DUP', NULL, '100.00', '100.00'),
  ('e2b00000-0000-4000-8000-000000000018', 'billed_amount', 100.00, 'billed', 'SYN-E2-DUP', NULL, '100.00', '100.00'),
  ('e2b00000-0000-4000-8000-000000000019', 'allowed_amount', 80.00, 'allowed', 'SYN-E2-DUP', NULL, '80.00', '80.00')
) AS v(id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, raw_value, normalized_value);

-- DUP: suppress the original billed (…017) as a duplicate of the corrected (…018).
INSERT INTO fte_review_resolutions
  (practice_id, observation_id, target_observation_id, claim_id, action, target_type,
   target_claim_number, resolved_by, is_superseded, metadata)
VALUES
  ('e2000000-0000-4000-8000-0000000000fe',
   'e2b00000-0000-4000-8000-000000000017',
   'e2b00000-0000-4000-8000-000000000018',
   'e2c00000-0000-4000-8000-00000000000f',
   'mark_duplicate','observation','SYN-E2-DUP','test_runner',false,'{}'::jsonb);

-- ---- Run reconciler ---------------------------------------------------------
SELECT fte_reconcile_practice('e2000000-0000-4000-8000-0000000000fe');


-- =============================================================================
-- CHECK 1 — BAL: balanced despite nonzero patient responsibility
-- =============================================================================
DO $$
DECLARE v_status text; v_ob numeric;
BEGIN
  SELECT reconciliation_status, open_balance_amount INTO v_status, v_ob
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a';
  ASSERT v_status = 'balanced', format('CHECK 1 FAIL: BAL status expected balanced, got %L', v_status);
  ASSERT v_ob = 0, format('CHECK 1 FAIL: BAL open_balance expected 0, got %s', v_ob);
  RAISE NOTICE 'PASS [1/15] BAL balanced with open_balance 0 (nonzero patient responsibility)';
END $$;

-- =============================================================================
-- CHECK 2 — BAL: contractual_adjustment_amount derived = billed - allowed = 20
-- =============================================================================
DO $$
DECLARE v_adj numeric;
BEGIN
  SELECT contractual_adjustment_amount INTO v_adj
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a';
  ASSERT v_adj = 20.00, format('CHECK 2 FAIL: BAL contractual_adjustment expected 20.00, got %s', v_adj);
  RAISE NOTICE 'PASS [2/15] BAL contractual_adjustment derived = billed - allowed';
END $$;

-- =============================================================================
-- CHECK 3 — No contractual_adjustment OBSERVATION was stored (policy ruling #7)
-- =============================================================================
DO $$
DECLARE v_ct bigint;
BEGIN
  SELECT COUNT(*) INTO v_ct FROM fte_observations
  WHERE practice_id = 'e2000000-0000-4000-8000-0000000000fe'
    AND observation_type = 'contractual_adjustment';
  ASSERT v_ct = 0, format('CHECK 3 FAIL: contractual_adjustment observations expected 0, got %s', v_ct);
  RAISE NOTICE 'PASS [3/15] contractual adjustment is derived, not stored as an observation';
END $$;

-- =============================================================================
-- CHECK 4 — BAL: derived contractual event has exactly two evidence links
-- =============================================================================
DO $$
DECLARE v_links bigint;
BEGIN
  SELECT COUNT(*) INTO v_links
  FROM fte_event_evidence ee
  JOIN fte_claim_events ce ON ce.id = ee.claim_event_id
  WHERE ce.claim_id = 'e2c00000-0000-4000-8000-00000000000a'
    AND ce.event_type = 'contractual_adjustment_applied';
  ASSERT v_links = 2, format('CHECK 4 FAIL: BAL contractual event evidence links expected 2, got %s', v_links);
  RAISE NOTICE 'PASS [4/15] derived contractual event has two evidence links';
END $$;

-- =============================================================================
-- CHECK 5 — BAL: patient_responsibility_assigned event exists with evidence link
-- =============================================================================
DO $$
DECLARE v_events bigint; v_links bigint;
BEGIN
  SELECT COUNT(*) INTO v_events FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a'
    AND event_type = 'patient_responsibility_assigned';
  ASSERT v_events = 1, format('CHECK 5 FAIL: BAL patient_responsibility_assigned events expected 1, got %s', v_events);
  SELECT COUNT(*) INTO v_links
  FROM fte_event_evidence ee JOIN fte_claim_events ce ON ce.id = ee.claim_event_id
  WHERE ce.claim_id = 'e2c00000-0000-4000-8000-00000000000a'
    AND ce.event_type = 'patient_responsibility_assigned';
  ASSERT v_links >= 1, format('CHECK 5 FAIL: BAL patient_responsibility event evidence links expected >=1, got %s', v_links);
  RAISE NOTICE 'PASS [5/15] patient_responsibility_assigned emitted with evidence link';
END $$;

-- =============================================================================
-- CHECK 6 — BAL: position patient_responsibility_amount populated (= 60)
-- =============================================================================
DO $$
DECLARE v_pr numeric;
BEGIN
  SELECT patient_responsibility_amount INTO v_pr
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a';
  ASSERT v_pr = 60.00, format('CHECK 6 FAIL: BAL patient_responsibility_amount expected 60.00, got %s', v_pr);
  RAISE NOTICE 'PASS [6/15] position patient_responsibility_amount populated';
END $$;

-- =============================================================================
-- CHECK 7 — BAL: allowed_amount derived (= billed - contractual = 80)
-- =============================================================================
DO $$
DECLARE v_allowed numeric;
BEGIN
  SELECT allowed_amount INTO v_allowed
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a';
  ASSERT v_allowed = 80.00, format('CHECK 7 FAIL: BAL allowed_amount expected 80.00, got %s', v_allowed);
  RAISE NOTICE 'PASS [7/15] position allowed_amount derived from events';
END $$;

-- =============================================================================
-- CHECK 8 — BAL: no short_pay_detected (balanced with patient responsibility)
-- =============================================================================
DO $$
DECLARE v_ct bigint;
BEGIN
  SELECT COUNT(*) INTO v_ct FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a' AND event_type = 'short_pay_detected';
  ASSERT v_ct = 0, format('CHECK 8 FAIL: BAL short_pay_detected expected 0, got %s', v_ct);
  RAISE NOTICE 'PASS [8/15] balanced claim with patient responsibility emits no short_pay';
END $$;

-- =============================================================================
-- CHECK 9 — SHORT: unbalanced, open_balance 40, short_pay_detected emitted
-- =============================================================================
DO $$
DECLARE v_status text; v_ob numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount INTO v_status, v_ob
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000b';
  ASSERT v_status = 'unbalanced', format('CHECK 9 FAIL: SHORT status expected unbalanced, got %L', v_status);
  ASSERT v_ob = 40.00, format('CHECK 9 FAIL: SHORT open_balance expected 40.00, got %s', v_ob);
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000b' AND event_type = 'short_pay_detected';
  ASSERT v_sp = 1, format('CHECK 9 FAIL: SHORT short_pay_detected expected 1, got %s', v_sp);
  RAISE NOTICE 'PASS [9/15] SHORT unbalanced with open_balance 40 and short_pay_detected';
END $$;

-- =============================================================================
-- CHECK 10 — INC: E1 preserved — incomplete, open_balance NULL, no short_pay
-- =============================================================================
DO $$
DECLARE v_status text; v_ob numeric; v_sp bigint;
BEGIN
  SELECT reconciliation_status, open_balance_amount INTO v_status, v_ob
  FROM fte_financial_positions WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000c';
  ASSERT v_status = 'incomplete', format('CHECK 10 FAIL: INC status expected incomplete, got %L', v_status);
  ASSERT v_ob IS NULL, format('CHECK 10 FAIL: INC open_balance expected NULL, got %L', v_ob);
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000c' AND event_type = 'short_pay_detected';
  ASSERT v_sp = 0, format('CHECK 10 FAIL: INC short_pay_detected expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [10/15] E1 preserved: payment-without-billed stays incomplete, no short_pay';
END $$;

-- =============================================================================
-- CHECK 11 — ALLOWONLY: no contractual_adjustment_applied and no claim_adjudicated
-- =============================================================================
DO $$
DECLARE v_adj bigint; v_billed bigint;
BEGIN
  SELECT COUNT(*) INTO v_adj FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000d' AND event_type = 'contractual_adjustment_applied';
  SELECT COUNT(*) INTO v_billed FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000d' AND event_type = 'claim_adjudicated';
  ASSERT v_adj = 0, format('CHECK 11 FAIL: ALLOWONLY contractual events expected 0, got %s', v_adj);
  ASSERT v_billed = 0, format('CHECK 11 FAIL: ALLOWONLY claim_adjudicated events expected 0, got %s', v_billed);
  RAISE NOTICE 'PASS [11/15] allowed without billed derives no contractual adjustment';
END $$;

-- =============================================================================
-- CHECK 12 — ZERO: billed == allowed emits a zero-amount contractual event; balanced
-- =============================================================================
DO $$
DECLARE v_ct bigint; v_amt numeric; v_status text;
BEGIN
  SELECT COUNT(*) INTO v_ct FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000e' AND event_type = 'contractual_adjustment_applied';
  ASSERT v_ct = 1, format('CHECK 12 FAIL: ZERO contractual events expected 1, got %s', v_ct);
  SELECT amount INTO v_amt FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000e' AND event_type = 'contractual_adjustment_applied';
  ASSERT v_amt = 0, format('CHECK 12 FAIL: ZERO contractual amount expected 0, got %s', v_amt);
  SELECT reconciliation_status INTO v_status FROM fte_financial_positions
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000e';
  ASSERT v_status = 'balanced', format('CHECK 12 FAIL: ZERO status expected balanced, got %L', v_status);
  RAISE NOTICE 'PASS [12/15] billed == allowed emits zero-amount contractual event; balanced';
END $$;

-- =============================================================================
-- CHECK 13 — DUP: mark_duplicate prevents double count (1 billed, 1 contractual)
-- =============================================================================
DO $$
DECLARE v_billed bigint; v_adj bigint;
BEGIN
  SELECT COUNT(*) INTO v_billed FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000f' AND event_type = 'claim_adjudicated';
  SELECT COUNT(*) INTO v_adj FROM fte_claim_events
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000f' AND event_type = 'contractual_adjustment_applied';
  ASSERT v_billed = 1, format('CHECK 13 FAIL: DUP claim_adjudicated expected 1 (no double count), got %s', v_billed);
  ASSERT v_adj = 1, format('CHECK 13 FAIL: DUP contractual events expected 1, got %s', v_adj);
  RAISE NOTICE 'PASS [13/15] mark_duplicate original/corrected pair does not double count';
END $$;

-- =============================================================================
-- CHECK 14 — DUP: derived contractual amount uses canonical billed (100-80=20)
-- =============================================================================
DO $$
DECLARE v_adj numeric;
BEGIN
  SELECT contractual_adjustment_amount INTO v_adj FROM fte_financial_positions
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000f';
  ASSERT v_adj = 20.00, format('CHECK 14 FAIL: DUP contractual_adjustment expected 20.00, got %s', v_adj);
  RAISE NOTICE 'PASS [14/15] DUP contractual derived from canonical billed only';
END $$;

-- =============================================================================
-- CHECK 15 — Idempotency: rerun yields same statuses and stable event count
-- =============================================================================
DO $$
DECLARE v_before bigint; v_after bigint; v_bal text; v_short text;
BEGIN
  SELECT COUNT(*) INTO v_before FROM fte_claim_events
  WHERE practice_id = 'e2000000-0000-4000-8000-0000000000fe';

  PERFORM fte_reconcile_practice('e2000000-0000-4000-8000-0000000000fe');

  SELECT COUNT(*) INTO v_after FROM fte_claim_events
  WHERE practice_id = 'e2000000-0000-4000-8000-0000000000fe';
  ASSERT v_after = v_before,
    format('CHECK 15 FAIL: event count changed on rerun (before %s, after %s)', v_before, v_after);

  SELECT reconciliation_status INTO v_bal FROM fte_financial_positions
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000a';
  SELECT reconciliation_status INTO v_short FROM fte_financial_positions
  WHERE claim_id = 'e2c00000-0000-4000-8000-00000000000b';
  ASSERT v_bal = 'balanced' AND v_short = 'unbalanced',
    format('CHECK 15 FAIL: statuses changed on rerun (BAL %L, SHORT %L)', v_bal, v_short);

  RAISE NOTICE 'PASS [15/15] reconciler idempotent on rerun (stable events and statuses)';
END $$;

rollback;

-- =============================================================================
-- All 15 checks passed if you see PASS [1/15]..[15/15] above and no EXCEPTION.
-- ROLLBACK ensures no synthetic fixture or derived row persists.
-- =============================================================================

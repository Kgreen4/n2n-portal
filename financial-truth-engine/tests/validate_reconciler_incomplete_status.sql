-- =============================================================================
-- Financial Truth Engine — Reconciler E1 Incomplete-Status Validation
-- tests/validate_reconciler_incomplete_status.sql
--
-- Covers Task 007J (E1): a claim with emitted events but NO claim_adjudicated
-- (billed) event must yield fte_financial_positions.reconciliation_status =
-- 'incomplete' (not 'balanced'), open_balance_amount NULL, a review_queue row,
-- and NO short_pay_detected event. Also asserts a billed-known claim still
-- reconciles to 'balanced' (regression), and that the reconciler is idempotent
-- on rerun.
--
-- Self-contained: this file creates its own synthetic fixture INSIDE the outer
-- transaction and ROLLs BACK at the end, so nothing persists. All identifiers
-- and amounts are synthetic. No PHI, no production data, no real claim/check
-- numbers.
--
-- Run AFTER:
--   psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
--   psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
--
-- Usage:
--   psql "$DATABASE_URL" -f tests/validate_reconciler_incomplete_status.sql
--
-- Emits RAISE NOTICE 'PASS [n/7] ...' on success; any failure RAISEs EXCEPTION
-- and the transaction rolls back.
-- =============================================================================

begin;

-- ---- Synthetic fixture (rolled back at end) ---------------------------------
INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e1000000-0000-4000-8000-0000000000fe', 'E1 Incomplete-Status Test Practice', 'SYN-E1-PRACTICE');

INSERT INTO fte_evidence
  (id, practice_id, evidence_type, fixture_id, source_uri, page_number, metadata)
VALUES
  ('e1e00000-0000-4000-8000-00000000000a',
   'e1000000-0000-4000-8000-0000000000fe',
   'ocr_text', 'SYN_E1_PAGE_TEXT',
   'private://fte/de-identified/SYN_E1/page_001', 1, '{}'::jsonb);

-- Two claims: INC (incomplete) and BAL (regression, balanced).
INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e1c00000-0000-4000-8000-00000000000a',
   'e1000000-0000-4000-8000-0000000000fe',
   'SYN-E1-INC-0001', 'SYN-E1-INC-0001', 'Synthetic Payer', 'open'),
  ('e1c00000-0000-4000-8000-00000000000b',
   'e1000000-0000-4000-8000-0000000000fe',
   'SYN-E1-BAL-0001', 'SYN-E1-BAL-0001', 'Synthetic Payer', 'open');

-- INC claim: payment observation only (trusted: has check_eft), NO billed obs.
INSERT INTO fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type,
   claim_identifier, check_eft_identifier, payer_name,
   raw_value, normalized_value, confidence_score, page_number,
   is_summary_row, is_superseded, metadata)
VALUES
  ('e10b0000-0000-4000-8000-00000000000a',
   'e1000000-0000-4000-8000-0000000000fe',
   'e1e00000-0000-4000-8000-00000000000a',
   'payment', 50.00, 'paid',
   'SYN-E1-INC-0001', 'SYN-E1-CHK-0001', 'Synthetic Payer',
   '50.00', '50.00', 0.9000, 1,
   false, false, '{}'::jsonb);

-- BAL claim: billed + payment, amounts equal → open_balance 0 → balanced.
INSERT INTO fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type,
   claim_identifier, check_eft_identifier, payer_name,
   raw_value, normalized_value, confidence_score, page_number,
   is_summary_row, is_superseded, metadata)
VALUES
  ('e10b0000-0000-4000-8000-00000000000b',
   'e1000000-0000-4000-8000-0000000000fe',
   'e1e00000-0000-4000-8000-00000000000a',
   'billed_amount', 100.00, 'billed',
   'SYN-E1-BAL-0001', NULL, 'Synthetic Payer',
   '100.00', '100.00', 0.9000, 1,
   false, false, '{}'::jsonb),
  ('e10b0000-0000-4000-8000-00000000000c',
   'e1000000-0000-4000-8000-0000000000fe',
   'e1e00000-0000-4000-8000-00000000000a',
   'payment', 100.00, 'paid',
   'SYN-E1-BAL-0001', 'SYN-E1-CHK-0002', 'Synthetic Payer',
   '100.00', '100.00', 0.9000, 1,
   false, false, '{}'::jsonb);

-- ---- Run reconciler ---------------------------------------------------------
SELECT fte_reconcile_practice('e1000000-0000-4000-8000-0000000000fe');


-- =============================================================================
-- CHECK 1 — INC claim position status is 'incomplete' (not 'balanced')
-- =============================================================================
DO $$
DECLARE v_status text;
BEGIN
  SELECT reconciliation_status INTO v_status
  FROM fte_financial_positions
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000a';
  ASSERT v_status = 'incomplete',
    format('CHECK 1 FAIL: INC position status expected incomplete, got %L', v_status);
  RAISE NOTICE 'PASS [1/7] INC claim position status = incomplete';
END $$;

-- =============================================================================
-- CHECK 2 — INC claim open_balance_amount is NULL (billed unknown)
-- =============================================================================
DO $$
DECLARE v_ob numeric;
BEGIN
  SELECT open_balance_amount INTO v_ob
  FROM fte_financial_positions
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000a';
  ASSERT v_ob IS NULL,
    format('CHECK 2 FAIL: INC open_balance expected NULL, got %L', v_ob);
  RAISE NOTICE 'PASS [2/7] INC claim open_balance_amount IS NULL';
END $$;

-- =============================================================================
-- CHECK 3 — INC claim routed to review_queue exactly once
-- =============================================================================
DO $$
DECLARE v_count bigint;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM fte_review_queue
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000a'
    AND reason  = 'unbalanced_financial_position';
  ASSERT v_count = 1,
    format('CHECK 3 FAIL: INC review_queue rows expected 1, got %s', v_count);
  RAISE NOTICE 'PASS [3/7] INC claim routed to review_queue once';
END $$;

-- =============================================================================
-- CHECK 4 — INC claim emits NO short_pay_detected event
-- =============================================================================
DO $$
DECLARE v_count bigint;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM fte_claim_events
  WHERE claim_id   = 'e1c00000-0000-4000-8000-00000000000a'
    AND event_type = 'short_pay_detected';
  ASSERT v_count = 0,
    format('CHECK 4 FAIL: INC short_pay_detected expected 0, got %s', v_count);
  RAISE NOTICE 'PASS [4/7] INC claim emits no short_pay_detected';
END $$;

-- =============================================================================
-- CHECK 5 — Regression: BAL claim (billed known) reconciles to 'balanced'
-- =============================================================================
DO $$
DECLARE v_status text;
BEGIN
  SELECT reconciliation_status INTO v_status
  FROM fte_financial_positions
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000b';
  ASSERT v_status = 'balanced',
    format('CHECK 5 FAIL: BAL position status expected balanced, got %L', v_status);
  RAISE NOTICE 'PASS [5/7] BAL claim (billed known) status = balanced';
END $$;

-- =============================================================================
-- CHECK 6 — Regression: BAL claim NOT routed to review_queue
-- =============================================================================
DO $$
DECLARE v_count bigint;
BEGIN
  SELECT COUNT(*) INTO v_count
  FROM fte_review_queue
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000b';
  ASSERT v_count = 0,
    format('CHECK 6 FAIL: BAL review_queue rows expected 0, got %s', v_count);
  RAISE NOTICE 'PASS [6/7] BAL claim not routed to review_queue';
END $$;

-- =============================================================================
-- CHECK 7 — Idempotency: rerun reconciler; INC status and event count stable
-- =============================================================================
DO $$
DECLARE v_status text; v_events bigint;
BEGIN
  PERFORM fte_reconcile_practice('e1000000-0000-4000-8000-0000000000fe');

  SELECT reconciliation_status INTO v_status
  FROM fte_financial_positions
  WHERE claim_id = 'e1c00000-0000-4000-8000-00000000000a';
  ASSERT v_status = 'incomplete',
    format('CHECK 7 FAIL: after rerun INC status expected incomplete, got %L', v_status);

  SELECT COUNT(*) INTO v_events
  FROM fte_claim_events
  WHERE practice_id = 'e1000000-0000-4000-8000-0000000000fe';
  -- Expected events: INC payment_applied (1) + BAL claim_adjudicated (1)
  -- + BAL payment_applied (1) = 3, stable across reruns.
  ASSERT v_events = 3,
    format('CHECK 7 FAIL: after rerun total events expected 3, got %s', v_events);

  RAISE NOTICE 'PASS [7/7] reconciler idempotent on rerun (INC incomplete, 3 events)';
END $$;

rollback;

-- =============================================================================
-- All 7 checks passed if you see PASS [1/7]..[7/7] above and no EXCEPTION.
-- ROLLBACK ensures no synthetic fixture or derived row persists.
-- =============================================================================

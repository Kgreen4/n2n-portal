-- =============================================================================
-- Financial Truth Engine — Mock Extraction Contract Validation
-- tests/validate_mock_extraction.sql
--
-- 17 PASS checks verifying the end-to-end extraction boundary introduced in
-- Task 006F:
--
--   evidence → fte_mock_extract_observations → fte_observations
--           → fte_reconcile_practice → fte_financial_positions
--           → fte_explain_claim
--
-- CHECK  1  fte_mock_extract_observations exists in pg_proc
-- CHECK  2  calling function returns 6 (3 observations × 2 pages)
-- CHECK  3  fte_observations count = 6 for Phase 3B practice
-- CHECK  4  all extracted observations have confidence_score = 0.9500
-- CHECK  5  all extracted observations have metadata->>'extractor' = 'fte_mock_extract_observations'
-- CHECK  6  all extracted observations have raw_value LIKE '[SYNTHETIC]%'
-- CHECK  7  CLM-P3B-0001 reconciliation_status = 'balanced'
-- CHECK  8  CLM-P3B-0001 open_balance_amount = 0.00
-- CHECK  9  CLM-P3B-0002 reconciliation_status = 'unbalanced'
-- CHECK 10  CLM-P3B-0002 open_balance_amount = 70.00
-- CHECK 11  fte_review_queue has reason='unbalanced_financial_position' for CLM-P3B-0002
-- CHECK 12  fte_explain_claim for CLM-P3B-0001 returns non-null jsonb
-- CHECK 13  CLM-P3B-0001 explain output claim_number = 'CLM-P3B-0001'
-- CHECK 14  CLM-P3B-0001 explain output reconciliation_status = 'balanced'
-- CHECK 15  CLM-P3B-0001 explain output events array length = 3
-- CHECK 16  CLM-P3B-0001 explain output evidence array length = 2
-- CHECK 17  CLM-P3B-0001 payment_applied event has evidence_count = 2
--
-- Test vehicle: fixtures/synthetic_phase3b_mock_extractor_fixture.sql
--   Practice:   b3000000-0000-4000-8000-0000000000fe
--   Claims:     CLM-P3B-0001 (c3b00000-0000-4000-8000-000000000001) — balanced
--               CLM-P3B-0002 (c3b00000-0000-4000-8000-000000000002) — unbalanced
--
-- Prerequisites:
--   1. migrations 001–011 applied
--   2. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--   3. reconciler/fte_explain_claim.sql registered (CREATE OR REPLACE)
--   4. reconciler/fte_mock_extract_observations.sql registered (CREATE OR REPLACE)
--   5. fixtures/synthetic_phase3b_mock_extractor_fixture.sql loaded (committed)
--
-- Supabase SQL Editor note:
--   This suite assumes the Phase 3B fixture has already been loaded and
--   reconciler/fte_mock_extract_observations.sql has already been registered.
--   When running in the Supabase SQL Editor, paste and execute this file
--   starting from the BEGIN block.
--
-- psql convenience (from repo root):
--   \i fixtures/synthetic_phase3b_mock_extractor_fixture.sql
--   \i reconciler/fte_mock_extract_observations.sql
--   \i reconciler/fte_explain_claim.sql
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_mock_extraction.sql
--
-- No credentials or connection strings are stored here.
-- All fixtures are synthetic. No PHI. No production data.
-- =============================================================================

BEGIN;

DO $$
DECLARE
  v_practice_id  uuid := 'b3000000-0000-4000-8000-0000000000fe';
  v_claim_0001   uuid := 'c3b00000-0000-4000-8000-000000000001';
  v_claim_0002   uuid := 'c3b00000-0000-4000-8000-000000000002';

  v_result_0001  jsonb;
  v_count        bigint;
  v_num          integer;
  v_text         text;
BEGIN

  -- =========================================================================
  -- CHECK 1: fte_mock_extract_observations exists in pg_proc
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   pg_proc p
  JOIN   pg_namespace n ON n.oid = p.pronamespace
  WHERE  p.proname = 'fte_mock_extract_observations'
    AND  n.nspname = 'public';

  IF v_count = 0 THEN
    RAISE EXCEPTION 'FAIL [1/17] fte_mock_extract_observations not found in pg_proc';
  END IF;
  RAISE NOTICE 'PASS [1/17] fte_mock_extract_observations exists in pg_proc';


  -- =========================================================================
  -- CHECK 2: calling function returns 6 (3 obs × 2 pages)
  -- =========================================================================
  v_num := fte_mock_extract_observations(v_practice_id);

  IF v_num <> 6 THEN
    RAISE EXCEPTION 'FAIL [2/17] expected fte_mock_extract_observations to return 6, got %', v_num;
  END IF;
  RAISE NOTICE 'PASS [2/17] fte_mock_extract_observations returned 6';


  -- =========================================================================
  -- CHECK 3: fte_observations count = 6 for Phase 3B practice
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   fte_observations
  WHERE  practice_id = v_practice_id;

  IF v_count <> 6 THEN
    RAISE EXCEPTION 'FAIL [3/17] expected 6 observations, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [3/17] fte_observations count = 6';


  -- =========================================================================
  -- CHECK 4: all extracted observations have confidence_score = 0.9500
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   fte_observations
  WHERE  practice_id     = v_practice_id
    AND  confidence_score <> 0.9500;

  IF v_count > 0 THEN
    RAISE EXCEPTION 'FAIL [4/17] % observation(s) have confidence_score != 0.9500', v_count;
  END IF;
  RAISE NOTICE 'PASS [4/17] all observations have confidence_score = 0.9500';


  -- =========================================================================
  -- CHECK 5: all observations have metadata->>'extractor' = 'fte_mock_extract_observations'
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   fte_observations
  WHERE  practice_id = v_practice_id
    AND  metadata->>'extractor' <> 'fte_mock_extract_observations';

  IF v_count > 0 THEN
    RAISE EXCEPTION 'FAIL [5/17] % observation(s) missing correct extractor in metadata', v_count;
  END IF;
  RAISE NOTICE 'PASS [5/17] all observations have metadata extractor = fte_mock_extract_observations';


  -- =========================================================================
  -- CHECK 6: all extracted observations have raw_value LIKE '[SYNTHETIC]%'
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   fte_observations
  WHERE  practice_id = v_practice_id
    AND  (raw_value IS NULL OR raw_value NOT LIKE '[SYNTHETIC]%');

  IF v_count > 0 THEN
    RAISE EXCEPTION 'FAIL [6/17] % observation(s) have missing or non-[SYNTHETIC] raw_value', v_count;
  END IF;
  RAISE NOTICE 'PASS [6/17] all observations have raw_value starting with [SYNTHETIC]';


  -- =========================================================================
  -- Setup: materialize positions by running the reconciler.
  -- =========================================================================
  PERFORM fte_reconcile_practice(v_practice_id);


  -- =========================================================================
  -- CHECK 7: CLM-P3B-0001 reconciliation_status = 'balanced'
  -- =========================================================================
  SELECT fp.reconciliation_status INTO v_text
  FROM   fte_financial_positions fp
  JOIN   fte_claims c ON c.id = fp.claim_id
  WHERE  fp.practice_id  = v_practice_id
    AND  c.claim_number  = 'CLM-P3B-0001';

  IF v_text IS NULL OR v_text <> 'balanced' THEN
    RAISE EXCEPTION 'FAIL [7/17] expected CLM-P3B-0001 reconciliation_status=balanced, got %', v_text;
  END IF;
  RAISE NOTICE 'PASS [7/17] CLM-P3B-0001 reconciliation_status = balanced';


  -- =========================================================================
  -- CHECK 8: CLM-P3B-0001 open_balance_amount = 0.00
  -- =========================================================================
  SELECT fp.open_balance_amount INTO v_count
  FROM   fte_financial_positions fp
  JOIN   fte_claims c ON c.id = fp.claim_id
  WHERE  fp.practice_id = v_practice_id
    AND  c.claim_number = 'CLM-P3B-0001';

  IF v_count IS NULL OR v_count <> 0.00 THEN
    RAISE EXCEPTION 'FAIL [8/17] expected CLM-P3B-0001 open_balance_amount=0.00, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [8/17] CLM-P3B-0001 open_balance_amount = 0.00';


  -- =========================================================================
  -- CHECK 9: CLM-P3B-0002 reconciliation_status = 'unbalanced'
  -- =========================================================================
  SELECT fp.reconciliation_status INTO v_text
  FROM   fte_financial_positions fp
  JOIN   fte_claims c ON c.id = fp.claim_id
  WHERE  fp.practice_id = v_practice_id
    AND  c.claim_number = 'CLM-P3B-0002';

  IF v_text IS NULL OR v_text <> 'unbalanced' THEN
    RAISE EXCEPTION 'FAIL [9/17] expected CLM-P3B-0002 reconciliation_status=unbalanced, got %', v_text;
  END IF;
  RAISE NOTICE 'PASS [9/17] CLM-P3B-0002 reconciliation_status = unbalanced';


  -- =========================================================================
  -- CHECK 10: CLM-P3B-0002 open_balance_amount = 70.00
  -- =========================================================================
  SELECT fp.open_balance_amount INTO v_count
  FROM   fte_financial_positions fp
  JOIN   fte_claims c ON c.id = fp.claim_id
  WHERE  fp.practice_id = v_practice_id
    AND  c.claim_number = 'CLM-P3B-0002';

  IF v_count IS NULL OR v_count <> 70.00 THEN
    RAISE EXCEPTION 'FAIL [10/17] expected CLM-P3B-0002 open_balance_amount=70.00, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [10/17] CLM-P3B-0002 open_balance_amount = 70.00';


  -- =========================================================================
  -- CHECK 11: fte_review_queue has reason='unbalanced_financial_position' for CLM-P3B-0002
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   fte_review_queue rq
  JOIN   fte_claims c ON c.id = rq.claim_id
  WHERE  rq.practice_id = v_practice_id
    AND  c.claim_number = 'CLM-P3B-0002'
    AND  rq.reason      = 'unbalanced_financial_position';

  IF v_count = 0 THEN
    RAISE EXCEPTION 'FAIL [11/17] no unbalanced_financial_position review queue row for CLM-P3B-0002';
  END IF;
  RAISE NOTICE 'PASS [11/17] unbalanced_financial_position review queue row exists for CLM-P3B-0002';


  -- =========================================================================
  -- CHECK 12: fte_explain_claim for CLM-P3B-0001 returns non-null jsonb
  -- =========================================================================
  BEGIN
    v_result_0001 := fte_explain_claim(v_practice_id, v_claim_0001);
    IF v_result_0001 IS NULL THEN
      RAISE EXCEPTION 'FAIL [12/17] fte_explain_claim returned NULL for CLM-P3B-0001';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL [12/17] fte_explain_claim raised exception for CLM-P3B-0001: %', SQLERRM;
  END;
  RAISE NOTICE 'PASS [12/17] fte_explain_claim returns jsonb for CLM-P3B-0001';


  -- =========================================================================
  -- CHECK 13: CLM-P3B-0001 explain output claim_number = 'CLM-P3B-0001'
  -- =========================================================================
  IF v_result_0001->>'claim_number' <> 'CLM-P3B-0001' THEN
    RAISE EXCEPTION 'FAIL [13/17] expected claim_number=CLM-P3B-0001, got %',
      v_result_0001->>'claim_number';
  END IF;
  RAISE NOTICE 'PASS [13/17] CLM-P3B-0001 explain claim_number correct';


  -- =========================================================================
  -- CHECK 14: CLM-P3B-0001 explain output reconciliation_status = 'balanced'
  -- =========================================================================
  IF v_result_0001->>'reconciliation_status' <> 'balanced' THEN
    RAISE EXCEPTION 'FAIL [14/17] expected reconciliation_status=balanced, got %',
      v_result_0001->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [14/17] CLM-P3B-0001 explain reconciliation_status = balanced';


  -- =========================================================================
  -- CHECK 15: CLM-P3B-0001 explain output events array length = 3
  -- (claim_adjudicated + contractual_adjustment_applied + payment_applied)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'events');
  IF v_count <> 3 THEN
    RAISE EXCEPTION 'FAIL [15/17] expected events length=3, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [15/17] CLM-P3B-0001 explain events array length = 3';


  -- =========================================================================
  -- CHECK 16: CLM-P3B-0001 explain output evidence array length = 2
  -- (page evidence from observation + check_payment stub from two-link)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'evidence');
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [16/17] expected evidence length=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [16/17] CLM-P3B-0001 explain evidence array length = 2';


  -- =========================================================================
  -- CHECK 17: CLM-P3B-0001 payment_applied event has evidence_count = 2
  -- (page observation link + check_payment stub link = two-link)
  -- =========================================================================
  SELECT (e->>'evidence_count')::bigint INTO v_count
  FROM   jsonb_array_elements(v_result_0001->'events') AS e
  WHERE  e->>'event_type' = 'payment_applied'
  LIMIT 1;

  IF v_count IS NULL THEN
    RAISE EXCEPTION 'FAIL [17/17] payment_applied event not found in CLM-P3B-0001 events';
  END IF;
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [17/17] expected payment_applied evidence_count=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [17/17] CLM-P3B-0001 payment_applied evidence_count = 2';

END;
$$;

ROLLBACK;

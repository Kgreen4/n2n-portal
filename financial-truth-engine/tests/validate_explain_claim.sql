-- =============================================================================
-- Financial Truth Engine — Claim Explanation Function Validation
-- tests/validate_explain_claim.sql
--
-- 14 PASS checks verifying fte_explain_claim introduced in Task 006D.
--
-- CHECK  1  fte_explain_claim exists in pg_proc
-- CHECK  2  returns jsonb without exception for CLM-P3A-0001
-- CHECK  3  CLM-P3A-0001 claim_number = 'CLM-P3A-0001'
-- CHECK  4  CLM-P3A-0001 reconciliation_status = 'balanced'
-- CHECK  5  CLM-P3A-0001 open_balance_amount = '0.00'
-- CHECK  6  CLM-P3A-0001 summary contains 'balanced'
-- CHECK  7  CLM-P3A-0001 events array length = 3
-- CHECK  8  CLM-P3A-0001 evidence array length = 2
-- CHECK  9  CLM-P3A-0003 reconciliation_status = 'unbalanced'
-- CHECK 10  CLM-P3A-0003 open_balance_amount = '180.00'
-- CHECK 11  CLM-P3A-0003 summary contains '180.00'
-- CHECK 12  CLM-P3A-0003 review_queue length = 1 and reason = 'unbalanced_financial_position'
-- CHECK 13  CLM-P3A-0001 payment_applied event has evidence_count = 2
-- CHECK 14  all non-null raw_text_snippet values in both outputs have length <= 500
--
-- Test vehicle: fixtures/synthetic_phase3a_extraction_fixture.sql
--   Practice: a3000000-0000-4000-8000-0000000000fe
--   Claims:   CLM-P3A-0001 (c3a00000-0000-4000-8000-000000000001) — balanced
--             CLM-P3A-0003 (c3a00000-0000-4000-8000-000000000003) — unbalanced
--
-- Prerequisites:
--   1. migrations 001–011 applied
--   2. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--   3. reconciler/fte_explain_claim.sql registered (CREATE OR REPLACE)
--   4. fixtures/synthetic_phase3a_extraction_fixture.sql loaded (committed)
--
-- Supabase SQL Editor note:
--   This suite assumes the Phase 3A fixture has already been loaded and
--   reconciler/fte_explain_claim.sql has already been registered. When running in
--   the Supabase SQL Editor, paste and execute this file starting from the BEGIN
--   block.
--
-- psql convenience (from repo root):
--   \i fixtures/synthetic_phase3a_extraction_fixture.sql
--   \i reconciler/fte_explain_claim.sql
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_explain_claim.sql
--
-- No credentials or connection strings are stored here.
-- All fixtures are synthetic. No PHI. No production data.
-- =============================================================================

BEGIN;

DO $$
DECLARE
  v_practice_id  uuid := 'a3000000-0000-4000-8000-0000000000fe';
  v_claim_0001   uuid := 'c3a00000-0000-4000-8000-000000000001';
  v_claim_0003   uuid := 'c3a00000-0000-4000-8000-000000000003';

  v_result_0001  jsonb;
  v_result_0003  jsonb;

  v_count        bigint;
  v_text         text;
  v_bool         boolean;
BEGIN

  -- =========================================================================
  -- CHECK 1: fte_explain_claim exists in pg_proc
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM   pg_proc p
  JOIN   pg_namespace n ON n.oid = p.pronamespace
  WHERE  p.proname = 'fte_explain_claim'
    AND  n.nspname = 'public';

  IF v_count = 0 THEN
    RAISE EXCEPTION 'FAIL [1/14] fte_explain_claim not found in pg_proc';
  END IF;
  RAISE NOTICE 'PASS [1/14] fte_explain_claim exists in pg_proc';


  -- =========================================================================
  -- Setup: materialize positions by running the reconciler.
  -- =========================================================================
  PERFORM fte_reconcile_practice(v_practice_id);


  -- =========================================================================
  -- CHECK 2: returns jsonb without exception for CLM-P3A-0001
  -- =========================================================================
  BEGIN
    v_result_0001 := fte_explain_claim(v_practice_id, v_claim_0001);
    IF v_result_0001 IS NULL THEN
      RAISE EXCEPTION 'FAIL [2/14] fte_explain_claim returned NULL for CLM-P3A-0001';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL [2/14] fte_explain_claim raised exception for CLM-P3A-0001: %', SQLERRM;
  END;
  RAISE NOTICE 'PASS [2/14] fte_explain_claim returns jsonb for CLM-P3A-0001';


  -- =========================================================================
  -- CHECK 3: CLM-P3A-0001 claim_number = 'CLM-P3A-0001'
  -- =========================================================================
  IF v_result_0001->>'claim_number' <> 'CLM-P3A-0001' THEN
    RAISE EXCEPTION 'FAIL [3/14] expected claim_number=CLM-P3A-0001, got %',
      v_result_0001->>'claim_number';
  END IF;
  RAISE NOTICE 'PASS [3/14] CLM-P3A-0001 claim_number correct';


  -- =========================================================================
  -- CHECK 4: CLM-P3A-0001 reconciliation_status = 'balanced'
  -- =========================================================================
  IF v_result_0001->>'reconciliation_status' <> 'balanced' THEN
    RAISE EXCEPTION 'FAIL [4/14] expected reconciliation_status=balanced, got %',
      v_result_0001->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [4/14] CLM-P3A-0001 reconciliation_status = balanced';


  -- =========================================================================
  -- CHECK 5: CLM-P3A-0001 open_balance_amount = '0.00'
  -- =========================================================================
  IF v_result_0001->>'open_balance_amount' <> '0.00' THEN
    RAISE EXCEPTION 'FAIL [5/14] expected open_balance_amount=0.00, got %',
      v_result_0001->>'open_balance_amount';
  END IF;
  RAISE NOTICE 'PASS [5/14] CLM-P3A-0001 open_balance_amount = 0.00';


  -- =========================================================================
  -- CHECK 6: CLM-P3A-0001 summary contains 'balanced'
  -- =========================================================================
  v_text := v_result_0001->>'summary';
  IF v_text NOT LIKE '%balanced%' THEN
    RAISE EXCEPTION 'FAIL [6/14] summary does not contain ''balanced'': %', v_text;
  END IF;
  RAISE NOTICE 'PASS [6/14] CLM-P3A-0001 summary contains ''balanced''';


  -- =========================================================================
  -- CHECK 7: CLM-P3A-0001 events array length = 3
  -- (claim_adjudicated + contractual_adjustment_applied + payment_applied)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'events');
  IF v_count <> 3 THEN
    RAISE EXCEPTION 'FAIL [7/14] expected events length=3, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [7/14] CLM-P3A-0001 events array length = 3';


  -- =========================================================================
  -- CHECK 8: CLM-P3A-0001 evidence array length = 2
  -- (page evidence from observation + check_payment stub from two-link)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'evidence');
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [8/14] expected evidence length=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [8/14] CLM-P3A-0001 evidence array length = 2';


  -- =========================================================================
  -- Fetch CLM-P3A-0003 result
  -- =========================================================================
  v_result_0003 := fte_explain_claim(v_practice_id, v_claim_0003);
  IF v_result_0003 IS NULL THEN
    RAISE EXCEPTION 'FAIL fte_explain_claim returned NULL for CLM-P3A-0003';
  END IF;


  -- =========================================================================
  -- CHECK 9: CLM-P3A-0003 reconciliation_status = 'unbalanced'
  -- =========================================================================
  IF v_result_0003->>'reconciliation_status' <> 'unbalanced' THEN
    RAISE EXCEPTION 'FAIL [9/14] expected reconciliation_status=unbalanced, got %',
      v_result_0003->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [9/14] CLM-P3A-0003 reconciliation_status = unbalanced';


  -- =========================================================================
  -- CHECK 10: CLM-P3A-0003 open_balance_amount = '180.00'
  -- =========================================================================
  IF v_result_0003->>'open_balance_amount' <> '180.00' THEN
    RAISE EXCEPTION 'FAIL [10/14] expected open_balance_amount=180.00, got %',
      v_result_0003->>'open_balance_amount';
  END IF;
  RAISE NOTICE 'PASS [10/14] CLM-P3A-0003 open_balance_amount = 180.00';


  -- =========================================================================
  -- CHECK 11: CLM-P3A-0003 summary contains '180.00'
  -- =========================================================================
  v_text := v_result_0003->>'summary';
  IF v_text NOT LIKE '%180.00%' THEN
    RAISE EXCEPTION 'FAIL [11/14] summary does not contain ''180.00'': %', v_text;
  END IF;
  RAISE NOTICE 'PASS [11/14] CLM-P3A-0003 summary contains ''180.00''';


  -- =========================================================================
  -- CHECK 12: CLM-P3A-0003 review_queue length = 1 and
  --           reason = 'unbalanced_financial_position'
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0003->'review_queue');
  IF v_count <> 1 THEN
    RAISE EXCEPTION 'FAIL [12/14] expected review_queue length=1, got %', v_count;
  END IF;
  v_text := v_result_0003->'review_queue'->0->>'reason';
  IF v_text <> 'unbalanced_financial_position' THEN
    RAISE EXCEPTION 'FAIL [12/14] expected reason=unbalanced_financial_position, got %', v_text;
  END IF;
  RAISE NOTICE 'PASS [12/14] CLM-P3A-0003 review_queue length=1 and reason correct';


  -- =========================================================================
  -- CHECK 13: CLM-P3A-0001 payment_applied event has evidence_count = 2
  -- (page observation link + check_payment stub link)
  -- =========================================================================
  SELECT (e->>'evidence_count')::bigint INTO v_count
  FROM   jsonb_array_elements(v_result_0001->'events') AS e
  WHERE  e->>'event_type' = 'payment_applied'
  LIMIT 1;

  IF v_count IS NULL THEN
    RAISE EXCEPTION 'FAIL [13/14] payment_applied event not found in CLM-P3A-0001 events';
  END IF;
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [13/14] expected payment_applied evidence_count=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [13/14] CLM-P3A-0001 payment_applied evidence_count = 2';


  -- =========================================================================
  -- CHECK 14: all non-null raw_text_snippet values in both outputs have length <= 500
  -- =========================================================================
  SELECT COUNT(*) INTO v_count
  FROM (
    SELECT e->>'raw_text_snippet' AS snippet
    FROM   jsonb_array_elements(v_result_0001->'evidence') AS e
    UNION ALL
    SELECT e->>'raw_text_snippet' AS snippet
    FROM   jsonb_array_elements(v_result_0003->'evidence') AS e
  ) snippets
  WHERE snippet IS NOT NULL
    AND length(snippet) > 500;

  IF v_count > 0 THEN
    RAISE EXCEPTION 'FAIL [14/14] % raw_text_snippet value(s) exceed 500 chars', v_count;
  END IF;
  RAISE NOTICE 'PASS [14/14] all non-null raw_text_snippet values have length <= 500';

END;
$$;

ROLLBACK;

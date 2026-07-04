-- =============================================================================
-- Financial Truth Engine — Claim Explanation Function Validation
-- tests/validate_explain_claim.sql
--
-- 20 PASS checks verifying fte_explain_claim (Task 006D; extended in Task 014E2
-- to surface the E2 / denial / recoverable ledger fields).
--
-- CHECK 15  no-denial claim: denied/recoverable/nonrecoverable are null
-- CHECK 16  explanation surfaces allowed/patient_responsibility/denied/recoverable keys
-- CHECK 17  REC claim denied_amount surfaced
-- CHECK 18  REC recoverable_amount + derived nonrecoverable_denied_amount
-- CHECK 19  MIXED claim denied/recoverable/nonrecoverable split
-- CHECK 20  recoverable overlay leaves open_balance/status unchanged
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

-- ---------------------------------------------------------------------------
-- Task 014E2 self-contained denial + recoverable fixture (rolled back at end).
-- Exercises the new explanation ledger fields with real values, independent of
-- the (no-denial) Phase 3A fixture. Synthetic practice; CO-45 / CO-97 are
-- synthetic CARC placeholders.
-- ---------------------------------------------------------------------------
INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e5000000-0000-4000-8000-0000000000fe', 'Explain Ledger Fields Test Practice', 'SYN-EXP-PRACTICE');

INSERT INTO fte_evidence
  (id, practice_id, evidence_type, fixture_id, source_uri, page_number, metadata)
VALUES
  ('e5e00000-0000-4000-8000-00000000000a','e5000000-0000-4000-8000-0000000000fe',
   'ocr_text','SYN_EXP_PAGE_TEXT','private://fte/de-identified/SYN_EXP/page_001',1,'{}'::jsonb);

INSERT INTO fte_denial_knowledge (id, practice_id, carc_code, rarc_code, payer_name, recoverable) VALUES
  ('e5d00000-0000-4000-8000-000000000001', NULL, 'CO-45', NULL, NULL, true),
  ('e5d00000-0000-4000-8000-000000000002', NULL, 'CO-97', NULL, NULL, false);

INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e5c00000-0000-4000-8000-00000000000a','e5000000-0000-4000-8000-0000000000fe','SYN-EXP-REC','SYN-EXP-REC','Synthetic Payer','open'),
  ('e5c00000-0000-4000-8000-00000000000b','e5000000-0000-4000-8000-0000000000fe','SYN-EXP-MIXED','SYN-EXP-MIXED','Synthetic Payer','open');

-- Column-explicit INSERT ... SELECT: invariant columns (practice_id,
-- evidence_id, payer_name, confidence_score, page_number, flags, metadata) are
-- set ONCE as constants; per-row tuples carry only the varying fields, so
-- practice_id/evidence_id can't be positionally miswritten.
INSERT INTO fte_observations
  (practice_id, evidence_id, payer_name, confidence_score, page_number,
   is_summary_row, is_superseded, metadata,
   id, observation_type, amount, amount_type, claim_identifier,
   check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value)
SELECT
  'e5000000-0000-4000-8000-0000000000fe'::uuid,
  'e5e00000-0000-4000-8000-00000000000a'::uuid,
  'Synthetic Payer', 0.9000, 1,
  false, false, '{}'::jsonb,
  v.id, v.observation_type, v.amount, v.amount_type, v.claim_identifier,
  v.check_eft_identifier, v.carc_code, v.rarc_code, v.raw_value, v.normalized_value
FROM (VALUES
  -- id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value
  -- REC: billed 100, denial 100 CO-45 (recoverable) -> denied 100, recoverable 100, nonrecoverable 0.
  ('e5b00000-0000-4000-8000-000000000001'::uuid, 'billed_amount'::text, 100.00::numeric, 'billed'::text, 'SYN-EXP-REC'::text, NULL::text, NULL::text, NULL::text, '100.00'::text, '100.00'::text),
  ('e5b00000-0000-4000-8000-000000000002', 'denial', 100.00, 'denied', 'SYN-EXP-REC', NULL, 'CO-45', NULL, '100.00', '100.00'),
  -- MIXED: billed 100, denial 60 CO-45 (rec) + denial 40 CO-97 (non-rec) -> denied 100, recoverable 60, nonrecoverable 40.
  ('e5b00000-0000-4000-8000-000000000003', 'billed_amount', 100.00, 'billed', 'SYN-EXP-MIXED', NULL, NULL, NULL, '100.00', '100.00'),
  ('e5b00000-0000-4000-8000-000000000004', 'denial', 60.00, 'denied', 'SYN-EXP-MIXED', NULL, 'CO-45', NULL, '60.00', '60.00'),
  ('e5b00000-0000-4000-8000-000000000005', 'denial', 40.00, 'denied', 'SYN-EXP-MIXED', NULL, 'CO-97', NULL, '40.00', '40.00')
) AS v(id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value);

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
    RAISE EXCEPTION 'FAIL [1/20] fte_explain_claim not found in pg_proc';
  END IF;
  RAISE NOTICE 'PASS [1/20] fte_explain_claim exists in pg_proc';


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
      RAISE EXCEPTION 'FAIL [2/20] fte_explain_claim returned NULL for CLM-P3A-0001';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL [2/20] fte_explain_claim raised exception for CLM-P3A-0001: %', SQLERRM;
  END;
  RAISE NOTICE 'PASS [2/20] fte_explain_claim returns jsonb for CLM-P3A-0001';


  -- =========================================================================
  -- CHECK 3: CLM-P3A-0001 claim_number = 'CLM-P3A-0001'
  -- =========================================================================
  IF v_result_0001->>'claim_number' <> 'CLM-P3A-0001' THEN
    RAISE EXCEPTION 'FAIL [3/20] expected claim_number=CLM-P3A-0001, got %',
      v_result_0001->>'claim_number';
  END IF;
  RAISE NOTICE 'PASS [3/20] CLM-P3A-0001 claim_number correct';


  -- =========================================================================
  -- CHECK 4: CLM-P3A-0001 reconciliation_status = 'balanced'
  -- =========================================================================
  IF v_result_0001->>'reconciliation_status' <> 'balanced' THEN
    RAISE EXCEPTION 'FAIL [4/20] expected reconciliation_status=balanced, got %',
      v_result_0001->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [4/20] CLM-P3A-0001 reconciliation_status = balanced';


  -- =========================================================================
  -- CHECK 5: CLM-P3A-0001 open_balance_amount = '0.00'
  -- =========================================================================
  IF v_result_0001->>'open_balance_amount' <> '0.00' THEN
    RAISE EXCEPTION 'FAIL [5/20] expected open_balance_amount=0.00, got %',
      v_result_0001->>'open_balance_amount';
  END IF;
  RAISE NOTICE 'PASS [5/20] CLM-P3A-0001 open_balance_amount = 0.00';


  -- =========================================================================
  -- CHECK 6: CLM-P3A-0001 summary contains 'balanced'
  -- =========================================================================
  v_text := v_result_0001->>'summary';
  IF v_text NOT LIKE '%balanced%' THEN
    RAISE EXCEPTION 'FAIL [6/20] summary does not contain ''balanced'': %', v_text;
  END IF;
  RAISE NOTICE 'PASS [6/20] CLM-P3A-0001 summary contains ''balanced''';


  -- =========================================================================
  -- CHECK 7: CLM-P3A-0001 events array length = 3
  -- (claim_adjudicated + contractual_adjustment_applied + payment_applied)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'events');
  IF v_count <> 3 THEN
    RAISE EXCEPTION 'FAIL [7/20] expected events length=3, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [7/20] CLM-P3A-0001 events array length = 3';


  -- =========================================================================
  -- CHECK 8: CLM-P3A-0001 evidence array length = 2
  -- (page evidence from observation + check_payment stub from two-link)
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0001->'evidence');
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [8/20] expected evidence length=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [8/20] CLM-P3A-0001 evidence array length = 2';


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
    RAISE EXCEPTION 'FAIL [9/20] expected reconciliation_status=unbalanced, got %',
      v_result_0003->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [9/20] CLM-P3A-0003 reconciliation_status = unbalanced';


  -- =========================================================================
  -- CHECK 10: CLM-P3A-0003 open_balance_amount = '180.00'
  -- =========================================================================
  IF v_result_0003->>'open_balance_amount' <> '180.00' THEN
    RAISE EXCEPTION 'FAIL [10/20] expected open_balance_amount=180.00, got %',
      v_result_0003->>'open_balance_amount';
  END IF;
  RAISE NOTICE 'PASS [10/20] CLM-P3A-0003 open_balance_amount = 180.00';


  -- =========================================================================
  -- CHECK 11: CLM-P3A-0003 summary contains '180.00'
  -- =========================================================================
  v_text := v_result_0003->>'summary';
  IF v_text NOT LIKE '%180.00%' THEN
    RAISE EXCEPTION 'FAIL [11/20] summary does not contain ''180.00'': %', v_text;
  END IF;
  RAISE NOTICE 'PASS [11/20] CLM-P3A-0003 summary contains ''180.00''';


  -- =========================================================================
  -- CHECK 12: CLM-P3A-0003 review_queue length = 1 and
  --           reason = 'unbalanced_financial_position'
  -- =========================================================================
  v_count := jsonb_array_length(v_result_0003->'review_queue');
  IF v_count <> 1 THEN
    RAISE EXCEPTION 'FAIL [12/20] expected review_queue length=1, got %', v_count;
  END IF;
  v_text := v_result_0003->'review_queue'->0->>'reason';
  IF v_text <> 'unbalanced_financial_position' THEN
    RAISE EXCEPTION 'FAIL [12/20] expected reason=unbalanced_financial_position, got %', v_text;
  END IF;
  RAISE NOTICE 'PASS [12/20] CLM-P3A-0003 review_queue length=1 and reason correct';


  -- =========================================================================
  -- CHECK 13: CLM-P3A-0001 payment_applied event has evidence_count = 2
  -- (page observation link + check_payment stub link)
  -- =========================================================================
  SELECT (e->>'evidence_count')::bigint INTO v_count
  FROM   jsonb_array_elements(v_result_0001->'events') AS e
  WHERE  e->>'event_type' = 'payment_applied'
  LIMIT 1;

  IF v_count IS NULL THEN
    RAISE EXCEPTION 'FAIL [13/20] payment_applied event not found in CLM-P3A-0001 events';
  END IF;
  IF v_count <> 2 THEN
    RAISE EXCEPTION 'FAIL [13/20] expected payment_applied evidence_count=2, got %', v_count;
  END IF;
  RAISE NOTICE 'PASS [13/20] CLM-P3A-0001 payment_applied evidence_count = 2';


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
    RAISE EXCEPTION 'FAIL [14/20] % raw_text_snippet value(s) exceed 500 chars', v_count;
  END IF;
  RAISE NOTICE 'PASS [14/20] all non-null raw_text_snippet values have length <= 500';

END;
$$;


-- ===========================================================================
-- Task 014E2: new ledger-field surfacing (denied / recoverable / etc.).
-- Reconciles the self-contained SYN-EXP practice and asserts the extended
-- explanation output. No persistent 006L/009C reconcile.
-- ===========================================================================
DO $$
DECLARE
  v_exp_practice uuid := 'e5000000-0000-4000-8000-0000000000fe';
  v_rec          jsonb;
  v_mixed        jsonb;
  v_p3a          jsonb;
BEGIN
  PERFORM fte_reconcile_practice(v_exp_practice);

  v_rec   := fte_explain_claim(v_exp_practice, 'e5c00000-0000-4000-8000-00000000000a');
  v_mixed := fte_explain_claim(v_exp_practice, 'e5c00000-0000-4000-8000-00000000000b');
  v_p3a   := fte_explain_claim('a3000000-0000-4000-8000-0000000000fe', 'c3a00000-0000-4000-8000-000000000001');

  -- CHECK 15: no-denial claim surfaces denied/recoverable/nonrecoverable as JSON null.
  IF NOT (v_p3a->>'denied_amount' IS NULL
          AND v_p3a->>'recoverable_amount' IS NULL
          AND v_p3a->>'nonrecoverable_denied_amount' IS NULL) THEN
    RAISE EXCEPTION 'FAIL [15/20] no-denial claim expected null denial fields, got denied=% recoverable=% nonrec=%',
      v_p3a->>'denied_amount', v_p3a->>'recoverable_amount', v_p3a->>'nonrecoverable_denied_amount';
  END IF;
  RAISE NOTICE 'PASS [15/20] no-denial claim: denied/recoverable/nonrecoverable are null';

  -- CHECK 16: E2/denial ledger keys are present (surfaced) in the explanation.
  IF NOT (v_p3a ? 'allowed_amount' AND v_p3a ? 'patient_responsibility_amount'
          AND v_p3a ? 'denied_amount' AND v_p3a ? 'recoverable_amount'
          AND v_p3a ? 'nonrecoverable_denied_amount') THEN
    RAISE EXCEPTION 'FAIL [16/20] explanation missing one or more new ledger keys';
  END IF;
  RAISE NOTICE 'PASS [16/20] explanation surfaces allowed/patient_responsibility/denied/recoverable keys';

  -- CHECK 17: REC claim surfaces denied_amount.
  IF v_rec->>'denied_amount' <> '100.00' THEN
    RAISE EXCEPTION 'FAIL [17/20] REC denied_amount expected 100.00, got %', v_rec->>'denied_amount';
  END IF;
  RAISE NOTICE 'PASS [17/20] REC denied_amount surfaced';

  -- CHECK 18: REC recoverable_amount + derived nonrecoverable_denied_amount.
  IF NOT (v_rec->>'recoverable_amount' = '100.00' AND v_rec->>'nonrecoverable_denied_amount' = '0.00') THEN
    RAISE EXCEPTION 'FAIL [18/20] REC recoverable=100.00/nonrecoverable=0.00 expected, got rec=% nonrec=%',
      v_rec->>'recoverable_amount', v_rec->>'nonrecoverable_denied_amount';
  END IF;
  RAISE NOTICE 'PASS [18/20] REC recoverable + derived nonrecoverable correct';

  -- CHECK 19: MIXED claim aggregates recoverable subset; nonrecoverable is the remainder.
  IF NOT (v_mixed->>'denied_amount' = '100.00'
          AND v_mixed->>'recoverable_amount' = '60.00'
          AND v_mixed->>'nonrecoverable_denied_amount' = '40.00') THEN
    RAISE EXCEPTION 'FAIL [19/20] MIXED expected denied=100.00 recoverable=60.00 nonrec=40.00, got %/%/%',
      v_mixed->>'denied_amount', v_mixed->>'recoverable_amount', v_mixed->>'nonrecoverable_denied_amount';
  END IF;
  RAISE NOTICE 'PASS [19/20] MIXED denied/recoverable/nonrecoverable split correct';

  -- CHECK 20: recoverable overlay does not change accounting (open_balance / status).
  IF NOT (v_rec->>'open_balance_amount' = '0.00' AND v_rec->>'reconciliation_status' = 'balanced') THEN
    RAISE EXCEPTION 'FAIL [20/20] REC accounting changed by overlay: open=% status=%',
      v_rec->>'open_balance_amount', v_rec->>'reconciliation_status';
  END IF;
  RAISE NOTICE 'PASS [20/20] recoverable overlay leaves open_balance/status unchanged';

END;
$$;

ROLLBACK;

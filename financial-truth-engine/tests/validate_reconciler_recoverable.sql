-- =============================================================================
-- Financial Truth Engine — Reconciler Recoverable-Amount Validation
-- tests/validate_reconciler_recoverable.sql
--
-- Covers Task 014D (recoverable_amount overlay via fte_denial_knowledge):
--   * Phase 6b overlays recoverable_amount = recoverable subset of denied_amount,
--     using the most-specific applicable denial-knowledge row per denial_posted
--     event (practice+8 / payer+4 / carc+2 / rarc+1; top-tier conflict or no
--     match fails closed to non-recoverable).
--   * Reporting overlay only: does NOT change open_balance / short_pay / status.
--   * recoverable_amount NULL when no denials; 0 when denials but none recoverable.
--   * E1 (incomplete) and E2 (balanced) and 014B denied_amount behavior preserved.
--   * Idempotent on rerun.
--
-- Self-contained: creates its own synthetic fixture AND its own synthetic
-- fte_denial_knowledge rows INSIDE the transaction, and ROLLs BACK at the end,
-- so nothing persists. All identifiers/amounts are synthetic. CO-45 / CO-97 /
-- CO-50 / CO-16 / CO-11 / CO-99 / N130 are synthetic placeholders standing in
-- for CARC/RARC codes. No PHI, no production data.
--
-- Run AFTER migrations 001..011 and reconciler/fte_reconcile.sql.
-- Emits RAISE NOTICE 'PASS [n/13] ...'; any failure RAISEs EXCEPTION and rolls back.
-- =============================================================================

begin;

INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e4000000-0000-4000-8000-0000000000fe', 'Recoverable Amount Test Practice', 'SYN-REC-PRACTICE');

INSERT INTO fte_evidence
  (id, practice_id, evidence_type, fixture_id, source_uri, page_number, metadata)
VALUES
  ('e4e00000-0000-4000-8000-00000000000a',
   'e4000000-0000-4000-8000-0000000000fe',
   'ocr_text', 'SYN_REC_PAGE_TEXT',
   'private://fte/de-identified/SYN_REC/page_001', 1, '{}'::jsonb);

-- Synthetic denial-knowledge rows (rolled back at end).
INSERT INTO fte_denial_knowledge (id, practice_id, carc_code, rarc_code, payer_name, recoverable) VALUES
  ('e4d00000-0000-4000-8000-000000000001', NULL, 'CO-45', NULL, NULL, true),   -- carc-only recoverable
  ('e4d00000-0000-4000-8000-000000000002', NULL, 'CO-97', NULL, NULL, false),  -- carc-only non-recoverable
  ('e4d00000-0000-4000-8000-000000000003', NULL, 'CO-50', 'N130', NULL, true), -- exact carc+rarc recoverable (score 3)
  ('e4d00000-0000-4000-8000-000000000004', NULL, 'CO-50', NULL, NULL, false),  -- carc-only for CO-50 (score 2)
  ('e4d00000-0000-4000-8000-000000000005', NULL, 'CO-16', NULL, NULL, false),  -- global CO-16 non-recoverable (score 2)
  ('e4d00000-0000-4000-8000-000000000006', NULL, 'CO-16', NULL, 'Synthetic Payer', true), -- payer-specific CO-16 recoverable (score 6)
  ('e4d00000-0000-4000-8000-000000000007', NULL, 'CO-11', NULL, NULL, true),   -- CO-11 conflict A (score 2)
  ('e4d00000-0000-4000-8000-000000000008', NULL, 'CO-11', NULL, NULL, false);  -- CO-11 conflict B (score 2)

INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e4c00000-0000-4000-8000-00000000000a','e4000000-0000-4000-8000-0000000000fe','SYN-REC-REC','SYN-REC-REC','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-00000000000b','e4000000-0000-4000-8000-0000000000fe','SYN-REC-NONREC','SYN-REC-NONREC','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-00000000000c','e4000000-0000-4000-8000-0000000000fe','SYN-REC-MIXED','SYN-REC-MIXED','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-00000000000d','e4000000-0000-4000-8000-0000000000fe','SYN-REC-UNKNOWN','SYN-REC-UNKNOWN','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-00000000000e','e4000000-0000-4000-8000-0000000000fe','SYN-REC-NOCODE','SYN-REC-NOCODE','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-00000000000f','e4000000-0000-4000-8000-0000000000fe','SYN-REC-EXACT','SYN-REC-EXACT','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-000000000010','e4000000-0000-4000-8000-0000000000fe','SYN-REC-PAYER','SYN-REC-PAYER','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-000000000011','e4000000-0000-4000-8000-0000000000fe','SYN-REC-CONFLICT','SYN-REC-CONFLICT','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-000000000012','e4000000-0000-4000-8000-0000000000fe','SYN-REC-E1','SYN-REC-E1','Synthetic Payer','open'),
  ('e4c00000-0000-4000-8000-000000000013','e4000000-0000-4000-8000-0000000000fe','SYN-REC-E2','SYN-REC-E2','Synthetic Payer','open');

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
  'e4000000-0000-4000-8000-0000000000fe'::uuid,
  'e4e00000-0000-4000-8000-00000000000a'::uuid,
  'Synthetic Payer', 0.9000, 1,
  false, false, '{}'::jsonb,
  v.id, v.observation_type, v.amount, v.amount_type, v.claim_identifier,
  v.check_eft_identifier, v.carc_code, v.rarc_code, v.raw_value, v.normalized_value
FROM (VALUES
  -- id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value
  -- REC: billed 100, denial 100 CO-45 (recoverable).
  ('e4b00000-0000-4000-8000-000000000001'::uuid, 'billed_amount'::text, 100.00::numeric, 'billed'::text, 'SYN-REC-REC'::text, NULL::text, NULL::text, NULL::text, '100.00'::text, '100.00'::text),
  ('e4b00000-0000-4000-8000-000000000002', 'denial', 100.00, 'denied', 'SYN-REC-REC', NULL, 'CO-45', NULL, '100.00', '100.00'),
  -- NONREC: billed 100, denial 100 CO-97 (non-recoverable).
  ('e4b00000-0000-4000-8000-000000000003', 'billed_amount', 100.00, 'billed', 'SYN-REC-NONREC', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-000000000004', 'denial', 100.00, 'denied', 'SYN-REC-NONREC', NULL, 'CO-97', NULL, '100.00', '100.00'),
  -- MIXED: billed 100, denial 60 CO-45 (rec) + denial 40 CO-97 (non-rec) -> recoverable 60.
  ('e4b00000-0000-4000-8000-000000000005', 'billed_amount', 100.00, 'billed', 'SYN-REC-MIXED', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-000000000006', 'denial', 60.00, 'denied', 'SYN-REC-MIXED', NULL, 'CO-45', NULL, '60.00', '60.00'),
  ('e4b00000-0000-4000-8000-000000000007', 'denial', 40.00, 'denied', 'SYN-REC-MIXED', NULL, 'CO-97', NULL, '40.00', '40.00'),
  -- UNKNOWN: billed 100, denial 100 CO-99 (no knowledge row) -> not recoverable.
  ('e4b00000-0000-4000-8000-000000000008', 'billed_amount', 100.00, 'billed', 'SYN-REC-UNKNOWN', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-000000000009', 'denial', 100.00, 'denied', 'SYN-REC-UNKNOWN', NULL, 'CO-99', NULL, '100.00', '100.00'),
  -- NOCODE: billed 100, denial 100 with no CARC/RARC -> not recoverable (safest default).
  ('e4b00000-0000-4000-8000-00000000000a', 'billed_amount', 100.00, 'billed', 'SYN-REC-NOCODE', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-00000000000b', 'denial', 100.00, 'denied', 'SYN-REC-NOCODE', NULL, NULL, NULL, '100.00', '100.00'),
  -- EXACT: billed 100, denial 100 CO-50 + N130 -> exact match (recoverable) beats carc-only (false).
  ('e4b00000-0000-4000-8000-00000000000c', 'billed_amount', 100.00, 'billed', 'SYN-REC-EXACT', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-00000000000d', 'denial', 100.00, 'denied', 'SYN-REC-EXACT', NULL, 'CO-50', 'N130', '100.00', '100.00'),
  -- PAYER: billed 100, denial 100 CO-16 -> payer-specific (recoverable) beats global (false).
  ('e4b00000-0000-4000-8000-00000000000e', 'billed_amount', 100.00, 'billed', 'SYN-REC-PAYER', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-00000000000f', 'denial', 100.00, 'denied', 'SYN-REC-PAYER', NULL, 'CO-16', NULL, '100.00', '100.00'),
  -- CONFLICT: billed 100, denial 100 CO-11 -> tie disagreement -> fail closed (not recoverable).
  ('e4b00000-0000-4000-8000-000000000010', 'billed_amount', 100.00, 'billed', 'SYN-REC-CONFLICT', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-000000000011', 'denial', 100.00, 'denied', 'SYN-REC-CONFLICT', NULL, 'CO-11', NULL, '100.00', '100.00'),
  -- E1: payment only (trusted via check), no billed -> incomplete.
  ('e4b00000-0000-4000-8000-000000000012', 'payment', 25.00, 'paid', 'SYN-REC-E1', 'SYN-REC-CHK-E1', NULL, NULL, '25.00', '25.00'),
  -- E2: billed 100, allowed 80, paid 20, patient_resp 60 (no denial) -> balanced.
  ('e4b00000-0000-4000-8000-000000000013', 'billed_amount', 100.00, 'billed', 'SYN-REC-E2', NULL, NULL, NULL, '100.00', '100.00'),
  ('e4b00000-0000-4000-8000-000000000014', 'allowed_amount', 80.00, 'allowed', 'SYN-REC-E2', NULL, NULL, NULL, '80.00', '80.00'),
  ('e4b00000-0000-4000-8000-000000000015', 'payment', 20.00, 'paid', 'SYN-REC-E2', 'SYN-REC-CHK-E2', NULL, NULL, '20.00', '20.00'),
  ('e4b00000-0000-4000-8000-000000000016', 'patient_responsibility', 60.00, 'patient_responsibility', 'SYN-REC-E2', NULL, NULL, NULL, '60.00', '60.00')
) AS v(id, observation_type, amount, amount_type, claim_identifier, check_eft_identifier, carc_code, rarc_code, raw_value, normalized_value);

SELECT fte_reconcile_practice('e4000000-0000-4000-8000-0000000000fe');


-- CHECK 1 — REC: carc-only recoverable match populates recoverable_amount.
DO $$
DECLARE v_rec numeric; v_den numeric;
BEGIN
  SELECT recoverable_amount, denied_amount INTO v_rec, v_den FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000a';
  ASSERT v_rec=100.00, format('CHECK 1 FAIL: REC recoverable_amount expected 100.00, got %s', v_rec);
  ASSERT v_den=100.00, format('CHECK 1 FAIL: REC denied_amount expected 100.00, got %s', v_den);
  RAISE NOTICE 'PASS [1/13] recoverable denial populates recoverable_amount';
END $$;

-- CHECK 2 — NONREC: matched non-recoverable -> recoverable_amount 0.
DO $$
DECLARE v_rec numeric;
BEGIN
  SELECT recoverable_amount INTO v_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000b';
  ASSERT v_rec=0, format('CHECK 2 FAIL: NONREC recoverable_amount expected 0, got %s', v_rec);
  RAISE NOTICE 'PASS [2/13] non-recoverable denial -> recoverable_amount 0';
END $$;

-- CHECK 3 — MIXED: only the recoverable subset aggregates.
DO $$
DECLARE v_rec numeric; v_den numeric;
BEGIN
  SELECT recoverable_amount, denied_amount INTO v_rec, v_den FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000c';
  ASSERT v_rec=60.00, format('CHECK 3 FAIL: MIXED recoverable_amount expected 60.00, got %s', v_rec);
  ASSERT v_den=100.00, format('CHECK 3 FAIL: MIXED denied_amount expected 100.00, got %s', v_den);
  RAISE NOTICE 'PASS [3/13] mixed denials aggregate only the recoverable subset';
END $$;

-- CHECK 4 — UNKNOWN: no knowledge match -> recoverable 0; accounting unchanged, no short_pay.
DO $$
DECLARE v_rec numeric; v_status text; v_sp bigint;
BEGIN
  SELECT recoverable_amount, reconciliation_status INTO v_rec, v_status FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000d';
  SELECT COUNT(*) INTO v_sp FROM fte_claim_events WHERE claim_id='e4c00000-0000-4000-8000-00000000000d' AND event_type='short_pay_detected';
  ASSERT v_rec=0, format('CHECK 4 FAIL: UNKNOWN recoverable_amount expected 0, got %s', v_rec);
  ASSERT v_status='balanced', format('CHECK 4 FAIL: UNKNOWN status expected balanced (overlay unchanged), got %L', v_status);
  ASSERT v_sp=0, format('CHECK 4 FAIL: UNKNOWN short_pay expected 0, got %s', v_sp);
  RAISE NOTICE 'PASS [4/13] unknown CARC -> recoverable 0; accounting/short_pay unaffected';
END $$;

-- CHECK 5 — NOCODE: denial with no CARC/RARC -> not recoverable (safest default).
DO $$
DECLARE v_rec numeric;
BEGIN
  SELECT recoverable_amount INTO v_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000e';
  ASSERT v_rec=0, format('CHECK 5 FAIL: NOCODE recoverable_amount expected 0, got %s', v_rec);
  RAISE NOTICE 'PASS [5/13] denial with no CARC/RARC -> not recoverable (safest default)';
END $$;

-- CHECK 6 — EXACT: CARC+RARC exact match outranks CARC-only (differing recoverable).
DO $$
DECLARE v_rec numeric;
BEGIN
  SELECT recoverable_amount INTO v_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000f';
  ASSERT v_rec=100.00, format('CHECK 6 FAIL: EXACT recoverable_amount expected 100.00 (exact wins), got %s', v_rec);
  RAISE NOTICE 'PASS [6/13] exact CARC+RARC match outranks CARC-only';
END $$;

-- CHECK 7 — PAYER: payer-specific knowledge outranks global (differing recoverable).
DO $$
DECLARE v_rec numeric;
BEGIN
  SELECT recoverable_amount INTO v_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-000000000010';
  ASSERT v_rec=100.00, format('CHECK 7 FAIL: PAYER recoverable_amount expected 100.00 (payer-specific wins), got %s', v_rec);
  RAISE NOTICE 'PASS [7/13] payer-specific knowledge outranks global';
END $$;

-- CHECK 8 — CONFLICT: equally-specific disagreeing rows -> fail closed; not routed to review.
DO $$
DECLARE v_rec numeric; v_status text;
BEGIN
  SELECT recoverable_amount, reconciliation_status INTO v_rec, v_status FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-000000000011';
  ASSERT v_rec=0, format('CHECK 8 FAIL: CONFLICT recoverable_amount expected 0 (fail closed), got %s', v_rec);
  ASSERT v_status='balanced', format('CHECK 8 FAIL: CONFLICT status expected balanced (recoverability conflict does not route review), got %L', v_status);
  RAISE NOTICE 'PASS [8/13] top-tier knowledge conflict fails closed; no review routing';
END $$;

-- CHECK 9 — E1 preserved: incomplete claim -> recoverable_amount NULL.
DO $$
DECLARE v_status text; v_rec numeric;
BEGIN
  SELECT reconciliation_status, recoverable_amount INTO v_status, v_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-000000000012';
  ASSERT v_status='incomplete', format('CHECK 9 FAIL: E1 status expected incomplete, got %L', v_status);
  ASSERT v_rec IS NULL, format('CHECK 9 FAIL: E1 recoverable_amount expected NULL, got %s', v_rec);
  RAISE NOTICE 'PASS [9/13] E1 incomplete preserved; recoverable_amount NULL';
END $$;

-- CHECK 10 — E2 preserved: balanced no-denial claim -> recoverable_amount + denied_amount NULL.
DO $$
DECLARE v_status text; v_rec numeric; v_den numeric;
BEGIN
  SELECT reconciliation_status, recoverable_amount, denied_amount INTO v_status, v_rec, v_den FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-000000000013';
  ASSERT v_status='balanced', format('CHECK 10 FAIL: E2 status expected balanced, got %L', v_status);
  ASSERT v_rec IS NULL, format('CHECK 10 FAIL: E2 recoverable_amount expected NULL, got %s', v_rec);
  ASSERT v_den IS NULL, format('CHECK 10 FAIL: E2 denied_amount expected NULL, got %s', v_den);
  RAISE NOTICE 'PASS [10/13] E2 balanced preserved; recoverable/denied NULL (no denial)';
END $$;

-- CHECK 11 — 014B preserved: REC denied_amount + open_balance unaffected by overlay.
DO $$
DECLARE v_den numeric; v_ob numeric;
BEGIN
  SELECT denied_amount, open_balance_amount INTO v_den, v_ob FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000a';
  ASSERT v_den=100.00, format('CHECK 11 FAIL: REC denied_amount expected 100.00, got %s', v_den);
  ASSERT v_ob=0, format('CHECK 11 FAIL: REC open_balance expected 0 (denial explained), got %s', v_ob);
  RAISE NOTICE 'PASS [11/13] 014B denied_amount + open_balance unaffected by recoverable overlay';
END $$;

-- CHECK 12 — Invariant: recoverable_amount <= denied_amount wherever denied_amount is set.
DO $$
DECLARE v_bad bigint;
BEGIN
  SELECT COUNT(*) INTO v_bad FROM fte_financial_positions
  WHERE practice_id='e4000000-0000-4000-8000-0000000000fe'
    AND denied_amount IS NOT NULL
    AND recoverable_amount IS NOT NULL
    AND recoverable_amount > denied_amount;
  ASSERT v_bad=0, format('CHECK 12 FAIL: %s positions have recoverable_amount > denied_amount', v_bad);
  RAISE NOTICE 'PASS [12/13] invariant recoverable_amount <= denied_amount holds';
END $$;

-- CHECK 13 — Idempotency: rerun keeps recoverable_amount stable.
DO $$
DECLARE v_rec_rec numeric; v_rec_mixed numeric;
BEGIN
  PERFORM fte_reconcile_practice('e4000000-0000-4000-8000-0000000000fe');
  SELECT recoverable_amount INTO v_rec_rec FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000a';
  SELECT recoverable_amount INTO v_rec_mixed FROM fte_financial_positions WHERE claim_id='e4c00000-0000-4000-8000-00000000000c';
  ASSERT v_rec_rec=100.00, format('CHECK 13 FAIL: after rerun REC recoverable_amount expected 100.00, got %s', v_rec_rec);
  ASSERT v_rec_mixed=60.00, format('CHECK 13 FAIL: after rerun MIXED recoverable_amount expected 60.00, got %s', v_rec_mixed);
  RAISE NOTICE 'PASS [13/13] recoverable_amount stable on rerun (idempotent overlay)';
END $$;

rollback;

-- =============================================================================
-- All 13 checks passed if you see PASS [1/13]..[13/13] above and no EXCEPTION.
-- ROLLBACK ensures no synthetic fixture, denial-knowledge row, or derived row persists.
-- =============================================================================

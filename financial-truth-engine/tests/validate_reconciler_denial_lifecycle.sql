-- =============================================================================
-- Financial Truth Engine — Denial Lifecycle Reconciler Validation
-- tests/validate_reconciler_denial_lifecycle.sql
--
-- Covers Task 017C (denial lifecycle reconciler slice): the reconciler consumes
-- actionable lifecycle resolutions (file_appeal / record_recovery /
-- approve_write_off) from _fte_active_resolutions and emits appeal_filed /
-- recovery_received / write_off_approved events, folded into the derived
-- position by Phase 6:
--   * appeal_filed is reporting-only (no monetary effect)
--   * recovery_received / write_off_approved reclassify denied money and share
--     ONE per-claim gross denied pool (cap: recovered + written_off <= gross)
--   * denied_amount becomes NET; recovered_amount / written_off_amount are the
--     cumulative buckets; open_balance stays on GROSS denied (unchanged)
--   * over-consumption / missing target route to the existing anomaly path
--     (fte_review_queue, reason 'conflicting_observations') with NO monetary
--     mutation and NO clamping
--   * rerun is idempotent
--
-- Self-contained: creates its own synthetic fixture INSIDE the transaction,
-- calls fte_reconcile_practice, asserts, then ROLLs BACK — nothing persists.
-- All identifiers/amounts are synthetic. No PHI, no production data.
--
-- Depends on migrations 001..012 and reconciler/fte_reconcile.sql.
-- Emits RAISE NOTICE 'PASS [n/14] ...'; any failure RAISEs EXCEPTION and rolls back.
--
-- Explicit distinct resolved_at values are used so the cumulative-cap ordering
-- (claim_id, resolved_at, id) is deterministic within this single transaction
-- (now() is constant per transaction).
-- =============================================================================

begin;

-- ---- Synthetic fixture -------------------------------------------------------
INSERT INTO fte_practices (id, name, external_ref) VALUES
  ('e6000000-0000-4000-8000-0000000000fe', 'Denial Lifecycle Test Practice', 'SYN-LIFE-PRACTICE');

INSERT INTO fte_evidence (id, practice_id, evidence_type, fixture_id, raw_text) VALUES
  ('e6e00000-0000-4000-8000-00000000000a', 'e6000000-0000-4000-8000-0000000000fe',
   'page', 'SYN-LIFE', '[SYNTHETIC] denial lifecycle page');

INSERT INTO fte_claims (id, practice_id, internal_claim_id, claim_number, payer_name, status) VALUES
  ('e6c00000-0000-4000-8000-000000000001', 'e6000000-0000-4000-8000-0000000000fe',
   'SYN-LIFE-1', 'SYN-LIFE-1', 'Synthetic Payer', 'open');

-- Column-explicit INSERT ... SELECT (positional-drift-safe): CLM SYN-LIFE-1 has
-- billed 100 and a single denial 100 -> gross denied 100, open_balance 0, balanced.
INSERT INTO fte_observations
  (practice_id, evidence_id, payer_name, confidence_score, page_number,
   is_summary_row, is_superseded, metadata,
   id, observation_type, amount, amount_type, claim_identifier)
SELECT
  'e6000000-0000-4000-8000-0000000000fe'::uuid,
  'e6e00000-0000-4000-8000-00000000000a'::uuid,
  'Synthetic Payer', 0.9000, 1,
  false, false, '{}'::jsonb,
  v.id, v.observation_type, v.amount, v.amount_type, v.claim_identifier
FROM (VALUES
  ('e6b00000-0000-4000-8000-000000000001'::uuid, 'billed_amount'::text, 100.00::numeric, 'billed'::text, 'SYN-LIFE-1'::text),
  ('e6b00000-0000-4000-8000-000000000002', 'denial', 100.00, 'denied', 'SYN-LIFE-1')
) AS v(id, observation_type, amount, amount_type, claim_identifier);


-- =============================================================================
-- STEP 1: Baseline reconcile — no lifecycle resolutions.
--   Expected CLM SYN-LIFE-1: denied_amount=100, recovered/written_off NULL,
--   open_balance=0, status balanced.
-- =============================================================================
SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_denied numeric; v_rec numeric; v_wo numeric; v_ob numeric; v_status text;
BEGIN
  SELECT denied_amount, recovered_amount, written_off_amount, open_balance_amount, reconciliation_status
    INTO v_denied, v_rec, v_wo, v_ob, v_status
  FROM fte_financial_positions fp JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = 'e6000000-0000-4000-8000-0000000000fe' AND c.claim_number = 'SYN-LIFE-1';

  ASSERT v_denied = 100.00, format('baseline denied expected 100, got %s', v_denied);
  ASSERT v_rec IS NULL,     format('baseline recovered expected NULL, got %s', v_rec);
  ASSERT v_wo IS NULL,      format('baseline written_off expected NULL, got %s', v_wo);
  ASSERT v_ob = 0.00,       format('baseline open_balance expected 0, got %s', v_ob);
  ASSERT v_status = 'balanced', format('baseline status expected balanced, got %s', v_status);
END $$;


-- =============================================================================
-- STEP 2: file_appeal — reporting-only. (validations 1, 2)
-- =============================================================================
INSERT INTO fte_review_resolutions (practice_id, claim_id, action, target_type, resolved_by, resolved_at)
  VALUES ('e6000000-0000-4000-8000-0000000000fe', 'e6c00000-0000-4000-8000-000000000001',
          'file_appeal', 'claim', 'test_runner', '2026-07-05 10:00:00+00');

SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_appeal int; v_denied numeric; v_rec numeric; v_wo numeric; v_ob numeric;
BEGIN
  SELECT count(*) INTO v_appeal
  FROM fte_claim_events ce JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = 'e6000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'SYN-LIFE-1' AND ce.event_type = 'appeal_filed';
  ASSERT v_appeal = 1, format('FAIL [1/14] appeal_filed expected 1, got %s', v_appeal);
  RAISE NOTICE 'PASS [1/14] file_appeal emits exactly one appeal_filed event';

  SELECT denied_amount, recovered_amount, written_off_amount, open_balance_amount
    INTO v_denied, v_rec, v_wo, v_ob
  FROM fte_financial_positions fp JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = 'e6000000-0000-4000-8000-0000000000fe' AND c.claim_number = 'SYN-LIFE-1';
  ASSERT v_denied = 100.00 AND v_rec IS NULL AND v_wo IS NULL AND v_ob = 0.00,
    format('FAIL [2/14] appeal must not mutate money; denied=%s rec=%s wo=%s ob=%s', v_denied, v_rec, v_wo, v_ob);
  RAISE NOTICE 'PASS [2/14] file_appeal leaves all monetary position fields unchanged';
END $$;


-- =============================================================================
-- STEP 3: record_recovery 60 — reclassify. (validations 3, 4, 5, 6)
-- =============================================================================
INSERT INTO fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by, resolved_at)
  VALUES ('e6000000-0000-4000-8000-0000000000fe', 'e6c00000-0000-4000-8000-000000000001',
          'record_recovery', 'claim', 60.00, 'test_runner', '2026-07-05 10:01:00+00');

SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_evt int; v_denied numeric; v_rec numeric; v_ob numeric; v_status text;
BEGIN
  SELECT count(*) INTO v_evt
  FROM fte_claim_events ce JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = 'e6000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'SYN-LIFE-1' AND ce.event_type = 'recovery_received';
  ASSERT v_evt = 1, format('FAIL [3/14] recovery_received expected 1, got %s', v_evt);
  RAISE NOTICE 'PASS [3/14] record_recovery emits exactly one recovery_received event';

  SELECT denied_amount, recovered_amount, open_balance_amount, reconciliation_status
    INTO v_denied, v_rec, v_ob, v_status
  FROM fte_financial_positions fp JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = 'e6000000-0000-4000-8000-0000000000fe' AND c.claim_number = 'SYN-LIFE-1';

  ASSERT v_rec = 60.00,    format('FAIL [4/14] recovered_amount expected 60, got %s', v_rec);
  RAISE NOTICE 'PASS [4/14] valid recovery increments recovered_amount to 60';
  ASSERT v_denied = 40.00, format('FAIL [5/14] net denied expected 40 (100-60), got %s', v_denied);
  RAISE NOTICE 'PASS [5/14] valid recovery reduces denied_amount by the recovered amount (100->40)';
  ASSERT v_ob = 0.00,      format('FAIL [6/14] open_balance expected 0 (unchanged), got %s', v_ob);
  ASSERT v_status = 'balanced', format('FAIL [6/14] status expected balanced, got %s', v_status);
  RAISE NOTICE 'PASS [6/14] valid recovery leaves open_balance unchanged (0)';
END $$;


-- =============================================================================
-- STEP 4: approve_write_off 40 — exhausts pool exactly (60+40=100). (7, 8, 9, 10)
-- =============================================================================
INSERT INTO fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by, resolved_at)
  VALUES ('e6000000-0000-4000-8000-0000000000fe', 'e6c00000-0000-4000-8000-000000000001',
          'approve_write_off', 'claim', 40.00, 'test_runner', '2026-07-05 10:02:00+00');

SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_evt int; v_denied numeric; v_rec numeric; v_wo numeric; v_ob numeric;
BEGIN
  SELECT count(*) INTO v_evt
  FROM fte_claim_events ce JOIN fte_claims c ON c.id = ce.claim_id
  WHERE ce.practice_id = 'e6000000-0000-4000-8000-0000000000fe'
    AND c.claim_number = 'SYN-LIFE-1' AND ce.event_type = 'write_off_approved';
  ASSERT v_evt = 1, format('FAIL [7/14] write_off_approved expected 1, got %s', v_evt);
  RAISE NOTICE 'PASS [7/14] approve_write_off emits exactly one write_off_approved event';

  SELECT denied_amount, recovered_amount, written_off_amount, open_balance_amount
    INTO v_denied, v_rec, v_wo, v_ob
  FROM fte_financial_positions fp JOIN fte_claims c ON c.id = fp.claim_id
  WHERE fp.practice_id = 'e6000000-0000-4000-8000-0000000000fe' AND c.claim_number = 'SYN-LIFE-1';

  ASSERT v_wo = 40.00,     format('FAIL [8/14] written_off_amount expected 40, got %s', v_wo);
  RAISE NOTICE 'PASS [8/14] valid write-off increments written_off_amount to 40';
  ASSERT v_denied = 0.00,  format('FAIL [9/14] net denied expected 0 (100-60-40), got %s', v_denied);
  RAISE NOTICE 'PASS [9/14] valid write-off reduces denied_amount by the written-off amount (40->0)';
  ASSERT v_ob = 0.00,      format('FAIL [10/14] open_balance expected 0 (unchanged), got %s', v_ob);
  ASSERT v_rec = 60.00,    format('FAIL [10/14] recovered_amount expected still 60, got %s', v_rec);
  RAISE NOTICE 'PASS [10/14] valid write-off leaves open_balance unchanged (0); recovery bucket intact';
END $$;


-- =============================================================================
-- STEP 5: overflow — pool already fully consumed (100). Add recovery 10 and
-- write-off 5; both must route to anomaly with NO monetary mutation. (11, 12)
-- =============================================================================
INSERT INTO fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by, resolved_at)
  VALUES
    ('e6000000-0000-4000-8000-0000000000fe', 'e6c00000-0000-4000-8000-000000000001',
     'record_recovery', 'claim', 10.00, 'test_runner', '2026-07-05 10:03:00+00'),
    ('e6000000-0000-4000-8000-0000000000fe', 'e6c00000-0000-4000-8000-000000000001',
     'approve_write_off', 'claim', 5.00, 'test_runner', '2026-07-05 10:04:00+00');

SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_rec_evt int; v_wo_evt int; v_rec numeric; v_wo numeric; v_denied numeric;
  v_anom_rec int; v_anom_wo int;
BEGIN
  -- No new monetary events beyond the valid recovery(60) + write_off(40).
  SELECT count(*) INTO v_rec_evt FROM fte_claim_events ce JOIN fte_claims c ON c.id = ce.claim_id
   WHERE ce.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1' AND ce.event_type='recovery_received';
  SELECT count(*) INTO v_wo_evt FROM fte_claim_events ce JOIN fte_claims c ON c.id = ce.claim_id
   WHERE ce.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1' AND ce.event_type='write_off_approved';
  SELECT recovered_amount, written_off_amount, denied_amount INTO v_rec, v_wo, v_denied
   FROM fte_financial_positions fp JOIN fte_claims c ON c.id = fp.claim_id
   WHERE fp.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1';

  -- Anomaly entries (existing review_queue mechanism), one per over-consuming action.
  SELECT count(*) INTO v_anom_rec FROM fte_review_queue
   WHERE practice_id='e6000000-0000-4000-8000-0000000000fe' AND reason='conflicting_observations'
     AND details->>'anomaly'='lifecycle_amount_exceeds_remaining_denied' AND details->>'action'='record_recovery';
  SELECT count(*) INTO v_anom_wo FROM fte_review_queue
   WHERE practice_id='e6000000-0000-4000-8000-0000000000fe' AND reason='conflicting_observations'
     AND details->>'anomaly'='lifecycle_amount_exceeds_remaining_denied' AND details->>'action'='approve_write_off';

  ASSERT v_rec_evt = 1 AND v_rec = 60.00 AND v_denied = 0.00 AND v_anom_rec = 1,
    format('FAIL [11/14] overflow recovery: rec_evt=%s rec=%s denied=%s anom=%s', v_rec_evt, v_rec, v_denied, v_anom_rec);
  RAISE NOTICE 'PASS [11/14] overflow recovery routes to anomaly with no monetary mutation';

  ASSERT v_wo_evt = 1 AND v_wo = 40.00 AND v_anom_wo = 1,
    format('FAIL [12/14] overflow write-off: wo_evt=%s wo=%s anom=%s', v_wo_evt, v_wo, v_anom_wo);
  RAISE NOTICE 'PASS [12/14] overflow write-off routes to anomaly with no monetary mutation';
END $$;


-- =============================================================================
-- STEP 6: missing target — record_recovery with NULL claim_id (observation
-- target satisfies target-present) must route to anomaly, no event, no mutation.
-- (validation 13)
-- =============================================================================
INSERT INTO fte_review_resolutions (practice_id, claim_id, observation_id, action, target_type, lifecycle_amount, resolved_by, resolved_at)
  VALUES ('e6000000-0000-4000-8000-0000000000fe', NULL, 'e6b00000-0000-4000-8000-000000000002',
          'record_recovery', 'claim', 20.00, 'test_runner', '2026-07-05 10:05:00+00');

SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_anom int; v_rec numeric; v_rec_evt int;
BEGIN
  SELECT count(*) INTO v_anom FROM fte_review_queue
   WHERE practice_id='e6000000-0000-4000-8000-0000000000fe' AND reason='conflicting_observations'
     AND details->>'anomaly'='lifecycle_target_unresolved';
  -- claim SYN-LIFE-1 remains unaffected (recovery still exactly 60, one event).
  SELECT recovered_amount INTO v_rec FROM fte_financial_positions fp JOIN fte_claims c ON c.id=fp.claim_id
   WHERE fp.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1';
  SELECT count(*) INTO v_rec_evt FROM fte_claim_events ce JOIN fte_claims c ON c.id=ce.claim_id
   WHERE ce.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1' AND ce.event_type='recovery_received';

  ASSERT v_anom = 1 AND v_rec = 60.00 AND v_rec_evt = 1,
    format('FAIL [13/14] missing-target: anom=%s rec=%s rec_evt=%s', v_anom, v_rec, v_rec_evt);
  RAISE NOTICE 'PASS [13/14] missing/unresolvable target routes to anomaly with no monetary mutation';
END $$;


-- =============================================================================
-- STEP 7: idempotency — a second reconcile with the same active resolutions
-- yields identical events and position amounts. (validation 14)
-- =============================================================================
SELECT fte_reconcile_practice('e6000000-0000-4000-8000-0000000000fe');

DO $$
DECLARE
  v_appeal int; v_rec_evt int; v_wo_evt int; v_denied numeric; v_rec numeric; v_wo numeric; v_ob numeric;
BEGIN
  SELECT
    count(*) FILTER (WHERE ce.event_type='appeal_filed'),
    count(*) FILTER (WHERE ce.event_type='recovery_received'),
    count(*) FILTER (WHERE ce.event_type='write_off_approved')
    INTO v_appeal, v_rec_evt, v_wo_evt
  FROM fte_claim_events ce JOIN fte_claims c ON c.id=ce.claim_id
  WHERE ce.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1';

  SELECT denied_amount, recovered_amount, written_off_amount, open_balance_amount
    INTO v_denied, v_rec, v_wo, v_ob
  FROM fte_financial_positions fp JOIN fte_claims c ON c.id=fp.claim_id
  WHERE fp.practice_id='e6000000-0000-4000-8000-0000000000fe' AND c.claim_number='SYN-LIFE-1';

  ASSERT v_appeal = 1 AND v_rec_evt = 1 AND v_wo_evt = 1,
    format('FAIL [14/14] idempotency events: appeal=%s rec=%s wo=%s', v_appeal, v_rec_evt, v_wo_evt);
  ASSERT v_denied = 0.00 AND v_rec = 60.00 AND v_wo = 40.00 AND v_ob = 0.00,
    format('FAIL [14/14] idempotency amounts: denied=%s rec=%s wo=%s ob=%s', v_denied, v_rec, v_wo, v_ob);
  RAISE NOTICE 'PASS [14/14] rerun is idempotent (no duplicate events, no duplicate amount mutation)';
END $$;


-- Discard all fixture + derived data. Nothing persists.
rollback;

-- =============================================================================
-- Financial Truth Engine — Synthetic Phase 3A Extraction Fixture
-- fixtures/synthetic_phase3a_extraction_fixture.sql
--
-- Practice ID : a3000000-0000-4000-8000-0000000000fe  (Phase 3A extraction test)
-- Covers      : 3-claim balanced/unbalanced extraction baseline, summary-row
--               exclusion, check_payment stub for two-link event evidence
--
-- Scenario:
--   A 4-page remittance from Synthetic Payer A, check SYN-4001 ($354.00).
--   Page 1: CLM-P3A-0001 — billed $250, adj $100, paid $150 → balanced (0.00)  CPT 99213
--   Page 2: CLM-P3A-0002 — billed $180, adj $76,  paid $104 → balanced (0.00)  CPT 93000
--   Page 3: CLM-P3A-0003 — billed $350, adj $70, paid $100 → unbalanced ($180.00) CPT 99213
--   Page 4: summary row  — total paid $354.00, is_summary_row=true → excluded
--   + check_payment stub for SYN-4001 (enables two-link fte_event_evidence check)
--
-- CPT note: CLM-P3A-0001 and CLM-P3A-0003 use CPT 99213 (office visit).
--           CLM-P3A-0002 uses CPT 93000 (EKG) — intentionally distinct to
--           verify the reconciler does not conflate claims by CPT code.
--
-- Evidence rows  : 6  (1 document + 4 page + 1 check_payment stub)
-- Claim rows     : 3
-- Observation rows: 10  (9 claim-level + 1 summary)
--
-- Safety:
--   All raw_text values are prefixed [SYNTHETIC].
--   No PHI, no real patient data, no real member IDs, no production exports.
--   source_uri uses private://fte/... scheme.
--   content_hash uses sha256:SYNTHETIC_... pattern.
--   No legacy EOB tables or connection strings.
--
-- Idempotent: INSERT ... ON CONFLICT DO NOTHING throughout.
-- Cleanup block at top allows safe reload.
-- =============================================================================

begin;

-- ---------------------------------------------------------------------------
-- Idempotent cleanup (safe to run before reload)
-- Order: event_evidence → review_queue → analysis_runs → financial_positions
--        → claim_events → claims → observations
--        → evidence (children) → evidence (parents) → practices
-- ---------------------------------------------------------------------------
delete from fte_event_evidence   where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_review_queue     where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_analysis_runs    where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_financial_positions where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_claim_events     where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_claims           where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_observations     where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_evidence         where practice_id = 'a3000000-0000-4000-8000-0000000000fe'
                                   and parent_evidence_id is not null;
delete from fte_evidence         where practice_id = 'a3000000-0000-4000-8000-0000000000fe';
delete from fte_practices        where id = 'a3000000-0000-4000-8000-0000000000fe';

-- ---------------------------------------------------------------------------
-- Practice
-- ---------------------------------------------------------------------------
insert into fte_practices (id, name, external_ref) values
  ('a3000000-0000-4000-8000-0000000000fe',
   'Synthetic Practice Phase3A', 'phase3a_extraction_fixture')
on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Evidence — 6 rows
-- Row 1: parent document (raw_text null — not scanned text, just a handle)
-- Rows 2–5: page children (raw_text prefixed [SYNTHETIC])
-- Row 6: check_payment stub (raw_text null — structured record, not OCR text)
-- ---------------------------------------------------------------------------
insert into fte_evidence
  (id, practice_id, parent_evidence_id, evidence_type, fixture_id, source_uri,
   content_hash, page_number, raw_text, metadata)
values
  -- parent document
  ('e3a00000-0000-4000-8000-000000000001',
   'a3000000-0000-4000-8000-0000000000fe',
   null, 'document', 'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/remittance-syn-4001.pdf',
   'sha256:SYNTHETIC_PHASE3A_DOC_001',
   null, null, '{}'),

  -- page 1 — CLM-P3A-0001 (CPT 99213)
  ('e3a00000-0000-4000-8000-000000000002',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/remittance-syn-4001.pdf#page=1',
   'sha256:SYNTHETIC_PHASE3A_PAGE1',
   1,
   '[SYNTHETIC] Remittance Advice Page 1 of 4 | Payer: Synthetic Payer A | Claim: CLM-P3A-0001 | Patient: SYNTHETIC PATIENT A | DOS: 2026-05-15 | CPT 99213 | Billed: $250.00 | Allowed: $150.00 | Adj: $100.00 | Paid: $150.00',
   '{}'),

  -- page 2 — CLM-P3A-0002 (CPT 93000 — intentionally distinct from CLM-P3A-0001/0003)
  ('e3a00000-0000-4000-8000-000000000003',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/remittance-syn-4001.pdf#page=2',
   'sha256:SYNTHETIC_PHASE3A_PAGE2',
   2,
   '[SYNTHETIC] Remittance Advice Page 2 of 4 | Payer: Synthetic Payer A | Claim: CLM-P3A-0002 | Patient: SYNTHETIC PATIENT B | DOS: 2026-05-15 | CPT 93000 | Billed: $180.00 | Allowed: $104.00 | Adj: $76.00 | Paid: $104.00',
   '{}'),

  -- page 3 — CLM-P3A-0003 (CPT 99213, underpaid — synthetic short pay, no CARC codes)
  ('e3a00000-0000-4000-8000-000000000004',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/remittance-syn-4001.pdf#page=3',
   'sha256:SYNTHETIC_PHASE3A_PAGE3',
   3,
   '[SYNTHETIC] Remittance Advice Page 3 of 4 | Payer: Synthetic Payer A | Claim: CLM-P3A-0003 | Patient: SYNTHETIC PATIENT C | DOS: 2026-05-15 | CPT 99213 | Billed: $350.00 | Allowed: $280.00 | Adj: $70.00 | Paid: $100.00',
   '{}'),

  -- page 4 — summary (do not use for individual claim adjudication)
  ('e3a00000-0000-4000-8000-000000000005',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/remittance-syn-4001.pdf#page=4',
   'sha256:SYNTHETIC_PHASE3A_PAGE4',
   4,
   '[SYNTHETIC] Remittance Summary Page 4 of 4 | Payer: Synthetic Payer A | Check No: SYN-4001 | Check Date: 2026-05-16 | Total Payment This Remittance: $354.00 | Do not use for individual claim adjudication',
   '{}'),

  -- check_payment stub — enables two-link fte_event_evidence for payment_applied events
  ('e3a00000-0000-4000-8000-000000000006',
   'a3000000-0000-4000-8000-0000000000fe',
   null, 'check_payment', 'synthetic_phase3a_extraction_fixture',
   'private://fte/phase3a/check-stub-SYN-4001',
   'sha256:SYNTHETIC_PHASE3A_CHECK4001',
   null, null,
   '{"check_number":"SYN-4001","check_amount":"354.00","payer_name":"Synthetic Payer A","payment_date":"2026-05-16"}')

on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Claims — 3 rows
-- claim_number must match observation.claim_identifier for Phase 3/4/5c JOINs
-- ---------------------------------------------------------------------------
insert into fte_claims
  (id, practice_id, internal_claim_id, claim_number, payer_claim_number,
   patient_identifier_hash, service_date_start, service_date_end, payer_name, status)
values
  ('c3a00000-0000-4000-8000-000000000001',
   'a3000000-0000-4000-8000-0000000000fe',
   'FTE-P3A-0001', 'CLM-P3A-0001', 'SYN-PCN-0001',
   'synthhash:patient-p3a-a', '2026-05-15', '2026-05-15',
   'Synthetic Payer A', 'open'),

  ('c3a00000-0000-4000-8000-000000000002',
   'a3000000-0000-4000-8000-0000000000fe',
   'FTE-P3A-0002', 'CLM-P3A-0002', 'SYN-PCN-0002',
   'synthhash:patient-p3a-b', '2026-05-15', '2026-05-15',
   'Synthetic Payer A', 'open'),

  ('c3a00000-0000-4000-8000-000000000003',
   'a3000000-0000-4000-8000-0000000000fe',
   'FTE-P3A-0003', 'CLM-P3A-0003', 'SYN-PCN-0003',
   'synthhash:patient-p3a-c', '2026-05-15', '2026-05-15',
   'Synthetic Payer A', 'open')

on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Observations — 10 rows
-- 9 claim-level (3 per claim: billed_amount, contractual_adjustment, payment)
-- 1 summary row (is_summary_row=true, no claim_identifier)
--
-- check_eft_identifier = 'SYN-4001' on all payment observations triggers
-- Phase 5c Link 2 lookup against the check_payment stub above.
-- ---------------------------------------------------------------------------
insert into fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type,
   claim_identifier, payer_name, service_date, cpt_code, check_eft_identifier,
   confidence_score, raw_value, normalized_value, page_number,
   is_summary_row, is_superseded, metadata)
values

  -- CLM-P3A-0001 (page 1, CPT 99213) — billed
  ('0b3a0000-0000-4000-8000-000000000001',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000002',
   'billed_amount', 250.00, 'billed',
   'CLM-P3A-0001', 'Synthetic Payer A', '2026-05-15', '99213', null,
   0.97, 'SYN:250.00', '250.00', 1, false, false, '{}'),

  -- CLM-P3A-0001 (page 1, CPT 99213) — contractual adjustment
  ('0b3a0000-0000-4000-8000-000000000002',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000002',
   'contractual_adjustment', 100.00, 'contractual_adjustment',
   'CLM-P3A-0001', 'Synthetic Payer A', '2026-05-15', '99213', null,
   0.97, 'SYN:100.00', '100.00', 1, false, false, '{}'),

  -- CLM-P3A-0001 (page 1, CPT 99213) — payment (check_eft_identifier triggers check stub link)
  ('0b3a0000-0000-4000-8000-000000000003',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000002',
   'payment', 150.00, 'paid',
   'CLM-P3A-0001', 'Synthetic Payer A', '2026-05-15', '99213', 'SYN-4001',
   0.97, 'SYN:150.00', '150.00', 1, false, false, '{}'),

  -- CLM-P3A-0002 (page 2, CPT 93000) — billed
  ('0b3a0000-0000-4000-8000-000000000004',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000003',
   'billed_amount', 180.00, 'billed',
   'CLM-P3A-0002', 'Synthetic Payer A', '2026-05-15', '93000', null,
   0.97, 'SYN:180.00', '180.00', 2, false, false, '{}'),

  -- CLM-P3A-0002 (page 2, CPT 93000) — contractual adjustment
  ('0b3a0000-0000-4000-8000-000000000005',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000003',
   'contractual_adjustment', 76.00, 'contractual_adjustment',
   'CLM-P3A-0002', 'Synthetic Payer A', '2026-05-15', '93000', null,
   0.97, 'SYN:76.00', '76.00', 2, false, false, '{}'),

  -- CLM-P3A-0002 (page 2, CPT 93000) — payment
  ('0b3a0000-0000-4000-8000-000000000006',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000003',
   'payment', 104.00, 'paid',
   'CLM-P3A-0002', 'Synthetic Payer A', '2026-05-15', '93000', 'SYN-4001',
   0.97, 'SYN:104.00', '104.00', 2, false, false, '{}'),

  -- CLM-P3A-0003 (page 3, CPT 99213) — billed (synthetic underpayment: open=$180.00)
  ('0b3a0000-0000-4000-8000-000000000007',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000004',
   'billed_amount', 350.00, 'billed',
   'CLM-P3A-0003', 'Synthetic Payer A', '2026-05-15', '99213', null,
   0.97, 'SYN:350.00', '350.00', 3, false, false, '{}'),

  -- CLM-P3A-0003 (page 3, CPT 99213) — contractual adjustment
  ('0b3a0000-0000-4000-8000-000000000008',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000004',
   'contractual_adjustment', 70.00, 'contractual_adjustment',
   'CLM-P3A-0003', 'Synthetic Payer A', '2026-05-15', '99213', null,
   0.97, 'SYN:70.00', '70.00', 3, false, false, '{}'),

  -- CLM-P3A-0003 (page 3, CPT 99213) — payment (underpaid: 350−70−100=180 open)
  -- No CARC codes, no patient-responsibility language — clean synthetic short pay.
  ('0b3a0000-0000-4000-8000-000000000009',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000004',
   'payment', 100.00, 'paid',
   'CLM-P3A-0003', 'Synthetic Payer A', '2026-05-15', '99213', 'SYN-4001',
   0.97, 'SYN:100.00', '100.00', 3, false, false, '{}'),

  -- Summary row (page 4) — excluded by Phase 1 (is_summary_row=true)
  -- No claim_identifier — Phase 2 LEFT JOIN yields claim_id=NULL in review queue
  ('0b3a0000-0000-4000-8000-00000000000a',
   'a3000000-0000-4000-8000-0000000000fe',
   'e3a00000-0000-4000-8000-000000000005',
   'payment', 354.00, 'paid',
   null, 'Synthetic Payer A', '2026-05-16', null, 'SYN-4001',
   0.90, 'SYN:354.00', '354.00', 4, true, false, '{}')

on conflict do nothing;

commit;

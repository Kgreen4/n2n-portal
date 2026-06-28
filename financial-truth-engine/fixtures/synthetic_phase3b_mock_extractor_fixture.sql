-- =============================================================================
-- Financial Truth Engine — Synthetic Phase 3B Mock Extractor Fixture
-- fixtures/synthetic_phase3b_mock_extractor_fixture.sql
--
-- Practice ID : b3000000-0000-4000-8000-0000000000fe  (Phase 3B mock extractor test)
-- Covers      : Evidence-only baseline for mock extraction contract validation.
--               The mock extractor (fte_mock_extract_observations) is the SOLE
--               source of observations for this practice — no observations are
--               hand-authored here.
--
-- Scenario:
--   A 2-page remittance from Synthetic Payer B, check SYN-5001 ($260.00).
--   Page 1: CLM-P3B-0001 — billed $300, adj $120, paid $180 → balanced (0.00) CPT 99213
--   Page 2: CLM-P3B-0002 — billed $200, adj $50,  paid $80  → unbalanced ($70.00)  CPT 99214
--   + check_payment stub for SYN-5001 ($180 + $80 = $260; enables two-link event evidence)
--
-- Evidence rows  : 4  (1 document + 2 page + 1 check_payment stub)
-- Claim rows     : 2
-- Observation rows: 0  (all observations are produced by fte_mock_extract_observations)
--
-- Evidence raw_text format (parsed by fte_mock_extract_observations):
--   Line-based key:value pairs, one observation block per page.
--   Parser recognises: CLAIM, PAYER, SERVICE_DATE, CPT, BILLED, ADJ, PAID, CHECK.
--   All raw_text values are prefixed [SYNTHETIC] per fixture conventions.
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
-- Order mirrors FK dependency graph.
-- ---------------------------------------------------------------------------
delete from fte_event_evidence      where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_review_queue        where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_analysis_runs       where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_financial_positions where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_claim_events        where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_claims              where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_observations        where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_evidence            where practice_id = 'b3000000-0000-4000-8000-0000000000fe'
                                      and parent_evidence_id is not null;
delete from fte_evidence            where practice_id = 'b3000000-0000-4000-8000-0000000000fe';
delete from fte_practices           where id = 'b3000000-0000-4000-8000-0000000000fe';

-- ---------------------------------------------------------------------------
-- Practice
-- ---------------------------------------------------------------------------
insert into fte_practices (id, name, external_ref) values
  ('b3000000-0000-4000-8000-0000000000fe',
   'Synthetic Practice Phase3B Mock Extractor',
   'synthetic_phase3b_mock_extractor')
on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Evidence — 4 rows
-- Row 1: parent document (raw_text null — not scanned text, just a handle)
-- Rows 2–3: page children (raw_text prefixed [SYNTHETIC], structured key:value
--           format consumed by fte_mock_extract_observations)
-- Row 4: check_payment stub (raw_text null — structured record, not OCR text)
-- ---------------------------------------------------------------------------
insert into fte_evidence
  (id, practice_id, parent_evidence_id, evidence_type, fixture_id, source_uri,
   content_hash, page_number, raw_text, metadata)
values

  -- parent document
  ('e3b00000-0000-4000-8000-000000000001',
   'b3000000-0000-4000-8000-0000000000fe',
   null, 'document', 'synthetic_phase3b_mock_extractor',
   'private://fte/phase3b/remittance-syn-5001.pdf',
   'sha256:SYNTHETIC_PHASE3B_DOC_001',
   null, null, '{}'),

  -- page 1 — CLM-P3B-0001 (CPT 99213, balanced: 300 − 120 − 180 = 0.00)
  ('e3b00000-0000-4000-8000-000000000002',
   'b3000000-0000-4000-8000-0000000000fe',
   'e3b00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3b_mock_extractor',
   'private://fte/phase3b/remittance-syn-5001.pdf#page=1',
   'sha256:SYNTHETIC_PHASE3B_PAGE1',
   1,
   '[SYNTHETIC]
CLAIM: CLM-P3B-0001
PAYER: Synthetic Payer B
SERVICE_DATE: 2026-05-20
CPT: 99213
BILLED: 300.00
ADJ: 120.00
PAID: 180.00
CHECK: SYN-5001',
   '{}'),

  -- page 2 — CLM-P3B-0002 (CPT 99214, unbalanced: 200 − 50 − 80 = 70.00)
  ('e3b00000-0000-4000-8000-000000000003',
   'b3000000-0000-4000-8000-0000000000fe',
   'e3b00000-0000-4000-8000-000000000001', 'page',
   'synthetic_phase3b_mock_extractor',
   'private://fte/phase3b/remittance-syn-5001.pdf#page=2',
   'sha256:SYNTHETIC_PHASE3B_PAGE2',
   2,
   '[SYNTHETIC]
CLAIM: CLM-P3B-0002
PAYER: Synthetic Payer B
SERVICE_DATE: 2026-05-20
CPT: 99214
BILLED: 200.00
ADJ: 50.00
PAID: 80.00
CHECK: SYN-5001',
   '{}'),

  -- check_payment stub — enables two-link fte_event_evidence for payment_applied events
  -- check_amount = 260.00 = 180.00 (CLM-P3B-0001) + 80.00 (CLM-P3B-0002)
  ('e3b00000-0000-4000-8000-000000000004',
   'b3000000-0000-4000-8000-0000000000fe',
   null, 'check_payment', 'synthetic_phase3b_mock_extractor',
   'private://fte/phase3b/check-stub-SYN-5001',
   'sha256:SYNTHETIC_PHASE3B_CHECK5001',
   null, null,
   '{"check_number":"SYN-5001","check_amount":"260.00","payer_name":"Synthetic Payer B","payment_date":"2026-05-21"}')

on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Claims — 2 rows
-- claim_number must match the CLAIM: value in page raw_text so that
-- fte_mock_extract_observations can set claim_identifier, and the reconciler
-- Phase 3/4/5c JOINs can resolve observations to claims.
-- ---------------------------------------------------------------------------
insert into fte_claims
  (id, practice_id, internal_claim_id, claim_number, payer_claim_number,
   patient_identifier_hash, service_date_start, service_date_end, payer_name, status)
values
  ('c3b00000-0000-4000-8000-000000000001',
   'b3000000-0000-4000-8000-0000000000fe',
   'FTE-P3B-0001', 'CLM-P3B-0001', 'SYN-PCN-5001',
   'synthhash:patient-p3b-a', '2026-05-20', '2026-05-20',
   'Synthetic Payer B', 'open'),

  ('c3b00000-0000-4000-8000-000000000002',
   'b3000000-0000-4000-8000-0000000000fe',
   'FTE-P3B-0002', 'CLM-P3B-0002', 'SYN-PCN-5002',
   'synthhash:patient-p3b-b', '2026-05-20', '2026-05-20',
   'Synthetic Payer B', 'open')

on conflict do nothing;

-- ---------------------------------------------------------------------------
-- Observations — 0 rows
-- All observations are produced by fte_mock_extract_observations at test time.
-- Do not add any observation inserts here.
-- ---------------------------------------------------------------------------

commit;

-- =============================================================================
-- Synthetic Fixture: 96c5c357 (SECONDARY REGRESSION CASE)
-- Simulates an Arizona Priority Care EOB.
--
-- ALL DATA IS SYNTHETIC. No PHI, no real PDF, no real patient/member/claim IDs.
-- '96c5c357' is an opaque internal label only.
--
-- Failure modes simulated (see ../FIXTURE_PLAN.md):
--   * large single-check gap scenario  (~$1,248.11)
--   * a second, still-unresolved gap   (~$966.20), retry-pending
--   * retry-pending ambiguity caused by check-number spacing variants
--     fragmenting one physical check across multiple observations
--
-- Expected behavior demonstrated:
--   * the unresolved gap is visible in fte_review_queue
--   * ledger output stays explainable and evidence-linked
--   * the retry-pending check fragmentation is held in review, not auto-merged
--
-- Idempotent + run as a BYPASSRLS role (Supabase service_role / postgres).
-- =============================================================================

begin;

-- practice: 96000000-0000-4000-8000-0000000000fe

-- ---- Idempotent cleanup -----------------------------------------------------
delete from fte_event_evidence      where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_review_queue        where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_analysis_runs       where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_financial_positions where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_claim_events        where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_claims              where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_observations        where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_evidence            where practice_id = '96000000-0000-4000-8000-0000000000fe' and parent_evidence_id is not null;
delete from fte_evidence            where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_contract_terms      where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_denial_knowledge    where practice_id = '96000000-0000-4000-8000-0000000000fe';
delete from fte_practices           where id = '96000000-0000-4000-8000-0000000000fe';

-- ---- Practice ---------------------------------------------------------------
insert into fte_practices (id, name, external_ref) values
  ('96000000-0000-4000-8000-0000000000fe', 'Synthetic Practice (96c5c357)', 'SYNTH-96C5C357');

-- ---- Evidence ---------------------------------------------------------------
insert into fte_evidence (id, practice_id, parent_evidence_id, evidence_type, fixture_id, source_uri, content_hash, page_number, raw_text, metadata) values
  ('dddd9600-0000-4000-8000-000000000d00', '96000000-0000-4000-8000-0000000000fe', null,
     'document', '96c5c357', 'private://fte/96c5c357/source.pdf', 'sha256:SYNTHETIC_DOC_HASH_96', null,
     null, '{"payer":"Arizona Priority Care","note":"synthetic EOB"}'),
  ('dddd9600-0000-4000-8000-000000000d01', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d00',
     'page', '96c5c357', null, null, 1,
     '[SYNTHETIC] Page 1 — Check #2-1835212 — claim CLM-APC-1000 large gap', '{}'),
  ('dddd9600-0000-4000-8000-000000000d08', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d00',
     'page', '96c5c357', null, null, 8,
     '[SYNTHETIC] Page 8 — Check #2-1835642 (retry-pending render)', '{"retry_pending":true}'),
  ('dddd9600-0000-4000-8000-000000000d0c', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d00',
     'page', '96c5c357', null, null, 12,
     '[SYNTHETIC] Page 12 — Check #2 - 1835642 (spacing variant of page 8)', '{"retry_pending":true}'),
  ('dddd9600-0000-4000-8000-0000000000ca', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d00',
     'check_payment', '96c5c357', null, null, null,
     '[SYNTHETIC] Check stub #2-1835212', '{"check_number":"2-1835212"}');

-- ---- Observations -----------------------------------------------------------
insert into fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type, claim_identifier,
   payer_name, service_date, cpt_code, check_eft_identifier, confidence_score,
   raw_value, normalized_value, page_number, is_summary_row, is_superseded, metadata) values
  -- Claim 1: large single-check gap. Billed high, only a partial payment observed.
  ('0b590000-0000-4000-8000-0000000000a1', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d01',
     'billed_amount', 1600.00, 'billed', 'CLM-APC-1000', 'Arizona Priority Care', '2026-05-02', '93000',
     '2-1835212', 0.95, '$1,600.00', '1600.00', 1, false, false, '{}'),
  ('0b590000-0000-4000-8000-0000000000a2', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d01',
     'payment', 351.89, 'paid', 'CLM-APC-1000', 'Arizona Priority Care', '2026-05-02', '93000',
     '2-1835212', 0.93, '$351.89', '351.89', 1, false, false,
     '{"note":"only partial payment visible; remainder unexplained -> gap ~1248.11"}'),

  -- Claim 2: retry-pending fragmentation — ONE physical check #2-1835642 fragmented into
  -- three observations with check-number spacing variants. None is authoritative yet.
  ('0b590000-0000-4000-8000-0000000000b1', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d08',
     'payment', 982.34, 'paid', 'CLM-APC-2000', 'Arizona Priority Care', '2026-05-02', '99215',
     '2-1835642', 0.66, '$982.34 (ck 2-1835642)', '982.34', 8, false, false,
     '{"failure_mode":"check_spacing_variant_fragmentation","variant":"2-1835642"}'),
  ('0b590000-0000-4000-8000-0000000000b2', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d0c',
     'payment', 39.84, 'paid', 'CLM-APC-2000', 'Arizona Priority Care', '2026-05-02', '99215',
     '2 - 1835642', 0.61, '$39.84 (ck 2 - 1835642)', '39.84', 12, false, false,
     '{"failure_mode":"check_spacing_variant_fragmentation","variant":"2 - 1835642"}'),
  ('0b590000-0000-4000-8000-0000000000b3', '96000000-0000-4000-8000-0000000000fe', 'dddd9600-0000-4000-8000-000000000d0c',
     'check_eft_identifier', null, null, 'CLM-APC-2000', 'Arizona Priority Care', '2026-05-02', null,
     '2- 1835642', 0.40, 'ck 2- 1835642 (third spacing variant, retry-pending)', '2-1835642', 12, false, true,
     '{"failure_mode":"check_spacing_variant_fragmentation","variant":"2- 1835642","retry_pending":true}');

-- ---- Claims -----------------------------------------------------------------
insert into fte_claims
  (id, practice_id, internal_claim_id, claim_number, payer_claim_number,
   patient_identifier_hash, service_date_start, service_date_end, payer_name, status) values
  ('c1a90000-0000-4000-8000-000000001000', '96000000-0000-4000-8000-0000000000fe',
     'FTE-96C5C357-1000', 'CLM-APC-1000', 'APC-PCN-1000', 'synthhash:patient-1000',
     '2026-05-02', '2026-05-02', 'Arizona Priority Care', 'in_review'),
  ('c1a90000-0000-4000-8000-000000002000', '96000000-0000-4000-8000-0000000000fe',
     'FTE-96C5C357-2000', 'CLM-APC-2000', 'APC-PCN-2000', 'synthhash:patient-2000',
     '2026-05-02', '2026-05-02', 'Arizona Priority Care', 'in_review');

-- ---- Claim events -----------------------------------------------------------
-- Claim 1: a confirmed partial payment event (the rest is an open gap, not invented).
insert into fte_claim_events
  (id, practice_id, claim_id, event_type, event_date, amount, amount_type, payer_name,
   reason_category, confidence_score, reconciliation_status, metadata) values
  ('e7e90000-0000-4000-8000-000000001001', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000001000',
     'payment_applied', '2026-05-02', 351.89, 'paid', 'Arizona Priority Care',
     'payment', 0.93, 'reconciled', '{}'),
  ('e7e90000-0000-4000-8000-000000001002', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000001000',
     'short_pay_detected', '2026-05-02', 1248.11, 'other', 'Arizona Priority Care',
     'underpayment', 0.80, 'unbalanced', '{"note":"billed 1600.00 vs paid 351.89 -> gap 1248.11 (recoverable?)"}');
-- Claim 2: NO payment event emitted yet — the fragmented check is retry-pending in review.

-- ---- Event -> evidence links ------------------------------------------------
insert into fte_event_evidence (id, practice_id, claim_event_id, evidence_id, observation_id, link_role) values
  (gen_random_uuid(), '96000000-0000-4000-8000-0000000000fe', 'e7e90000-0000-4000-8000-000000001001', 'dddd9600-0000-4000-8000-000000000d01', '0b590000-0000-4000-8000-0000000000a2', 'supports'),
  (gen_random_uuid(), '96000000-0000-4000-8000-0000000000fe', 'e7e90000-0000-4000-8000-000000001001', 'dddd9600-0000-4000-8000-0000000000ca', null, 'supports'),
  (gen_random_uuid(), '96000000-0000-4000-8000-0000000000fe', 'e7e90000-0000-4000-8000-000000001002', 'dddd9600-0000-4000-8000-000000000d01', '0b590000-0000-4000-8000-0000000000a1', 'derived_from');

-- ---- Financial positions (DERIVED) ------------------------------------------
-- Claim 1: unbalanced with a visible recoverable gap.
insert into fte_financial_positions
  (id, practice_id, claim_id, billed_amount, allowed_amount, contractual_adjustment_amount,
   paid_amount, denied_amount, patient_responsibility_amount, recoverable_amount,
   open_balance_amount, position_confidence_score, reconciliation_status, last_reconciled_at) values
  ('f2090000-0000-4000-8000-000000001000', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000001000',
     1600.00, null, null, 351.89, null, null, 1248.11, 1248.11, 0.70, 'unbalanced', now()),
-- Claim 2: incomplete — cannot materialize a confident position while the check is fragmented.
  ('f2090000-0000-4000-8000-000000002000', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000002000',
     null, null, null, null, null, null, null, 966.20, 0.35, 'in_review', now());

-- ---- Review queue -----------------------------------------------------------
insert into fte_review_queue (id, practice_id, claim_id, claim_event_id, observation_id, evidence_id, reason, status, details) values
  ('6e690000-0000-4000-8000-000000001001', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000001000', 'e7e90000-0000-4000-8000-000000001002',
     null, null, 'unbalanced_financial_position', 'open',
     '{"why":"billed 1600.00 vs paid 351.89 -> open gap 1248.11; confirm underpayment vs missing remit"}'),
  ('6e690000-0000-4000-8000-000000002001', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000002000', null,
     '0b590000-0000-4000-8000-0000000000b3', 'dddd9600-0000-4000-8000-000000000d0c', 'late_retry_page_contradiction', 'open',
     '{"why":"check #2-1835642 fragmented across spacing variants (2-1835642 / 2 - 1835642 / 2- 1835642) on retry-pending pages 8 & 12; sums to ~966.20 but not auto-merged"}'),
  ('6e690000-0000-4000-8000-000000002002', '96000000-0000-4000-8000-0000000000fe', 'c1a90000-0000-4000-8000-000000002000', null,
     '0b590000-0000-4000-8000-0000000000b1', 'dddd9600-0000-4000-8000-000000000d08', 'suspected_duplicate', 'open',
     '{"why":"possible fragmentation of one physical check into multiple payment observations; hold for human confirm"}');

-- ---- Analysis run -----------------------------------------------------------
insert into fte_analysis_runs
  (id, practice_id, run_type, fixture_id, status, inputs_hash, summary, started_at, finished_at) values
  (gen_random_uuid(), '96000000-0000-4000-8000-0000000000fe', 'seed_fixture', '96c5c357', 'succeeded',
     'sha256:SYNTHETIC_INPUTS_96', '2 claims. Claim 1: confirmed partial pay + visible 1248.11 gap. '
     'Claim 2: retry-pending check fragmentation held in review (966.20), no position invented.',
     now(), now());

commit;

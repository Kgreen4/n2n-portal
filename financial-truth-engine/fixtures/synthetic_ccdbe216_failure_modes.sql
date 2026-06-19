-- =============================================================================
-- Synthetic Fixture: ccdbe216 (PRIMARY HARD CASE)
-- Simulates a 112-page BCBS-AZ multiple-payment EOB.
--
-- ALL DATA IS SYNTHETIC. No PHI, no real PDF, no real patient/member/claim IDs.
-- 'ccdbe216' is an opaque internal label only; the real document is never in this repo.
--
-- Failure modes simulated (see ../FIXTURE_PLAN.md):
--   * phantom duplicate check / reference observation
--   * section-delimiter double counting
--   * null-check crossbleed across multiple check sections
--   * spurious summary row ($1,479.08)
--   * late/retry page-63 contradiction
--   * conflicting observations routed to review (never overwritten)
--   * unbalanced financial position routed to review
--
-- Architectural guarantees demonstrated:
--   * conflicting/duplicate/late observations COEXIST (flagged, not deleted)
--   * the summary row is NOT turned into a payment event
--   * financial position is DERIVED from events only (never from observations)
--   * every claim event links to evidence/observations
--   * every ambiguity is visible in fte_review_queue
--
-- Idempotent: deletes its own synthetic practice subtree before re-inserting.
-- Run as a BYPASSRLS role (Supabase service_role / postgres).
-- =============================================================================

begin;

-- ---- Fixed synthetic identifiers --------------------------------------------
-- practice:  c0000000-0000-4000-8000-0000000000fe
-- document:  dddd0000-0000-4000-8000-000000000d00
-- page 1:    dddd0000-0000-4000-8000-000000000d01
-- page 63:   dddd0000-0000-4000-8000-000000000d63   (late/retry page)
-- summary pg:dddd0000-0000-4000-8000-000000000dfa
-- check A:   dddd0000-0000-4000-8000-0000000000ca   (#0000447412)

-- ---- Idempotent cleanup (children first; evidence is delete-restricted) ------
delete from fte_event_evidence      where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_review_queue        where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_analysis_runs       where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_financial_positions where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_claim_events        where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_claims              where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_observations        where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_evidence            where practice_id = 'c0000000-0000-4000-8000-0000000000fe' and parent_evidence_id is not null;
delete from fte_evidence            where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_contract_terms      where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_denial_knowledge    where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
delete from fte_practices           where id = 'c0000000-0000-4000-8000-0000000000fe';

-- ---- Practice ---------------------------------------------------------------
insert into fte_practices (id, name, external_ref) values
  ('c0000000-0000-4000-8000-0000000000fe', 'Synthetic Practice (ccdbe216)', 'SYNTH-CCDBE216');

-- ---- Evidence (immutable, append-only) --------------------------------------
insert into fte_evidence (id, practice_id, parent_evidence_id, evidence_type, fixture_id, source_uri, content_hash, page_number, raw_text, metadata) values
  ('dddd0000-0000-4000-8000-000000000d00', 'c0000000-0000-4000-8000-0000000000fe', null,
     'document', 'ccdbe216', 'private://fte/ccdbe216/source.pdf', 'sha256:SYNTHETIC_DOC_HASH', null,
     null, '{"page_count":112,"payer":"BCBS of Arizona","note":"synthetic multi-payment EOB"}'),
  ('dddd0000-0000-4000-8000-000000000d01', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d00',
     'page', 'ccdbe216', null, null, 1,
     '[SYNTHETIC] Page 1 — Check #0000447412 BCBS AZ — claim CLM-AZ-0001 payment 510.40', '{}'),
  ('dddd0000-0000-4000-8000-000000000d63', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d00',
     'page', 'ccdbe216', null, null, 63,
     '[SYNTHETIC] Page 63 — LATE/RETRY render — claim CLM-AZ-0001 shows DENIED 0.00 (contradicts page 1)', '{"late_retry":true}'),
  ('dddd0000-0000-4000-8000-000000000dfa', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d00',
     'page', 'ccdbe216', null, null, 110,
     '[SYNTHETIC] Page 110 — REMITTANCE SUMMARY — aggregate total 1479.08 (not a per-claim line)', '{"summary_page":true}'),
  ('dddd0000-0000-4000-8000-0000000000ca', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d00',
     'check_payment', 'ccdbe216', null, null, null,
     '[SYNTHETIC] Check stub #0000447412 — amount 510.40', '{"check_number":"0000447412","check_amount":510.40}');

-- ---- Observations (AI-visible facts; NOT truth) -----------------------------
-- obs A: the AUTHORITATIVE payment observation (high confidence, page 1).
insert into fte_observations
  (id, practice_id, evidence_id, observation_type, amount, amount_type, claim_identifier,
   payer_name, service_date, cpt_code, carc_code, check_eft_identifier, confidence_score,
   raw_value, normalized_value, page_number, is_summary_row, is_superseded, metadata) values
  ('0b500000-0000-4000-8000-0000000000a1', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'payment', 510.40, 'paid', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', null,
     '0000447412', 0.97, '$510.40', '510.40', 1, false, false,
     '{"role":"authoritative"}'),
  -- supporting non-payment observations for the same claim
  ('0b500000-0000-4000-8000-0000000000a2', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'billed_amount', 720.00, 'billed', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', null,
     '0000447412', 0.96, '$720.00', '720.00', 1, false, false, '{}'),
  ('0b500000-0000-4000-8000-0000000000a3', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'contractual_adjustment', 209.60, 'contractual_adjustment', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', 'CO-45',
     '0000447412', 0.95, '$209.60 CO-45', '209.60', 1, false, false,
     '{"note":"CO-45 contractual adjustment — must NOT be recorded as paid_amount"}'),

  -- FAILURE MODE 1: phantom duplicate check reference (OCR misread of #0000447412).
  ('0b500000-0000-4000-8000-0000000000b1', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'payment', 510.40, 'paid', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', null,
     'O000447412', 0.52, '$510.40 (ck O000447412)', '510.40', 1, false, true,
     '{"failure_mode":"phantom_duplicate_check_ref","ocr_variant_of":"0000447412"}'),

  -- FAILURE MODE 2: section-delimiter double count (same $ re-read across a section boundary).
  ('0b500000-0000-4000-8000-0000000000b2', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'payment', 510.40, 'paid', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', null,
     '0000447412', 0.58, '$510.40 (repeated past delimiter)', '510.40', 2, false, true,
     '{"failure_mode":"section_delimiter_double_count"}'),

  -- FAILURE MODE 3: null-check crossbleed (amount with NO check id near a section boundary).
  ('0b500000-0000-4000-8000-0000000000b3', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d01',
     'payment', 265.36, 'paid', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99213', null,
     null, 0.49, '$265.36 (no check id visible)', '265.36', 2, false, true,
     '{"failure_mode":"null_check_crossbleed","belongs_to":"different_check_section_unknown"}'),

  -- FAILURE MODE 4: spurious summary row ($1,479.08) — aggregate, NOT a transaction.
  ('0b500000-0000-4000-8000-0000000000b4', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000dfa',
     'summary_total', 1479.08, 'paid', null, 'BCBS of Arizona', null, null, null,
     null, 0.90, 'REMITTANCE TOTAL $1,479.08', '1479.08', 110, true, false,
     '{"failure_mode":"spurious_summary_row","is_aggregate":true}'),

  -- FAILURE MODE 5: late/retry page-63 contradiction (same claim now shows DENIED 0.00).
  ('0b500000-0000-4000-8000-0000000000b5', 'c0000000-0000-4000-8000-0000000000fe', 'dddd0000-0000-4000-8000-000000000d63',
     'denial', 0.00, 'denied', 'CLM-AZ-0001', 'BCBS of Arizona', '2026-04-15', '99214', 'CO-97',
     '0000447412', 0.71, 'DENIED 0.00 (page 63 retry render)', '0.00', 63, false, true,
     '{"failure_mode":"late_retry_page_contradiction","contradicts":"0b500000-0000-4000-8000-0000000000a1"}');

-- ---- Claim (identity) -------------------------------------------------------
insert into fte_claims
  (id, practice_id, internal_claim_id, claim_number, payer_claim_number,
   patient_identifier_hash, service_date_start, service_date_end, payer_name, status) values
  ('c1a10000-0000-4000-8000-000000000001', 'c0000000-0000-4000-8000-0000000000fe',
     'FTE-CCDBE216-0001', 'CLM-AZ-0001', 'BCBSAZ-PCN-0001',
     'synthhash:patient-0001', '2026-04-15', '2026-04-15', 'BCBS of Arizona', 'in_review');

-- ---- Claim events (deterministic; derived ONLY from trusted observations) ----
-- The reconciler emits ONE payment event from the authoritative observation (obs A).
-- It does NOT emit events from the phantom dup, the double-count, the null crossbleed,
-- the summary row, or the contradicting late-retry observation — those go to review.
insert into fte_claim_events
  (id, practice_id, claim_id, event_type, event_date, amount, amount_type, payer_name,
   carc_code, reason_category, confidence_score, reconciliation_status, metadata) values
  ('e7e70000-0000-4000-8000-000000000001', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001',
     'claim_adjudicated', '2026-04-15', 720.00, 'billed', 'BCBS of Arizona',
     null, 'adjudication', 0.96, 'reconciled', '{}'),
  ('e7e70000-0000-4000-8000-000000000002', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001',
     'contractual_adjustment_applied', '2026-04-15', 209.60, 'contractual_adjustment', 'BCBS of Arizona',
     'CO-45', 'contractual', 0.95, 'reconciled', '{}'),
  ('e7e70000-0000-4000-8000-000000000003', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001',
     'payment_applied', '2026-04-15', 510.40, 'paid', 'BCBS of Arizona',
     null, 'payment', 0.97, 'ambiguous', '{"note":"authoritative payment; contradicted by late-retry obs -> see review_queue"}');

-- ---- Event -> evidence/observation links (audit spine) -----------------------
insert into fte_event_evidence (id, practice_id, claim_event_id, evidence_id, observation_id, link_role) values
  -- adjudication event <- billed observation + page 1
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'e7e70000-0000-4000-8000-000000000001', 'dddd0000-0000-4000-8000-000000000d01', '0b500000-0000-4000-8000-0000000000a2', 'derived_from'),
  -- contractual adjustment event <- CO-45 observation
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'e7e70000-0000-4000-8000-000000000002', 'dddd0000-0000-4000-8000-000000000d01', '0b500000-0000-4000-8000-0000000000a3', 'derived_from'),
  -- payment event <- authoritative payment observation + check stub
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'e7e70000-0000-4000-8000-000000000003', 'dddd0000-0000-4000-8000-000000000d01', '0b500000-0000-4000-8000-0000000000a1', 'supports'),
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'e7e70000-0000-4000-8000-000000000003', 'dddd0000-0000-4000-8000-0000000000ca', null, 'supports'),
  -- the late-retry observation is recorded as CONTRADICTING evidence for the payment event
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'e7e70000-0000-4000-8000-000000000003', 'dddd0000-0000-4000-8000-000000000d63', '0b500000-0000-4000-8000-0000000000b5', 'contradicts');

-- ---- Financial position (DERIVED from events; never from observations) -------
-- paid 510.40 is recorded, but the page-63 contradiction + null-check crossbleed leave the
-- position unable to confirm fully => unbalanced, with an open balance routed to review.
insert into fte_financial_positions
  (id, practice_id, claim_id, billed_amount, allowed_amount, contractual_adjustment_amount,
   paid_amount, denied_amount, patient_responsibility_amount, recoverable_amount,
   open_balance_amount, position_confidence_score, reconciliation_status, last_reconciled_at) values
  ('f2050000-0000-4000-8000-000000000001', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001',
     720.00, 510.40, 209.60, 510.40, 0.00, 0.00, 638.23, 638.23, 0.55, 'unbalanced', now());

-- ---- Review queue (every ambiguity is explicit) -----------------------------
insert into fte_review_queue (id, practice_id, claim_id, claim_event_id, observation_id, evidence_id, reason, status, details) values
  ('6e6e0000-0000-4000-8000-000000000001', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001', null,
     '0b500000-0000-4000-8000-0000000000b1', 'dddd0000-0000-4000-8000-000000000d01', 'suspected_duplicate', 'open',
     '{"why":"phantom check ref O000447412 is an OCR variant of 0000447412 for the same $510.40 payment"}'),
  ('6e6e0000-0000-4000-8000-000000000002', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001', null,
     '0b500000-0000-4000-8000-0000000000b2', 'dddd0000-0000-4000-8000-000000000d01', 'conflicting_observations', 'open',
     '{"why":"section-delimiter double count of $510.40 across a section boundary"}'),
  ('6e6e0000-0000-4000-8000-000000000003', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001', null,
     '0b500000-0000-4000-8000-0000000000b3', 'dddd0000-0000-4000-8000-000000000d01', 'missing_evidence_link', 'open',
     '{"why":"$265.36 has no visible check id -> cannot attribute to a check section (null-check crossbleed)"}'),
  ('6e6e0000-0000-4000-8000-000000000004', 'c0000000-0000-4000-8000-0000000000fe', null, null,
     '0b500000-0000-4000-8000-0000000000b4', 'dddd0000-0000-4000-8000-000000000dfa', 'suspected_summary_row', 'open',
     '{"why":"$1,479.08 is a remittance aggregate, not a per-claim transaction; must not become a payment event"}'),
  ('6e6e0000-0000-4000-8000-000000000005', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001', 'e7e70000-0000-4000-8000-000000000003',
     '0b500000-0000-4000-8000-0000000000b5', 'dddd0000-0000-4000-8000-000000000d63', 'late_retry_page_contradiction', 'open',
     '{"why":"page-63 late/retry render shows DENIED 0.00 for a claim page 1 shows paid 510.40"}'),
  ('6e6e0000-0000-4000-8000-000000000006', 'c0000000-0000-4000-8000-0000000000fe', 'c1a10000-0000-4000-8000-000000000001', null,
     null, null, 'unbalanced_financial_position', 'open',
     '{"why":"open_balance 638.23 unresolved pending crossbleed + contradiction review","gap_context":"original doc-level gap was 4720.98"}');

-- ---- Analysis run (audit metadata) ------------------------------------------
insert into fte_analysis_runs
  (id, practice_id, run_type, fixture_id, status, inputs_hash, summary, started_at, finished_at) values
  (gen_random_uuid(), 'c0000000-0000-4000-8000-0000000000fe', 'seed_fixture', 'ccdbe216', 'succeeded',
     'sha256:SYNTHETIC_INPUTS', '1 claim, 9 observations (4 conflicting/duplicate/late, 1 summary), '
     '3 events, 1 unbalanced position, 6 review items. Observations did not mutate the position.',
     now(), now());

commit;

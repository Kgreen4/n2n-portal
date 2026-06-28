-- =============================================================================
-- Financial Truth Engine — Mock Observation Extractor
-- reconciler/fte_mock_extract_observations.sql
--
-- Function: fte_mock_extract_observations(p_practice_id uuid) RETURNS integer
--
-- Deterministic SQL-only mock of the AI extraction boundary.
-- Reads [SYNTHETIC] page evidence rows for the given practice and inserts
-- fte_observations rows that the reconciler can then classify and process.
--
-- This function proves the evidence → observations interface contract without
-- any live AI calls, Edge Functions, or production data. The real AI extractor
-- will replace this function once the pipeline contract is proven; the function
-- signature and observation shape defined here remain stable.
--
-- Contract rules:
--   - Reads only evidence_type='page' rows where raw_text LIKE '[SYNTHETIC]%'
--     and fixture_id = 'synthetic_phase3b_mock_extractor'.
--   - Parses the line-based key:value format used in the Phase 3B fixture.
--   - Inserts exactly 3 observations per page: billed_amount, contractual_adjustment,
--     payment (in that order).
--   - Does NOT set classification — Phase 1 of fte_reconcile_practice owns that.
--   - All raw_value fields start with '[SYNTHETIC]' to satisfy the fixture invariant.
--   - Idempotent: skips any page that already has observations with
--     metadata->>'extractor' = 'fte_mock_extract_observations' for the same
--     practice, evidence_id, claim_identifier, and all three observation types.
--
-- Prerequisites: migrations 001–011 applied; fixtures loaded.
-- Run as a database role with ordinary read/write access to the FTE tables.
--
-- No AI calls. No live data. No PHI. No credentials stored here.
-- =============================================================================

CREATE OR REPLACE FUNCTION fte_mock_extract_observations(
  p_practice_id uuid
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
SET search_path = public
AS $$
DECLARE
  v_row            record;
  v_lines          text[];
  v_line           text;
  v_claim_id       text := null;
  v_payer          text := null;
  v_svc_date       date := null;
  v_cpt            text := null;
  v_billed         numeric(14,2) := null;
  v_adj            numeric(14,2) := null;
  v_paid           numeric(14,2) := null;
  v_check_id       text := null;
  v_meta           jsonb;
  v_inserted       integer := 0;
  v_already_exists bigint;
BEGIN

  v_meta := jsonb_build_object(
    'extractor',  'fte_mock_extract_observations',
    'fixture_id', 'synthetic_phase3b_mock_extractor',
    'mock',       true
  );

  FOR v_row IN
    SELECT id, page_number, raw_text
    FROM   fte_evidence
    WHERE  practice_id   = p_practice_id
      AND  evidence_type = 'page'
      AND  raw_text IS NOT NULL
      AND  raw_text LIKE '[SYNTHETIC]%'
      AND  fixture_id    = 'synthetic_phase3b_mock_extractor'
    ORDER BY page_number
  LOOP

    -- -----------------------------------------------------------------------
    -- Parse line-based key:value block
    -- -----------------------------------------------------------------------
    v_claim_id  := null;
    v_payer     := null;
    v_svc_date  := null;
    v_cpt       := null;
    v_billed    := null;
    v_adj       := null;
    v_paid      := null;
    v_check_id  := null;

    v_lines := regexp_split_to_array(v_row.raw_text, E'\n');

    FOREACH v_line IN ARRAY v_lines
    LOOP
      v_line := trim(v_line);
      IF    v_line LIKE 'CLAIM: %'        THEN v_claim_id := trim(substring(v_line FROM 8));
      ELSIF v_line LIKE 'PAYER: %'        THEN v_payer    := trim(substring(v_line FROM 8));
      ELSIF v_line LIKE 'SERVICE_DATE: %' THEN v_svc_date := trim(substring(v_line FROM 14))::date;
      ELSIF v_line LIKE 'CPT: %'          THEN v_cpt      := trim(substring(v_line FROM 6));
      ELSIF v_line LIKE 'BILLED: %'       THEN v_billed   := trim(substring(v_line FROM 9))::numeric(14,2);
      ELSIF v_line LIKE 'ADJ: %'          THEN v_adj      := trim(substring(v_line FROM 6))::numeric(14,2);
      ELSIF v_line LIKE 'PAID: %'         THEN v_paid     := trim(substring(v_line FROM 7))::numeric(14,2);
      ELSIF v_line LIKE 'CHECK: %'        THEN v_check_id := trim(substring(v_line FROM 8));
      END IF;
    END LOOP;

    -- Skip malformed or incomplete pages
    IF v_claim_id IS NULL OR v_billed IS NULL OR v_adj IS NULL OR v_paid IS NULL THEN
      CONTINUE;
    END IF;

    -- -----------------------------------------------------------------------
    -- Idempotency: skip if this page's observations already exist for this
    -- practice/evidence/claim/extractor combination.
    -- -----------------------------------------------------------------------
    SELECT COUNT(*) INTO v_already_exists
    FROM   fte_observations
    WHERE  practice_id      = p_practice_id
      AND  evidence_id      = v_row.id
      AND  claim_identifier = v_claim_id
      AND  metadata->>'extractor' = 'fte_mock_extract_observations'
      AND  observation_type IN ('billed_amount', 'contractual_adjustment', 'payment');

    IF v_already_exists >= 3 THEN
      CONTINUE;
    END IF;

    -- -----------------------------------------------------------------------
    -- Insert 3 observations for this page
    -- -----------------------------------------------------------------------

    -- 1. billed_amount
    INSERT INTO fte_observations
      (practice_id, evidence_id, observation_type, amount, amount_type,
       claim_identifier, payer_name, service_date, cpt_code, check_eft_identifier,
       confidence_score, raw_value, normalized_value, page_number,
       is_summary_row, is_superseded, bounding_box, metadata)
    VALUES
      (p_practice_id, v_row.id,
       'billed_amount', v_billed, 'billed',
       v_claim_id, v_payer, v_svc_date, v_cpt, null,
       0.9500,
       '[SYNTHETIC] billed_amount ' || to_char(v_billed, 'FM999999999999990.00'),
       to_char(v_billed, 'FM999999999999990.00'),
       v_row.page_number, false, false, null, v_meta);

    -- 2. contractual_adjustment
    INSERT INTO fte_observations
      (practice_id, evidence_id, observation_type, amount, amount_type,
       claim_identifier, payer_name, service_date, cpt_code, check_eft_identifier,
       confidence_score, raw_value, normalized_value, page_number,
       is_summary_row, is_superseded, bounding_box, metadata)
    VALUES
      (p_practice_id, v_row.id,
       'contractual_adjustment', v_adj, 'contractual_adjustment',
       v_claim_id, v_payer, v_svc_date, v_cpt, null,
       0.9500,
       '[SYNTHETIC] contractual_adjustment ' || to_char(v_adj, 'FM999999999999990.00'),
       to_char(v_adj, 'FM999999999999990.00'),
       v_row.page_number, false, false, null, v_meta);

    -- 3. payment (check_eft_identifier links to the check_payment stub in Phase 5c)
    INSERT INTO fte_observations
      (practice_id, evidence_id, observation_type, amount, amount_type,
       claim_identifier, payer_name, service_date, cpt_code, check_eft_identifier,
       confidence_score, raw_value, normalized_value, page_number,
       is_summary_row, is_superseded, bounding_box, metadata)
    VALUES
      (p_practice_id, v_row.id,
       'payment', v_paid, 'paid',
       v_claim_id, v_payer, v_svc_date, v_cpt, v_check_id,
       0.9500,
       '[SYNTHETIC] payment ' || to_char(v_paid, 'FM999999999999990.00'),
       to_char(v_paid, 'FM999999999999990.00'),
       v_row.page_number, false, false, null, v_meta);

    v_inserted := v_inserted + 3;

  END LOOP;

  RETURN v_inserted;

END;
$$;

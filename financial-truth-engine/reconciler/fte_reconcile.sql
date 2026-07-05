-- =============================================================================
-- Financial Truth Engine — Reconciler
-- reconciler/fte_reconcile.sql
--
-- Stored procedure: fte_reconcile_practice(p_practice_id uuid) RETURNS jsonb
--
-- Runs deterministically inside a single DB transaction. All derived tables
-- (fte_claim_events, fte_financial_positions, fte_review_queue,
-- fte_event_evidence) are wiped for the practice in Phase 0 and re-derived
-- from scratch, making every call idempotent.
--
-- Observations are classified (trusted / suspect / excluded) and routed.
-- Only TRUSTED observations produce claim events. Suspect and excluded
-- observations are captured in fte_review_queue for human review.
--
-- 9 phases (E2 adds derived sub-phases 4b and 5d):
--   0. Idempotent reset
--   1. Classify observations into temp table _fte_classified
--   2. Route non-trusted observations to review queue
--   3. Emit claim_adjudicated events from trusted billed_amount observations
--   4. Emit contractual_adjustment_applied events from trusted
--      contractual_adjustment observations
--   4b. (E2) DERIVE contractual_adjustment_applied = billed - allowed from
--       canonical billed_amount + allowed_amount observations (no observation
--       row is created). Only when no observed contractual event already exists
--       for the claim. Ambiguous (>1 canonical billed/allowed) or anomalous
--       (allowed > billed) cases fail closed to an ambiguous marker → in_review.
--   5c. Emit payment_applied events from trusted payment observations
--   5. (late/retry) Wire late_retry review entries to ambiguous payment events
--   5d. (E2) Emit patient_responsibility_assigned from canonical
--       patient_responsibility observations (>1 canonical → ambiguous → in_review)
--   5e. (Denial / Task 014B) Emit denial_posted from amount-bearing canonical
--       denial observations (multiple denials aggregate; over-denial — denied >
--       billed − contractual − paid − patient_responsibility — routes the claim
--       to in_review via an ambiguous marker). No new enum / review reason.
--   6. Derive financial positions (E2: allowed_amount +
--      patient_responsibility_amount columns; Denial: denied_amount column;
--      open_balance also subtracts patient responsibility AND denied)
--   6b. (Recoverable / Task 014D) Overlay recoverable_amount from
--       fte_denial_knowledge — the recoverable subset of denied_amount. Reporting
--       overlay only: does NOT affect open_balance / short_pay / status. Most-
--       specific applicable knowledge row wins; top-tier conflict or no match
--       fails closed to non-recoverable. No new events / evidence / review reason.
--   7. Route unbalanced / in_review / incomplete positions to review queue
--   8. Emit short_pay_detected events for positive open balances
--   9. Record analysis run, return summary JSON
--
-- Prerequisites: migration 001_create_financial_truth_schema.sql applied.
-- Run as a role with BYPASSRLS (Supabase service_role / postgres).
-- =============================================================================

CREATE OR REPLACE FUNCTION fte_reconcile_practice(p_practice_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id        uuid        := gen_random_uuid();
  v_started_at    timestamptz := clock_timestamp();
  v_obs           record;
  v_event_id      uuid;
  v_pay_event_id  uuid;
  v_event_date    date;
  v_rq_rec        record;
  v_pos           record;
  v_claim_count        bigint;
  v_event_count        bigint;
  v_review_count       bigint;
  v_pos_count          bigint;
  v_resolution_count   integer := 0;
  -- E2 (Task 010B) derived-accounting scratch values.
  v_billed_eff         numeric(14,2);
  v_allowed_eff        numeric(14,2);
  v_adj                numeric(14,2);
  v_pr_eff             numeric(14,2);
  -- E2 canonical single-candidate id/evidence selectors (Task 012B hardening):
  -- populated by explicit scalar SELECTs only inside the count = 1 branches.
  v_billed_obs_id      uuid;
  v_billed_ev_id       uuid;
  v_allowed_obs_id     uuid;
  v_allowed_ev_id      uuid;
  v_pr_obs_id          uuid;
  v_pr_ev_id           uuid;
BEGIN

  -- =========================================================================
  -- PHASE 0: Idempotent reset.
  --
  -- Delete all derived rows for this practice in FK-safe order:
  --   fte_event_evidence → fte_review_queue → fte_financial_positions
  --   → fte_claim_events
  -- fte_analysis_runs is append-only (audit trail); it is NOT wiped.
  -- =========================================================================
  DELETE FROM fte_event_evidence      WHERE practice_id = p_practice_id;
  DELETE FROM fte_review_queue        WHERE practice_id = p_practice_id;
  DELETE FROM fte_financial_positions WHERE practice_id = p_practice_id;
  DELETE FROM fte_claim_events        WHERE practice_id = p_practice_id;


  -- =========================================================================
  -- PHASE 0.5: Load active review resolutions.
  --
  -- Non-superseded resolutions for this practice are snapshotted into a temp
  -- table for use by downstream phases. Zero rows is valid — empty table
  -- means no active resolutions and all downstream phases behave unchanged.
  --
  -- DROP before CREATE mirrors the Phase 1 pattern for _fte_classified:
  -- guards against duplicate-table errors when the function is called
  -- multiple times in the same outer transaction (idempotency requirement).
  -- =========================================================================
  DROP TABLE IF EXISTS _fte_active_resolutions;

  CREATE TEMP TABLE _fte_active_resolutions ON COMMIT DROP AS
  SELECT *
  FROM fte_review_resolutions
  WHERE practice_id  = p_practice_id
    AND is_superseded = false;

  GET DIAGNOSTICS v_resolution_count = ROW_COUNT;

  -- Suppressed observations: reject_observation and mark_duplicate remove the
  -- observation from Phase 1 entirely — no classification, events, or queue
  -- entry are derived from it in this reconciler run.
  DROP TABLE IF EXISTS _fte_suppressed_observations;

  CREATE TEMP TABLE _fte_suppressed_observations ON COMMIT DROP AS
  SELECT observation_id
  FROM _fte_active_resolutions
  WHERE action IN ('reject_observation', 'mark_duplicate')
    AND observation_id IS NOT NULL;

  -- Rejected payment-event claims: observations remain classifiable/trusted;
  -- only Phase 5c payment_applied event emission is suppressed.
  DROP TABLE IF EXISTS _fte_rejected_payment_event_claims;

  CREATE TEMP TABLE _fte_rejected_payment_event_claims ON COMMIT DROP AS
  SELECT DISTINCT claim_id
  FROM _fte_active_resolutions
  WHERE action = 'reject_payment_event'
    AND claim_id IS NOT NULL;


  -- =========================================================================
  -- PHASE 1: Classify every observation for this practice.
  --
  -- Five rules, first-match wins:
  --   Rule 1  is_superseded = true                              → excluded
  --   Rule 2  is_summary_row = true (not superseded)           → excluded
  --   Rule 3  observation_type = 'payment' AND
  --           check_eft_identifier IS NULL (not superseded,
  --           not summary)                                      → excluded
  --   Rule 4  failure_mode IS NOT NULL AND <> '' (not superseded,
  --           not summary, not rule-3)                          → suspect
  --   Rule 5  everything else                                   → trusted
  --
  -- DROP before CREATE ensures idempotency when the function is called
  -- multiple times in the same outer transaction. ON COMMIT DROP alone is
  -- insufficient because the temp table persists for the life of the
  -- transaction, not just the function call.
  -- =========================================================================
  DROP TABLE IF EXISTS _fte_classified;

  CREATE TEMP TABLE _fte_classified ON COMMIT DROP AS
  WITH base AS (
    SELECT
      obs.*,
      CASE
        WHEN obs.is_superseded
          THEN 'excluded'
        WHEN obs.is_summary_row
          THEN 'excluded'
        WHEN obs.observation_type = 'payment'
             AND obs.check_eft_identifier IS NULL
          THEN 'excluded'
        WHEN (obs.metadata->>'failure_mode') IS NOT NULL
             AND (obs.metadata->>'failure_mode') <> ''
          THEN 'suspect'
        ELSE 'trusted'
      END AS classification,
      -- Pre-compute failure_mode → candidate review_reason for reuse in the
      -- outer SELECT. The actual review_reason must respect Rule 2 and Rule 3
      -- overrides, so this value is only used for excluded-by-Rule-1 and
      -- suspect (Rule 4) rows.
      CASE obs.metadata->>'failure_mode'
        WHEN 'phantom_duplicate_check_ref'
          THEN 'suspected_duplicate'
        WHEN 'section_delimiter_double_count'
          THEN 'conflicting_observations'
        WHEN 'null_check_crossbleed'
          THEN 'missing_evidence_link'
        WHEN 'late_retry_page_contradiction'
          THEN 'late_retry_page_contradiction'
        WHEN 'check_spacing_variant_fragmentation'
          THEN
            CASE WHEN (obs.metadata->>'retry_pending') = 'true'
              THEN 'late_retry_page_contradiction'
              ELSE 'suspected_duplicate'
            END
        ELSE 'conflicting_observations'
      END AS fm_reason
    FROM fte_observations obs
    WHERE obs.practice_id = p_practice_id
      AND NOT EXISTS (
        SELECT 1
        FROM _fte_suppressed_observations so
        WHERE so.observation_id = obs.id
      )
  )
  SELECT
    base.*,
    -- review_reason is meaningful only for excluded/suspect rows.
    CASE
      WHEN classification = 'trusted'
        THEN NULL
      WHEN is_superseded
        THEN fm_reason                    -- Rule 1: use failure_mode mapping
      WHEN is_summary_row
        THEN 'suspected_summary_row'      -- Rule 2: override regardless of failure_mode
      WHEN observation_type = 'payment'
           AND check_eft_identifier IS NULL
        THEN 'missing_evidence_link'      -- Rule 3: payment with no check reference
      ELSE fm_reason                      -- Rule 4: suspect via failure_mode
    END AS review_reason
  FROM base;


  -- =========================================================================
  -- PHASE 2: Route all non-trusted observations to the review queue.
  --
  -- claim_id may be NULL (e.g., summary rows with no claim_identifier).
  -- Phase 5 (late/retry) will wire claim_event_id for late_retry entries
  -- after payment events are emitted.
  -- =========================================================================
  INSERT INTO fte_review_queue
    (practice_id, claim_id, observation_id, evidence_id, reason, status, details)
  SELECT
    p_practice_id,
    c.id,
    cl.id          AS observation_id,
    cl.evidence_id,
    cl.review_reason,
    'open',
    jsonb_build_object(
      'classification',   cl.classification,
      'failure_mode',     cl.metadata->>'failure_mode',
      'observation_type', cl.observation_type
    )
  FROM _fte_classified cl
  LEFT JOIN fte_claims c
    ON  c.practice_id = p_practice_id
    AND c.claim_number = cl.claim_identifier
  WHERE cl.classification IN ('excluded', 'suspect')
    AND NOT EXISTS (
      SELECT 1 FROM _fte_active_resolutions ar
      WHERE ar.observation_id = cl.id
        AND ar.action = 'confirm_observation'
    );


  -- =========================================================================
  -- PHASE 3: Emit claim_adjudicated events from trusted billed_amount obs.
  --
  -- Each event gets one fte_event_evidence link (derived_from) pointing at
  -- the observation and its source evidence.
  --
  -- corrected_billed_amount: correlated subquery looks up any active
  -- attach_corrected_value resolution for this observation (same pattern as
  -- Phase 4 contractual adjustment and Phase 5c payment corrections). The
  -- unique partial index on fte_review_resolutions (migration 004) guarantees
  -- at most one active row, making LIMIT 1 deterministic. COALESCE falls back
  -- to the extracted amount when no correction exists, preserving existing
  -- behaviour.
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid,
      (SELECT ar.corrected_value
       FROM _fte_active_resolutions ar
       WHERE ar.observation_id = cl.id
         AND ar.action         = 'attach_corrected_value'
       LIMIT 1) AS corrected_billed_amount
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification  = 'trusted'
      AND cl.observation_type = 'billed_amount'
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'claim_adjudicated', v_obs.service_date,
       COALESCE(v_obs.corrected_billed_amount, v_obs.amount), 'billed', v_obs.payer_name,
       'adjudication', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_obs.evidence_id, v_obs.id, 'derived_from');

  END LOOP;


  -- =========================================================================
  -- PHASE 4: Emit contractual_adjustment_applied events from trusted
  --          contractual_adjustment observations.
  --
  -- carc_code is propagated from the observation to the event.
  --
  -- corrected_adj_amount: correlated subquery looks up any active
  -- attach_corrected_value resolution for this observation (same pattern as
  -- Phase 5c payment corrections). The unique partial index on
  -- fte_review_resolutions (migration 004) guarantees at most one active row,
  -- making LIMIT 1 deterministic. COALESCE falls back to the extracted amount
  -- when no correction exists, preserving existing behaviour.
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid,
      (SELECT ar.corrected_value
       FROM _fte_active_resolutions ar
       WHERE ar.observation_id = cl.id
         AND ar.action         = 'attach_corrected_value'
       LIMIT 1) AS corrected_adj_amount
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'contractual_adjustment'
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, carc_code, reason_category, confidence_score,
       reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid,
       'contractual_adjustment_applied', v_obs.service_date,
       COALESCE(v_obs.corrected_adj_amount, v_obs.amount),
       'contractual_adjustment', v_obs.payer_name, v_obs.carc_code,
       'contractual', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_obs.evidence_id, v_obs.id, 'derived_from');

  END LOOP;


  -- =========================================================================
  -- PHASE 4b (E2 / Task 010B): DERIVE contractual_adjustment_applied from
  --          canonical billed_amount and allowed_amount observations.
  --
  -- Policy ruling #7: contractual adjustment is DERIVED as an event
  -- (amount = billed - allowed); NO fte_observations.contractual_adjustment
  -- row is ever created or mutated here.
  --
  -- Per claim (one aggregated row per claim that has >=1 trusted billed or
  -- allowed observation):
  --   * Skip if an OBSERVED contractual_adjustment_applied event already
  --     exists for the claim (Phase 4) — prefer observed over derived.
  --   * Derive only when EXACTLY one canonical billed_amount AND exactly one
  --     canonical allowed_amount exist for the claim.
  --   * billed == allowed  -> emit a zero-amount event (allowed stays
  --     reconstructible; ruling #1). Zero amounts are permitted by the
  --     fte_claim_events.amount column (no non-zero constraint).
  --   * allowed  > billed   -> anomaly: emit NO negative event; instead emit an
  --     ambiguous-status marker (NULL amount) so Phase 6 routes the claim to
  --     in_review (existing mechanism; no new enum/queue-reason/migration).
  --   * >1 canonical billed or allowed -> ambiguous: same fail-closed marker.
  --   * billed or allowed absent -> no derived event (not ambiguous).
  -- Effective amounts honor active attach_corrected_value the same way
  -- Phases 3/4/5c do, so the derived adjustment stays consistent with the
  -- billed value used for claim_adjudicated.
  -- =========================================================================
  FOR v_obs IN (
    SELECT
      c.id AS claim_uuid,
      COUNT(*) FILTER (WHERE cl.observation_type = 'billed_amount')  AS billed_ct,
      COUNT(*) FILTER (WHERE cl.observation_type = 'allowed_amount') AS allowed_ct,
      -- Only aggregate the scalar (numeric/text/date) fields here. The single
      -- billed/allowed observation id + evidence_id are fetched by explicit
      -- scalar SELECTs inside the count = 1 branch below (Task 012B), avoiding
      -- an aggregate over uuid.
      MAX(cl.amount)           FILTER (WHERE cl.observation_type = 'billed_amount')  AS billed_amt,
      MAX(cl.confidence_score) FILTER (WHERE cl.observation_type = 'billed_amount')  AS billed_conf,
      MAX(cl.payer_name)       FILTER (WHERE cl.observation_type = 'billed_amount')  AS billed_payer,
      MAX(cl.service_date)     FILTER (WHERE cl.observation_type = 'billed_amount')  AS billed_date,
      MAX(cl.amount)           FILTER (WHERE cl.observation_type = 'allowed_amount') AS allowed_amt,
      MAX(cl.confidence_score) FILTER (WHERE cl.observation_type = 'allowed_amount') AS allowed_conf
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id  = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type IN ('billed_amount', 'allowed_amount')
    GROUP BY c.id
  ) LOOP

    -- Prefer observed contractual adjustment (Phase 4) over derived.
    IF EXISTS (
      SELECT 1 FROM fte_claim_events ce
      WHERE ce.practice_id = p_practice_id
        AND ce.claim_id    = v_obs.claim_uuid
        AND ce.event_type  = 'contractual_adjustment_applied'
    ) THEN
      CONTINUE;
    END IF;

    -- Ambiguous: more than one canonical billed or allowed candidate.
    IF v_obs.billed_ct > 1 OR v_obs.allowed_ct > 1 THEN
      INSERT INTO fte_claim_events
        (practice_id, claim_id, event_type, event_date, amount, amount_type,
         reason_category, confidence_score, reconciliation_status, metadata)
      VALUES
        (p_practice_id, v_obs.claim_uuid, 'contractual_adjustment_applied', NULL,
         NULL, 'contractual_adjustment', 'contractual', NULL, 'ambiguous',
         jsonb_build_object('derivation', 'billed_minus_allowed',
                            'ambiguous_reason', 'multiple_canonical_billed_or_allowed'))
      RETURNING id INTO v_event_id;

      INSERT INTO fte_event_evidence
        (practice_id, claim_event_id, evidence_id, observation_id, link_role)
      SELECT p_practice_id, v_event_id, cl.evidence_id, cl.id, 'derived_from'
      FROM _fte_classified cl
      JOIN fte_claims c
        ON  c.practice_id  = p_practice_id
        AND c.claim_number = cl.claim_identifier
      WHERE cl.classification   = 'trusted'
        AND cl.observation_type IN ('billed_amount', 'allowed_amount')
        AND c.id = v_obs.claim_uuid;

      CONTINUE;
    END IF;

    -- Derive only when exactly one billed AND exactly one allowed exist.
    IF v_obs.billed_ct = 1 AND v_obs.allowed_ct = 1 THEN
      -- Exactly one canonical billed and one canonical allowed observation exist
      -- for this claim (guaranteed by the counts above). Fetch each one's id +
      -- evidence_id with an explicit scalar SELECT. This is safe precisely
      -- because it runs only in this count = 1 branch: the query returns exactly
      -- one row, so no LIMIT / ORDER BY (which would mask ambiguity) is used.
      SELECT cl.id, cl.evidence_id
        INTO v_billed_obs_id, v_billed_ev_id
      FROM _fte_classified cl
      JOIN fte_claims c
        ON  c.practice_id  = p_practice_id
        AND c.claim_number = cl.claim_identifier
      WHERE c.id = v_obs.claim_uuid
        AND cl.classification   = 'trusted'
        AND cl.observation_type = 'billed_amount';

      SELECT cl.id, cl.evidence_id
        INTO v_allowed_obs_id, v_allowed_ev_id
      FROM _fte_classified cl
      JOIN fte_claims c
        ON  c.practice_id  = p_practice_id
        AND c.claim_number = cl.claim_identifier
      WHERE c.id = v_obs.claim_uuid
        AND cl.classification   = 'trusted'
        AND cl.observation_type = 'allowed_amount';

      v_billed_eff := COALESCE(
        (SELECT ar.corrected_value FROM _fte_active_resolutions ar
         WHERE ar.observation_id = v_billed_obs_id
           AND ar.action = 'attach_corrected_value' LIMIT 1),
        v_obs.billed_amt);
      v_allowed_eff := COALESCE(
        (SELECT ar.corrected_value FROM _fte_active_resolutions ar
         WHERE ar.observation_id = v_allowed_obs_id
           AND ar.action = 'attach_corrected_value' LIMIT 1),
        v_obs.allowed_amt);
      v_adj := v_billed_eff - v_allowed_eff;

      IF v_adj < 0 THEN
        -- Anomaly (allowed > billed): fail closed to ambiguous marker (no
        -- negative event), routing the claim to in_review via Phase 6.
        INSERT INTO fte_claim_events
          (practice_id, claim_id, event_type, event_date, amount, amount_type,
           reason_category, confidence_score, reconciliation_status, metadata)
        VALUES
          (p_practice_id, v_obs.claim_uuid, 'contractual_adjustment_applied', NULL,
           NULL, 'contractual_adjustment', 'contractual', NULL, 'ambiguous',
           jsonb_build_object('derivation', 'billed_minus_allowed',
                              'ambiguous_reason', 'allowed_exceeds_billed'))
        RETURNING id INTO v_event_id;

        INSERT INTO fte_event_evidence
          (practice_id, claim_event_id, evidence_id, observation_id, link_role)
        VALUES
          (p_practice_id, v_event_id, v_billed_ev_id,  v_billed_obs_id,  'derived_from'),
          (p_practice_id, v_event_id, v_allowed_ev_id, v_allowed_obs_id, 'derived_from');

        CONTINUE;
      END IF;

      -- v_adj >= 0 (including exactly 0 when billed == allowed): emit derived
      -- contractual_adjustment_applied with two evidence links.
      INSERT INTO fte_claim_events
        (practice_id, claim_id, event_type, event_date, amount, amount_type,
         payer_name, reason_category, confidence_score, reconciliation_status, metadata)
      VALUES
        (p_practice_id, v_obs.claim_uuid, 'contractual_adjustment_applied',
         v_obs.billed_date, v_adj, 'contractual_adjustment',
         v_obs.billed_payer, 'contractual',
         LEAST(v_obs.billed_conf, v_obs.allowed_conf), 'reconciled',
         jsonb_build_object('derivation', 'billed_minus_allowed'))
      RETURNING id INTO v_event_id;

      INSERT INTO fte_event_evidence
        (practice_id, claim_event_id, evidence_id, observation_id, link_role)
      VALUES
        (p_practice_id, v_event_id, v_billed_ev_id,  v_billed_obs_id,  'derived_from'),
        (p_practice_id, v_event_id, v_allowed_ev_id, v_allowed_obs_id, 'derived_from');
    END IF;

  END LOOP;


  -- =========================================================================
  -- PHASE 5c: Emit payment_applied events from trusted payment observations.
  --
  -- Each payment event gets two fte_event_evidence links (both link_role=
  -- 'supports'):
  --   (1) the page observation that reported the payment
  --   (2) the check_payment evidence stub matched by check_eft_identifier
  --       (if a matching stub exists; the INSERT is a no-op if not found)
  --
  -- corrected_amount: correlated subquery looks up any active
  -- attach_corrected_value resolution for this observation.  The unique
  -- partial index on fte_review_resolutions (migration 004) guarantees at
  -- most one active row, making LIMIT 1 deterministic rather than advisory.
  -- COALESCE falls back to the extracted amount when no correction exists,
  -- so existing behaviour is unchanged when no resolution is present.
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid,
      (SELECT ar.corrected_value
       FROM _fte_active_resolutions ar
       WHERE ar.observation_id = cl.id
         AND ar.action         = 'attach_corrected_value'
       LIMIT 1) AS corrected_amount
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'payment'
  ) LOOP

    -- Skip payment_applied event emission when reviewer has rejected this payment.
    -- The observation remains 'trusted' and participates in Phase 1 classification;
    -- only the Phase 5c INSERT is suppressed, so Phase 6 recalculates open_balance
    -- as billed − adj − 0 (full billed amount), and Phases 7/8 are not suppressed.
    IF EXISTS (
      SELECT 1
      FROM _fte_rejected_payment_event_claims r
      WHERE r.claim_id = v_obs.claim_uuid
    ) THEN
      CONTINUE;
    END IF;

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'payment_applied', v_obs.service_date,
       COALESCE(v_obs.corrected_amount, v_obs.amount), 'paid', v_obs.payer_name,
       'payment', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    -- Link 1: the page/observation that reported the payment.
    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_obs.evidence_id, v_obs.id, 'supports');

    -- Link 2: the check_payment stub matched by check number (if present).
    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    SELECT p_practice_id, v_event_id, ev.id, NULL, 'supports'
    FROM fte_evidence ev
    WHERE ev.practice_id  = p_practice_id
      AND ev.evidence_type = 'check_payment'
      AND ev.metadata->>'check_number' = v_obs.check_eft_identifier
    LIMIT 1;

  END LOOP;


  -- =========================================================================
  -- PHASE 5 (late/retry): For each late_retry_page_contradiction review
  -- entry that has an observation, find the payment_applied event for the
  -- same claim and:
  --   a. Mark the payment event 'ambiguous'.
  --   b. Wire claim_event_id on the review queue entry.
  --   c. Add a 'contradicts' fte_event_evidence link from the contradicting
  --      evidence / observation to the payment event.
  --
  -- Runs AFTER Phase 5c so payment events already exist.
  -- If no payment event exists for the claim (e.g. all payment obs were
  -- suspect/excluded), this loop is a no-op for that entry.
  -- =========================================================================
  FOR v_rq_rec IN (
    SELECT
      rq.id           AS rq_id,
      rq.claim_id,
      rq.observation_id,
      obs.claim_identifier,
      obs.evidence_id AS obs_evidence_id
    FROM fte_review_queue rq
    JOIN fte_observations obs
      ON obs.id = rq.observation_id
    WHERE rq.practice_id = p_practice_id
      AND rq.reason      = 'late_retry_page_contradiction'
      AND rq.observation_id IS NOT NULL
  ) LOOP

    -- Find the payment_applied event for the claim this contradiction targets.
    SELECT ce.id INTO v_pay_event_id
    FROM fte_claim_events ce
    JOIN fte_claims c ON c.id = ce.claim_id
    WHERE ce.practice_id  = p_practice_id
      AND ce.event_type   = 'payment_applied'
      AND c.claim_number  = v_rq_rec.claim_identifier
    LIMIT 1;

    IF v_pay_event_id IS NOT NULL THEN
      -- a. If an active confirm_payment_event resolution exists for this claim,
      --    the reviewer has confirmed the original payment is correct → 'reconciled'.
      --    Otherwise the contradiction is unresolved → 'ambiguous'.
      UPDATE fte_claim_events
         SET reconciliation_status = CASE
           WHEN EXISTS (
             SELECT 1 FROM _fte_active_resolutions
             WHERE claim_id = v_rq_rec.claim_id
               AND action   = 'confirm_payment_event'
           ) THEN 'reconciled'
           ELSE 'ambiguous'
         END
       WHERE id = v_pay_event_id;

      -- b. Wire the review entry to the payment event.
      UPDATE fte_review_queue
         SET claim_event_id = v_pay_event_id
       WHERE id = v_rq_rec.rq_id;

      -- c. Add the contradicts audit link.
      INSERT INTO fte_event_evidence
        (practice_id, claim_event_id, evidence_id, observation_id, link_role)
      VALUES
        (p_practice_id, v_pay_event_id,
         v_rq_rec.obs_evidence_id, v_rq_rec.observation_id, 'contradicts');
    END IF;

  END LOOP;


  -- =========================================================================
  -- PHASE 5d (E2 / Task 010B): Emit patient_responsibility_assigned events
  --          from canonical patient_responsibility observations.
  --
  -- Per claim (one aggregated row per claim with >=1 trusted
  -- patient_responsibility observation):
  --   * exactly one canonical candidate -> emit patient_responsibility_assigned
  --     (honoring active attach_corrected_value, like Phases 3/4/5c).
  --   * >1 canonical candidate -> ambiguous: emit NULL-amount ambiguous marker
  --     so Phase 6 routes the claim to in_review (existing mechanism; no new
  --     enum/queue-reason/migration).
  --   * absent -> no event.
  -- A nonzero patient responsibility is NOT itself a short pay; it only feeds
  -- the Phase 6 open_balance formula.
  -- =========================================================================
  FOR v_obs IN (
    SELECT
      c.id AS claim_uuid,
      COUNT(*)                 AS pr_ct,
      -- Only aggregate the scalar (numeric/text/date) fields here. The single
      -- patient_responsibility observation id + evidence_id are fetched by an
      -- explicit scalar SELECT inside the count = 1 path below (Task 012B),
      -- avoiding an aggregate over uuid.
      MAX(cl.amount)           AS pr_amt,
      MAX(cl.confidence_score) AS pr_conf,
      MAX(cl.payer_name)       AS pr_payer,
      MAX(cl.service_date)     AS pr_date
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id  = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'patient_responsibility'
    GROUP BY c.id
  ) LOOP

    -- Ambiguous: more than one canonical patient_responsibility candidate.
    IF v_obs.pr_ct > 1 THEN
      INSERT INTO fte_claim_events
        (practice_id, claim_id, event_type, event_date, amount, amount_type,
         reason_category, confidence_score, reconciliation_status, metadata)
      VALUES
        (p_practice_id, v_obs.claim_uuid, 'patient_responsibility_assigned', NULL,
         NULL, 'patient_responsibility', 'patient_responsibility', NULL, 'ambiguous',
         jsonb_build_object('ambiguous_reason', 'multiple_canonical_patient_responsibility'))
      RETURNING id INTO v_event_id;

      INSERT INTO fte_event_evidence
        (practice_id, claim_event_id, evidence_id, observation_id, link_role)
      SELECT p_practice_id, v_event_id, cl.evidence_id, cl.id, 'derived_from'
      FROM _fte_classified cl
      JOIN fte_claims c
        ON  c.practice_id  = p_practice_id
        AND c.claim_number = cl.claim_identifier
      WHERE cl.classification   = 'trusted'
        AND cl.observation_type = 'patient_responsibility'
        AND c.id = v_obs.claim_uuid;

      CONTINUE;
    END IF;

    -- Exactly one canonical patient_responsibility observation (pr_ct = 1 here,
    -- the >1 case returned above). Fetch its id + evidence_id with an explicit
    -- scalar SELECT — safe because it runs only in this count = 1 path: the
    -- query returns exactly one row, so no LIMIT / ORDER BY is used.
    SELECT cl.id, cl.evidence_id
      INTO v_pr_obs_id, v_pr_ev_id
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id  = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE c.id = v_obs.claim_uuid
      AND cl.classification   = 'trusted'
      AND cl.observation_type = 'patient_responsibility';

    v_pr_eff := COALESCE(
      (SELECT ar.corrected_value FROM _fte_active_resolutions ar
       WHERE ar.observation_id = v_pr_obs_id
         AND ar.action = 'attach_corrected_value' LIMIT 1),
      v_obs.pr_amt);

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'patient_responsibility_assigned',
       v_obs.pr_date, v_pr_eff, 'patient_responsibility',
       v_obs.pr_payer, 'patient_responsibility', v_obs.pr_conf, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_pr_ev_id, v_pr_obs_id, 'derived_from');

  END LOOP;


  -- =========================================================================
  -- PHASE 5e (Denial / Task 014B): Emit denial_posted events from amount-bearing
  --          canonical denial observations.
  --
  -- Ruling: denial is EXPLAINED accounting — the denied amount reduces
  -- open_balance (Phase 6) and is tracked in denied_amount; it is NOT itself a
  -- short pay. Multiple denials on a claim are legitimate (line/CARC level) and
  -- AGGREGATE: one denial_posted per amount-bearing canonical denial observation,
  -- summed into denied_amount. Multiplicity alone is NOT ambiguity.
  --
  -- Part 1 (per observation): emit one denial_posted per amount-bearing canonical
  -- denial observation (amount IS NOT NULL), honoring active attach_corrected_value
  -- like Phases 3/4/4b/5c/5d, propagating carc/rarc, with one evidence link.
  -- No-amount CARC/RARC-only denial signals do NOT derive an amount and emit no
  -- financial event here (Task 014B ruling; recoverable_amount is Task 014C).
  --
  -- Part 2 (per claim): over-denial guard. If billed is known and the claim's
  -- total denied exceeds the residual available before denial
  -- (billed − contractual − paid − patient_responsibility), emit a NULL-amount
  -- ambiguous marker so Phase 6 routes the claim to in_review (existing
  -- mechanism; no new enum / queue reason). Denied money is not silently clamped
  -- to balanced.
  -- =========================================================================
  -- Part 1: one denial_posted per amount-bearing canonical denial observation.
  FOR v_obs IN (
    SELECT
      cl.id            AS obs_id,
      cl.evidence_id   AS ev_id,
      cl.amount        AS denied_amt,
      cl.carc_code     AS carc_code,
      cl.rarc_code     AS rarc_code,
      cl.payer_name    AS payer_name,
      cl.service_date  AS service_date,
      cl.confidence_score AS confidence_score,
      c.id             AS claim_uuid,
      (SELECT ar.corrected_value FROM _fte_active_resolutions ar
       WHERE ar.observation_id = cl.id
         AND ar.action = 'attach_corrected_value' LIMIT 1) AS corrected_denied
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id  = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'denial'
      AND cl.amount IS NOT NULL
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, carc_code, rarc_code, reason_category, confidence_score,
       reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'denial_posted', v_obs.service_date,
       COALESCE(v_obs.corrected_denied, v_obs.denied_amt), 'denied',
       v_obs.payer_name, v_obs.carc_code, v_obs.rarc_code, 'denial',
       v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_obs.ev_id, v_obs.obs_id, 'derived_from');

  END LOOP;

  -- Part 2: over-denial guard (per claim). billed known + denied > residual
  -- available before denial -> ambiguous marker -> in_review.
  FOR v_obs IN (
    SELECT
      c.id AS claim_uuid,
      (SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated')) IS NULL
        AS billed_unknown,
      COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),                 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'),  0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),                 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'patient_responsibility_assigned'), 0)
        AS residual_before_denial,
      COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted'), 0) AS denied_sum
    FROM fte_claims c
    JOIN fte_claim_events ce
      ON  ce.claim_id    = c.id
      AND ce.practice_id = p_practice_id
    WHERE c.practice_id = p_practice_id
    GROUP BY c.id
    HAVING COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted'), 0) > 0
  ) LOOP

    IF NOT v_obs.billed_unknown
       AND v_obs.denied_sum > v_obs.residual_before_denial THEN
      INSERT INTO fte_claim_events
        (practice_id, claim_id, event_type, event_date, amount, amount_type,
         reason_category, confidence_score, reconciliation_status, metadata)
      VALUES
        (p_practice_id, v_obs.claim_uuid, 'denial_posted', NULL,
         NULL, 'denied', 'denial', NULL, 'ambiguous',
         jsonb_build_object('ambiguous_reason', 'denied_exceeds_residual'))
      RETURNING id INTO v_event_id;

      INSERT INTO fte_event_evidence
        (practice_id, claim_event_id, evidence_id, observation_id, link_role)
      SELECT p_practice_id, v_event_id, cl.evidence_id, cl.id, 'derived_from'
      FROM _fte_classified cl
      JOIN fte_claims c
        ON  c.practice_id  = p_practice_id
        AND c.claim_number = cl.claim_identifier
      WHERE cl.classification   = 'trusted'
        AND cl.observation_type = 'denial'
        AND cl.amount IS NOT NULL
        AND c.id = v_obs.claim_uuid;
    END IF;

  END LOOP;


  -- =========================================================================
  -- PHASE 5f (Denial lifecycle / Task 017C): consume actionable lifecycle
  -- resolutions from _fte_active_resolutions and emit lifecycle events.
  --
  -- Gate: the existing actionable-resolution gate — a row present in
  -- _fte_active_resolutions (Phase 0.5 already filtered is_superseded = false).
  -- No new approval path is introduced.
  --
  --   file_appeal        -> appeal_filed        (reporting-only, non-monetary)
  --   record_recovery    -> recovery_received   (reclassifies denied money)
  --   approve_write_off  -> write_off_approved  (reclassifies denied money)
  --
  -- recovery_received and write_off_approved draw from the SAME per-claim GROSS
  -- denied pool (SUM of amount-bearing denial_posted events). A running per-claim
  -- consumed total enforces the cumulative cap recovered + written_off <= gross
  -- denied. Rows are processed in deterministic (claim_id, resolved_at, id) order
  -- and each amount is validated against the REMAINING pool after prior lifecycle
  -- consumption. Phase 6 folds these events into the position: denied_amount
  -- becomes NET (gross - recovered - written_off); recovered_amount /
  -- written_off_amount are cumulative buckets; open_balance stays on GROSS denied
  -- (unchanged). Lifecycle events are 'reconciled' with NULL confidence, so they
  -- do not alter position status or confidence.
  --
  -- Event-sourced idempotency: Phase 0 wiped prior events, so re-emitting here
  -- from the same active resolutions yields identical events/amounts on rerun.
  --
  -- Anomaly routing (existing mechanism; NO monetary effect, NO clamping): an
  -- unresolvable/ambiguous target, a claim with no denied pool, or an amount that
  -- exceeds the remaining pool is recorded in fte_review_queue with the existing
  -- reason 'conflicting_observations' and an anomaly descriptor in details, and
  -- the resolution emits no event and consumes nothing.
  -- =========================================================================
  DECLARE
    v_lc         record;
    v_claim_uuid uuid;
    v_claim_ct   integer;
    v_gross      numeric;
    v_consumed   numeric := 0;
    v_prev_claim uuid    := NULL;
    v_remaining  numeric;
    v_lc_event   uuid;
    v_link_ev    uuid;
    v_link_obs   uuid;
    v_anom       text;
  BEGIN
    FOR v_lc IN (
      SELECT ar.id, ar.claim_id, ar.action, ar.lifecycle_amount,
             ar.observation_id, ar.evidence_id, ar.resolved_at
      FROM _fte_active_resolutions ar
      WHERE ar.action IN ('file_appeal', 'record_recovery', 'approve_write_off')
      ORDER BY ar.claim_id NULLS LAST, ar.resolved_at, ar.id
    ) LOOP

      -- Reset the per-claim running consumed total when the claim changes.
      IF v_prev_claim IS DISTINCT FROM v_lc.claim_id THEN
        v_consumed   := 0;
        v_prev_claim := v_lc.claim_id;
      END IF;

      -- Resolve the target position (claim), practice-scoped and unique.
      v_claim_uuid := NULL;
      v_claim_ct   := 0;
      IF v_lc.claim_id IS NOT NULL THEN
        SELECT count(*) INTO v_claim_ct
        FROM fte_claims c
        WHERE c.id = v_lc.claim_id AND c.practice_id = p_practice_id;
        IF v_claim_ct = 1 THEN
          v_claim_uuid := v_lc.claim_id;
        END IF;
      END IF;

      IF v_claim_uuid IS NULL THEN
        -- Anomaly: no (or non-unique) target position resolvable. No mutation.
        v_anom := CASE WHEN v_claim_ct > 1 THEN 'lifecycle_target_ambiguous'
                       ELSE 'lifecycle_target_unresolved' END;
        INSERT INTO fte_review_queue (practice_id, claim_id, reason, status, details)
        VALUES (p_practice_id, NULL, 'conflicting_observations', 'open',
          jsonb_build_object('anomaly', v_anom, 'action', v_lc.action,
            'lifecycle_amount', v_lc.lifecycle_amount, 'resolution_id', v_lc.id));
        CONTINUE;
      END IF;

      -- Best available evidence link: prefer the claim's denial_posted evidence
      -- chain; fall back to the resolution's own evidence/observation reference.
      v_link_ev  := NULL;
      v_link_obs := NULL;
      SELECT ee.evidence_id, ee.observation_id INTO v_link_ev, v_link_obs
      FROM fte_event_evidence ee
      JOIN fte_claim_events ce ON ce.id = ee.claim_event_id
      WHERE ce.practice_id = p_practice_id
        AND ce.claim_id    = v_claim_uuid
        AND ce.event_type  = 'denial_posted'
        AND ee.link_role   = 'derived_from'
      LIMIT 1;
      IF v_link_ev IS NULL AND v_link_obs IS NULL THEN
        v_link_ev  := v_lc.evidence_id;
        v_link_obs := v_lc.observation_id;
      END IF;

      -- file_appeal: reporting-only marker. Emit appeal_filed; no consumption.
      IF v_lc.action = 'file_appeal' THEN
        INSERT INTO fte_claim_events
          (practice_id, claim_id, event_type, event_date, amount, amount_type,
           reason_category, confidence_score, reconciliation_status, metadata)
        VALUES
          (p_practice_id, v_claim_uuid, 'appeal_filed', v_lc.resolved_at::date,
           NULL, NULL, 'appeal', NULL, 'reconciled',
           jsonb_build_object('resolution_id', v_lc.id))
        RETURNING id INTO v_lc_event;

        IF v_link_ev IS NOT NULL OR v_link_obs IS NOT NULL THEN
          INSERT INTO fte_event_evidence
            (practice_id, claim_event_id, evidence_id, observation_id, link_role)
          VALUES (p_practice_id, v_lc_event, v_link_ev, v_link_obs, 'derived_from');
        END IF;
        CONTINUE;
      END IF;

      -- record_recovery / approve_write_off: validate against the denied pool.
      SELECT COALESCE(SUM(ce.amount), 0) INTO v_gross
      FROM fte_claim_events ce
      WHERE ce.practice_id = p_practice_id
        AND ce.claim_id    = v_claim_uuid
        AND ce.event_type  = 'denial_posted'
        AND ce.amount IS NOT NULL;

      v_remaining := v_gross - v_consumed;

      IF v_gross <= 0 OR v_lc.lifecycle_amount > v_remaining THEN
        -- Anomaly: no denied pool, or amount exceeds remaining pool. No mutation,
        -- no clamping.
        v_anom := CASE WHEN v_gross <= 0 THEN 'lifecycle_no_denied_pool'
                       ELSE 'lifecycle_amount_exceeds_remaining_denied' END;
        INSERT INTO fte_review_queue (practice_id, claim_id, reason, status, details)
        VALUES (p_practice_id, v_claim_uuid, 'conflicting_observations', 'open',
          jsonb_build_object('anomaly', v_anom, 'action', v_lc.action,
            'lifecycle_amount', v_lc.lifecycle_amount,
            'gross_denied', v_gross, 'remaining_denied', v_remaining,
            'resolution_id', v_lc.id));
        CONTINUE;
      END IF;

      -- Valid: emit the lifecycle event and consume from the denied pool.
      INSERT INTO fte_claim_events
        (practice_id, claim_id, event_type, event_date, amount, amount_type,
         reason_category, confidence_score, reconciliation_status, metadata)
      VALUES
        (p_practice_id, v_claim_uuid,
         CASE v_lc.action WHEN 'record_recovery' THEN 'recovery_received'
                          ELSE 'write_off_approved' END,
         v_lc.resolved_at::date, v_lc.lifecycle_amount,
         CASE v_lc.action WHEN 'record_recovery' THEN 'recovery' ELSE 'write_off' END,
         CASE v_lc.action WHEN 'record_recovery' THEN 'recovery' ELSE 'write_off' END,
         NULL, 'reconciled',
         jsonb_build_object('resolution_id', v_lc.id))
      RETURNING id INTO v_lc_event;

      IF v_link_ev IS NOT NULL OR v_link_obs IS NOT NULL THEN
        INSERT INTO fte_event_evidence
          (practice_id, claim_event_id, evidence_id, observation_id, link_role)
        VALUES (p_practice_id, v_lc_event, v_link_ev, v_link_obs, 'derived_from');
      END IF;

      v_consumed := v_consumed + v_lc.lifecycle_amount;

    END LOOP;
  END;


  -- =========================================================================
  -- PHASE 6: Derive financial positions.
  --
  -- A position row is created for every claim that has at least one emitted
  -- claim event OR at least one review queue entry (even if all review entries
  -- have NULL claim_id, those do not contribute to position rows).
  --
  -- reconciliation_status derivation (priority order):
  --   1. No events at all               → 'in_review'
  --   2. Any event is 'ambiguous'        → 'in_review'
  --      (schema does not allow 'ambiguous' on positions; 'in_review' is the
  --       correct mapping — the claim needs human review before it can be
  --       considered balanced or unbalanced)
  --   3. Any event is 'unbalanced'       → 'unbalanced'
  --   4. open_balance_amount > 0         → 'unbalanced'
  --   5. otherwise                       → 'balanced'
  --
  -- position_confidence_score: MIN(confidence_score) across all events except
  -- short_pay_detected (which is derived, not directly observed). Falls back
  -- to 0.0000 when no eligible events exist.
  --
  -- open_balance_amount: NULL when billed_amount is unknown (no claim_adjudicated
  -- event); else GREATEST(0, billed - adj - paid).
  -- =========================================================================
  INSERT INTO fte_financial_positions
    (practice_id, claim_id,
     billed_amount, allowed_amount, contractual_adjustment_amount, paid_amount,
     denied_amount, patient_responsibility_amount, recovered_amount, written_off_amount,
     open_balance_amount, position_confidence_score,
     reconciliation_status, last_reconciled_at)
  SELECT
    p_practice_id,
    c.id,
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),
    -- allowed_amount (E2): events-derived as billed - contractual_adjustment,
    -- preserving the "positions computed only from claim_events" invariant.
    -- NULL when either billed or contractual adjustment is unknown.
    CASE
      WHEN SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated') IS NULL
        OR SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied') IS NULL
        THEN NULL
      ELSE SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated')
         - SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied')
    END,
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'),
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),
    -- denied_amount (Denial 014B; NET after lifecycle 017C): gross SUM of
    -- denial_posted, reduced by recovery_received and write_off_approved. NULL
    -- when the claim has no denials. Non-negative — the Phase 5f cumulative cap
    -- guarantees recovered + written_off <= gross denied. The ambiguous
    -- over-denial marker carries a NULL amount and is ignored by SUM.
    CASE
      WHEN SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted') IS NULL
        THEN NULL
      ELSE SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted')
         - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'recovery_received'),   0)
         - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'write_off_approved'), 0)
    END,
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'patient_responsibility_assigned'),
    -- recovered_amount / written_off_amount (017C): cumulative reclassified
    -- buckets. NULL when none. open_balance and status below stay on GROSS denied
    -- (SUM of denial_posted), so they are unchanged by lifecycle reclassification.
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'recovery_received'),
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'write_off_approved'),
    -- open_balance (E2 + Denial): NULL when billed is unknown; else GREATEST(0,
    -- billed - contractual_adjustment - paid - patient_responsibility - denied).
    -- Patient responsibility and denial are known, explained portions of the
    -- balance, not gaps.
    CASE
      WHEN SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated') IS NULL
        THEN NULL
      ELSE GREATEST(0,
        COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),                 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'),  0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),                 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'patient_responsibility_assigned'), 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted'),                   0)
      )
    END,
    -- position_confidence_score: min across non-short_pay events; 0 if none.
    COALESCE(
      MIN(ce.confidence_score) FILTER (WHERE ce.event_type <> 'short_pay_detected'),
      0.0000
    ),
    -- reconciliation_status
    -- Note: 'ambiguous' is a valid status on fte_claim_events but NOT on
    -- fte_financial_positions (schema CHECK constraint). When any linked
    -- payment event is 'ambiguous', the position maps to 'in_review' — even
    -- when the math balances to zero. Financial truth cannot be finalized
    -- while contradicting evidence is unresolved.
    --
    -- E1 (Task 007J) incomplete-status fix: when a claim has one or more
    -- emitted events but NO claim_adjudicated (billed) event, the billed
    -- amount is unknown, so the balance cannot be computed. Such a position
    -- must NOT be reported 'balanced' (the old fall-through behavior, since
    -- GREATEST(0, 0 - adj - paid) is never > 0). It is 'incomplete' instead
    -- and is routed to review by Phase 7. This branch is placed after the
    -- ambiguous/unbalanced-event checks (those remain authoritative) and
    -- before the balance math, so billed-known cases are unaffected.
    CASE
      WHEN COUNT(ce.id) = 0
        THEN 'in_review'
      WHEN COUNT(ce.id) FILTER (WHERE ce.reconciliation_status = 'ambiguous') > 0
        THEN 'in_review'
      WHEN COUNT(ce.id) FILTER (WHERE ce.reconciliation_status = 'unbalanced') > 0
        THEN 'unbalanced'
      WHEN SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated') IS NULL
        THEN 'incomplete'
      WHEN GREATEST(0,
          COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),                 0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'),  0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),                 0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'patient_responsibility_assigned'), 0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'denial_posted'),                   0)
        ) > 0
        THEN 'unbalanced'
      ELSE 'balanced'
    END,
    clock_timestamp()
  FROM fte_claims c
  LEFT JOIN fte_claim_events ce
    ON  ce.claim_id    = c.id
    AND ce.practice_id = p_practice_id
  WHERE c.practice_id = p_practice_id
    AND (
      EXISTS (
        SELECT 1 FROM fte_claim_events ce2
        WHERE  ce2.claim_id    = c.id
          AND  ce2.practice_id = p_practice_id
      )
      OR EXISTS (
        SELECT 1 FROM fte_review_queue rq
        WHERE  rq.claim_id    = c.id
          AND  rq.practice_id = p_practice_id
      )
    )
  GROUP BY c.id;


  -- =========================================================================
  -- PHASE 6b (Recoverable / Task 014D): overlay recoverable_amount onto the
  --          financial positions using fte_denial_knowledge.
  --
  -- recoverable_amount is a REPORTING OVERLAY only: it is the subset of
  -- denied_amount that denial knowledge marks recoverable. It does NOT affect
  -- open_balance, short_pay, or reconciliation_status (all computed in Phase 6
  -- above and untouched here). By construction recoverable_amount <= denied_amount.
  --
  -- Per amount-bearing denial_posted event, the most-specific applicable
  -- fte_denial_knowledge row decides recoverability. A knowledge row is
  -- APPLICABLE when each scoped field is NULL (wildcard) or equals the event/claim
  -- context. Specificity score: practice(+8) + payer(+4) + carc(+2) + rarc(+1);
  -- a fully-wildcard row scores 0 (allowed as lowest-specificity catch-all). The
  -- highest score wins; among the top-score rows the event is recoverable ONLY if
  -- they unanimously agree recoverable = true (bool_and) — so a no-match event or
  -- a top-tier conflict fails closed to non-recoverable. No LIMIT/ORDER BY tie
  -- guessing. No new events, no event_evidence, no review-queue routing.
  --
  -- Value convention: recoverable_amount stays NULL when the claim has no denials
  -- (denied_amount IS NULL); otherwise COALESCE(sum of recoverable denials, 0).
  -- The ambiguous over-denial marker (NULL amount) is excluded (amount IS NOT NULL).
  -- Idempotent: Phase 0 clears positions; Phase 6 re-inserts; Phase 6b re-overlays.
  -- =========================================================================
  WITH denial_events AS (
    SELECT ce.id AS event_id, ce.claim_id, ce.amount,
           ce.carc_code, ce.rarc_code, ce.payer_name, c.practice_id
    FROM fte_claim_events ce
    JOIN fte_claims c ON c.id = ce.claim_id
    WHERE ce.practice_id = p_practice_id
      AND ce.event_type  = 'denial_posted'
      AND ce.amount IS NOT NULL
  ),
  scored AS (
    SELECT de.event_id, de.claim_id, de.amount, dk.recoverable,
           (CASE WHEN dk.practice_id IS NOT NULL THEN 8 ELSE 0 END
          + CASE WHEN dk.payer_name  IS NOT NULL THEN 4 ELSE 0 END
          + CASE WHEN dk.carc_code   IS NOT NULL THEN 2 ELSE 0 END
          + CASE WHEN dk.rarc_code   IS NOT NULL THEN 1 ELSE 0 END) AS score
    FROM denial_events de
    JOIN fte_denial_knowledge dk
      ON  (dk.practice_id = de.practice_id OR dk.practice_id IS NULL)
      AND (dk.payer_name  = de.payer_name  OR dk.payer_name  IS NULL)
      AND (dk.carc_code   = de.carc_code   OR dk.carc_code   IS NULL)
      AND (dk.rarc_code   = de.rarc_code   OR dk.rarc_code   IS NULL)
  ),
  best AS (
    SELECT event_id, MAX(score) AS max_score
    FROM scored
    GROUP BY event_id
  ),
  event_recoverable AS (
    -- Recoverable only if EVERY top-score row agrees recoverable = true.
    -- Mixed top-score rows (conflict) or all-false -> bool_and = false -> not recoverable.
    SELECT s.event_id, s.claim_id, s.amount, bool_and(s.recoverable) AS is_recoverable
    FROM scored s
    JOIN best b ON b.event_id = s.event_id AND s.score = b.max_score
    GROUP BY s.event_id, s.claim_id, s.amount
  ),
  claim_recoverable AS (
    -- Per claim with denial events: sum only the recoverable amounts (no-match
    -- events are absent from event_recoverable and contribute 0). COALESCE gives 0
    -- when the claim has denials but none recoverable.
    SELECT de.claim_id,
           COALESCE(SUM(er.amount) FILTER (WHERE er.is_recoverable), 0) AS recoverable_sum
    FROM denial_events de
    LEFT JOIN event_recoverable er ON er.event_id = de.event_id
    GROUP BY de.claim_id
  )
  UPDATE fte_financial_positions fp
     SET recoverable_amount = cr.recoverable_sum
  FROM claim_recoverable cr
  WHERE fp.practice_id = p_practice_id
    AND fp.claim_id    = cr.claim_id;


  -- =========================================================================
  -- PHASE 7: Route every unbalanced, in_review, or incomplete position to the
  -- review queue with reason 'unbalanced_financial_position'.
  --
  -- E1 (Task 007J): 'incomplete' positions (billed unknown but other events
  -- present) are routed here so they are surfaced for review rather than
  -- silently reported as balanced. Like 'in_review', they always route
  -- regardless of any resolution (the suppression branch below targets only
  -- 'unbalanced'). The distinguishing reconciliation_status is preserved in
  -- the details JSON; no new review-queue reason enum value is introduced
  -- (that would require a migration, which is out of scope for E1).
  --
  -- dismiss_short_pay suppression: unbalanced positions are skipped when
  -- an active dismiss_short_pay resolution exists for the claim.  The
  -- position row in fte_financial_positions is NOT changed — it retains
  -- reconciliation_status = 'unbalanced' and the correct open_balance_amount.
  -- The reviewer decision lives in fte_review_resolutions; the queue entry
  -- is simply not emitted so the claim stops reappearing in the work list.
  --
  -- confirm_short_pay suppression: unbalanced positions are also skipped when
  -- an active confirm_short_pay resolution exists for the claim.  The reviewer
  -- has confirmed the short pay is real/actionable — it no longer needs generic
  -- triage routing.  Unlike dismiss_short_pay, the short_pay_detected event
  -- (Phase 8) is NOT suppressed; only the queue row is suppressed here.
  --
  -- in_review positions are always routed regardless of any resolution.
  -- =========================================================================
  INSERT INTO fte_review_queue
    (practice_id, claim_id, reason, status, details)
  SELECT
    fp.practice_id,
    fp.claim_id,
    'unbalanced_financial_position',
    'open',
    jsonb_build_object(
      'reconciliation_status', fp.reconciliation_status,
      'open_balance',          fp.open_balance_amount
    )
  FROM fte_financial_positions fp
  WHERE fp.practice_id          = p_practice_id
    AND fp.reconciliation_status IN ('unbalanced', 'in_review', 'incomplete')
    AND (
      -- Only suppress queue routing for unbalanced positions;
      -- in_review positions always route regardless of any resolution.
      -- dismiss_short_pay, confirm_short_pay, and mark_position_resolved all
      -- suppress the generic unbalanced-position queue row: dismiss because
      -- the reviewer has decided not to pursue it; confirm because the
      -- reviewer has triaged it and confirmed the short pay is real/actionable;
      -- mark_position_resolved because the reviewer has reviewed the position
      -- and determined it no longer needs generic queue routing (without
      -- committing to dismiss or pursue).  None of these changes Phase 6 math
      -- or the position row itself.  Phase 8 (short_pay_detected) is suppressed
      -- only by dismiss_short_pay — confirm_short_pay and mark_position_resolved
      -- both preserve the event.
      fp.reconciliation_status <> 'unbalanced'
      OR NOT EXISTS (
        SELECT 1
        FROM   _fte_active_resolutions ar
        WHERE  ar.claim_id = fp.claim_id
          AND  ar.action   IN ('dismiss_short_pay', 'confirm_short_pay',
                               'mark_position_resolved')
      )
    );


  -- =========================================================================
  -- PHASE 8: Emit short_pay_detected events for claims with a positive
  -- open balance (reconciliation_status = 'unbalanced').
  --
  -- The short_pay event is linked (derived_from) to the same evidence and
  -- observation that backs the claim_adjudicated event.
  --
  -- dismiss_short_pay suppression: if an active dismiss_short_pay resolution
  -- exists for the claim, the short_pay_detected event is not emitted.
  -- The position row retains reconciliation_status = 'unbalanced' and the
  -- correct open_balance_amount — the math is preserved as financial truth.
  -- Only workflow routing (this event and the Phase 7 queue entry) is
  -- suppressed.  Superseding the dismiss_short_pay row re-enables emission
  -- on the next reconciler run.
  -- =========================================================================
  FOR v_pos IN (
    SELECT fp.*, c.id AS claim_uuid
    FROM fte_financial_positions fp
    JOIN fte_claims c ON c.id = fp.claim_id
    WHERE fp.practice_id          = p_practice_id
      AND fp.reconciliation_status = 'unbalanced'
      AND fp.open_balance_amount IS NOT NULL
      AND fp.open_balance_amount  > 0
      AND NOT EXISTS (
        SELECT 1
        FROM   _fte_active_resolutions ar
        WHERE  ar.claim_id = fp.claim_id
          AND  ar.action   = 'dismiss_short_pay'
      )
  ) LOOP

    -- Use the billed event's date as the short_pay event date.
    SELECT ce.event_date INTO v_event_date
    FROM fte_claim_events ce
    WHERE ce.practice_id = p_practice_id
      AND ce.claim_id    = v_pos.claim_uuid
      AND ce.event_type  = 'claim_adjudicated'
    LIMIT 1;

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_pos.claim_uuid, 'short_pay_detected',
       COALESCE(v_event_date, CURRENT_DATE),
       v_pos.open_balance_amount, 'other',
       'underpayment', v_pos.position_confidence_score, 'unbalanced',
       jsonb_build_object('gap', v_pos.open_balance_amount))
    RETURNING id INTO v_event_id;

    -- Derive the short_pay audit link from the claim_adjudicated evidence chain.
    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    SELECT p_practice_id, v_event_id, ee.evidence_id, ee.observation_id, 'derived_from'
    FROM fte_event_evidence ee
    JOIN fte_claim_events   ce ON ce.id = ee.claim_event_id
    WHERE ce.practice_id = p_practice_id
      AND ce.claim_id    = v_pos.claim_uuid
      AND ce.event_type  = 'claim_adjudicated'
      AND ee.link_role   = 'derived_from'
    LIMIT 1;

  END LOOP;


  -- =========================================================================
  -- PHASE 9: Record analysis run and return summary JSON.
  -- =========================================================================
  SELECT COUNT(*) INTO v_claim_count
  FROM fte_financial_positions WHERE practice_id = p_practice_id;

  SELECT COUNT(*) INTO v_event_count
  FROM fte_claim_events WHERE practice_id = p_practice_id;

  SELECT COUNT(*) INTO v_review_count
  FROM fte_review_queue WHERE practice_id = p_practice_id;

  v_pos_count := v_claim_count;

  INSERT INTO fte_analysis_runs
    (id, practice_id, run_type, status, summary, started_at, finished_at)
  VALUES
    (v_run_id, p_practice_id, 'reconciler', 'succeeded',
     format('%s positions derived, %s events emitted, %s review entries',
            v_pos_count, v_event_count, v_review_count),
     v_started_at, clock_timestamp());

  RETURN jsonb_build_object(
    'run_id',                     v_run_id,
    'practice_id',                p_practice_id,
    'claims_processed',           v_claim_count,
    'events_emitted',             v_event_count,
    'positions_derived',          v_pos_count,
    'review_entries',             v_review_count,
    'review_resolutions_applied', v_resolution_count
  );

END;
$$;

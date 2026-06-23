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
-- 9 phases:
--   0. Idempotent reset
--   1. Classify observations into temp table _fte_classified
--   2. Route non-trusted observations to review queue
--   3. Emit claim_adjudicated events from trusted billed_amount observations
--   4. Emit contractual_adjustment_applied events from trusted
--      contractual_adjustment observations
--   5c. Emit payment_applied events from trusted payment observations
--   5. (late/retry) Wire late_retry review entries to ambiguous payment events
--   6. Derive financial positions
--   7. Route unbalanced / in_review positions to review queue
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
  WHERE cl.classification IN ('excluded', 'suspect');


  -- =========================================================================
  -- PHASE 3: Emit claim_adjudicated events from trusted billed_amount obs.
  --
  -- Each event gets one fte_event_evidence link (derived_from) pointing at
  -- the observation and its source evidence.
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid
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
       v_obs.amount, 'billed', v_obs.payer_name,
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
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid
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
       v_obs.amount, 'contractual_adjustment', v_obs.payer_name, v_obs.carc_code,
       'contractual', v_obs.confidence_score, 'reconciled', '{}')
    RETURNING id INTO v_event_id;

    INSERT INTO fte_event_evidence
      (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    VALUES
      (p_practice_id, v_event_id, v_obs.evidence_id, v_obs.id, 'derived_from');

  END LOOP;


  -- =========================================================================
  -- PHASE 5c: Emit payment_applied events from trusted payment observations.
  --
  -- Each payment event gets two fte_event_evidence links (both link_role=
  -- 'supports'):
  --   (1) the page observation that reported the payment
  --   (2) the check_payment evidence stub matched by check_eft_identifier
  --       (if a matching stub exists; the INSERT is a no-op if not found)
  -- =========================================================================
  FOR v_obs IN (
    SELECT cl.*, c.id AS claim_uuid
    FROM _fte_classified cl
    JOIN fte_claims c
      ON  c.practice_id = p_practice_id
      AND c.claim_number = cl.claim_identifier
    WHERE cl.classification   = 'trusted'
      AND cl.observation_type = 'payment'
  ) LOOP

    INSERT INTO fte_claim_events
      (practice_id, claim_id, event_type, event_date, amount, amount_type,
       payer_name, reason_category, confidence_score, reconciliation_status, metadata)
    VALUES
      (p_practice_id, v_obs.claim_uuid, 'payment_applied', v_obs.service_date,
       v_obs.amount, 'paid', v_obs.payer_name,
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
     billed_amount, contractual_adjustment_amount, paid_amount,
     open_balance_amount, position_confidence_score,
     reconciliation_status, last_reconciled_at)
  SELECT
    p_practice_id,
    c.id,
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'),
    SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),
    -- open_balance: NULL when billed is unknown; GREATEST(0,...) otherwise.
    CASE
      WHEN SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated') IS NULL
        THEN NULL
      ELSE GREATEST(0,
        COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),              0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'), 0)
        - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),               0)
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
    CASE
      WHEN COUNT(ce.id) = 0
        THEN 'in_review'
      WHEN COUNT(ce.id) FILTER (WHERE ce.reconciliation_status = 'ambiguous') > 0
        THEN 'in_review'
      WHEN COUNT(ce.id) FILTER (WHERE ce.reconciliation_status = 'unbalanced') > 0
        THEN 'unbalanced'
      WHEN GREATEST(0,
          COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'claim_adjudicated'),              0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'contractual_adjustment_applied'), 0)
          - COALESCE(SUM(ce.amount) FILTER (WHERE ce.event_type = 'payment_applied'),               0)
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
  -- PHASE 7: Route every unbalanced or in_review position to the review
  -- queue with reason 'unbalanced_financial_position'.
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
    AND fp.reconciliation_status IN ('unbalanced', 'in_review');


  -- =========================================================================
  -- PHASE 8: Emit short_pay_detected events for claims with a positive
  -- open balance (reconciliation_status = 'unbalanced').
  --
  -- The short_pay event is linked (derived_from) to the same evidence and
  -- observation that backs the claim_adjudicated event.
  -- =========================================================================
  FOR v_pos IN (
    SELECT fp.*, c.id AS claim_uuid
    FROM fte_financial_positions fp
    JOIN fte_claims c ON c.id = fp.claim_id
    WHERE fp.practice_id          = p_practice_id
      AND fp.reconciliation_status = 'unbalanced'
      AND fp.open_balance_amount IS NOT NULL
      AND fp.open_balance_amount  > 0
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

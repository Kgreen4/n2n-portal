-- BigQuery VIEW — view_collections_timeline
-- Project:  cardio-metrics-dev
-- Dataset:  billing_audit_practice_test
--
-- Purpose: Tracks the full revenue cycle timeline from DOS to final collection.
-- Used by Looker Page 9: "Collections Timeline — Days DOS→payment by track"
--
-- Three collection tracks:
--   Primary   : DOS → primary ERA payment (check_date in eob_line_items)
--   Secondary : primary payment → secondary billed (date_secondary_billed in charge_report)
--   Patient   : DOS → patient statement (patient_billing_uploads)
--
-- Biller lag flags (submission speed):
--   🟡 SLOW   : >7 days DOS→submission
--   🔴 DELAYED: >14 days DOS→submission
--   ✅ OK     : ≤7 days
--
-- Run in BigQuery console:
--   https://console.cloud.google.com/bigquery?project=cardio-metrics-dev

CREATE OR REPLACE VIEW `cardio-metrics-dev.billing_audit_practice_test.view_collections_timeline` AS

WITH era AS (
  -- One row per claim: take the earliest payment date per claim
  SELECT
    practice_id,
    claim_number,
    patient_name,
    payer_name,
    cpt_code,
    date_of_service,
    claim_status,
    billed_amount,
    paid_amount,
    allowed_amount,
    patient_responsibility,
    -- Primary collection date: prefer payment_date, fall back to check_date
    COALESCE(payment_date, check_date) AS primary_payment_date,
    remark_code,
    npi,
    source_type,
    ingested_at
  FROM `cardio-metrics-dev.billing_audit_practice_test.view_eob_line_items`
),

charges AS (
  SELECT
    practice_id,
    patient_name,
    cpt_code,
    date_of_service,
    date_submitted,
    date_secondary_billed,
    payer,
    billed_amount AS charge_billed_amount
  FROM `cardio-metrics-dev.billing_audit_practice_test.view_charge_report`
)

SELECT
  e.practice_id,
  e.claim_number,
  e.patient_name,
  e.payer_name,
  e.cpt_code,
  e.date_of_service,
  e.claim_status,
  e.billed_amount,
  e.paid_amount,
  e.allowed_amount,
  e.patient_responsibility,
  e.remark_code,
  e.npi,

  -- ── Submission lag (charge report → clearinghouse) ─────────────────────────
  c.date_submitted,
  DATE_DIFF(c.date_submitted, e.date_of_service, DAY) AS submission_lag_days,
  CASE
    WHEN c.date_submitted IS NULL                                            THEN 'NOT_SUBMITTED'
    WHEN DATE_DIFF(c.date_submitted, e.date_of_service, DAY) > 14           THEN 'DELAYED'
    WHEN DATE_DIFF(c.date_submitted, e.date_of_service, DAY) > 7            THEN 'SLOW'
    ELSE 'OK'
  END AS submission_lag_flag,

  -- ── Primary collection lag (DOS → ERA payment) ──────────────────────────────
  e.primary_payment_date,
  DATE_DIFF(e.primary_payment_date, e.date_of_service, DAY) AS primary_collection_lag_days,

  -- ── Secondary billing lag (primary payment → secondary billed) ─────────────
  c.date_secondary_billed,
  DATE_DIFF(c.date_secondary_billed, e.primary_payment_date, DAY) AS secondary_billing_lag_days,

  -- ── Overall status ──────────────────────────────────────────────────────────
  CASE
    WHEN e.claim_status = 'paid' AND c.date_secondary_billed IS NOT NULL THEN 'secondary_billed'
    WHEN e.claim_status = 'paid'                                          THEN 'primary_paid'
    WHEN e.claim_status = 'denied'                                        THEN 'denied'
    ELSE 'pending'
  END AS collection_track,

  e.ingested_at

FROM era e
LEFT JOIN charges c
  ON  c.practice_id    = e.practice_id
  AND c.patient_name   = e.patient_name
  AND c.cpt_code       = e.cpt_code
  AND c.date_of_service = e.date_of_service;

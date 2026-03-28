-- BigQuery Deduplication Views
-- These views sit on top of the raw streaming tables and ensure all downstream
-- queries (Looker, view_revenue_leakage, get-practice-summary) always see exactly
-- one row per unique claim/charge, regardless of streaming insert duplicates.
--
-- Run in BigQuery console: https://console.cloud.google.com/bigquery?project=cardio-metrics-dev

-- ── 1. Deduplicated ERA line items ────────────────────────────────────────────
-- Dedup key: payer_id + claim_number + cpt_code + date_of_service (same as insertId)
-- Tie-break: keep the most recently ingested row (latest ingested_at)
CREATE OR REPLACE VIEW `cardio-metrics-dev.billing_audit_practice_test.view_eob_line_items` AS
SELECT * EXCEPT (row_num)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY payer_id, claim_number, cpt_code, date_of_service
      ORDER BY ingested_at DESC
    ) AS row_num
  FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
)
WHERE row_num = 1;

-- ── 2. Deduplicated charge report ─────────────────────────────────────────────
-- Dedup key: id (SHA-256 of practice_id|account_number|cpt_code|date_of_service)
-- Tie-break: keep the most recently loaded row
CREATE OR REPLACE VIEW `cardio-metrics-dev.billing_audit_practice_test.view_charge_report` AS
SELECT * EXCEPT (row_num)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY loaded_at DESC
    ) AS row_num
  FROM `cardio-metrics-dev.billing_audit_practice_test.charge_report`
)
WHERE row_num = 1;

-- ── 3. Black Hole Detector — updated to use dedup views ───────────────────────
-- Re-creates view_revenue_leakage using the deduped sources above
CREATE OR REPLACE VIEW `cardio-metrics-dev.billing_audit_practice_test.view_revenue_leakage` AS
SELECT
  c.practice_id,
  c.patient_name,
  c.date_of_service,
  c.cpt_code,
  c.billed_amount,
  c.payer,
  c.billing_status,
  c.npi,
  c.source_filename,
  c.loaded_at,
  DATE_DIFF(CURRENT_DATE(), c.date_of_service, DAY) AS days_outstanding,
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), c.date_of_service, DAY) > 270 THEN 'CRITICAL'
    WHEN DATE_DIFF(CURRENT_DATE(), c.date_of_service, DAY) > 90  THEN 'HIGH'
    ELSE 'PENDING'
  END AS urgency_flag
FROM `cardio-metrics-dev.billing_audit_practice_test.view_charge_report` c
WHERE c.practice_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM `cardio-metrics-dev.billing_audit_practice_test.view_eob_line_items` e
    WHERE e.practice_id     = c.practice_id
      AND e.patient_name    = c.patient_name
      AND e.date_of_service = c.date_of_service
      AND e.cpt_code        = c.cpt_code
  );

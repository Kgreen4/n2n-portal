-- ============================================================
-- RUN #8: BigQuery Views for EMR Export and Reconciliation
-- Creates views on eob_line_items to separate payment data
-- from summary totals, and provide reconciliation auditing.
-- ============================================================

-- Phase 2 Schema: Add 835-ready columns to eob_line_items
-- (Run these ALTER TABLE statements once in BigQuery console)
--
-- ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
--   ADD COLUMN IF NOT EXISTS claim_number STRING;
-- ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
--   ADD COLUMN IF NOT EXISTS payment_date DATE;
-- ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
--   ADD COLUMN IF NOT EXISTS payer_name STRING;
-- ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
--   ADD COLUMN IF NOT EXISTS payer_id STRING;
-- ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
--   ADD COLUMN IF NOT EXISTS adjustment_amount FLOAT64;

-- ============================================================
-- VIEW 1: eob_payment_items — EMR-ready payment data
-- Excludes summary_total rows; enriches each line with the
-- check/EFT number and total from the document's summary row.
-- Use this view for 835 file generation and EMR import.
-- ============================================================
CREATE OR REPLACE VIEW
  `cardio-metrics-dev.billing_audit_practice_test.eob_payment_items` AS
SELECT
  li.*,
  st.remark_code    AS check_number,
  st.paid_amount     AS check_total_amount,
  st.cpt_description AS payment_method   -- "Check Total" or "EFT Total"
FROM
  `cardio-metrics-dev.billing_audit_practice_test.eob_line_items` li
LEFT JOIN (
  SELECT
    eob_document_id,
    remark_code,
    paid_amount,
    cpt_description,
    ROW_NUMBER() OVER (PARTITION BY eob_document_id ORDER BY page_number DESC) AS rn
  FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  WHERE line_type = 'summary_total'
) st ON li.eob_document_id = st.eob_document_id AND st.rn = 1
WHERE li.line_type != 'summary_total';


-- ============================================================
-- VIEW 2: eob_reconciliation — Audit/reconciliation dashboard
-- One row per document: compares check total to the sum of all
-- individual line item payments. Flags balanced vs. unbalanced.
-- Uses two-pass ROW_NUMBER dedup to handle streaming buffer
-- duplicates and subtotal phantom rows (same as fetch-line-items).
-- ============================================================
CREATE OR REPLACE VIEW
  `cardio-metrics-dev.billing_audit_practice_test.eob_reconciliation` AS
WITH deduped AS (
  SELECT * EXCEPT(dedup_rn2) FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY
          eob_document_id,
          UPPER(COALESCE(claim_number, '')),
          COALESCE(CAST(date_of_service AS STRING), ''),
          CAST(COALESCE(paid_amount, 0) AS STRING)
        ORDER BY
          confidence_score DESC NULLS LAST,
          contractual_adjustment DESC NULLS LAST,
          billed_amount DESC NULLS LAST
      ) AS dedup_rn2
    FROM (
      SELECT * EXCEPT(dedup_rn) FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY
              eob_document_id,
              UPPER(COALESCE(patient_name, '')),
              UPPER(COALESCE(cpt_code, '')),
              COALESCE(CAST(date_of_service AS STRING), ''),
              CAST(COALESCE(paid_amount, 0) AS STRING)
            ORDER BY
              confidence_score DESC NULLS LAST,
              contractual_adjustment DESC NULLS LAST,
              claim_number DESC NULLS LAST
          ) AS dedup_rn
        FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
        WHERE line_type != 'summary_total'
      )
      WHERE dedup_rn = 1
    )
  )
  WHERE dedup_rn2 = 1
)
SELECT
  d.eob_document_id,
  d.practice_id,
  d.file_name,
  st.remark_code   AS check_number,
  st.paid_amount    AS check_total_amount,
  SUM(d.paid_amount)  AS sum_line_item_payments,
  COUNT(*)             AS line_count,
  st.paid_amount - SUM(d.paid_amount) AS reconciliation_delta,
  CASE
    WHEN st.paid_amount IS NULL THEN 'no_check_total'
    WHEN ABS(st.paid_amount - SUM(d.paid_amount)) < 0.01 THEN 'balanced'
    ELSE 'unbalanced'
  END AS reconciliation_status
FROM deduped d
LEFT JOIN (
  SELECT
    eob_document_id,
    remark_code,
    paid_amount,
    ROW_NUMBER() OVER (PARTITION BY eob_document_id ORDER BY page_number DESC) AS rn
  FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  WHERE line_type = 'summary_total'
) st ON d.eob_document_id = st.eob_document_id AND st.rn = 1
GROUP BY 1, 2, 3, 4, 5;

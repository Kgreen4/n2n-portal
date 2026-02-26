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
-- ============================================================
CREATE OR REPLACE VIEW
  `cardio-metrics-dev.billing_audit_practice_test.eob_reconciliation` AS
SELECT
  li.eob_document_id,
  li.practice_id,
  li.file_name,
  st.remark_code   AS check_number,
  st.paid_amount    AS check_total_amount,
  SUM(li.paid_amount)  AS sum_line_item_payments,
  COUNT(*)             AS line_count,
  st.paid_amount - SUM(li.paid_amount) AS reconciliation_delta,
  CASE
    WHEN st.paid_amount IS NULL THEN 'no_check_total'
    WHEN ABS(st.paid_amount - SUM(li.paid_amount)) < 0.01 THEN 'balanced'
    ELSE 'unbalanced'
  END AS reconciliation_status
FROM
  `cardio-metrics-dev.billing_audit_practice_test.eob_line_items` li
LEFT JOIN (
  SELECT
    eob_document_id,
    remark_code,
    paid_amount,
    ROW_NUMBER() OVER (PARTITION BY eob_document_id ORDER BY page_number DESC) AS rn
  FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  WHERE line_type = 'summary_total'
) st ON li.eob_document_id = st.eob_document_id AND st.rn = 1
WHERE li.line_type != 'summary_total'
GROUP BY 1, 2, 3, 4, 5;

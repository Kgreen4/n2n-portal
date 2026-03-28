-- BigQuery VIEW — view_revenue_leakage (Black Hole Detector)
-- Project:  cardio-metrics-dev
-- Dataset:  billing_audit_practice_test
--
-- Purpose: Finds charges entered in Ethizo (EMR) that have NO matching ERA payment
-- from Trizetto. These are claims that were billed but never responded to by insurance
-- — potentially lost revenue.
--
-- Join key: patient_name + date_of_service + cpt_code + practice_id
-- Urgency flags (Page 8 of Looker report):
--   CRITICAL : >270 days since DOS
--   HIGH     : >90 days since DOS
--   PENDING  : ≤90 days since DOS

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
FROM
  `cardio-metrics-dev.billing_audit_practice_test.charge_report` c
WHERE
  c.practice_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items` e
    WHERE e.practice_id    = c.practice_id
      AND e.patient_name   = c.patient_name
      AND e.date_of_service = c.date_of_service
      AND e.cpt_code       = c.cpt_code
  );

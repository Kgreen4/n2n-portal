-- Add batch_id column to eob_line_items and charge_report
-- Run in BigQuery console: https://console.cloud.google.com/bigquery?project=cardio-metrics-dev

ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  ADD COLUMN IF NOT EXISTS batch_id STRING;

ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.charge_report`
  ADD COLUMN IF NOT EXISTS batch_id STRING;

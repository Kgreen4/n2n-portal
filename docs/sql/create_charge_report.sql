-- BigQuery DDL — create charge_report
-- Project:  cardio-metrics-dev
-- Dataset:  billing_audit_practice_test
--
-- Run in BigQuery console:
--   https://console.cloud.google.com/bigquery?project=cardio-metrics-dev

CREATE TABLE IF NOT EXISTS `cardio-metrics-dev.billing_audit_practice_test.charge_report` (
  id               STRING    NOT NULL,   -- SHA-256 dedup key
  practice_id      STRING,
  patient_name     STRING,
  account_number   STRING,
  date_of_service  DATE,
  cpt_code         STRING,
  billed_amount    FLOAT64,
  billing_status   STRING,
  date_submitted   DATE,
  date_secondary_billed DATE,            -- per CLAUDE.md pending DDL
  secondary_payer  STRING,               -- per CLAUDE.md pending DDL
  payer            STRING,
  provider_name    STRING,
  npi              STRING,
  diagnosis_codes  STRING,
  units            INT64,
  place_of_service STRING,
  source_filename  STRING,
  loaded_at        TIMESTAMP
)
OPTIONS (
  description = "Ethizo charge report. Joined with eob_line_items in view_revenue_leakage to detect claims created in EMR but never sent to clearinghouse."
);

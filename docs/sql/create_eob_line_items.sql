-- BigQuery DDL — create eob_line_items
-- Project:  cardio-metrics-dev
-- Dataset:  billing_audit_practice_test
--
-- Run in BigQuery console:
--   https://console.cloud.google.com/bigquery?project=cardio-metrics-dev
--
-- Paste the statement below into the query editor and click Run.

CREATE TABLE IF NOT EXISTS `cardio-metrics-dev.billing_audit_practice_test.eob_line_items` (
  -- Dedup / identity key
  payer_id          STRING    NOT NULL,
  claim_number      STRING    NOT NULL,
  cpt_code          STRING    NOT NULL,
  date_of_service   DATE      NOT NULL,

  -- Financials
  billed_amount          FLOAT64,
  allowed_amount         FLOAT64,
  paid_amount            FLOAT64,
  adjustment_amount      FLOAT64,
  patient_responsibility FLOAT64,

  -- Claim metadata
  payer_name         STRING,
  patient_name       STRING,
  patient_account    STRING,
  npi                STRING,
  claim_status       STRING,   -- 'paid' | 'denied' | 'partial'
  remark_code        STRING,   -- CO-45, PR-1, CO-4, etc.
  adjustment_reason  STRING,
  check_number       STRING,
  check_date         DATE,
  payment_date       DATE,     -- ERA EFT settlement date (primary timeline field)
  era_transaction_date DATE,

  -- Pipeline metadata
  practice_id  STRING,
  source_type  STRING,         -- 'trizetto_era' | 'pdf_parser'
  ingested_at  TIMESTAMP
)
OPTIONS (
  description = "ERA line items from Trizetto + PDF EOB extractions. Primary source for all AR and denial reporting."
);

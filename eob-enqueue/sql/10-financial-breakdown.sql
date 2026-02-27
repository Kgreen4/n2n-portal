-- ============================================================
-- RUN #10: Financial Breakdown Enhancement (Phase 7)
-- Adds granular adjustment columns for multi-CAS 835 output.
-- Enables proper PR*1 (deductible), PR*2 (coinsurance), PR*3 (copay),
-- and CO*45 (contractual write-off) CAS segments in the 835 file
-- so EMR systems flag patient accounts as "Statement Ready"
-- for the Trizetto billing export.
-- ============================================================

-- Deductible portion of patient responsibility
ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  ADD COLUMN IF NOT EXISTS deductible_amount FLOAT64;

-- Coinsurance portion of patient responsibility
ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  ADD COLUMN IF NOT EXISTS coinsurance_amount FLOAT64;

-- Copay portion of patient responsibility
ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  ADD COLUMN IF NOT EXISTS copay_amount FLOAT64;

-- Contractual write-off (billed minus allowed, CO-45)
ALTER TABLE `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
  ADD COLUMN IF NOT EXISTS contractual_adjustment FLOAT64;

-- NOTE: eob_payment_items view uses li.* so no view update needed.
-- NOTE: patient_responsibility column already exists in the table.

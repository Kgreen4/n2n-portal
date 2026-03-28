-- Phase 1: Financial Pipeline Schema
-- Creates tables for: QuickBooks P&L, QuickSight/Ethizo billing, and dashboard config
-- All tables scoped per-practice for multi-tenant support

-- QuickBooks P&L per month
CREATE TABLE IF NOT EXISTS qb_monthly_summary (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  practice_id uuid REFERENCES practices(id) ON DELETE CASCADE,
  period_month date NOT NULL,                    -- First day of month: e.g. 2026-02-01
  total_income numeric(12,2),
  insurance_income numeric(12,2),
  patient_fees numeric(12,2),
  total_expenses numeric(12,2),
  physician_salary numeric(12,2),
  staff_payroll numeric(12,2),
  payroll_taxes numeric(12,2),
  office_rent numeric(12,2),
  subscriptions numeric(12,2),
  supplies numeric(12,2),
  marketing numeric(12,2),
  bank_charges numeric(12,2),
  other_expenses numeric(12,2),
  net_income numeric(12,2),
  raw_qb_payload jsonb,
  synced_at timestamptz DEFAULT now(),
  UNIQUE(practice_id, period_month)
);

-- QuickSight / Ethizo: patient payment actuals + encounter detail per month
CREATE TABLE IF NOT EXISTS qs_billing_summary (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  practice_id uuid REFERENCES practices(id) ON DELETE CASCADE,
  report_month date NOT NULL,                    -- First day of month: e.g. 2026-02-01
  total_encounters int,
  unique_patients int,
  new_patients int,
  total_charges numeric(12,2),
  total_patient_payments numeric(12,2),          -- ACTUAL patient cash collected (what was PAID)
  total_insurance_payments numeric(12,2),        -- Cross-check vs Trizetto ERAs
  total_patient_balance numeric(12,2),           -- Still owed by patients
  total_insurance_balance numeric(12,2),         -- Still owed by insurance (AR)
  payer_breakdown jsonb,                         -- { "Medicare": { billed, paid, adj }, ... }
  cpt_breakdown jsonb,                           -- { "99214": { count, charges, paid }, ... }
  ar_aging_snapshot jsonb,                       -- { "0-30": 12000, "31-60": 4500, ... }
  ingested_at timestamptz DEFAULT now(),
  UNIQUE(practice_id, report_month)
);

-- User-adjustable projection settings (persisted to Supabase)
-- One row per practice; upserted by the dashboard "Save Settings" button
CREATE TABLE IF NOT EXISTS dashboard_config (
  practice_id uuid REFERENCES practices(id) ON DELETE CASCADE PRIMARY KEY,
  base_monthly_revenue numeric(12,2) DEFAULT 40000,
  monthly_growth_rate numeric(5,4) DEFAULT 0.04,
  collection_rate numeric(5,4) DEFAULT 0.45,
  dr_greatwood_salary numeric(12,2) DEFAULT 300000,
  dr_sharma_salary numeric(12,2) DEFAULT 230000,
  nicole_salary numeric(12,2) DEFAULT 45000,
  est_monthly_encounters int DEFAULT 120,
  rent_current numeric(12,2) DEFAULT 2250,
  rent_new numeric(12,2) DEFAULT 7310,
  rent_change_month date DEFAULT '2026-09-01',
  va_monthly numeric(12,2) DEFAULT 2500,
  admin_monthly numeric(12,2) DEFAULT 3000,
  updated_at timestamptz DEFAULT now()
);

-- Pipeline event log: all sync runs, errors, and completions
-- Used for audit trail and dashboard "last synced" badge
CREATE TABLE IF NOT EXISTS pipeline_events (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  practice_id uuid REFERENCES practices(id) ON DELETE CASCADE,
  event_type text NOT NULL,                      -- e.g. 'era_sync_completed', 'era_sync_error'
  source_system text NOT NULL,                   -- 'trizetto_era', 'pdf_parser', 'quickbooks', 'quicksight'
  records_processed int DEFAULT 0,
  records_inserted int DEFAULT 0,
  records_skipped int DEFAULT 0,
  error_message text,
  metadata jsonb,
  created_at timestamptz DEFAULT now()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_qb_monthly_practice_month ON qb_monthly_summary(practice_id, period_month DESC);
CREATE INDEX IF NOT EXISTS idx_qs_billing_practice_month ON qs_billing_summary(practice_id, report_month DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_practice_type ON pipeline_events(practice_id, event_type, created_at DESC);

-- RLS: practice members can read their own practice data; service role bypasses all
ALTER TABLE qb_monthly_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE qs_billing_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE dashboard_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY "members_read_qb" ON qb_monthly_summary FOR SELECT
  USING (practice_id IN (
    SELECT practice_id FROM practice_members WHERE user_id = auth.uid()
  ));

CREATE POLICY "members_read_qs" ON qs_billing_summary FOR SELECT
  USING (practice_id IN (
    SELECT practice_id FROM practice_members WHERE user_id = auth.uid()
  ));

CREATE POLICY "members_read_config" ON dashboard_config FOR SELECT
  USING (practice_id IN (
    SELECT practice_id FROM practice_members WHERE user_id = auth.uid()
  ));

CREATE POLICY "members_upsert_config" ON dashboard_config FOR ALL
  USING (practice_id IN (
    SELECT practice_id FROM practice_members WHERE user_id = auth.uid()
  ));

CREATE POLICY "members_read_events" ON pipeline_events FOR SELECT
  USING (practice_id IN (
    SELECT practice_id FROM practice_members WHERE user_id = auth.uid()
  ));

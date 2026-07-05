CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS patients (
  id VARCHAR(50) PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  date_of_birth DATE,
  insurance_member_id VARCHAR(50) UNIQUE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS claims (
  id BIGSERIAL PRIMARY KEY,
  claim_number VARCHAR(50) NOT NULL,
  patient_id VARCHAR(50) REFERENCES patients(id) ON DELETE CASCADE,
  provider_name VARCHAR(150),
  date_of_service DATE NOT NULL,
  cpt_hcpcs_code VARCHAR(10),
  service_description TEXT,
  billed_amount NUMERIC(10, 2) DEFAULT 0.00,
  allowed_amount NUMERIC(10, 2) DEFAULT 0.00,
  disallowed_amount NUMERIC(10, 2) DEFAULT 0.00,
  co_pay NUMERIC(10, 2) DEFAULT 0.00,
  deductible NUMERIC(10, 2) DEFAULT 0.00,
  co_insurance NUMERIC(10, 2) DEFAULT 0.00,
  discount_amount NUMERIC(10, 2) DEFAULT 0.00,
  paid_amount NUMERIC(10, 2) DEFAULT 0.00,
  patient_responsibility NUMERIC(10, 2) DEFAULT 0.00,
  explanation_code VARCHAR(50),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_claims_patient_id ON claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_claims_provider_name ON claims(provider_name);
CREATE INDEX IF NOT EXISTS idx_claims_date_of_service ON claims(date_of_service DESC);
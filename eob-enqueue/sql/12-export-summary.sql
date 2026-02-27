-- ============================================================
-- RUN #12: Export Summary Stats — Batch-Level Financials
-- Adds per-document summary stats that are computed during
-- 835 generation and stored for the Export History dashboard.
-- ============================================================

-- Total paid amount across all line items in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_total_paid numeric;

-- Total patient responsibility across all line items
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_total_patient_resp numeric;

-- Number of unique claims in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_claim_count integer;

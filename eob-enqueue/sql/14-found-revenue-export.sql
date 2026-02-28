-- ============================================================
-- RUN #14: Found Revenue Export Stats
-- Adds per-document found revenue (MIPS/incentive) tracking
-- to export summary stats for the Export History dashboard.
-- ============================================================

-- Total found revenue (incentive_bonus) amount in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_found_revenue_amount numeric;

-- Number of incentive_bonus line items in this document
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_found_revenue_count integer;

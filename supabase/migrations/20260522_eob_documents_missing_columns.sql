-- ============================================================
-- Migration: Add missing eob_documents columns
-- Created: 2026-05-22
--
-- These columns were defined in the eob-enqueue/sql/ phase scripts
-- (09, 11, 12, 14) but were never promoted to a Supabase migration.
-- All use IF NOT EXISTS — safe to re-run against a DB that already
-- has some of the columns.
-- ============================================================

-- ── Phase 9: Error Inbox ──────────────────────────────────
-- review_status: 'clear' | 'needs_review' | 'resolved'
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS review_status text DEFAULT 'clear';

-- review_reasons: JSON array of exception tags
-- e.g. ["math_variance", "missing_claim_id", "low_confidence"]
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS review_reasons jsonb DEFAULT '[]'::jsonb;

-- has_found_revenue: true when doc contains incentive_bonus line items
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS has_found_revenue boolean DEFAULT false;

-- Partial index for fast inbox queries (only indexes needs_review rows)
CREATE INDEX IF NOT EXISTS idx_eob_documents_review_status
  ON public.eob_documents (review_status)
  WHERE review_status = 'needs_review';

-- ── Phase 11: Export Tracking ────────────────────────────
-- last_exported_at: timestamp of the most recent 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS last_exported_at timestamptz;

-- export_batch_id: UUID grouping all documents in the same batch export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_batch_id uuid;

CREATE INDEX IF NOT EXISTS idx_eob_documents_exported
  ON public.eob_documents (last_exported_at)
  WHERE last_exported_at IS NULL;

-- ── Phase 12: Export Summary Stats ──────────────────────
-- Total paid amount across all line items in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_total_paid numeric;

-- Total patient responsibility across all line items
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_total_patient_resp numeric;

-- Number of unique claims in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_claim_count integer;

-- ── Phase 14: Found Revenue Export Stats ────────────────
-- Total found revenue (incentive_bonus) amount in this document's 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_found_revenue_amount numeric;

-- Number of incentive_bonus line items in this document
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_found_revenue_count integer;

-- ── Defensive extras (written by eob-enqueue / eob-sweeper) ──
-- These may already exist; IF NOT EXISTS makes this safe either way.
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS pages_capped boolean DEFAULT false;

ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS pages_actual integer;

ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS error_code text;

ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

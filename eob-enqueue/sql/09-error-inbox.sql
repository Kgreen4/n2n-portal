-- ============================================================
-- RUN #9: Error Inbox — Exception Dashboard Schema
-- Adds review_status, review_reasons, and has_found_revenue
-- columns to eob_documents for the Error Inbox feature.
-- ============================================================

-- review_status: 'clear' | 'needs_review' | 'resolved'
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS review_status text DEFAULT 'clear';

-- review_reasons: JSON array of exception tags
-- e.g., ["math_variance", "missing_claim_id", "low_confidence"]
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS review_reasons jsonb DEFAULT '[]'::jsonb;

-- has_found_revenue: true if document contains incentive_bonus line items
-- Used for "Found Revenue" badge in UI without BigQuery round-trip
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS has_found_revenue boolean DEFAULT false;

-- Partial index for fast inbox queries (only indexes needs_review rows)
CREATE INDEX IF NOT EXISTS idx_eob_documents_review_status
  ON public.eob_documents (review_status) WHERE review_status = 'needs_review';

-- Storage policy: Allow authenticated users to read page PDFs
-- for documents belonging to their practice (eob-pages bucket).
CREATE POLICY "Users read own practice page PDFs"
  ON storage.objects FOR SELECT TO authenticated
  USING (
    bucket_id = 'eob-pages'
    AND (storage.foldername(name))[1] IN (
      SELECT d.id::text
      FROM public.eob_documents d
      JOIN public.practice_users pu ON d.practice_id = pu.practice_id
      WHERE pu.user_id = auth.uid()
    )
  );

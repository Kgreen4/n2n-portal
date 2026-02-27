-- ============================================================
-- RUN #11: Export Tracking — Batch 835 Audit Trail
-- Adds last_exported_at and export_batch_id columns to
-- eob_documents for tracking which docs have been exported
-- and which batch they belong to.
-- ============================================================

-- last_exported_at: timestamp of the most recent 835 export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS last_exported_at timestamptz;

-- export_batch_id: UUID grouping all documents in the same batch export
ALTER TABLE public.eob_documents
  ADD COLUMN IF NOT EXISTS export_batch_id uuid;

-- Partial index for fast filtering of unexported documents (the "Ready" queue)
CREATE INDEX IF NOT EXISTS idx_eob_documents_exported
  ON public.eob_documents (last_exported_at) WHERE last_exported_at IS NULL;

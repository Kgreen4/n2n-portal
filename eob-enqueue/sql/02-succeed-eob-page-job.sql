-- ============================================================
-- RUN #2: succeed_eob_page_job (run by itself)
-- Marks a page job as succeeded. When ALL pages for a document
-- are succeeded, auto-completes the parent eob_document and log.
-- Called by eob-worker.js after Gemini extraction + BigQuery insert.
-- ============================================================
CREATE OR REPLACE FUNCTION public.succeed_eob_page_job(
  p_page_job_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.eob_page_jobs
  SET status = 'succeeded', completed_at = now(), updated_at = now()
  WHERE id = p_page_job_id;

  UPDATE public.eob_documents
  SET status = 'completed', updated_at = now()
  WHERE id = (SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id)
  AND NOT EXISTS (
    SELECT 1 FROM public.eob_page_jobs
    WHERE eob_document_id = (SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id)
    AND status NOT IN ('succeeded')
  );

  UPDATE public.eob_processing_logs
  SET status = 'completed', processing_completed_at = now()
  WHERE eob_document_id = (SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id)
  AND NOT EXISTS (
    SELECT 1 FROM public.eob_page_jobs
    WHERE eob_document_id = (SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id)
    AND status NOT IN ('succeeded')
  );
END;
$$;

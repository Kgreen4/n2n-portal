-- ============================================================
-- RUN #2: succeed_eob_page_job (run by itself)
-- Marks a page job as succeeded with audit data. When ALL pages
-- for a document have succeeded (count >= total_pages), auto-
-- completes the parent eob_document and processing log.
-- Called by eob-worker after Gemini extraction + BigQuery insert.
-- ============================================================
CREATE OR REPLACE FUNCTION public.succeed_eob_page_job(
  p_page_job_id uuid,
  p_items_extracted int DEFAULT 0,
  p_gemini_response_type text DEFAULT 'unknown',
  p_gemini_raw_response jsonb DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $fn$
DECLARE
  v_doc_id uuid;
  v_total_pages int;
  v_succeeded_count int;
BEGIN
  -- Step 1: Mark this page job as succeeded and store audit data
  UPDATE public.eob_page_jobs
  SET status = 'succeeded',
      completed_at = now(),
      updated_at = now(),
      items_extracted = p_items_extracted,
      gemini_response_type = p_gemini_response_type,
      gemini_raw_response = p_gemini_raw_response
  WHERE id = p_page_job_id;

  -- Step 2: Get the document ID and total_pages for this job
  SELECT eob_document_id, total_pages
  INTO v_doc_id, v_total_pages
  FROM public.eob_page_jobs
  WHERE id = p_page_job_id;

  -- Step 3: Count succeeded page jobs for this document
  SELECT count(*)
  INTO v_succeeded_count
  FROM public.eob_page_jobs
  WHERE eob_document_id = v_doc_id
    AND status = 'succeeded';

  -- Step 4: Only mark document completed when all pages have succeeded
  IF v_succeeded_count >= v_total_pages THEN
    UPDATE public.eob_documents
    SET status = 'completed', updated_at = now()
    WHERE id = v_doc_id;

    UPDATE public.eob_processing_logs
    SET status = 'completed', processing_completed_at = now()
    WHERE eob_document_id = v_doc_id;
  END IF;
END;
$fn$;

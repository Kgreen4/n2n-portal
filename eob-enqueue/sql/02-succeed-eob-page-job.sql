-- ============================================================
-- RUN #2: succeed_eob_page_job (run by itself)
-- Marks a page job as succeeded with audit data. When ALL pages
-- for a document are terminal (succeeded or permanently failed):
--   - All succeeded → document status = 'completed'
--   - Some failed   → document status = 'partial_failure'
-- Also updates items_extracted on the parent document.
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
  v_terminal_count int;
  v_total_items int;
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

  -- Step 3: Count succeeded and total terminal (succeeded + failed) page jobs
  SELECT
    count(*) FILTER (WHERE status = 'succeeded'),
    count(*) FILTER (WHERE status IN ('succeeded', 'failed'))
  INTO v_succeeded_count, v_terminal_count
  FROM public.eob_page_jobs
  WHERE eob_document_id = v_doc_id;

  -- Step 4: Sum items_extracted across all succeeded pages for this document
  SELECT coalesce(sum(items_extracted), 0)
  INTO v_total_items
  FROM public.eob_page_jobs
  WHERE eob_document_id = v_doc_id AND status = 'succeeded';

  -- Step 5: Update document items_extracted count
  UPDATE public.eob_documents
  SET items_extracted = v_total_items, updated_at = now()
  WHERE id = v_doc_id;

  -- Step 6: When all pages are terminal, determine final document status
  IF v_terminal_count >= v_total_pages THEN
    IF v_succeeded_count >= v_total_pages THEN
      -- All pages succeeded → completed
      UPDATE public.eob_documents
      SET status = 'completed', updated_at = now()
      WHERE id = v_doc_id;

      UPDATE public.eob_processing_logs
      SET status = 'completed', processing_completed_at = now()
      WHERE eob_document_id = v_doc_id;
    ELSE
      -- Some pages permanently failed → partial_failure
      UPDATE public.eob_documents
      SET status = 'partial_failure',
          error_code = 'partial_failure',
          error_message = v_succeeded_count || ' of ' || v_total_pages || ' pages processed. ' || (v_total_pages - v_succeeded_count) || ' pages had errors.',
          updated_at = now()
      WHERE id = v_doc_id;

      UPDATE public.eob_processing_logs
      SET status = 'partial_failure', processing_completed_at = now()
      WHERE eob_document_id = v_doc_id;
    END IF;
  END IF;
END;
$fn$;

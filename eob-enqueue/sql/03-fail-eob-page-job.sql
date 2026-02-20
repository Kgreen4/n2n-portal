-- ============================================================
-- RUN #3: fail_eob_page_job (run by itself)
-- Marks a page job as retryable or permanently failed.
-- Increments attempt_count; if attempt_count >= max_attempts
-- the job is marked 'failed' (permanent), otherwise 'retryable'.
-- When a job becomes permanently 'failed', checks if all pages
-- are now terminal → sets document to 'partial_failure' or 'failed'.
-- Called by eob-worker.js in the catch block.
-- ============================================================
DROP FUNCTION IF EXISTS public.fail_eob_page_job(uuid, text);

CREATE OR REPLACE FUNCTION public.fail_eob_page_job(
  p_page_job_id uuid,
  p_error_message text DEFAULT 'Unknown error'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $fn$
DECLARE
  v_attempt_count int;
  v_max_attempts int;
  v_new_status text;
  v_doc_id uuid;
  v_total_pages int;
  v_succeeded_count int;
  v_terminal_count int;
BEGIN
  -- Step 1: Get current attempt info
  SELECT attempt_count + 1, max_attempts
  INTO v_attempt_count, v_max_attempts
  FROM public.eob_page_jobs WHERE id = p_page_job_id;

  -- Step 2: Determine new status
  v_new_status := CASE WHEN v_attempt_count >= v_max_attempts THEN 'failed' ELSE 'retryable' END;

  -- Step 3: Update the page job
  UPDATE public.eob_page_jobs
  SET status = v_new_status,
      error_message = p_error_message,
      attempt_count = v_attempt_count,
      updated_at = now()
  WHERE id = p_page_job_id;

  -- Step 4: If permanently failed, check if all pages are now terminal
  IF v_new_status = 'failed' THEN
    SELECT eob_document_id, total_pages
    INTO v_doc_id, v_total_pages
    FROM public.eob_page_jobs
    WHERE id = p_page_job_id;

    SELECT
      count(*) FILTER (WHERE status = 'succeeded'),
      count(*) FILTER (WHERE status IN ('succeeded', 'failed'))
    INTO v_succeeded_count, v_terminal_count
    FROM public.eob_page_jobs
    WHERE eob_document_id = v_doc_id;

    IF v_terminal_count >= v_total_pages THEN
      IF v_succeeded_count > 0 THEN
        -- Some pages succeeded, some failed → partial_failure
        UPDATE public.eob_documents
        SET status = 'partial_failure',
            error_code = 'partial_failure',
            error_message = v_succeeded_count || ' of ' || v_total_pages || ' pages processed. ' || (v_total_pages - v_succeeded_count) || ' pages had errors.',
            items_extracted = (SELECT coalesce(sum(items_extracted), 0) FROM public.eob_page_jobs WHERE eob_document_id = v_doc_id AND status = 'succeeded'),
            updated_at = now()
        WHERE id = v_doc_id;

        UPDATE public.eob_processing_logs
        SET status = 'partial_failure', processing_completed_at = now()
        WHERE eob_document_id = v_doc_id;
      ELSE
        -- All pages failed → document failed
        UPDATE public.eob_documents
        SET status = 'failed',
            error_code = 'extraction_failed',
            error_message = 'All ' || v_total_pages || ' pages failed extraction.',
            updated_at = now()
        WHERE id = v_doc_id;

        UPDATE public.eob_processing_logs
        SET status = 'failed', processing_completed_at = now()
        WHERE eob_document_id = v_doc_id;
      END IF;
    END IF;
  END IF;
END;
$fn$;

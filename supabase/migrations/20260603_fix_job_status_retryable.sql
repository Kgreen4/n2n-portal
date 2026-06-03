-- ============================================================
-- Fix: infinite retry loop caused by invalid job_status value
-- ============================================================
--
-- Root cause (discovered 2026-06-03):
--   fail_eob_page_job (p_page_job_id version) sets
--   status = 'retryable' when a page job has retries remaining.
--   'retryable' was NOT a member of the job_status enum, so every
--   UPDATE rolled back silently.  Result: job stayed 'queued' with
--   attempt_count = 0 forever → sweeper re-fired the worker every
--   5 minutes → infinite loop, 39 pages never resolved.
--
-- Fix 1 — add the missing enum value.
-- Fix 2 — recreate fail_eob_page_job to use the correct typed cast
--          and include 'dead' in the terminal-page check so orphaned
--          dead-letter jobs don't block document finalization.
-- ============================================================

-- Fix 1: add 'retryable' to the job_status enum
-- (IF NOT EXISTS guard makes this idempotent)
ALTER TYPE public.job_status ADD VALUE IF NOT EXISTS 'retryable' AFTER 'failed';


-- Fix 2: recreate fail_eob_page_job (p_page_job_id overload)
-- Uses explicit job_status cast so the compiler catches future
-- enum mismatches at function-creation time, not at runtime.
CREATE OR REPLACE FUNCTION public.fail_eob_page_job(
  p_page_job_id   uuid,
  p_error_message text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_attempt_count   int;
  v_max_attempts    int;
  v_new_status      job_status;
  v_doc_id          uuid;
  v_total_pages     int;
  v_succeeded_count int;
  v_terminal_count  int;
BEGIN
  -- Step 1: Increment attempt count
  SELECT attempt_count + 1, max_attempts
  INTO   v_attempt_count, v_max_attempts
  FROM   public.eob_page_jobs
  WHERE  id = p_page_job_id;

  -- Step 2: Determine new status with explicit enum cast
  v_new_status := CASE
    WHEN v_attempt_count >= v_max_attempts THEN 'failed'::job_status
    ELSE 'retryable'::job_status
  END;

  -- Step 3: Update the page job
  UPDATE public.eob_page_jobs
  SET    status        = v_new_status,
         error_message = p_error_message,
         attempt_count = v_attempt_count,
         updated_at    = now()
  WHERE  id = p_page_job_id;

  -- Step 4: Only finalize document when job is permanently failed
  IF v_new_status = 'failed' THEN
    SELECT eob_document_id, total_pages
    INTO   v_doc_id, v_total_pages
    FROM   public.eob_page_jobs
    WHERE  id = p_page_job_id;

    -- Include 'dead' in terminal count so dead-letter jobs don't
    -- block document finalization (Scenario 3 orphan recovery).
    SELECT
      count(*) FILTER (WHERE status = 'succeeded'),
      count(*) FILTER (WHERE status IN ('succeeded', 'failed', 'dead'))
    INTO v_succeeded_count, v_terminal_count
    FROM public.eob_page_jobs
    WHERE eob_document_id = v_doc_id;

    IF v_terminal_count >= v_total_pages THEN
      IF v_succeeded_count > 0 THEN
        -- Partial failure
        UPDATE public.eob_documents
        SET    status           = 'partial_failure',
               error_code       = 'partial_failure',
               error_message    = v_succeeded_count || ' of ' || v_total_pages ||
                                  ' pages processed. ' ||
                                  (v_total_pages - v_succeeded_count) || ' pages had errors.',
               items_extracted  = (
                 SELECT coalesce(sum(items_extracted), 0)
                 FROM   public.eob_page_jobs
                 WHERE  eob_document_id = v_doc_id
                   AND  status = 'succeeded'
               ),
               updated_at       = now()
        WHERE  id = v_doc_id;

        UPDATE public.eob_processing_logs
        SET    status                   = 'partial_failure',
               processing_completed_at  = now()
        WHERE  eob_document_id = v_doc_id;

      ELSE
        -- All pages failed
        UPDATE public.eob_documents
        SET    status        = 'failed',
               error_code    = 'extraction_failed',
               error_message = 'All ' || v_total_pages || ' pages failed extraction.',
               updated_at    = now()
        WHERE  id = v_doc_id;

        UPDATE public.eob_processing_logs
        SET    status                   = 'failed',
               processing_completed_at  = now()
        WHERE  eob_document_id = v_doc_id;
      END IF;
    END IF;
  END IF;
END;
$$;

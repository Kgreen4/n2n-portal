-- ============================================================
-- RUN #3: fail_eob_page_job (run by itself)
-- Marks a page job as failed with an error message.
-- Increments attempt_count for retry tracking.
-- Called by eob-worker.js in the catch block.
-- ============================================================
CREATE OR REPLACE FUNCTION public.fail_eob_page_job(
  p_page_job_id uuid,
  p_error_message text DEFAULT 'Unknown error'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.eob_page_jobs
  SET status = 'failed', error_message = p_error_message,
      attempt_count = attempt_count + 1, updated_at = now()
  WHERE id = p_page_job_id;
END;
$$;

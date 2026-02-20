-- ============================================================
-- RUN #3: fail_eob_page_job (run by itself)
-- Marks a page job as retryable or permanently failed.
-- Increments attempt_count; if attempt_count >= max_attempts
-- the job is marked 'failed' (permanent), otherwise 'retryable'.
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
BEGIN
  SELECT attempt_count + 1, max_attempts
  INTO v_attempt_count, v_max_attempts
  FROM public.eob_page_jobs WHERE id = p_page_job_id;

  UPDATE public.eob_page_jobs
  SET status = CASE WHEN v_attempt_count >= v_max_attempts THEN 'failed' ELSE 'retryable' END,
      error_message = p_error_message,
      attempt_count = v_attempt_count,
      updated_at = now()
  WHERE id = p_page_job_id;
END;
$fn$;

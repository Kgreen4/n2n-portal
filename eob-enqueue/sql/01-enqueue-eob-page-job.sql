-- ============================================================
-- RUN #1: enqueue_eob_page_job (run by itself)
-- Creates a page job row. Uses ON CONFLICT for idempotency —
-- if the same (document, page) already exists, just touches
-- updated_at and returns the existing job ID.
-- ============================================================
CREATE OR REPLACE FUNCTION public.enqueue_eob_page_job(
  p_eob_document_id uuid,
  p_practice_id uuid,
  p_page_number int,
  p_total_pages int,
  p_page_storage_bucket text,
  p_page_storage_path text,
  p_run_after timestamptz DEFAULT now()
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
AS $fn$
DECLARE
  v_job_id uuid;
BEGIN
  INSERT INTO public.eob_page_jobs (
    eob_document_id, practice_id, page_number, total_pages,
    storage_bucket, storage_path, run_after,
    status, max_attempts, attempt_count
  ) VALUES (
    p_eob_document_id, p_practice_id, p_page_number, p_total_pages,
    p_page_storage_bucket, p_page_storage_path, p_run_after,
    'queued', 3, 0
  )
  ON CONFLICT (eob_document_id, page_number) DO UPDATE
    SET updated_at = now()
  RETURNING id INTO v_job_id;
  RETURN v_job_id;
END;
$fn$;

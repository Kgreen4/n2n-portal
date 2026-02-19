-- ============================================================
-- RUN #1: enqueue_eob_page_job (run by itself)
-- Inserts a page job and RETURNS its UUID.
-- Called by eob-enqueue.js for each page after splitting.
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
AS $$
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
  RETURNING id INTO v_job_id;
  RETURN v_job_id;
END;
$$;

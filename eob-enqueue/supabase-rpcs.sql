-- ============================================================
-- Supabase RPCs for EOB Pipeline
-- Run these in the Supabase SQL Editor (Dashboard > SQL Editor)
-- These are CREATE OR REPLACE so they are safe to re-run.
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1) enqueue_eob_page_job — Insert a page job and RETURN its UUID
--    Called by eob-enqueue.js for each page after splitting the PDF.
--    MUST return the job UUID so eob-enqueue can pass it to eob-worker.
-- ──────────────────────────────────────────────────────────────

-- First, ensure the job_status enum type exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
    CREATE TYPE public.job_status AS ENUM ('queued', 'processing', 'succeeded', 'failed');
  END IF;
END
$$;

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


-- ──────────────────────────────────────────────────────────────
-- 2) succeed_eob_page_job — Mark a page job as succeeded
--    Called by eob-worker.js after successful Gemini extraction
--    and BigQuery insert.
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.succeed_eob_page_job(
  p_page_job_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.eob_page_jobs
  SET
    status = 'succeeded',
    completed_at = now(),
    updated_at = now()
  WHERE id = p_page_job_id;

  -- Check if ALL page jobs for this document are now succeeded
  -- If so, mark the parent eob_document as 'completed'
  UPDATE public.eob_documents
  SET
    status = 'completed',
    updated_at = now()
  WHERE id = (
    SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM public.eob_page_jobs
    WHERE eob_document_id = (
      SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id
    )
    AND status NOT IN ('succeeded')
  );

  -- Also update the processing log if all pages are done
  UPDATE public.eob_processing_logs
  SET
    status = 'completed',
    processing_completed_at = now()
  WHERE eob_document_id = (
    SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM public.eob_page_jobs
    WHERE eob_document_id = (
      SELECT eob_document_id FROM public.eob_page_jobs WHERE id = p_page_job_id
    )
    AND status NOT IN ('succeeded')
  );
END;
$$;


-- ──────────────────────────────────────────────────────────────
-- 3) fail_eob_page_job — Mark a page job as failed with error message
--    Called by eob-worker.js in the catch block when extraction fails.
--    Increments attempt_count for retry tracking.
-- ──────────────────────────────────────────────────────────────
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
  SET
    status = 'failed',
    error_message = p_error_message,
    attempt_count = attempt_count + 1,
    updated_at = now()
  WHERE id = p_page_job_id;
END;
$$;


-- ──────────────────────────────────────────────────────────────
-- 4) use_parsing_credit — Charge credits for page processing
--    Updated to accept p_amount (number of pages) for per-page billing.
--    Returns true if credits were successfully charged, false if insufficient.
--
--    Called by eob-enqueue.js with p_amount = totalPages.
--    (Also supports legacy call without p_amount, defaults to 1)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.use_parsing_credit(
  p_practice_id uuid,
  p_amount int DEFAULT 1
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_balance int;
BEGIN
  -- Lock the row to prevent race conditions
  SELECT credit_balance INTO v_balance
  FROM public.practice_credits
  WHERE id = p_practice_id
  FOR UPDATE;

  IF v_balance IS NULL THEN
    RAISE EXCEPTION 'Practice not found: %', p_practice_id;
  END IF;

  IF v_balance < p_amount THEN
    RETURN false;
  END IF;

  UPDATE public.practice_credits
  SET
    credit_balance = credit_balance - p_amount,
    updated_at = now()
  WHERE id = p_practice_id;

  RETURN true;
END;
$$;


-- ──────────────────────────────────────────────────────────────
-- 5) refund_parsing_credit — Refund credits on failure
--    Called by eob-enqueue.js when an error occurs after credits
--    were charged. Best-effort; failures are logged but not fatal.
--
--    NOTE: This does a simple +1 refund. If per-page charging was
--    used, you may want to track the exact amount charged and refund
--    that. For now, this is a safety net.
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refund_parsing_credit(
  practice_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.practice_credits
  SET
    credit_balance = credit_balance + 1,
    updated_at = now()
  WHERE id = practice_id;
END;
$$;


-- ============================================================
-- TABLE: eob_page_jobs (create if it doesn't exist)
-- This table tracks individual page processing jobs.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.eob_page_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  eob_document_id uuid NOT NULL REFERENCES public.eob_documents(id),
  practice_id uuid NOT NULL,
  page_number int NOT NULL,
  total_pages int NOT NULL,
  storage_bucket text,
  storage_path text,
  status text DEFAULT 'queued',
  run_after timestamptz DEFAULT now(),
  max_attempts int DEFAULT 3,
  attempt_count int DEFAULT 0,
  error_message text,
  completed_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Index for fast lookups by document
CREATE INDEX IF NOT EXISTS idx_eob_page_jobs_document
  ON public.eob_page_jobs (eob_document_id, page_number);

-- Index for finding queued/failed jobs (for retry logic)
CREATE INDEX IF NOT EXISTS idx_eob_page_jobs_status
  ON public.eob_page_jobs (status, run_after);


-- ============================================================
-- TABLE: practice_credits (create if it doesn't exist)
-- Tracks credit balance per practice for pay-per-page billing.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.practice_credits (
  id uuid PRIMARY KEY,
  credit_balance int DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);


-- ============================================================
-- TABLE: eob_processing_logs (create if it doesn't exist)
-- Audit trail for PDF processing handoffs.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.eob_processing_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  practice_id uuid NOT NULL,
  eob_document_id uuid,
  gcs_object_name text,
  status text DEFAULT 'pending',
  credits_used int DEFAULT 0,
  error_message text,
  processing_completed_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

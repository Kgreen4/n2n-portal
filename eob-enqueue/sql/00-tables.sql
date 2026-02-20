-- ============================================================
-- RUN #0: Create tables (run this FIRST, by itself)
-- ============================================================

-- eob_page_jobs — tracks individual page processing jobs
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
  items_extracted int,
  gemini_response_type text,
  gemini_raw_response jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  CONSTRAINT uq_eob_page_jobs_doc_page UNIQUE (eob_document_id, page_number)
);

CREATE INDEX IF NOT EXISTS idx_eob_page_jobs_status
  ON public.eob_page_jobs (status, run_after);

-- practice_credits — credit balance per practice
CREATE TABLE IF NOT EXISTS public.practice_credits (
  practice_id uuid PRIMARY KEY,
  credits_remaining int DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- eob_processing_logs — audit trail for PDF handoffs
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

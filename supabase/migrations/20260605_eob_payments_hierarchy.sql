-- =============================================================================
-- Migration: eob_payments hierarchy
-- Creates the authoritative check/EFT register (one row per check per document)
-- and links eob_line_items to it via FK + denormalized check_number column.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Create eob_payments
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.eob_payments (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  eob_document_id UUID        NOT NULL REFERENCES public.eob_documents(id) ON DELETE CASCADE,
  practice_id     UUID        NOT NULL REFERENCES public.practices(id),
  check_number    TEXT,
  payment_date    DATE,
  payer_name      TEXT,
  payer_id        TEXT,
  check_amount    NUMERIC(10,2),
  page_number     INT,
  created_at      TIMESTAMPTZ DEFAULT now(),

  -- One row per check per document; parallel page workers upsert safely
  UNIQUE (eob_document_id, check_number)
);

-- ---------------------------------------------------------------------------
-- 2. RLS
-- ---------------------------------------------------------------------------
ALTER TABLE public.eob_payments ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Practice members can read their eob_payments"
  ON public.eob_payments FOR SELECT
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "Service role can manage eob_payments"
  ON public.eob_payments FOR ALL
  USING (auth.role() = 'service_role');

-- ---------------------------------------------------------------------------
-- 3. Add eob_payment_id FK to eob_line_items
--    source_check_number already exists (migration 20260601120000) — reuse it.
--    eob_payment_id — FK to eob_payments; SET NULL on parent delete so legacy
--                     rows without a parent record are preserved, not deleted.
-- ---------------------------------------------------------------------------
ALTER TABLE public.eob_line_items
  ADD COLUMN IF NOT EXISTS eob_payment_id UUID
    REFERENCES public.eob_payments(id) ON DELETE SET NULL;

-- ---------------------------------------------------------------------------
-- 4. Index for FK look-ups and report joins
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_eob_payments_document
  ON public.eob_payments(eob_document_id);

CREATE INDEX IF NOT EXISTS idx_eob_payments_practice
  ON public.eob_payments(practice_id);

CREATE INDEX IF NOT EXISTS idx_eob_line_items_payment_id
  ON public.eob_line_items(eob_payment_id);

-- source_check_number index already exists from 20260601120000 migration

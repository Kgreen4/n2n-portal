-- Flat line items table in Supabase for in-app reporting
-- Populated by eob-worker alongside the BigQuery insert

CREATE TABLE IF NOT EXISTS public.eob_line_items (
  id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  eob_document_id         uuid NOT NULL REFERENCES public.eob_documents(id) ON DELETE CASCADE,
  practice_id             uuid NOT NULL REFERENCES public.practices(id),
  page_number             integer NOT NULL,
  file_name               text,
  line_type               text,  -- medical_service, incentive_bonus, adjustment, summary_total
  patient_name            text,
  member_id               text,
  date_of_service         date,
  cpt_code                text,
  cpt_description         text,
  billed_amount           numeric(12,2),
  allowed_amount          numeric(12,2),
  paid_amount             numeric(12,2),
  patient_responsibility  numeric(12,2),
  rendering_provider_npi  text,
  remark_code             text,
  remark_reason           text,
  remark_description      text,
  claim_status            text,
  claim_number            text,
  payment_date            date,
  payer_name              text,
  payer_id                text,
  adjustment_amount       numeric(12,2),
  deductible_amount       numeric(12,2),
  coinsurance_amount      numeric(12,2),
  copay_amount            numeric(12,2),
  contractual_adjustment  numeric(12,2),
  non_covered_amount      numeric(12,2),
  confidence_score        integer,
  created_at              timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_eob_li_practice_id      ON public.eob_line_items(practice_id);
CREATE INDEX IF NOT EXISTS idx_eob_li_eob_document_id  ON public.eob_line_items(eob_document_id);
CREATE INDEX IF NOT EXISTS idx_eob_li_payment_date     ON public.eob_line_items(payment_date);
CREATE INDEX IF NOT EXISTS idx_eob_li_payer_name       ON public.eob_line_items(payer_name);
CREATE INDEX IF NOT EXISTS idx_eob_li_claim_status     ON public.eob_line_items(claim_status);
CREATE INDEX IF NOT EXISTS idx_eob_li_created_at       ON public.eob_line_items(created_at);

ALTER TABLE public.eob_line_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Practice members can read their line items"
  ON public.eob_line_items FOR SELECT
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid()
    )
  );

CREATE POLICY "Service role full access to line items"
  ON public.eob_line_items FOR ALL
  USING (auth.role() = 'service_role')
  WITH CHECK (auth.role() = 'service_role');

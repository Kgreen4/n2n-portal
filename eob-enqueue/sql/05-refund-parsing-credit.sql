-- ============================================================
-- RUN #5: refund_parsing_credit (run by itself)
-- FIXED: Uses actual column names: practice_id, credits_remaining
-- ============================================================
CREATE OR REPLACE FUNCTION public.refund_parsing_credit(
  p_practice_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.practice_credits
  SET credits_remaining = credits_remaining + 1, updated_at = now()
  WHERE practice_id = p_practice_id;
END;
$$;

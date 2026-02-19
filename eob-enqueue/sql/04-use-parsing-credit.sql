-- ============================================================
-- RUN #4: use_parsing_credit (run by itself)
-- FIXED: Uses actual column names: practice_id, credits_remaining
-- (not id, credit_balance)
-- ============================================================
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
  SELECT credits_remaining INTO v_balance
  FROM public.practice_credits
  WHERE practice_id = p_practice_id
  FOR UPDATE;

  IF v_balance IS NULL THEN
    RAISE EXCEPTION 'Practice not found: %', p_practice_id;
  END IF;

  IF v_balance < p_amount THEN
    RETURN false;
  END IF;

  UPDATE public.practice_credits
  SET credits_remaining = credits_remaining - p_amount, updated_at = now()
  WHERE practice_id = p_practice_id;

  RETURN true;
END;
$$;

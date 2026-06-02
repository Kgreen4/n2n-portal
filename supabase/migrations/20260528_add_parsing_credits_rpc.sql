-- Migration: add_parsing_credits RPC
-- Used by reprocess-document to refund the original page-count credits
-- before re-enqueueing, preventing double-charging on reprocess.
--
-- The existing refund_parsing_credit() only refunds 1 credit at a time;
-- this function refunds any amount atomically.

CREATE OR REPLACE FUNCTION public.add_parsing_credits(p_practice_id uuid, p_amount integer)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE public.practice_credits
  SET credits_remaining = credits_remaining + p_amount,
      updated_at = now()
  WHERE practice_id = p_practice_id;
END;
$$;

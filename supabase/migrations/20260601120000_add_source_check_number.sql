-- Add source_check_number to eob_line_items for document-level check stitching.
-- Populated by finalize-document edge function after all pages are extracted.
-- Enables check-level gap analysis: join service lines → summary_total → bank deposit
-- by a common check/EFT identifier even when the check stub appears on a different page.

ALTER TABLE public.eob_line_items
  ADD COLUMN IF NOT EXISTS source_check_number text;

COMMENT ON COLUMN public.eob_line_items.source_check_number IS
  'Check or EFT trace number this row belongs to, resolved order-agnostically '
  'by finalize-document across all pages of the parent EOB document.';

CREATE INDEX IF NOT EXISTS idx_eob_li_source_check_number
  ON public.eob_line_items(source_check_number)
  WHERE source_check_number IS NOT NULL;

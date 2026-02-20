-- ============================================================
-- RUN #6: practices + practice_users (multi-tenancy foundation)
-- Creates the core tenant tables for multi-practice support.
-- practices: one row per dental/medical practice
-- practice_users: junction table linking auth.users to practices
-- ============================================================

-- practices — core tenant record
CREATE TABLE IF NOT EXISTS public.practices (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  created_at timestamptz DEFAULT now()
);

-- Enable RLS
ALTER TABLE public.practices ENABLE ROW LEVEL SECURITY;

-- practice_users — links auth.users to practices with role
CREATE TABLE IF NOT EXISTS public.practice_users (
  practice_id uuid NOT NULL REFERENCES public.practices(id),
  user_id uuid NOT NULL REFERENCES auth.users(id),
  role text DEFAULT 'member' CHECK (role IN ('owner','admin','member','viewer')),
  PRIMARY KEY (practice_id, user_id)
);

-- Enable RLS
ALTER TABLE public.practice_users ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- Also added to eob_documents (ALTER TABLE):
--   uploaded_by uuid REFERENCES auth.users(id)
--   total_pages int
--   items_extracted int DEFAULT 0
--   error_code text
--   check_number text
--   total_payment_amount numeric
-- ============================================================

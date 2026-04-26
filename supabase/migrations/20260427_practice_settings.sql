-- Codify practice_settings table (table already exists in production;
-- this migration makes the schema reproducible).
CREATE TABLE IF NOT EXISTS public.practice_settings (
  practice_id                uuid PRIMARY KEY REFERENCES public.practices(id) ON DELETE CASCADE,
  gdrive_folder_id           text,
  gdrive_folder_name         text,
  gdrive_processed_folder_id text,
  watcher_enabled            boolean     DEFAULT false,
  watcher_interval_minutes   integer     DEFAULT 5,
  auto_move_processed        boolean     DEFAULT true,
  updated_at                 timestamptz DEFAULT now()
);

-- RLS: users can only see settings for their own practice
ALTER TABLE public.practice_settings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "practice_settings_select_own" ON public.practice_settings;
CREATE POLICY "practice_settings_select_own"
  ON public.practice_settings FOR SELECT
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS "practice_settings_upsert_owner" ON public.practice_settings;
CREATE POLICY "practice_settings_upsert_owner"
  ON public.practice_settings FOR ALL
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role IN ('owner', 'admin')
    )
  );

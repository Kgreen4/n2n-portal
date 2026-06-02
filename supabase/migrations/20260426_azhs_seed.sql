-- Seed: Arizona Heart Specialists (AZHS) — new client
-- Fixed UUID ensures idempotent re-runs across environments.
DO $$
DECLARE
  azhs_id uuid := 'aa000001-0000-4000-8000-000000000001';
BEGIN
  INSERT INTO public.practices (id, name, slug)
  VALUES (azhs_id, 'Arizona Heart Specialists', 'azhs')
  ON CONFLICT DO NOTHING;

  INSERT INTO public.practice_settings (
    practice_id, gdrive_folder_id, gdrive_folder_name, watcher_enabled
  )
  VALUES (azhs_id, '1qTu7ZVVCxpmAPp-jyustCj5bmNWAuSMJ', 'AZHS EOB Files', false)
  ON CONFLICT (practice_id) DO UPDATE
    SET gdrive_folder_id   = EXCLUDED.gdrive_folder_id,
        gdrive_folder_name = EXCLUDED.gdrive_folder_name,
        updated_at         = now();
END $$;

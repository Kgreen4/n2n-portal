-- Schedule the eob-sweeper edge function to run every 5 minutes via pg_cron + pg_net.
-- pg_cron and pg_net are both available on Supabase projects.
--
-- This recovers stuck page jobs automatically, eliminating the need for
-- manual intervention or an n8n scheduled workflow.

-- Unschedule any previous version first (idempotent — no error if job doesn't exist)
DO $$
BEGIN
  PERFORM cron.unschedule('eob-sweeper-every-5min');
EXCEPTION WHEN others THEN NULL;
END $$;

-- Schedule sweeper every 5 minutes
-- NOTE: service role key is embedded here for server-side-only pg_cron execution.
-- This SQL runs inside Supabase's private DB — it is never exposed to clients.
SELECT cron.schedule(
  'eob-sweeper-every-5min',
  '*/5 * * * *',
  $cron$
  SELECT net.http_post(
    url     := 'https://jdmyjdvricpyrsfchakk.supabase.co/functions/v1/eob-sweeper',
    headers := '{"Content-Type":"application/json","Authorization":"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpkbXlqZHZyaWNweXJzZmNoYWtrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2OTQ2ODU0NSwiZXhwIjoyMDg1MDQ0NTQ1fQ.vkCwde5l8Nbm0bflbK_AKxreqG9lfBWTfRoVAMpl93c"}'::jsonb,
    body    := '{}'::jsonb
  ) AS request_id;
  $cron$
);

-- Verify the schedule was created
SELECT jobid, jobname, schedule, active
FROM cron.job
WHERE jobname = 'eob-sweeper-every-5min';

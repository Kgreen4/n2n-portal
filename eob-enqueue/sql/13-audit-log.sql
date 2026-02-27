-- ============================================================
-- RUN #13: Security Audit Log
-- HIPAA-compliant audit trail for all user actions.
-- Tracks document uploads, views, edits, exports, and unlocks.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.security_audit_log (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id uuid,
  practice_id uuid,
  action text NOT NULL,          -- 'document.upload', 'document.view', 'document.export',
                                 -- 'document.edit', 'document.unlock', 'user.login'
  resource_type text,            -- 'eob_document', 'practice', 'line_item'
  resource_id text,              -- UUID of the affected resource
  metadata jsonb DEFAULT '{}',   -- extra context (batch_id, file_name, doc_count, user_agent)
  created_at timestamptz DEFAULT now()
);

-- RLS: practice members can read their own audit log
ALTER TABLE public.security_audit_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Practice members read own audit log"
  ON public.security_audit_log FOR SELECT
  USING (practice_id IN (
    SELECT practice_id FROM practice_users WHERE user_id = auth.uid()
  ));

-- Allow inserts from any authenticated user
CREATE POLICY "Authenticated users insert audit entries"
  ON public.security_audit_log FOR INSERT
  WITH CHECK (true);

-- Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_audit_log_practice_date
  ON security_audit_log(practice_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_log_action
  ON security_audit_log(action, created_at DESC);

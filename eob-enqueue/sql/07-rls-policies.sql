-- ============================================================
-- RUN #7: RLS policies for user-scoped access
-- All policies use the practice_users junction table to scope
-- data by practice. service_role (edge functions) bypasses RLS.
-- ============================================================

-- ============================================================
-- practices
-- ============================================================
CREATE POLICY "Users see own practices"
  ON public.practices FOR SELECT
  USING (
    id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Authenticated users can create practices"
  ON public.practices FOR INSERT
  WITH CHECK (auth.uid() IS NOT NULL);

CREATE POLICY "Owners and admins can update practices"
  ON public.practices FOR UPDATE
  USING (
    id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role IN ('owner', 'admin')
    )
  )
  WITH CHECK (
    id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role IN ('owner', 'admin')
    )
  );

-- ============================================================
-- practice_users
-- ============================================================
CREATE POLICY "Users see own memberships"
  ON public.practice_users FOR SELECT
  USING (user_id = auth.uid());

CREATE POLICY "Owners and admins can add members"
  ON public.practice_users FOR INSERT
  WITH CHECK (
    practice_id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role IN ('owner', 'admin')
    )
    OR user_id = auth.uid()  -- self-insert for new practice onboarding
  );

CREATE POLICY "Owners can update member roles"
  ON public.practice_users FOR UPDATE
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role = 'owner'
    )
  )
  WITH CHECK (
    practice_id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role = 'owner'
    )
  );

CREATE POLICY "Owners can remove members"
  ON public.practice_users FOR DELETE
  USING (
    practice_id IN (
      SELECT practice_id FROM public.practice_users
      WHERE user_id = auth.uid() AND role = 'owner'
    )
  );

-- ============================================================
-- eob_documents
-- ============================================================
CREATE POLICY "Users see own practice documents"
  ON public.eob_documents FOR SELECT
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Users can create documents for own practice"
  ON public.eob_documents FOR INSERT
  WITH CHECK (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Users can update own practice documents"
  ON public.eob_documents FOR UPDATE
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  )
  WITH CHECK (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

-- ============================================================
-- eob_page_jobs
-- ============================================================
CREATE POLICY "Users see own practice page jobs"
  ON public.eob_page_jobs FOR SELECT
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Users can create page jobs for own practice"
  ON public.eob_page_jobs FOR INSERT
  WITH CHECK (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Users can update own practice page jobs"
  ON public.eob_page_jobs FOR UPDATE
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  )
  WITH CHECK (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

-- ============================================================
-- eob_processing_logs
-- ============================================================
CREATE POLICY "Users see own practice processing logs"
  ON public.eob_processing_logs FOR SELECT
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

CREATE POLICY "Users can create logs for own practice"
  ON public.eob_processing_logs FOR INSERT
  WITH CHECK (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

-- ============================================================
-- practice_credits (read-only for users; managed by service_role)
-- ============================================================
CREATE POLICY "Users see own practice credits"
  ON public.practice_credits FOR SELECT
  USING (
    practice_id IN (SELECT practice_id FROM public.practice_users WHERE user_id = auth.uid())
  );

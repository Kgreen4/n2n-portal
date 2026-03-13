-- View: practice_members
-- Joins practice_users with auth.users and practices so you can see
-- who has access to each practice without decoding UUIDs manually.
-- Visible in Supabase Table Editor as a queryable view.

CREATE OR REPLACE VIEW public.practice_members AS
SELECT
  p.name                                                          AS practice_name,
  p.slug                                                          AS practice_slug,
  pu.role,
  u.email,
  COALESCE(
    u.raw_user_meta_data->>'full_name',
    u.raw_user_meta_data->>'name',
    u.email
  )                                                               AS full_name,
  pu.practice_id,
  pu.user_id,
  p.trial_ends_at,
  p.created_at                                                    AS practice_created_at
FROM  public.practice_users  pu
JOIN  auth.users             u  ON u.id = pu.user_id
JOIN  public.practices       p  ON p.id = pu.practice_id
ORDER BY p.name, pu.role;

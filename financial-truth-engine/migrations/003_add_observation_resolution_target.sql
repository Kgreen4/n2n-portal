-- =============================================================================
-- Financial Truth Engine (FTE) — Observation Resolution Constraints
-- Migration: 003_add_observation_resolution_target.sql
-- Depends on: 002_add_review_resolutions.sql
-- Created: 2026-06-22
--
-- WHAT THIS MIGRATION ADDS
-- -----------------------------------------------------------------------------
-- 1. target_observation_id column — typed FK to fte_observations.
--    Required for mark_duplicate to record the canonical observation.
--    NULL for all other action types (enforced by constraint #5 below).
--    Hard FK to fte_observations, which is a stable entity table not touched
--    by Phase 0.
--
-- 2. Five CHECK constraints that together enforce valid shape for the three
--    observation-level resolution actions:
--
--    For action IN ('confirm_observation', 'reject_observation', 'mark_duplicate'):
--      #1  observation_id   IS NOT NULL      (the observation being resolved)
--      #2  target_type      = 'observation'  (semantic type guard)
--
--    For action = 'mark_duplicate' specifically:
--      #3  target_observation_id IS NOT NULL  (the canonical observation; already
--                                              implied by #4 below but stated
--                                              explicitly for clarity)
--      #4  target_observation_id <> observation_id  (cannot be a duplicate of itself)
--
--    For all OTHER actions:
--      #5  target_observation_id IS NULL     (prevents misuse of the new column
--                                             for actions that have no canonical
--                                             observation concept)
--
-- 3. Partial index for reverse lookup: "which observations have been marked as
--    duplicates of canonical X?"
-- =============================================================================

begin;

alter table fte_review_resolutions
  add column target_observation_id uuid
    references fte_observations(id) on delete restrict;

comment on column fte_review_resolutions.target_observation_id is
  'For mark_duplicate resolutions: the canonical observation that the flagged '
  'observation is a duplicate of. Hard FK to fte_observations (stable entity, '
  'not touched by Phase 0). NULL for all non-mark_duplicate action types — '
  'enforced by constraint fte_review_resolutions_non_duplicate_no_target.';

-- CONSTRAINT #1: observation-level actions require observation_id to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_obs_action_needs_obs_id
    check (
      action NOT IN ('confirm_observation', 'reject_observation', 'mark_duplicate')
      OR observation_id IS NOT NULL
    );

-- CONSTRAINT #2: observation-level actions require target_type = 'observation'.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_obs_action_needs_observation_type
    check (
      action NOT IN ('confirm_observation', 'reject_observation', 'mark_duplicate')
      OR target_type = 'observation'
    );

-- CONSTRAINT #3: mark_duplicate requires a canonical target observation.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_duplicate_needs_target
    check (
      action <> 'mark_duplicate'
      OR target_observation_id IS NOT NULL
    );

-- CONSTRAINT #4: an observation cannot be marked as a duplicate of itself.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_duplicate_no_self
    check (
      action <> 'mark_duplicate'
      OR target_observation_id <> observation_id
    );

-- CONSTRAINT #5: only mark_duplicate may populate target_observation_id.
-- Keeps the column from being silently misused for other action types.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_non_duplicate_no_target
    check (
      action = 'mark_duplicate'
      OR target_observation_id IS NULL
    );

-- Support reverse lookup: "which observations have been marked as duplicates
-- of canonical X?" Partial index keeps it lean for the common NULL case.
create index idx_fte_resolutions_target_observation
  on fte_review_resolutions (practice_id, target_observation_id)
  where target_observation_id is not null;

-- =============================================================================
-- End of migration 003.
-- =============================================================================

commit;

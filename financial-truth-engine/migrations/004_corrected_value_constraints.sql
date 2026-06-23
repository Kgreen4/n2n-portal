-- =============================================================================
-- Financial Truth Engine (FTE) — Corrected-Value Constraints
-- Migration: 004_corrected_value_constraints.sql
-- Depends on: 003_add_observation_resolution_target.sql
-- Created: 2026-06-23
--
-- WHAT THIS MIGRATION ADDS
-- -----------------------------------------------------------------------------
-- 1. Four CHECK constraints that enforce valid shape for the
--    attach_corrected_value action:
--
--    #1  observation_id IS NOT NULL   (the observation being corrected)
--    #2  target_type = 'observation'  (semantic type guard)
--    #3  corrected_value IS NOT NULL  (the replacement amount must be supplied)
--    #4  corrected_value >= 0         (negative corrections are invalid)
--
-- 2. Unique partial index on (practice_id, observation_id, action) filtered
--    to is_superseded = false AND action = 'attach_corrected_value'.
--    Enforces at most one active correction per observation at the DB level.
--    To supersede a correction: set is_superseded = true on the old row,
--    then insert a new row.  The index permits multiple historical corrections
--    (is_superseded = true) but rejects a second is_superseded = false row.
-- =============================================================================

begin;

-- CONSTRAINT #1: attach_corrected_value requires observation_id to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_obs_id
    check (
      action <> 'attach_corrected_value'
      OR observation_id IS NOT NULL
    );

-- CONSTRAINT #2: attach_corrected_value requires target_type = 'observation'.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_observation_type
    check (
      action <> 'attach_corrected_value'
      OR target_type = 'observation'
    );

-- CONSTRAINT #3: attach_corrected_value requires corrected_value to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_needs_corrected_value
    check (
      action <> 'attach_corrected_value'
      OR corrected_value IS NOT NULL
    );

-- CONSTRAINT #4: attach_corrected_value corrected_value must be non-negative.
-- A zero correction is permitted (full write-off scenario).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_cv_action_value_nonnegative
    check (
      action <> 'attach_corrected_value'
      OR corrected_value >= 0
    );

-- UNIQUE PARTIAL INDEX: at most one active correction per observation.
-- Permits any number of is_superseded = true historical rows for the same
-- observation; rejects a second is_superseded = false row.
create unique index idx_fte_resolutions_single_active_correction
  on fte_review_resolutions (practice_id, observation_id, action)
  where is_superseded = false
    and action = 'attach_corrected_value';

-- =============================================================================
-- End of migration 004.
-- =============================================================================

commit;

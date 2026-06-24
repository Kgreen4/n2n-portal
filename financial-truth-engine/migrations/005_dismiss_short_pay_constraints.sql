-- =============================================================================
-- Migration 005: dismiss_short_pay shape constraints
-- Financial Truth Engine
--
-- Adds two CHECK constraints to fte_review_resolutions that enforce the
-- correct shape for the dismiss_short_pay position-level review action.
--
-- Follows the same pattern as migrations 003 and 004:
--   - A constraint guards a required field for the specific action.
--   - All other actions are unaffected (action <> 'dismiss_short_pay' OR ...).
--
-- No new columns, no new indexes, no new enum values.
--
-- Why claim_id is required:
--   claim_id is the only stable identifier that Phase 0 never deletes.
--   source_position_id is a plain uuid snapshot field (no REFERENCES clause)
--   and becomes stale after a reprocess. Phase 7 and Phase 8 look up active
--   dismiss_short_pay resolutions by claim_id, not by position ID.
--
-- Why target_type = 'position' is required:
--   dismiss_short_pay targets a fte_financial_positions row, not an
--   observation or payment event. The constraint encodes this semantic.
-- =============================================================================

-- CONSTRAINT 1: dismiss_short_pay requires a stable claim anchor.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_dismiss_shortpay_needs_claim_id
    check (action <> 'dismiss_short_pay' or claim_id is not null);

-- CONSTRAINT 2: dismiss_short_pay must target a position, not an observation
-- or event.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_dismiss_shortpay_needs_position_type
    check (action <> 'dismiss_short_pay' or target_type = 'position');

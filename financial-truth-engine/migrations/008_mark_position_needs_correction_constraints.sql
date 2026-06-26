-- =============================================================================
-- Financial Truth Engine — Migration 008
-- migrations/008_mark_position_needs_correction_constraints.sql
--
-- Adds DB-level enforcement for the mark_position_needs_correction position-level
-- review resolution action that is already present in the migration 002 action
-- vocabulary (position-level group, action #12 of 15).
--
-- Objects added (no new columns, no new tables, no new status values,
-- no new queue reasons, no new event types):
--   1. CHECK constraint: notes IS NOT NULL and btrim(notes) <> ''
--      when action = 'mark_position_needs_correction'
--   2. CHECK constraint: claim_id IS NOT NULL
--      when action = 'mark_position_needs_correction'
--   3. CHECK constraint: target_type = 'position'
--      when action = 'mark_position_needs_correction'
--   4. Partial unique index: at most one active correction-needed marker per
--      (practice_id, claim_id) where is_superseded = false
--      and action = 'mark_position_needs_correction'
--
-- Why notes is required:
--   A correction-needed marker without a written explanation of what is wrong
--   and why the position needs correction is not actionable.  The reviewer must
--   record both.  btrim guards against whitespace-only strings that would pass
--   a bare IS NOT NULL check.
--
-- Why claim_id is the stable anchor:
--   fte_financial_positions rows are deleted and re-derived on every Phase 0
--   reset; source_position_id becomes stale after each reprocess.  claim_id
--   is a hard FK to fte_claims (never deleted by Phase 0) and is the only
--   stable anchor for position-level decisions.  This is identical to the
--   rationale for claim_id constraints on dismiss_short_pay (migration 005),
--   confirm_short_pay (migration 006), and request_more_evidence (migration 007).
--
-- Why a partial unique index (not a cross-action conflict index):
--   mark_position_needs_correction has no logical conflict partner in the
--   current action vocabulary — it coexists correctly with an active
--   dismiss_short_pay, confirm_short_pay, or request_more_evidence for the same
--   claim.  The index prevents only duplicate simultaneous active
--   correction-needed markers for the same claim.  Contrast with
--   idx_fte_resolutions_single_active_position_short_pay (migration 006), which
--   spans two mutually contradictory actions.
--
-- Reconciler behavior: UNCHANGED.
--   No phase reads or acts on mark_position_needs_correction.  The resolution
--   row is loaded into _fte_active_resolutions by Phase 0.5 (all non-superseded
--   rows are loaded unconditionally) but no downstream phase filters on this
--   action.  The claim retains its reconciler-derived position status
--   (in_review or unbalanced) and its review queue entry across reruns
--   regardless of any active correction-needed marker.  Phase 7 queue routing
--   is NOT suppressed (contrast: dismiss_short_pay and confirm_short_pay both
--   suppress Phase 7).  Phase 8 short_pay_detected event emission is NOT
--   suppressed (contrast: dismiss_short_pay suppresses Phase 8).
--
-- Idempotency: these constraints are additive.  Run this migration once on a
-- DB that has already had migrations 001–007 applied.  Re-running produces a
-- duplicate-object error — check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. Correction-needed marker requires an actionable note (non-null and non-blank).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpnc_needs_notes
    check (
      action <> 'mark_position_needs_correction'
      or (notes is not null and btrim(notes) <> '')
    );

-- 2. Correction-needed marker requires a stable claim anchor (not
--    source_position_id, which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpnc_needs_claim_id
    check (action <> 'mark_position_needs_correction' or claim_id is not null);

-- 3. Correction-needed marker must target a position row, not an observation
--    or event.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpnc_needs_position_type
    check (action <> 'mark_position_needs_correction' or target_type = 'position');

-- 4. At most one active correction-needed marker per (practice_id, claim_id).
--    Prevents a claim accumulating multiple simultaneous conflicting markers.
--    Superseded rows are not covered by this index; an audit trail of
--    superseded markers is retained for history.
create unique index idx_fte_resolutions_single_active_correction_needed
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action = 'mark_position_needs_correction';

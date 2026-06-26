-- =============================================================================
-- Financial Truth Engine — Migration 009
-- migrations/009_mark_position_resolved_constraints.sql
--
-- Adds DB-level enforcement for the mark_position_resolved position-level
-- review resolution action that is already present in the migration 002 action
-- vocabulary (position-level group, action #12 of 15).
--
-- Objects added (no new columns, no new tables, no new status values,
-- no new queue reasons, no new event types):
--   1. CHECK constraint: notes IS NOT NULL and btrim(notes) <> ''
--      when action = 'mark_position_resolved'
--   2. CHECK constraint: claim_id IS NOT NULL
--      when action = 'mark_position_resolved'
--   3. CHECK constraint: target_type = 'position'
--      when action = 'mark_position_resolved'
--   4. Partial unique index: at most one active resolved marker per
--      (practice_id, claim_id) where is_superseded = false
--      and action = 'mark_position_resolved'
--
-- Why notes is required:
--   mark_position_resolved records a subjective workflow closure decision.
--   Without a written explanation the reviewer's reasoning is lost.  A future
--   reviewer or auditor cannot determine why the position was considered
--   resolved (e.g., "claim paid in full per ERA 2026-06-01", "write-off
--   approved by billing director").  btrim guards against whitespace-only
--   strings that would pass a bare IS NOT NULL check.
--
-- Why claim_id is the stable anchor:
--   fte_financial_positions rows are deleted and re-derived on every Phase 0
--   reset; source_position_id becomes stale after each reprocess.  claim_id
--   is a hard FK to fte_claims (never deleted by Phase 0) and is the only
--   stable anchor for position-level decisions.  Identical rationale to
--   dismiss_short_pay (migration 005), confirm_short_pay (migration 006),
--   request_more_evidence (migration 007), and mark_position_needs_correction
--   (migration 008).
--
-- Why a partial unique index (not a cross-action conflict index):
--   mark_position_resolved has no logical conflict partner in the current
--   action vocabulary that produces contradictory instructions to the
--   reconciler.  It coexists correctly with an active dismiss_short_pay or
--   confirm_short_pay for the same claim (different intent, same Phase 7
--   suppression effect for unbalanced positions).  The index prevents only
--   duplicate simultaneous active resolved markers for the same claim.
--
-- Reconciler behavior: Phase 7 ONLY.
--   When active for a claim whose position has reconciliation_status =
--   'unbalanced', the action suppresses the unbalanced_financial_position
--   queue row in Phase 7 (added to the existing IN-list alongside
--   dismiss_short_pay and confirm_short_pay).  Phase 8 (short_pay_detected)
--   is NOT suppressed.  reconciliation_status and open_balance_amount in
--   fte_financial_positions are NOT changed.
--
--   in_review positions are never suppressed by this action — the Phase 7
--   guard already limits suppression to reconciliation_status = 'unbalanced'.
--   Unknown or ambiguous financial state must remain visible.
--
-- Idempotency: these constraints are additive.  Run this migration once on a
-- DB that has already had migrations 001–008 applied.  Re-running produces a
-- duplicate-object error — check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. mark_position_resolved requires an actionable note (non-null and non-blank).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpr_needs_notes
    check (
      action <> 'mark_position_resolved'
      or (notes is not null and btrim(notes) <> '')
    );

-- 2. mark_position_resolved requires a stable claim anchor (not
--    source_position_id, which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpr_needs_claim_id
    check (action <> 'mark_position_resolved' or claim_id is not null);

-- 3. mark_position_resolved must target a position row, not an observation
--    or event.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_mpr_needs_position_type
    check (action <> 'mark_position_resolved' or target_type = 'position');

-- 4. At most one active resolved marker per (practice_id, claim_id).
--    Prevents a claim accumulating multiple simultaneous resolved markers.
--    Superseded rows are not covered by this index; an audit trail of
--    superseded markers is retained for history.
create unique index idx_fte_resolutions_single_active_position_resolved
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action = 'mark_position_resolved';

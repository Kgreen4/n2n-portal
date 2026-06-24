-- =============================================================================
-- Financial Truth Engine — Migration 006
-- migrations/006_confirm_short_pay_constraints.sql
--
-- Adds DB-level enforcement for the confirm_short_pay position-level review
-- resolution introduced in Task 005B.
--
-- Objects added (no new columns, no new tables, no new status values):
--   1. CHECK constraint: claim_id IS NOT NULL when action = 'confirm_short_pay'
--   2. CHECK constraint: target_type = 'position' when action = 'confirm_short_pay'
--   3. Partial unique index: at most one active short-pay position-level decision
--      per (practice_id, claim_id) across confirm_short_pay and dismiss_short_pay.
--
-- Why the unique index spans both actions:
--   confirm_short_pay affirms a short pay is real/actionable; dismiss_short_pay
--   suppresses it.  Allowing both to be simultaneously active for the same claim
--   would produce contradictory instructions to the reconciler (Phase 7 would
--   suppress the queue row from both, while Phase 8 would only suppress the event
--   from dismiss).  The conflict-prevention index prevents this: once a
--   confirm_short_pay row is active for (practice_id, claim_id), inserting an
--   active dismiss_short_pay raises a unique_violation, and vice versa.  To switch
--   from one to the other: set is_superseded = true on the active row, then insert
--   the new one.
--
-- Idempotency: these constraints are additive.  Run this migration once on a
-- DB that has already had migrations 001-005 applied.  Re-running produces a
-- duplicate-object error -- check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. confirm_short_pay requires a stable claim anchor (not source_position_id,
--    which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_confirm_shortpay_needs_claim_id
    check (action <> 'confirm_short_pay' or claim_id is not null);

-- 2. confirm_short_pay must target a position row, not an observation or event.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_confirm_shortpay_needs_position_type
    check (action <> 'confirm_short_pay' or target_type = 'position');

-- 3. At most one active short-pay position-level decision per (practice_id, claim_id).
--    Prevents simultaneous active confirm_short_pay + dismiss_short_pay for the same
--    claim, and also prevents two active rows of the same action.
create unique index idx_fte_resolutions_single_active_position_short_pay
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action in ('confirm_short_pay', 'dismiss_short_pay');

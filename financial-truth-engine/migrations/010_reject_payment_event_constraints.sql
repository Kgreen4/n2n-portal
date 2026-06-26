-- =============================================================================
-- Financial Truth Engine — Migration 010
-- migrations/010_reject_payment_event_constraints.sql
--
-- Adds DB-level enforcement for the reject_payment_event payment-event-level
-- review resolution action that is already present in the migration 002 action
-- vocabulary (payment-event-level group, action #2 of 3).
--
-- Objects added (no new columns, no new tables, no new status values,
-- no new queue reasons, no new event types):
--   1. CHECK constraint: notes IS NOT NULL and btrim(notes) <> ''
--      when action = 'reject_payment_event'
--   2. CHECK constraint: claim_id IS NOT NULL
--      when action = 'reject_payment_event'
--   3. CHECK constraint: target_type = 'payment_event'
--      when action = 'reject_payment_event'
--   4. Cross-action partial unique index: at most one active payment-event
--      decision (confirm OR reject) per (practice_id, claim_id) where
--      is_superseded = false and action IN ('confirm_payment_event',
--      'reject_payment_event')
--
-- Why notes is required:
--   Rejecting a payment event is a financially significant decision —
--   it removes a payment from the claim's ledger and increases the derived
--   open_balance_amount to the full billed amount. The reviewer must record an
--   actionable explanation. btrim guards against whitespace-only strings.
--
-- Why claim_id is the stable anchor:
--   fte_claim_events rows are deleted and re-derived on every Phase 0 reset;
--   source_claim_event_id becomes stale after each reprocess. claim_id is a
--   hard FK to fte_claims (never deleted by Phase 0) and is the only stable
--   anchor for payment-event-level decisions. This mirrors the rationale for
--   claim_id constraints on position-level actions (migrations 005–009).
--
-- Why a cross-action conflict index (not a single-action index):
--   confirm_payment_event and reject_payment_event are logically contradictory
--   for the same claim — both cannot be simultaneously active. The same cross-
--   action index pattern used by migration 006 (confirm_short_pay +
--   dismiss_short_pay) is applied here. Contrast with mark_position_needs_
--   correction (migration 008) and mark_position_resolved (migration 009),
--   which use single-action indexes because they have no conflict partner.
--
-- Reconciler behavior:
--   Phase 1: UNCHANGED. Payment observations remain classified 'trusted' for
--   claims with an active reject_payment_event — the observation is kept in
--   the ledger as evidence; only Phase 5c event emission is suppressed.
--   Phase 5c: The new _fte_rejected_payment_event_claims temp table (Phase 0.5
--   addition in the reconciler) is checked before each payment_applied INSERT.
--   When the claim appears in that table, CONTINUE skips the INSERT. The
--   observation is not mutated; fte_observations.classification remains
--   'trusted'. Phase 6: open_balance_amount recalculates without the payment,
--   so it equals the full billed amount (GREATEST(0, billed - adj - 0)).
--   Phase 7: NOT suppressed — unbalanced_financial_position queue row still
--   emits. Phase 8: NOT suppressed — short_pay_detected event still emits
--   with the recalculated open balance.
--
-- Idempotency: these constraints are additive. Run this migration once on a
-- DB that has already had migrations 001–009 applied. Re-running produces a
-- duplicate-object error — check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. Reject-payment-event requires an actionable note (non-null and non-blank).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rpe_needs_notes
    check (
      action <> 'reject_payment_event'
      or (notes is not null and btrim(notes) <> '')
    );

-- 2. Reject-payment-event requires a stable claim anchor (not
--    source_claim_event_id, which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rpe_needs_claim_id
    check (action <> 'reject_payment_event' or claim_id is not null);

-- 3. Reject-payment-event must target a payment event, not an observation
--    or position row.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rpe_needs_payment_event_type
    check (action <> 'reject_payment_event' or target_type = 'payment_event');

-- 4. At most one active payment-event decision per (practice_id, claim_id).
--    Prevents confirm_payment_event and reject_payment_event from being
--    simultaneously active for the same claim — they are logically contradictory.
--    Superseded rows are not covered by this index; an audit trail of
--    superseded decisions is retained for history.
create unique index idx_fte_resolutions_single_active_payment_event_decision
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action in ('confirm_payment_event', 'reject_payment_event');

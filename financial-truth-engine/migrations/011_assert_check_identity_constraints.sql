-- =============================================================================
-- Financial Truth Engine — Migration 011
-- migrations/011_assert_check_identity_constraints.sql
--
-- Adds DB-level enforcement for the assert_check_identity action that is
-- already present in the migration 002 action vocabulary (payment-event-level
-- group, action #3 of 3).
--
-- Objects added (no new columns, no new tables, no new status values,
-- no new queue reasons, no new event types):
--   1. CHECK constraint: notes IS NOT NULL and btrim(notes) <> ''
--      when action = 'assert_check_identity'
--   2. CHECK constraint: claim_id IS NOT NULL
--      when action = 'assert_check_identity'
--   3. CHECK constraint: corrected_identifier IS NOT NULL and
--      btrim(corrected_identifier) <> '' when action = 'assert_check_identity'
--   4. CHECK constraint: target_type = 'payment_event'
--      when action = 'assert_check_identity'
--   5. Single-action partial unique index: at most one active check identity
--      assertion per (practice_id, claim_id) where is_superseded = false
--      and action = 'assert_check_identity'
--
-- Why notes is required:
--   Asserting a canonical check identity is a financially significant decision
--   that must be auditable. The reviewer must record an actionable explanation
--   (e.g. "check #361951 is the canonical number; pages 2-3 display a per-page
--   reference/control number that is not the check number"). btrim guards
--   against whitespace-only strings.
--
-- Why claim_id is the stable anchor:
--   fte_claim_events rows are deleted and re-derived on every Phase 0 reset;
--   source_claim_event_id becomes stale after each reprocess. claim_id is a
--   hard FK to fte_claims (never deleted by Phase 0) and is the only stable
--   anchor for payment-event-level decisions. This mirrors the rationale for
--   claim_id constraints on position-level actions (migrations 005–009) and
--   the other payment-event-level action (migration 010).
--
-- Why corrected_identifier is required:
--   An identity assertion without a canonical value is meaningless — the
--   corrected_identifier column (migration 002, line 122) exists specifically
--   to hold the reviewer-supplied canonical check number. Asserting identity
--   without providing the corrected value is not a complete resolution.
--
-- Why target_type = 'payment_event':
--   assert_check_identity targets a payment event (a check/EFT identifier
--   associated with a claim's payment), not an observation or position row.
--   This is consistent with the other two payment-event-level actions
--   (confirm_payment_event, reject_payment_event).
--
-- Why a single-action index (not a cross-action conflict index):
--   assert_check_identity has no logically contradictory counterpart in the
--   current action vocabulary. Unlike confirm_payment_event + reject_payment_event
--   (which are mutually exclusive and share a cross-action index in migration 010),
--   an active assert_check_identity can coexist with an active
--   confirm_payment_event or reject_payment_event for the same claim. The
--   single-action index prevents only duplicate simultaneous identity assertions.
--   This mirrors the pattern from migrations 007, 008, and 009. Contrast with
--   migrations 006 and 010 which use cross-action indexes for mutually exclusive
--   action pairs.
--
-- Reconciler behavior: UNCHANGED.
--   Phase 0.5 loads assert_check_identity rows into _fte_active_resolutions
--   (all non-superseded rows are loaded unconditionally) but no downstream
--   phase acts on them. The identity assertion is a durable reviewer note —
--   the corrected_identifier is stored in fte_review_resolutions for human
--   review and future phase integration but does not affect claim event
--   emission, open_balance_amount, queue routing, or short_pay detection.
--   review_resolutions_applied increments by 1 (Phase 0.5 count), which is
--   a reporting counter only.
--
-- Idempotency: these constraints are additive. Run this migration once on a
-- DB that has already had migrations 001–010 applied. Re-running produces a
-- duplicate-object error — check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. Assert-check-identity requires an actionable note (non-null and non-blank).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_aci_needs_notes
    check (
      action <> 'assert_check_identity'
      or (notes is not null and btrim(notes) <> '')
    );

-- 2. Assert-check-identity requires a stable claim anchor (not
--    source_claim_event_id, which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_aci_needs_claim_id
    check (action <> 'assert_check_identity' or claim_id is not null);

-- 3. Assert-check-identity requires a non-blank corrected identifier.
--    An assertion without a canonical check number is not a complete resolution.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_aci_needs_corrected_identifier
    check (
      action <> 'assert_check_identity'
      or (corrected_identifier is not null and btrim(corrected_identifier) <> '')
    );

-- 4. Assert-check-identity must target a payment event, not an observation
--    or position row.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_aci_needs_payment_event_type
    check (action <> 'assert_check_identity' or target_type = 'payment_event');

-- 5. At most one active check identity assertion per (practice_id, claim_id).
--    Unlike the cross-action index for confirm/reject_payment_event (migration
--    010), this is a single-action index — assert_check_identity has no
--    logically contradictory counterpart. A superseded assertion is retained
--    as a permanent audit trail; only the active row is constrained.
create unique index idx_fte_resolutions_single_active_check_identity
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action = 'assert_check_identity';

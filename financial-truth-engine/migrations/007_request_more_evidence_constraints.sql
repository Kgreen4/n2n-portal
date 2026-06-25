-- =============================================================================
-- Financial Truth Engine — Migration 007
-- migrations/007_request_more_evidence_constraints.sql
--
-- Adds DB-level enforcement for the request_more_evidence position-level review
-- resolution action that is already present in the migration 002 action
-- vocabulary (position-level group, action #13 of 15).
--
-- Objects added (no new columns, no new tables, no new status values,
-- no new queue reasons, no new event types):
--   1. CHECK constraint: notes IS NOT NULL and btrim(notes) <> ''
--      when action = 'request_more_evidence'
--   2. CHECK constraint: claim_id IS NOT NULL
--      when action = 'request_more_evidence'
--   3. CHECK constraint: target_type = 'position'
--      when action = 'request_more_evidence'
--   4. Partial unique index: at most one active evidence request per
--      (practice_id, claim_id) where is_superseded = false
--      and action = 'request_more_evidence'
--
-- Why notes is required:
--   An evidence request without a written explanation of what evidence is
--   needed and why the claim is stuck is not actionable.  The reviewer must
--   record both.  btrim guards against whitespace-only strings that would
--   pass a bare IS NOT NULL check.
--
-- Why claim_id is the stable anchor:
--   fte_financial_positions rows are deleted and re-derived on every Phase 0
--   reset; source_position_id becomes stale after each reprocess.  claim_id
--   is a hard FK to fte_claims (never deleted by Phase 0) and is the only
--   stable anchor for position-level decisions.  This is identical to the
--   rationale for claim_id constraints on dismiss_short_pay (migration 005)
--   and confirm_short_pay (migration 006).
--
-- Why a partial unique index (not a cross-action conflict index):
--   request_more_evidence has no logical conflict partner in the current
--   action vocabulary — it coexists correctly with an active dismiss_short_pay
--   or confirm_short_pay for the same claim.  The index prevents only
--   duplicate simultaneous active evidence requests for the same claim.
--   Contrast with idx_fte_resolutions_single_active_position_short_pay
--   (migration 006), which spans two mutually contradictory actions.
--
-- Reconciler behavior: UNCHANGED.
--   No phase reads or acts on request_more_evidence.  The resolution row is
--   loaded into _fte_active_resolutions by Phase 0.5 (all non-superseded rows
--   are loaded unconditionally) but no downstream phase filters on this action.
--   The claim retains its reconciler-derived position status (in_review or
--   unbalanced) across reruns regardless of any active evidence request.
--
-- Idempotency: these constraints are additive.  Run this migration once on a
-- DB that has already had migrations 001–006 applied.  Re-running produces a
-- duplicate-object error — check IF NOT EXISTS first or apply once per DB.
-- =============================================================================

-- 1. Evidence request requires an actionable note (non-null and non-blank).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rme_needs_notes
    check (
      action <> 'request_more_evidence'
      or (notes is not null and btrim(notes) <> '')
    );

-- 2. Evidence request requires a stable claim anchor (not source_position_id,
--    which becomes stale after each Phase 0 reset).
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rme_needs_claim_id
    check (action <> 'request_more_evidence' or claim_id is not null);

-- 3. Evidence request must target a position row, not an observation or event.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_rme_needs_position_type
    check (action <> 'request_more_evidence' or target_type = 'position');

-- 4. At most one active evidence request per (practice_id, claim_id).
--    Prevents a claim accumulating multiple simultaneous conflicting evidence
--    requests.  Superseded rows are not covered by this index; an audit trail
--    of superseded requests is retained for history.
create unique index idx_fte_resolutions_single_active_evidence_request
  on fte_review_resolutions (practice_id, claim_id)
  where is_superseded = false
    and action = 'request_more_evidence';

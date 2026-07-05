-- =============================================================================
-- Financial Truth Engine (FTE) — Denial Lifecycle Schema Support
-- Migration: 012_denial_lifecycle_constraints.sql
-- Depends on: 011_assert_check_identity_constraints.sql
-- Created: 2026-07-05
--
-- WHAT THIS MIGRATION ADDS (schema-only; NO reconciler/explain behavior change)
-- -----------------------------------------------------------------------------
-- Additive schema support for the denial lifecycle designed in Task 017A. This
-- migration adds ONLY structure; no lifecycle events are emitted yet and no
-- accounting changes are made (those arrive in later reconciler/explain slices).
--
-- 1. Three new reviewer actions in the fte_review_resolutions action vocabulary:
--      file_appeal        — workflow marker on a recoverable denial (no amount)
--      record_recovery    — reviewer-supplied recovered amount (payer reversal)
--      approve_write_off  — reviewer-gated internal write-off amount
--    The base vocabulary lives in the inline column CHECK from migration 002,
--    auto-named fte_review_resolutions_action_check. It is dropped and re-added
--    with the full 18-value list (15 existing + 3 lifecycle). All 15 existing
--    actions remain valid and behavior-compatible.
--
-- 2. lifecycle_amount numeric(14,2) — nullable reviewer-supplied amount for
--    record_recovery / approve_write_off. Kept separate from corrected_value
--    (attach_corrected_value) so the two semantics never overload one column.
--
-- 3. Shape constraints for the lifecycle actions:
--      record_recovery   requires lifecycle_amount IS NOT NULL AND > 0
--      approve_write_off requires lifecycle_amount IS NOT NULL AND > 0
--      file_appeal       requires lifecycle_amount IS NULL (it is workflow-only)
--    Existing actions are unconstrained by lifecycle_amount (they leave it NULL).
--
-- 4. Two nullable DERIVED reporting columns on fte_financial_positions:
--      recovered_amount    numeric(14,2)  — realized appeal revenue (future 017C)
--      written_off_amount  numeric(14,2)  — internally written-off balance (017C)
--    Populated by a later reconciler slice; added here so that slice needs no
--    schema change. Non-negative CHECKs guard both.
--
-- NOT CHANGED (verified against migration 001):
--   * event_type already includes appeal_filed / recovery_received /
--     write_off_approved; amount_type already includes recovery / write_off.
--     No enum/status changes are made here.
--   * No new tables. No new claim/position status values. RLS unchanged
--     (both tables already have RLS + policies from migrations 001/002).
--
-- Idempotent: every ADD is preceded by DROP ... IF EXISTS / uses ADD COLUMN IF
-- NOT EXISTS, so the migration is safe to re-run against an already-migrated
-- disposable database.
-- =============================================================================

begin;

-- -----------------------------------------------------------------------------
-- 1. Extend the action vocabulary CHECK (15 existing + 3 lifecycle = 18).
--    Drop the migration-002 inline check (auto-named *_action_check) and
--    re-add it with the lifecycle actions appended.
-- -----------------------------------------------------------------------------
alter table fte_review_resolutions
  drop constraint if exists fte_review_resolutions_action_check;

alter table fte_review_resolutions
  add constraint fte_review_resolutions_action_check
    check (action in (
      -- Observation-level (5)
      'confirm_observation',
      'reject_observation',
      'mark_duplicate',
      'resolve_contradiction',
      'attach_corrected_value',
      -- Payment / event-level (3)
      'confirm_payment_event',
      'reject_payment_event',
      'assert_check_identity',
      -- Position-level (7)
      'confirm_short_pay',
      'dismiss_short_pay',
      'mark_position_resolved',
      'mark_position_needs_correction',
      'request_more_evidence',
      'confirm_position_balanced',
      'override_position_status',
      -- Denial lifecycle (3) — Task 017B
      'file_appeal',
      'record_recovery',
      'approve_write_off'
    ));

-- -----------------------------------------------------------------------------
-- 2. lifecycle_amount — nullable reviewer-supplied amount for lifecycle actions.
-- -----------------------------------------------------------------------------
alter table fte_review_resolutions
  add column if not exists lifecycle_amount numeric(14,2);

comment on column fte_review_resolutions.lifecycle_amount is
  'Reviewer-supplied amount for denial-lifecycle actions: the recovered amount '
  'for record_recovery and the written-off amount for approve_write_off. NULL '
  'for file_appeal (workflow-only) and for all non-lifecycle actions. Distinct '
  'from corrected_value (attach_corrected_value) to keep the two semantics '
  'separate.';

-- -----------------------------------------------------------------------------
-- 3. Shape constraints for the lifecycle actions (mirrors the migration-004
--    "action <> 'X' OR <cond>" pattern).
-- -----------------------------------------------------------------------------

-- record_recovery requires a positive lifecycle_amount.
alter table fte_review_resolutions
  drop constraint if exists fte_review_resolutions_record_recovery_amount_positive;
alter table fte_review_resolutions
  add constraint fte_review_resolutions_record_recovery_amount_positive
    check (
      action <> 'record_recovery'
      OR (lifecycle_amount IS NOT NULL AND lifecycle_amount > 0)
    );

-- approve_write_off requires a positive lifecycle_amount.
alter table fte_review_resolutions
  drop constraint if exists fte_review_resolutions_write_off_amount_positive;
alter table fte_review_resolutions
  add constraint fte_review_resolutions_write_off_amount_positive
    check (
      action <> 'approve_write_off'
      OR (lifecycle_amount IS NOT NULL AND lifecycle_amount > 0)
    );

-- file_appeal must NOT carry an amount (workflow/reporting only).
alter table fte_review_resolutions
  drop constraint if exists fte_review_resolutions_file_appeal_no_amount;
alter table fte_review_resolutions
  add constraint fte_review_resolutions_file_appeal_no_amount
    check (
      action <> 'file_appeal'
      OR lifecycle_amount IS NULL
    );

-- -----------------------------------------------------------------------------
-- 4. Derived reporting columns on fte_financial_positions (nullable), populated
--    by a future reconciler slice. Parallel to recoverable_amount.
-- -----------------------------------------------------------------------------
alter table fte_financial_positions
  add column if not exists recovered_amount numeric(14,2);
alter table fte_financial_positions
  add column if not exists written_off_amount numeric(14,2);

comment on column fte_financial_positions.recovered_amount is
  'DERIVED (future 017C): realized appeal revenue — the portion of denied_amount '
  'reclassified to paid via record_recovery. Reporting overlay; the recovery is '
  'open_balance-invariant within denied. NULL when no recovery. Not source truth.';
comment on column fte_financial_positions.written_off_amount is
  'DERIVED (future 017C): internally written-off remaining balance via '
  'approve_write_off. Always surfaced so a reduced open_balance is explainable. '
  'Distinct from payer denied_amount. NULL when no write-off. Not source truth.';

-- -----------------------------------------------------------------------------
-- 5. Non-negative guards for the new position columns.
-- -----------------------------------------------------------------------------
alter table fte_financial_positions
  drop constraint if exists fte_financial_positions_recovered_amount_nonneg;
alter table fte_financial_positions
  add constraint fte_financial_positions_recovered_amount_nonneg
    check (recovered_amount IS NULL OR recovered_amount >= 0);

alter table fte_financial_positions
  drop constraint if exists fte_financial_positions_written_off_amount_nonneg;
alter table fte_financial_positions
  add constraint fte_financial_positions_written_off_amount_nonneg
    check (written_off_amount IS NULL OR written_off_amount >= 0);

-- =============================================================================
-- End of migration 012.
-- =============================================================================

commit;

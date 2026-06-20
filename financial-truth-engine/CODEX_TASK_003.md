# Codex Task 003 — Review Queue Resolution Spec

**Status:** Planning / Specification Only  
**Created:** 2026-06-19  
**Author:** Keith Green  
**Task type:** Design spec — no implementation in this task  
**Depends on:** Task 001 (schema), Task 002 (reconciler)  
**Blocks:** Task 004 (implementation of review resolution, if approved)

---

## Purpose

Task 002 delivered a deterministic 9-phase reconciler that classifies
observations and derives financial positions. It knows how to detect problems:

- Suspect observations (non-null `failure_mode`)
- Ambiguous payment events (Phase 5: late/retry contradiction)
- Short pay (Phase 8: `short_pay_detected` analysis run flag)
- Unbalanced positions (open balance ≠ 0)
- No-event in-review positions (no trusted observations produced any events)

The system does not yet know how a human reviewer resolves those problems in an
auditable, non-destructive way. This spec defines that.

---

## Non-Goals for This Task

This is a planning document only. The following are explicitly excluded:

- No SQL implementation
- No schema migrations
- No UI or frontend work
- No API or Edge Function code
- No test files
- No AI observation ingestion
- No real PDF intake or real evidence loading
- No production data of any kind
- No PHI, real member IDs, real DOBs, or real claim numbers
- No connection to the legacy EOB Supabase project
- No modification of legacy EOB code

Implementation (Task 004) must be separately approved after this spec is
reviewed.

---

## 1. Problem Statement

### 1.1 What Task 002 Produces

After `fte_reconcile_practice()` runs, the following items can require human
review:

| Source | Table | Condition | Reconciler phase |
|--------|-------|-----------|-----------------|
| Suspect observation | `fte_review_queue` | `failure_mode` not null | Phase 2 |
| Non-trusted observation (no events) | `fte_review_queue` | observation excluded from event generation | Phase 2 |
| Ambiguous payment event | `fte_claim_events` | `reconciliation_status = 'ambiguous'` | Phase 5 |
| Unbalanced position | `fte_financial_positions` | `reconciliation_status = 'unbalanced'` | Phase 6 |
| In-review position (no events) | `fte_financial_positions` | `reconciliation_status = 'in_review'`, all monetary fields null | Phase 6 |
| In-review position (ambiguous event) | `fte_financial_positions` | `reconciliation_status = 'in_review'`, math may balance | Phase 6 |
| Short pay detected | `fte_analysis_runs` | `short_pay_detected = true` | Phase 8 |

### 1.2 What Is Missing

There is currently no:

- Mechanism for a reviewer to record a decision against a review queue item
- Mechanism for a reviewer to confirm or reject an observation
- Mechanism for a reviewer to assert what the correct financial outcome is
- Audit trail of who decided what and when
- Instruction to the reconciler about whether and how to consume past reviewer
  decisions on the next run

### 1.3 Why This Matters

Without review resolution:

- `in_review` and `unbalanced` positions accumulate indefinitely
- The system cannot distinguish "not yet reviewed" from "reviewed and confirmed
  uncertain"
- Reprocessing a document wipes all derived state (Phase 0 DELETE), including
  any informal notes or corrections
- There is no auditable record of the human judgment that produced a final
  financial position

---

## 2. Review Action Vocabulary

The following reviewer actions are proposed. Each action resolves one or more
items in `fte_review_queue` or one `fte_claim_events` / `fte_financial_positions`
row.

### 2.1 Observation-Level Actions

| Action | Applies to | Meaning |
|--------|-----------|---------|
| `confirm_observation` | `fte_observations` row linked via review queue | Reviewer confirms the AI observation is accurate |
| `reject_observation` | `fte_observations` row linked via review queue | Reviewer asserts the observation is wrong (e.g. hallucinated amount) |
| `mark_duplicate` | `fte_observations` row | Reviewer asserts this observation duplicates another |
| `resolve_contradiction` | Two `fte_observations` rows | Reviewer identifies which of two contradicting observations is authoritative |
| `attach_corrected_value` | `fte_observations` row | Reviewer supplies a corrected monetary or identifier value |

### 2.2 Payment / Event-Level Actions

| Action | Applies to | Meaning |
|--------|-----------|---------|
| `confirm_payment_event` | `fte_claim_events` row (`type='payment_applied'`, status `'ambiguous'`) | Reviewer confirms the payment is real despite the late/retry flag |
| `reject_payment_event` | `fte_claim_events` row | Reviewer asserts this event should not exist |
| `assert_check_identity` | `fte_claim_events` row | Reviewer asserts that two payment rows represent the same physical check |

### 2.3 Financial Position-Level Actions

| Action | Applies to | Meaning |
|--------|-----------|---------|
| `confirm_short_pay` | `fte_financial_positions` row, `fte_analysis_runs` | Reviewer confirms payer underpaid; initiates appeal workflow |
| `dismiss_short_pay` | `fte_financial_positions` row, `fte_analysis_runs` | Reviewer confirms contractual write-off, short pay is expected |
| `mark_position_resolved` | `fte_financial_positions` row | Reviewer attests the position is correct as-is |
| `mark_position_needs_correction` | `fte_financial_positions` row | Reviewer flags position requires re-extraction or re-review |
| `request_more_evidence` | `fte_review_queue` row | Reviewer cannot resolve without additional source documents |

---

## 3. Data Model Options

### Option A: New `fte_review_resolutions` Table

A separate append-only table records every reviewer decision. `fte_review_queue`
rows remain open until explicitly resolved.

**Proposed schema (conceptual — not yet implemented):**

```sql
-- NOT implemented. Requires Task 004 approval.
create table fte_review_resolutions (
    id                  uuid primary key default gen_random_uuid(),
    practice_id         uuid not null references fte_practices(id),
    review_queue_id     uuid references fte_review_queue(id),
    claim_event_id      uuid references fte_claim_events(id),
    observation_id      uuid references fte_observations(id),
    position_id         uuid references fte_financial_positions(id),
    action              text not null,        -- vocab from §2 above
    resolved_by         text,                -- reviewer identifier (email or user id)
    resolved_at         timestamptz not null default now(),
    notes               text,
    corrected_value     numeric(12,2),        -- optional, for attach_corrected_value
    corrected_identifier text,               -- optional, for assert_check_identity
    is_superseded       boolean not null default false -- if reviewer later changes their mind
);
```

**Pros:**
- Fully append-only; reviewer decisions are never deleted or mutated
- Complete audit trail: who, what, when, on which entity
- Multiple resolutions per queue item are natural (reviewer changes mind →
  new row with `is_superseded = false`, old row updated to `is_superseded = true`
  by application logic — does NOT mutate `fte_review_queue`)
- Reconciler can JOIN to this table in a future Phase 0.5 to honour past
  decisions before re-deriving
- Safe under Phase 0 DELETE: positions and events are re-derived; resolutions
  survive because they are in a separate table not deleted by Phase 0

**Cons:**
- New migration required
- Reconciler needs new logic to consume resolutions (Phase 0.5 or a new Phase 1a)
- Application layer must enforce that `is_superseded` is set on the old row when
  a new resolution replaces it (schema cannot enforce this alone)
- Slightly more complex to query current effective resolution (need to filter
  `is_superseded = false`)

**Audit implications:**
- Immutable history: every decision visible to auditors
- Reversal is visible (old row `is_superseded = true`, new row created) — no
  silent overwrites

**Idempotency implications:**
- Reconciler can consume `fte_review_resolutions` during Phase 0.5 (before
  deleting derived rows): copy relevant decisions into a temp table, then apply
  them during event / position derivation
- Re-running the reconciler without new resolutions produces the same result as
  before (idempotent)
- Re-running after a new resolution may change a position's status from
  `in_review` to `balanced` (if reviewer confirmed an ambiguous event) — this
  is expected and desired

---

### Option B: Resolution Fields on `fte_review_queue`

Add `resolved_at`, `resolved_by`, `resolution_action`, and `resolution_notes`
columns directly to `fte_review_queue`.

**Pros:**
- No new table; simpler to query (one table for queue state and resolution)
- Lower migration surface area

**Cons:**
- `fte_review_queue` rows become mutable — reviewer decisions overwrite each
  other; no natural history of "I changed my mind"
- Phase 0 DELETE of `fte_review_queue` (current reconciler Phase 0, step 2)
  would wipe all resolution history on reprocess — **this is a fundamental
  conflict**. Fixing it would require either (a) excluding `resolved` rows from
  Phase 0 DELETE, or (b) changing Phase 0 to re-insert resolutions after DELETE
  (fragile)
- Single resolution per queue item — reviewer cannot change their mind without
  overwriting the record
- Harder to query "what decisions has a reviewer made" across the practice
- Not truly auditable: final state only, no history

**Audit implications:**
- Poor — last-write-wins; no history of changed decisions

**Idempotency implications:**
- Fragile: Phase 0 DELETE destroys resolution state unless the DELETE logic is
  modified to skip resolved rows, creating a stateful carve-out that complicates
  the otherwise clean Phase 0 wipe

---

### Option C: Event-Sourced Review Actions Table

A dedicated append-only event-sourced log: every reviewer interaction is an
event. Current resolution state is computed by replaying events.

```sql
-- NOT implemented. Requires Task 004 approval.
create table fte_review_events (
    id              uuid primary key default gen_random_uuid(),
    practice_id     uuid not null references fte_practices(id),
    occurred_at     timestamptz not null default now(),
    actor           text not null,
    event_type      text not null,    -- 'confirm_observation', 'reject_observation', etc.
    target_type     text not null,    -- 'observation', 'claim_event', 'financial_position', 'review_queue'
    target_id       uuid not null,
    payload         jsonb             -- flexible; corrected values, notes, etc.
);
```

**Pros:**
- Maximum auditability: full replay of all reviewer actions
- No mutation ever; complete history
- Flexible `payload` handles new action types without schema changes
- Replay is possible if a bug is found in how events were processed

**Cons:**
- Highest complexity: requires a projection/view or materialization step to
  determine "current" state of a review item
- JSONB `payload` loses type safety; corrected amounts, identifiers, and notes
  are all unstructured
- For a small review queue (hundreds of items per practice), event sourcing adds
  architectural overhead without proportional benefit
- Reconciler consumption is harder: must materialize current state from event
  log before Phase 0 can honour decisions

**Audit implications:**
- Best possible — full replay, nothing deleted

**Idempotency implications:**
- Complex: reconciler needs a materialization step to project current review
  state, then consume it; this projection must itself be idempotent

---

## 4. Recommended Direction

**Option A: `fte_review_resolutions` table.**

Reasoning:

1. **Survives Phase 0.** Resolution rows live outside the tables Phase 0 deletes.
   The reconciler can honour past reviewer decisions on rerun without carrying
   forward derived state.
2. **Auditable by default.** Append-only with `is_superseded` flag. No silent
   overwrites. Changed decisions are visible, not erased.
3. **Simpler than Option C.** Structured columns (not JSONB payload) preserve
   type safety for corrected amounts and identifiers. Full event sourcing would
   be proportionate for a high-volume system; for a review queue of dozens to
   hundreds of items per practice, a structured row per resolution is sufficient.
4. **Foreign keys are explicit.** `review_queue_id`, `claim_event_id`,
   `observation_id`, and `position_id` are nullable but typed; the application
   layer can enforce that exactly one is set per row without needing JSONB.
5. **Clean reconciler extension point.** A future Phase 0.5 SELECT from
   `fte_review_resolutions WHERE is_superseded = false` can load active
   resolutions into a temp table before Phase 0 wipes derived state, then re-apply
   them during position derivation — no architectural change to existing phases.

**The single known weakness** (application must set `is_superseded = true` on
the old row when a reviewer changes their mind) is acceptable: it is enforced
in the application layer, not the schema, and is straightforward to test.

---

## 5. Reconciler Interaction

### 5.1 Phase 0 Behaviour (Current)

Phase 0 currently deletes, in FK-safe order:
1. `fte_event_evidence`
2. `fte_review_queue`
3. `fte_financial_positions`
4. `fte_claim_events`

`fte_analysis_runs` is NOT deleted (append-only).

Under Option A, `fte_review_resolutions` would also NOT be deleted by Phase 0.
It is a separate table and represents human judgment, not derived state.

### 5.2 Proposed Phase 0.5 (Future — not implemented here)

Between Phase 0 (delete derived state) and Phase 1 (classify observations), a
new Phase 0.5 would:

1. SELECT all non-superseded resolutions for the practice into a temp table
   `_fte_active_resolutions`.
2. Make this temp table available to Phase 2 (observation trust classification)
   and Phase 5 (ambiguous event handling).

Effects on Phase 2:
- An observation with a `reject_observation` resolution is treated as excluded
  regardless of its current `failure_mode`.
- An observation with a `confirm_observation` resolution is treated as trusted
  even if `failure_mode` is not null (reviewer overrides extraction failure).

Effects on Phase 5:
- A `payment_applied` event with a `confirm_payment_event` resolution does NOT
  receive `reconciliation_status = 'ambiguous'` — the reviewer has confirmed it.
- A `payment_applied` event with a `reject_payment_event` resolution is not
  emitted at all.

### 5.3 Reconciler Must Not Overwrite Human Decisions

On rerun:
- Phase 0 must not DELETE `fte_review_resolutions`.
- Phase 6 must produce `reconciliation_status = 'balanced'` for a position
  where the reviewer's resolutions have resolved all ambiguity — not leave it
  permanently `in_review`.
- Phase 9 `fte_analysis_runs` entry should record `review_resolutions_applied: N`
  in its JSONB summary so the log reflects that human decisions shaped the run.

### 5.4 New Claim Events vs. Review State Update

Two possible models after a reviewer confirms an observation:

**Model A — Update position status only.** Phase 6 re-derives the position's
`reconciliation_status` based on the now-confirmed events. No new `fte_claim_events`
row is created. Simpler; position status changes on rerun.

**Model B — Emit a new `review_confirmed` claim event.** After a reviewer
confirms the payment, Phase 0.5 emits a new event (`type = 'review_confirmed'`,
linked to the resolution). Phase 6 consumes this event and can then produce
`balanced`. More auditable; position history richer.

**Recommendation:** start with Model A. It requires no new event type, no new
schema, and leverages the existing Phase 6 CASE block. If audit requirements
demand a `review_confirmed` event trail, Model B can be added in a later task
without breaking Model A.

---

## 6. Acceptance Criteria for Task 004

Task 004 (implementation of review resolution) would need to prove all of the
following before merging:

### Schema

- [ ] `fte_review_resolutions` table exists in a new migration file
- [ ] Migration is idempotent (`CREATE TABLE IF NOT EXISTS` or migration version guard)
- [ ] `practice_id` column present with FK to `fte_practices`
- [ ] `action` column accepts only the vocabulary in §2 (CHECK constraint or enum)
- [ ] `is_superseded` column defaults to false; no NOT NULL enforcement on
      `review_queue_id`, `claim_event_id`, `observation_id`, `position_id` (all
      nullable; exactly-one enforced by application logic)
- [ ] RLS policy stubs present (consistent with Task 001 pattern)

### Reconciler Phase 0.5

- [ ] Phase 0.5 SELECT runs after Phase 0 DELETE and before Phase 1 TEMP TABLE
- [ ] Result stored in temp table `_fte_active_resolutions ON COMMIT DROP`
- [ ] Phase 2 observation classification JOIN to `_fte_active_resolutions`:
  - `reject_observation` → excluded regardless of `failure_mode`
  - `confirm_observation` → trusted regardless of `failure_mode`
- [ ] Phase 5 ambiguous event handling checks `_fte_active_resolutions`:
  - `confirm_payment_event` → event NOT marked `'ambiguous'`
  - `reject_payment_event` → event excluded
- [ ] Phase 9 JSON summary includes `review_resolutions_applied` count

### Idempotency

- [ ] Running reconciler twice with zero new resolutions produces identical
      positions, events, and review queue entries
- [ ] Running reconciler after adding a `confirm_payment_event` resolution for
      the ccdbe216 synthetic fixture re-derives `reconciliation_status = 'balanced'`
      and `open_balance_amount = 0.00`. The following must all persist after
      the rerun: the `fte_review_resolutions` record (reviewer decision is not
      consumed or deleted), the prior `fte_analysis_runs` entry showing the
      position was once `in_review` (append-only log; Phase 0 does not touch
      it), and the Phase 5 late/retry contradiction evidence re-created by the
      new run (the observation's `failure_mode` has not changed; the
      contradiction is still flagged). The contradiction does not disappear and
      no history is overwritten — only the derived position changes, because
      the reconciler now treats the ambiguous payment as reviewer-confirmed.

### Audit

- [ ] Inserting a new resolution with `is_superseded = false` for a queue item
      that already has an active resolution does NOT automatically supersede the
      old one — application layer must set `is_superseded = true` on the old row
      before inserting the new one (test: verify both rows exist with different
      `is_superseded` values, not that the old row was deleted)
- [ ] Phase 0 DELETE does NOT touch `fte_review_resolutions`

### Safety

- [ ] Validation suite uses only synthetic fixture UUIDs
      (`c0000000-0000-4000-8000-0000000000fe` and `96000000-0000-4000-8000-0000000000fe`)
- [ ] No PHI, no production exports, no legacy EOB identifiers in test SQL
- [ ] No connection to legacy EOB Supabase project in any code or test

---

## 7. Safety Boundaries

These constraints apply to Task 004 implementation and any future tasks derived
from this spec:

- **Synthetic fixtures only** — no PHI, no real member IDs, no real DOBs, no
  real claim numbers, no production exports
- **No raw PDFs** — evidence records may reference synthetic source documents
  only
- **No legacy EOB repo** — do not import, copy, or adapt code from
  `kgreen41-eob/cardio-metrics-saas`
- **No legacy Supabase project** — the FTE test environment is a disposable
  Supabase project; no connection to the production EOB project
- **No direct production database access** — all testing is done by the human
  reviewer running SQL manually in the Supabase SQL editor; no credentials
  embedded in code or docs

---

## 8. Open Questions (Resolve Before Task 004)

1. **Who is a "reviewer"?** Is it the practice owner (Keith / clinic admin), a
   dedicated billing reviewer, or a system process? The `resolved_by` column
   accepts a text identifier — define the format before implementation.

2. **Is Phase 0.5 a new numbered phase or a sub-phase of Phase 0?** Naming
   convention matters for the reconciler README.

3. **Should `confirm_payment_event` on the ccdbe216 synthetic fixture be the
   primary acceptance test, or should a new fixture be added?** Using existing
   synthetic data is preferred to avoid adding real data, but the existing
   fixture does not yet include a pre-inserted `fte_review_resolutions` row.

4. **Model A vs. Model B for new events:** confirm that Model A (no new event
   type) is sufficient before Task 004 begins. If a `review_confirmed` event
   trail is required for billing audit purposes, the event vocabulary and
   `fte_claim_events.type` CHECK constraint must be updated in the migration.

5. **Scope of `request_more_evidence`:** does this action pause re-runs of the
   reconciler for that claim, or is it purely informational? If it creates a
   hold, Phase 0.5 needs to detect it and leave the position `in_review` even
   if no other ambiguity exists.

---

## Summary

| Section | Decision |
|---------|----------|
| Data model | Option A — `fte_review_resolutions` (append-only, `is_superseded`) |
| Phase 0 behaviour | No change — resolutions survive Phase 0 DELETE |
| New reconciler phase | Phase 0.5 — load active resolutions before Phase 1 |
| Event model | Model A — no new event type; position re-derived from confirmed observations |
| Implementation | Deferred to Task 004, subject to approval |
| Spec status | Planning only — no code written |

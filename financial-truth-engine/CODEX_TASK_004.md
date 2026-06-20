# Codex Task 004 — Implement Append-Only Review Resolutions

**Task type:** Implementation spec — approved for coding after this doc is reviewed  
**Status:** Draft — not yet implemented  
**Depends on:** Task 001 (schema), Task 002 (reconciler), Task 003 (design spec)  
**Branch:** `docs/fte-task-004-review-resolution-implementation-spec`  
**Created:** 2026-06-20

---

## §1 Purpose

Tasks 001 and 002 established that the reconciler is idempotent: Phase 0 deletes all
derived rows and rebuilds them from scratch on every call. This is correct for
financial truth — but it means there is currently no way to record a human reviewer's
decision that survives a reprocess.

The problem is concrete: the `ccdbe216` synthetic fixture produces a `payment_applied`
event that Phase 5 marks `'ambiguous'` (late/retry contradiction), which causes Phase 6
to set `reconciliation_status = 'in_review'` on the financial position. The math already
balances (`720.00 − 209.60 − 510.40 = 0.00`). A reviewer who inspects the evidence and
confirms the payment is correct has no mechanism to record that decision in the current
system. The next reprocess overwrites the position back to `'in_review'` regardless.

Task 004 introduces `fte_review_resolutions`: an append-only, Phase-0-survivable table
that records typed reviewer decisions and feeds them into a new Phase 0.5 in the
reconciler. Phase 0.5 loads all non-superseded resolutions before classification begins,
allowing Phase 2 (observation classification) and Phase 5 (ambiguity detection) to
consume those decisions without modifying the underlying contradiction evidence.

---

## §2 Non-Goals

The following are explicitly out of scope for Task 004:

- **No UI.** Resolutions are inserted by the human reviewer directly in the Supabase
  SQL editor, exactly as the synthetic validation scripts are run today.
- **No API endpoint.** No Edge Function, REST route, or RPC wrapper.
- **No Model B.** Do not add a `review_confirmed` event type to `fte_claim_events`.
  The existing `event_type` CHECK constraint must not be altered in Task 004. The
  position re-derives as `'balanced'` through the existing Phase 6 logic — not through
  a new event (see §6).
- **No users table and no `resolved_by` FK.** `resolved_by` is a free-text field only.
  Authentication and reviewer identity management are deferred to a future task.
- **No `request_more_evidence` gating.** In Task 004, `action = 'request_more_evidence'`
  is recorded and counted but does not block or delay reconciler reruns.
- **No hard FK to any Phase-0-deleted table.** `fte_review_resolutions` must not
  reference `fte_review_queue`, `fte_claim_events`, or `fte_financial_positions` as
  enforced foreign keys. See §4 for the reasoning and the correct alternative.
- **No modification of base failure-mode fixtures.** `synthetic_ccdbe216_failure_modes.sql`
  and `synthetic_96c5c357_failure_modes.sql` must not be changed. Resolution seeds live
  in a separate file.
- **No renumbering of existing reconciler phases.** The new phase is called Phase 0.5
  and is documented as a sub-phase between Phase 0 and Phase 1.

---

## §3 Schema Plan — `fte_review_resolutions`

Migration file: `migrations/002_add_review_resolutions.sql`

### Table definition (not yet implemented)

```text
fte_review_resolutions
──────────────────────────────────────────────────────────────────
id                   uuid primary key default gen_random_uuid()
practice_id          uuid not null → fte_practices(id)    [stable FK]
claim_id             uuid          → fte_claims(id)       [stable FK, nullable]
observation_id       uuid          → fte_observations(id) [stable FK, nullable]
evidence_id          uuid          → fte_evidence(id)     [stable FK, nullable]

action               text not null [CHECK 15 values — see below]
target_type          text          snapshot: 'observation' | 'payment_event' |
                                             'position' | 'review_entry' | 'claim'
target_event_type    text          snapshot: e.g. 'payment_applied', 'denial_posted'
target_review_reason text          snapshot: e.g. 'late_retry_page_contradiction'
target_claim_number  text          snapshot: readable claim identifier for auditability

source_review_queue_id  uuid       nullable — snapshot value ONLY, no FK enforced
source_claim_event_id   uuid       nullable — snapshot value ONLY, no FK enforced
source_position_id      uuid       nullable — snapshot value ONLY, no FK enforced

resolved_by          text          free-text reviewer identifier (no FK, no users table)
resolved_at          timestamptz   not null default now()
notes                text
corrected_value      numeric(12,2)
corrected_identifier text
is_superseded        boolean       not null default false
metadata             jsonb         not null default '{}'
created_at           timestamptz   not null default now()
```

### Action CHECK constraint — 15 valid values

```text
Observation-level (5):
  'confirm_observation'
  'reject_observation'
  'mark_duplicate'
  'resolve_contradiction'
  'attach_corrected_value'

Payment/event-level (3):
  'confirm_payment_event'
  'reject_payment_event'
  'assert_check_identity'

Position-level (7):
  'confirm_short_pay'
  'dismiss_short_pay'
  'mark_position_resolved'
  'mark_position_needs_correction'
  'request_more_evidence'
  'confirm_position_balanced'
  'override_position_status'
```

### Required infrastructure

- RLS policy on `fte_review_resolutions` (same pattern as other FTE tables — tenant-scoped
  via `fte_accessible_practice_ids()`).
- Index on `(practice_id, is_superseded)` — used by Phase 0.5 for every reconciler call.
- Index on `(practice_id, claim_id, action)` — used by Phase 5 to look up
  `confirm_payment_event` resolutions efficiently.
- Index on `(practice_id, observation_id, action)` — used by Phase 2 to look up
  observation-level overrides.
- `updated_at` trigger using `fte_set_updated_at()` (already defined in migration 001).

### What is NOT in this table

- No `updated_at` column is needed if the table is truly append-only and `is_superseded`
  is the only mutation. If `is_superseded` updates need auditing, add `updated_at` plus
  the trigger; otherwise omit it.
- No reference to `fte_review_queue.id`, `fte_claim_events.id`, or
  `fte_financial_positions.id` as enforced foreign keys (see §4).

---

## §4 Stable Target Strategy — No Hard FK to Phase-0-Derived Rows

### Why volatile IDs cannot be used as enforced FKs

Phase 0 deletes four tables in FK-safe order:

```text
fte_event_evidence   → deleted first
fte_review_queue     → deleted second
fte_financial_positions → deleted third
fte_claim_events     → deleted fourth
```

The UUIDs of rows in these tables are **not stable across reruns**. They are generated
with `gen_random_uuid()` at reconciler runtime. After every Phase 0, the old IDs are
gone and new IDs are assigned.

If `fte_review_resolutions` held an enforced FK to `fte_review_queue(id)` with
`ON DELETE CASCADE`, Phase 0 would cascade-delete the resolution — destroying reviewer
history. If the FK used `ON DELETE RESTRICT`, Phase 0's DELETE would fail with a foreign
key violation, breaking the reconciler's idempotency guarantee.

Neither outcome is acceptable.

### What to use instead

Target the **entity layer**, which is stable and not deleted by Phase 0:

| Column | Type | Table | Stable? |
|---|---|---|---|
| `practice_id` | hard FK | `fte_practices` | ✅ Yes — tenant root, never deleted |
| `claim_id` | hard FK | `fte_claims` | ✅ Yes — entity, not derived |
| `observation_id` | hard FK | `fte_observations` | ✅ Yes — evidence layer, not deleted |
| `evidence_id` | hard FK | `fte_evidence` | ✅ Yes — immutable, never deleted |

The volatile derived-row IDs are captured as **nullable snapshot fields** (plain `uuid`
columns, no `REFERENCES` clause). They record what was visible when the resolution was
created for audit purposes but do not enforce referential integrity. They become stale
after a reprocess — that is expected and acceptable for snapshot fields.

### How the reconciler matches resolutions without volatile IDs

Phase 0.5 loads non-superseded resolutions into `_fte_active_resolutions`. Downstream
phases match resolutions using stable identifiers:

- **Phase 2** joins on `observation_id` to override observation classification.
- **Phase 5** matches on `claim_id` + `action = 'confirm_payment_event'` to suppress
  ambiguous marking.
- **Phase 9** counts all rows in `_fte_active_resolutions` as `review_resolutions_applied`.

No phase looks up a resolution by `fte_claim_events.id`, `fte_review_queue.id`, or
`fte_financial_positions.id`.

### For `confirm_payment_event` on ccdbe216

The resolution row targets:
- `claim_id` → the stable `fte_claims` row for the ccdbe216 payment claim
- `observation_id` → the stable `fte_observations` row for the payment observation
  (the one whose `failure_mode = 'late_retry'` triggers Phase 5)
- `action = 'confirm_payment_event'`
- `target_event_type = 'payment_applied'` (snapshot)
- `target_review_reason = 'late_retry_page_contradiction'` (snapshot)

Phase 5 finds this resolution by matching `claim_id` and `action` in
`_fte_active_resolutions`, then allows the payment event to be emitted as `'reconciled'`
rather than `'ambiguous'`.

---

## §5 Reconciler Phase 0.5 Design

Phase 0.5 runs **after** Phase 0's four DELETEs and **before** Phase 1's observation
classification. No existing phase is renumbered.

### Logic

```text
Phase 0.5 — Load active resolutions
  1. Declare: v_resolution_count integer := 0;
  2. CREATE TEMP TABLE _fte_active_resolutions ON COMMIT DROP AS
       SELECT *
         FROM fte_review_resolutions
        WHERE practice_id = p_practice_id
          AND is_superseded = false;
  3. GET DIAGNOSTICS v_resolution_count = ROW_COUNT;
  4. (No error if 0 rows — an empty temp table is valid.)
```

### Downstream usage

| Phase | How it uses `_fte_active_resolutions` |
|---|---|
| Phase 2 | JOIN on `observation_id` to override `_fte_classified.classification` |
| Phase 5 | EXISTS check on `claim_id + action = 'confirm_payment_event'` to suppress `'ambiguous'` |
| Phase 9 | `v_resolution_count` added to return JSON as `review_resolutions_applied` |

### What Phase 0.5 does NOT do

- It does not modify `fte_review_resolutions` rows.
- It does not gate or skip reconciliation if `request_more_evidence` resolutions exist.
- It does not consume resolutions (they persist unchanged after the reconciler runs).

---

## §6 How `confirm_payment_event` Affects ccdbe216 Without Erasing Contradiction History

### Without resolution (baseline behavior)

1. Phase 5 detects the late/retry contradiction on the ccdbe216 payment observation.
2. Phase 5 marks the resulting `payment_applied` event as `reconciliation_status = 'ambiguous'`.
3. Phase 5 inserts a `fte_event_evidence` row with `link_role = 'contradicts'`.
4. Phase 6 sees the ambiguous event → derives `reconciliation_status = 'in_review'` on
   the financial position (`'ambiguous'` is not a valid position status per schema CHECK).
5. Result: `in_review`, `open_balance_amount = 0.00`.

### With `confirm_payment_event` resolution (post-Task-004 behavior)

Phase 0.5 loads the resolution row. Phase 5 checks `_fte_active_resolutions`:

```text
EXISTS (
  SELECT 1 FROM _fte_active_resolutions
   WHERE claim_id = <ccdbe216 claim_id>
     AND action = 'confirm_payment_event'
)
```

When the EXISTS check is true:
- Phase 5 marks the `payment_applied` event as `reconciliation_status = 'reconciled'`
  (not `'ambiguous'`).
- Phase 5 **still inserts** the `fte_event_evidence` row with `link_role = 'contradicts'`.
  The contradiction is re-derived and re-recorded by every reconciler run — it is not
  suppressed. The observation's `failure_mode` was set at ingestion time and is never
  touched by Phase 0.
- Phase 6 sees no ambiguous events → derives `reconciliation_status = 'balanced'`.
- Result: `balanced`, `open_balance_amount = 0.00`.

### What is preserved across both runs

| Element | Behavior |
|---|---|
| `fte_review_resolutions` row | Persists — Phase 0 does not touch this table |
| Prior `fte_analysis_runs` entries | Persist — append-only, Phase 0 does not touch |
| Observation's `failure_mode = 'late_retry'` | Persists — set at ingestion, never mutated |
| `fte_event_evidence` with `link_role = 'contradicts'` | Re-created by Phase 5 on every run |
| `payment_applied` event status | Changes from `'ambiguous'` to `'reconciled'` post-resolution |
| Financial position status | Changes from `'in_review'` to `'balanced'` post-resolution |

The key distinction: **the contradiction is re-flagged** (the `'contradicts'` evidence
link is re-created, the observation's `failure_mode` persists) but the position is
re-derived as `'balanced'` because the reviewer's decision suppressed only the
`'ambiguous'` marking on the event, not the underlying evidence. The contradiction does
not disappear and no history is overwritten — only the derived position changes.

---

## §7 Test Plan

### New file: `tests/validate_review_resolution.sql`

Wrapped in `BEGIN` / `ROLLBACK` — nothing persists after the test.

**Test structure:**

```text
Step 1 — Baseline (no resolution)
  Run: SELECT fte_reconcile_practice('<ccdbe216 practice_id>');
  Assert: ccdbe216 financial position = 'in_review'
  Assert: ccdbe216 open_balance_amount = 0.00
  Assert: payment_applied event reconciliation_status = 'ambiguous'
  Assert: fte_event_evidence row with link_role = 'contradicts' exists

Step 2 — Insert resolution
  INSERT INTO fte_review_resolutions (practice_id, claim_id, observation_id,
    action, target_type, target_event_type, target_review_reason,
    resolved_by, notes)
  VALUES (
    '<ccdbe216 practice_id>',
    '<ccdbe216 claim_id>',             -- stable FK
    '<ccdbe216 payment observation_id>',-- stable FK
    'confirm_payment_event',
    'payment_event',
    'payment_applied',
    'late_retry_page_contradiction',
    'test_runner',
    'Reviewer confirms: retry page is OCR artifact, original payment is correct'
  );

Step 3 — Rerun reconciler
  Run: SELECT fte_reconcile_practice('<ccdbe216 practice_id>');

Step 4 — Assert post-resolution state
  Assert: ccdbe216 financial position = 'balanced'         ← changed
  Assert: ccdbe216 open_balance_amount = 0.00              ← unchanged
  Assert: payment_applied event reconciliation_status = 'reconciled' ← changed
  Assert: fte_event_evidence 'contradicts' link still exists ← unchanged
  Assert: fte_review_resolutions row still exists (not deleted)
  Assert: fte_analysis_runs count = 2 (prior run preserved, new run appended)
  Assert: Phase 9 return JSON has 'review_resolutions_applied' = 1

Step 5 — Idempotency check
  Run: SELECT fte_reconcile_practice('<ccdbe216 practice_id>');
  Assert: all Step 4 assertions still hold after a third run
  Assert: fte_review_resolutions row still exists
  Assert: fte_analysis_runs count = 3
```

### Scope

- All assertions use synthetic practice IDs only (`c0000000-0000-4000-8000-0000000000fe`).
- No PHI, no real claim numbers, no production exports.
- Test runs in a disposable Supabase project — same environment used for Tasks 001/002.

### What is NOT tested in Task 004

- Observation-level actions (`confirm_observation`, `reject_observation`,
  `mark_duplicate`) — deferred to a future task.
- `assert_check_identity` — deferred.
- Position-level actions — deferred.
- 96c5c357 fixture with resolution — deferred (the `ccdbe216` path is the primary
  acceptance test for Task 004).

---

## §8 Acceptance Criteria

Task 004 is complete when all of the following pass in the disposable test Supabase project:

- [ ] Migration `002_add_review_resolutions.sql` applies cleanly after migration 001
      with no errors.
- [ ] `fte_review_resolutions` table exists with all columns in §3.
- [ ] `fte_review_resolutions` has RLS enabled with a tenant-scoped SELECT/INSERT policy.
- [ ] `action` CHECK constraint rejects values outside the 15-value vocabulary.
- [ ] `source_review_queue_id`, `source_claim_event_id`, `source_position_id` are plain
      `uuid` columns with no `REFERENCES` clause (confirmed by inspecting `information_schema`
      or `pg_constraint` — no FK constraint name should reference these columns).
- [ ] `practice_id`, `claim_id`, `observation_id`, `evidence_id` carry enforced FK
      constraints to their respective stable tables.
- [ ] Phase 0 DELETE sequence completes successfully even when `fte_review_resolutions`
      contains rows for the practice being reprocessed (no FK violation, no cascade delete).
- [ ] Phase 0.5 appears in reconciler code between Phase 0 and Phase 1; creates
      `_fte_active_resolutions ON COMMIT DROP`.
- [ ] Phase 9 return JSON includes `'review_resolutions_applied'` key.
- [ ] With no resolutions present: `ccdbe216` financial position = `'in_review'`,
      `open_balance_amount = 0.00` (baseline unchanged from Task 002 validation).
- [ ] After inserting a `confirm_payment_event` resolution for ccdbe216 (targeting
      `claim_id` + `observation_id`): rerunning the reconciler produces
      `reconciliation_status = 'balanced'` and `open_balance_amount = 0.00`.
- [ ] After the above rerun:
      (a) the `fte_review_resolutions` row still exists — not deleted or consumed;
      (b) the prior `fte_analysis_runs` entry is still present — append-only log preserved;
      (c) a `fte_event_evidence` row with `link_role = 'contradicts'` exists for the
          ccdbe216 payment claim — contradiction re-flagged, not erased;
      (d) the `payment_applied` event has `reconciliation_status = 'reconciled'`, not
          `'ambiguous'`.
- [ ] A third reconciler call produces identical results to the second (idempotency).
- [ ] `validate_review_resolution.sql` exits without assertion failures, wrapped in ROLLBACK.
- [ ] No modifications to `synthetic_ccdbe216_failure_modes.sql` or
      `synthetic_96c5c357_failure_modes.sql`.

---

## §9 Safety Boundaries

All boundaries from CODEX_TASK_003.md §7 remain in effect:

**No PHI.** Synthetic fixtures only. No real member IDs, real DOBs, real claim numbers,
real patient names, or production Supabase exports.

**No raw PDFs.** Evidence records may reference synthetic `fixture_id` values only.
No binary PDF content is embedded in SQL or committed to the repository.

**No legacy EOB repo.** Do not import, copy, or adapt code from
`kgreen41-eob/cardio-metrics-saas`. The FTE reconciler and schema must be built from
the FTE codebase only.

**No legacy Supabase project.** The FTE test environment is a disposable Supabase project.
No connection string, service role key, or schema reference points to the legacy EOB
production Supabase project.

**No direct production database access.** All testing is performed by the human reviewer
running SQL manually in the Supabase SQL editor. No credentials are embedded in code or
docs.

**No hard FK to Phase-0-deleted tables.** `fte_review_resolutions` must not carry an
enforced `REFERENCES` clause pointing to `fte_review_queue`, `fte_claim_events`, or
`fte_financial_positions`. Volatile IDs may appear only as snapshot fields.

**No new claim event type.** The `fte_claim_events.event_type` CHECK constraint must not
be altered in Task 004 (Model A — position re-derives from confirmed observations, not
from a new event type).

**No reconciler rerun gating.** `request_more_evidence` resolutions are recorded but do
not pause or block reconciler execution in Task 004.

---

## Summary

| Item | Decision |
|---|---|
| New table | `fte_review_resolutions` — append-only, survives Phase 0 |
| Stable FKs | `practice_id`, `claim_id`, `observation_id`, `evidence_id` |
| Volatile IDs | Snapshot fields only — `source_review_queue_id`, `source_claim_event_id`, `source_position_id` — no `REFERENCES` |
| New reconciler phase | Phase 0.5 — loads `_fte_active_resolutions ON COMMIT DROP` |
| Phase 2 change | Observation classification override via `observation_id` join |
| Phase 5 change | `confirm_payment_event` suppresses `'ambiguous'` but still re-creates `'contradicts'` evidence |
| Phase 9 change | Adds `review_resolutions_applied` to return JSON |
| Model | A — no new event type |
| `resolved_by` | Free text, no FK |
| `request_more_evidence` | Informational only — no reconciler gating |
| Phase naming | Phase 0.5 — no renumbering of existing phases |
| Fixtures | Separate resolution seed file — base fixtures unchanged |
| Test | `tests/validate_review_resolution.sql` — ROLLBACK-wrapped, disposable project only |

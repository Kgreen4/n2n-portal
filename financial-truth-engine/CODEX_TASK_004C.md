# CODEX TASK 004C — Observation-Level Review Resolutions

**Task ID:** 004C
**Branch:** `feature/fte-task-004c-observation-resolutions` (create from latest `main`)
**Repo:** `Kgreen4/n2n-portal` · scope: `financial-truth-engine/` only
**Depends on:** Migration 001, 002 applied; `fte_reconcile.sql` registered (Task 004B merged)

---

## 1. Scope

Implement three new observation-level reviewer actions that were reserved in the
`fte_review_resolutions.action` CHECK constraint (migration 002) but have no
reconciler behaviour yet:

| Action | Behaviour |
|---|---|
| `confirm_observation` | **Queue-only effect.** Reviewer confirms a flagged observation is correctly classified. The observation still enters `_fte_classified` with its original classification (excluded or suspect) and still influences ledger events and positions exactly as in the baseline. Only the `fte_review_queue` entry is suppressed. `confirm_observation` does **not** promote excluded observations to trusted, does **not** change any `fte_claim_events` rows, and does **not** change any `fte_financial_positions` rows. |
| `reject_observation` | Reviewer marks an observation as entirely invalid. The observation is **suppressed from Phase 1** — it does not enter `_fte_classified`, produces no claim events, and produces no queue entry. Rejecting a trusted observation removes its contribution to ledger events and causes financial positions to recalculate without it. |
| `mark_duplicate` | Reviewer marks an observation as a duplicate of a canonical observation. Same Phase 1 suppression as `reject_observation`. `target_observation_id` (new FK column in migration 003) records the canonical observation. `target_observation_id` must not equal `observation_id`. |

### Out of scope for this task
- Corrected values, supersession workflow, UI/API/Edge Functions
- Bulk review tools, position overrides
- Any new `fte_claim_events.event_type`
- Any change to the `action` CHECK constraint (all 15 values already present in migration 002)
- Modification of migration 001, migration 002, or any existing fixture
- The 96c5c357 practice fixture — it is a regression anchor only; not used here

---

## 2. Security constraints

The following constraints are in effect and must not be violated by any file
written in this task:

- No PHI. Synthetic fixtures only — no real member IDs, no real DOBs, no real
  claim numbers, no production exports.
- No raw PDFs — evidence records may reference synthetic source documents only.
- No legacy EOB repo — do not import, copy, or adapt code from
  `kgreen41-eob/cardio-metrics-saas`.
- No legacy Supabase project — the FTE test environment is a disposable Supabase
  project; no connection to the production EOB project.
- No direct production database access — all testing is done by the human
  reviewer running SQL manually in the Supabase SQL editor; no credentials
  embedded in code or docs.
- No hard FK to Phase-0-deleted tables.
- No new claim event type. Do not alter `fte_claim_events.event_type` CHECK
  constraint.
- Do not delete review resolution history. Do not remove contradiction evidence
  links.
- Do not modify migration 002. Do not modify base fixtures.
- Do not add UI/API/Edge Functions.
- **Do not commit until Keith reviews.**

---

## 3. Fixture inventory — ccdbe216 practice

Practice ID: `c0000000-0000-4000-8000-0000000000fe`
Claim: `c1a10000-0000-4000-8000-000000000001` (CLM-AZ-0001)
Source: `fixtures/synthetic_ccdbe216_failure_modes.sql` (do not modify)

| Obs | UUID | Type | Key properties | Phase 1 result |
|---|---|---|---|---|
| a1 | `0b500000-0000-4000-8000-0000000000a1` | payment $510.40 | is_superseded=false, check_eft_identifier='0000447412' | TRUSTED |
| a2 | `0b500000-0000-4000-8000-0000000000a2` | billed $720.00 | is_superseded=false | TRUSTED |
| a3 | `0b500000-0000-4000-8000-0000000000a3` | contractual_adjustment $209.60 | is_superseded=false | TRUSTED |
| b1 | `0b500000-0000-4000-8000-0000000000b1` | payment | is_superseded=TRUE, failure_mode='phantom_duplicate_check_ref' | EXCLUDED Rule 1 (`suspected_duplicate`) |
| b2 | `0b500000-0000-4000-8000-0000000000b2` | payment | is_superseded=TRUE, failure_mode='section_delimiter_double_count' | EXCLUDED Rule 1 (`conflicting_observations`) |
| b3 | `0b500000-0000-4000-8000-0000000000b3` | payment $265.36 | is_superseded=TRUE, check_eft_identifier=NULL, failure_mode='null_check_crossbleed' | EXCLUDED Rule 1 (`missing_evidence_link`) |
| b4 | `0b500000-0000-4000-8000-0000000000b4` | summary_total $1479.08 | is_superseded=FALSE, is_summary_row=TRUE | EXCLUDED Rule 2 (`suspected_summary_row`) |
| b5 | `0b500000-0000-4000-8000-0000000000b5` | denial $0.00 | is_superseded=TRUE, failure_mode='late_retry_page_contradiction' | EXCLUDED Rule 1 (`late_retry_page_contradiction`) |

**Fixture resolution targets:**

| Target obs | Action | Why chosen |
|---|---|---|
| b4 | `confirm_observation` | Only observation excluded by Rule 2 (not Rule 1); is_superseded=false; proves queue suppression without Phase 1 filter |
| b3 | `reject_observation` (queue-suppression case) | Already excluded by Rule 1; proves that reject suppresses Phase 1 classification and queue entry for an already-problematic observation |
| a3 | `reject_observation` (ledger-impact case) | Trusted observation that emits `contractual_adjustment_applied` event; proves that rejecting a trusted observation suppresses the event and causes position recalculation |
| b1 → a1 | `mark_duplicate` | b1 is the phantom-duplicate of canonical a1; proves Phase 1 suppression and `target_observation_id` FK wiring |

---

## 4. Queue count tracing

Baseline reconciler run (no resolutions):
- Phase 2 produces 5 observation entries: b1 (suspected_duplicate), b2 (conflicting_observations), b3 (missing_evidence_link), b4 (suspected_summary_row), b5 (late_retry_page_contradiction)
- Phase 5 finds b5 in queue → marks payment_applied 'ambiguous'
- Phase 6 derives position as 'in_review' (ambiguous event)
- Phase 7 produces 1 position entry: unbalanced_financial_position

**Baseline total: 6**

After each resolution applied cumulatively (steps 1–8 of the validation test):

| Resolution applied | Phase 2 entries | Phase 7 entry | Queue total |
|---|---|---|---|
| none | b1, b2, b3, b4, b5 | unbalanced_financial_position | **6** |
| + confirm_observation(b4) | b1, b2, b3, b5 | unbalanced_financial_position | **5** |
| + reject_observation(b3) | b1, b2, b5 | unbalanced_financial_position | **4** |
| + mark_duplicate(b1→a1) | b2, b5 | unbalanced_financial_position | **3** |

The `unbalanced_financial_position` entry persists throughout because b5's
`late_retry_page_contradiction` is never resolved in 004C — Phase 5 always
marks payment_applied 'ambiguous', Phase 6 always sets position 'in_review',
Phase 7 always adds the entry.

**Additional ledger-impact test (steps 9–10 of the validation test):**

After also adding reject_observation(a3) — cumulative with the three resolutions above:
- a3 was trusted → no queue entry in baseline; reject does not change queue count
- Queue remains **3**
- `contractual_adjustment_applied` event: **0** (was 1 without reject)
- Position open_balance: **$209.60** (billed=$720.00 − paid=$510.40; adjustment no longer applied)
- Position status: still 'in_review' (b5 ambiguity unchanged)

---

## 5. Files to create / modify

| File | Action | Description |
|---|---|---|
| `migrations/003_add_observation_resolution_target.sql` | **CREATE** | `target_observation_id` FK column + 5 CHECK constraints + index |
| `reconciler/fte_reconcile.sql` | **MODIFY** | Three targeted changes (Phase 0.5, Phase 1 NOT EXISTS, Phase 2) |
| `tests/validate_observation_resolution.sql` | **CREATE** | 12-check validation, 10 steps + rollback, ccdbe216 fixture only |
| `tests/README.md` | **MODIFY** | Add section for new validation file |
| `reconciler/README.md` | **MODIFY** | Document three new action types and Phase 0.5 addition |

Implementation order: migration → reconciler → test → READMEs.

---

## 6. Migration 003

**File:** `migrations/003_add_observation_resolution_target.sql`

```sql
-- =============================================================================
-- Financial Truth Engine (FTE) — Observation Resolution Constraints
-- Migration: 003_add_observation_resolution_target.sql
-- Depends on: 002_add_review_resolutions.sql
-- Created: 2026-06-22
--
-- WHAT THIS MIGRATION ADDS
-- -----------------------------------------------------------------------------
-- 1. target_observation_id column — typed FK to fte_observations.
--    Required for mark_duplicate to record the canonical observation.
--    NULL for all other action types (enforced by constraint #5 below).
--    Hard FK to fte_observations, which is a stable entity table not touched
--    by Phase 0.
--
-- 2. Five CHECK constraints that together enforce valid shape for the three
--    observation-level resolution actions:
--
--    For action IN ('confirm_observation', 'reject_observation', 'mark_duplicate'):
--      #1  observation_id   IS NOT NULL      (the observation being resolved)
--      #2  target_type      = 'observation'  (semantic type guard)
--
--    For action = 'mark_duplicate' specifically:
--      #3  target_observation_id IS NOT NULL  (the canonical observation; already
--                                              implied by #4 below but stated
--                                              explicitly for clarity)
--      #4  target_observation_id <> observation_id  (cannot be a duplicate of itself)
--
--    For all OTHER actions:
--      #5  target_observation_id IS NULL     (prevents misuse of the new column
--                                             for actions that have no canonical
--                                             observation concept)
--
-- 3. Partial index for reverse lookup: "which observations have been marked as
--    duplicates of canonical X?"
-- =============================================================================

begin;

alter table fte_review_resolutions
  add column target_observation_id uuid
    references fte_observations(id) on delete restrict;

comment on column fte_review_resolutions.target_observation_id is
  'For mark_duplicate resolutions: the canonical observation that the flagged '
  'observation is a duplicate of. Hard FK to fte_observations (stable entity, '
  'not touched by Phase 0). NULL for all non-mark_duplicate action types — '
  'enforced by constraint fte_review_resolutions_non_duplicate_no_target.';

-- CONSTRAINT #1: observation-level actions require observation_id to be set.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_obs_action_needs_obs_id
    check (
      action NOT IN ('confirm_observation', 'reject_observation', 'mark_duplicate')
      OR observation_id IS NOT NULL
    );

-- CONSTRAINT #2: observation-level actions require target_type = 'observation'.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_obs_action_needs_observation_type
    check (
      action NOT IN ('confirm_observation', 'reject_observation', 'mark_duplicate')
      OR target_type = 'observation'
    );

-- CONSTRAINT #3: mark_duplicate requires a canonical target observation.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_duplicate_needs_target
    check (
      action <> 'mark_duplicate'
      OR target_observation_id IS NOT NULL
    );

-- CONSTRAINT #4: an observation cannot be marked as a duplicate of itself.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_duplicate_no_self
    check (
      action <> 'mark_duplicate'
      OR target_observation_id <> observation_id
    );

-- CONSTRAINT #5: only mark_duplicate may populate target_observation_id.
-- Keeps the column from being silently misused for other action types.
alter table fte_review_resolutions
  add constraint fte_review_resolutions_non_duplicate_no_target
    check (
      action = 'mark_duplicate'
      OR target_observation_id IS NULL
    );

-- Support reverse lookup: "which observations have been marked as duplicates
-- of canonical X?" Partial index keeps it lean for the common NULL case.
create index idx_fte_resolutions_target_observation
  on fte_review_resolutions (practice_id, target_observation_id)
  where target_observation_id is not null;

-- =============================================================================
-- End of migration 003.
-- =============================================================================

commit;
```

---

## 7. Reconciler changes — `reconciler/fte_reconcile.sql`

Three targeted edits. No phase is added, removed, or renumbered.

### 7a. Phase 0.5 — add `_fte_suppressed_observations` (after line 87)

After `GET DIAGNOSTICS v_resolution_count = ROW_COUNT;` (currently line 87),
insert the following block:

```sql
  -- Suppressed observations: reject_observation and mark_duplicate remove the
  -- observation from Phase 1 entirely — no classification, events, or queue
  -- entry are derived from it in this reconciler run.
  DROP TABLE IF EXISTS _fte_suppressed_observations;

  CREATE TEMP TABLE _fte_suppressed_observations ON COMMIT DROP AS
  SELECT observation_id
  FROM _fte_active_resolutions
  WHERE action IN ('reject_observation', 'mark_duplicate')
    AND observation_id IS NOT NULL;
```

The `DROP TABLE IF EXISTS` guard mirrors the existing pattern used for
`_fte_active_resolutions` and `_fte_classified` — ensures idempotency when
the function is called multiple times in the same outer transaction.

The `AND observation_id IS NOT NULL` guard is defense-in-depth. Migration 003
constraint #1 (`fte_review_resolutions_obs_action_needs_obs_id`) already
enforces `observation_id IS NOT NULL` for all three observation-level actions
at the DB level, so the filter here is redundant in practice but harmless and
makes the intent of the subquery clear.

### 7b. Phase 1 — filter suppressed observations (line 149)

**Current** (line 148–149):
```sql
    FROM fte_observations obs
    WHERE obs.practice_id = p_practice_id
```

**Replace with:**
```sql
    FROM fte_observations obs
    WHERE obs.practice_id = p_practice_id
      AND NOT EXISTS (
        SELECT 1
        FROM _fte_suppressed_observations so
        WHERE so.observation_id = obs.id
      )
```

`NOT EXISTS` is used instead of `NOT IN` to avoid the SQL NULL-coalescion
trap: a `NOT IN (subquery)` evaluates to `UNKNOWN` (not `TRUE`) if the
subquery returns any NULL, filtering out all rows. `NOT EXISTS` has no such
trap — it evaluates cleanly whether or not the subquery could theoretically
produce NULLs. The migration constraint makes NULLs impossible, but `NOT EXISTS`
is the more robust and readable form regardless.

Suppressed observations never enter `_fte_classified`, so they produce no
events, no review queue entries, and are completely invisible to the rest of
the reconciler.

### 7c. Phase 2 — suppress queue entry for confirm_observation (line 194)

**Current** (line 194):
```sql
  WHERE cl.classification IN ('excluded', 'suspect');
```

**Replace with:**
```sql
  WHERE cl.classification IN ('excluded', 'suspect')
    AND NOT EXISTS (
      SELECT 1 FROM _fte_active_resolutions ar
      WHERE ar.observation_id = cl.id
        AND ar.action = 'confirm_observation'
    );
```

This is the **queue-only** effect of `confirm_observation`. The observation
remains in `_fte_classified` with its original classification, continues to
influence ledger events and financial positions exactly as in the baseline, and
its `fte_event_evidence` links are preserved. Phase 2 simply does not insert a
`fte_review_queue` row for it. The reviewer has acknowledged the classification
and dismissed it from the active queue — nothing else changes.

---

## 8. Validation test — `tests/validate_observation_resolution.sql`

12 PASS checks across 10 steps (+ rollback). The test:
1. Verifies queue count drops from 6 → 3 as cumulative resolutions are applied (checks 1–10).
2. Adds a separate ledger-impact case: rejects trusted observation a3 and asserts that the `contractual_adjustment_applied` event is suppressed and the financial position recalculates (checks 11–12).

```
STEP 1   Baseline run — no resolutions → queue = 6, resolutions_applied = 0       [1-3/12]
STEP 2   Insert confirm_observation for obs b4 (suspected_summary_row)
STEP 3   Second run → queue = 5                                                    [4-5/12]
STEP 4   Insert reject_observation for obs b3 (queue-suppression case)
STEP 5   Third run → queue = 4                                                     [6-7/12]
STEP 6   Insert mark_duplicate for obs b1 → canonical a1
STEP 7   Fourth run → queue = 3                                                    [8-9/12]
STEP 8   Idempotency — fifth run → queue still = 3                                [10/12]
STEP 9   Insert reject_observation for obs a3 (trusted, ledger-impact case)
STEP 10  Sixth run → contractual_adjustment_applied event absent; open_balance     [11-12/12]
         recalculates to $209.60
STEP 11  rollback — all reconciler output and resolution rows discarded
```

Full file content:

```sql
-- =============================================================================
-- Financial Truth Engine — Observation Resolution Validation
-- tests/validate_observation_resolution.sql
--
-- 12 PASS checks across 10 steps that verify the confirm_observation,
-- reject_observation (queue-suppression and ledger-impact), and mark_duplicate
-- resolution paths introduced in Task 004C.
--
-- STEP 1   Baseline run (no resolutions) → queue = 6, resolutions_applied = 0     [1-3/12]
-- STEP 2   Insert confirm_observation resolution for obs b4
-- STEP 3   Second run (confirm active) → queue = 5                                [4-5/12]
-- STEP 4   Insert reject_observation resolution for obs b3 (queue-suppression)
-- STEP 5   Third run (confirm + reject b3) → queue = 4                            [6-7/12]
-- STEP 6   Insert mark_duplicate resolution for obs b1 → canonical a1
-- STEP 7   Fourth run (all three resolutions active) → queue = 3                  [8-9/12]
-- STEP 8   Idempotency — fifth run → queue still = 3                              [10/12]
-- STEP 9   Insert reject_observation for obs a3 (trusted, ledger-impact)
-- STEP 10  Sixth run → contractual_adjustment_applied event absent;               [11-12/12]
--           open_balance recalculates to $209.60
-- STEP 11  rollback — all reconciler output and resolution rows discarded
--
-- Prerequisites:
--   1. migration 001_create_financial_truth_schema.sql applied
--   2. migration 002_add_review_resolutions.sql applied
--   3. migration 003_add_observation_resolution_target.sql applied
--   4. reconciler/fte_reconcile.sql registered (CREATE OR REPLACE)
--
-- Run via:
--   psql "$DATABASE_URL" -f tests/validate_observation_resolution.sql
--
-- All 12 checks emit RAISE NOTICE 'PASS [N/12] ...' and exit without exception.
-- Any ASSERT failure raises an unhandled EXCEPTION and aborts the run.
--
-- Supabase SQL editor note:
--   The \i metacommand below is psql-only. When running in the Supabase SQL
--   editor, paste the contents of fixtures/synthetic_ccdbe216_failure_modes.sql
--   first (execute separately so it commits), then paste this file's body
--   starting from the BEGIN block.
-- =============================================================================

\i fixtures/synthetic_ccdbe216_failure_modes.sql

begin;

-- Ensure no stale resolutions from a previous aborted run bleed in.
-- Phase 0 (inside the reconciler) handles derived tables; only
-- fte_review_resolutions survives Phase 0 and must be wiped manually.
delete from fte_review_resolutions
where practice_id = 'c0000000-0000-4000-8000-0000000000fe';

-- Capture the starting reconciler-run count. fte_analysis_runs is append-only;
-- a disposable DB may hold prior reconciler runs from earlier manual validation.
-- All run-count assertions below test deltas (advance >= N), not absolute totals.
create temp table _vor_baseline on commit drop as
  select count(*) as run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';


-- =============================================================================
-- STEP 1: Baseline run — no resolutions exist.
--
-- Expected:
--   review_resolutions_applied = 0
--   fte_review_queue count     = 6 (5 observation entries + 1 position entry)
--   b4 (suspected_summary_row) in queue
--   b1 (suspected_duplicate)   in queue
--   b3 (missing_evidence_link) in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 1/12: return JSON reports zero active resolutions loaded
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 0,
    format('FAIL [1/12] baseline: review_resolutions_applied expected 0, got %s', v_count);
  raise notice 'PASS [1/12] baseline: review_resolutions_applied = 0 (no active resolutions)';

  -- CHECK 2/12: queue has 6 entries (5 obs + 1 position)
  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 6,
    format('FAIL [2/12] baseline: queue count expected 6, got %s', v_count);
  raise notice 'PASS [2/12] baseline: queue count = 6';

  -- CHECK 3/12: key observations present in queue with correct reasons
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4'
    and reason         = 'suspected_summary_row';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b4 suspected_summary_row expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1'
    and reason         = 'suspected_duplicate';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b1 suspected_duplicate expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3'
    and reason         = 'missing_evidence_link';
  assert v_count = 1,
    format('FAIL [3/12] baseline: b3 missing_evidence_link expected 1, got %s', v_count);

  raise notice 'PASS [3/12] baseline: b4 (suspected_summary_row), b1 (suspected_duplicate), b3 (missing_evidence_link) all in queue';
end $$;


-- =============================================================================
-- STEP 2: Insert confirm_observation resolution for obs b4.
--
-- Reviewer acknowledges that the b4 summary row ($1,479.08) is correctly
-- classified as a summary aggregate and should be suppressed from the queue.
-- Queue-only effect: the observation still classifies as suspected_summary_row
-- and does not change ledger events or positions.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b4',
  'confirm_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer confirms b4 ($1,479.08 summary_total) is correctly classified as a suspected_summary_row — no queue entry needed'
);


-- =============================================================================
-- STEP 3: Second run — confirm_observation active.
--
-- Expected:
--   review_resolutions_applied = 1
--   queue count                = 5 (b4 suppressed from queue; b1/b2/b3/b5 + position)
--   b4 NOT in queue
--   b1 still in queue
--   b3 still in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 4/12: 1 resolution loaded; queue drops to 5
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 1,
    format('FAIL [4/12] confirm: review_resolutions_applied expected 1, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 5,
    format('FAIL [4/12] confirm: queue count expected 5, got %s', v_count);
  raise notice 'PASS [4/12] confirm: review_resolutions_applied = 1; queue count = 5';

  -- CHECK 5/12: b4 absent from queue; b1 and b3 still present
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4';
  assert v_count = 0,
    format('FAIL [5/12] confirm: b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1';
  assert v_count = 1,
    format('FAIL [5/12] confirm: b1 expected still in queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3';
  assert v_count = 1,
    format('FAIL [5/12] confirm: b3 expected still in queue, got count=%s', v_count);

  raise notice 'PASS [5/12] confirm: b4 absent from queue; b1 and b3 still present';
end $$;


-- =============================================================================
-- STEP 4: Insert reject_observation resolution for obs b3 (queue-suppression case).
--
-- Reviewer marks b3 ($265.36 null_check_crossbleed) as entirely invalid —
-- it has no check identifier and cannot be traced to a real disbursement.
-- b3 was already excluded by Phase 1 Rule 1 (is_superseded=true). This proves
-- that reject suppresses Phase 1 classification and queue routing for an
-- already-problematic observation.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b3',
  'reject_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer marks b3 ($265.36 null_check_crossbleed) as invalid — no check identifier, cannot trace to a real disbursement; exclude from reconciliation'
);


-- =============================================================================
-- STEP 5: Third run — confirm + reject(b3) active.
--
-- Expected:
--   review_resolutions_applied = 2
--   queue count                = 4 (b3 and b4 gone; b1/b2/b5 + position)
--   b3 NOT in queue
--   b4 NOT in queue
--   b1 still in queue
--   b2 still in queue
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 6/12: 2 resolutions loaded; queue drops to 4
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 2,
    format('FAIL [6/12] reject-b3: review_resolutions_applied expected 2, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 4,
    format('FAIL [6/12] reject-b3: queue count expected 4, got %s', v_count);
  raise notice 'PASS [6/12] reject-b3: review_resolutions_applied = 2; queue count = 4';

  -- CHECK 7/12: b3 and b4 absent; b1 and b2 still present
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b3';
  assert v_count = 0,
    format('FAIL [7/12] reject-b3: b3 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b4';
  assert v_count = 0,
    format('FAIL [7/12] reject-b3: b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b1';
  assert v_count = 1,
    format('FAIL [7/12] reject-b3: b1 expected still in queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id = '0b500000-0000-4000-8000-0000000000b2';
  assert v_count = 1,
    format('FAIL [7/12] reject-b3: b2 expected still in queue, got count=%s', v_count);

  raise notice 'PASS [7/12] reject-b3: b3 and b4 absent; b1 and b2 still present';
end $$;


-- =============================================================================
-- STEP 6: Insert mark_duplicate resolution for obs b1 → canonical a1.
--
-- Reviewer identifies b1 (OCR variant check ref "O000447412") as a duplicate
-- of canonical observation a1 (check "0000447412", $510.40). On the next run
-- Phase 1 suppresses b1 entirely (same mechanism as reject_observation).
-- target_observation_id records the canonical for audit.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  target_observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000b1',
  '0b500000-0000-4000-8000-0000000000a1',
  'mark_duplicate',
  'observation',
  'test_runner',
  'Synthetic: b1 (OCR variant "O000447412") is a duplicate of canonical obs a1 (check "0000447412", $510.40) — suppress b1 from reconciliation'
);


-- =============================================================================
-- STEP 7: Fourth run — three resolutions active (confirm b4, reject b3, dup b1).
--
-- Expected:
--   review_resolutions_applied = 3
--   queue count                = 3 (b1/b3/b4 gone; b2/b5 + position remain)
--   b1 NOT in queue
--   b3 NOT in queue
--   b4 NOT in queue
--   mark_duplicate resolution row has target_observation_id = a1
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 8/12: 3 resolutions loaded; queue drops to 3
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 3,
    format('FAIL [8/12] duplicate: review_resolutions_applied expected 3, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [8/12] duplicate: queue count expected 3, got %s', v_count);
  raise notice 'PASS [8/12] duplicate: review_resolutions_applied = 3; queue count = 3';

  -- CHECK 9/12: b1/b3/b4 absent; target_observation_id wired to a1
  select count(*) into v_count
  from fte_review_queue
  where practice_id    = 'c0000000-0000-4000-8000-0000000000fe'
    and observation_id in (
      '0b500000-0000-4000-8000-0000000000b1',
      '0b500000-0000-4000-8000-0000000000b3',
      '0b500000-0000-4000-8000-0000000000b4'
    );
  assert v_count = 0,
    format('FAIL [9/12] duplicate: b1/b3/b4 expected absent from queue, got count=%s', v_count);

  select count(*) into v_count
  from fte_review_resolutions
  where practice_id           = 'c0000000-0000-4000-8000-0000000000fe'
    and action                = 'mark_duplicate'
    and observation_id        = '0b500000-0000-4000-8000-0000000000b1'
    and target_observation_id = '0b500000-0000-4000-8000-0000000000a1'
    and is_superseded         = false;
  assert v_count = 1,
    format('FAIL [9/12] duplicate: mark_duplicate row with target=a1 expected 1, got %s', v_count);

  raise notice 'PASS [9/12] duplicate: b1/b3/b4 absent from queue; mark_duplicate target_observation_id = a1';
end $$;


-- =============================================================================
-- STEP 8: Idempotency — fifth run, all-resolved state persists unchanged.
-- =============================================================================
do $$
declare
  v_result    jsonb;
  v_count     int;
  v_run_count int;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 3,
    format('FAIL [10/12] idempotency: review_resolutions_applied expected 3, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [10/12] idempotency: queue count expected 3, got %s', v_count);

  -- fte_analysis_runs is append-only; assert delta from baseline (5 reconciler
  -- calls so far in this transaction — steps 1, 3, 5, 7, 8).
  select count(*) - (select run_count from _vor_baseline)
    into v_run_count
  from   fte_analysis_runs
  where  practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and  run_type    = 'reconciler';

  assert v_run_count >= 5,
    format('FAIL [10/12] idempotency: analysis_runs expected advance of >= 5, got %s', v_run_count);

  raise notice 'PASS [10/12] idempotency: fifth run — resolutions_applied = 3; queue = 3; analysis_runs advanced by %', v_run_count;
end $$;


-- =============================================================================
-- STEP 9: Insert reject_observation for obs a3 (ledger-impact case).
--
-- a3 is a TRUSTED contractual_adjustment observation ($209.60). In the baseline
-- it emits a contractual_adjustment_applied event and brings the claim's
-- open_balance to $0.00 (billed $720 − adj $209.60 − paid $510.40 = $0.00).
-- Rejecting a3 removes it from Phase 1 entirely: no event is emitted and the
-- position recalculates without the adjustment.
--
-- Unlike b3 (already excluded), this proves reject_observation has financial
-- consequences when applied to a trusted observation.
-- =============================================================================
insert into fte_review_resolutions (
  practice_id,
  observation_id,
  action,
  target_type,
  resolved_by,
  notes
) values (
  'c0000000-0000-4000-8000-0000000000fe',
  '0b500000-0000-4000-8000-0000000000a3',
  'reject_observation',
  'observation',
  'test_runner',
  'Synthetic: reviewer rejects a3 ($209.60 contractual_adjustment) — proves ledger-impact suppression of a trusted observation'
);


-- =============================================================================
-- STEP 10: Sixth run — all four resolutions active; ledger-impact of a3 reject.
--
-- Expected:
--   review_resolutions_applied = 4
--   queue count                = 3 (unchanged — a3 was trusted, never queued)
--   contractual_adjustment_applied event for CLM-AZ-0001: count = 0
--   financial position open_balance for CLM-AZ-0001: $209.60
--     (billed $720.00 − paid $510.40; adjustment no longer applied)
--   position reconciliation_status: still 'in_review'
--     (b5 late_retry ambiguity is unresolved; payment_applied remains 'ambiguous')
-- =============================================================================
do $$
declare
  v_result     jsonb;
  v_count      int;
  v_balance    numeric;
  v_pos_status text;
begin
  select fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe') into v_result;

  -- CHECK 11/12: contractual_adjustment_applied event absent; queue count unchanged
  v_count := (v_result->>'review_resolutions_applied')::int;
  assert v_count = 4,
    format('FAIL [11/12] reject-a3: review_resolutions_applied expected 4, got %s', v_count);

  select count(*) into v_count
  from fte_claim_events ce
  join fte_claims c on c.id = ce.claim_id
  where ce.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and ce.event_type  = 'contractual_adjustment_applied'
    and c.claim_number = 'CLM-AZ-0001';
  assert v_count = 0,
    format('FAIL [11/12] reject-a3: contractual_adjustment_applied expected 0, got %s', v_count);

  select count(*) into v_count
  from fte_review_queue
  where practice_id = 'c0000000-0000-4000-8000-0000000000fe';
  assert v_count = 3,
    format('FAIL [11/12] reject-a3: queue count expected still 3, got %s', v_count);

  raise notice 'PASS [11/12] reject-a3: resolutions_applied = 4; contractual_adjustment_applied absent; queue = 3';

  -- CHECK 12/12: financial position recalculates without the adjustment
  select fp.open_balance_amount, fp.reconciliation_status
    into v_balance, v_pos_status
  from fte_financial_positions fp
  join fte_claims c on c.id = fp.claim_id
  where fp.practice_id = 'c0000000-0000-4000-8000-0000000000fe'
    and c.claim_number = 'CLM-AZ-0001';

  assert v_balance = 209.60,
    format('FAIL [12/12] reject-a3: open_balance expected 209.60, got %s', v_balance);
  assert v_pos_status = 'in_review',
    format('FAIL [12/12] reject-a3: position expected in_review (b5 ambiguity persists), got %s', v_pos_status);

  raise notice 'PASS [12/12] reject-a3: open_balance = $209.60 (adj removed); position = in_review (b5 ambiguity persists)';
end $$;


-- =============================================================================
-- STEP 11: Rollback — all reconciler output and resolution rows discarded.
-- The fixture data (fte_observations, fte_claims, fte_evidence) and any
-- seed_fixture fte_analysis_runs rows were committed by the fixture's own
-- begin/commit and are NOT rolled back here.
-- =============================================================================
rollback;
```

---

## 9. README updates

### `tests/README.md` — add after the `validate_review_resolution.sql` section

```markdown
---

## `validate_observation_resolution.sql`

Targeted validation of the `confirm_observation`, `reject_observation`, and
`mark_duplicate` resolution paths introduced in Task 004C. Covers only the
ccdbe216 fixture practice. Inserts three cumulative resolutions (steps 1–8) to
verify queue count drops from 6 → 5 → 4 → 3, then adds a separate ledger-impact
case (steps 9–10) that rejects a trusted observation and asserts event suppression
and financial position recalculation.

### What it asserts (10 steps + rollback, 12 checks)

| Step | # | Check |
|---|---|---|
| 1 — baseline | 1/12 | Return JSON: `review_resolutions_applied = 0` |
| 1 — baseline | 2/12 | Queue count = 6 (5 observation entries + `unbalanced_financial_position`) |
| 1 — baseline | 3/12 | b4 (`suspected_summary_row`), b1 (`suspected_duplicate`), b3 (`missing_evidence_link`) all in queue |
| 3 — confirm | 4/12 | Return JSON: `review_resolutions_applied = 1`; queue count = 5 |
| 3 — confirm | 5/12 | b4 NOT in queue; b1 and b3 still present |
| 5 — reject b3 | 6/12 | Return JSON: `review_resolutions_applied = 2`; queue count = 4 |
| 5 — reject b3 | 7/12 | b3 NOT in queue; b4 NOT in queue; b1 and b2 still present |
| 7 — mark dup | 8/12 | Return JSON: `review_resolutions_applied = 3`; queue count = 3 |
| 7 — mark dup | 9/12 | b1/b3/b4 NOT in queue; `mark_duplicate` resolution has `target_observation_id = a1` |
| 8 — idempotency | 10/12 | Fifth call: `review_resolutions_applied = 3`, queue = 3, analysis_runs advanced by ≥ 5 |
| 10 — reject a3 (ledger) | 11/12 | `review_resolutions_applied = 4`; `contractual_adjustment_applied` event count = 0; queue still = 3 |
| 10 — reject a3 (ledger) | 12/12 | Position `open_balance_amount = $209.60`; `reconciliation_status = 'in_review'` |

### Key behavioral invariants verified

- **confirm_observation** is queue-only: the observation still enters
  `_fte_classified` with its original classification, still influences ledger
  events and positions as in the baseline, and only the queue entry is
  suppressed. Confirmed by checks 4-5 (b4 absent from queue; ledger unchanged).
- **reject_observation** suppresses Phase 1 entirely for the target observation:
  - Queue-suppression case (b3): already-excluded observation is no longer routed
    to the queue (checks 6-7).
  - Ledger-impact case (a3): trusted observation's `contractual_adjustment_applied`
    event is absent; position `open_balance_amount` recalculates from $0.00 to
    $209.60 (checks 11-12).
- **mark_duplicate** uses the same Phase 1 suppression as reject_observation.
  `target_observation_id` FK to the canonical observation is verified in check 9.
- **`unbalanced_financial_position`** persists throughout checks 1–11 because b5's
  `late_retry_page_contradiction` is never resolved — Phase 5 always marks
  payment_applied 'ambiguous'. Position status remains 'in_review' even after
  rejecting a3 (confirmed in check 12).
- **Resolution rows survive Phase 0** — all four rows are still present after
  each reconciler call.
- **`fte_analysis_runs` is append-only** — run-count assertion uses delta logic
  (baseline captured at transaction start in `_vor_baseline` temp table).

### How to run

```bash
# Prerequisites: migrations 001 + 002 + 003 applied; function registered

psql "$DATABASE_URL" -f tests/validate_observation_resolution.sql
```

Supabase SQL editor note: `\i` is psql-only. Paste
`fixtures/synthetic_ccdbe216_failure_modes.sql` first (execute separately so it
commits), then paste the body of this file starting from the `begin;` block.

### Expected output

Twelve `PASS [n/12] …` `NOTICE` lines. The outer `ROLLBACK` discards all
reconciler output and the four resolution rows; fixture entity data is
unaffected.
```

### `reconciler/README.md` — updates

**Section 3 (phase table)** — update Phase 0.5 row and Phase 2 row:

Phase 0.5 (replace existing row):
> **0.5** | Load active review resolutions: snapshot non-superseded `fte_review_resolutions` rows for this practice into temp table `_fte_active_resolutions ON COMMIT DROP`. Then build `_fte_suppressed_observations ON COMMIT DROP` — a set of `observation_id` values where `action IN ('reject_observation', 'mark_duplicate')`; Phase 1 filters these out with `NOT EXISTS`. Zero rows in either temp table is valid — downstream phases behave identically to the no-resolution baseline. `GET DIAGNOSTICS` captures the resolution row count for `review_resolutions_applied` in the return JSON.

Phase 2 (replace existing row):
> **2** | Route EXCLUDED and SUSPECT observations to `fte_review_queue`, excluding any observation where an active `confirm_observation` resolution exists (`NOT EXISTS` on `_fte_active_resolutions`). `confirm_observation` is queue-only: the observation retains its classification and continues to influence ledger events and positions. Observations suppressed in Phase 0.5 (`reject_observation` / `mark_duplicate`) never enter `_fte_classified` and are therefore never routed here.

**Section 5 (How to extend)** — add a new subsection after "New review reason":

```markdown
**New observation-level resolution action:**

The `fte_review_resolutions.action` CHECK constraint already contains all
planned action types. To make a new observation-level action affect reconciler
behaviour:

1. If the action should suppress the observation from Phase 1 (same as
   `reject_observation` / `mark_duplicate`), add the action name to the
   `WHERE action IN (...)` filter in Phase 0.5's `_fte_suppressed_observations`
   CREATE.
2. If the action should allow the observation to classify but skip the queue
   (same as `confirm_observation`), add a `NOT EXISTS` branch to Phase 2's
   WHERE clause.
3. If the action requires a typed FK to another observation, add it in a new
   migration (as `target_observation_id` was added in migration 003 for
   `mark_duplicate`), including the constraint that it must be NULL for all
   other action types.
4. Add a fixture resolution INSERT and new assertions in
   `tests/validate_observation_resolution.sql`.
```

---

## 10. Invariants to preserve

- No existing migrations are modified.
- No existing fixtures are modified.
- Existing regression tests remain unchanged and must still pass:
  - `validate_reconciler.sql` (12 checks): runs with no resolutions in the
    table, so Phase 0.5 produces empty temp tables and Phase 1/Phase 2 behave
    identically to the pre-004C baseline.
  - `validate_review_resolution.sql` (7 checks): inserts only a
    `confirm_payment_event` resolution, which is not in the
    `_fte_suppressed_observations` action list and is not `confirm_observation`,
    so neither the Phase 1 `NOT EXISTS` filter nor the Phase 2 `NOT EXISTS` filter
    touches its path.
- `fte_analysis_runs` remains append-only; baseline temp table name is
  `_vor_baseline` (distinct from `_vrr_baseline` in validate_review_resolution)
  to allow both tests to run in the same Postgres session without conflict.

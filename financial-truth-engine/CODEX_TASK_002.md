# Codex Task 002 — Deterministic Observation-to-Ledger Reconciler

**Status:** Ready for Codex  
**Created:** 2026-06-19  
**Scope:** Financial Truth Engine only  
**Depends on:** Task 001 (schema + fixtures must be applied first)  
**Do not modify:** Existing EOB extraction project / any legacy `eob_*` table

---

## Task Summary

Task 001 created the schema and synthetic fixtures that demonstrate the five known
EOB failure modes. The fixtures hand-authored the "correct answer" — which events,
positions, and review queue entries a reconciler should produce.

This task builds the reconciler itself: a deterministic Postgres stored procedure
(`fte_reconcile_practice`) that reads only from the read-only evidence/observation
tables and re-derives every event, position, and review entry from scratch. Running
it against the fixtures should reproduce (structurally and quantitatively) the
expected outputs described in this document.

Deliverables:

1. `reconciler/fte_reconcile.sql` — the stored procedure
2. `reconciler/README.md` — architecture note
3. `tests/validate_reconciler.sql` — runs the reconciler against both fixtures and
   asserts correctness

Do not build UI.  
Do not write extraction/AI code.  
Do not modify the schema migration.  
Do not modify the fixture files.  
Do not commit PHI.

---

## Architecture Principle

```text
Evidence -> Observations -> [RECONCILER] -> Claim Events -> Financial Positions
                                       \-> Review Queue (ambiguity is explicit)
```

The reconciler is the only process that may write to:
- `fte_claim_events`
- `fte_event_evidence`
- `fte_financial_positions`
- `fte_review_queue`

It reads (never modifies):
- `fte_observations`
- `fte_evidence`
- `fte_claims`
- `fte_practices`

AI observations are not financial truth. The reconciler promotes only trusted
observations to events, and routes all suspect/ambiguous observations to the
review queue without silently discarding them.

---

## Required Files

```text
financial-truth-engine/
  reconciler/
    fte_reconcile.sql          <- stored procedure (CREATE OR REPLACE FUNCTION)
    README.md                  <- brief architecture note
  tests/
    validate_reconciler.sql    <- asserts expected outputs for both fixtures
```

---

## Stored Procedure Specification

### Signature

```sql
CREATE OR REPLACE FUNCTION fte_reconcile_practice(p_practice_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
```

Returns a summary JSON object with run metrics (counts of events emitted, review
entries created, positions materialized, etc.).

The function must be idempotent: calling it twice in a row produces the same
result. It achieves this by deleting all derived rows for the practice at the top
of Phase 0.

### Phase 0 — Clear derived tables (idempotent reset)

Delete in dependency order so FK constraints are not violated:

```sql
DELETE FROM fte_event_evidence      WHERE practice_id = p_practice_id;
DELETE FROM fte_review_queue        WHERE practice_id = p_practice_id;
DELETE FROM fte_financial_positions WHERE practice_id = p_practice_id;
DELETE FROM fte_claim_events        WHERE practice_id = p_practice_id;
```

Do not touch `fte_observations`, `fte_evidence`, `fte_claims`, or
`fte_practices`.

---

## Observation Classification Rules

Every `fte_observations` row for the practice is classified into exactly one
of three buckets: **TRUSTED**, **SUSPECT**, or **EXCLUDED**.

Apply the rules in order; the first matching rule wins.

### Rule 1 — Superseded (EXCLUDED)

```
is_superseded = true
```

The observation was explicitly flagged as superseded at ingestion time.
Never emit an event from it. Route it to the review queue using the reason
derived from `metadata->>'failure_mode'`:

| `failure_mode` value              | review reason                  |
|-----------------------------------|--------------------------------|
| `phantom_duplicate_check_ref`     | `suspected_duplicate`          |
| `section_delimiter_double_count`  | `conflicting_observations`     |
| `null_check_crossbleed`           | `missing_evidence_link`        |
| `late_retry_page_contradiction`   | `late_retry_page_contradiction`|
| `check_spacing_variant_fragmentation` (with `metadata->>'retry_pending' = 'true'`) | `late_retry_page_contradiction` |
| anything else / null              | `conflicting_observations`     |

### Rule 2 — Summary row (EXCLUDED)

```
is_summary_row = true  AND  is_superseded = false
```

Aggregate totals must never become payment events. Route to
`suspected_summary_row` review. Record the aggregate amount in the review
details so a human can cross-check.

### Rule 3 — Null-check payment (EXCLUDED)

```
observation_type = 'payment'
AND check_eft_identifier IS NULL
AND is_superseded = false
AND is_summary_row = false
```

A payment with no associated check/EFT identifier cannot be attributed to a
payment group. Route to `missing_evidence_link` review. Do not emit a payment
event.

### Rule 4 — Labeled low-confidence observation (SUSPECT → REVIEW)

```
metadata->>'failure_mode' IS NOT NULL
AND metadata->>'failure_mode' != ''
AND is_superseded = false
AND is_summary_row = false
```

The observation carries an explicit failure-mode label even though it was not
superseded. This occurs when the AI flagged the problem but the document
processor did not suppress it. Treat as SUSPECT; do not emit an event. Route to
review using the same `failure_mode`→reason table from Rule 1 (omit the
`retry_pending` sub-case since these are not superseded).

### Rule 5 — Trusted (TRUSTED)

All observations not caught by Rules 1–4 are TRUSTED and eligible for event
emission (subject to the per-type rules below).

---

## Event Emission Rules

Apply only to TRUSTED observations.

### 5a — `billed_amount` → `claim_adjudicated`

For each TRUSTED observation where `observation_type = 'billed_amount'`:

1. Resolve the claim via `fte_claims` where
   `claim_number = obs.claim_identifier AND practice_id = p_practice_id`.
2. Insert one `fte_claim_events` row:
   - `event_type = 'claim_adjudicated'`
   - `event_date = obs.service_date`
   - `amount = obs.amount`
   - `amount_type = 'billed'`
   - `payer_name = obs.payer_name`
   - `confidence_score = obs.confidence_score`
   - `reconciliation_status = 'reconciled'`
3. Insert one `fte_event_evidence` row linking the event to `obs.evidence_id`
   and `obs.id`.

### 5b — `contractual_adjustment` → `contractual_adjustment_applied`

For each TRUSTED observation where
`observation_type = 'contractual_adjustment'`:

Same pattern as 5a with `event_type = 'contractual_adjustment_applied'` and
`amount_type = 'contractual_adjustment'`. Carry `carc_code = obs.carc_code`.

### 5c — `payment` → `payment_applied`

For each TRUSTED observation where `observation_type = 'payment'`:

1. Resolve the claim as above.
2. Insert `fte_claim_events` with `event_type = 'payment_applied'`,
   `reconciliation_status = 'reconciled'` (initially; see Phase 5 below for
   contradiction detection that may change this to `'ambiguous'`).
3. Link to `obs.evidence_id` and `obs.id` via `fte_event_evidence`.
4. If the observation's `evidence_id` points to evidence of type
   `check_payment` (or if a `check_payment` evidence row exists in the same
   practice with metadata `check_number` matching `obs.check_eft_identifier`),
   add a second `fte_event_evidence` row for that check stub with
   `link_role = 'supports'`.

---

## Late/Retry Contradiction Detection (Phase 5)

After all payment events are emitted, scan the review queue entries that were
created with reason `late_retry_page_contradiction`. For each such entry:

1. Identify the claim referenced by the review entry's linked observation
   (`obs.claim_identifier` → `fte_claims.claim_number`).
2. Find any `payment_applied` event for that claim that was emitted in Phase 4.
3. Update that event's `reconciliation_status` to `'ambiguous'`.
4. Update the review entry's `claim_event_id` to point to that event (so a
   human can see the contradiction in context).
5. Add a second `fte_event_evidence` row for the payment event, linking it to
   the contradicting observation with `link_role = 'contradicts'`.

---

## Financial Position Derivation (Phase 6)

After all claim events are emitted, materialize one `fte_financial_positions`
row for **every claim** that meets either condition:

1. The claim has at least one emitted `fte_claim_events` row, **or**
2. The claim has at least one suspect or excluded observation (i.e., a row
   in `fte_observations` that was routed to `fte_review_queue` by Rules 1–4).

Omitting a position row is never acceptable. A claim with no emitted events
but with at least one review-queued observation receives an `in_review`
position with `billed_amount = NULL`, `paid_amount = NULL`,
`open_balance_amount = NULL`, `reconciliation_status = 'in_review'`, and
`position_confidence_score = 0.00`.

Aggregate from `fte_claim_events` for the claim:

```
billed_amount              = SUM(amount) WHERE amount_type = 'billed'
contractual_adjustment_amt = SUM(amount) WHERE amount_type = 'contractual_adjustment'
paid_amount                = SUM(amount) WHERE event_type = 'payment_applied'
denied_amount              = SUM(amount) WHERE event_type = 'denial_posted'
patient_responsibility_amt = SUM(amount) WHERE amount_type = 'patient_responsibility'
```

Calculate the mathematical balance:

```
expected_paid = billed_amount
              - contractual_adjustment_amount
              - patient_responsibility_amount
              - denied_amount

gap = ABS(expected_paid - paid_amount)
```

Derive `reconciliation_status`:

| Condition                                                        | status       |
|------------------------------------------------------------------|--------------|
| No events for this claim at all                                  | `in_review`  |
| gap > 0.01                                                       | `unbalanced` |
| gap ≤ 0.01 AND any linked payment event has status `'ambiguous'` | `in_review`  |
| gap ≤ 0.01 AND no ambiguous events                               | `balanced`   |

Set `open_balance_amount`:

- `in_review`: NULL (cannot compute without events)
- `unbalanced`: `gap` (positive number)
- `ambiguous` or `balanced`: 0

Set `position_confidence_score` to the minimum `confidence_score` among all
events linked to this claim (capped to the range [0, 1]).

Set `last_reconciled_at = now()`.

---

## Unbalanced Position → Review Queue (Phase 7)

For every position where `reconciliation_status IN ('unbalanced', 'in_review')`:

Insert one `fte_review_queue` row with `reason = 'unbalanced_financial_position'`
and a details JSONB that includes the gap amount and the reconciliation status.

---

## Short-Pay Detection (Phase 8)

For every position where `reconciliation_status = 'unbalanced'` and
`open_balance_amount > 0`:

Insert one additional `fte_claim_events` row with
`event_type = 'short_pay_detected'` and `amount = open_balance_amount`. This
event should have `reconciliation_status = 'unbalanced'` and
`reason_category = 'underpayment'`.

Link it to the same evidence as the `claim_adjudicated` event for the same
claim (if one exists).

---

## Analysis Run Record (Phase 9)

At the end of the function, insert one `fte_analysis_runs` row with:

- `run_type = 'reconcile'`
- `status = 'succeeded'` (or `'failed'` if the function raised an exception
  and is being run in a EXCEPTION block)
- `summary`: a brief text summary including counts from the return JSON

Return the summary JSON.

---

## Expected Reconciler Outputs

These are the expected outputs when the reconciler is run against each fixture
practice from a CLEAN state (derived tables wiped). Use these as the assertions
in `tests/validate_reconciler.sql`.

### Fixture: ccdbe216 (practice `c0000000-0000-4000-8000-0000000000fe`)

**Source observations:** 8 total — 3 trusted, 5 suspect/excluded

Trusted observations (Rules 1–4 do NOT apply):
- `a1` payment 510.40 (confidence 0.97) — check `0000447412`
- `a2` billed_amount 720.00 (confidence 0.96)
- `a3` contractual_adjustment 209.60 CO-45 (confidence 0.95)

Suspect/excluded observations:
- `b1` is_superseded=true, failure_mode=`phantom_duplicate_check_ref` → Rule 1
- `b2` is_superseded=true, failure_mode=`section_delimiter_double_count` → Rule 1
- `b3` is_superseded=true, failure_mode=`null_check_crossbleed` → Rule 1
- `b4` is_summary_row=true → Rule 2
- `b5` is_superseded=true, failure_mode=`late_retry_page_contradiction` → Rule 1

**Expected claim events (3):**

| event_type                   | amount  | status       |
|------------------------------|---------|--------------|
| `claim_adjudicated`          | 720.00  | `reconciled` |
| `contractual_adjustment_applied` | 209.60 | `reconciled` |
| `payment_applied`            | 510.40  | `ambiguous`  |

The payment event is `ambiguous` because Phase 5 detects b5 as a
`late_retry_page_contradiction` review entry for the same claim.

**Expected review queue entries (6):**

| reason                        | linked observation |
|-------------------------------|--------------------|
| `suspected_duplicate`         | b1                 |
| `conflicting_observations`    | b2                 |
| `missing_evidence_link`       | b3                 |
| `suspected_summary_row`       | b4                 |
| `late_retry_page_contradiction` | b5               |
| `unbalanced_financial_position` | (no observation) |

**Expected financial position:**

| field                         | value       |
|-------------------------------|-------------|
| `billed_amount`               | 720.00      |
| `contractual_adjustment_amount` | 209.60    |
| `paid_amount`                 | 510.40      |
| `open_balance_amount`         | 0           |
| `reconciliation_status`       | `in_review` |
| `position_confidence_score`   | 0.95 (min of event confidence scores) |

No `short_pay_detected` event (gap = 0).

---

### Fixture: 96c5c357 (practice `96000000-0000-4000-8000-0000000000fe`)

**Claim CLM-APC-1000 — large gap**

Trusted:
- billed_amount 1600.00 (confidence 0.95)
- payment 351.89 (confidence 0.93)

**Expected events (3):**

| event_type              | amount   | status        |
|-------------------------|----------|---------------|
| `claim_adjudicated`     | 1600.00  | `reconciled`  |
| `payment_applied`       | 351.89   | `reconciled`  |
| `short_pay_detected`    | 1248.11  | `unbalanced`  |

No late/retry contradiction for this claim → payment stays `reconciled`.

**Expected review queue (1):**

| reason                         |
|--------------------------------|
| `unbalanced_financial_position` |

**Expected financial position:**

| field                       | value         |
|-----------------------------|---------------|
| `billed_amount`             | 1600.00       |
| `paid_amount`               | 351.89        |
| `open_balance_amount`       | 1248.11       |
| `reconciliation_status`     | `unbalanced`  |
| `position_confidence_score` | 0.93          |

---

**Claim CLM-APC-2000 — check fragmentation**

All three observations (b1, b2, b3) are suspect or excluded:
- b1: `failure_mode = 'check_spacing_variant_fragmentation'`, not superseded → Rule 4 → SUSPECT
- b2: `failure_mode = 'check_spacing_variant_fragmentation'`, not superseded → Rule 4 → SUSPECT
- b3: `is_superseded = true`, `failure_mode = 'check_spacing_variant_fragmentation'`,
  `retry_pending = true` → Rule 1 → `late_retry_page_contradiction`

**Expected events (0):** no events emitted for CLM-APC-2000.

**Expected review queue (3):**

| reason                          | linked observation |
|---------------------------------|--------------------|
| `suspected_duplicate`           | b1                 |
| `suspected_duplicate`           | b2                 |
| `late_retry_page_contradiction` | b3                 |
| `unbalanced_financial_position` | (no observation)   |

Wait — 4 entries total: 3 from suspect observations + 1 from Phase 7 (since no
events means `in_review`). Validation should assert exactly 4 review entries for
this claim.

**Expected financial position:**

| field                       | value       |
|-----------------------------|-------------|
| `billed_amount`             | NULL        |
| `paid_amount`               | NULL        |
| `open_balance_amount`       | NULL        |
| `reconciliation_status`     | `in_review` |
| `position_confidence_score` | 0.00 (no events → no confidence) |

Position confidence 0.00 because there are no emitted events to derive a minimum
from. The reconciler **must** create this position row. Omitting it is not
acceptable. CLM-APC-2000 has three suspect/excluded observations (b1, b2, b3)
that all reached the review queue, satisfying the Rule-2 condition for position
materialization even with zero emitted events.

---

## Validation Script Requirements

`tests/validate_reconciler.sql` must:

1. **Load fixtures:** Call or inline the two fixture files. If the fixture SQL
   files are already applied to the database, re-run them (they are idempotent)
   to ensure a clean starting state for all seeded data including events and
   positions.

2. **Wipe derived tables for both practices**, then call the reconciler for each:

   ```sql
   -- Wipe derived tables for ccdbe216 practice
   DELETE FROM fte_event_evidence      WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_review_queue        WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_financial_positions WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_claim_events        WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe';

   SELECT fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe');

   -- Same for 96c5c357
   DELETE FROM fte_event_evidence      WHERE practice_id = '96000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_review_queue        WHERE practice_id = '96000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_financial_positions WHERE practice_id = '96000000-0000-4000-8000-0000000000fe';
   DELETE FROM fte_claim_events        WHERE practice_id = '96000000-0000-4000-8000-0000000000fe';

   SELECT fte_reconcile_practice('96000000-0000-4000-8000-0000000000fe');
   ```

3. **Assert using DO $$ blocks** (same pattern as `tests/validate_schema.sql`).
   Each check should RAISE NOTICE on pass or RAISE EXCEPTION on failure.

4. **Wrap all assertions in a single transaction and ROLLBACK** at the end so
   the validation script leaves the database in the post-fixture state.

Minimum assertions (implement as numbered checks):

**Check 1 — ccdbe216: exactly 3 claim events emitted**

```sql
ASSERT (SELECT COUNT(*) FROM fte_claim_events
        WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe') = 3;
```

**Check 2 — ccdbe216: payment event is 'ambiguous'**

```sql
ASSERT (SELECT reconciliation_status FROM fte_claim_events
        WHERE practice_id = 'c0000000-0000-4000-8000-0000000000fe'
          AND event_type = 'payment_applied') = 'ambiguous';
```

**Check 3 — ccdbe216: exactly 6 review queue entries**

**Check 4 — ccdbe216: all 7 required reason types present across both practices**
(Re-use the reason list from `validate_schema.sql` check 5.)

**Check 5 — ccdbe216: no event was emitted from a suspect observation**

For every suspect observation (b1–b5), assert there is no `fte_event_evidence`
row linking to that observation as `link_role = 'supports'` or `'derived_from'`.
`link_role = 'contradicts'` is allowed for b5.

**Check 6 — ccdbe216: position is in_review (payment event is ambiguous), not balanced or unbalanced**

**Check 7 — 96c5c357 CLM-APC-1000: payment_applied event amount = 351.89**

**Check 8 — 96c5c357 CLM-APC-1000: short_pay_detected event amount = 1248.11**

**Check 9 — 96c5c357 CLM-APC-1000: open_balance_amount = 1248.11**

**Check 10 — 96c5c357 CLM-APC-2000: 0 payment events**

```sql
ASSERT (SELECT COUNT(*) FROM fte_claim_events
        WHERE practice_id = '96000000-0000-4000-8000-0000000000fe'
          AND claim_id = 'c1a90000-0000-4000-8000-000000002000'
          AND event_type = 'payment_applied') = 0;
```

**Check 11 — 96c5c357 CLM-APC-2000: position row exists with in_review status and NULL monetary fields**

The position must be present — CLM-APC-2000 has no emitted events but has
three review-queued observations, so Rule-2 requires materialization.

```sql
ASSERT EXISTS (
  SELECT 1 FROM fte_financial_positions
  WHERE practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND claim_id    = 'c1a90000-0000-4000-8000-000000002000'
);

ASSERT (
  SELECT reconciliation_status FROM fte_financial_positions
  WHERE practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND claim_id    = 'c1a90000-0000-4000-8000-000000002000'
) = 'in_review';

ASSERT (
  SELECT billed_amount IS NULL
     AND paid_amount IS NULL
     AND open_balance_amount IS NULL
     AND position_confidence_score = 0.00
  FROM fte_financial_positions
  WHERE practice_id = '96000000-0000-4000-8000-0000000000fe'
    AND claim_id    = 'c1a90000-0000-4000-8000-000000002000'
) = true;
```

**Check 12 — idempotency: calling reconciler twice gives same counts**

Call `fte_reconcile_practice` a second time for each practice inside the
validation transaction and re-assert Checks 1 and 7. The counts must be
identical.

---

## Reconciler README Requirements

`reconciler/README.md` must explain (briefly, in plain English):

1. What the reconciler does and what it does not do.
2. The five observation classification rules and which review reason each maps to.
3. The nine phases of the function.
4. How to run it against the fixtures.
5. How to extend it: adding a new `observation_type` or a new review reason.
6. Why it is a SQL stored procedure rather than application code (answer: it runs
   inside the database transaction boundary, is idempotent, and has direct access
   to all tables without a network round-trip).

---

## Implementation Notes

### SQL CTEs are preferred

Use `WITH` CTEs to compute the observation classification in one step, then join
downstream. This keeps the classification logic in one place and makes it easy to
audit which branch a specific observation took.

Suggested CTE structure:

```sql
WITH
  classified_obs AS (
    SELECT
      o.*,
      CASE
        WHEN o.is_superseded THEN 'excluded_superseded'
        WHEN o.is_summary_row THEN 'excluded_summary'
        WHEN o.observation_type = 'payment' AND o.check_eft_identifier IS NULL THEN 'excluded_null_check'
        WHEN o.metadata->>'failure_mode' IS NOT NULL AND o.metadata->>'failure_mode' != '' THEN 'suspect'
        ELSE 'trusted'
      END AS classification,
      CASE
        WHEN o.metadata->>'failure_mode' = 'phantom_duplicate_check_ref'
          THEN 'suspected_duplicate'
        WHEN o.metadata->>'failure_mode' = 'section_delimiter_double_count'
          THEN 'conflicting_observations'
        WHEN o.metadata->>'failure_mode' = 'null_check_crossbleed'
          THEN 'missing_evidence_link'
        WHEN o.metadata->>'failure_mode' IN ('late_retry_page_contradiction')
          THEN 'late_retry_page_contradiction'
        WHEN o.metadata->>'failure_mode' = 'check_spacing_variant_fragmentation'
         AND (o.metadata->>'retry_pending')::boolean IS TRUE
          THEN 'late_retry_page_contradiction'
        WHEN o.is_summary_row THEN 'suspected_summary_row'
        ELSE 'conflicting_observations'
      END AS review_reason
    FROM fte_observations o
    WHERE o.practice_id = p_practice_id
  ),
  trusted AS (SELECT * FROM classified_obs WHERE classification = 'trusted'),
  suspect AS (SELECT * FROM classified_obs WHERE classification != 'trusted')
  -- continue with INSERT ... SELECT from trusted / suspect CTEs
```

### Claim resolution

When resolving `obs.claim_identifier` to a `fte_claims.id`, do:

```sql
JOIN fte_claims c
  ON c.practice_id = p_practice_id
 AND c.claim_number = obs.claim_identifier
```

If no matching claim is found, skip the observation and log it to the review queue
as `missing_evidence_link` with details explaining the unresolved claim identifier.
Do not raise an exception.

### Confidence floor for positions with no events

When materializing a position for a claim with `in_review` status (no events),
set `position_confidence_score = 0.00` rather than NULL. The column is defined as
`numeric(4,2) not null default 0` in the schema, so this is consistent.

---

## Out of Scope

- No UI, no API endpoint, no Edge Function wrapper (that is Task 003).
- No AI extraction or Gemini calls.
- No real EOB data, no PHI.
- No modifications to migration 001 or the fixture files.
- No handling of ERA/835 segments (those come with a dedicated parser in a later
  task).
- No contract variance detection or denial recovery classification (Task 004).
- No multi-practice batch runs (the function takes one `practice_id` at a time).

---

## Definition of Done

This task is complete when:

1. `reconciler/fte_reconcile.sql` exists and is valid SQL (no syntax errors).
2. `tests/validate_reconciler.sql` passes all 12 checks with NOTICE output and
   no EXCEPTION when run against a database that has migration 001 and both
   fixtures applied.
3. Running the reconciler twice in a row against either fixture produces identical
   counts (idempotency confirmed by Check 12).
4. No event is traceable (via `fte_event_evidence`) to a suspect observation as a
   `supports` or `derived_from` link.
5. All five failure mode observations from each fixture appear in `fte_review_queue`
   with an appropriate reason code.
6. `reconciler/README.md` exists and covers the six points listed above.

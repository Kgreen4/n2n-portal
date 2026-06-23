# Financial Truth Engine — Tests

## `validate_schema.sql`

Structural + behavioral validation of the ledger schema (`../migrations/001_create_financial_truth_schema.sql`).

### What it asserts

| # | Check | How |
|---|---|---|
| 1 | RLS is enabled on every `fte_` table | reads `pg_class.relrowsecurity` |
| 2 | No `fte_` table has a foreign key to a non-`fte_` table (proves isolation from legacy `eob_*`) | reads `pg_constraint` |
| 3 | The event→evidence audit-link constraint exists (an event link must point at evidence and/or an observation) | reads `pg_constraint` |
| 4 | `fte_financial_positions` is claim-scoped (`unique(claim_id)`) and practice-scoped (`practice_id NOT NULL`) | reads `pg_constraint` / `pg_attribute` |
| 5 | Inserting an observation creates **0** financial positions; the audit-link constraint rejects an empty link; `fte_review_queue` accepts all 7 reason types | inserts under a throwaway practice, then `ROLLBACK` |
| 6 | A derived financial position is stored claim+practice scoped, independent of observations | inserts then `ROLLBACK` |

Checks 1–4 read system catalogs and need no data. Checks 5–6 insert synthetic rows
under a temporary validation practice (`ffffffff-0000-4000-8000-0000000000aa`) and the
whole file ends in `ROLLBACK`, so **nothing is persisted**.

### How to run

```bash
# 1) apply the schema
psql "$DATABASE_URL" -f ../migrations/001_create_financial_truth_schema.sql

# 2) validate
psql "$DATABASE_URL" -f validate_schema.sql
```

Run as a role allowed to insert. On Supabase use the `service_role` / `postgres`
connection (both `BYPASSRLS`, so the deny-by-default RLS stubs do not block validation).

### Expected output

Six `PASS [n/6] …` `NOTICE` lines and a final banner. **Any** failed invariant raises an
`EXCEPTION`, which both fails the run and rolls back the transaction.

### Optional: validate the fixtures too

After loading the fixtures you can spot-check the architectural guarantees manually:

```sql
-- Summary rows exist but are flagged, never reconciled as payment events:
select count(*) from fte_observations where is_summary_row;            -- > 0
select count(*) from fte_claim_events e
  join fte_event_evidence ee on ee.claim_event_id = e.id
  join fte_observations o on o.id = ee.observation_id
  where o.is_summary_row and e.event_type = 'payment_applied';         -- expect 0

-- Every ambiguity is visible in review:
select reason, count(*) from fte_review_queue group by reason order by reason;

-- Every claim event has at least one evidence/observation link:
select e.id from fte_claim_events e
  left join fte_event_evidence ee on ee.claim_event_id = e.id
  where ee.id is null;                                                 -- expect 0 rows
```

---

## `validate_reconciler.sql`

End-to-end behavioral validation of `reconciler/fte_reconcile.sql`. Loads both
synthetic fixtures, calls `fte_reconcile_practice()` for each, asserts 12
checks, then rolls everything back.

### What it asserts

| # | Practice | Check |
|---|---|---|
| 1 | ccdbe216 | Exactly 3 claim events emitted (adjudicated + adjustment + payment) |
| 2 | ccdbe216 | `payment_applied` amount = 510.40; `supports` link to observation a1 |
| 3 | ccdbe216 | `check_payment` stub evidence linked to `payment_applied` |
| 4 | ccdbe216 | All 5 observation-derived review_reason types present in queue |
| 5 | ccdbe216 | Position = `in_review`, open_balance = 0.00; `payment_applied` = `ambiguous` (math balances but contradiction is unresolved) |
| 6 | 96c5c357 / CLM-APC-1000 | `claim_adjudicated`, `payment_applied`, and `short_pay_detected` emitted; payment = 351.89 |
| 7 | 96c5c357 / CLM-APC-1000 | Position = `unbalanced`, open_balance = 1248.11 |
| 8 | 96c5c357 / CLM-APC-2000 | 0 events emitted; position = `in_review` with NULL billed |
| 9 | 96c5c357 | b1+b2 → `suspected_duplicate`; b3 → `late_retry_page_contradiction` |
| 10 | both | `fte_analysis_runs` rows created with `status = 'succeeded'` |
| 11 | both | No `fte_event_evidence` row has both `evidence_id` and `observation_id` NULL |
| 12 | both | Idempotency — second reconciler call produces identical ledger state |

### How to run

```bash
# 1) apply migrations
psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql

# 2) register the function
psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql

# 3) validate
psql "$DATABASE_URL" -f tests/validate_reconciler.sql
```

### Expected output

Twelve `PASS [n/12] …` `NOTICE` lines and a final banner. Regression: when no
`fte_review_resolutions` rows exist (baseline), CHECK 5 must still emit
`payment_applied = 'ambiguous'` — the Phase 0.5 empty-table path must not alter
the no-resolution baseline.

---

## `validate_review_resolution.sql`

Targeted validation of the `confirm_payment_event` resolution path introduced
in Task 004B. Covers only the ccdbe216 fixture practice and uses a single
resolution INSERT to move CLM-AZ-0001 from `ambiguous`/`in_review` to
`reconciled`/`balanced`.

### What it asserts (5 steps, 7 checks)

| Step | # | Check |
|---|---|---|
| 1 — baseline | 1/7 | Return JSON: `review_resolutions_applied = 0` |
| 1 — baseline | 2/7 | `payment_applied = 'ambiguous'`, position = `in_review`, `open_balance = 0.00` |
| 1 — baseline | 3/7 | `contradicts` evidence link exists for obs b5 → `payment_applied` |
| 3 — resolved | 4/7 | Return JSON: `review_resolutions_applied = 1`; `payment_applied = 'reconciled'` |
| 3 — resolved | 5/7 | Position = `balanced`, `open_balance = 0.00` |
| 3 — resolved | 6/7 | `contradicts` link preserved; resolution row intact; `run_type='reconciler'` analysis_runs advanced by ≥ 2 |
| 4 — idempotency | 7/7 | Third call: `review_resolutions_applied = 1`, `payment_applied = 'reconciled'`, position = `balanced`, analysis_runs advanced by ≥ 3 |

### Key behavioral invariants verified

- **Contradiction evidence is always preserved** — the `contradicts` link from
  obs b5 to the `payment_applied` event exists in both the baseline and resolved
  runs. Phase 5 step (c) is unconditional.
- **Resolution row survives reprocess** — `fte_review_resolutions` is not
  touched by Phase 0; the row inserted in STEP 2 is still present after the
  second and third reconciler calls.
- **`fte_analysis_runs` is append-only** — run-count assertions use delta logic:
  a baseline count is captured at the start of the transaction (in a temp table)
  and each check asserts that the count has advanced by at least N, not that the
  absolute count equals N. This makes the test robust against a disposable DB
  that already holds prior reconciler runs from earlier manual validation.

### How to run

```bash
# Prerequisites: migrations + function registered (same as validate_reconciler.sql)

psql "$DATABASE_URL" -f tests/validate_review_resolution.sql
```

Supabase SQL editor note: `\i` is psql-only. Paste
`fixtures/synthetic_ccdbe216_failure_modes.sql` first (execute separately so it
commits), then paste the body of this file starting from the `begin;` block.

### Expected output

Seven `PASS [n/7] …` `NOTICE` lines. The outer `ROLLBACK` discards all
reconciler output and the resolution row; fixture entity data is unaffected.

---

## `validate_observation_resolution.sql`

Targeted validation of the three observation-level resolution actions introduced
in Task 004C: `confirm_observation`, `reject_observation`, and `mark_duplicate`.
Covers only the ccdbe216 fixture practice and exercises both queue-suppression
paths and the ledger-impact path (rejecting a trusted observation).

### What it asserts (10 steps, 12 checks)

| Step | Check | What is verified |
|------|-------|-----------------|
| 1 — baseline | 1/12 | Return JSON: `review_resolutions_applied = 0` |
| 1 — baseline | 2/12 | Queue count = 6 (5 obs entries + 1 position entry) |
| 1 — baseline | 3/12 | b4 (`suspected_summary_row`), b1 (`suspected_duplicate`), b3 (`missing_evidence_link`) all in queue |
| 3 — confirm b4 | 4/12 | Return JSON: `review_resolutions_applied = 1`; queue count = 5 |
| 3 — confirm b4 | 5/12 | b4 absent from queue; b1 and b3 still present |
| 5 — reject b3 | 6/12 | Return JSON: `review_resolutions_applied = 2`; queue count = 4 |
| 5 — reject b3 | 7/12 | b3 and b4 absent from queue; b1 and b2 still present |
| 7 — duplicate b1→a1 | 8/12 | Return JSON: `review_resolutions_applied = 3`; queue count = 3 |
| 7 — duplicate b1→a1 | 9/12 | b1/b3/b4 absent from queue; `mark_duplicate` row has `target_observation_id = a1` and `is_superseded = false` |
| 8 — idempotency | 10/12 | Fifth run: `resolutions_applied = 3`, queue = 3, `analysis_runs` advanced by ≥ 5 |
| 10 — reject a3 | 11/12 | `review_resolutions_applied = 4`; `contractual_adjustment_applied` event count = 0 for CLM-AZ-0001; queue still = 3 |
| 10 — reject a3 | 12/12 | `open_balance_amount = 209.60` (billed $720 − paid $510.40; adj removed); position = `in_review` (b5 ambiguity persists) |

### Key behavioral invariants verified

- **`confirm_observation` is queue-only** — obs b4 is classified by Phase 1 with
  its original `suspected_summary_row` rule. Only its queue entry is suppressed;
  no events or position changes result.
- **`reject_observation` suppresses Phase 1 entirely** — obs b3 (EXCLUDED by
  Rule 1) and obs a3 (TRUSTED) both disappear from `_fte_classified` on the
  next run. No events, no queue entry, no ledger contribution.
- **`mark_duplicate` suppresses Phase 1 identically to `reject_observation`** —
  obs b1 is removed from Phase 1; `target_observation_id` FK records the
  canonical observation (a1) for audit.
- **`unbalanced_financial_position` persists from b5 ambiguity** — the b5
  `late_retry_page_contradiction` keeps `payment_applied = 'ambiguous'` and
  position = `in_review` throughout checks 8–12. Queue count remains 3
  (b2 `conflicting_observations`, b5 `late_retry_page_contradiction`, position
  `unbalanced_financial_position`) regardless of how many other resolutions
  are active.
- **Rejection of a trusted observation has financial consequences** — rejecting
  a3 removes the `contractual_adjustment_applied` event, which causes
  `open_balance_amount` to recalculate from `$0.00` to `$209.60`.

### How to run

```bash
# Prerequisites: migrations 001–003 applied; reconciler/fte_reconcile.sql registered

psql "$DATABASE_URL" -f tests/validate_observation_resolution.sql
```

Supabase SQL editor note: `\i` is psql-only. Paste
`fixtures/synthetic_ccdbe216_failure_modes.sql` first (execute separately so it
commits), then paste the body of this file starting from the `begin;` block.

### Expected output

Twelve `PASS [n/12] …` `NOTICE` lines. The outer `ROLLBACK` discards all
reconciler output and all four resolution rows; fixture entity data is unaffected.

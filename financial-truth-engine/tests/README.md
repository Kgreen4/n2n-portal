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
| 3 — resolved | 6/7 | `contradicts` link preserved; resolution row intact; `run_type='reconciler'` count = 2 |
| 4 — idempotency | 7/7 | Third call: `review_resolutions_applied = 1`, `payment_applied = 'reconciled'`, position = `balanced`, `run_type='reconciler'` count = 3 |

### Key behavioral invariants verified

- **Contradiction evidence is always preserved** — the `contradicts` link from
  obs b5 to the `payment_applied` event exists in both the baseline and resolved
  runs. Phase 5 step (c) is unconditional.
- **Resolution row survives reprocess** — `fte_review_resolutions` is not
  touched by Phase 0; the row inserted in STEP 2 is still present after the
  second and third reconciler calls.
- **`fte_analysis_runs` is append-only** — the run count check uses
  `WHERE run_type = 'reconciler'` to exclude the fixture's pre-seeded
  `run_type = 'seed_fixture'` row.

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

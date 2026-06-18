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

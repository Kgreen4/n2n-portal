# Financial Truth Engine — Validation Runbook

**Purpose:** Authoritative run order for the FTE validation suite.
Reduces the most common mistakes: stale registered function, psql metacommand
errors in the Supabase SQL Editor, and missing fixture loads.

---

## Validation Suites

| File | Numeric checks | Covers |
|---|---|---|
| `tests/validate_schema.sql` | structure only (not counted) | 11 tables, RLS, FK isolation, constraints, indexes |
| `tests/validate_reconciler.sql` | 12 | 9-phase reconciler, event classification, short-pay detection |
| `tests/validate_review_resolution.sql` | 7 | `confirm_payment_event` — ambiguous → reconciled/balanced |
| `tests/validate_observation_resolution.sql` | 12 | confirm/reject/mark_duplicate, Phase 1 suppression, ledger recalc |
| `tests/validate_corrected_value.sql` | 11 | `attach_corrected_value` — correction applied, balanced, idempotency, index |
| `tests/validate_corrected_value_supersession.sql` | 10 | supersession — replace active correction, audit trail, index enforcement |
| `tests/validate_corrected_contractual_adjustment.sql` | 10 | `attach_corrected_value` on contractual_adjustment obs — Phase 4 corrected amount, payment unchanged, index enforcement |

**Total numeric checks: 62**

All suites are wrapped in `ROLLBACK` — nothing persists to the database.
`fte_analysis_runs` is append-only and is **not** rolled back; suites use
run-count delta logic so the absolute count does not matter.

---

## Fixtures

| File | Practice ID | Covers |
|---|---|---|
| `fixtures/synthetic_ccdbe216_failure_modes.sql` | `c0000000-0000-4000-8000-0000000000fe` | phantom duplicate, section-delimiter double-count, null-check crossbleed, summary-row exclusion, late-retry/page contradiction |
| `fixtures/synthetic_96c5c357_failure_modes.sql` | `96000000-0000-4000-8000-0000000000fe` | check-spacing fragmentation variants, short-pay (CLM-APC-1000) |

Fixture files use `INSERT ... ON CONFLICT DO NOTHING` — safe to load more than
once. Fixtures are **not** wrapped in ROLLBACK; they commit and remain available
for all subsequent suite runs in the same session.

Suite → fixture dependency:

| Suite | Fixture required |
|---|---|
| `validate_schema.sql` | none (uses throwaway practice) |
| `validate_reconciler.sql` | both |
| `validate_review_resolution.sql` | ccdbe216 |
| `validate_observation_resolution.sql` | ccdbe216 |
| `validate_corrected_value.sql` | 96c5c357 |
| `validate_corrected_value_supersession.sql` | 96c5c357 |
| `validate_corrected_contractual_adjustment.sql` | 96c5c357 |

---

## First-Time Setup (disposable DB only — run once)

Migrations are one-time DDL. Do not blindly rerun them if the schema is already
applied — see the Troubleshooting section if you hit a duplicate-object error.

### Local psql

```bash
# Apply schema migrations in order
psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql
psql "$DATABASE_URL" -f migrations/003_add_observation_resolution_target.sql
psql "$DATABASE_URL" -f migrations/004_corrected_value_constraints.sql

# Register the reconciler function (CREATE OR REPLACE — safe to rerun)
psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
```

### Supabase SQL Editor

Paste and execute each file separately, in order. Execute each before moving
to the next — do not batch them.

1. Paste + execute `migrations/001_create_financial_truth_schema.sql`
2. Paste + execute `migrations/002_add_review_resolutions.sql`
3. Paste + execute `migrations/003_add_observation_resolution_target.sql`
4. Paste + execute `migrations/004_corrected_value_constraints.sql`
5. Paste + execute `reconciler/fte_reconcile.sql`

"Success. No rows returned" after each step is correct — DDL and
`CREATE OR REPLACE FUNCTION` produce no result rows.

---

## Repeatable Validation Run

Run in this order every time. Migrations are **not** repeated here.

### Local psql — single command

```bash
psql "$DATABASE_URL" -f tests/run_all_validations.sql
```

This runner loads both fixtures, then executes all six suites in the correct
order. See `tests/run_all_validations.sql` for the exact sequence.

Expected output: 62 `PASS` NOTICE lines across seven suites, plus a banner
after each suite. Each suite is independent — a failure in one suite does not
prevent the next from running, but scroll up to find the EXCEPTION output from
any failed suite.

### Supabase SQL Editor — manual sequence

`\i` metacommands are psql-only and will cause a syntax error in the SQL
Editor. Run each file as a separate paste-and-execute. For validation files,
either comment out any `\i` lines at the top or paste only the content
starting from the `begin;` block.

**Step 1 — Load fixtures** (execute each separately; they commit):

1. Paste + execute `fixtures/synthetic_ccdbe216_failure_modes.sql`
2. Paste + execute `fixtures/synthetic_96c5c357_failure_modes.sql`

**Step 2 — Run validation suites** (execute each separately):

3. Paste + execute `tests/validate_schema.sql`
4. Paste + execute `tests/validate_reconciler.sql`
5. Paste + execute `tests/validate_review_resolution.sql`
6. Paste + execute `tests/validate_observation_resolution.sql`
7. Paste + execute `tests/validate_corrected_value.sql`
8. Paste + execute `tests/validate_corrected_value_supersession.sql`
9. Paste + execute `tests/validate_corrected_contractual_adjustment.sql`

**What to expect in the SQL Editor:**

- "Success. No rows returned" = the `DO $$...$$` block ran without error.
  PASS output appears in the **Messages** panel, not the Results panel.
- If the Messages panel is collapsed, expand it to see NOTICE lines.
- Any `FAIL` raises an EXCEPTION, which aborts the current suite and rolls
  back its transaction. Stop and troubleshoot before continuing.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| CHECK N fails with "expected X, got Y" on a payment/balance value | Stale registered reconciler — Supabase has a pre-004D (or older) version of `fte_reconcile_practice` lacking the corrected-value or Phase 0.5 logic | Re-paste and execute `reconciler/fte_reconcile.sql` (it is `CREATE OR REPLACE`; safe to rerun), then rerun the failing suite |
| `syntax error at or near "\"` in Supabase SQL Editor | `\i` psql metacommand in the file | Comment out or delete the `\i` line; paste only the content from `begin;` onward, or use local psql instead |
| `duplicate key value violates unique constraint` or `already exists` when applying a migration | Migration already applied to this DB | Skip that migration; check the others and apply only the ones not yet applied |
| NOTICE / PASS lines not visible | Supabase SQL Editor Messages panel is collapsed | Expand the Messages panel below the Results panel |
| Queue count off by 1 or unexpected `review_resolutions_applied` count | Stale `fte_review_resolutions` row left by a prior run that crashed before ROLLBACK | Each validation suite deletes resolutions for its practice inside its transaction, so a clean run will reset them. For a manual cleanup in a disposable DB: `DELETE FROM fte_review_resolutions WHERE practice_id = '<practice_id>';` |
| Run-count delta assertion fails unexpectedly | `fte_analysis_runs` is append-only and already has many rows | Suites use delta logic (count at end minus count at start), so existing rows should not cause failures. If deltas still fail, the reconciler is likely stale — reregister it |
| Fixture `INSERT` errors on a column that does not exist | A migration was applied partially or out of order | Drop and recreate the disposable DB, then apply all four migrations in order before loading fixtures |

---

## Safety

- Use a **disposable** Supabase project for all FTE development and validation.
- All fixtures are **synthetic** — no PHI, no real patient data, no real member
  IDs, no production exports.
- Do not load raw PDFs or production data into the validation DB.
- Do not paste credentials, service-role keys, or anon keys into any file in
  this repo.
- Do not connect to the legacy EOB Supabase project
  for any FTE validation step.
- `$DATABASE_URL` in the commands above is a shell variable you supply at
  runtime — it is never stored in this file.

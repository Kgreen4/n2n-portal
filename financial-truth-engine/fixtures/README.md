# Financial Truth Engine — Synthetic Fixtures

**Status:** Synthetic hard-case fixtures
**Important:** Everything here is **synthetic**. No PHI, no real PDFs, no real patient names,
no real member IDs, no production exports. All amounts, claim numbers, and identifiers below are
fabricated for testing the architecture. The internal fixture IDs (`ccdbe216`, `96c5c357`) are
opaque labels only — the real source documents live in private, PHI-safe storage and are never
committed to this public repo.

---

## Purpose

These fixtures simulate the **known hard failure modes** from the legacy EOB project (see
`../FIXTURE_PLAN.md`) using synthetic rows, so we can prove the FTE ledger architecture handles
them **without corrupting financial truth**:

- conflicting / duplicate / late-retry observations *coexist* (nothing is silently overwritten)
- summary rows are flagged as non-transactions, never reconciled as truth
- multiple payment sections do not crossbleed into one event without evidence
- observations never directly mutate `fte_financial_positions`
- every ambiguity is visible in `fte_review_queue`
- every claim event traces back to evidence/observations via `fte_event_evidence`

---

## Files

| File | Simulates | Failure modes covered |
|---|---|---|
| `synthetic_ccdbe216_failure_modes.sql` | 112-page BCBS-AZ multi-payment EOB (primary hard case) | phantom duplicate check ref, section-delimiter double count, null-check crossbleed, spurious summary row, page-63 late/retry contradiction, conflicting observations, unbalanced position |
| `synthetic_96c5c357_failure_modes.sql` | Arizona Priority Care EOB (secondary regression case) | large single-check gap, second unresolved gap, retry-pending ambiguity |

---

## How To Load

Apply the schema migration first, then the fixtures, against a clean Postgres / Supabase DB
(run as a role with `BYPASSRLS`, e.g. Supabase `service_role` / `postgres`):

```bash
psql "$DATABASE_URL" -f ../migrations/001_create_financial_truth_schema.sql
psql "$DATABASE_URL" -f synthetic_ccdbe216_failure_modes.sql
psql "$DATABASE_URL" -f synthetic_96c5c357_failure_modes.sql
```

Both fixture files are **idempotent**: they delete their own synthetic practice (by fixed UUID,
cascading to all child rows) before re-inserting, so they can be re-run safely.

---

## Synthetic Identity Conventions

To keep the rows readable and collision-free, fixtures use fixed, obviously-synthetic UUIDs:

- Practice: `c0000000-0000-4000-8000-0000000000fe` (ccdbe216) /
  `96000000-0000-4000-8000-0000000000fe` (96c5c357)
- Child rows use a `…dddd…` (document/evidence), `…0b5…` (observation), `…c1a1…` (claim),
  `…e7e7…` (event), `…f205…` (position), `…6e6e…` (review) flavoring in the first segment.

These are arbitrary readability aids, not meaningful identifiers.

---

## What "Correct" Looks Like After Loading

After loading a fixture, the ledger should show:

1. The **summary-total** observation exists but is `is_summary_row = true`, and there is **no**
   `payment_applied` claim event derived from it.
2. The **phantom/duplicate** and **late-retry** observations exist but at least one is flagged
   `is_superseded` and surfaced in `fte_review_queue` — none are deleted.
3. `fte_financial_positions.reconciliation_status` is `unbalanced` (or `in_review`) for the
   claims with open gaps, and a matching `fte_review_queue` row explains why.
4. Every `fte_claim_events` row has at least one `fte_event_evidence` link.

`../tests/validate_schema.sql` asserts the structural guarantees that make the above possible.

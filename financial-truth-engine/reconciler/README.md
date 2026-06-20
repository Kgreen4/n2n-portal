# Financial Truth Engine — Reconciler

`reconciler/fte_reconcile.sql` defines the PL/pgSQL stored procedure
`fte_reconcile_practice(p_practice_id uuid) RETURNS jsonb`.

---

## 1. What the reconciler does and does not do

**Does:**
- Reads `fte_observations` for the given practice and classifies each one as
  TRUSTED, SUSPECT, or EXCLUDED using five deterministic rules.
- Emits `fte_claim_events` (claim_adjudicated, contractual_adjustment_applied,
  payment_applied, short_pay_detected) exclusively from TRUSTED observations.
- Links every event back to its evidence and observation via `fte_event_evidence`
  so every dollar is explainable.
- Derives a `fte_financial_positions` row per claim (billed, adjusted, paid,
  open balance, confidence score, reconciliation status).
- Routes non-trusted observations and unbalanced/in_review positions to
  `fte_review_queue` for human resolution.
- Records a completed `fte_analysis_runs` entry and returns a summary JSON.
- Is **idempotent**: calling it twice for the same practice produces the same
  final state. Phase 0 deletes all derived rows before re-deriving them.

**Does not:**
- Invent amounts not directly supported by a trusted observation.
- Auto-merge fragmented or ambiguous check observations — those go to review.
- Modify `fte_observations`, `fte_evidence`, `fte_claims`, or `fte_practices`.
- Access the network, call external services, or produce side effects outside
  the database transaction.
- Run against production Supabase using real patient data (PHI). Fixtures are
  entirely synthetic — see `fixtures/README.md`.

---

## 2. Observation classification rules (first-match wins)

| Rule | Condition | Classification | review_reason |
|------|-----------|----------------|---------------|
| 1 | `is_superseded = true` | EXCLUDED | from failure_mode mapping below |
| 2 | `is_summary_row = true` (not superseded) | EXCLUDED | `suspected_summary_row` |
| 3 | `observation_type = 'payment'` AND `check_eft_identifier IS NULL` (not Rule 1/2) | EXCLUDED | `missing_evidence_link` |
| 4 | `metadata->>'failure_mode'` IS NOT NULL AND `<> ''` (not Rule 1/2/3) | SUSPECT | from failure_mode mapping below |
| 5 | everything else | TRUSTED | — |

**failure_mode → review_reason mapping** (used by Rules 1 and 4):

| failure_mode | review_reason |
|---|---|
| `phantom_duplicate_check_ref` | `suspected_duplicate` |
| `section_delimiter_double_count` | `conflicting_observations` |
| `null_check_crossbleed` | `missing_evidence_link` |
| `late_retry_page_contradiction` | `late_retry_page_contradiction` |
| `check_spacing_variant_fragmentation` with `retry_pending=true` | `late_retry_page_contradiction` |
| `check_spacing_variant_fragmentation` without `retry_pending` | `suspected_duplicate` |
| anything else / null | `conflicting_observations` |

Note: Rule 2 (`is_summary_row`) always overrides the failure_mode mapping and
produces `suspected_summary_row`, even if failure_mode is also set.

---

## 3. The nine reconciler phases

| Phase | Description |
|-------|-------------|
| **0** | Idempotent reset: DELETE all derived rows for the practice in FK-safe order (`fte_event_evidence` → `fte_review_queue` → `fte_financial_positions` → `fte_claim_events`). `fte_analysis_runs` is append-only and is NOT deleted. |
| **1** | Classify all observations into the temp table `_fte_classified` using the five rules above. `DROP TABLE IF EXISTS` ensures idempotency within the same outer transaction. |
| **2** | Route EXCLUDED and SUSPECT observations to `fte_review_queue`. Summary rows with no `claim_identifier` produce review entries with `claim_id = NULL`. |
| **3** | Emit `claim_adjudicated` events from TRUSTED `billed_amount` observations. Each event gets one `derived_from` evidence link. |
| **4** | Emit `contractual_adjustment_applied` events from TRUSTED `contractual_adjustment` observations. `carc_code` is propagated. Each event gets one `derived_from` link. |
| **5c** | Emit `payment_applied` events from TRUSTED `payment` observations. Each event gets two `supports` evidence links: (1) the page observation, and (2) the matching `check_payment` stub (if one exists with `metadata->>'check_number' = check_eft_identifier`). |
| **5** (late/retry) | For each `late_retry_page_contradiction` review entry that has an `observation_id`, find the `payment_applied` event for the same claim, mark it `ambiguous`, wire the review entry's `claim_event_id`, and insert a `contradicts` evidence link. If no payment event exists the loop is a no-op for that entry. |
| **6** | Derive `fte_financial_positions` for every claim that has at least one event OR at least one review queue entry. `reconciliation_status` CASE (evaluated in priority order): (1) no events → `in_review`; (2) any linked event has `reconciliation_status = 'ambiguous'` → `in_review` — **this applies even when the math balances to zero** (gap = 0). Financial truth cannot be finalized while contradicting evidence is unresolved; the claim must go to human review. Note: `'ambiguous'` is valid on `fte_claim_events` but is **not** a valid `fte_financial_positions.reconciliation_status` value (schema CHECK forbids it) — `in_review` is the correct surrogate; (3) any unbalanced event or positive open balance → `unbalanced`; (4) else → `balanced`. `open_balance_amount` is NULL when billed is unknown; otherwise `GREATEST(0, billed − adj − paid)`. `position_confidence_score` = MIN(confidence_score) across non–short_pay events, falling back to `0.0000`. |
| **7** | Route every `unbalanced` or `in_review` position to `fte_review_queue` with reason `unbalanced_financial_position`. |
| **8** | For each unbalanced position with `open_balance_amount > 0`, emit a `short_pay_detected` event and inherit the `derived_from` evidence link from the corresponding `claim_adjudicated` event. |
| **9** | Insert a `fte_analysis_runs` row with status `succeeded` and return a summary JSON with keys `run_id`, `practice_id`, `claims_processed`, `events_emitted`, `positions_derived`, `review_entries`. |

---

## 4. How to run against fixtures

Prerequisites:
1. Apply the schema: `psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql`
2. Load the fixtures: `psql "$DATABASE_URL" -f fixtures/synthetic_ccdbe216_failure_modes.sql`
3. Register the function: `psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql`

Run the reconciler for a single fixture practice:

```sql
SELECT fte_reconcile_practice('c0000000-0000-4000-8000-0000000000fe');
```

Run the full validation suite (12 checks, wrapped in ROLLBACK — nothing persists):

```sql
psql "$DATABASE_URL" -f tests/validate_reconciler.sql
```

All checks must emit `PASS` notices and exit without an unhandled `EXCEPTION`.

---

## 5. How to extend

**New observation_type:**

1. If the new type should produce a new event_type, add a new FOR loop in the
   appropriate phase (after Phase 4 for financial events). Emit the event,
   insert `fte_event_evidence` links, and choose `link_role` (`derived_from`,
   `supports`, or `contradicts`).
2. Update Phase 6's aggregation CASE expression if the new event_type
   contributes to position amounts.
3. If the new type is informational-only (no event), ensure it falls through
   to TRUSTED and is silently ignored by the event-emission phases (no extra
   branch needed).

**New review reason:**

1. Add the value to the `fte_review_queue.reason` CHECK constraint in a new
   migration.
2. Add a WHEN branch to the `fm_reason` CASE in Phase 1 (if it maps from a
   failure_mode), or add a new classification rule to the outer CASE (if it's
   a structural condition like Rule 2/3).
3. Add a fixture observation that exercises the new path.
4. Add a validation check to `tests/validate_reconciler.sql`.

---

## 6. Why a SQL stored procedure?

- **Single transaction:** the entire 9-phase derivation runs atomically. Either
  all derived rows are committed together or none are — there is no partial-
  reconciliation state visible to readers.
- **No network round-trip:** the reconciler reads and writes entirely within
  the database. There is no application server, no serialization overhead, and
  no partial failure due to network interruption between phases.
- **Idempotent by construction:** Phase 0 deletes all derived rows before
  re-deriving them, so calling the function twice is equivalent to calling it
  once. This makes retries and reprocessing safe with no external coordination.
- **Portable audit:** the procedure definition lives in the repo alongside the
  schema migration and fixtures. Any developer can reproduce the full derivation
  locally with `psql` and the synthetic fixtures — no application server, API
  key, or running service required.

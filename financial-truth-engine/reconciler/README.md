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
| **0.5** | Load active review resolutions: snapshot non-superseded `fte_review_resolutions` rows for this practice into temp table `_fte_active_resolutions ON COMMIT DROP`. Zero rows is valid — downstream phases behave identically to the no-resolution baseline. `DROP TABLE IF EXISTS` guard (same pattern as Phase 1's `_fte_classified`) ensures idempotency across multiple calls in the same outer transaction. `GET DIAGNOSTICS` captures the row count for `review_resolutions_applied` in the return JSON. Additionally builds `_fte_suppressed_observations ON COMMIT DROP` — the set of `observation_id` values where `action IN ('reject_observation', 'mark_duplicate')`. Phase 1 uses a `NOT EXISTS` subquery against this table to exclude suppressed observations from classification entirely. |
| **1** | Classify all observations into the temp table `_fte_classified` using the five rules above. `DROP TABLE IF EXISTS` ensures idempotency within the same outer transaction. |
| **2** | Route EXCLUDED and SUSPECT observations to `fte_review_queue`. Summary rows with no `claim_identifier` produce review entries with `claim_id = NULL`. A `confirm_observation` active resolution for an observation suppresses that observation's queue entry only (checked via `NOT EXISTS` on `_fte_active_resolutions`) — the observation still classifies in Phase 1 with its original rule, but no queue row is emitted. This is queue-only suppression: it does not promote an EXCLUDED observation to TRUSTED, and it does not change ledger events or positions. |
| **3** | Emit `claim_adjudicated` events from TRUSTED `billed_amount` observations. Each event gets one `derived_from` evidence link. A reviewer-supplied corrected billed amount is applied via `COALESCE` — see §4. |
| **4** | Emit `contractual_adjustment_applied` events from TRUSTED `contractual_adjustment` observations. `carc_code` is propagated. Each event gets one `derived_from` link. A reviewer-supplied corrected adjustment amount is applied via `COALESCE` — see §4. |
| **5c** | Emit `payment_applied` events from TRUSTED `payment` observations. Each event gets two `supports` evidence links: (1) the page observation, and (2) the matching `check_payment` stub (if one exists with `metadata->>'check_number' = check_eft_identifier`). A reviewer-supplied corrected payment amount is applied via `COALESCE` — see §4. |
| **5** (late/retry) | For each `late_retry_page_contradiction` review entry that has an `observation_id`, find the `payment_applied` event for the same claim, then: **(a)** if an active `confirm_payment_event` resolution exists for the claim in `_fte_active_resolutions`, mark the event `reconciled`; otherwise mark it `ambiguous`; **(b)** wire the review entry's `claim_event_id` (unconditional); **(c)** insert a `contradicts` evidence link (unconditional — the contradiction record is always preserved even when the reviewer has confirmed the payment). If no payment event exists the loop is a no-op for that entry. |
| **6** | Derive `fte_financial_positions` for every claim that has at least one event OR at least one review queue entry. `reconciliation_status` CASE (evaluated in priority order): (1) no events → `in_review`; (2) any linked event has `reconciliation_status = 'ambiguous'` → `in_review` — **this applies even when the math balances to zero** (gap = 0). Financial truth cannot be finalized while contradicting evidence is unresolved; the claim must go to human review. Note: `'ambiguous'` is valid on `fte_claim_events` but is **not** a valid `fte_financial_positions.reconciliation_status` value (schema CHECK forbids it) — `in_review` is the correct surrogate; (3) any unbalanced event or positive open balance → `unbalanced`; (4) else → `balanced`. `open_balance_amount` is NULL when billed is unknown; otherwise `GREATEST(0, billed − adj − paid)`. `position_confidence_score` = MIN(confidence_score) across non–short_pay events, falling back to `0.0000`. |
| **7** | Route every `unbalanced` or `in_review` position to `fte_review_queue` with reason `unbalanced_financial_position`. **dismiss_short_pay / confirm_short_pay suppression:** `unbalanced` positions are skipped when an active `dismiss_short_pay` or `confirm_short_pay` resolution exists for the claim in `_fte_active_resolutions`. The `fte_financial_positions` row is NOT changed — `reconciliation_status = 'unbalanced'` and `open_balance_amount` remain correct. `in_review` positions are always routed regardless of any resolution. |
| **8** | For each unbalanced position with `open_balance_amount > 0`, emit a `short_pay_detected` event and inherit the `derived_from` evidence link from the corresponding `claim_adjudicated` event. **dismiss_short_pay suppression only:** the `short_pay_detected` event is not emitted when an active `dismiss_short_pay` resolution exists for the claim. `confirm_short_pay` does NOT suppress this event — the short-pay signal remains active so downstream workflows can act on the confirmed recovery need. The position row retains `reconciliation_status = 'unbalanced'` and the correct `open_balance_amount` — math is preserved as financial truth. |
| **9** | Insert a `fte_analysis_runs` row with status `succeeded` and return a summary JSON with keys `run_id`, `practice_id`, `claims_processed`, `events_emitted`, `positions_derived`, `review_entries`, `review_resolutions_applied`. |

---

## 4. Corrected-value correction model

`attach_corrected_value` is the reviewer action that replaces an extracted
observation amount with a verified figure. All three financial observation
types — `billed_amount`, `contractual_adjustment`, and `payment` — share a
single correction path backed by the same DB constraints, the same partial
index, and the same COALESCE pattern.

### 4.1 Overview

When AI extraction produces an incorrect amount, the reviewer inserts a row
into `fte_review_resolutions` with `action = 'attach_corrected_value'`,
`observation_id` pointing to the observation being corrected, and
`corrected_value` set to the verified amount. The reconciler picks this up in
Phase 0.5 and applies it transparently in the relevant event-emission phase.
The original observation is never mutated — `fte_observations` rows are
immutable.

### 4.2 Phase 0.5 loading

Phase 0.5 snapshots all non-superseded `fte_review_resolutions` rows for the
practice into the temp table `_fte_active_resolutions ON COMMIT DROP`. This
table is created before any event-emission phase runs. Phases 3, 4, and 5c
each query it independently with a correlated subquery.

### 4.3 Correlated-subquery + COALESCE pattern

Each event-emission phase (3, 4, 5c) adds a correlated subquery to its
`FOR v_obs` cursor that selects the active correction for the observation
being processed:

```sql
(SELECT ar.corrected_value
 FROM _fte_active_resolutions ar
 WHERE ar.observation_id = cl.id
   AND ar.action         = 'attach_corrected_value'
 LIMIT 1) AS corrected_<type>_amount
```

The emitted event amount is then:

```sql
COALESCE(v_obs.corrected_<type>_amount, v_obs.amount)
```

If no active correction exists, `COALESCE` falls back to the extracted amount
and reconciler behavior is identical to the no-correction baseline. The
`LIMIT 1` is deterministic because at most one active correction per
observation is enforced by the unique partial index (see §4.6).

### 4.4 Phase 6 math passthrough

Phase 6 derives `fte_financial_positions` from events already in
`fte_claim_events`. Because the corrected amount is written into the event row
by Phases 3/4/5c, Phase 6 reads it automatically without any
correction-aware logic. `open_balance_amount = GREATEST(0, billed − adj − paid)`
uses whichever amounts — extracted or corrected — ended up in the event rows.

### 4.5 Supersession workflow

To replace an active correction:

1. `UPDATE fte_review_resolutions SET is_superseded = true WHERE observation_id = '<obs_id>' AND action = 'attach_corrected_value' AND is_superseded = false;`
2. `INSERT INTO fte_review_resolutions (..., corrected_value = <new_value>, is_superseded = false);`
3. Rerun `fte_reconcile_practice(practice_id)`.

The unique partial index permits any number of superseded (historical)
corrections for the same observation. Only a second `is_superseded = false`
row is rejected.

### 4.6 Migration 004 DB constraints

`migrations/004_corrected_value_constraints.sql` enforces correct shape at the
database level:

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_cv_action_needs_obs_id` | `observation_id IS NOT NULL` when `action = 'attach_corrected_value'` |
| `fte_review_resolutions_cv_action_needs_observation_type` | `target_type = 'observation'` when `action = 'attach_corrected_value'` |
| `fte_review_resolutions_cv_action_needs_corrected_value` | `corrected_value IS NOT NULL` when `action = 'attach_corrected_value'` |
| `fte_review_resolutions_cv_action_value_nonnegative` | `corrected_value >= 0` when `action = 'attach_corrected_value'` |
| `idx_fte_resolutions_single_active_correction` | UNIQUE `(practice_id, observation_id, action) WHERE is_superseded = false AND action = 'attach_corrected_value'` |

### 4.7 Validation suites

| Suite | Checks | What it proves |
|---|---|---|
| `tests/validate_corrected_value.sql` | 11 | `attach_corrected_value` on a `payment` observation — correction applied, balanced, idempotency, index enforcement |
| `tests/validate_corrected_value_supersession.sql` | 10 | Supersession — replace active correction, audit trail, index enforcement |
| `tests/validate_corrected_contractual_adjustment.sql` | 10 | `attach_corrected_value` on a `contractual_adjustment` observation — Phase 4 corrected amount, payment unchanged, index enforcement |
| `tests/validate_corrected_billed_amount.sql` | 10 | `attach_corrected_value` on a `billed_amount` observation — Phase 3 corrected amount, payment unchanged, index enforcement |

### 4.8 Supabase stale-function caveat

If a validation check fails with an unexpected amount (e.g., `claim_adjudicated`
returns the extracted amount instead of the corrected value), the most likely
cause is a stale registered version of `fte_reconcile_practice` in Supabase.
Re-paste and execute `reconciler/fte_reconcile.sql` (`CREATE OR REPLACE` —
safe to rerun), then rerun the failing suite.

---

## 5. Position-level resolution model

Two position-level reviewer actions are implemented: `dismiss_short_pay` and
`confirm_short_pay`. Both suppress generic unbalanced-position triage routing
without altering the mathematical position derived by Phase 6. They differ in
what happens to the `short_pay_detected` event (Phase 8).

### 5.1 Overview

When a reviewer decides that an open balance is known, accepted, or not worth
pursuing (e.g., a known write-off, a contractual allowance, or a credentialing
exclusion), they insert a row into `fte_review_resolutions` with
`action = 'dismiss_short_pay'`, `target_type = 'position'`,
`claim_id` pointing to the stable claim anchor, and `is_superseded = false`.

On the next reconciler run, Phase 7 skips the `fte_review_queue` insert for
that claim's unbalanced position, and Phase 8 skips the `short_pay_detected`
event. The `fte_financial_positions` row is left exactly as Phase 6 derived it:
`reconciliation_status = 'unbalanced'`, `open_balance_amount` mathematically
correct. The suppression is operational, not mathematical — financial truth is
preserved.

### 5.2 Why `claim_id` (not `source_position_id`) is the stable anchor

`fte_financial_positions` rows are deleted and re-derived on every Phase 0 reset.
`source_position_id` is a plain uuid snapshot field with no `REFERENCES` clause —
it becomes stale after each reprocess. `claim_id` is a hard FK to `fte_claims`,
which Phase 0 never deletes. Phases 7 and 8 look up active `dismiss_short_pay`
resolutions by `claim_id`, guaranteeing the lookup survives reruns.

### 5.3 Supersession workflow

To re-enable short-pay routing for a previously dismissed claim:

1. `UPDATE fte_review_resolutions SET is_superseded = true WHERE claim_id = '<claim_id>' AND action = 'dismiss_short_pay' AND is_superseded = false;`
2. Rerun `fte_reconcile_practice(practice_id)`.

On the next run Phase 0.5 finds no active `dismiss_short_pay` row for the claim,
so Phases 7 and 8 behave as if no resolution existed — the queue entry and
`short_pay_detected` event reappear.

### 5.4 Migration 005 DB constraints

`migrations/005_dismiss_short_pay_constraints.sql` enforces the required shape:

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_dismiss_shortpay_needs_claim_id` | `claim_id IS NOT NULL` when `action = 'dismiss_short_pay'` |
| `fte_review_resolutions_dismiss_shortpay_needs_position_type` | `target_type = 'position'` when `action = 'dismiss_short_pay'` |

### 5.5 Validation suite

`tests/validate_dismiss_short_pay.sql` — 9 checks (wrapped in ROLLBACK):

| Step | Checks | What it proves |
|---|---|---|
| 1: baseline | 1–3 | `review_resolutions_applied = 0`; `short_pay_detected` emitted; CLM-APC-1000 queued |
| 3: dismissed | 4–7 | `review_resolutions_applied = 1`; event suppressed; queue entry absent; position math unchanged |
| 4: isolation | 8 | CLM-APC-2000 still queued (unaffected) |
| 5: supersession | 9 | After `is_superseded = true`, event re-emitted and queue row reappears |

---

### 5.6 `confirm_short_pay` overview

When a reviewer has triaged an open balance and confirmed it is a genuine,
actionable short pay they intend to pursue, they insert a row into
`fte_review_resolutions` with `action = 'confirm_short_pay'`,
`target_type = 'position'`, `claim_id` pointing to the stable claim anchor,
and `is_superseded = false`.

On the next reconciler run:

- **Phase 7** skips the `fte_review_queue` insert (reason
  `unbalanced_financial_position`) — the claim no longer needs generic triage
  routing because the reviewer has already decided to pursue recovery.
- **Phase 8** still emits the `short_pay_detected` event — this is the key
  difference from `dismiss_short_pay`. The short-pay signal remains active
  so that downstream recovery workflows can act on it.
- **Phase 6** is unchanged — `reconciliation_status = 'unbalanced'` and
  `open_balance_amount` remain mathematically correct.

### 5.7 Why `claim_id` is the stable anchor (confirm_short_pay)

Same rationale as `dismiss_short_pay` (§5.2). `fte_financial_positions` rows
are deleted and re-derived on every Phase 0 reset; `source_position_id` goes
stale. Phase 7 looks up active `confirm_short_pay` resolutions by `claim_id`,
which is a hard FK to `fte_claims` and survives all reruns.

### 5.8 Supersession workflow (confirm_short_pay)

To re-enable generic queue routing for a previously confirmed claim:

1. `UPDATE fte_review_resolutions SET is_superseded = true WHERE claim_id = '<claim_id>' AND action = 'confirm_short_pay' AND is_superseded = false;`
2. Rerun `fte_reconcile_practice(practice_id)`.

On the next run Phase 0.5 finds no active `confirm_short_pay` row for the
claim, so Phase 7 routes the position normally. The `short_pay_detected` event
continues to emit (it was never suppressed by `confirm_short_pay`).

### 5.9 Migration 006 DB constraints

`migrations/006_confirm_short_pay_constraints.sql` enforces the required shape:

| Constraint / Index | Rule |
|---|---|
| `fte_review_resolutions_confirm_shortpay_needs_claim_id` | `claim_id IS NOT NULL` when `action = 'confirm_short_pay'` |
| `fte_review_resolutions_confirm_shortpay_needs_position_type` | `target_type = 'position'` when `action = 'confirm_short_pay'` |
| `idx_fte_resolutions_single_active_position_short_pay` | Partial unique index on `(practice_id, claim_id)` where `is_superseded = false AND action IN ('confirm_short_pay', 'dismiss_short_pay')` — prevents simultaneous active rows of both actions for the same claim |

The conflict-prevention index covers both action values so that neither can
be inserted as active while the other is already active for the same
`(practice_id, claim_id)`. To switch from one to the other: set the current
row to `is_superseded = true`, then insert the new row.

### 5.10 Validation suite (confirm_short_pay)

`tests/validate_confirm_short_pay.sql` — 10 checks (wrapped in ROLLBACK):

| Step | Checks | What it proves |
|---|---|---|
| 1: baseline | 1–3 | `review_resolutions_applied = 0`; `short_pay_detected` emitted; CLM-APC-1000 queued |
| 3: confirmed | 4–7 | `review_resolutions_applied = 1`; queue entry absent; `short_pay_detected` preserved (count=1); position math unchanged |
| 4: conflict | 8 | Inserting active `dismiss_short_pay` alongside active `confirm_short_pay` raises `unique_violation` |
| 5: isolation | 9 | CLM-APC-2000 still queued (unaffected) |
| 6: supersession | 10 | After `is_superseded = true`, queue row reappears; `short_pay_detected` remains present |

### 5.11 Deferred: `confirm_position_balanced`

`confirm_position_balanced` is listed in the migration 002 action vocabulary
(position-level group) but is **not implemented** in the reconciler and has no
migration constraints.

**Why deferred:**

`reconciliation_status = 'balanced'` currently has exactly one meaning: the
reconciler derived a zero open balance from claim events (Phase 6 rule 4 —
no events are ambiguous, no event is unbalanced, `open_balance_amount = 0`).
Every `balanced` position is event-derived and mathematically verifiable.

Zero-event claims (e.g., CLM-APC-2000 with all SUSPECT / retry-pending
observations) have `NULL` monetary fields — unknown math, not zero math.
Ambiguous-event claims that happen to balance (e.g., CLM-AZ-0001 where
720.00 − 209.60 − 510.40 = 0.00) have unresolved contradicting evidence;
the correct resolution is `confirm_payment_event`, which promotes the
event to `reconciled` and lets the reconciler derive `balanced` from events.

Implementing `confirm_position_balanced` as a reviewer assertion that bypasses
event derivation would make `balanced` mean two different things:

1. Reconciler-derived math proves zero open balance (current meaning).
2. Reviewer asserted balanced without event math.

Conflating them weakens the "balanced means financial truth" invariant and
makes `fte_financial_positions.reconciliation_status` no longer self-verifying
from events alone.

**Correct paths for claims stuck `in_review`:**

- **Ambiguous-event claim where math balances:** use `confirm_payment_event` —
  promotes the event to `reconciled`, causing Phase 6 to derive `balanced`
  from events on the next run. No position-level assertion needed.
- **Zero-event claim (all observations SUSPECT/EXCLUDED):** correct or
  supersede the underlying observations (`attach_corrected_value`,
  `confirm_observation`, `reject_observation`, `mark_duplicate`) so the
  reconciler can emit events and derive a position from evidence.

**Future implementation — if reviewer-asserted balanced state is needed:**

Options that preserve the invariant:
- A new `reconciliation_status` value (`balanced_by_review`) distinct from
  `balanced`, keeping `balanced` = event-derived.
- A separate workflow-state field outside `reconciliation_status` that records
  the reviewer assertion without overwriting the reconciler's math.
- A new event type or evidence model that lets the reviewer supply the missing
  evidence so the reconciler can derive `balanced` from events as usual.

Any of these requires a new migration and reconciler phase changes. Do not
implement by reusing `confirm_position_balanced` against the existing `balanced`
status value without resolving the semantic collision described above.

---

### 5.12 `request_more_evidence` — durable workflow note, no reconciler change

**What it records:** A reviewer decision that a claim cannot be resolved with
currently available evidence, together with a required written explanation of
what evidence is needed and why the claim is stuck (e.g. "clean 835 remittance
needed — check #2-1835642 fragmented across three spacing variants on pages 8
and 12 of the source document; hold pending payer callback").

**Reconciler behavior: UNCHANGED.** No phase reads or filters on
`request_more_evidence`. Phase 0.5 loads the row into `_fte_active_resolutions`
(all non-superseded rows are loaded unconditionally) but no downstream phase
acts on it. Specifically:

- Phase 6 (position derivation): unchanged — claim retains its reconciler-derived
  `reconciliation_status` (`in_review` or `unbalanced`) as if no evidence request existed.
- Phase 7 (queue routing): unchanged — `request_more_evidence` does **not** suppress
  Phase 7 routing; the claim's review queue entry remains active. (Contrast:
  `dismiss_short_pay` and `confirm_short_pay` both suppress Phase 7 routing.)
- Phase 8 (`short_pay_detected` event emission): unchanged — not suppressed.
- `review_resolutions_applied` counter in the reconciler result JSON: increments by 1
  for the loaded row (Phase 0.5 counts all non-superseded rows), but this is a
  reporting count only — it does not indicate any financial recalculation.

**Why no reconciler change:** The evidence request is a *workflow* signal, not
a financial correction. Suppressing the queue entry would hide the claim from
the reviewer worklist; emitting a new event would pollute the event ledger with
non-financial facts. The claim must remain visible and unresolved until real
evidence arrives and a substantive resolution action is taken.

**DB-level enforcement (migration 007):**

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_rme_needs_notes` | `notes IS NOT NULL AND btrim(notes) <> ''` — whitespace-only strings are rejected |
| `fte_review_resolutions_rme_needs_claim_id` | `claim_id IS NOT NULL` — required because `source_position_id` goes stale after each Phase 0 reset |
| `fte_review_resolutions_rme_needs_position_type` | `target_type = 'position'` |
| `idx_fte_resolutions_single_active_evidence_request` | `UNIQUE (practice_id, claim_id) WHERE is_superseded = false AND action = 'request_more_evidence'` — at most one active evidence request per claim |

**Why `claim_id` as the anchor (not `source_position_id`):** identical rationale
to `dismiss_short_pay` (§5.1) and `confirm_short_pay` (§5.7) — `fte_financial_positions`
rows are deleted by Phase 0 on every reprocess, making `source_position_id`
stale after each run. `claim_id` is a hard FK to `fte_claims`, which Phase 0
never deletes.

**Why a single-action partial unique index (not a cross-action conflict index):**
`request_more_evidence` has no logical conflict partner in the current action
vocabulary. An active evidence request coexists correctly with an active
`dismiss_short_pay` or `confirm_short_pay` for the same claim. The partial
unique index prevents only duplicate simultaneous evidence requests for the same
claim. Contrast: `idx_fte_resolutions_single_active_position_short_pay`
(migration 006) spans two mutually exclusive actions.

**Supersession workflow:** to close an active evidence request (e.g. the
requested evidence has been received):

```sql
-- 1. Mark the existing request superseded.
update fte_review_resolutions
   set is_superseded = true
 where practice_id = '<practice_id>'
   and claim_id    = '<claim_id>'
   and action      = 'request_more_evidence'
   and is_superseded = false;

-- 2. Insert a substantive resolution (e.g. confirm_short_pay, dismiss_short_pay,
--    or attach_corrected_value) now that the evidence is available.
insert into fte_review_resolutions (practice_id, claim_id, action, ...)
values ('<practice_id>', '<claim_id>', 'confirm_short_pay', ...);

-- 3. Re-run the reconciler.  The evidence request no longer appears in
--    _fte_active_resolutions; the new resolution takes effect.
select fte_reconcile_practice('<practice_id>');
```

The superseded evidence-request row is retained in `fte_review_resolutions` as
a permanent audit trail — it is never deleted.

---

### 5.13 `mark_position_needs_correction` — durable correction-needed marker, no reconciler change

**What it records:** A reviewer decision that a financial position contains an
extraction or attribution error that must be corrected before the claim can be
resolved. The marker is a durable workflow note — it does not trigger any
automated correction and does not modify the claim's reconciler-derived status.

**Reconciler behavior: UNCHANGED.** No phase reads or filters on
`mark_position_needs_correction`. Phase 0.5 loads the row into
`_fte_active_resolutions` (all non-superseded rows are loaded unconditionally)
but no downstream phase acts on it. Specifically:

- Phase 6 (position derivation): unchanged — claim retains its reconciler-derived
  `reconciliation_status` (`in_review` or `unbalanced`) as if no correction marker existed.
- Phase 7 (queue routing): unchanged — `mark_position_needs_correction` does **not**
  suppress Phase 7 routing; the claim's review queue entry remains active. (Contrast:
  `dismiss_short_pay` and `confirm_short_pay` both suppress Phase 7 routing;
  `request_more_evidence` also does not suppress Phase 7.)
- Phase 8 (`short_pay_detected` event emission): unchanged — the event is emitted if
  the position math qualifies, regardless of any active correction marker. (Contrast:
  `dismiss_short_pay` suppresses Phase 8; `confirm_short_pay` does not suppress Phase 8.)
- No claim events are emitted as a result of inserting or superseding a
  `mark_position_needs_correction` row.

The claim must remain visible in the review queue and its `short_pay_detected`
event must remain active so that downstream correction workflows can engage.
Once a lower-level correction is applied (e.g. `attach_corrected_value` on an
affected observation), the reconciler re-derives the position from updated math
and the marker can be superseded.

**DB constraints (migration 008):**

| Constraint / index | Rule |
|---|---|
| `fte_review_resolutions_mpnc_needs_notes` | `notes IS NOT NULL AND btrim(notes) <> ''` — whitespace-only notes rejected; a correction-needed marker without an actionable explanation is not useful |
| `fte_review_resolutions_mpnc_needs_claim_id` | `claim_id IS NOT NULL` — required because `source_position_id` goes stale after each Phase 0 reset |
| `fte_review_resolutions_mpnc_needs_position_type` | `target_type = 'position'` |
| `idx_fte_resolutions_single_active_correction_needed` | `UNIQUE (practice_id, claim_id) WHERE is_superseded = false AND action = 'mark_position_needs_correction'` — at most one active correction-needed marker per claim |

**Why `claim_id` as the anchor (not `source_position_id`):** identical rationale
to §5.2 — `fte_financial_positions` rows are deleted wholesale by Phase 0 on
every reconciler run; `source_position_id` references those rows and becomes
stale immediately after the first rerun. `claim_id` is a hard FK to
`fte_claims`, which is never deleted by the reconciler — it is a stable,
permanent anchor that survives all reruns.

**Why a single-action partial unique index (not a cross-action conflict index):**
`mark_position_needs_correction` has no logical conflict partner in the current
action vocabulary. An active correction-needed marker coexists correctly with an
active `dismiss_short_pay`, `confirm_short_pay`, or `request_more_evidence` for
the same claim. The partial unique index prevents redundant duplicate markers
(two simultaneous active correction-needed markers for the same claim carry no
additional information), not cross-action conflicts.

**Supersession workflow:** when a lower-level correction resolves the issue, or
when the reviewer changes the description, supersede the existing marker and
insert a fresh row:

```sql
-- 1. Supersede the active marker.
update fte_review_resolutions
   set is_superseded = true
 where practice_id = '<practice_id>'
   and claim_id    = '<claim_id>'
   and action      = 'mark_position_needs_correction'
   and is_superseded = false;

-- 2. Optionally insert an updated marker or a substantive resolution
--    (e.g. attach_corrected_value) now that the correction is applied.

-- 3. Re-run the reconciler.  The old marker no longer appears in
--    _fte_active_resolutions; the reconciler re-derives the position from
--    the corrected observation amounts.
select fte_reconcile_practice('<practice_id>');
```

The superseded row is retained in `fte_review_resolutions` as a permanent audit
trail — it is never deleted.

**Coexistence note:** `mark_position_needs_correction` and `request_more_evidence`
(§5.12) serve complementary purposes. An evidence request means the reviewer
cannot resolve the claim without additional external information. A
correction-needed marker means the reviewer has identified a specific extraction
or attribution error that must be fixed in the ledger. Both may be active
simultaneously for the same claim — there is no conflict index between them.

**Validation:** `tests/validate_mark_position_needs_correction.sql` — 12 checks
using the 96c5c357 fixture (CLM-APC-1000 as the primary vehicle, CLM-APC-2000 as
the shape-violation and isolation target). Requires migration 008 applied before
running (check 9 uses the partial unique index).

---

## 7. How to run against fixtures

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

## 8. How to extend

**New observation-level resolution action:**

When you need a new resolution action that operates on `fte_observations` (not
on claim events or positions), follow this four-step guide:

1. **Phase 1 suppression (reject from reconciliation entirely):** if the new
   action should prevent an observation from being classified at all, add its
   `action` value to the `action IN (...)` list in Phase 0.5's
   `_fte_suppressed_observations` INSERT. Phase 1's `NOT EXISTS` filter will
   then exclude it from `_fte_classified` automatically.
2. **Phase 2 queue suppression only (classify but skip queue):** if the new
   action should let the observation classify normally in Phase 1 but suppress
   its queue entry, add a `NOT EXISTS (SELECT 1 FROM _fte_active_resolutions ar
   WHERE ar.observation_id = cl.id AND ar.action = '<new_action>')` branch to
   the Phase 2 WHERE clause (same pattern as `confirm_observation`).
3. **Typed FK to a target entity:** if the new action records a reference to
   another entity (similar to `mark_duplicate` → `target_observation_id`),
   add a new nullable FK column in a new migration and add a CHECK constraint
   that the column is NULL for all other action types.
4. **Tests:** add a fixture resolution INSERT and corresponding assertion checks
   to `tests/validate_observation_resolution.sql`.

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

## 9. Why a SQL stored procedure?

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

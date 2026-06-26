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
| **0.5** | Load active review resolutions: snapshot non-superseded `fte_review_resolutions` rows for this practice into temp table `_fte_active_resolutions ON COMMIT DROP`. Zero rows is valid — downstream phases behave identically to the no-resolution baseline. `DROP TABLE IF EXISTS` guard (same pattern as Phase 1's `_fte_classified`) ensures idempotency across multiple calls in the same outer transaction. `GET DIAGNOSTICS` captures the row count for `review_resolutions_applied` in the return JSON. Additionally builds `_fte_suppressed_observations ON COMMIT DROP` — the set of `observation_id` values where `action IN ('reject_observation', 'mark_duplicate')` — Phase 1 uses a `NOT EXISTS` subquery against this table to exclude suppressed observations from classification entirely. Also builds `_fte_rejected_payment_event_claims ON COMMIT DROP` — the set of `DISTINCT claim_id` values from active `reject_payment_event` resolutions — Phase 5c checks this table before each `payment_applied` INSERT and skips the INSERT when the claim matches (see §5.15). |
| **1** | Classify all observations into the temp table `_fte_classified` using the five rules above. `DROP TABLE IF EXISTS` ensures idempotency within the same outer transaction. |
| **2** | Route EXCLUDED and SUSPECT observations to `fte_review_queue`. Summary rows with no `claim_identifier` produce review entries with `claim_id = NULL`. A `confirm_observation` active resolution for an observation suppresses that observation's queue entry only (checked via `NOT EXISTS` on `_fte_active_resolutions`) — the observation still classifies in Phase 1 with its original rule, but no queue row is emitted. This is queue-only suppression: it does not promote an EXCLUDED observation to TRUSTED, and it does not change ledger events or positions. |
| **3** | Emit `claim_adjudicated` events from TRUSTED `billed_amount` observations. Each event gets one `derived_from` evidence link. A reviewer-supplied corrected billed amount is applied via `COALESCE` — see §4. |
| **4** | Emit `contractual_adjustment_applied` events from TRUSTED `contractual_adjustment` observations. `carc_code` is propagated. Each event gets one `derived_from` link. A reviewer-supplied corrected adjustment amount is applied via `COALESCE` — see §4. |
| **5c** | Emit `payment_applied` events from TRUSTED `payment` observations. Each event gets two `supports` evidence links: (1) the page observation, and (2) the matching `check_payment` stub (if one exists with `metadata->>'check_number' = check_eft_identifier`). A reviewer-supplied corrected payment amount is applied via `COALESCE` — see §4. Before each INSERT, the phase checks `_fte_rejected_payment_event_claims`; if the claim's `claim_id` matches, `CONTINUE` skips the INSERT entirely — the observation retains `classification = 'trusted'` and no event row is created for that claim (see §5.15). |
| **5** (late/retry) | For each `late_retry_page_contradiction` review entry that has an `observation_id`, find the `payment_applied` event for the same claim, then: **(a)** if an active `confirm_payment_event` resolution exists for the claim in `_fte_active_resolutions`, mark the event `reconciled`; otherwise mark it `ambiguous`; **(b)** wire the review entry's `claim_event_id` (unconditional); **(c)** insert a `contradicts` evidence link (unconditional — the contradiction record is always preserved even when the reviewer has confirmed the payment). If no payment event exists the loop is a no-op for that entry. |
| **6** | Derive `fte_financial_positions` for every claim that has at least one event OR at least one review queue entry. `reconciliation_status` CASE (evaluated in priority order): (1) no events → `in_review`; (2) any linked event has `reconciliation_status = 'ambiguous'` → `in_review` — **this applies even when the math balances to zero** (gap = 0). Financial truth cannot be finalized while contradicting evidence is unresolved; the claim must go to human review. Note: `'ambiguous'` is valid on `fte_claim_events` but is **not** a valid `fte_financial_positions.reconciliation_status` value (schema CHECK forbids it) — `in_review` is the correct surrogate; (3) any unbalanced event or positive open balance → `unbalanced`; (4) else → `balanced`. `open_balance_amount` is NULL when billed is unknown; otherwise `GREATEST(0, billed − adj − paid)`. `position_confidence_score` = MIN(confidence_score) across non–short_pay events, falling back to `0.0000`. |
| **7** | Route every `unbalanced` or `in_review` position to `fte_review_queue` with reason `unbalanced_financial_position`. **dismiss_short_pay / confirm_short_pay / mark_position_resolved suppression:** `unbalanced` positions are skipped when an active `dismiss_short_pay`, `confirm_short_pay`, or `mark_position_resolved` resolution exists for the claim in `_fte_active_resolutions`. The `fte_financial_positions` row is NOT changed — `reconciliation_status = 'unbalanced'` and `open_balance_amount` remain correct. `in_review` positions are always routed regardless of any resolution. |
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

### 5.14 `mark_position_resolved` — Phase 7 queue suppression for `unbalanced` positions

`mark_position_resolved` is a durable position-level reviewer decision that
signals: "I have reviewed this position and determined it no longer needs generic
queue routing — without committing to write it off (`dismiss_short_pay`) or
actively pursue recovery (`confirm_short_pay`)." It requires an explanatory note
so the reasoning is auditable.

#### Reconciler behavior (Phase 7 only)

When a non-superseded `mark_position_resolved` row is active for a claim, Phase 7
skips the `unbalanced_financial_position` review queue row for that claim. It is
added to the existing suppression IN-list alongside `dismiss_short_pay` and
`confirm_short_pay`:

```sql
fp.reconciliation_status <> 'unbalanced'
OR NOT EXISTS (
  SELECT 1
  FROM   _fte_active_resolutions ar
  WHERE  ar.claim_id = fp.claim_id
    AND  ar.action   IN ('dismiss_short_pay', 'confirm_short_pay',
                         'mark_position_resolved')
)
```

No other phase is affected:

- **Phase 0** — deletes `fte_financial_positions` / `fte_claim_events` / `fte_review_queue` / `fte_event_evidence` as normal. `fte_review_resolutions` is never touched by Phase 0.
- **Phase 0.5** — loads the `mark_position_resolved` row into `_fte_active_resolutions` alongside all other non-superseded resolutions (unconditional load).
- **Phase 6** — `reconciliation_status` and `open_balance_amount` are derived from events. `mark_position_resolved` does not change Phase 6 math. A resolved claim retains `reconciliation_status = 'unbalanced'` and the correct `open_balance_amount` — financial truth is preserved.
- **Phase 7** — suppresses the queue row for `unbalanced` positions only (see above). `in_review` positions are **never** suppressed — the Phase 7 guard explicitly checks `reconciliation_status = 'unbalanced'`. Unknown or ambiguous financial state must remain visible regardless of any resolution.
- **Phase 8** — `short_pay_detected` event emission is **not** suppressed. Contrast with `dismiss_short_pay`, which suppresses both Phase 7 and Phase 8. `mark_position_resolved` (and `confirm_short_pay`) preserve Phase 8 so downstream workflows can act on the confirmed short-pay signal if needed.

#### Shape constraints (migration 009)

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_mpr_needs_notes` | `notes IS NOT NULL AND btrim(notes) <> ''` when `action = 'mark_position_resolved'` |
| `fte_review_resolutions_mpr_needs_claim_id` | `claim_id IS NOT NULL` when `action = 'mark_position_resolved'` |
| `fte_review_resolutions_mpr_needs_position_type` | `target_type = 'position'` when `action = 'mark_position_resolved'` |
| `idx_fte_resolutions_single_active_position_resolved` | `UNIQUE (practice_id, claim_id) WHERE is_superseded = false AND action = 'mark_position_resolved'` — at most one active resolved marker per claim |

**Why notes is required:** `mark_position_resolved` records a subjective workflow
closure decision. Without a written explanation the reviewer's reasoning is lost.
A future reviewer or auditor cannot determine why the position was considered
resolved (e.g., "claim paid in full per ERA 2026-06-01", "write-off approved by
billing director").

**Why `claim_id` is the stable anchor:** `fte_financial_positions` rows are deleted
by Phase 0 on every reprocess; `source_position_id` becomes stale after each run.
`claim_id` is a hard FK to `fte_claims`, which Phase 0 never deletes. Identical
rationale to `dismiss_short_pay` (§5.1), `confirm_short_pay` (§5.6),
`request_more_evidence` (§5.12), and `mark_position_needs_correction` (§5.13).

**Why a partial unique index (not a cross-action conflict index):** `mark_position_resolved`
has no logical conflict partner in the current action vocabulary. It coexists
correctly with an active `dismiss_short_pay` or `confirm_short_pay` for the same
claim — all three suppress Phase 7 for `unbalanced` positions (different intent,
same suppression effect). The index only prevents duplicate simultaneous active
resolved markers.

#### Supersession workflow

To replace an active `mark_position_resolved`:

```sql
-- Step 1: supersede the current active row
UPDATE fte_review_resolutions
SET    is_superseded = true
WHERE  practice_id   = '<practice_id>'
  AND  claim_id      = '<claim_id>'
  AND  action        = 'mark_position_resolved'
  AND  is_superseded = false;

-- Step 2: insert the new marker (now the only active row)
INSERT INTO fte_review_resolutions (
  practice_id, claim_id, action, target_type, notes, resolved_by
) VALUES (
  '<practice_id>', '<claim_id>', 'mark_position_resolved', 'position',
  '<updated rationale>', '<reviewer_id>'
);
```

Superseded rows are retained as an audit trail. Phase 0.5 loads only
`WHERE is_superseded = false`, so the superseded row has no reconciler effect.

#### Contrast with all other position-level actions

| Action | Phase 7 (`unbalanced`) | Phase 7 (`in_review`) | Phase 8 (`short_pay_detected`) | Notes required |
|---|---|---|---|---|
| `dismiss_short_pay` | Suppressed | Never suppressed | **Suppressed** | No |
| `confirm_short_pay` | Suppressed | Never suppressed | Preserved | No |
| `mark_position_resolved` | **Suppressed** | **Never suppressed** | **Preserved** | **Yes** |
| `request_more_evidence` | Not suppressed | Not suppressed | Not suppressed | Yes |
| `mark_position_needs_correction` | Not suppressed | Not suppressed | Not suppressed | Yes |
| `reject_payment_event` | Not suppressed ¹ | Not suppressed ¹ | Not suppressed ¹ | **Yes** |

¹ `reject_payment_event` is a **payment-event-level** action, not a position-level
action. It acts in Phase 5c (suppresses `payment_applied` INSERT), not in Phase 7
or Phase 8. The claim remains `unbalanced` and `short_pay_detected` still emits —
with `open_balance_amount` equal to the full billed amount (no paid amount applied).
See §5.15 for the full specification.

**Validation:** `tests/validate_mark_position_resolved.sql` — 14 checks using the
96c5c357 fixture (CLM-APC-1000 as the primary vehicle for Phase 7 suppression and
Phase 8 preservation; CLM-APC-2000 as the `in_review` invariant and shape-violation
target). Requires migration 009 applied before running (check 11 uses the partial
unique index).

---

### 5.15 `reject_payment_event` — Phase 5c payment-event suppression

`reject_payment_event` is a **payment-event-level** reviewer decision that signals:
"Do not emit a `payment_applied` ledger event for this claim — the payment observation
is present in the source document but should not be treated as a settled ledger entry
at this time." The payment observation retains `classification = 'trusted'` (Phase 1
is unchanged). Only the Phase 5c ledger event emission is suppressed.

This action is distinct from position-level actions (`dismiss_short_pay`,
`confirm_short_pay`, `mark_position_resolved`): those act in Phase 7 or Phase 8 after
the financial position exists; `reject_payment_event` prevents the `payment_applied`
event from being created at all, so the financial position derives without any paid
amount.

#### Reconciler behavior (Phase 0.5 and Phase 5c)

**Phase 0.5** builds a second claim-set temp table alongside `_fte_suppressed_observations`:

```sql
DROP TABLE IF EXISTS _fte_rejected_payment_event_claims;

CREATE TEMP TABLE _fte_rejected_payment_event_claims ON COMMIT DROP AS
SELECT DISTINCT claim_id
FROM _fte_active_resolutions
WHERE action    = 'reject_payment_event'
  AND claim_id IS NOT NULL;
```

**Phase 5c** checks this table before each `payment_applied` INSERT:

```sql
IF EXISTS (
  SELECT 1
  FROM   _fte_rejected_payment_event_claims r
  WHERE  r.claim_id = v_obs.claim_uuid
) THEN
  CONTINUE;
END IF;
```

When the claim matches, the loop body skips the INSERT via `CONTINUE`. No event row
is created; no evidence link is created. The `fte_observations` row is untouched —
`classification` remains `'trusted'`.

#### Effect on subsequent phases

- **Phase 6** — `open_balance_amount` derives as `GREATEST(0, billed − adj − 0)` because
  `SUM(paid)` is 0 (no `payment_applied` event). The position derives as `unbalanced`.
  `reconciliation_status = 'unbalanced'` and `open_balance_amount = billed − adj` —
  financial truth reflects that no payment landed in the ledger.
- **Phase 7** — the `unbalanced` position is routed to `fte_review_queue` as normal.
  `reject_payment_event` does NOT suppress Phase 7 routing (contrast with
  `dismiss_short_pay` / `confirm_short_pay` / `mark_position_resolved`).
- **Phase 8** — `short_pay_detected` is emitted as normal with `open_balance_amount`
  equal to the full billed amount. `reject_payment_event` does NOT suppress Phase 8.

#### Shape constraints (migration 010)

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_rpe_needs_notes` | `notes IS NOT NULL AND btrim(notes) <> ''` when `action = 'reject_payment_event'` |
| `fte_review_resolutions_rpe_needs_claim_id` | `claim_id IS NOT NULL` when `action = 'reject_payment_event'` |
| `fte_review_resolutions_rpe_needs_payment_event_type` | `target_type = 'payment_event'` when `action = 'reject_payment_event'` |
| `idx_fte_resolutions_single_active_payment_event_decision` | `UNIQUE (practice_id, claim_id) WHERE is_superseded = false AND action IN ('confirm_payment_event', 'reject_payment_event')` |

**Why notes is required:** rejecting a payment event is a financially significant
decision — the payment is visible in the source document but the reviewer is asserting
it should not appear in the ledger. Without a written rationale the decision is not
auditable (e.g., "duplicate EFT — same check already applied via primary ERA",
"payment reversed by payer per phone call 2026-06-24").

**Why `claim_id` is the stable anchor:** `fte_claim_events` rows (including
`payment_applied`) are deleted by Phase 0 on every reprocess; `source_claim_event_id`
goes stale after the first Phase 0 reset. `claim_id` is a hard FK to `fte_claims`,
which Phase 0 never deletes. Same rationale as all other claim-anchored resolutions
(§5.1, §5.6, §5.12, §5.13, §5.14).

**Why a cross-action partial unique index:** `confirm_payment_event` promotes an
ambiguous payment event to `reconciled` (the reviewer asserts the payment is legitimate);
`reject_payment_event` suppresses the event entirely (the reviewer asserts the payment
should not be in the ledger). These are logically contradictory decisions for the same
claim — both active simultaneously would produce undefined behavior. The index mirrors
the migration 006 pattern (`confirm_short_pay` + `dismiss_short_pay`) and prevents
the contradiction at the DB level.

#### Supersession workflow

To replace an active `reject_payment_event`:

```sql
-- Step 1: supersede the current active row
UPDATE fte_review_resolutions
SET    is_superseded = true
WHERE  practice_id   = '<practice_id>'
  AND  claim_id      = '<claim_id>'
  AND  action        = 'reject_payment_event'
  AND  is_superseded = false;

-- Step 2: insert the replacement (or a confirm_payment_event to reverse the rejection)
INSERT INTO fte_review_resolutions (
  practice_id, claim_id, action, target_type, notes, resolved_by
) VALUES (
  '<practice_id>', '<claim_id>', 'reject_payment_event', 'payment_event',
  '<updated rationale>', '<reviewer_id>'
);
```

To reverse a rejection and allow the payment event to emit: supersede the
`reject_payment_event` row (step 1) and do not insert a replacement — or insert a
`confirm_payment_event` row instead.

#### Validation

`tests/validate_reject_payment_event.sql` — 18 checks using the 96c5c357 fixture
(`fixtures/synthetic_96c5c357_failure_modes.sql`):

- **Checks 1–4** — baseline (no resolution): `payment_applied` emitted, `open_balance_amount =
  $1,248.11`, `unbalanced`, `short_pay_detected` emitted, queue row present.
- **Checks 5–13** — with active `reject_payment_event`: `payment_applied` not emitted,
  `open_balance_amount = $1,600.00` (full billed), `unbalanced`, `short_pay_detected`
  emitted with recalculated amount, queue row present, observation remains `'trusted'`,
  CLM-APC-2000 unaffected.
- **Checks 14–15** — migration 010 constraint violations: blank notes rejected,
  null `claim_id` rejected.

Requires migration 010 applied before running (checks 14–15 exercise the CHECK
constraints).

---

### 5.16 `assert_check_identity` — durable check-identity note (no reconciler phase changes)

`assert_check_identity` is a **payment-event-level** durable reviewer note that signals:
"The canonical check number for this payment event is `<corrected_identifier>` — the
identifier stored in the source extraction is an OCR variant, a per-page reference
number, or a fragmented sub-identifier that does not represent the physical check."
The reviewer asserts the correct identity without modifying any ledger amounts.

This action is a **durable note only**: no reconciler phase changes. The corrected
identifier is stored in `fte_review_resolutions.corrected_identifier` for human
review and future phase integration (see Future Extension below). Phase 0.5 loads the
row into `_fte_active_resolutions`, but no downstream phase acts on it — payment events
still emit, positions and balances are unchanged, and Phase 7/8 are not suppressed.

The primary use case is Root Cause #7 (check fragmentation): one physical check split
across multiple `eob_payments` rows because EOB pages display different per-page
reference/control numbers. A reviewer who has confirmed the canonical check number
records it here so the assertion is auditable and available to future tooling that
can perform programmatic re-grouping.

#### Reconciler behavior — all phases UNCHANGED

- **Phase 0** — deletes `fte_financial_positions` / `fte_claim_events` / `fte_review_queue` /
  `fte_event_evidence` as normal. `fte_review_resolutions` is never touched by Phase 0.
- **Phase 0.5** — loads the `assert_check_identity` row into `_fte_active_resolutions`
  alongside all other non-superseded resolutions (unconditional load). The row
  increments the `review_resolutions_applied` counter that Phase 0.5 returns.
  No additional temp table (like `_fte_rejected_payment_event_claims`) is created —
  `assert_check_identity` has no downstream phase effect.
- **Phase 1** — observation classification is unchanged. The `assert_check_identity`
  action is NOT added to the `_fte_suppressed_observations` IN-list (contrast:
  `reject_observation`, `mark_duplicate`, `confirm_observation`). The payment
  observation remains classified as `'trusted'`.
- **Phase 5c** — `payment_applied` INSERT is not suppressed. Contrast with
  `reject_payment_event`, which skips the INSERT via `_fte_rejected_payment_event_claims`.
  `assert_check_identity` makes no change to Phase 5c — the event emits exactly as it
  would without the resolution.
- **Phase 6** — `open_balance_amount` and `reconciliation_status` derive from events
  as normal. An `assert_check_identity` row does not change Phase 6 math. Financial
  truth is preserved exactly.
- **Phase 7** — the `unbalanced_financial_position` queue row is NOT suppressed.
  `assert_check_identity` is not in the `dismiss_short_pay` / `confirm_short_pay` /
  `mark_position_resolved` IN-list.
- **Phase 8** — `short_pay_detected` event emission is NOT suppressed.

#### Shape constraints (migration 011)

| Constraint | Rule |
|---|---|
| `fte_review_resolutions_aci_needs_notes` | `notes IS NOT NULL AND btrim(notes) <> ''` when `action = 'assert_check_identity'` |
| `fte_review_resolutions_aci_needs_claim_id` | `claim_id IS NOT NULL` when `action = 'assert_check_identity'` |
| `fte_review_resolutions_aci_needs_observation_id` | `observation_id IS NOT NULL` when `action = 'assert_check_identity'` |
| `fte_review_resolutions_aci_needs_corrected_identifier` | `corrected_identifier IS NOT NULL AND btrim(corrected_identifier) <> ''` when `action = 'assert_check_identity'` |
| `fte_review_resolutions_aci_needs_payment_event_type` | `target_type = 'payment_event'` when `action = 'assert_check_identity'` |
| `idx_fte_resolutions_single_active_check_identity_observation` | `UNIQUE (practice_id, observation_id) WHERE is_superseded = false AND action = 'assert_check_identity'` — at most one active check identity assertion per payment observation (per-observation, not per-claim, because a claim may have multiple payment observations) |

**Why `corrected_identifier` is required:** an identity assertion without a canonical
identifier is meaningless — if the reviewer cannot supply the correct check number
they cannot assert identity. A NULL or blank `corrected_identifier` would store a
row that looks like an identity assertion but carries no actionable information.

**Why `notes` is required:** consistent with all financially-relevant reviewer actions
(`request_more_evidence`, `mark_position_needs_correction`, `mark_position_resolved`,
`reject_payment_event`). The notes field captures the reasoning — e.g., "confirmed
with payer: check #CK-0001 is the canonical check; page 3 displays the per-page
remittance reference number instead."

**Why `claim_id` is required:** `fte_claim_events` rows are deleted by Phase 0 on every
reprocess; `source_claim_event_id` goes stale after the first Phase 0 reset. `claim_id`
is a hard FK to `fte_claims`, which Phase 0 never deletes, and provides claim context for
the assertion. Same rationale as all other claim-anchored resolutions (§5.1, §5.6, §5.12,
§5.13, §5.14, §5.15).

**Why `observation_id` is the durable payment-observation anchor:** a claim may later have
multiple payment observations (multiple checks, EFT payments, or check-fragmentation
variants). Uniqueness must therefore be per observation, not per claim — the reviewer is
asserting the canonical identifier for a specific observed payment row, not for the claim
as a whole. `observation_id` is a FK to `fte_observations` (evidence layer, never deleted
by Phase 0), making it the stable, fine-grained anchor. `claim_id` gives claim context;
`observation_id` gives the specific payment-observation anchor.

**Why a single-action partial unique index on `(practice_id, observation_id)` (not a cross-action index):**
`assert_check_identity` has no logically contradictory counterpart in the current action
vocabulary. It coexists correctly with an active `confirm_payment_event` or
`reject_payment_event` on the same claim — asserting the canonical identifier does not
conflict with accepting or rejecting the payment event. Per-observation uniqueness is
correct because a claim may have multiple payment observations, each independently needing
an assertion. The index only prevents duplicate simultaneous active assertions for the
same observation (which would produce ambiguity about which identifier is canonical).
Contrast with the cross-action index in migration 006 (`confirm_short_pay` +
`dismiss_short_pay`) and migration 010 (`confirm_payment_event` + `reject_payment_event`),
both of which guard against logically contradictory decision pairs.

#### Supersession workflow

To replace an active `assert_check_identity` (e.g., after payer confirmation changes
the canonical check number):

```sql
-- Step 1: supersede the current active row
UPDATE fte_review_resolutions
SET    is_superseded = true
WHERE  practice_id   = '<practice_id>'
  AND  observation_id = '<observation_id>'
  AND  action        = 'assert_check_identity'
  AND  is_superseded = false;

-- Step 2: insert the corrected assertion (now the only active row)
INSERT INTO fte_review_resolutions (
  practice_id, claim_id, observation_id, action, target_type,
  corrected_identifier, notes, resolved_by
) VALUES (
  '<practice_id>', '<claim_id>', '<observation_id>', 'assert_check_identity', 'payment_event',
  '<canonical_check_number>', '<updated rationale>', '<reviewer_id>'
);
```

Superseded rows are retained as an audit trail — they are never deleted. Phase 0.5
loads only `WHERE is_superseded = false`, so superseded rows have no reconciler effect.

#### Future extension (Phase 5c check-number substitution)

The current implementation stores `corrected_identifier` as a passive record. A
future Phase 5c extension could read the active `assert_check_identity` row's
`corrected_identifier` and substitute it as the canonical `check_number` on the
`payment_applied` event — grouping fragmented payment events under a single
canonical identifier. This substitution is not yet wired. The current
per-observation unique index (`practice_id`, `observation_id`) already models
sub-claim granularity correctly: each payment observation may independently
receive an assertion, so the constraint is ready for Phase 5c integration
without modification.

#### Coexistence with other payment-event-level actions

| Action | Phase 5c (`payment_applied`) | Phase 7 (`unbalanced`) | Phase 8 (`short_pay_detected`) | Notes required |
|---|---|---|---|---|
| `confirm_payment_event` | **Preserved** (promotes ambiguous→reconciled) | Not suppressed | Not suppressed | No |
| `reject_payment_event` | **Suppressed** (event not emitted) | Not suppressed | Not suppressed | Yes |
| `assert_check_identity` | **Preserved** (no suppression) | Not suppressed | Not suppressed | **Yes** |

#### Validation

`tests/validate_assert_check_identity.sql` — 13 checks using the 96c5c357 fixture
(`fixtures/synthetic_96c5c357_failure_modes.sql`):

- **Check 1** — baseline: `payment_applied` event emits (count >= 1).
- **Check 2** — baseline: `status=unbalanced`, `open_balance_amount=$1,248.11`.
- **Check 3** — valid assertion with dual anchors (`claim_id` + `observation_id=a2`,
  `corrected_identifier='CK-0001'`): `review_resolutions_applied=1`.
- **Check 4** — `payment_applied` still emits after assertion (Phase 5c unchanged).
- **Check 5** — position unchanged: `status=unbalanced`, `open_balance_amount=$1,248.11`.
- **Check 6** — `corrected_identifier='CK-0001'` stored correctly in `fte_review_resolutions`.
- **Check 7** — duplicate active assertion on same `(practice_id, observation_id=a2)` → `unique_violation`.
- **Check 8** — blank `corrected_identifier` (uses obs b1) → `check_violation`.
- **Check 9** — blank `notes` (uses obs b2) → `check_violation`.
- **Check 10** — null `claim_id` (uses obs b3) → `check_violation`.
- **Check 11** — null `observation_id` → `check_violation`.
- **Check 12** — wrong `target_type='observation'` (uses obs a1, the billed_amount obs) → `check_violation`.
- **Check 13** — supersession: UPDATE is_superseded=true for obs a2, INSERT new row for obs a2
  with `corrected_identifier='CK-0002'`; `review_resolutions_applied=1`, one active row,
  new identifier stored.

Requires migration 011 applied before running (checks 8–9 exercise the CHECK constraints
and partial unique index).

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

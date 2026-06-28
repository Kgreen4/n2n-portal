# CODEX Task 006G — Real AI Extraction Boundary Design/Spec

**Status:** Spec-only — approved for design. Implementation requires separate approval.
**Created:** 2026-06-28
**Author:** Keith Green / N2N Analytics
**Depends on:** Tasks 001–006F (all complete and merged)
**Validation baseline:** 209 numeric checks across 18 suites (unchanged by this PR)

---

## Purpose

Define every architectural decision required before implementing a real AI
observation extractor that replaces `fte_mock_extract_observations`. This spec
documents the interface contract, safety gates, open decisions, and implementation
constraints so that a future implementation task can be approved and executed without
ambiguity.

This task produces documentation only. No SQL, no AI calls, no Edge Functions,
no migrations, no fixtures, no test changes.

---

## Current Proven Baseline After Task 006F

Task 006F delivered `fte_mock_extract_observations(p_practice_id uuid) RETURNS integer`,
a deterministic SQL-only mock of the AI extraction boundary. It proved the following
end-to-end pipeline on synthetic data:

```text
fte_evidence (page rows, [SYNTHETIC] raw_text)
  → fte_mock_extract_observations()        [extraction boundary]
  → fte_observations (3 rows per page)     [stable observation interface]
  → fte_reconcile_practice()               [9-phase deterministic reconciler]
  → fte_financial_positions                [derived financial truth]
  → fte_claim_events / fte_review_queue    [auditable event ledger]
  → fte_explain_claim()                    [structured JSON explanation]
```

Proven outcomes:
- CLM-P3B-0001: balanced, open_balance = $0.00
- CLM-P3B-0002: unbalanced, open_balance = $70.00, review queue row present
- `fte_explain_claim` returns correct structured JSON end-to-end
- 17 validation checks, all passing

The mock extractor proved that the interface contract works. The real extractor
must satisfy the same contract.

---

## Non-Goals for This Spec PR

The following are **not** decisions made in this PR:

- Model or vendor selection (Claude, Gemini, or other). See § Open Decisions.
- Prompt text, temperature, or token budget.
- Infrastructure hosting (psql extension, Edge Function, external service).
- Any use of real PDFs, real patient data, real payer exports, or production data.
- Any use of secrets, API keys, project URLs, or DB URLs.
- Any change to `fte_reconcile.sql`, migrations, fixtures, or test suites.
- Any implementation of the real AI extractor.
- Any change to the 209/18 validation baseline.

---

## Safety and Privacy Gates

The following gates apply to any future implementation task. None may be bypassed
without explicit written approval from Keith before implementation begins.

### Gate 1 — Evidence Approval
"Approved evidence" for any implementation or testing phase means:
- Synthetic fixtures (as used in Tasks 001–006F), OR
- Explicitly approved de-identified test evidence (Keith approves per document batch).

Real PDFs, raw payer exports, live patient records, real member IDs, real DOBs,
real SSNs, real check numbers, and real claim identifiers are **not** approved
evidence for any FTE implementation or test task unless Keith explicitly approves
them in writing for a specific named task.

### Gate 2 — AI Calls
No live AI calls of any kind (to any model, any vendor, any endpoint) may be made
in any FTE task unless a future approved task explicitly authorizes them. The
current ban on live AI calls in CLAUDE.md applies to all tasks through the end of
this spec.

### Gate 3 — Edge Functions and Infra
Any implementation that requires an Edge Function, an HTTP extension, a cloud
service call, secrets injection, or environment variables must receive separate
explicit approval before that infrastructure is provisioned or deployed. Edge
Function work is not authorized by this spec.

### Gate 4 — Secrets and Credentials
No API keys, model endpoint URLs, project URLs, DB URLs, service-role keys, anon
keys, or connection strings may appear in any FTE file, fixture, migration, or
test. This applies to the real extractor implementation, any prompt file, and any
environment configuration committed to the repo.

### Gate 5 — Model and Vendor Selection
The implementation task must not permanently bind the system to a specific AI model
or vendor. Model selection must be made at implementation time based on:
- Current model capabilities and pricing at that date
- Accuracy on de-identified test evidence (empirically evaluated)
- Keith's explicit approval of the selected model

This spec is intentionally model-neutral. Do not interpret any language here as
selecting Claude, Gemini, or any other model.

---

## Stable Observation Contract

The `fte_observations` table shape proven by Task 006F is the stable interface.
The real extractor must produce observations that satisfy this shape exactly.

### Required columns per observation row

| Column | Type | Requirement |
|---|---|---|
| `practice_id` | uuid | Must match the target practice |
| `evidence_id` | uuid | FK to the source `fte_evidence` row |
| `observation_type` | text | One of: `billed_amount`, `contractual_adjustment`, `payment` (at minimum) |
| `amount` | numeric(14,2) | Extracted numeric value |
| `amount_type` | text | Matches `observation_type` semantics |
| `claim_identifier` | text | Extracted claim number (payer-assigned) |
| `payer_name` | text | Extracted payer name |
| `service_date` | date | Extracted service date (nullable if unreadable) |
| `cpt_code` | text | Extracted CPT/procedure code (nullable) |
| `check_eft_identifier` | text | Check or EFT number (nullable; payment obs only) |
| `confidence_score` | numeric(5,4) | Model confidence — must be set, not null |
| `raw_value` | text | The raw string from which `amount` was parsed |
| `normalized_value` | text | Canonical two-decimal string form |
| `page_number` | integer | Source page number from `fte_evidence` |
| `is_summary_row` | boolean | True if the row appears to be a summary/total row |
| `is_superseded` | boolean | Always false on insert |
| `metadata` | jsonb | Must include `extractor` (function/model identifier) and `mock: false` |

### Classification rule
The real extractor must NOT set `classification`. Phase 1 of `fte_reconcile_practice`
owns all classification decisions. This is a hard interface invariant.

### Confidence score semantics
A real extractor must emit a real model confidence value, not a hardcoded constant.
The reconciler's Phase 1 classification rules use the confidence score to route
observations to `trusted`, `suspect`, or `excluded`. The threshold at which an
observation is marked `suspect` rather than `trusted` is a Phase 1 tunable — it
is not the extractor's responsibility to enforce it.

Current Phase 1 thresholds (from `fte_reconcile.sql` — reconciler-owned, not
extractor-owned):
- `confidence_score < 0.70` → `excluded`
- `confidence_score < 0.85` → `suspect`
- `confidence_score >= 0.85` → `trusted`

The extractor must emit confidence values on the `[0.00, 1.00]` range. What those
values mean depends on the model's output calibration — validating calibration
against real evidence is part of the implementation task, not this spec.

### Idempotency requirement
The real extractor must be idempotent per `(practice_id, evidence_id, claim_identifier,
extractor)`. If called twice on the same evidence without new evidence rows, it must
not insert duplicate observations. The mock extractor's pattern (check if 3 obs already
exist, skip if so) is a reference implementation — the real extractor may use a stricter
content-hash-based dedup strategy.

---

## Proposed Extractor Boundary Options

Three deployment patterns are available. Each has different infrastructure requirements
and approval gates. None is selected by this spec; Keith must choose one before
implementation begins.

### Option 1 — SQL Function Calling an HTTP Extension

```text
fte_extract_observations(p_practice_id uuid, p_evidence_id uuid) RETURNS integer
  (plpgsql VOLATILE, same signature pattern as fte_mock_extract_observations)

  → calls pg_net or http extension with evidence raw_text
  → calls AI model endpoint
  → parses structured response
  → inserts fte_observations rows
  → returns count of inserted rows
```

**Pros:** Stays within the SQL layer; no new runtime surface; caller pattern identical
to mock extractor; easiest to validate with existing ROLLBACK-wrapped test suites.

**Cons:** Requires an HTTP extension (pg_net or equivalent) to be enabled in the
Supabase project — a separate infrastructure approval. Synchronous; blocks the DB
connection for the duration of the AI call. May hit Supabase function timeout limits
for large documents.

**Infra gate:** Requires Gate 3 approval (Edge Function / HTTP extension / cloud
service call) before provisioning.

### Option 2 — Edge Function Orchestrator

```text
POST /functions/v1/fte-extract-observations
  { practice_id, evidence_id }

  → reads fte_evidence.raw_text from Supabase
  → calls AI model API
  → parses structured response
  → inserts fte_observations rows via service-role client
  → returns { inserted_count }
```

**Pros:** Decoupled from DB connection; async-friendly; can handle timeouts and
retries outside the DB; can be called from n8n, a UI trigger, or a batch runner.

**Cons:** Requires an Edge Function (Deno runtime) — a separate infra approval.
Requires a service-role key and AI API key to be set as Supabase secrets. Cannot
be tested with ROLLBACK-wrapped SQL suites; requires a separate integration test
strategy (see § Validation Strategy for the Future Implementation).

**Infra gate:** Requires Gate 2 (AI calls), Gate 3 (Edge Function), and Gate 4
(secrets) approvals before provisioning.

### Option 3 — External Script / CLI Tool

```text
python fte_extract.py --practice-id <uuid> --evidence-id <uuid>
  (or Node.js equivalent)

  → reads evidence from DB
  → calls AI model API
  → inserts fte_observations rows
  → prints { inserted_count }
```

**Pros:** Easiest to iterate on during development; no Supabase infra changes;
can be run locally against a disposable Supabase project; test strategy mirrors
the SQL mock — load fixture, run script, verify observations in SQL Editor.

**Cons:** Requires a local runtime dependency; not suitable for production
automation without a runner (GCP VM, n8n, cron); secrets managed outside the
repo (`.env` file, not committed).

**Infra gate:** Requires Gate 2 (AI calls) and Gate 4 (secrets management
outside the repo) approvals before implementation.

---

## Recommended Architecture

**Recommended starting point: Option 3 (external script) against a disposable
Supabase project with de-identified test evidence.**

Rationale:
- Allows AI call approval and model selection to be evaluated empirically on
  approved test evidence before any Supabase infra changes.
- Does not require Edge Function provisioning, service-role key injection into
  production Supabase secrets, or HTTP extension enablement.
- The observation-shape contract is model-neutral and identical to the mock;
  switching to Option 1 or 2 later does not require changing the contract or
  the reconciler.
- Keeps the iteration loop short: edit prompt → run script → inspect observations
  in SQL Editor → run reconciler → check positions.

If Option 3 proves the extraction quality on approved test evidence, the
implementation can graduate to Option 1 or 2 in a subsequent approved task.

**This recommendation does not constitute a decision.** Keith must approve the
chosen option in writing before the implementation task begins.

---

## Open Decisions Requiring Keith Approval

The following decisions must be resolved before a real implementation task can
be written and approved:

| # | Decision | Options | Why it blocks implementation |
|---|---|---|---|
| 1 | Deployment pattern | Option 1, 2, or 3 (see above) | Determines infra gates, test strategy, and secrets handling |
| 2 | AI model and vendor | Model-neutral until approval | Must be selected at implementation time based on current capabilities and empirical accuracy testing |
| 3 | Evidence input format | `raw_text` only vs. multimodal (PDF page image) | Determines whether the extractor needs PDF rendering capability and what evidence rows must contain |
| 4 | Observation types in scope | `billed_amount`, `contractual_adjustment`, `payment` (current) + CARC/RARC, CPT, patient-responsibility? | Determines whether Phase 1 classification rules need extension before real extraction |
| 5 | Confidence threshold tuning | Current Phase 1 thresholds (0.70 / 0.85) | Need empirical validation against real model output before implementation |
| 6 | First approved test evidence | Synthetic only, or a named set of de-identified documents | Gate 1 — cannot run any real extraction test without this approval |
| 7 | Prompt review process | Keith reviews and approves prompt text before any AI call | The prompt is a policy artifact — it determines what the AI is asked to observe and assert |
| 8 | Error handling policy | Skip page / insert low-confidence placeholder / raise | Determines what happens when the AI returns a malformed or empty response |
| 9 | Batch vs. per-page invocation | Extract one page per call vs. entire document per call | Affects token budget, cost, and idempotency granularity |

---

## Future Implementation Task Outline

Once Keith approves the open decisions above, a Task 006H (or numbered as
appropriate) implementation spec should cover:

1. **Infrastructure provisioning** (whichever option is approved): HTTP extension
   enablement, Edge Function creation, or script runtime setup — all under Gate 3.
2. **Secrets setup**: AI API key and any endpoint configuration — under Gate 4.
   Secrets must not be committed to the repo.
3. **Prompt authoring**: Keith reviews and approves the extraction prompt before
   any AI call is made — Decision #7 above.
4. **Test evidence approval**: A named set of de-identified documents approved for
   extraction testing — Gate 1 / Decision #6 above.
5. **Extractor implementation**: Function or script matching the stable observation
   contract (§ Stable Observation Contract).
6. **Observation quality validation**: Compare AI-extracted observations against
   known-good values from the approved test evidence. Evaluate confidence
   calibration against Phase 1 thresholds.
7. **End-to-end reconciler run**: Call `fte_reconcile_practice` after extraction;
   verify `fte_financial_positions` match expected values for the approved test
   evidence.
8. **Validation suite** (see § Validation Strategy).
9. **Documentation updates**: `NEXT_STEPS.md`, `README.md`, `reconciler/README.md`.

---

## Validation Strategy for the Future Implementation

The mock extractor's ROLLBACK-wrapped SQL suite pattern is not directly applicable
to a real AI extractor because AI calls are non-deterministic and cannot be run
inside a database transaction. The future implementation task must define an
alternative validation approach. Recommended minimum:

### Tier 1 — Observation shape contract (SQL, ROLLBACK-wrapped)
After running the real extractor against approved test evidence:
- Verify all inserted `fte_observations` rows satisfy the shape contract
  (§ Stable Observation Contract): no null `confidence_score`, correct `metadata`,
  correct `observation_type` values, `is_superseded = false`, `classification = null`.
- These checks are model-independent and can be ROLLBACK-wrapped in the SQL Editor.

### Tier 2 — Financial position accuracy (SQL, committed run)
- Run `fte_reconcile_practice` on the practice that received real extractions.
- Compare `fte_financial_positions` values against the known-correct financial
  outcomes for the approved test evidence (pre-computed from the source documents
  by Keith or a designated reviewer).
- If positions are wrong: inspect `fte_review_queue` and `fte_claim_events` to
  locate the classification or math error; adjust confidence thresholds or prompt.

### Tier 3 — Review queue routing accuracy (SQL, committed run)
- Verify that claims the real documents show as ambiguous or underpaid are routed
  to `fte_review_queue` with the expected `reason` values.
- This is the primary quality signal: the system's uncertainty routing must match
  a human reviewer's assessment of which claims need review.

### What the future task must NOT do
- Hardcode expected observation values in the test suite (AI output varies).
- Mark real extraction complete until Tier 1, 2, and 3 validations pass for
  at least two independent approved test documents.
- Count AI-extraction validation checks in the numeric baseline until they are
  stable and reproducible (non-determinism makes count-based assertions fragile
  for Tier 2/3).

---

## Files Explicitly Forbidden for This PR (006G)

The following files must not be created, modified, or deleted as part of Task 006G:

- Any `*.sql` file (migrations, fixtures, reconciler, tests, functions)
- `financial-truth-engine/reconciler/fte_reconcile.sql`
- `financial-truth-engine/reconciler/fte_mock_extract_observations.sql`
- `financial-truth-engine/reconciler/fte_explain_claim.sql`
- `financial-truth-engine/migrations/` (any file)
- `financial-truth-engine/fixtures/` (any file)
- `financial-truth-engine/tests/` (any file)
- `financial-truth-engine/reconciler/README.md`
- `financial-truth-engine/README_SCHEMA.md`
- Any Edge Function file
- Any UI or frontend file
- Any legacy EOB extraction file
- Any config file (`.env`, `settings.json`, `.mcp.json`, etc.)
- Any file containing secrets, credentials, project URLs, DB URLs, or API keys
- `CLAUDE.md`

---

## Exit Criteria for This Spec-Only PR

This PR is ready to merge when:

1. `CODEX_TASK_006G.md` is present and complete.
2. `NEXT_STEPS.md` Immediate Next Action section reflects 006G as the current
   design checkpoint.
3. `README.md` status line is updated to note 006G as a spec checkpoint.
4. No SQL files, fixture files, test files, migrations, or reconciler files have
   been created or modified.
5. No secrets, credentials, project URLs, DB URLs, or PHI appear in any changed file.
6. Validation baseline is unchanged: **209 numeric checks across 18 suites**.
7. The spec does not claim real AI extraction is complete.
8. Keith has reviewed and approved the spec PR before the implementation task is
   written.

---

*This document is a design specification only. Real AI extraction is not
implemented. Do not begin implementation until Keith explicitly approves a
separate implementation task referencing this spec.*

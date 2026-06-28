# CODEX Task 006H — Real AI Observation Extractor External Script Implementation Spec

**Status:** Spec-only — approved for design. Implementation requires separate approval.
**Created:** 2026-06-28
**Author:** Keith Green / N2N Analytics
**Depends on:** Tasks 001–006G (all complete and merged)
**Validation baseline:** 209 numeric checks across 18 suites (unchanged by this PR)

---

## Purpose

Define every implementation detail required to build a provider-configurable external
extraction script that satisfies the stable observation contract proven by
`fte_mock_extract_observations` in Task 006F. This spec resolves the nine open decisions
from CODEX_TASK_006G.md and specifies the script interface, de-identification
requirements, provider configuration, error handling, prerequisite gates, and
validation strategy.

This task produces documentation only. No script is delivered. No AI calls are made.
No model or vendor is locked. No credentials are provisioned.

---

## Depends On

Tasks 001–006G complete and merged. Nine open decisions from CODEX_TASK_006G.md
resolved 2026-06-28 (see § Approved Decisions below).

---

## Non-Goals for This Spec PR

The following are not delivered in this PR:

- The extraction script itself
- Any AI calls (to any model, any vendor, any endpoint)
- Any hardcoded model ID or vendor binding
- Any secrets, API keys, project URLs, or DB URLs
- Any real PDFs, real patient data, real payer exports, real check numbers, real
  member IDs, real DOBs, patient names
- Any SQL changes, migrations, fixtures, tests, or reconciler changes
- Any Edge Functions, UI, or legacy EOB code changes
- Any config files with values (`.env`, `settings.json`, etc.)
- Any change to the 209/18 validation baseline
- Any claim that real AI extraction is complete

---

## Approved Decisions

The following nine decisions were approved by Keith on 2026-06-28 before this spec
was written. They are recorded here verbatim as the binding defaults for the
implementation task.

| # | Decision | Approved Default |
|---|---|---|
| 1 | Deployment pattern | Option 3 — external script against disposable Supabase |
| 2 | Model / vendor | Provider-configurable and runtime-selected; no hardcoded model ID |
| 3 | Evidence input format | `raw_text` only for first implementation |
| 4 | Observation types in scope | `billed_amount`, `contractual_adjustment`, `payment` only |
| 5 | Confidence thresholds | Unchanged; keep current Phase 1 thresholds (0.70 / 0.85) |
| 6 | Approved test evidence | Keith must approve a named de-identified batch before any AI call |
| 7 | Prompt review | Keith must approve final prompt text before any AI call |
| 8 | Error handling | Skip malformed/empty page and log warning; do not insert placeholder observations |
| 9 | Invocation granularity | Per-page; one AI call per page evidence row |

### Amendments

**Amendment A — Provider-configurable, no model ID locked:**
The implementation must accept the model identifier and provider as runtime parameters.
No specific model name or version is selected or preferred in this spec. Provider names
may appear only as generic adapter examples; final provider and model selection remains
a runtime approval decision. Selection happens only after prompt and evidence-batch
approval, based on empirical accuracy on the approved test evidence at implementation time.

**Amendment B — PHI and PII scope extended:**
The following fields are treated as potentially identifying regardless of whether they
appear in a document labeled "de-identified":

- Claim numbers
- Check / EFT numbers
- Member IDs
- Dates of birth
- Patient names
- Social Security numbers
- Payer exports (in any format)

Claim and check identifiers may be retained only if Keith explicitly approves that
specific de-identified batch in writing for a named task. Otherwise, all of the above
must be replaced with synthetic values or hashes before any evidence is loaded into
a disposable Supabase project or passed to an AI model.

---

## Safety and Privacy Gates (Inherited from CODEX_TASK_006G.md)

The five gates from CODEX_TASK_006G.md apply unchanged to the implementation task.
None may be bypassed without explicit written approval from Keith.

| Gate | Requirement |
|---|---|
| Gate 1 — Evidence Approval | Synthetic fixtures or a Keith-approved named de-identified batch only |
| Gate 2 — AI Calls | No live AI calls until a future approved task explicitly authorizes them |
| Gate 3 — Edge Functions and Infra | No Edge Functions, HTTP extensions, or cloud service calls without separate approval |
| Gate 4 — Secrets and Credentials | No API keys, DB URLs, project URLs, or service-role keys in any committed file |
| Gate 5 — Model and Vendor Selection | No permanent binding to a specific model or vendor |

---

## Script Contract

### Conceptual Signature

```text
fte_extract_observations.py
  --practice-id   <uuid>           required; target practice
  --evidence-id   <uuid>           optional; single page only; if omitted, process
                                   all unextracted page evidence rows for the practice
  --provider      <provider-name>  required; runtime-selected; not hardcoded in the script
  --model         <provider-model-id> required; runtime-selected, not hardcoded in the script
  --db-env        DATABASE_URL     required; names the environment variable holding the
                                   DB connection string — never the actual URL
  --dry-run                        optional; parse and validate prompt/evidence but do not
                                   insert; logs what would be inserted
```

The script is a local Python (or equivalent) CLI tool. It requires no Supabase
infrastructure changes and no Edge Function. It is run locally against a disposable
Supabase project using approved test evidence.

### Secrets Pattern

- AI API keys are read from environment variables (e.g. `<PROVIDER>_API_KEY`).
  They are never passed as CLI arguments.
- The DB connection string is stored in an environment variable (e.g. `DATABASE_URL`).
  The `--db-env` flag names the environment variable; the actual URL is never a CLI
  argument and never committed to the repo.
- A placeholder env-var template file (e.g. `.env.example` with no values) is an
  option for the implementation task to document required variables. Whether to commit
  such a file is an implementation-task decision; it is not created in this spec PR.
- The real `.env` file must be gitignored and never committed.

### Input

The script reads `fte_evidence` rows where:
- `practice_id = <--practice-id value>`
- `evidence_type = 'page'`
- `raw_text IS NOT NULL`

It passes `raw_text` to the AI model. Multimodal (PDF image) input is not in scope
for the first implementation (Decision 3). The `raw_text` field must not contain
unredacted PHI or PII beyond what Keith has explicitly approved for the specific
evidence batch (Amendment B).

### Output

For each page evidence row, the script inserts `fte_observations` rows into the
disposable Supabase project. Each observation must satisfy the stable observation
contract defined in CODEX_TASK_006G.md § Stable Observation Contract (reproduced
below for reference).

### Stable Observation Contract (Reference)

Each inserted `fte_observations` row must satisfy the following:

| Column | Requirement |
|---|---|
| `practice_id` | Must match the target practice |
| `evidence_id` | FK to the source `fte_evidence` row |
| `observation_type` | One of: `billed_amount`, `contractual_adjustment`, `payment` |
| `amount` | Extracted numeric value, two decimal places |
| `amount_type` | Matches `observation_type` semantics |
| `claim_identifier` | Extracted claim number (synthetic or approved de-identified) |
| `payer_name` | Extracted payer name |
| `service_date` | Extracted service date (nullable if unreadable) |
| `cpt_code` | Extracted CPT/procedure code (nullable) |
| `check_eft_identifier` | Check or EFT number (nullable; payment obs only) |
| `confidence_score` | Real model confidence value on `[0.00, 1.00]`; must be set, not null |
| `raw_value` | The raw string from which `amount` was parsed |
| `normalized_value` | Canonical two-decimal string form |
| `page_number` | Source page number from `fte_evidence` |
| `is_summary_row` | True if the row appears to be a summary/total row |
| `is_superseded` | Always false on insert |
| `metadata` | Must include `extractor` (model/provider identifier string) and `mock: false` |

**Classification invariant:** The extractor must not write a `classification` value.
`classification` is not a column set by the extractor. Phase 1 of
`fte_reconcile_practice` owns all classification decisions based on `confidence_score`.
Current Phase 1 thresholds (reconciler-owned, not extractor-owned):
- `confidence_score < 0.70` → `excluded`
- `confidence_score < 0.85` → `suspect`
- `confidence_score >= 0.85` → `trusted`

The extractor emits a real model confidence value; the reconciler classifies.

### Idempotency

The script must be idempotent per `(practice_id, evidence_id, claim_identifier,
extractor)`. If called twice on the same evidence without new evidence rows, it must
not insert duplicate observations. The mock extractor's pattern (check if 3 obs already
exist, skip if so) is a reference implementation. A stricter content-hash-based dedup
strategy is an acceptable alternative.

### Error Handling (Decision 8)

If the AI returns a malformed, empty, or unparseable response for a page:
- Log a structured warning to stderr: `evidence_id`, `page_number`, error detail.
- Skip the page. Do not insert placeholder observations.
- Continue to the next page evidence row.
- Do not halt the run.

The count of skipped pages must be reported in the final summary output.

---

## Provider Configuration

The script must not hardcode any model ID, vendor URL, or API base path. All of the
following must be accepted as runtime parameters or environment variables:

- Provider name (`--provider <provider-name>`) — runtime-selected, not hardcoded
- Model identifier (`--model <provider-model-id>`) — runtime-selected, not
  hardcoded in the script source
- API key — from environment variable only
- API endpoint — optional override from environment variable; default to provider SDK
  default

A thin provider adapter layer (e.g. a small abstraction over the provider SDK) is the
recommended implementation pattern. This allows switching providers without changing
the core extraction logic or the observation-insertion logic.

---

## Prompt Contract

This spec defines what the prompt must ask for and what structured output shape is
required. The actual prompt text is a separate Keith-approved artifact and is NOT
delivered in this spec.

**What the prompt must ask the AI to identify (visible facts only):**
- Billed amount for each claim line or summary
- Contractual adjustment amount
- Payment / paid amount
- Claim identifier (payer-assigned)
- Payer name
- Service date
- CPT/procedure code (if visible)
- Check or EFT identifier (if visible on a payment page)

**What the prompt must NOT ask the AI to assert:**
- Final financial truth
- Whether the claim is correctly adjudicated
- Whether the payer underpaid
- Any denial determination

**Required structured output shape:**
The AI must return a structured response (e.g. JSON) that maps to the observation
contract columns. The implementation task must define the exact JSON schema. The
response parser must validate the shape before inserting any observations.

---

## De-identification Requirements (Amendment B)

Before any approved test evidence is loaded into a disposable Supabase project or
passed to an AI model, the following rules apply:

| Field | Rule |
|---|---|
| Patient name | Always replace with synthetic value (e.g. `PATIENT-0001`) |
| Date of birth | Always replace with synthetic value (e.g. `1970-01-01`) |
| Member ID | Always replace with synthetic value or hash |
| SSN | Always replace; never retain in any form |
| Claim number | Retain only with explicit per-batch Keith approval; otherwise hash or replace with `CLAIM-XXXX` |
| Check / EFT number | Same gate as claim number |
| Payer name | May be retained as payer identification is necessary for reconciliation; subject to per-batch approval |
| Source URI in `fte_evidence` | Must use `private://fte/de-identified/...` scheme; not a real file path or URL |

The implementation task must include a documented de-identification step before any
evidence is loaded. Keith must explicitly approve the de-identified batch in writing
before the script runs against it.

---

## Prerequisites Before Any AI Call

The implementation task must verify each item below before the script is run with a
real AI provider. These are hard gates, not suggestions.

```text
[ ] Keith has approved the specific de-identified evidence batch (Decision 6 / Gate 1)
[ ] Keith has approved the final prompt text (Decision 7)
[ ] Keith has selected the model and provider at runtime (Decision 2 / Gate 5)
[ ] All PHI/PII fields have been replaced per Amendment B de-identification rules
[ ] .env is local-only, gitignored, not committed
[ ] Disposable Supabase project confirmed — not the legacy EOB Supabase project
[ ] No secrets, API keys, project URLs, or DB URLs appear in any committed file
```

---

## Validation Strategy

The mock extractor's ROLLBACK-wrapped SQL suite pattern cannot be applied directly
to a real AI extractor because AI output is non-deterministic and cannot be run inside
a database transaction. The implementation task must use the three-tier approach defined
in CODEX_TASK_006G.md § Validation Strategy.

### Tier 1 — Observation shape contract (SQL, ROLLBACK-wrapped)

After running the real extractor against approved test evidence, verify:
- All inserted `fte_observations` rows satisfy the shape contract (§ Stable Observation
  Contract above)
- `confidence_score` is not null and is on `[0.00, 1.00]`
- `metadata->>'mock'` is `'false'`
- `metadata->>'extractor'` is set to the model/provider identifier string
- `observation_type` is one of: `billed_amount`, `contractual_adjustment`, `payment`
- `is_superseded = false` on all inserted rows
- The extractor has not written a `classification` value (the column is not set by
  the extractor; Phase 1 owns it)

These checks are model-independent and can be ROLLBACK-wrapped in the SQL Editor.

### Tier 2 — Financial position accuracy (SQL, committed run)

- Run `fte_reconcile_practice` on the practice that received real extractions.
- Compare `fte_financial_positions` values against known-correct financial outcomes for
  the approved test evidence (pre-computed from the source documents by Keith or a
  designated reviewer before the extraction run).
- If positions are wrong: inspect `fte_review_queue` and `fte_claim_events` to locate
  the classification or math error; adjust confidence thresholds or prompt accordingly.

### Tier 3 — Review queue routing accuracy (SQL, committed run)

- Verify that claims the real documents show as ambiguous or underpaid are routed to
  `fte_review_queue` with the expected `reason` values.
- This is the primary quality signal: the system's uncertainty routing must match a
  human reviewer's assessment of which claims need review.

### What the implementation task must NOT do

- Hardcode expected observation values in the test suite (AI output varies).
- Mark real extraction complete until Tier 1, 2, and 3 validations pass for at least
  two independent approved test documents.
- Count AI-extraction validation checks in the 209/18 numeric baseline until they are
  stable and reproducible (non-determinism makes count-based assertions fragile for
  Tier 2/3).

A new validation SQL file (`tests/validate_real_extraction.sql`) is in scope for the
implementation task, not this spec.

---

## Implementation Task Outline (Future Task)

Once Keith approves the implementation task based on this spec, the implementation task
should cover these steps in order:

1. **De-identification step:** apply Amendment B rules to the approved evidence batch;
   Keith confirms the batch is ready.
2. **Evidence loading:** load de-identified evidence into a fresh disposable Supabase
   project using the approved fixture pattern.
3. **Secrets setup:** configure `.env` locally (gitignored); do not commit.
4. **Prompt authoring:** write draft prompt; Keith reviews and approves before any AI
   call is made.
5. **Script implementation:** build the provider-configurable external script matching
   the stable observation contract.
6. **First run — dry-run:** `--dry-run` to verify parsing logic before inserting.
7. **First run — live:** run against disposable Supabase with approved evidence.
8. **Tier 1 validation:** shape contract SQL checks (ROLLBACK-wrapped).
9. **Tier 2 validation:** `fte_reconcile_practice` + position comparison.
10. **Tier 3 validation:** review queue routing accuracy.
11. **Confidence calibration check:** inspect distribution of confidence scores against
    Phase 1 thresholds; determine whether threshold tuning is needed.
12. **Documentation updates:** `NEXT_STEPS.md`, `README.md`, `reconciler/README.md`,
    `tests/RUNBOOK.md`.

---

## Files Explicitly Forbidden for This PR (006H)

The following files must not be created, modified, or deleted as part of Task 006H:

- Any `*.sql` file (migrations, fixtures, reconciler, tests, functions)
- `financial-truth-engine/reconciler/fte_reconcile.sql`
- `financial-truth-engine/reconciler/fte_mock_extract_observations.sql`
- `financial-truth-engine/reconciler/fte_explain_claim.sql`
- `financial-truth-engine/migrations/` (any file)
- `financial-truth-engine/fixtures/` (any file)
- `financial-truth-engine/tests/` (any file)
- `financial-truth-engine/reconciler/README.md`
- `financial-truth-engine/README_SCHEMA.md`
- Any extraction script (`*.py`, `*.ts`, `*.js`, `*.sh`, etc.)
- Any config file with values (`.env`, `settings.json`, `.mcp.json`, etc.)
- `.env.example` (reserved for the implementation task if Keith approves)
- Any Edge Function file
- Any UI or frontend file
- Any legacy EOB extraction file
- Any file containing secrets, credentials, project URLs, DB URLs, or API keys
- `CLAUDE.md`

---

## Exit Criteria for This Spec-Only PR

This PR is ready to merge when:

1. `CODEX_TASK_006H.md` is present and complete.
2. `NEXT_STEPS.md` Immediate Next Action section reflects 006H as the current
   design checkpoint.
3. `README.md` status line is updated to note 006H as a spec checkpoint.
4. No SQL files, fixture files, test files, migrations, reconciler files, scripts,
   or config files have been created or modified.
5. No model ID is hardcoded anywhere in the changed files.
6. No secrets, credentials, project URLs, DB URLs, or PHI appear in any changed file.
7. Validation baseline is unchanged: **209 numeric checks across 18 suites**.
8. The spec does not claim real AI extraction is complete.
9. Keith has reviewed and approved the spec PR before the implementation task is
   written.

---

*This document is a design specification only. Real AI extraction is not implemented.
Do not begin implementation until Keith explicitly approves a separate implementation
task referencing this spec.*

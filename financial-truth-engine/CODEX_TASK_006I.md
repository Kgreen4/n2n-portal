# CODEX Task 006I — Real AI Observation Extractor Implementation Spec

**Status:** Spec-only — approved for design. Implementation requires separate approval.
**Created:** 2026-06-28
**Author:** Keith Green / N2N Analytics
**Depends on:** Tasks 001–006H (all complete and merged)
**Validation baseline:** 209 numeric checks across 18 suites (unchanged by this PR)

---

## Purpose

Define every implementation detail required to build the real AI observation extractor
described in CODEX_TASK_006H.md. This spec documents the script contract, provider
adapter interface, de-identification requirements, stop gates, and validation strategy
so that a future implementation task can be approved and executed without ambiguity.

This document is a specification only. The implementation — the extractor script,
provider adapters, `.env.example`, and new validation SQL — is delivered in a separate
future task after Keith's explicit approval of that implementation task.

---

## Scope of This Spec PR vs. the Future Implementation Task

**This spec PR (`docs/fte-006i-real-extractor-implementation-spec`) delivers:**

| File | Role |
|---|---|
| `financial-truth-engine/CODEX_TASK_006I.md` | This spec document (new) |
| `financial-truth-engine/NEXT_STEPS.md` | Immediate Next Action update only |
| `financial-truth-engine/README.md` | Status line update only |

**The future implementation task (a separate approved PR) may deliver:**

| File | Role |
|---|---|
| `financial-truth-engine/extractor/fte_extract_observations.py` | Extraction script |
| `financial-truth-engine/extractor/providers/__init__.py` | Adapter registry |
| `financial-truth-engine/extractor/providers/base.py` | Abstract adapter interface |
| `financial-truth-engine/extractor/providers/<provider-name>.py` | Concrete adapter (one per approved provider) |
| `financial-truth-engine/extractor/.env.example` | Placeholder env-var names only; no values |
| `financial-truth-engine/tests/validate_real_extraction.sql` | Tier 1 shape-contract validation |
| `financial-truth-engine/NEXT_STEPS.md` | Immediate Next Action update |
| `financial-truth-engine/README.md` | Status line and capabilities update |
| `financial-truth-engine/reconciler/README.md` | Extraction section update |
| `financial-truth-engine/tests/RUNBOOK.md` | Add validate_real_extraction entry |

No implementation files are created in this spec PR.

---

## Non-Goals for This Spec PR

- No extractor script, adapter files, or `extractor/` directory created
- No SQL files, migrations, fixtures, tests, or reconciler changes
- No `.env` or `.env.example`
- No AI calls (to any model, any vendor, any endpoint)
- No model or vendor selected or preferred
- No secrets, API keys, project URLs, or DB URLs
- No real PDFs, real patient data, real payer exports, real check numbers, real
  member IDs, real DOBs, patient names
- No Edge Functions, UI, or legacy EOB code changes
- No change to the 209/18 validation baseline
- No claim that real AI extraction is complete

---

## A. Already-Approved Defaults (from CODEX_TASK_006H.md)

The following nine decisions were approved by Keith on 2026-06-28 and carry forward
into the implementation task without re-approval:

| # | Decision | Approved Default |
|---|---|---|
| 1 | Deployment pattern | Option 3 — external script against disposable Supabase |
| 2 | Model / vendor | Provider-configurable; runtime-selected; no hardcoded model ID |
| 3 | Evidence input format | `raw_text` only for first implementation |
| 4 | Observation types in scope | `billed_amount`, `contractual_adjustment`, `payment` only |
| 5 | Confidence thresholds | Unchanged; keep current Phase 1 thresholds (0.70 / 0.85) |
| 6 | Approved test evidence | Keith must approve a named de-identified batch before any AI call |
| 7 | Prompt approval | Keith must approve final prompt text before any AI call |
| 8 | Error handling | Skip malformed/empty page and log warning; no placeholder observations |
| 9 | Invocation granularity | Per-page; one AI call per page evidence row |

**Amendment A (model-neutral):** No specific model name or version is selected or
preferred in this spec. Provider names may appear only as generic adapter examples
using placeholders (`<provider-name>`, `<provider-model-id>`). Final provider and
model selection is a runtime approval decision (gate B3).

**Amendment B (PHI/PII scope):** Claim numbers, check/EFT numbers, member IDs, DOBs,
patient names, SSNs, and payer exports are all potentially identifying. Retain claim
and check identifiers only with explicit per-batch Keith approval in writing; otherwise
replace with synthetic values or hashes. All other Amendment B fields always replaced
before any evidence is loaded or passed to an AI model.

---

## B. Still-Required Keith Approvals Before Any AI Call

Three gates remain open. The implementation script may be written, reviewed, and
pre-gate dry-run tested without these approvals. **No live AI call may be made
until all three are obtained.**

### B1 — Named de-identified evidence batch (Gate 1 / Decision 6)

Keith must name the specific document batch to be used and confirm in writing that
de-identification is complete. Before any batch may be loaded into a disposable
Supabase project or passed to an AI model:

- Patient names → synthetic value (e.g. `PATIENT-0001`)
- Dates of birth → synthetic value (e.g. `1970-01-01`)
- Member IDs → synthetic value or hash
- SSNs → always replaced; never retained in any form
- Claim numbers → replaced with `CLAIM-XXXX` or hash, unless Keith explicitly approves
  retention for this specific batch in writing
- Check / EFT numbers → same gate as claim numbers
- Payer names → may be retained; subject to per-batch approval
- Source URIs in `fte_evidence` → must use `private://fte/de-identified/...` scheme;
  not a real file path or URL

Keith's written approval of B1 must name the batch and confirm all of the above.

### B2 — Final prompt text (Decision 7)

Keith must read and approve the exact prompt text before the script sends it to any
AI model. The approved prompt must:

- Ask the AI to identify visible facts only: amounts, claim identifiers, payer name,
  service date, CPT/procedure code, check/EFT identifier
- Not ask the AI to assert financial truth, adjudication correctness, or denial
  determinations
- Require structured output (JSON) mapping to the observation contract columns
- Be submitted to Keith as a separate artifact before the first live run

The prompt text is not drafted in this spec.

### B3 — Runtime provider and model (Gate 5 / Decision 2)

Keith selects the provider and model at first run time by specifying:

```text
--provider <provider-name>
--model    <provider-model-id>
```

No provider or model ID is selected or preferred in this spec or in the implementation
task spec. Selection happens only after B1 and B2 are approved, based on empirical
accuracy on the approved test evidence at implementation time.

---

## C. Future Implementation Scope

The implementation task (a separate future PR) builds the script in the following
ordered steps. Steps 1–6 may proceed before B-gate approvals. Steps 7 onward require
all three B-gate approvals.

**Step 1 — `extractor/` directory and script skeleton**
Provider-configurable Python CLI (`fte_extract_observations.py`) matching the contract
in CODEX_TASK_006H.md § Script Contract. Reads `fte_evidence` rows, assembles prompt,
dispatches to provider adapter, parses response, inserts `fte_observations`. Accepts:

```text
--practice-id   <uuid>            required
--evidence-id   <uuid>            optional; single page; if omitted, all unextracted pages
--provider      <provider-name>   required; runtime-selected
--model         <provider-model-id> required; runtime-selected
--db-env        DATABASE_URL      required; names the env var holding the DB URL
--dry-run                         optional; evidence selection, DB read, prompt assembly,
                                  de-identification guardrails, env-var resolution,
                                  and no-insert behavior — does not call AI
```

**Step 2 — Provider adapter layer**
Abstract base adapter (`providers/base.py`) defining the interface: takes a prompt
string and model identifier, returns a structured response object. Concrete adapters
(`providers/<provider-name>.py`) implement the interface for each approved provider.
No concrete adapter is implemented for live use until Keith approves B3.

**Step 3 — Structured output parser**
Validates the AI response JSON against required fields (observation contract columns)
before any `INSERT`. On parse failure: log structured warning to stderr (`evidence_id`,
`page_number`, error detail), skip page, continue (Decision 8 / Amendment A).

**Step 4 — Idempotency guard**
Before inserting, check `(practice_id, evidence_id, claim_identifier, extractor)` for
existing observations. Skip page if 3 observations already exist for that combination.
Mirrors the mock extractor pattern.

**Step 5 — `.env.example`**
Committed to repo with placeholder variable names only. No values. Documents the
environment variables the script requires (e.g. `DATABASE_URL`, `<PROVIDER>_API_KEY`).
The real `.env` is gitignored and never committed.

**Step 6 — Pre-gate dry-run smoke test**
Run `--dry-run` against the Phase 3B synthetic fixture in a disposable Supabase
project. This confirms: evidence selection logic, DB read, prompt assembly, Amendment B
de-identification guardrails, env-var resolution, and no-insert behavior. It does not
call an AI model and does not confirm the parser on real AI output.

**→ STOP POINT after Step 6.** All three B-gate approvals (B1, B2, B3) required
before proceeding to Step 7.

**Step 7 — [Post-approval] Live first run**
Keith provides: approved batch (B1), approved prompt (B2), runtime provider and model
(B3). Script runs against the disposable Supabase project with the approved evidence.

**Step 8 — Tier 1 validation**
`tests/validate_real_extraction.sql` (new file): ROLLBACK-wrapped shape-contract checks
on the inserted observations. See § F Validation Plan.

**Step 9 — Tier 2 / Tier 3 validation**
`fte_reconcile_practice` run + position comparison (Tier 2) and review queue routing
accuracy (Tier 3). Requires Tier 1 passing for at least two independent approved
documents. May be deferred to a separate follow-on task (Task 006J) if prompt or
threshold tuning is needed after the first live run.

**Step 10 — Documentation updates**
`NEXT_STEPS.md`, `README.md`, `reconciler/README.md`, `tests/RUNBOOK.md`.

---

## D. Future Allowed Files (Implementation Task Only)

The following files may be created or modified in the future implementation task.
They are listed here for planning purposes only; they are not created in this spec PR.

```text
financial-truth-engine/extractor/fte_extract_observations.py
financial-truth-engine/extractor/providers/__init__.py
financial-truth-engine/extractor/providers/base.py
financial-truth-engine/extractor/providers/<provider-name>.py
financial-truth-engine/extractor/.env.example
financial-truth-engine/tests/validate_real_extraction.sql
financial-truth-engine/NEXT_STEPS.md
financial-truth-engine/README.md
financial-truth-engine/reconciler/README.md
financial-truth-engine/tests/RUNBOOK.md
```

No other files may be created or modified by the implementation task without separate
approval.

---

## E. Forbidden Files (This Spec PR and Implementation Task)

The following files must not be created, modified, or deleted in this spec PR.
They also remain forbidden in the implementation task unless explicitly re-approved:

```text
Any *.sql file not listed in § D (no migration, reconciler, or fixture SQL)
financial-truth-engine/reconciler/fte_reconcile.sql
financial-truth-engine/reconciler/fte_mock_extract_observations.sql
financial-truth-engine/reconciler/fte_explain_claim.sql
financial-truth-engine/migrations/ (any file)
financial-truth-engine/fixtures/ (any file)
financial-truth-engine/README_SCHEMA.md
CLAUDE.md
.env (real secrets file; gitignored and never committed)
Any Edge Function file
Any UI or frontend file
Any legacy EOB extraction file
Any config file with values (.env, settings.json, .mcp.json, etc.)
Any file containing secrets, credentials, project URLs, DB URLs, or API keys
Any file containing PHI, real patient data, real payer exports, real check numbers,
  real member IDs, real DOBs, patient names, or references to raw real PDFs
```

---

## F. Validation Plan

| Tier | Type | Trigger | File | Baseline impact |
|---|---|---|---|---|
| Pre-gate dry-run | Script `--dry-run` (no AI call) | After Step 6; before B-gate approvals | n/a (stdout/stderr only) | None |
| Tier 1 — shape contract | SQL, ROLLBACK-wrapped | After first live run (Step 8) | `tests/validate_real_extraction.sql` | Not added to 209/18 until stable |
| Tier 2 — position accuracy | SQL, committed run | After Tier 1 passes for ≥ 2 docs | Ad hoc SQL Editor run | Not added to 209/18 (non-deterministic) |
| Tier 3 — queue routing accuracy | SQL, committed run | After Tier 2 passes | Ad hoc SQL Editor run | Not added to 209/18 (non-deterministic) |

**Tier 1 check list** (`validate_real_extraction.sql`, ROLLBACK-wrapped):
- All inserted `fte_observations` rows satisfy the shape contract column requirements
- `confidence_score` is not null and is on `[0.00, 1.00]`
- `observation_type` is one of: `billed_amount`, `contractual_adjustment`, `payment`
- `metadata->>'mock'` is `'false'`
- `metadata->>'extractor'` is set (non-null, non-empty)
- `is_superseded = false` on all inserted rows
- `is_summary_row` is set (not null)
- The extractor has not written a `classification` value — `classification` is not a
  column set by the extractor; Phase 1 of `fte_reconcile_practice` owns it

**What the implementation task must NOT do:**
- Hardcode expected observation values in the test suite (AI output varies)
- Mark real extraction complete until Tier 1 passes for ≥ 2 independent approved docs
- Add AI-extraction validation checks to the 209/18 numeric baseline until stable
  and reproducible (non-determinism makes count-based assertions fragile for Tier 2/3)

---

## G. Exact Stop Points Before Live AI Calls

```text
STOP 1 — After implementation task scope approved by Keith:
  Branch created. No script written yet. No AI call.

STOP 2 — After Steps 1–5 complete (script + adapters + .env.example):
  Script source may be reviewed by Keith before dry-run.
  No AI call. No evidence batch loaded beyond Phase 3B synthetic fixture.

STOP 3 — After Step 6 complete (pre-gate dry-run passes):
  Dry-run confirms evidence selection, DB read, prompt assembly,
  de-identification guardrails, env-var resolution, and no-insert behavior.
  No AI call. No approved evidence batch loaded.
  Draft prompt submitted to Keith for B2 approval.

STOP 4 — Await all three B-gate approvals (B1 + B2 + B3):
  B1: Keith names and approves the specific de-identified batch in writing.
  B2: Keith approves the exact prompt text in writing.
  B3: Keith selects provider and model at runtime.
  No live AI call until all three confirmed.

STOP 5 — After first live run, before Tier 2/3:
  Tier 1 shape-contract validation must pass.
  If Tier 1 has failures: fix prompt or parser; re-submit to Keith before re-run.
  No Tier 2/3 run until Tier 1 passes for ≥ 2 independent approved documents.
```

---

## H. Exit Criteria for This Spec-Only PR

This PR is ready to merge when:

1. `CODEX_TASK_006I.md` is present and complete.
2. `NEXT_STEPS.md` Immediate Next Action section reflects 006I as the current
   design checkpoint.
3. `README.md` status line is updated to note 006I as a spec checkpoint.
4. No extractor script, adapter files, `.env.example`, SQL files, fixture files,
   test files, migrations, reconciler files, or config files have been created
   or modified.
5. No model ID is hardcoded and no provider is selected or preferred anywhere in
   the changed files.
6. No secrets, credentials, project URLs, DB URLs, or PHI appear in any changed file.
7. Validation baseline is unchanged: **209 numeric checks across 18 suites**.
8. The spec does not claim real AI extraction is complete.
9. Keith has reviewed and approved the spec PR before the implementation task is
   written.

---

*This document is a design specification only. Real AI extraction is not implemented.
Do not begin implementation until Keith explicitly approves a separate implementation
task referencing this spec.*

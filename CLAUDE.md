# N2N Analytics — Project Brief
# Repo-level project memory. Updated at end of each session.
# Last updated: 2026-06-05

---

## PROJECT IDENTITY

**N2N Portal** is a multi-tenant SaaS platform automating EOB extraction and
Revenue Cycle Management reporting for medical practices. Production system
with a live paying client.

**Owner:** Keith Green — CPA, MBA, Data Engineer, Founder N2N Analytics
**Stack:** Next.js 14 · Supabase (Postgres + Edge Functions + Storage) ·
           Google BigQuery · Vertex AI (Gemini 3.5 Flash — global endpoint) · Vercel · Stripe ·
           n8n (Render.com)
**Architecture reference:** `docs/N2N_Portal_Architecture.md` — read in full
                            before touching any code.

---

## TWO REPOS — NEVER MIX THEM

### N2N Portal (this repo — SaaS product)
Sells to any practice. Contains reusable RCM reporting module.
Repo root: `C:\Users\kgree\Dropbox\N2N\Business Offerings\EOB extraction\Claude Code`
GitLab: `gitlab.com/kgreen41-eob/cardio-metrics-saas`

### GCW Analytics (separate repo — client delivery)
Bespoke pipeline for Dr. Ravi / GreatCare & Wellness LLC only.
Repo root: `C:\Users\kgree\Dropbox\Marcomm\Dr. Ravi\GreatCare and Wellness\Analytics`

---

## GCW CLIENT CONTEXT

| Item | Value |
|---|---|
| Client | Dr. Ravi — owner, GreatCare & Wellness LLC (primary care) |
| Practice UUID | `df52d2fd-5e88-48de-9c45-13d0ae7847b0` |
| BQ project | `cardio-metrics-dev` |
| BQ dataset | `billing_audit_practice_test` |
| Supabase project | `jdmyjdvricpyrsfchakk` |
| ERA Drive folder | `1GOvEj8UJg4inP0WyoZQl3UoXC-u0rsFw` (RCM Reporting Data › ERA) |
| Current physician | Dr. Greatwood (established, Medicare-heavy) |
| Incoming physician | Dr. Sharma (credentialing in progress) |
| Biller | Third-party — uses Ethizo (PM/EMR) + Trizetto (clearinghouse) |

**Audit sensitivity:** Data request to biller framed as routine recordkeeping.
Real purpose is auditing biller performance. Never reference audit intent in
any output, filename, comment, or log message visible to the biller.

**Humana Gold:** 47 claims being written off — not credentialed with payer.
No investigation needed. Write-offs surface on Looker Page 7.

---

## INFRASTRUCTURE STATUS

### BigQuery — `billing_audit_practice_test`

| Object | Status | Notes |
|---|---|---|
| `eob_line_items` | ✅ Live | Schema includes `batch_id`, `payment_date`. Dedup via `view_eob_line_items` |
| `charge_report` | ✅ Live | Schema includes `batch_id`, `date_secondary_billed`, `secondary_payer` |
| `patient_account_ledger` | ✅ Exists | Payments, adjustments, write-offs |
| `patient_billing_uploads` | ✅ Exists | Trizetto statement upload log |
| `view_eob_line_items` | ✅ Live | Deduplicated view of `eob_line_items` — use this in all queries |
| `view_charge_report` | ✅ Live | Deduplicated view of `charge_report` — use this in all queries |
| `view_revenue_leakage` | ✅ Live | Black Hole Detector — queries dedup views. Tested ✅ |
| `view_collections_timeline` | ✅ Live | DOS-to-payment lag metrics. Looker Page 9. |

**DEDUP RULE:** Always query `view_eob_line_items` and `view_charge_report` (not raw tables).
Raw tables may contain streaming buffer duplicates within 90-minute window.

**⚠️ DO NOT DELETE any object in this table.** All 8 objects above are production-critical.
`eob_line_items` was accidentally deleted during BQ housekeeping on 2026-05-28 — required
emergency recreation. Other GCW-era tables from the prototyping phase (pre-portal) are safe
to delete; the 8 objects listed above are NOT.

### Supabase Edge Functions — project `jdmyjdvricpyrsfchakk`

| Function | Repo | Status |
|---|---|---|
| `ingest-era-data` | N2N Portal | ✅ Deployed + tested · pipeline_events schema fixed |
| `ingest-era-file` | N2N Portal | ✅ Deployed · deriveStatus bug fixed (80/20 Medicare) |
| `ingest-charges` | GCW Analytics | ✅ Deployed + tested · pipeline_events schema fixed |
| `ingest-ledger` | GCW Analytics | ✅ Deployed |
| `ingest-uploads` | GCW Analytics | ✅ Deployed |
| `get-practice-summary` | N2N Portal | ✅ Deployed + tested · all 8 KPIs verified |
| `eob-enqueue` | N2N Portal | ✅ Deployed · waitUntil fix — workers no longer abandoned on large docs |
| `eob-worker` | N2N Portal | ✅ Deployed 2026-06-05 · Gemini global endpoint · BCBS section-delimiter · retryable enum fix |
| `eob-sweeper` | N2N Portal | ✅ Deployed 2026-06-05 · retryable enum fix |
| `reprocess-document` | N2N Portal | ✅ Deployed · BQ delete + full re-extraction from original file path |
| `trigger-eob-parser` | N2N Portal | ✅ Deployed · COMPLETED guard + bypass_completed_guard flag |
| `scan-drive-folder` | N2N Portal | ✅ Deployed `--no-verify-jwt` · per-trigger 6s timeout for large batches |

All functions: `--no-verify-jwt` · env var is `GCP_SA_JSON` (not
`GCP_SERVICE_ACCOUNT_JSON` — that was a fixed bug, never revert it).

**eob-enqueue critical fix (2026-05-21):** Fire-and-forget worker fetches were
being abandoned by Deno when the backgroundTask IIFE resolved. Fix: register
each `workerFetch` with `EdgeRuntime.waitUntil(Promise.race([workerFetch, sleep(4000)]))`.
Without this, documents with 30+ pages would silently stall in "queued" status.

### n8n — Render.com (`cardio-metrics-n8n.onrender.com`)

| Workflow | Status |
|---|---|
| `gcw-drive-watcher.json` | ✅ Rewritten — valid format, ERA folder ID set. Needs credential ID + Supabase key configured in n8n UI |

**n8n remaining config (do in n8n UI):**
- Replace `REPLACE_WITH_CREDENTIAL_ID` in Drive Search node with actual GCP-Master-Account credential ID
- Set `SUPABASE_SERVICE_ROLE_KEY` as n8n environment variable

### GCW Financial Dashboard

| Item | Detail |
|---|---|
| File | `GCW_Financial_Dashboard.html` |
| Path | `..\Dr. Ravi\GreatCare and Wellness\Financials\` |
| Status | Complete, in use. Do not rebuild. |
| Gap | KPI data is hardcoded — `get-practice-summary` will replace it |

---

## AZHS CLIENT

| Item | Value |
|---|---|
| Client | Arizona Heart Specialists |
| Practice (active) | "AZHS-test" · UUID `45204f7a-3f24-4048-935c-686e0fcd89ad` |
| User linked | `a2ebab83-10d7-4f5b-97e2-a592dffb49dd` |
| Drive folder (MAY 2026) | `1IMWX86UIORNmbGlalLNmg5XcoZFX2_tI` |
| Seeded practice (unused) | "Arizona Heart Specialists" · UUID `aa000001-0000-4000-8000-000000000001` · no user linked |

**Important:** Two AZHS practices exist in the DB. "AZHS-test" is the live
practice (has user link, Settings page configured). The seeded "Arizona Heart
Specialists" (`aa000001`) has no user and cannot be logged into. Keith should
confirm whether to delete the seed record or link his user to it.

**Nightly workflow:** A manual processor marks finished files with "COMPLETED"
in the filename. `scan-drive-folder` skips these by default. Use
`include_completed: true` with `after_date` for catch-up runs against already-
completed files (normalizes out the COMPLETED prefix before dedup check).

**Settings page:** Drive folder scan button is live at Settings → Google Drive
section. Supports `after_date` filter and `include_completed` toggle.

---

## BILLER DATA — AWAITING 5 CSV EXPORTS

Request sent March 2026. Files not yet received.

| File | Source | Target Function | Target Table |
|---|---|---|---|
| ERA Primary | Trizetto | `ingest-era-file` | `eob_line_items` |
| ERA Secondary | Trizetto | `ingest-era-file` | `eob_line_items` |
| Patient Upload Log | Trizetto | `ingest-uploads` | `patient_billing_uploads` |
| Patient Ledger | Ethizo | `ingest-ledger` | `patient_account_ledger` |
| Charge Report | Ethizo | `ingest-charges` | `charge_report` |

**CRITICAL mapping note:** ERA check date / EFT settlement date →
`eob_line_items.payment_date`. Required for all timeline calculations.

When files arrive → drop in ERA Drive folder → n8n picks up automatically.

---

## PIPELINE TEST — VALIDATED 2026-03-28

Synthetic test data in `docs/test-fixtures/`. Run via:
```bash
node docs/test-fixtures/run-pipeline-test.js
```

| Check | Result |
|---|---|
| ERA paid=16, $1,891.83 | ✅ |
| ERA denied=3 (CO-4, CO-22, PR-1) | ✅ |
| Black holes=4 (CRITICAL/HIGH/PENDING) | ✅ |
| Dedup views collapse duplicates | ✅ |

---

## LOOKER STUDIO REPORT — 9 PAGES

Report: GCW Revenue Cycle Intelligence | BQ project: `cardio-metrics-dev`

| Page | Name | Source | Key Question |
|---|---|---|---|
| 1 | Executive Summary | All tables | Overall financial picture? |
| 2 | Insurance AR by Payer | `view_eob_line_items` | Which payers owe most? |
| 3 | Claim-Level Drill-Down | `view_eob_line_items` | Why paid / denied / short? |
| 4 | Secondary Payer Activity | `view_eob_line_items` | Medicare patients without secondary filed? |
| 5 | Denial Analysis | `view_eob_line_items` | Top denial codes by $ and trend? |
| 6 | Patient Billing & Collections | `patient_billing_uploads` + `patient_account_ledger` | Billed? Paid? |
| 7 | Write-Off & Adjustment Audit | `patient_account_ledger` | Write-offs authorized? |
| 8 | Charge Report / Black Hole | `view_revenue_leakage` | Created in Ethizo, never sent to Trizetto? |
| 9 | Collections Timeline | `view_collections_timeline` | Days DOS→payment by track? |

**Page 8 urgency flags:** CRITICAL >270 days · HIGH >90 days · PENDING ≤90 days
**Page 9 biller lag flags:** >7 days 🟡 · >14 days 🔴

---

## `get-practice-summary` EDGE FUNCTION SPEC

Read-only. Resilient to empty tables — return 0/null, never throw.
Deploy with `--no-verify-jwt`. Auth via `GCP_SA_JSON`.
Query `view_eob_line_items` and `view_charge_report` (not raw tables).

```json
{
  "total_ar_balance": 0.00,
  "claims_this_month": 0,
  "paid_this_month": 0.00,
  "denial_rate_pct": 0.0,
  "charge_capture_rate_pct": 0.0,
  "days_in_ar_avg": 0.0,
  "avg_submission_lag_days": 0.0,
  "avg_primary_collection_lag_days": 0.0,
  "avg_secondary_collection_lag_days": 0.0,
  "avg_patient_collection_lag_days": 0.0,
  "last_updated": "ISO timestamp"
}
```

`charge_capture_rate_pct` = (view_eob_line_items claims / view_charge_report charges) × 100
`denial_rate_pct` = denied claims / total claims × 100

---

## ACTIVE TASK QUEUE

- [x] Bootstrap: create all config files and commit to repo
- [x] Fix `n8n/gcw-drive-watcher.json` — rewritten, ERA folder ID set
- [x] Apply schema DDL: `payment_date` + `batch_id` on `eob_line_items`,
      `date_secondary_billed` + `secondary_payer` + `batch_id` on `charge_report`
- [x] Create `view_revenue_leakage` (Black Hole Detector) — tested ✅
- [x] Create `view_eob_line_items` + `view_charge_report` dedup views
- [x] Fix `deriveStatus` bug in `ingest-era-file` (80/20 Medicare splits)
- [x] Fix `pipeline_events` schema mismatch in `ingest-era-data` + `ingest-charges`
- [x] Full end-to-end pipeline test — all assertions pass
- [ ] Configure n8n workflow in UI (credential ID + SUPABASE_SERVICE_ROLE_KEY)
- [x] Create `view_collections_timeline` in BigQuery
- [x] Build and deploy `get-practice-summary` edge function
- [x] Wire `get-practice-summary` into `GCW_Financial_Dashboard.html` — live RCM panel on EOB tab
- [x] Create `reporting/looker/REPORT_STRUCTURE.md`
- [x] Fix `eob-enqueue` fire-and-forget workers abandoned on large docs (waitUntil fix)
- [x] Deploy `scan-drive-folder` edge function with per-trigger timeout
- [x] COMPLETED guard in `trigger-eob-parser` + `bypass_completed_guard` flag
- [x] `practice_settings` migration (codified table, RLS policies)
- [x] AZHS seed migration (`aa000001` practice + settings row)
- [x] Settings page "Scan & Process Folder" button — live
- [x] Reset 4 stuck documents to `failed` status for reprocessing via UI
- [x] Keith reprocessed 4 stuck documents via UI (BCBS OF AZ_EOB'S, BCBS OF AZ_MULTIPLE, MERCY CARE, BCBS OF MI)
- [x] Add Reports link to dashboard sidebar nav (layout.tsx) — deployed + committed
- [x] Fix Reports reconciliation gap Action column — live DB polling replaces ephemeral state;
      shows Submitting/Processing/Queued/Failed/Reprocess based on eob_documents.processing_status;
      persists across page reloads; auto-refreshes line items when processing completes (2026-06-02)
- [x] Merge open MRs: `fix/bcbs-section-delimited-double-count`, `fix/reports-processing-status-indicator`, `fix/bcbs-phantom-deposit-prompt`, `fix/retryable-enum-sweeper` — all merged into main ✅
- [x] Deploy eob-worker (Gemini global endpoint + BCBS section-delimiter fix) ✅
- [x] Deploy eob-sweeper (retryable enum fix) ✅
- [x] Fix OCR check-number sanitization — strip non-alnum before Levenshtein dedup (closes $159.95 phantom duplicate on BCBS EOB'S) · branch `fix/ocr-check-sanitize-non-alnum` merged + deployed ✅
- [x] Investigate BCBS OF AZ_EOB'S gap ($4,720.98) — root causes identified:
      1. OCR phantom duplicate `00025888?2` / `0002588872` ($159.95) — fixed by OCR sanitize above
      2. Thin-row dedup over-removing claims (~$2,676) — improves after section-delimiter fix
      3. True extraction gap (~$2,044) from 0-item pages — improves with new Gemini model + BCBS fix
- [ ] Verify reconciliation gaps closed after reprocessing (2026-06-05 reprocess in-flight) ⬅ CHECK REPORTS PAGE
      · BCBS OF AZ_EOB'S_MULTIPLE PAYMENTS (ccdbe216) — was $4,720.98 gap
      · BCBS OF AZ_MULTIPLE EOB'S_PAYMENTS (c4a94f14) — was $620.84 gap
      · MERCY CARE_EOB'S_MULTIPLE PAYMENTS (49d0d52b) — was $583.79 gap
- [ ] Resolve duplicate AZHS practice — confirm if `aa000001` (no user) should be
      deleted or if Keith's user should be linked to it ⬅ CLARIFY WITH KEITH
- [ ] Configure n8n eob-sweeper trigger (credential ID not set) — recovery for stuck jobs
- [x] Upgrade Supabase to Pro — HIPAA BAA enabled ✅
- [ ] Sign BAA with Supabase
- [ ] Sign BAA with Dr. Ravi (Keith's responsibility)
- [x] Rotate Supabase service role key ✅

---

## KNOWN ISSUES

- `cardio-metrics-dev` named dev but is production. Treat as production.
- n8n workflow not yet run end-to-end with real files — biller data pending.
- Supabase BAA still needs to be signed (Pro plan is active, BAA signature pending).
- n8n eob-sweeper trigger not yet configured (credential ID missing) — stuck
  queued jobs require manual UI reprocess until this is set up.
- AZHS duplicate practice: two rows in `practices` / `practice_settings` for AZHS.
  Active practice is "AZHS-test" (`45204f7a`). Seeded "Arizona Heart Specialists"
  (`aa000001`) has no user link — confirm with Keith before cleaning up.
- Reconciliation gaps — reprocessed 2026-06-05 with fixed eob-worker (Gemini global endpoint +
  BCBS section-delimiter + OCR check sanitize). Awaiting results on Reports page:
  · BCBS OF AZ_EOB'S_MULTIPLE PAYMENTS (ccdbe216) — prior gap $4,720.98; multi-causal
    (phantom duplicate, thin-row dedup over-removal, true extraction gap on 0-item pages)
  · BCBS OF AZ_MULTIPLE EOB'S_PAYMENTS (c4a94f14) — prior gap $620.84
  · MERCY CARE_EOB'S_MULTIPLE PAYMENTS (49d0d52b) — prior gap $583.79
- Gemini model: switched to `gemini-3.5-flash` via global endpoint
  (`aiplatform.googleapis.com/v1/projects/.../locations/global/...`) — `us-central1` was
  throwing "Publisher Model not found". All page workers now use global endpoint.

---

## CONVENTIONS

- TypeScript/Deno for all edge functions. No Python.
- BigQuery: always query dedup views (`view_eob_line_items`, `view_charge_report`)
- BigQuery dataset: always `billing_audit_practice_test`
- Never hardcode credentials — Supabase secrets only
- Never push directly to main — always branch + PR
- Branch naming: `feature/` · `fix/` · `schema/` · `chore/`
- Commit format: `feat:` · `fix:` · `schema:` · `chore:`

---

## GIT WORKFLOW — NEVER ASK KEITH TO RUN GIT COMMANDS

Handle all version control directly.

1. Create branch before any work: `git checkout -b feature/[description]`
2. Commit frequently with descriptive messages
3. Push and open MR at natural stopping points
4. End of every session → update CLAUDE.md → commit → push → MR

**Keith's end-of-session trigger:** *"Session complete."*
Run `.claude/commands/session-end.md` procedure immediately.

---

*N2N Analytics — Confidential. Internal use only.*

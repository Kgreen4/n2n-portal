# N2N Analytics — Claude Code Global Brief
# Loaded automatically at the start of every session.

---

## PROJECT IDENTITY

**N2N Portal** is a multi-tenant SaaS platform automating EOB extraction and
Revenue Cycle Management reporting for medical practices. Production system
with a live paying client.

**Owner:** Keith Green — CPA, MBA, Data Engineer, Founder N2N Analytics
**Stack:** Next.js 14 · Supabase (Postgres + Edge Functions + Storage) ·
           Google BigQuery · Vertex AI (Gemini 2.0 Flash) · Vercel · Stripe ·
           n8n (self-hosted GCP VM)
**Architecture reference:** `docs/N2N_Portal_Architecture.md` — read in full
                            before touching any code.

---

## TWO REPOS — NEVER MIX THEM

### N2N Portal (this repo — SaaS product)
Sells to any practice. Contains reusable RCM reporting module.
Repo root: `C:\Users\kgree\Dropbox\N2N\Business Offerings\EOB extraction\Claude Code`

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
| `eob_line_items` | ✅ Live — 2,076 rows | Has `payment_date`, `claim_type`, secondary fields |
| `charge_report` | ✅ Created | Has `date_secondary_billed`, `secondary_payer` |
| `patient_account_ledger` | ✅ Created | Payments, adjustments, write-offs |
| `patient_billing_uploads` | ✅ Created | Trizetto statement upload log |
| `view_revenue_leakage` | ✅ Created | Black Hole Detector |
| `view_collections_timeline` | ✅ Created | DOS-to-payment lag metrics |

### Supabase Edge Functions — project `jdmyjdvricpyrsfchakk`

| Function | Repo | Status |
|---|---|---|
| `ingest-era-data` | N2N Portal | ✅ Deployed |
| `ingest-charges` | GCW Analytics | ✅ Deployed |
| `ingest-ledger` | GCW Analytics | ✅ Deployed |
| `ingest-uploads` | GCW Analytics | ✅ Deployed |
| `get-practice-summary` | N2N Portal | 🔲 To build |

All functions: `--no-verify-jwt` · env var is `GCP_SA_JSON` (not
`GCP_SERVICE_ACCOUNT_JSON` — that was a fixed bug, never revert it).

### n8n

| Workflow | Status |
|---|---|
| `gcw-drive-watcher.json` | ❌ Fails to import — format error. Needs rewrite. |

### GCW Financial Dashboard

| Item | Detail |
|---|---|
| File | `GCW_Financial_Dashboard.html` |
| Path | `..\Dr. Ravi\GreatCare and Wellness\Financials\` |
| Status | Complete, in use. Do not rebuild. |
| Gap | KPI data is hardcoded — `get-practice-summary` will replace it |

---

## BILLER DATA — AWAITING 5 CSV EXPORTS

Request sent March 2026. Files not yet received.

| File | Source | Target Table |
|---|---|---|
| ERA Primary | Trizetto | `eob_line_items` |
| ERA Secondary | Trizetto | `eob_line_items` |
| Patient Upload Log | Trizetto | `patient_billing_uploads` |
| Patient Ledger | Ethizo | `patient_account_ledger` |
| Charge Report | Ethizo | `charge_report` |

**CRITICAL mapping note:** ERA check date / EFT settlement date field from
the 835 file → `eob_line_items.payment_date`. Required for all timeline
calculations. Exists in every ERA file. Never skip it.

---

## LOOKER STUDIO REPORT — 9 PAGES

Report: GCW Revenue Cycle Intelligence | BQ project: `cardio-metrics-dev`

| Page | Name | Source | Key Question |
|---|---|---|---|
| 1 | Executive Summary | All tables | Overall financial picture? |
| 2 | Insurance AR by Payer | `eob_line_items` | Which payers owe most? |
| 3 | Claim-Level Drill-Down | `eob_line_items` | Why paid / denied / short? |
| 4 | Secondary Payer Activity | `eob_line_items` (Secondary) | Medicare patients without secondary filed? |
| 5 | Denial Analysis | `eob_line_items` | Top denial codes by $ and trend? |
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

`charge_capture_rate_pct` = (eob_line_items claims / charge_report charges) × 100
`denial_rate_pct` = denied claims / total claims × 100 where
                    claim_status IN ('denied','rejected')

---

## REPORTING MODULE STRUCTURE (Sellable Add-On)

RCM reporting is a purchasable add-on in N2N Portal.
Reusable assets live here. Client-specific pipelines stay in GCW repo.

```
/reporting
├── views/
│   ├── view_revenue_leakage.sql
│   ├── view_collections_timeline.sql
│   └── view_practice_summary.sql
├── looker/
│   ├── REPORT_STRUCTURE.md
│   └── templates/
└── edge-functions/
    └── get-practice-summary/
```

---

## ACTIVE TASK QUEUE

- [x] Bootstrap: create all config files and commit to repo (first session only)
- [ ] Fix `n8n/gcw-drive-watcher.json` format error — rewrite for valid n8n import
- [ ] Apply schema DDL: `payment_date` on `eob_line_items`,
      `date_secondary_billed` + `secondary_payer` on `charge_report`
- [ ] Build and deploy `get-practice-summary` edge function
- [ ] Create `reporting/looker/REPORT_STRUCTURE.md`
- [ ] Upgrade Supabase to Pro — required for HIPAA BAA
- [ ] Sign BAA with Supabase
- [ ] Sign BAA with Dr. Ravi (Keith's responsibility — not a code task)
- [ ] Verify GitHub CLI (`gh`) is authenticated for PR creation

---

## KNOWN ISSUES

- Supabase on FREE plan — PHI is live. Upgrade to Pro for HIPAA BAA. Urgent.
- `cardio-metrics-dev` named dev but is production. Treat as production.
- n8n workflow never run end-to-end — biller files not yet received.
- `gh` CLI auth status unknown — verify in first session.

---

## CONVENTIONS

- TypeScript/Deno for all edge functions. No Python.
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
3. Push and open PR at natural stopping points
4. End of every session → update CLAUDE.md → commit → push → PR

**Keith's end-of-session trigger:** *"Session complete."*
Run `.claude/commands/session-end.md` procedure immediately.

---

## SALES PLAYBOOK SUMMARY
## (Full version in `docs/SALES_PLAYBOOK.md`)

### Core Positioning
"I am an independent visibility and accountability layer between the biller
and the owner. I don't replace anyone. I don't require changing systems.
I tell the owner what's actually happening with their money."

The CFO comparison: healthcare CFO = $15–25K/month. N2N delivers the same
outcome at $2,500–$5,000. Same result. Fraction of the cost.

### Five Discovery Questions (if they can't answer → you've proven your value)
1. What is your clean claim rate? (benchmark: 95%+)
2. What is your average days in AR? (benchmark: <40 days)
3. What is your denial rate and top 3 denial codes? (benchmark: <5%)
4. How do you verify you're paid correctly against contracted rates?
5. What happened to your [specific payer] claims last quarter?

### Three Budget-Opening Pain Points
- "I don't trust my biller" → independent verification layer
- "I'm working harder but collecting the same" → charge capture rate
- "I can't afford [new physician / expansion]" → financial forecast model

### The RCM Iceberg
Most revenue loss is created at the front desk, not the billing department.
By the time a claim is denied, the damage was done weeks earlier at
registration, insurance verification, or documentation. The biller gets
blamed for problems they didn't create.

### Key Insight — Step 9 (Always Lead With This in Marketing)
Most RCM vendors give owners data. N2N gives owners answers.
Practices don't have an AI problem. They have a visibility problem.
The reporting layer — the dashboard, the drill-down, the timeline —
IS the product. Not the extraction. The intelligence.

---

*N2N Analytics — Confidential. Internal use only.*

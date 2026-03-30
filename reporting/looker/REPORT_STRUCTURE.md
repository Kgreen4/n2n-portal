# GCW Revenue Cycle Intelligence — Looker Studio Report Structure
# N2N Analytics — Internal Reference
# Last updated: 2026-03-29

---

## Overview

**Report name:** GCW Revenue Cycle Intelligence
**BQ project:** `cardio-metrics-dev`
**BQ dataset:** `billing_audit_practice_test`
**Practice UUID filter:** `df52d2fd-5e88-48de-9c45-13d0ae7847b0`
**Pages:** 9
**Audience:** Practice owner (Dr. Ravi). Not shared with biller.

This document is the rebuild guide. If the Looker report is ever deleted or needs
to be replicated for a new client, follow these instructions exactly.

---

## Data Sources

Every page pulls from one or more of these four BQ data sources.
**Always use the views, never the raw tables.**

| Looker Data Source Name | BQ Object | Purpose |
|---|---|---|
| `DS_EOB` | `view_eob_line_items` | ERA payments, denials, claim status |
| `DS_CHARGES` | `view_charge_report` | Ethizo charges, submission dates |
| `DS_LEDGER` | `patient_account_ledger` | Patient payments, write-offs, adjustments |
| `DS_UPLOADS` | `patient_billing_uploads` | Trizetto patient statement log |
| `DS_LEAKAGE` | `view_revenue_leakage` | Black hole detector (charges never sent) |
| `DS_TIMELINE` | `view_collections_timeline` | DOS-to-payment lag metrics |

### Connecting to BigQuery in Looker Studio

1. New data source → BigQuery → My Projects → `cardio-metrics-dev`
2. Dataset: `billing_audit_practice_test`
3. Select the view/table listed above
4. **Always add a calculated field or filter for `practice_id = 'df52d2fd-5e88-48de-9c45-13d0ae7847b0'`**
   to scope every data source to GCW only (multi-tenant safety).

---

## Global Report Controls

Add these controls to the **report header** (visible on all pages):

| Control | Field | Data Source |
|---|---|---|
| Date range control | `date_of_service` | DS_EOB |
| Payer filter | `payer_name` | DS_EOB |
| Claim status filter | `claim_status` | DS_EOB |
| Provider NPI filter | `npi` | DS_EOB |

---

## Page-by-Page Specification

---

### Page 1 — Executive Summary

**Question answered:** What is the overall financial picture right now?
**Data sources:** DS_EOB, DS_CHARGES, DS_LEAKAGE
**Refresh cadence:** Daily (when new ERA files arrive)

#### Scorecard KPIs (top row, 6 tiles)

| Tile Label | Metric | Formula / Field | Color Rule |
|---|---|---|---|
| Total AR Balance | `SUM(allowed_amount) - SUM(paid_amount)` | DS_EOB, claim_status ≠ 'paid' | — |
| Claims This Month | `COUNT(claim_number)` | DS_EOB, date_of_service = current month | — |
| Denial Rate | `COUNT(denied) / COUNT(all)` | DS_EOB, claim_status = 'denied' | 🟢 <5% · 🟠 <10% · 🔴 ≥10% |
| Charge Capture Rate | `COUNT(DS_EOB claims) / COUNT(DS_CHARGES charges)` | Cross-source | 🟢 >95% · 🟠 >85% · 🔴 lower |
| Avg Days in AR | `AVG(CURRENT_DATE - date_of_service)` | DS_EOB, unpaid only | 🟢 <40 · 🟠 <60 · 🔴 ≥60 |
| Black Holes (open) | `COUNT(*)` | DS_LEAKAGE | 🔴 any CRITICAL · 🟠 any HIGH |

#### Summary Charts (middle row, 2 columns)

**Left — Payments by Month (bar chart)**
- DS_EOB
- Dimension: `DATE_TRUNC(payment_date, MONTH)`
- Metric: `SUM(paid_amount)`
- Breakdown: `claim_status` (stacked: paid / denied / partial)

**Right — AR Aging Buckets (donut chart)**
- DS_EOB (unpaid claims only)
- Calculated field:
  ```
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 30  THEN '0–30 days'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 60  THEN '31–60 days'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 90  THEN '61–90 days'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 180 THEN '91–180 days'
    ELSE '180+ days'
  END
  ```
- Metric: `SUM(allowed_amount - paid_amount)`

#### Alert Table (bottom)
- DS_LEAKAGE, sorted by `urgency_flag` DESC, `days_outstanding` DESC
- Columns: `patient_name`, `cpt_code`, `date_of_service`, `billed_amount`, `payer`, `days_outstanding`, `urgency_flag`
- Row color rules: CRITICAL = red background, HIGH = orange, PENDING = yellow

---

### Page 2 — Insurance AR by Payer

**Question answered:** Which payers owe the most and why?
**Data source:** DS_EOB

#### Payer Summary Table

Columns: `payer_name` · `COUNT(claim_number)` · `SUM(billed_amount)` · `SUM(allowed_amount)` ·
`SUM(paid_amount)` · `SUM(allowed_amount) - SUM(paid_amount)` (Open AR) ·
`SUM(paid_amount) / SUM(allowed_amount)` (Collection %) · `AVG(DATE_DIFF(payment_date, date_of_service, DAY))` (Avg Days)

Sort: Open AR descending.

#### AR by Payer — Bar Chart
- Dimension: `payer_name`
- Metrics: `SUM(billed_amount)` (bar) + `SUM(paid_amount)` (bar, overlaid)
- Color: billed = navy, paid = teal

#### Open AR Trend by Payer — Line Chart
- Dimension: `DATE_TRUNC(date_of_service, MONTH)`
- Breakdown: `payer_name`
- Metric: `SUM(allowed_amount) - SUM(paid_amount)`

#### Key filters on this page
- Claim status filter (default: exclude 'paid' to show open AR only)
- Payer multi-select

---

### Page 3 — Claim-Level Drill-Down

**Question answered:** Why was each claim paid / denied / short-paid?
**Data source:** DS_EOB

#### Full Claim Table (paginated, 25 rows)

Columns: `claim_number` · `patient_name` · `payer_name` · `date_of_service` · `cpt_code` ·
`billed_amount` · `allowed_amount` · `paid_amount` · `patient_responsibility` ·
`claim_status` · `remark_code` · `adjustment_reason` · `check_number` · `payment_date`

Sort: `date_of_service` DESC (most recent first)

Conditional formatting:
- `claim_status = 'denied'` → row background #fee2e2 (light red)
- `claim_status = 'partial'` → row background #fff7ed (light orange)
- `claim_status = 'paid'` → row background #f0fdf4 (light green)

#### Filters on this page (all interactive)
- Date range: `date_of_service`
- Payer: `payer_name`
- Status: `claim_status`
- Remark code: `remark_code`
- CPT code: `cpt_code`
- Patient search: `patient_name`

---

### Page 4 — Secondary Payer Activity

**Question answered:** Which Medicare patients have no secondary billed — and how much is at risk?
**Data sources:** DS_EOB + DS_CHARGES (blended)

#### Secondary Coverage Gap Table

Logic: ERA claims where `payer_name CONTAINS 'Medicare'` AND `claim_status = 'paid'`
LEFT JOIN to charge_report on `patient_name + cpt_code + date_of_service`
WHERE `date_secondary_billed IS NULL`

Columns: `patient_name` · `date_of_service` · `cpt_code` · `paid_amount` · `patient_responsibility` ·
`date_secondary_billed` (NULL = gap) · `secondary_payer`

At-risk amount = `SUM(patient_responsibility)` where secondary not yet billed

#### Secondary Pipeline Scorecard (4 tiles)
- Medicare claims paid (this period)
- Of those: secondary billed count
- Of those: secondary billed %
- Estimated patient responsibility not yet sent to secondary

#### Secondary Activity Timeline — Bar Chart
- Dimension: `DATE_TRUNC(date_secondary_billed, MONTH)`
- Metric: `COUNT(*)`
- Shows secondary billing activity over time

---

### Page 5 — Denial Analysis

**Question answered:** What are the top denial reasons by dollar amount and trend?
**Data source:** DS_EOB, `claim_status = 'denied'`

#### Denial Summary Scorecard (3 tiles)
- Total denied claims (count)
- Total denied amount (`SUM(billed_amount)` on denied claims)
- Denial rate % (denied / total)

#### Top Denial Codes — Table + Bar

Table columns: `remark_code` · `adjustment_reason` · `COUNT(*)` · `SUM(billed_amount)` ·
`AVG(billed_amount)` · `payer_name` (most common)

Bar chart: `remark_code` on X-axis, `SUM(billed_amount)` on Y-axis, sorted descending.

#### Denial Trend — Line Chart
- Dimension: `DATE_TRUNC(date_of_service, MONTH)`
- Breakdown: `remark_code` (top 5 codes only)
- Metric: `COUNT(claim_number)` where `claim_status = 'denied'`

#### Denial by Payer Heat Table
- Rows: `payer_name`
- Columns: `remark_code` (top 5)
- Metric: `COUNT(*)`
- Heatmap coloring: higher count = darker red

#### Actionable Denial Detail (filterable table)
Same as Page 3 claim table, pre-filtered to `claim_status = 'denied'`.
Add `check_date` column to show when the denial was received.

---

### Page 6 — Patient Billing & Collections

**Question answered:** Which patients have been billed and which have paid?
**Data sources:** DS_UPLOADS + DS_LEDGER (blended on `patient_account`)

#### Patient Billing Pipeline Scorecard (4 tiles)
- Statements sent (DS_UPLOADS COUNT)
- Patient payments received (DS_LEDGER, transaction_type = 'payment')
- Outstanding patient balance (DS_LEDGER: charges - payments - adjustments)
- Avg days to patient payment

#### Patient Statement Log — Table
DS_UPLOADS columns: `patient_account` · `patient_name` · `statement_date` ·
`statement_amount` · `statement_type` · `upload_batch_id`

#### Patient Payment History — Table
DS_LEDGER columns: `patient_account` · `patient_name` · `transaction_date` ·
`transaction_type` · `amount` · `payer` · `running_balance`

Filter: `transaction_type IN ('payment', 'patient_payment')`

#### Monthly Patient Collections — Bar Chart
- Dimension: `DATE_TRUNC(transaction_date, MONTH)`
- Metric: `SUM(amount)`
- DS_LEDGER, patient payments only

---

### Page 7 — Write-Off & Adjustment Audit

**Question answered:** Are write-offs authorized and within expected ranges?
**Data source:** DS_LEDGER

> **Note:** Humana Gold write-offs ($11,951 across 47 claims) surface here.
> These are credentialing-gap write-offs — not billing errors. Flag in report
> header but do not investigate further.

#### Write-Off Summary Scorecard (3 tiles)
- Total write-offs YTD (`SUM(amount)` where `transaction_type = 'write_off'`)
- Contractual adjustments (`transaction_type = 'contractual_adjustment'`): expected, CO-45
- Discretionary write-offs (`transaction_type = 'write_off'`): require authorization

#### Write-Off Detail Table (all write-offs, sortable)
Columns: `patient_account` · `patient_name` · `transaction_date` · `transaction_type` ·
`amount` · `payer` · `reason_code` · `authorized_by`

Conditional formatting:
- Write-offs > $500 → red background (flag for review)
- Contractual adjustments → grey (expected)

#### Write-Off by Payer — Bar Chart
- Dimension: `payer`
- Metric: `SUM(amount)` where `transaction_type = 'write_off'`
- Sorted descending

#### Adjustment Trend — Line Chart
- Dimension: `DATE_TRUNC(transaction_date, MONTH)`
- Breakdown: `transaction_type`
- Metric: `SUM(amount)`

---

### Page 8 — Charge Report / Black Hole Detector

**Question answered:** Which charges were created in Ethizo but never sent to Trizetto?
**Data source:** DS_LEAKAGE (`view_revenue_leakage`)

> This is the highest-value page for biller accountability.
> A charge in this view = revenue that left the building and never came back.

#### Black Hole Scorecard (4 tiles)
- CRITICAL black holes (>270 days outstanding): count + `SUM(billed_amount)`
- HIGH black holes (91–270 days): count + `SUM(billed_amount)`
- PENDING black holes (≤90 days): count + `SUM(billed_amount)`
- Total at-risk dollars: `SUM(billed_amount)` across all

#### Black Hole Table — Full Detail

Columns: `patient_name` · `date_of_service` · `cpt_code` · `billed_amount` ·
`payer` · `billing_status` · `days_outstanding` · `urgency_flag` · `source_filename`

Sort: `urgency_flag` ASC (CRITICAL first), then `days_outstanding` DESC

Conditional formatting:
- `urgency_flag = 'CRITICAL'` → row background #fee2e2, bold
- `urgency_flag = 'HIGH'` → row background #fff7ed
- `urgency_flag = 'PENDING'` → row background #fefce8

#### Black Holes by Payer — Bar Chart
- Dimension: `payer`
- Metric: `SUM(billed_amount)`
- Color: CRITICAL = red, HIGH = orange, PENDING = yellow (use `urgency_flag` breakdown)

#### Urgency Flag Donut
- Dimension: `urgency_flag`
- Metric: `COUNT(*)`
- Colors: CRITICAL = #dc2626, HIGH = #ea580c, PENDING = #b45309

#### Urgency Flag Logic (reference)
| Flag | Condition | Action Required |
|---|---|---|
| CRITICAL | >270 days since DOS | Immediate: likely uncollectable, investigate or write off |
| HIGH | 91–270 days since DOS | This week: refile or appeal |
| PENDING | ≤90 days since DOS | Monitor: may still be in adjudication |

---

### Page 9 — Collections Timeline

**Question answered:** How many days does each step of the revenue cycle take?
**Data source:** DS_TIMELINE (`view_collections_timeline`)

#### Timeline KPI Scorecard (5 tiles)
- Avg DOS → Submission (`AVG(submission_lag_days)`, OK ≤7d)
- Avg DOS → Primary Payment (`AVG(primary_collection_lag_days)`)
- Avg Primary Payment → Secondary Billed (`AVG(secondary_billing_lag_days)`)
- Claims with DELAYED submission flag (`COUNT(*)` where `submission_lag_flag = 'DELAYED'`)
- Claims with SLOW submission flag (`COUNT(*)` where `submission_lag_flag = 'SLOW'`)

#### Submission Lag Distribution — Bar Chart
- Dimension: `submission_lag_flag` (OK / SLOW / DELAYED / NOT_SUBMITTED)
- Metric: `COUNT(*)`
- Colors: OK = teal, SLOW = orange, DELAYED = red, NOT_SUBMITTED = grey

#### Collection Lag by Payer — Table
Columns: `payer_name` · `COUNT(claim_number)` · `AVG(submission_lag_days)` ·
`AVG(primary_collection_lag_days)` · `AVG(secondary_billing_lag_days)` ·
`COUNT(*) where submission_lag_flag = 'DELAYED'`

Sort: `AVG(primary_collection_lag_days)` DESC (slowest payers first)

#### Full Timeline Waterfall — Line Chart (trend over time)
- Dimension: `DATE_TRUNC(date_of_service, MONTH)`
- Metrics (3 lines):
  - `AVG(submission_lag_days)` — DOS → clearinghouse
  - `AVG(primary_collection_lag_days)` — DOS → primary payment
  - `AVG(secondary_billing_lag_days)` — primary → secondary
- Reference lines: 7-day threshold (orange), 14-day threshold (red)

#### Submission Lag Detail Table (biller accountability)
Columns: `claim_number` · `patient_name` · `payer_name` · `date_of_service` ·
`date_submitted` · `submission_lag_days` · `submission_lag_flag` · `primary_payment_date` ·
`primary_collection_lag_days`

Pre-filter: `submission_lag_flag IN ('SLOW', 'DELAYED')` — show only problematic rows.
Sort: `submission_lag_days` DESC.

#### Submission Lag Flag Reference
| Flag | Condition | Meaning |
|---|---|---|
| `OK` | ≤7 days | Within target |
| `SLOW` 🟡 | 8–14 days | Biller took longer than expected |
| `DELAYED` 🔴 | >14 days | Biller significantly late — review |
| `NOT_SUBMITTED` | NULL `date_submitted` | Charge was never sent to clearinghouse |

---

## Rebuilding This Report for a New Client

1. **Create BQ tables** using DDL in `docs/sql/`:
   - `create_eob_line_items.sql`
   - `create_charge_report.sql`
   - `alter_add_batch_id.sql`
   - `create_dedup_views.sql`
   - `create_view_collections_timeline.sql`

2. **Deploy edge functions** from `supabase/functions/`:
   - `ingest-era-file` (CSV → `eob_line_items`)
   - `ingest-charges` (Ethizo → `charge_report`)
   - `ingest-ledger` (Ethizo → `patient_account_ledger`)
   - `ingest-uploads` (Trizetto → `patient_billing_uploads`)
   - `get-practice-summary` (KPI API for dashboard)

3. **Configure n8n** workflow `gcw-drive-watcher.json`:
   - Set ERA Drive folder ID
   - Set Google credential
   - Set `SUPABASE_SERVICE_ROLE_KEY` env var

4. **Create Looker data sources** (one per BQ view/table listed above)
   - Add `practice_id` filter on every data source
   - Set `date_of_service` as the default date field

5. **Build pages** in order: 1 → 8 → 5 → 9 → 2 → 3 → 4 → 6 → 7
   (Executive Summary and Black Hole Detector first — highest value)

6. **Set report-level date range** default: Last 12 months

7. **Share with client:** View-only access, no data source editing.

---

## Field Reference — `view_eob_line_items`

| Field | Type | Notes |
|---|---|---|
| `payer_id` | STRING | Clearinghouse payer code |
| `claim_number` | STRING | Unique per claim |
| `cpt_code` | STRING | Procedure code |
| `date_of_service` | DATE | DOS — primary timeline anchor |
| `billed_amount` | FLOAT64 | Charged at practice fee schedule |
| `allowed_amount` | FLOAT64 | Contracted rate |
| `paid_amount` | FLOAT64 | What payer actually paid |
| `adjustment_amount` | FLOAT64 | Contractual write-down |
| `patient_responsibility` | FLOAT64 | Copay + deductible |
| `payer_name` | STRING | Human-readable payer name |
| `patient_name` | STRING | Used as join key to charge_report |
| `patient_account` | STRING | Practice account number |
| `claim_status` | STRING | `paid` / `denied` / `partial` |
| `remark_code` | STRING | CO-45, CO-4, PR-1, CO-22, etc. |
| `adjustment_reason` | STRING | Human-readable denial/adjustment reason |
| `check_number` | STRING | EFT/check reference |
| `check_date` | DATE | Date on the check/EFT |
| `payment_date` | DATE | ERA EFT settlement date — **primary timeline field** |
| `practice_id` | STRING | Multi-tenant scope key |
| `source_type` | STRING | `trizetto_era` or `pdf_parser` |
| `ingested_at` | TIMESTAMP | Pipeline timestamp |

## Field Reference — `view_charge_report`

| Field | Type | Notes |
|---|---|---|
| `id` | STRING | SHA-256 dedup key |
| `practice_id` | STRING | Multi-tenant scope key |
| `patient_name` | STRING | Join key to eob_line_items |
| `account_number` | STRING | Practice account number |
| `date_of_service` | DATE | DOS |
| `cpt_code` | STRING | Procedure code |
| `billed_amount` | FLOAT64 | Amount entered in Ethizo |
| `billing_status` | STRING | Ethizo status field |
| `date_submitted` | DATE | Date sent to clearinghouse |
| `date_secondary_billed` | DATE | Date secondary claim filed |
| `secondary_payer` | STRING | Name of secondary payer |
| `payer` | STRING | Primary payer name |
| `provider_name` | STRING | Rendering provider |
| `npi` | STRING | Provider NPI |
| `diagnosis_codes` | STRING | ICD-10 codes |

## Field Reference — `view_revenue_leakage`

| Field | Type | Notes |
|---|---|---|
| `patient_name` | STRING | |
| `date_of_service` | DATE | |
| `cpt_code` | STRING | |
| `billed_amount` | FLOAT64 | At-risk dollars |
| `payer` | STRING | Intended payer (from charge_report) |
| `billing_status` | STRING | Ethizo status at time of export |
| `days_outstanding` | INT64 | Calculated from CURRENT_DATE |
| `urgency_flag` | STRING | `CRITICAL` / `HIGH` / `PENDING` |

## Field Reference — `view_collections_timeline`

| Field | Type | Notes |
|---|---|---|
| `submission_lag_days` | INT64 | DOS → date_submitted |
| `submission_lag_flag` | STRING | `OK` / `SLOW` / `DELAYED` / `NOT_SUBMITTED` |
| `primary_payment_date` | DATE | COALESCE(payment_date, check_date) |
| `primary_collection_lag_days` | INT64 | DOS → primary_payment_date |
| `date_secondary_billed` | DATE | From charge_report |
| `secondary_billing_lag_days` | INT64 | primary_payment_date → date_secondary_billed |
| `collection_track` | STRING | `primary_paid` / `secondary_billed` / `denied` / `pending` |

---

## Common Calculated Fields (add to Looker data source)

```
-- Open AR (use in DS_EOB)
Open AR = SUM(allowed_amount) - SUM(paid_amount)

-- Collection rate on allowed
Collection Rate = SUM(paid_amount) / SUM(allowed_amount)

-- Collection rate on billed
Billed Collection Rate = SUM(paid_amount) / SUM(billed_amount)

-- AR aging bucket (add as calculated dimension to DS_EOB)
AR Aging Bucket =
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 30  THEN '0–30'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 60  THEN '31–60'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 90  THEN '61–90'
    WHEN DATE_DIFF(CURRENT_DATE(), date_of_service, DAY) <= 180 THEN '91–180'
    ELSE '180+'
  END

-- Denial flag (add as calculated dimension to DS_EOB)
Is Denied = CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END
```

---

## Report Theme / Style Guide

Match the GCW Financial Dashboard visual language:

| Element | Value |
|---|---|
| Background | `#f9fafb` |
| Header / Navy | `#1a2744` |
| Accent / Teal | `#0d9488` |
| Positive | `#16a34a` |
| Warning | `#ea580c` |
| Negative | `#dc2626` |
| Font | Google Sans or Roboto |
| Chart style | Clean, no gridlines, minimal legend |

---

*N2N Analytics — Confidential. Internal use only.*

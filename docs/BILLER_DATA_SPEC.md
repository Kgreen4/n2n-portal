# GCW — Biller Data Specification
## ERA & Reporting File Exports from Trizetto / Ethizo

> **Audience:** Third-party biller (GreatCare & Wellness)
> **Purpose:** Defines the file formats, column names, and delivery method
>              required for automated revenue cycle reporting.
> **Contact:** Keith Green — N2N Analytics

---

## How to Deliver Files

1. Export the files listed below from Trizetto and Ethizo
2. Drop them into the shared Google Drive folder: **RCM Reporting Data**
   - You have been shared on this folder — it appears in your "Shared with me"
3. Files are picked up automatically within 5 minutes
4. No email or notification needed — the system detects new files on its own

**Frequency:** Weekly (preferred) or after each ERA batch settlement

---

## File 1 — Insurance Payments Report (ERA / Remittance)

**Source:** Trizetto portal → Reports → Insurance Payments (or ERA summary)
**Format:** `.xlsx` or `.csv` — either is accepted
**Suggested filename:** `insurance_payments_YYYY-MM-DD.xlsx`

### Required Columns

| Column Name | Description | Example |
|-------------|-------------|---------|
| `Payer ID` | Payer/insurance identifier | `00431`, `BCBS001` |
| `Payer Name` | Insurance company name | `Blue Cross Blue Shield` |
| `Claim Number` | Claim or Patient Control Number | `CLM-2026-00412` |
| `Patient Name` | Patient full name | `Smith, John` |
| `Member ID` | Insurance member/subscriber ID | `XYZ123456789` |
| `Date of Service` | Service date (any standard format) | `2026-03-15` or `3/15/2026` |
| `CPT Code` | Procedure code | `99213` |
| `Billed Amount` | Amount submitted to payer | `$185.00` |
| `Allowed Amount` | Payer's contracted allowed amount | `$142.50` |
| `Paid Amount` | Amount actually paid by payer | `$114.00` |
| `Patient Responsibility` | Copay + deductible + coinsurance | `$28.50` |
| `Check Number` | EFT/check reference number | `EFT20260318001` |
| `Check Date` | EFT settlement / check issue date | `2026-03-18` |
| `Adjustment Reason Code` | CARC code (CO-45, PR-1, etc.) | `CO-45` |

### Optional but Helpful Columns

| Column Name | Description |
|-------------|-------------|
| `Remark Description` | Human-readable denial or adjustment reason |
| `NPI` | Rendering provider NPI (if available) |
| `Service Code` | Same as CPT Code — include if available |

### Notes
- Dollar amounts can include `$` signs and commas — the system strips them automatically
- Dates can be in any standard format (MM/DD/YYYY, YYYY-MM-DD, M/D/YY)
- Column names do not need to match exactly — common variations are handled automatically
- If a claim has multiple service lines, each line should be a separate row

---

## File 2 — Charges Report (Daily Charges)

**Source:** Ethizo → Reports → Daily Charges or Charge Entry Report
**Format:** `.xlsx` or `.csv`
**Suggested filename:** `daily_charges_YYYY-MM-DD.xlsx`

### Required Columns

| Column Name | Description | Example |
|-------------|-------------|---------|
| `Patient Name` | Patient full name | `Smith, John` |
| `Date of Service` | Service date | `2026-03-15` |
| `CPT Code` | Procedure code | `99213` |
| `Billed Amount` | Charge amount | `$185.00` |
| `Payer Name` | Primary insurance | `Medicare` |
| `Claim Status` | Status in billing system | `Submitted`, `Pending` |

### Optional but Helpful Columns

| Column Name | Description |
|-------------|-------------|
| `Secondary Payer` | Secondary insurance name |
| `Date Secondary Billed` | Date secondary claim was submitted |
| `Rendering NPI` | Provider NPI |

---

## File 3 — Patient Collections Report (Ledger)

**Source:** Ethizo → Reports → Patient Account Ledger or Collections
**Format:** `.xlsx` or `.csv`
**Suggested filename:** `patient_ledger_YYYY-MM-DD.xlsx`

### Required Columns

| Column Name | Description | Example |
|-------------|-------------|---------|
| `Patient Name` | Patient full name | `Smith, John` |
| `Date of Service` | Service date | `2026-03-15` |
| `Transaction Type` | Payment, Adjustment, Write-Off | `Payment` |
| `Amount` | Transaction amount | `$28.50` |
| `Payment Method` | Check, Card, Cash | `Card` |
| `Transaction Date` | Date of transaction | `2026-03-20` |

---

## File 4 — Patient Statement Upload Log

**Source:** Trizetto → Reports → Statement Upload or Patient Billing History
**Format:** `.xlsx` or `.csv`
**Suggested filename:** `patient_uploads_YYYY-MM-DD.xlsx`

### Required Columns

| Column Name | Description | Example |
|-------------|-------------|---------|
| `Patient Name` | Patient full name | `Smith, John` |
| `Statement Date` | Date statement was generated | `2026-03-10` |
| `Amount Billed` | Patient balance on statement | `$85.00` |
| `Upload Date` | Date sent to patient | `2026-03-11` |

---

## Important Notes for the Biller

### What We Are NOT Asking For
- We are not asking you to change your billing system or workflow
- We are not replacing you or auditing your individual claim decisions
- This is routine financial recordkeeping for the practice owner

### File Handling
- Files are processed automatically and moved to a `Processed/` subfolder
- You can drop multiple files at once — each is handled independently
- If a file has an error, it stays in the main folder and you will be notified

### Privacy
- The Google Drive folder is shared only with the practice owner and N2N Analytics
- Files are not shared with any third party

---

*N2N Analytics — Confidential. Internal use only.*

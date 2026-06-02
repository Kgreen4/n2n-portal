# Synthetic Test Fixture Guide
## ERA Pipeline End-to-End Test

> **Purpose:** Validate the full `ingest-era-file` → BigQuery → `view_revenue_leakage`
> pipeline before real Trizetto data arrives.
>
> **Files in this directory:**
> - `synthetic_trizetto_era_2026-03-20.csv` — simulated Trizetto ERA export (19 claims)
> - `synthetic_ethizo_charges_2026-03-20.csv` — simulated Ethizo charges (23 claims)

---

## What's Baked Into the Test Data

### ERA File (Trizetto side — `eob_line_items`)
19 claims across 5 payers. Includes deliberate edge cases:

| Claim | Scenario | What to Verify |
|-------|----------|----------------|
| CLM-2026-0305 (Medicare) | Denial — CO-4 (modifier issue) | `claim_status = 'denied'`, `paid_amount = 0` |
| CLM-2026-0309 (Aetna) | Denial — CO-22 (COB/coordination) | `claim_status = 'denied'`, `paid_amount = 0` |
| CLM-2026-0316 (United) | PR-1 — deductible applied | `claim_status = 'denied'`, `patient_responsibility > 0` |
| All others | Paid with CO-45 contractual adj | `claim_status = 'paid'`, `adjustment_amount > 0` |

### Charges File (Ethizo side — `charge_report`)
23 claims — the ERA file only covers 19. The 4 extras are **deliberate black holes**:

| Patient | DOS | CPT | Payer | Black Hole Urgency |
|---------|-----|-----|-------|--------------------|
| Turner Rachel | 2025-05-10 | 99214 | Humana | 🔴 **CRITICAL** (>270 days as of Mar 2026) |
| Turner Rachel | 2025-05-10 | 93000 | Humana | 🔴 **CRITICAL** (>270 days) |
| Phillips Victor | 2025-11-12 | 99213 | Medicare | 🟠 **HIGH** (>90 days) |
| Campbell Grace | 2026-02-28 | 99215 | Aetna | 🟡 **PENDING** (≤90 days) |

The `view_revenue_leakage` should flag exactly these 4 rows after both files are loaded.

---

## How to Run the Test

### Step 1 — Load the ERA file into BigQuery via `ingest-era-file`

Upload `synthetic_trizetto_era_2026-03-20.csv` to the GCW ERA Google Drive folder, then curl the function directly:

```bash
curl -X POST \
  https://jdmyjdvricpyrsfchakk.supabase.co/functions/v1/ingest-era-file \
  -H "Authorization: Bearer <SUPABASE_SERVICE_ROLE_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "practice_id": "df52d2fd-5e88-48de-9c45-13d0ae7847b0",
    "gdrive_file_id": "<FILE_ID_FROM_DRIVE>",
    "era_folder_id": "<ERA_FOLDER_ID>",
    "batch_id": "test-synthetic-2026-03-20"
  }'
```

**Expected response:**
```json
{
  "success": true,
  "records_processed": 19,
  "records_inserted": 19,
  "records_skipped": 0,
  "bq_errors": 0,
  "file_format": "csv",
  "event_type": "era_file_ingested"
}
```

### Step 2 — Load the Charges file into BigQuery via `ingest-charges`

The Ethizo charges file goes to `charge_report` via the existing `ingest-charges` function
in the GCW Analytics repo (not via `ingest-era-file`).

### Step 3 — Verify BigQuery Rows

```sql
-- Check ERA rows loaded correctly
SELECT claim_status, COUNT(*) as count, SUM(paid_amount) as total_paid
FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
WHERE source_type = 'trizetto_era'
  AND batch_id = 'test-synthetic-2026-03-20'
GROUP BY claim_status;

-- Expected:
-- paid    | 16 | ~$1,980
-- denied  |  3 | $0
```

```sql
-- Check denial codes
SELECT remark_code, COUNT(*) as count
FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
WHERE source_type = 'trizetto_era'
  AND batch_id = 'test-synthetic-2026-03-20'
  AND claim_status = 'denied'
GROUP BY remark_code;

-- Expected: CO-4 (1), CO-22 (1), PR-1 (1)
```

### Step 4 — Verify Black Hole Detector

```sql
-- Run the revenue leakage view
SELECT patient_name, date_of_service, cpt_code, billed_amount, urgency_flag
FROM `cardio-metrics-dev.billing_audit_practice_test.view_revenue_leakage`
WHERE practice_id = 'df52d2fd-5e88-48de-9c45-13d0ae7847b0'
ORDER BY date_of_service ASC;
```

**Expected output — exactly 4 rows:**

| patient_name | date_of_service | cpt_code | billed_amount | urgency_flag |
|---|---|---|---|---|
| Turner Rachel | 2025-05-10 | 99214 | $255.00 | CRITICAL |
| Turner Rachel | 2025-05-10 | 93000 | $55.00 | CRITICAL |
| Phillips Victor | 2025-11-12 | 99213 | $185.00 | HIGH |
| Campbell Grace | 2026-02-28 | 99215 | $350.00 | PENDING |

**Total opportunity identified: $845.00**

### Step 5 — Verify `pipeline_events` Log

```sql
-- In Supabase Table Editor or SQL Editor
SELECT event_type, records_processed, records_inserted, metadata
FROM pipeline_events
WHERE practice_id = 'df52d2fd-5e88-48de-9c45-13d0ae7847b0'
ORDER BY created_at DESC
LIMIT 5;
```

### Step 6 — Verify File Moved to Processed/ Subfolder

In Google Drive, confirm the test CSV file moved from the ERA folder root into
`ERA Folder / Processed/`. This confirms the re-processing guard works.

---

## Interpreting the Results

### If `records_skipped > 0`
Column mapping failed for some rows. Check the Supabase function logs:
- Supabase Dashboard → Edge Functions → `ingest-era-file` → Logs
- Look for `[ingest-era-file] skip row` lines showing which fields were empty

### If Black Hole Detector shows 0 rows (but charges were loaded)
The view's join logic isn't matching. Common causes:
- Patient name formatting differs between Ethizo and ERA (e.g., "Smith, John" vs "John Smith")
- Date format mismatch in BigQuery
- `practice_id` mismatch between tables

### If `success: false`
Check `pipeline_events.error_message` for the raw BQ error. Most common:
- Schema mismatch (column type wrong) → check `eob_line_items` DDL
- Auth failure → verify `GCP_SA_JSON` secret is set

---

## Cleanup After Testing

```sql
-- Remove synthetic test rows from BigQuery
DELETE FROM `cardio-metrics-dev.billing_audit_practice_test.eob_line_items`
WHERE source_type = 'trizetto_era'
  AND batch_id = 'test-synthetic-2026-03-20';
```

```sql
-- Remove synthetic charge rows (if loaded)
DELETE FROM `cardio-metrics-dev.billing_audit_practice_test.charge_report`
WHERE batch_id = 'test-synthetic-2026-03-20';
```

---

*N2N Analytics — Internal use only.*

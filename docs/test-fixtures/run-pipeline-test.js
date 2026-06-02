#!/usr/bin/env node
// run-pipeline-test.js — End-to-end pipeline test for GCW ERA + Charges
// Calls ingest-era-data and ingest-charges directly (bypasses Drive upload).
//
// Usage:
//   node run-pipeline-test.js
//
// Requires: SUPABASE_KEY env var (Supabase secret API key).
// Set before running: $env:SUPABASE_KEY="sb_secret_..."  (PowerShell)
//                 or: export SUPABASE_KEY="sb_secret_..."  (bash)

const SUPABASE_URL = "https://jdmyjdvricpyrsfchakk.supabase.co";
const SUPABASE_KEY = process.env.SUPABASE_KEY;
if (!SUPABASE_KEY) { console.error("ERROR: SUPABASE_KEY env var not set. Run: $env:SUPABASE_KEY='sb_secret_...'"); process.exit(1); }
const PRACTICE_ID  = "df52d2fd-5e88-48de-9c45-13d0ae7847b0";
const BATCH_ID     = "test-synthetic-2026-03-20";
const NPI          = "1831993245";
const ERA_FOLDER_ID = "1GOvEj8UJg4inP0WyoZQl3UoXC-u0rsFw"; // RCM Reporting Data > ERA

// ── ERA line items (19 rows from synthetic_trizetto_era_2026-03-20.csv) ───────
// claim_status manually derived:
//   CO-45 rows → "paid" (payer paid their contracted portion)
//   CO-4,  CO-22 → "denied" (payment = $0)
//   PR-1 → "denied" (deductible; patient responsibility, payer paid $0)
const ERA_ITEMS = [
  { payer_id:"00001", claim_number:"CLM-2026-0301", patient_name:"Johnson Margaret", patient_account:"1EG4-TE5-MK72", payer_name:"Medicare",          date_of_service:"2026-02-14", cpt_code:"99213", billed_amount:185.00, allowed_amount:130.23, paid_amount:104.18, adjustment_amount:54.77,  patient_responsibility:26.05, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Charges exceed fee schedule/maximum allowable", check_number:"EFT20260318001", check_date:"2026-03-18", era_transaction_date:"2026-03-18", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00001", claim_number:"CLM-2026-0302", patient_name:"Williams Robert",  patient_account:"2KJ7-HN3-PL91", payer_name:"Medicare",          date_of_service:"2026-02-14", cpt_code:"99214", billed_amount:255.00, allowed_amount:176.84, paid_amount:141.47, adjustment_amount:78.16,  patient_responsibility:35.37, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Charges exceed fee schedule/maximum allowable", check_number:"EFT20260318001", check_date:"2026-03-18", era_transaction_date:"2026-03-18", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00001", claim_number:"CLM-2026-0303", patient_name:"Davis Patricia",   patient_account:"3MN2-XQ8-RT44", payer_name:"Medicare",          date_of_service:"2026-02-15", cpt_code:"99396", billed_amount:175.00, allowed_amount:121.90, paid_amount:97.52,  adjustment_amount:53.10,  patient_responsibility:24.38, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Charges exceed fee schedule/maximum allowable", check_number:"EFT20260318001", check_date:"2026-03-18", era_transaction_date:"2026-03-18", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00001", claim_number:"CLM-2026-0304", patient_name:"Martinez Elena",   patient_account:"4PQ5-YW1-SU67", payer_name:"Medicare",          date_of_service:"2026-02-15", cpt_code:"93000", billed_amount:55.00,  allowed_amount:20.83,  paid_amount:16.66,  adjustment_amount:34.17,  patient_responsibility:4.17,  claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Charges exceed fee schedule/maximum allowable", check_number:"EFT20260318001", check_date:"2026-03-18", era_transaction_date:"2026-03-18", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00001", claim_number:"CLM-2026-0305", patient_name:"Anderson Thomas",  patient_account:"5RS8-ZV4-TV90", payer_name:"Medicare",          date_of_service:"2026-02-17", cpt_code:"99213", billed_amount:185.00, allowed_amount:130.23, paid_amount:0.00,   adjustment_amount:0.00,   patient_responsibility:0.00,  claim_status:"denied", remark_code:"CO-4",  adjustment_reason:"The service is inconsistent with the modifier used",   check_number:"EFT20260318001", check_date:"2026-03-18", era_transaction_date:"2026-03-18", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00271", claim_number:"CLM-2026-0306", patient_name:"Thompson Karen",   patient_account:"W123456789",   payer_name:"Aetna",              date_of_service:"2026-02-18", cpt_code:"99214", billed_amount:255.00, allowed_amount:210.00, paid_amount:168.00, adjustment_amount:45.00,  patient_responsibility:42.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320001", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00271", claim_number:"CLM-2026-0307", patient_name:"Jackson Linda",    patient_account:"W987654321",   payer_name:"Aetna",              date_of_service:"2026-02-18", cpt_code:"99215", billed_amount:350.00, allowed_amount:290.00, paid_amount:232.00, adjustment_amount:60.00,  patient_responsibility:58.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320001", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00271", claim_number:"CLM-2026-0308", patient_name:"White Michael",    patient_account:"W456789123",   payer_name:"Aetna",              date_of_service:"2026-02-19", cpt_code:"85027", billed_amount:45.00,  allowed_amount:38.00,  paid_amount:30.40,  adjustment_amount:7.00,   patient_responsibility:7.60,  claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320001", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00271", claim_number:"CLM-2026-0309", patient_name:"Harris Susan",     patient_account:"W321654987",   payer_name:"Aetna",              date_of_service:"2026-02-19", cpt_code:"99213", billed_amount:185.00, allowed_amount:0.00,   paid_amount:0.00,   adjustment_amount:185.00, patient_responsibility:0.00,  claim_status:"denied", remark_code:"CO-22", adjustment_reason:"This care may be covered by another payer per coordination of benefits", check_number:"CHK20260320001", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00291", claim_number:"CLM-2026-0310", patient_name:"Clark Nancy",      patient_account:"XNY-882341506",payer_name:"BCBS",               date_of_service:"2026-02-20", cpt_code:"99213", billed_amount:185.00, allowed_amount:145.00, paid_amount:116.00, adjustment_amount:40.00,  patient_responsibility:29.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320002", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00291", claim_number:"CLM-2026-0311", patient_name:"Robinson James",   patient_account:"XNY-773211407",payer_name:"BCBS",               date_of_service:"2026-02-20", cpt_code:"99214", billed_amount:255.00, allowed_amount:198.00, paid_amount:158.40, adjustment_amount:57.00,  patient_responsibility:39.60, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320002", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00291", claim_number:"CLM-2026-0312", patient_name:"Lewis Dorothy",    patient_account:"XNY-664100308",payer_name:"BCBS",               date_of_service:"2026-02-21", cpt_code:"99215", billed_amount:350.00, allowed_amount:275.00, paid_amount:220.00, adjustment_amount:75.00,  patient_responsibility:55.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320002", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00291", claim_number:"CLM-2026-0313", patient_name:"Walker Frank",     patient_account:"XNY-991234509",payer_name:"BCBS",               date_of_service:"2026-02-21", cpt_code:"36415", billed_amount:25.00,  allowed_amount:18.00,  paid_amount:14.40,  adjustment_amount:7.00,   patient_responsibility:3.60,  claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260320002", check_date:"2026-03-20", era_transaction_date:"2026-03-20", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00192", claim_number:"CLM-2026-0314", patient_name:"Hall Betty",       patient_account:"UHC-A12345678",payer_name:"United Healthcare",  date_of_service:"2026-02-24", cpt_code:"99213", billed_amount:185.00, allowed_amount:155.00, paid_amount:124.00, adjustment_amount:30.00,  patient_responsibility:31.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"EFT20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00192", claim_number:"CLM-2026-0315", patient_name:"Allen Henry",      patient_account:"UHC-B98765432",payer_name:"United Healthcare",  date_of_service:"2026-02-24", cpt_code:"99396", billed_amount:175.00, allowed_amount:130.00, paid_amount:104.00, adjustment_amount:45.00,  patient_responsibility:26.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"EFT20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00192", claim_number:"CLM-2026-0316", patient_name:"Young Emma",       patient_account:"UHC-C11223344",payer_name:"United Healthcare",  date_of_service:"2026-02-25", cpt_code:"99214", billed_amount:255.00, allowed_amount:210.00, paid_amount:0.00,   adjustment_amount:0.00,   patient_responsibility:0.00,  claim_status:"denied", remark_code:"PR-1", adjustment_reason:"Deductible amount",                                     check_number:"EFT20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00432", claim_number:"CLM-2026-0317", patient_name:"King George",      patient_account:"CIG-55667788", payer_name:"Cigna",              date_of_service:"2026-02-25", cpt_code:"99213", billed_amount:185.00, allowed_amount:148.00, paid_amount:118.40, adjustment_amount:37.00,  patient_responsibility:29.60, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00432", claim_number:"CLM-2026-0318", patient_name:"Wright Alice",     patient_account:"CIG-99887766", payer_name:"Cigna",              date_of_service:"2026-02-26", cpt_code:"99215", billed_amount:350.00, allowed_amount:280.00, paid_amount:224.00, adjustment_amount:70.00,  patient_responsibility:56.00, claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
  { payer_id:"00432", claim_number:"CLM-2026-0319", patient_name:"Scott Charles",    patient_account:"CIG-11334455", payer_name:"Cigna",              date_of_service:"2026-02-26", cpt_code:"81003", billed_amount:35.00,  allowed_amount:28.00,  paid_amount:22.40,  adjustment_amount:7.00,   patient_responsibility:5.60,  claim_status:"paid",   remark_code:"CO-45", adjustment_reason:"Contractual adjustment",                               check_number:"CHK20260321001", check_date:"2026-03-21", era_transaction_date:"2026-03-21", npi:NPI, practice_id:PRACTICE_ID, source_type:"trizetto_era", batch_id:BATCH_ID },
];

// ── Charge rows (23 rows from synthetic_ethizo_charges_2026-03-20.csv) ────────
// Includes 4 black holes: Turner Rachel (×2 CRITICAL), Phillips Victor (HIGH), Campbell Grace (PENDING)
const CHARGE_ROWS = [
  { patient_name:"Johnson Margaret", date_of_service:"2026-02-14", cpt_code:"99213", billed_amount:185.00, payer:"Medicare",          billing_status:"Submitted", npi:NPI },
  { patient_name:"Williams Robert",  date_of_service:"2026-02-14", cpt_code:"99214", billed_amount:255.00, payer:"Medicare",          billing_status:"Submitted", npi:NPI },
  { patient_name:"Davis Patricia",   date_of_service:"2026-02-15", cpt_code:"99396", billed_amount:175.00, payer:"Medicare",          billing_status:"Submitted", npi:NPI },
  { patient_name:"Martinez Elena",   date_of_service:"2026-02-15", cpt_code:"93000", billed_amount:55.00,  payer:"Medicare",          billing_status:"Submitted", npi:NPI },
  { patient_name:"Anderson Thomas",  date_of_service:"2026-02-17", cpt_code:"99213", billed_amount:185.00, payer:"Medicare",          billing_status:"Submitted", npi:NPI },
  { patient_name:"Thompson Karen",   date_of_service:"2026-02-18", cpt_code:"99214", billed_amount:255.00, payer:"Aetna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"Jackson Linda",    date_of_service:"2026-02-18", cpt_code:"99215", billed_amount:350.00, payer:"Aetna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"White Michael",    date_of_service:"2026-02-19", cpt_code:"85027", billed_amount:45.00,  payer:"Aetna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"Harris Susan",     date_of_service:"2026-02-19", cpt_code:"99213", billed_amount:185.00, payer:"Aetna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"Clark Nancy",      date_of_service:"2026-02-20", cpt_code:"99213", billed_amount:185.00, payer:"BCBS",              billing_status:"Submitted", npi:NPI },
  { patient_name:"Robinson James",   date_of_service:"2026-02-20", cpt_code:"99214", billed_amount:255.00, payer:"BCBS",              billing_status:"Submitted", npi:NPI },
  { patient_name:"Lewis Dorothy",    date_of_service:"2026-02-21", cpt_code:"99215", billed_amount:350.00, payer:"BCBS",              billing_status:"Submitted", npi:NPI },
  { patient_name:"Walker Frank",     date_of_service:"2026-02-21", cpt_code:"36415", billed_amount:25.00,  payer:"BCBS",              billing_status:"Submitted", npi:NPI },
  { patient_name:"Hall Betty",       date_of_service:"2026-02-24", cpt_code:"99213", billed_amount:185.00, payer:"United Healthcare", billing_status:"Submitted", npi:NPI },
  { patient_name:"Allen Henry",      date_of_service:"2026-02-24", cpt_code:"99396", billed_amount:175.00, payer:"United Healthcare", billing_status:"Submitted", npi:NPI },
  { patient_name:"Young Emma",       date_of_service:"2026-02-25", cpt_code:"99214", billed_amount:255.00, payer:"United Healthcare", billing_status:"Submitted", npi:NPI },
  { patient_name:"King George",      date_of_service:"2026-02-25", cpt_code:"99213", billed_amount:185.00, payer:"Cigna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"Wright Alice",     date_of_service:"2026-02-26", cpt_code:"99215", billed_amount:350.00, payer:"Cigna",             billing_status:"Submitted", npi:NPI },
  { patient_name:"Scott Charles",    date_of_service:"2026-02-26", cpt_code:"81003", billed_amount:35.00,  payer:"Cigna",             billing_status:"Submitted", npi:NPI },
  // ── Black holes ──────────────────────────────────────────────────────────────
  { patient_name:"Turner Rachel",    date_of_service:"2025-05-10", cpt_code:"99214", billed_amount:255.00, payer:"Humana",            billing_status:"Pending",   npi:NPI },
  { patient_name:"Turner Rachel",    date_of_service:"2025-05-10", cpt_code:"93000", billed_amount:55.00,  payer:"Humana",            billing_status:"Pending",   npi:NPI },
  { patient_name:"Phillips Victor",  date_of_service:"2025-11-12", cpt_code:"99213", billed_amount:185.00, payer:"Medicare",          billing_status:"Pending",   npi:NPI },
  { patient_name:"Campbell Grace",   date_of_service:"2026-02-28", cpt_code:"99215", billed_amount:350.00, payer:"Aetna",             billing_status:"Pending",   npi:NPI },
];

// ── HTTP helper ───────────────────────────────────────────────────────────────

async function post(path, body) {
  const url = `${SUPABASE_URL}/functions/v1/${path}`;
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SUPABASE_KEY}`,
      "Content-Type":  "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  return { status: resp.status, data };
}

// ── Main ──────────────────────────────────────────────────────────────────────

(async () => {
  console.log("=".repeat(60));
  console.log("GCW Pipeline Test — Synthetic Data 2026-03-20");
  console.log("=".repeat(60));

  // Step 1 — Load ERA rows via ingest-era-data
  console.log("\n[Step 1] Calling ingest-era-data with 19 ERA rows...");
  const eraResult = await post("ingest-era-data", {
    practice_id: PRACTICE_ID,
    items:        ERA_ITEMS,
    batch_id:     BATCH_ID,
  });
  console.log("  HTTP:", eraResult.status);
  console.log("  Response:", JSON.stringify(eraResult.data, null, 2));

  if (eraResult.status !== 200 || !eraResult.data?.success) {
    console.error("\n  ERROR: ingest-era-data failed. Aborting.");
    process.exit(1);
  }

  // Step 2 — Load charge rows via ingest-charges
  console.log("\n[Step 2] Calling ingest-charges with 23 charge rows...");
  const chargeResult = await post("ingest-charges", {
    practice_id: PRACTICE_ID,
    rows:         CHARGE_ROWS,
    filename:     "synthetic_ethizo_charges_2026-03-20.csv",
  });
  console.log("  HTTP:", chargeResult.status);
  console.log("  Response:", JSON.stringify(chargeResult.data, null, 2));

  if (chargeResult.status !== 200) {
    console.warn("\n  WARNING: ingest-charges returned non-200. Check response above.");
  }

  // Step 3 — Summary
  console.log("\n" + "=".repeat(60));
  console.log("DONE — Now run these BigQuery queries to verify:");
  console.log("=".repeat(60));

  console.log(`
-- 1. ERA summary (expect: paid=16 ~$1,892 | denied=3 $0)
SELECT claim_status, COUNT(*) AS cnt, ROUND(SUM(paid_amount),2) AS total_paid
FROM \`cardio-metrics-dev.billing_audit_practice_test.eob_line_items\`
WHERE source_type = 'trizetto_era'
  AND batch_id    = '${BATCH_ID}'
GROUP BY claim_status;

-- 2. Denial codes (expect: CO-4=1, CO-22=1, PR-1=1)
SELECT remark_code, COUNT(*) AS cnt
FROM \`cardio-metrics-dev.billing_audit_practice_test.eob_line_items\`
WHERE source_type = 'trizetto_era'
  AND batch_id    = '${BATCH_ID}'
  AND claim_status = 'denied'
GROUP BY remark_code;

-- 3. Black Hole Detector (expect: exactly 4 rows, $845 total)
SELECT patient_name, date_of_service, cpt_code, billed_amount, urgency_flag
FROM \`cardio-metrics-dev.billing_audit_practice_test.view_revenue_leakage\`
WHERE practice_id = '${PRACTICE_ID}'
ORDER BY date_of_service ASC;
`);

  // Cleanup reminder
  console.log("=".repeat(60));
  console.log("CLEANUP (run in BQ when done):");
  console.log(`
DELETE FROM \`cardio-metrics-dev.billing_audit_practice_test.eob_line_items\`
WHERE source_type = 'trizetto_era' AND batch_id = '${BATCH_ID}';

DELETE FROM \`cardio-metrics-dev.billing_audit_practice_test.charge_report\`
WHERE batch_id = '${BATCH_ID}';
`);
})();

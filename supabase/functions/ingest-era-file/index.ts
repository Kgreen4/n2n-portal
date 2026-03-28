// ingest-era-file — Downloads an ERA file from Google Drive, parses it
// (CSV or XLSX), inserts normalized line items into BigQuery eob_line_items,
// then moves the file into a "Processed" subfolder to prevent reprocessing.
// Deployed with --no-verify-jwt (called server-to-server by n8n).
//
// POST body:
//   {
//     practice_id:    string   (required)
//     gdrive_file_id: string   (required) — file ID from n8n Drive search
//     npi?:           string   — rendering NPI; defaults to GCW value
//     batch_id?:      string   — idempotency label for pipeline_events log
//     era_folder_id?: string   — parent folder ID; triggers move-to-Processed on success
//   }
//
// Response:
//   { success, records_processed, records_inserted, records_skipped, bq_errors, file_format, event_type }

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import * as XLSX from "npm:xlsx";
import { getGoogleAccessToken, moveToProcessedFolder } from "../_shared/gcp-auth.ts";

const GCP_PROJECT = "cardio-metrics-dev";
const BQ_DATASET  = "billing_audit_practice_test";
const BQ_TABLE    = "eob_line_items";

// ── Types ─────────────────────────────────────────────────────────────────────

interface IngestEraFilePayload {
  practice_id:    string;
  gdrive_file_id: string;
  npi?:           string;
  batch_id?:      string;
  era_folder_id?: string;
}

interface EraLineItem {
  // Dedup key (4 required fields)
  payer_id:        string;
  claim_number:    string;
  cpt_code:        string;
  date_of_service: string;
  // Financials
  billed_amount:          number;
  allowed_amount:         number;
  paid_amount:            number;
  adjustment_amount:      number;
  patient_responsibility: number;
  // Claim metadata
  payer_name:         string;
  patient_name?:      string;
  patient_account?:   string;
  npi:                string;
  claim_status:       string;
  remark_code?:       string;
  adjustment_reason?: string;
  check_number?:      string;
  check_date?:        string;
  era_transaction_date: string;
  // Pipeline metadata
  practice_id: string;
  source_type: "trizetto_era";
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function normalizeKey(s: string): string {
  return s.toLowerCase().replace(/\s+/g, "_").replace(/[^a-z0-9_]/g, "");
}

function col(row: Record<string, string>, ...candidates: string[]): string {
  for (const c of candidates) {
    const v = row[normalizeKey(c)];
    if (v !== undefined && v !== "") return v;
  }
  return "";
}

function money(val: string | number | undefined | null): number {
  if (val === "" || val === null || val === undefined) return 0;
  return parseFloat(String(val).replace(/[$,()]/g, "")) || 0;
}

// patResp: patient_responsibility (copay + deductible + coinsurance).
// A claim is "paid" when the payer's portion + patient's portion covers ≥99% of
// allowed — handles standard 80/20 Medicare splits correctly.
function deriveStatus(paid: number, allowed: number, patResp: number = 0): string {
  if (!allowed || allowed === 0)          return "denied";
  if ((paid + patResp) >= allowed * 0.99) return "paid";
  if (paid > 0)                           return "partial";
  return "denied";
}

// ── CSV parser (RFC 4180) ─────────────────────────────────────────────────────

function parseCSVText(text: string): Record<string, string>[] {
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (lines.length < 2) throw new Error("CSV has fewer than 2 lines (no data rows)");

  function parseRow(line: string): string[] {
    const fields: string[] = [];
    let field = "", inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuote && line[i + 1] === '"') { field += '"'; i++; }
        else inQuote = !inQuote;
      } else if (ch === "," && !inQuote) {
        fields.push(field.trim()); field = "";
      } else {
        field += ch;
      }
    }
    fields.push(field.trim());
    return fields;
  }

  const headers = parseRow(lines[0]).map(normalizeKey);
  return lines.slice(1).map(line => {
    const values = parseRow(line);
    return Object.fromEntries(headers.map((h, i) => [h, values[i] ?? ""]));
  });
}

// ── XLSX parser ───────────────────────────────────────────────────────────────

function parseXLSXBuffer(buffer: Uint8Array): Record<string, string>[] {
  const workbook = XLSX.read(buffer, { type: "array", cellDates: true });
  const sheetName = workbook.SheetNames[0]; // Always use first sheet
  const sheet = workbook.Sheets[sheetName];
  const raw = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
    defval: "",
    raw: false, // Format dates as strings
  });
  return raw.map(row => {
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(row)) {
      out[normalizeKey(k)] = String(v ?? "");
    }
    return out;
  });
}

// ── Row → EraLineItem mapper ──────────────────────────────────────────────────

function rowsToLineItems(
  rows: Record<string, string>[],
  practiceId: string,
  npi: string,
): { items: EraLineItem[]; skipped: number } {
  const batchDate = new Date().toISOString().slice(0, 10);
  const items: EraLineItem[] = [];
  let skipped = 0;

  for (const row of rows) {
    // ── Required dedup fields ─────────────────────────────────────────────────
    const payerId     = col(row, "payer_id", "payer id", "payerid", "payer_code", "insurance_id", "payer");
    const claimNumber = col(row, "claim_number", "claim number", "claimnumber", "patient_control_number", "pcn", "claim_ref", "claim_id");
    const cptCode     = col(row, "cpt_code", "cpt", "procedure_code", "service_code", "proc_code", "hcpcs", "procedure");
    const dos         = col(row, "date_of_service", "dos", "service_date", "from_date", "service_from", "date_of_svc");

    if (!payerId || !claimNumber || !cptCode || !dos) {
      skipped++;
      if (skipped <= 5) {
        console.warn(
          `[ingest-era-file] skip row — payer_id='${payerId}' claim='${claimNumber}' cpt='${cptCode}' dos='${dos}'`
        );
      }
      continue;
    }

    // ── Financials ────────────────────────────────────────────────────────────
    const billed  = money(col(row, "billed_amount", "charge_amount", "submitted_amount", "billed", "charges"));
    const allowed = money(col(row, "allowed_amount", "approved_amount", "allowed", "contract_amount", "approved"));
    const paid    = money(col(row, "paid_amount", "payment_amount", "paid", "check_amount", "net_payment", "amount_paid"));
    const patResp = money(col(row, "patient_responsibility", "patient_resp", "patient_amount", "copay", "deductible", "coinsurance"));
    const adj     = parseFloat((billed - allowed).toFixed(2));

    // ── Metadata ──────────────────────────────────────────────────────────────
    const checkDate   = col(row, "check_date", "payment_date", "eft_date", "settlement_date", "check_issue_date", "paid_date");
    const checkNumber = col(row, "check_number", "check_num", "eft_number", "trace_number", "payment_ref", "check_no");
    const remarkCode  = col(row, "remark_code", "adjustment_reason_code", "carc", "reason_code", "denial_code", "adj_reason_code");
    const remarkDesc  = col(row, "remark_description", "adjustment_reason", "denial_reason", "reason_description", "adj_reason");
    const payerName   = col(row, "payer_name", "payer", "insurance_company", "insurance_name", "insurance");
    const patientName = col(row, "patient_name", "patient", "member_name", "beneficiary", "patient_full_name");
    const memberId    = col(row, "member_id", "member_number", "subscriber_id", "member", "insurance_id", "member_no");

    items.push({
      payer_id:               payerId,
      claim_number:           claimNumber,
      cpt_code:               cptCode,
      date_of_service:        dos,
      billed_amount:          billed,
      allowed_amount:         allowed,
      paid_amount:            paid,
      adjustment_amount:      adj,
      patient_responsibility: patResp,
      payer_name:             payerName,
      ...(patientName && { patient_name: patientName }),
      ...(memberId    && { patient_account: memberId }),
      npi,
      claim_status:           deriveStatus(paid, allowed, patResp),
      ...(remarkCode  && { remark_code: remarkCode }),
      ...(remarkDesc  && { adjustment_reason: remarkDesc }),
      ...(checkNumber && { check_number: checkNumber }),
      check_date:             checkDate || batchDate,
      era_transaction_date:   checkDate || batchDate,
      practice_id:            practiceId,
      source_type:            "trizetto_era",
    });
  }

  return { items, skipped };
}

// ── BigQuery streaming insert ─────────────────────────────────────────────────

async function insertToBigQuery(
  items: EraLineItem[],
  gToken: string,
): Promise<{ inserted: number; errors: unknown[] }> {
  const bqUrl = `https://bigquery.googleapis.com/bigquery/v2/projects/${GCP_PROJECT}/datasets/${BQ_DATASET}/tables/${BQ_TABLE}/insertAll`;
  const BATCH_SIZE = 500;
  let inserted = 0;
  const errors: unknown[] = [];

  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const batch = items.slice(i, i + BATCH_SIZE).map(item => ({
      insertId: `${item.payer_id}|${item.claim_number}|${item.cpt_code}|${item.date_of_service}`,
      json: { ...item, ingested_at: new Date().toISOString() },
    }));

    const resp = await fetch(bqUrl, {
      method: "POST",
      headers: { "Authorization": `Bearer ${gToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ skipInvalidRows: false, ignoreUnknownValues: true, rows: batch }),
    });

    const data = await resp.json();
    if (!resp.ok) {
      console.error("[ingest-era-file] BQ HTTP error:", JSON.stringify(data));
      errors.push(data);
    } else if (data.insertErrors?.length > 0) {
      console.warn("[ingest-era-file] BQ insert errors:", JSON.stringify(data.insertErrors));
      errors.push(...data.insertErrors);
      inserted += batch.length - data.insertErrors.length;
    } else {
      inserted += batch.length;
    }
  }

  return { inserted, errors };
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req) => {
  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

  const SUPABASE_URL              = Deno.env.get("SUPABASE_URL")!;
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const GCP_SA_JSON               = Deno.env.get("GCP_SA_JSON");

  if (!GCP_SA_JSON) return json({ error: "GCP_SA_JSON not configured" }, 500);

  let body: IngestEraFilePayload;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const {
    practice_id,
    gdrive_file_id,
    npi           = "1831993245", // Default: GCW / Dr. Greatwood NPI
    batch_id,
    era_folder_id,
  } = body;

  if (!practice_id || !gdrive_file_id) {
    return json({ error: "practice_id and gdrive_file_id are required" }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  let bqInserted  = 0;
  let bqErrors: unknown[] = [];
  let rowsParsed  = 0;
  let rowsSkipped = 0;
  let fileFormat  = "unknown";

  try {
    const sa = JSON.parse(GCP_SA_JSON);
    // Default scope covers cloud-platform (BQ) + drive (read/write) — one token for everything
    const gToken = await getGoogleAccessToken(sa);

    // ── 1. Fetch file metadata ────────────────────────────────────────────────
    const metaResp = await fetch(
      `https://www.googleapis.com/drive/v3/files/${gdrive_file_id}?fields=id,name,mimeType`,
      { headers: { Authorization: `Bearer ${gToken}` } }
    );
    if (!metaResp.ok) {
      const err = await metaResp.text();
      return json({ error: `Drive metadata fetch failed (${metaResp.status}): ${err}` }, 502);
    }
    const meta = await metaResp.json();
    const mimeType: string      = meta.mimeType ?? "";
    const resolvedName: string  = meta.name ?? `era_file_${gdrive_file_id}`;

    console.info(`[ingest-era-file] processing "${resolvedName}" mimeType="${mimeType}"`);

    // ── 2. Download file ──────────────────────────────────────────────────────
    const dlResp = await fetch(
      `https://www.googleapis.com/drive/v3/files/${gdrive_file_id}?alt=media`,
      { headers: { Authorization: `Bearer ${gToken}` } }
    );
    if (!dlResp.ok) {
      const err = await dlResp.text();
      return json({ error: `Drive download failed (${dlResp.status}): ${err}` }, 502);
    }
    const fileBuffer = new Uint8Array(await dlResp.arrayBuffer());

    // ── 3. Detect format and parse ────────────────────────────────────────────
    let rows: Record<string, string>[];

    const isXlsx = mimeType.includes("spreadsheetml") ||
                   mimeType.includes("ms-excel") ||
                   resolvedName.toLowerCase().endsWith(".xlsx") ||
                   resolvedName.toLowerCase().endsWith(".xls");

    // 835 EDI files start with ASCII 'ISA'
    const isEdi  = fileBuffer[0] === 0x49 && fileBuffer[1] === 0x53 && fileBuffer[2] === 0x41;

    if (isEdi) {
      console.warn(`[ingest-era-file] 835 EDI detected in "${resolvedName}" — not yet parsed. Ask biller for CSV/XLSX export.`);
      fileFormat = "835_edi";
      rows = [];
    } else if (isXlsx) {
      fileFormat = "xlsx";
      rows = parseXLSXBuffer(fileBuffer);
      console.info(`[ingest-era-file] parsed XLSX: ${rows.length} rows from sheet "${XLSX.read(fileBuffer, { type: "array" }).SheetNames[0]}"`);
    } else {
      fileFormat = "csv";
      const text = new TextDecoder().decode(fileBuffer);
      rows = parseCSVText(text);
      console.info(`[ingest-era-file] parsed CSV: ${rows.length} rows`);
    }

    rowsParsed = rows.length;

    // ── 4. Map to EraLineItem[] ───────────────────────────────────────────────
    const { items, skipped } = rowsToLineItems(rows, practice_id, npi);
    rowsSkipped = skipped;

    if (skipped > 5) {
      console.warn(`[ingest-era-file] ...and ${skipped - 5} additional rows skipped. Verify column mappings match biller export headers.`);
    }
    console.info(`[ingest-era-file] mapped ${items.length} line items, skipped ${skipped} rows`);

    // ── 5. Insert to BigQuery ─────────────────────────────────────────────────
    if (items.length > 0) {
      const result = await insertToBigQuery(items, gToken);
      bqInserted = result.inserted;
      bqErrors   = result.errors;
    }

    // ── 6. Move file to Processed/ subfolder on clean success ─────────────────
    // Prevents n8n from re-picking the file on the next poll cycle.
    // Files that fail stay in the source folder and are retried automatically.
    if (era_folder_id && bqErrors.length === 0 && rowsParsed > 0) {
      await moveToProcessedFolder(gToken, gdrive_file_id, era_folder_id, null);
    }

  } catch (err) {
    console.error("[ingest-era-file] fatal error:", err);
    bqErrors.push(String(err));
  }

  // ── 7. Log to pipeline_events ─────────────────────────────────────────────
  const eventType = bqErrors.length === 0 ? "era_file_ingested" : "era_file_error";
  const { error: pgErr } = await supabase.from("pipeline_events").insert({
    practice_id,
    event_type:        eventType,
    source_system:     "trizetto_era",
    records_processed: rowsParsed,
    records_inserted:  bqInserted,
    records_skipped:   rowsSkipped,
    error_message:     bqErrors.length > 0 ? JSON.stringify(bqErrors[0]).slice(0, 500) : null,
    metadata: {
      batch_id:        batch_id ?? null,
      gdrive_file_id,
      file_format:     fileFormat,
      bq_errors_count: bqErrors.length,
    },
  });

  if (pgErr) console.error("[ingest-era-file] pipeline_events insert error:", pgErr);

  console.info(
    `[ingest-era-file] done — parsed: ${rowsParsed}, inserted: ${bqInserted}, skipped: ${rowsSkipped}, errors: ${bqErrors.length}`
  );

  return json({
    success:           bqErrors.length === 0,
    records_processed: rowsParsed,
    records_inserted:  bqInserted,
    records_skipped:   rowsSkipped,
    bq_errors:         bqErrors.length,
    file_format:       fileFormat,
    event_type:        eventType,
  });
});

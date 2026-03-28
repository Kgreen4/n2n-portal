// ingest-era-data — receives normalized ERA line items from n8n trizetto-era-sync workflow
// Deduplicates, inserts to BigQuery eob_line_items, logs to pipeline_events
// Deployed with --no-verify-jwt (called server-to-server by n8n)

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { getGoogleAccessToken } from "../_shared/gcp-auth.ts";

const GCP_PROJECT = "cardio-metrics-dev";
const BQ_DATASET  = "billing_audit_practice_test";
const BQ_TABLE    = "eob_line_items";

interface EraLineItem {
  // Identity / dedup key
  payer_id: string;
  claim_number: string;
  cpt_code: string;
  date_of_service: string;          // ISO date string: "2026-01-15"

  // Financials
  billed_amount: number;
  allowed_amount: number;
  paid_amount: number;
  adjustment_amount: number;
  patient_responsibility: number;   // What insurance says patient OWES (NOT what they paid)

  // Metadata
  patient_name?: string;
  patient_account?: string;
  payer_name: string;
  npi: string;
  claim_status: string;             // 'paid' | 'denied' | 'partial'
  remark_code?: string;             // CO-45, PR-1, etc.
  adjustment_reason?: string;       // Human-readable from code lookup
  check_number?: string;
  check_date?: string;
  era_transaction_date: string;

  // Pipeline metadata
  practice_id: string;
  source_type: "trizetto_era";      // Distinguishes from 'pdf_parser' rows
}

interface IngestPayload {
  practice_id: string;
  items: EraLineItem[];
  batch_id?: string;               // Optional idempotency key from n8n
}

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Build BigQuery streaming insertAll request body
function buildBQRows(items: EraLineItem[]) {
  return items.map(item => ({
    insertId: `${item.payer_id}|${item.claim_number}|${item.cpt_code}|${item.date_of_service}`,
    json: {
      ...item,
      ingested_at: new Date().toISOString(),
    }
  }));
}

Deno.serve(async (req) => {
  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

  const SUPABASE_URL              = Deno.env.get("SUPABASE_URL")!;
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const GCP_SA_JSON               = Deno.env.get("GCP_SA_JSON");

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return json({ error: "Missing Supabase env vars" }, 500);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  let body: IngestPayload;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const { practice_id, items, batch_id } = body;
  if (!practice_id || !Array.isArray(items) || items.length === 0) {
    return json({ error: "practice_id and items[] required" }, 400);
  }

  // ── Deduplication against existing BigQuery rows ────────────────────────────
  // Composite dedup key: payer_id + claim_number + cpt_code + date_of_service
  // We use the BigQuery insertId field (set above) which BigQuery uses for
  // best-effort deduplication within a ~1-minute window. For longer-term dedup,
  // query existing rows and filter before inserting.
  // For now: trust BQ insertId dedup + source_type column for audit filtering.
  const dedupedItems = items.filter(item =>
    item.payer_id && item.claim_number && item.cpt_code && item.date_of_service
  );

  if (dedupedItems.length === 0) {
    return json({ error: "All items missing required dedup fields" }, 400);
  }

  let bqInserted = 0;
  let bqErrors: unknown[] = [];

  // ── BigQuery Streaming Insert ───────────────────────────────────────────────
  if (GCP_SA_JSON) {
    try {
      const sa = JSON.parse(GCP_SA_JSON);
      const gToken = await getGoogleAccessToken(sa, "https://www.googleapis.com/auth/bigquery.insertdata");

      const bqUrl = `https://bigquery.googleapis.com/bigquery/v2/projects/${GCP_PROJECT}/datasets/${BQ_DATASET}/tables/${BQ_TABLE}/insertAll`;
      const rows = buildBQRows(dedupedItems);

      // Stream in batches of 500 (BQ streaming limit)
      const BATCH_SIZE = 500;
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const bqResp = await fetch(bqUrl, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${gToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            skipInvalidRows: false,
            ignoreUnknownValues: true,
            rows: batch,
          }),
        });

        const bqData = await bqResp.json();
        if (!bqResp.ok) {
          console.error("[ingest-era-data] BQ error:", JSON.stringify(bqData));
          bqErrors.push(bqData);
        } else if (bqData.insertErrors?.length > 0) {
          console.warn("[ingest-era-data] BQ insert errors:", JSON.stringify(bqData.insertErrors));
          bqErrors.push(...bqData.insertErrors);
          bqInserted += batch.length - bqData.insertErrors.length;
        } else {
          bqInserted += batch.length;
        }
      }
    } catch (err) {
      console.error("[ingest-era-data] GCP auth/BQ failure:", err);
      bqErrors.push(String(err));
    }
  } else {
    console.warn("[ingest-era-data] GCP_SA_JSON not set — skipping BigQuery insert");
  }

  // ── Log to pipeline_events ─────────────────────────────────────────────────
  // Uses the existing pipeline_events schema: event_type, source, file_name, details
  const eventType = bqErrors.length === 0 ? "era_sync_completed" : "era_sync_partial";
  const { error: pgErr } = await supabase
    .from("pipeline_events")
    .insert({
      practice_id,
      event_type: eventType,
      source:    "trizetto_era",
      file_name: batch_id ?? "era_batch",
      details: {
        batch_id:          batch_id ?? null,
        records_processed: items.length,
        records_inserted:  bqInserted,
        records_skipped:   items.length - dedupedItems.length,
        bq_errors_count:   bqErrors.length,
        first_error:       bqErrors.length > 0 ? JSON.stringify(bqErrors[0]).slice(0, 500) : null,
      },
    });

  if (pgErr) {
    console.error("[ingest-era-data] pipeline_events insert error:", pgErr);
  }

  console.info(`[ingest-era-data] done — processed: ${items.length}, inserted: ${bqInserted}, errors: ${bqErrors.length}`);

  return json({
    success: bqErrors.length === 0,
    records_processed: items.length,
    records_inserted: bqInserted,
    records_skipped: items.length - dedupedItems.length,
    bq_errors: bqErrors.length,
    event_type: eventType,
  });
});

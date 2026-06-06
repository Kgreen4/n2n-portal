// reprocess-document — Orchestrates full re-extraction of an EOB document.
// 0. Auth guard — requires valid user JWT or service role key
// 1. Verifies document exists
// 2. Deletes existing BigQuery rows (async Jobs API — required for DML)
// 2b. Deletes existing Supabase eob_line_items rows (prevents stacked duplicates on multi-run)
// 2c. Deletes existing Supabase eob_payments rows (check-level hierarchy — reset for re-ingestion)
// 3. Deletes page jobs from Supabase
// 4. Resets document status to 'pending' (single clean UPDATE)
// 5. Re-triggers eob-enqueue to re-extract with updated Gemini prompt

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const SUPABASE_ANON_KEY = Deno.env.get('SUPABASE_ANON_KEY');
const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// BigQuery config
const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';
const BQ_TABLE = 'eob_line_items';

// ──────────────────────────────────────────────────────────────
// GCP Auth (same pattern as eob-worker)
// ──────────────────────────────────────────────────────────────
function uint8ToBase64(bytes: Uint8Array): string {
  const CHUNK = 0x8000;
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += CHUNK) {
    parts.push(String.fromCharCode(...bytes.subarray(i, i + CHUNK)));
  }
  return btoa(parts.join(''));
}

const base64url = (buf: Uint8Array | string) => {
  const base64 = typeof buf === 'string' ? btoa(buf) : uint8ToBase64(buf);
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

async function getGoogleAccessToken(sa: any) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now - 30
  };
  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(payload));
  const dataToSign = new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`);
  const pem = sa.private_key.replace(/\\n/g, '\n');
  const binaryKey = atob(pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, ''));
  const keyBuffer = new Uint8Array(binaryKey.length);
  for (let i = 0; i < binaryKey.length; i++) {
    keyBuffer[i] = binaryKey.charCodeAt(i);
  }
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8', keyBuffer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', cryptoKey, dataToSign);
  const jwt = `${encodedHeader}.${encodedPayload}.${base64url(new Uint8Array(signature))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt })
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`GCP Auth Failed: ${JSON.stringify(data)}`);
  return data.access_token;
}

// ──────────────────────────────────────────────────────────────
// BigQuery DML via async Jobs API
// The synchronous /queries endpoint does not reliably execute DML (DELETE/UPDATE).
// The Jobs API is the correct path — create job, poll until DONE, read stats.
// ──────────────────────────────────────────────────────────────
async function bqDeleteDocument(gToken: string, eob_document_id: string): Promise<number> {
  const deleteQuery =
    `DELETE FROM \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\`` +
    ` WHERE eob_document_id = '${eob_document_id}'`;

  // 1. Start the async job
  const jobResp = await fetch(
    `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/jobs`,
    {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        configuration: { query: { query: deleteQuery, useLegacySql: false } }
      }),
    }
  );
  if (!jobResp.ok) {
    const errText = await jobResp.text();
    throw new Error(`BQ job create failed (${jobResp.status}): ${errText}`);
  }
  const job = await jobResp.json();
  const jobId = job.jobReference?.jobId;
  if (!jobId) throw new Error('BQ job create returned no jobId');

  console.info(`[reprocess] BQ DELETE job started: ${jobId}`);

  // 2. Poll until DONE (max 45 s — well within the 60 s edge function limit)
  const BQ_POLL_MS = 2000;
  const BQ_TIMEOUT_MS = 45_000;
  const deadline = Date.now() + BQ_TIMEOUT_MS;

  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, BQ_POLL_MS));

    const statusResp = await fetch(
      `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/jobs/${jobId}`,
      { headers: { 'Authorization': `Bearer ${gToken}` } }
    );
    if (!statusResp.ok) continue; // retry on transient HTTP error

    const s = await statusResp.json();
    if (s.status?.state !== 'DONE') continue;

    // Job finished — check for errors
    if (s.status?.errorResult) {
      throw new Error(`BQ DELETE job error: ${JSON.stringify(s.status.errorResult)}`);
    }
    const deleted = parseInt(s.statistics?.query?.numDmlAffectedRows || '0');
    console.info(`[reprocess] BQ DELETE complete — ${deleted} rows removed`);
    return deleted;
  }

  // Timed out: log and continue — rows will be overwritten when re-extraction runs
  console.warn(`[reprocess] BQ DELETE job ${jobId} timed out — rows may persist until re-extract overwrites them`);
  return 0;
}

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  const json = (body: any, status = 200) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });

  // ── 0. AUTH GUARD ──────────────────────────────────────────
  // Requires either a valid user JWT (from the frontend via supabase.functions.invoke)
  // or the service role key (for any future server-to-server calls).
  // Deployed with --no-verify-jwt so the Supabase gateway does not block the request,
  // but we enforce auth at the application level here.
  const authHeader = req.headers.get('Authorization');
  if (!authHeader) {
    return json({ error: 'Unauthorized' }, 401);
  }

  const token = authHeader.replace('Bearer ', '');
  if (token !== SUPABASE_SERVICE_ROLE_KEY) {
    // Not the service role key — validate as a user JWT via the anon-key client
    if (!SUPABASE_ANON_KEY) {
      console.error('[reprocess] SUPABASE_ANON_KEY not set — cannot validate user JWT');
      return json({ error: 'Unauthorized' }, 401);
    }
    const authClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      global: { headers: { Authorization: authHeader } },
    });
    const { data: { user }, error: authError } = await authClient.auth.getUser();
    if (authError || !user) {
      console.error(`[reprocess] Auth failed: ${authError?.message ?? 'no user'}`);
      return json({ error: 'Unauthorized' }, 401);
    }
    console.info(`[reprocess] Authenticated user: ${user.id}`);
  } else {
    console.info('[reprocess] Authenticated via service role key');
  }
  // ── END AUTH GUARD ─────────────────────────────────────────

  try {
    const { eob_document_id } = await req.json();
    if (!eob_document_id) return json({ error: 'eob_document_id is required' }, 400);

    console.info(`[reprocess] Starting re-process for document ${eob_document_id}`);

    // 1. VERIFY DOCUMENT EXISTS AND GET DETAILS
    const { data: doc, error: docErr } = await supabase
      .from('eob_documents')
      .select('id, practice_id, file_name, file_path, status, total_pages')
      .eq('id', eob_document_id)
      .single();

    if (docErr || !doc) return json({ error: 'Document not found' }, 404);

    // Block re-process if document is currently being processed
    if (['pending', 'queued', 'processing'].includes(doc.status)) {
      return json({ error: 'Document is currently being processed. Wait for it to finish.' }, 409);
    }

    // PRE-FLIGHT: detect orphaned failed rows that co-exist with a non-failed duplicate.
    // This happens when duplicate rows were inserted before the unique constraint
    // (practice_id, file_name) was enforced. Postgres fires the constraint during the
    // internal tuple-rewrite of any UPDATE on either row, even when the constrained
    // columns aren't being changed. Detect and block cleanly rather than crash.
    const { data: nfDupe } = await supabase
      .from('eob_documents')
      .select('id, status')
      .eq('practice_id', doc.practice_id)
      .eq('file_name', doc.file_name)
      .neq('id', eob_document_id)
      .neq('status', 'failed')
      .maybeSingle();

    if (nfDupe) {
      console.warn(`[reprocess] Blocked: non-failed duplicate exists for same file`, {
        failed_id: eob_document_id,
        duplicate_id: nfDupe.id,
        duplicate_status: nfDupe.status,
      });
      return json({
        error: `Cannot reprocess — a ${nfDupe.status} version of this document already exists. The failed record is an orphan and should be deleted.`,
        duplicate_id: nfDupe.id,
        duplicate_status: nfDupe.status,
      }, 409);
    }

    console.info(`[reprocess] Document ${eob_document_id}: status=${doc.status}, practice=${doc.practice_id}`);

    // 2. DELETE BIGQUERY ROWS (best-effort — non-fatal)
    // The BQ eob_line_items table was originally created without an eob_document_id
    // column; streaming inserts that reference it fail silently in eob-worker.
    // We attempt the delete so that if/when the schema is fixed we clean up properly,
    // but we never block a reprocess on a BQ schema mismatch.
    let bqDeleted = 0;
    try {
      const sa = JSON.parse(GCP_SA_JSON_STR.trim());
      const gToken = await getGoogleAccessToken(sa);
      bqDeleted = await bqDeleteDocument(gToken, eob_document_id);
    } catch (bqErr: any) {
      console.warn(`[reprocess] BigQuery delete failed (non-fatal, continuing): ${bqErr.message}`);
    }

    // 2b. DELETE SUPABASE LINE ITEMS
    // CRITICAL: without this, each reprocess appends new rows on top of old ones,
    // inflating extractedPaid in the Reports reconciliation gap 2-3x per run.
    const { error: liErr } = await supabase
      .from('eob_line_items')
      .delete()
      .eq('eob_document_id', eob_document_id);

    if (liErr) {
      console.warn(`[reprocess] Supabase line items delete failed (non-fatal): ${liErr.message}`);
    } else {
      console.info(`[reprocess] Deleted Supabase eob_line_items for document ${eob_document_id}`);
    }

    // 2c. DELETE SUPABASE eob_payments (check-level hierarchy)
    // These are upserted fresh during re-ingestion; stale rows cause check-level
    // gaps to persist in the Reports page even after a successful reprocess.
    const { error: pmtErr } = await supabase
      .from('eob_payments')
      .delete()
      .eq('eob_document_id', eob_document_id);

    if (pmtErr) {
      console.warn(`[reprocess] eob_payments delete failed (non-fatal): ${pmtErr.message}`);
    } else {
      console.info(`[reprocess] Deleted eob_payments for document ${eob_document_id}`);
    }

    // 3. DELETE PAGE JOBS
    const { error: jobsErr } = await supabase
      .from('eob_page_jobs')
      .delete()
      .eq('eob_document_id', eob_document_id);

    if (jobsErr) {
      console.error(`[reprocess] Failed to delete page jobs: ${jobsErr.message}`);
      // Non-fatal — continue with reset
    } else {
      console.info(`[reprocess] Deleted page jobs for document ${eob_document_id}`);
    }

    // 4. RESET DOCUMENT STATUS
    // Single clean UPDATE — all columns confirmed present via migration 20260522.
    const { error: resetErr } = await supabase
      .from('eob_documents')
      .update({
        status: 'pending',
        items_extracted: 0,
        error_message: null,
        review_status: null,
        review_reasons: null,
        last_exported_at: null,
        export_batch_id: null,
        export_total_paid: null,
        export_total_patient_resp: null,
        export_claim_count: null,
      })
      .eq('id', eob_document_id);

    if (resetErr) {
      console.error(`[reprocess] Reset failed: code=${(resetErr as any).code} msg=${resetErr.message} hint=${(resetErr as any).hint}`);
      return json({
        error: 'Failed to reset document',
        details: resetErr.message,
        code: (resetErr as any).code,
        hint: (resetErr as any).hint,
      }, 500);
    }
    console.info(`[reprocess] Reset document ${eob_document_id} to pending`);

    // 4b. REFUND CREDITS for the original page count so re-enqueue doesn't double-charge
    const originalPages = doc.total_pages ?? 0;
    if (originalPages > 0) {
      try {
        await supabase.rpc('add_parsing_credits', {
          p_practice_id: doc.practice_id,
          p_amount: originalPages,
        });
        console.info(`[reprocess] Refunded ${originalPages} credits for document ${eob_document_id}`);
      } catch (refundErr: any) {
        // Non-fatal — log and continue; worst case eob-enqueue will catch insufficient credits
        console.warn(`[reprocess] Credit refund failed (non-fatal): ${refundErr.message}`);
      }
    }

    // 5. RE-TRIGGER EXTRACTION
    // Determine source based on file_path format:
    //   - "gdrive://FILE_ID" → re-download from Google Drive (legacy records before archival)
    //   - anything else → Supabase Storage eob-uploads bucket
    const storagePath = doc.file_path;
    let enqueueBody: any;

    if (storagePath && storagePath.startsWith('gdrive://')) {
      // Legacy Google Drive record — file wasn't archived to eob-uploads yet
      const gdriveFileId = storagePath.replace('gdrive://', '');
      console.info(`[reprocess] file_path is Google Drive ref: ${gdriveFileId}`);
      enqueueBody = {
        eob_document_id,
        practice_id: doc.practice_id,
        gdrive_file_id: gdriveFileId,
      };
    } else {
      enqueueBody = {
        eob_document_id,
        practice_id: doc.practice_id,
        storage_bucket: 'eob-uploads',
        storage_path: storagePath,
      };
    }

    const enqueueResp = await fetch(`${SUPABASE_URL}/functions/v1/eob-enqueue`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(enqueueBody),
    });

    const enqueueResult = await enqueueResp.json();

    if (!enqueueResp.ok) {
      // Extract a readable string from whatever eob-enqueue returned
      const errDetail: string =
        typeof enqueueResult === 'string'
          ? enqueueResult
          : (enqueueResult?.error
              ? `${enqueueResult.error}${enqueueResult.details ? `: ${enqueueResult.details}` : ''}`
              : JSON.stringify(enqueueResult));
      console.error(`[reprocess] eob-enqueue failed (HTTP ${enqueueResp.status}): ${errDetail}`);
      return json({
        error: 'Document reset but re-enqueue failed',
        details: errDetail,          // always a string now — frontend can display it
        http_status: enqueueResp.status,
        bq_rows_deleted: bqDeleted,
      }, 500);
    }

    console.info(`[reprocess] Successfully re-triggered extraction for document ${eob_document_id}`);

    return json({
      status: 'reprocessing',
      eob_document_id,
      bq_rows_deleted: bqDeleted,
      message: 'Document has been reset and re-queued for extraction.',
    });

  } catch (err: any) {
    console.error(`[reprocess] Error: ${err.message}`);
    return json({ error: 'Re-process failed', details: err.message }, 500);
  }
});

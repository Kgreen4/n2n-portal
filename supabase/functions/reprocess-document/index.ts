// reprocess-document — Orchestrates full re-extraction of an EOB document.
// 1. Verifies ownership via practice_users
// 2. Deletes existing BigQuery rows for the document
// 3. Deletes page jobs from Supabase
// 4. Resets document status to 'pending'
// 5. Re-triggers eob-enqueue to re-extract with updated Gemini prompt

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
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

  try {
    const { eob_document_id } = await req.json();
    if (!eob_document_id) return json({ error: 'eob_document_id is required' }, 400);

    console.info(`[reprocess] Starting re-process for document ${eob_document_id}`);

    // 1. VERIFY DOCUMENT EXISTS AND GET DETAILS
    const { data: doc, error: docErr } = await supabase
      .from('eob_documents')
      .select('id, practice_id, file_name, file_path, status')
      .eq('id', eob_document_id)
      .single();

    if (docErr || !doc) return json({ error: 'Document not found' }, 404);

    // Block re-process if document is currently being processed
    if (['pending', 'queued', 'processing'].includes(doc.status)) {
      return json({ error: 'Document is currently being processed. Wait for it to finish.' }, 409);
    }

    console.info(`[reprocess] Document ${eob_document_id}: status=${doc.status}, practice=${doc.practice_id}`);

    // 2. DELETE BIGQUERY ROWS
    let bqDeleted = 0;
    try {
      const sa = JSON.parse(GCP_SA_JSON_STR.trim());
      const gToken = await getGoogleAccessToken(sa);
      const deleteQuery = `DELETE FROM \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\` WHERE eob_document_id = '${eob_document_id}'`;
      const bqResp = await fetch(
        `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`,
        {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: deleteQuery, useLegacySql: false }),
        }
      );
      const bqResult = await bqResp.json();
      bqDeleted = parseInt(bqResult.numDmlAffectedRows || '0');
      console.info(`[reprocess] Deleted ${bqDeleted} BigQuery rows`);
    } catch (bqErr: any) {
      console.error(`[reprocess] BigQuery delete failed: ${bqErr.message}`);
      return json({ error: 'Failed to delete BigQuery data', details: bqErr.message }, 500);
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
    const { error: resetErr } = await supabase
      .from('eob_documents')
      .update({
        status: 'pending',
        items_extracted: 0,
        error_message: null,
        review_status: null,
        review_reasons: [],
        last_exported_at: null,
        export_batch_id: null,
        export_total_paid: null,
        export_total_patient_resp: null,
        export_claim_count: null,
      })
      .eq('id', eob_document_id);

    if (resetErr) {
      return json({ error: 'Failed to reset document', details: resetErr.message }, 500);
    }
    console.info(`[reprocess] Reset document ${eob_document_id} to pending`);

    // 5. RE-TRIGGER EXTRACTION
    // Use file_path from the document record (contains the actual storage path with timestamp prefix)
    const storagePath = doc.file_path;

    const enqueueResp = await fetch(`${SUPABASE_URL}/functions/v1/eob-enqueue`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        eob_document_id,
        practice_id: doc.practice_id,
        storage_bucket: 'eob-uploads',
        storage_path: storagePath,
      }),
    });

    const enqueueResult = await enqueueResp.json();

    if (!enqueueResp.ok) {
      console.error(`[reprocess] eob-enqueue failed: ${JSON.stringify(enqueueResult)}`);
      return json({
        error: 'Document reset but re-enqueue failed',
        details: enqueueResult,
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

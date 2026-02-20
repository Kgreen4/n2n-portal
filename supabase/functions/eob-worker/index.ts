// eob-worker.js — Per-page EOB extraction worker
// Downloads a single PDF page from Supabase Storage, calls Gemini 2.0 Flash
// for extraction, and inserts structured line items into BigQuery.

import { createClient } from "npm:@supabase/supabase-js@2.39.7";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ──────────────────────────────────────────────────────────────
// BigQuery Configuration
// ──────────────────────────────────────────────────────────────
const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';
const BQ_TABLE = 'eob_line_items';
const BQ_INSERT_URL = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/datasets/${BQ_DATASET}/tables/${BQ_TABLE}/insertAll`;

// ──────────────────────────────────────────────────────────────
// Helpers: Encoding
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

// ──────────────────────────────────────────────────────────────
// Helpers: Data Sanitization
// ──────────────────────────────────────────────────────────────

/** Format a Date as BigQuery-compatible TIMESTAMP: YYYY-MM-DD HH:MM:SS */
function formatBQTimestamp(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/** Parse currency strings like "$1,234.56" or "1234.56" to a float. Returns null for invalid/empty. */
function parseCurrency(val: any): number | null {
  if (val === null || val === undefined || val === '') return null;
  const cleaned = String(val).replace(/[$,]/g, '').trim();
  // Handle parenthetical negatives: ($15.00) → -15.00
  const isNegative = cleaned.startsWith('(') && cleaned.endsWith(')');
  const numStr = isNegative ? cleaned.slice(1, -1) : cleaned;
  const num = parseFloat(numStr);
  if (isNaN(num)) return null;
  return isNegative ? -num : num;
}

/** Validate and normalize a date string to YYYY-MM-DD for BigQuery DATE column */
function formatBQDate(val: any): string | null {
  if (!val || val === 'null' || val === '') return null;
  const str = String(val).trim();
  // Already in YYYY-MM-DD format
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
  // Try MM/DD/YYYY format
  const match = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (match) {
    return `${match[3]}-${match[1].padStart(2, '0')}-${match[2].padStart(2, '0')}`;
  }
  // Try to parse as a Date object
  const d = new Date(str);
  if (!isNaN(d.getTime())) {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  }
  return null;
}

// ──────────────────────────────────────────────────────────────
// GCP Authentication (JWT → Access Token)
// ──────────────────────────────────────────────────────────────
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
// Helpers: Retry Logic for Gemini 429/503
// ──────────────────────────────────────────────────────────────
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const GEMINI_MAX_RETRIES = 2;        // 2 retries = 3 total attempts
const GEMINI_RETRY_BASE_MS = 10000;  // 10s initial backoff
const GEMINI_RETRY_MULTIPLIER = 1.5; // backoffs: 10s, 15s

// ──────────────────────────────────────────────────────────────
// Enhanced Gemini Prompt (13 fields, per-page extraction)
// ──────────────────────────────────────────────────────────────
const GEMINI_PROMPT = `Extract ALL medical line items from this EOB (Explanation of Benefits) page.

Return a JSON object with an 'items' array. Each item must include these fields (use null if not found):
- patient_name: Full name of the patient
- member_id: Member/subscriber ID number
- date_of_service: Date service was provided (format: YYYY-MM-DD)
- cpt_code: CPT/HCPCS procedure code
- cpt_description: Description of the procedure/service
- billed_amount: Amount billed by the provider (numeric, no $ sign)
- allowed_amount: Amount allowed by the insurance plan (numeric, no $ sign)
- paid_amount: Amount paid by the insurance company (numeric, no $ sign)
- patient_responsibility: Amount the patient owes (numeric, no $ sign)
- rendering_provider_npi: NPI number of the rendering provider
- denial_code: Denial/reason code if the claim was denied or adjusted
- denial_reason: Text explanation for denial or adjustment
- claim_status: Status of the claim (e.g., "Paid", "Denied", "Adjusted", "Partially Paid")

Important:
- Extract EVERY line item on the page, do not skip any
- If a field is not present on the EOB, set it to null
- Return amounts as numbers without dollar signs or commas
- If the page has no line items (e.g., it is a cover page or summary page), return: {"items": []}`;

// ──────────────────────────────────────────────────────────────
// Main Worker Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  let jobId: string | null = null;  // Track job ID for error handling

  try {
    const payload = await req.json();
    const job = payload.job || payload;
    jobId = job.id || null;
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());

    // Use practice_id from payload if provided (passed by eob-enqueue),
    // otherwise look it up from eob_documents
    let practice_id = job.practice_id;
    if (!practice_id) {
      const { data: docRow, error: docErr } = await supabase
        .from('eob_documents')
        .select('practice_id')
        .eq('id', job.eob_document_id)
        .single();
      if (docErr) throw new Error(`Doc lookup failed: ${docErr.message}`);
      practice_id = docRow.practice_id;
    }

    // STEP 1: DOWNLOAD PDF PAGE from Supabase Storage
    const pageStr = String(job.page_number).padStart(3, '0');
    const filePath = `${job.eob_document_id}/page-${pageStr}.pdf`;
    const { data: fileBlob, error: dlErr } = await supabase.storage.from('eob-pages').download(filePath);
    if (dlErr) throw new Error(`Download failed: ${dlErr.message}`);
    const base64PDF = uint8ToBase64(new Uint8Array(await fileBlob.arrayBuffer()));
    console.info(`[eob-worker] Downloaded page ${job.page_number} for doc ${job.eob_document_id}`);

    // STEP 2: AUTHENTICATE to GCP
    const gToken = await getGoogleAccessToken(sa);

    // STEP 3: CALL GEMINI 2.0 FLASH (Vertex AI) — per-page extraction with retry
    const VERTEX_URL = `https://us-central1-aiplatform.googleapis.com/v1/projects/${sa.project_id}/locations/us-central1/publishers/google/models/gemini-2.0-flash-exp:generateContent`;
    const geminiBody = JSON.stringify({
      contents: [{ "role": "user", "parts": [
        { "text": GEMINI_PROMPT },
        { "inlineData": { "mimeType": "application/pdf", "data": base64PDF } }
      ]}],
      generationConfig: { "responseMimeType": "application/json", "maxOutputTokens": 4096 }
    });

    let aiResp!: Response;
    let aiData: any;

    for (let attempt = 0; attempt <= GEMINI_MAX_RETRIES; attempt++) {
      aiResp = await fetch(VERTEX_URL, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
        body: geminiBody
      });
      aiData = await aiResp.json();

      if (aiResp.ok && !aiData.error) break;  // success

      const isRetryable = aiResp.status === 429 || aiResp.status === 503;
      const errMsg = aiData.error?.message || `Gemini HTTP ${aiResp.status}`;

      if (!isRetryable || attempt === GEMINI_MAX_RETRIES) {
        console.error(`[eob-worker] Gemini API error for page ${job.page_number}: ${errMsg}`);
        throw new Error(`Gemini API error: ${errMsg}`);
      }

      // Exponential backoff, respecting Retry-After header if present
      const retryAfterHeader = aiResp.headers.get('Retry-After');
      const retryAfterMs = retryAfterHeader ? parseInt(retryAfterHeader, 10) * 1000 : 0;
      const backoffMs = Math.max(
        GEMINI_RETRY_BASE_MS * Math.pow(GEMINI_RETRY_MULTIPLIER, attempt),
        retryAfterMs
      );
      console.warn(`[eob-worker] Gemini ${aiResp.status} on page ${job.page_number} (attempt ${attempt + 1}/${GEMINI_MAX_RETRIES + 1}), retrying in ${backoffMs / 1000}s...`);
      await sleep(backoffMs);
    }

    // Handle empty/blocked Gemini responses (cover pages, blank pages)
    if (!aiData.candidates || !aiData.candidates[0]?.content?.parts?.[0]?.text) {
      console.info(`[eob-worker] No content from Gemini for page ${job.page_number} — likely a cover/summary page`);
      await supabase.rpc('succeed_eob_page_job', {
        p_page_job_id: job.id,
        p_items_extracted: 0,
        p_gemini_response_type: 'no_candidates',
        p_gemini_raw_response: aiData
      });
      return new Response(JSON.stringify({ status: 'succeeded', count: 0, note: 'No extractable content on this page' }), { status: 200 });
    }

    const rawText = aiData.candidates[0].content.parts[0].text;
    const parsed = JSON.parse(rawText);
    const extracted = parsed.items || parsed.line_items || [];
    console.info(`[eob-worker] Gemini extracted ${extracted.length} line items from page ${job.page_number}`);

    // STEP 4: PERSIST TO BIGQUERY
    if (extracted.length > 0) {
      const now = formatBQTimestamp(new Date());

      const rows = extracted.map((it: any, idx: number) => ({
        insertId: `${job.eob_document_id}_p${job.page_number}_${idx}`,  // dedup key for retries
        json: {
          id: crypto.randomUUID(),
          eob_document_id: job.eob_document_id,
          practice_id: practice_id,
          page_number: job.page_number,
          patient_name: it.patient_name || null,
          member_id: it.member_id || null,
          date_of_service: formatBQDate(it.date_of_service),
          cpt_code: it.cpt_code || null,
          cpt_description: it.cpt_description || null,
          billed_amount: parseCurrency(it.billed_amount),
          allowed_amount: parseCurrency(it.allowed_amount),
          paid_amount: parseCurrency(it.paid_amount),
          patient_responsibility: parseCurrency(it.patient_responsibility),
          rendering_provider_npi: it.rendering_provider_npi || null,
          denial_code: it.denial_code || null,
          denial_reason: it.denial_reason || null,
          claim_status: it.claim_status || null,
          created_at: now,
        }
      }));

      const bqResp = await fetch(BQ_INSERT_URL, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${gToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ rows }),
      });

      const bqResult = await bqResp.json();

      if (!bqResp.ok) {
        throw new Error(`BigQuery insert failed (HTTP ${bqResp.status}): ${JSON.stringify(bqResult)}`);
      }

      if (bqResult.insertErrors && bqResult.insertErrors.length > 0) {
        console.error('[eob-worker] BigQuery partial insert errors:', JSON.stringify(bqResult.insertErrors));
        throw new Error(`BigQuery insert had ${bqResult.insertErrors.length} row errors: ${JSON.stringify(bqResult.insertErrors[0])}`);
      }

      console.info(`[eob-worker] Inserted ${rows.length} rows into BigQuery for page ${job.page_number}`);
    }

    // STEP 5: FINALIZE — mark job as succeeded with audit data
    await supabase.rpc('succeed_eob_page_job', {
      p_page_job_id: job.id,
      p_items_extracted: extracted.length,
      p_gemini_response_type: extracted.length > 0 ? 'items_found' : 'empty_items_array',
      p_gemini_raw_response: aiData
    });
    return new Response(JSON.stringify({
      status: 'succeeded',
      count: extracted.length,
      page_number: job.page_number,
      eob_document_id: job.eob_document_id
    }), { status: 200 });

  } catch (err) {
    console.error('[eob-worker] FAILED:', err.message);

    // Attempt to mark the job as failed in Supabase so it can be retried
    if (jobId) {
      try {
        await supabase.rpc('fail_eob_page_job', {
          p_page_job_id: jobId,
          p_error_message: err.message || 'Unknown error'
        });
        console.info(`[eob-worker] Marked job ${jobId} as failed`);
      } catch (rpcErr) {
        console.error('[eob-worker] fail_eob_page_job RPC also failed:', rpcErr.message);
      }
    }

    return new Response(JSON.stringify({ error: "Operation Failed", details: err.message }), { status: 500 });
  }
});

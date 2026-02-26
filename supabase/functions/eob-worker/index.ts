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
// Gemini Prompt (19 fields, polymorphic extraction + 835-ready)
// Handles standard claims, MIPS/incentive bonuses, and adjustments
// Uses remark_code/remark_reason (CARC/RARC) instead of denial_code
// Extracts claim_number, payment_date, payer_name, payer_id,
// and adjustment_amount for ANSI X12 835 file generation
// ──────────────────────────────────────────────────────────────
const GEMINI_PROMPT = `Extract ALL line items from this EOB (Explanation of Benefits) or payment document page.

FIRST — Identify the document type for this page:
- STANDARD CLAIM EOB: Contains CPT/HCPCS procedure codes, dates of service, billed/allowed/paid amounts per service line
- MIPS/INCENTIVE BONUS: Contains Merit-Based Incentive Payment System (MIPS) bonuses, quality incentive payments, or similar non-service-line payments. These typically list Claim IDs, Member IDs, and Bonus Amounts but NO procedure codes.
- SUMMARY/CHECK PAGE: Contains check-level totals, payment summaries, or provider-level payment totals. These pages show the total amount paid via a check or EFT, often with a check number.
- COVER PAGE: Contains only headers, instructions, or administrative info with no extractable data

Return a JSON object with an 'items' array. Each item must include these fields (use null if not found):
- line_type: "medical_service" for standard claims, "incentive_bonus" for MIPS/quality bonuses, "adjustment" for payment adjustments, "summary_total" for check/payment totals
- patient_name: Full name of the patient
- member_id: Member/subscriber ID number
- date_of_service: Date service was provided (format: YYYY-MM-DD)
- cpt_code: CPT/HCPCS procedure code. For MIPS/incentive bonuses, use "MIPS_BONUS". For summary totals, use "SUMMARY"
- cpt_description: Description of the procedure/service. For MIPS bonuses, use "MIPS Incentive Payment" or the bonus category description. For summary totals, use "Check Total" or "EFT Total" as appropriate
- billed_amount: Amount billed by the provider (numeric, no $ sign)
- allowed_amount: Amount allowed by the insurance plan (numeric, no $ sign)
- paid_amount: Amount paid by the insurance company (numeric, no $ sign). For MIPS bonuses, this is the bonus/incentive amount. For summary totals, this is the check/EFT total amount
- patient_responsibility: Amount the patient owes (numeric, no $ sign)
- rendering_provider_npi: NPI number of the rendering provider
- remark_code: CARC/RARC remark/reason code (e.g., "CO-45", "PR-1", "OA-23") if the claim was adjusted or denied. For summary totals, use the check number or EFT trace number if available
- remark_reason: Text explanation for the remark, adjustment, or denial. For MIPS bonuses, include the Claim ID here if available. For summary totals, include the payer name or payment method
- claim_status: Status of the claim (e.g., "Paid", "Denied", "Adjusted", "Partially Paid"). For MIPS bonuses, use "Incentive Paid". For summary totals, use "Summary"
- claim_number: The payer's claim control number / ICN (Internal Claim Number). This is the reference number assigned by the insurance company to this specific claim. Often printed as "Claim #", "ICN", "DCN", "Claim Reference", or "Reference #" on the EOB. Use null if not visible.
- payment_date: Date the check or EFT was issued (format: YYYY-MM-DD). Usually printed on the check stub, payment summary, or in the page header as "Payment Date", "Check Date", or "Date Issued". Apply the same date to all items on the page if it appears only in the header. Use null if not visible on this page.
- payer_name: Name of the insurance company / payer issuing this payment. Look in page headers, footers, letterhead, or the "From" / "Payer" section. Apply to all items on the page. Use null if not visible.
- payer_id: Payer identifier number (if visible). Sometimes shown as "Payer ID", "Plan ID", or near the payer name. Use null if not visible.
- adjustment_amount: For adjustment or denial lines, the dollar amount of the adjustment (numeric, no $ sign). This is typically billed_amount minus allowed_amount, or the specific denied/adjusted/contractual amount shown on the line. Use null for fully paid lines or if no adjustment amount is shown.

IMPORTANT — Header Memory:
- If the rendering provider name or NPI appears in the page header but NOT on each line item, apply that provider info to ALL line items on this page.
- Similarly, if the patient name or member ID appears only in the page header, carry it down to every line item.
- If the payer name, payment date, or claim number appears in the page header, apply it to ALL items on the page.
- Look for provider names, NPIs, patient info, and payer info in page headers, footers, and section headings.

IMPORTANT — MIPS/Bonus Pages:
- Do NOT put Member IDs, NPIs, or Account IDs into the cpt_code field. If there is no CPT code, set cpt_code to "MIPS_BONUS" for incentive pages.
- Map the incentive/bonus dollar amount to paid_amount.
- If the page shows a summary of multiple claims with incentive adjustments, extract each claim as a separate line item.

IMPORTANT — Summary/Check Pages:
- If the page shows a check total, EFT total, or provider payment summary, extract ONE summary_total item per check or EFT payment.
- Set paid_amount to the total check/EFT amount.
- Set remark_code to the check number or EFT trace number (e.g., "CHK-12345" or "EFT-98765").
- Set payment_date to the check/EFT issue date if visible.
- If the page lists both individual claim lines AND a total, extract BOTH: the individual lines as "medical_service" and the total as "summary_total".
- Do NOT double-count: the summary_total represents the check total, not an additional payment.

Other instructions:
- Extract EVERY line item on the page, do not skip any
- If a field is not present, set it to null
- Return amounts as numbers without dollar signs or commas
- If the page has absolutely no extractable data (blank page, signature-only page), return: {"items": []}`;

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
    let file_name = job.file_name || null;
    if (!practice_id || !file_name) {
      const { data: docRow, error: docErr } = await supabase
        .from('eob_documents')
        .select('practice_id, file_name')
        .eq('id', job.eob_document_id)
        .single();
      if (docErr) throw new Error(`Doc lookup failed: ${docErr.message}`);
      if (!practice_id) practice_id = docRow.practice_id;
      if (!file_name) file_name = docRow.file_name || null;
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
      // 4a: Delete any existing rows for this document+page (idempotent re-runs)
      const BQ_QUERY_URL = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`;
      const deleteQuery = `DELETE FROM \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\` WHERE eob_document_id = '${job.eob_document_id}' AND page_number = ${job.page_number}`;
      try {
        const delResp = await fetch(BQ_QUERY_URL, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: deleteQuery, useLegacySql: false })
        });
        const delResult = await delResp.json();
        if (delResult.numDmlAffectedRows && parseInt(delResult.numDmlAffectedRows) > 0) {
          console.info(`[eob-worker] Cleared ${delResult.numDmlAffectedRows} existing BQ rows for doc ${job.eob_document_id} page ${job.page_number}`);
        }
      } catch (delErr: any) {
        // Non-fatal: if delete fails (e.g., streaming buffer), insert will still work
        // Worst case: duplicates, which is better than losing data
        console.warn(`[eob-worker] BQ pre-delete failed (non-fatal): ${delErr.message}`);
      }

      // 4b: Insert fresh rows
      const now = formatBQTimestamp(new Date());

      const rows = extracted.map((it: any, idx: number) => ({
        insertId: `${job.eob_document_id}_p${job.page_number}_${idx}`,  // dedup key for short-window retries
        json: {
          id: crypto.randomUUID(),
          eob_document_id: job.eob_document_id,
          practice_id: practice_id,
          page_number: job.page_number,
          file_name: file_name,
          line_type: it.line_type || 'medical_service',
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
          remark_code: it.remark_code || null,
          remark_reason: it.remark_reason || null,
          claim_status: it.claim_status || null,
          // 835-ready fields (Phase 2)
          claim_number: it.claim_number || null,
          payment_date: formatBQDate(it.payment_date),
          payer_name: it.payer_name || null,
          payer_id: it.payer_id || null,
          adjustment_amount: parseCurrency(it.adjustment_amount),
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

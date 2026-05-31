// rematch-deposits — Refreshes bank_deposits match data from live BigQuery.
//
// After a document is reprocessed, BigQuery data changes but bank_deposits.match_delta
// is a static value written at CSV-upload time. This function re-queries BQ and updates
// match_delta / match_status for any deposits whose check_number matches the new BQ data.
//
// Input:  { practice_id: string, eob_document_id?: string }
//   - practice_id is required (scopes the bank_deposits query)
//   - eob_document_id is optional; if supplied, only deposits linked to that doc are rematched
//
// Output: { updated: number, matched: number, discrepancies: number, unchanged: number, details: [...] }

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL             = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const GCP_SA_JSON_STR           = Deno.env.get('GCP_SA_JSON')!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';

// ─── GCP Auth (identical to check-exceptions / eob-worker) ───────────────────

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

async function getGoogleAccessToken(sa: any): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const header  = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now - 30,
  };
  const encodedHeader  = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(payload));
  const dataToSign = new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`);
  const pem = sa.private_key.replace(/\\n/g, '\n');
  const binaryKey = atob(pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, ''));
  const keyBuffer = new Uint8Array(binaryKey.length);
  for (let i = 0; i < binaryKey.length; i++) keyBuffer[i] = binaryKey.charCodeAt(i);
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8', keyBuffer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign'],
  );
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', cryptoKey, dataToSign);
  const jwt = `${encodedHeader}.${encodedPayload}.${base64url(new Uint8Array(signature))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`GCP Auth Failed: ${JSON.stringify(data)}`);
  return data.access_token;
}

// ─── BQ Query Helper ─────────────────────────────────────────────────────────

async function bqQuery(gToken: string, sql: string): Promise<any[]> {
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql, useLegacySql: false, timeoutMs: 25000 }),
  });
  const result = await resp.json();
  if (!resp.ok) throw new Error(`BQ query failed (${resp.status}): ${JSON.stringify(result)}`);
  if (!result.jobComplete) throw new Error('BQ query timed out — retry after streaming buffer clears');
  const schema = result.schema?.fields || [];
  return (result.rows || []).map((row: any) => {
    const obj: any = {};
    row.f.forEach((field: any, i: number) => { obj[schema[i].name] = field.v; });
    return obj;
  });
}

// ─── Main Handler ─────────────────────────────────────────────────────────────

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  const json = (body: any, status = 200) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });

  try {
    const body = await req.json();
    const { practice_id, eob_document_id } = body;

    if (!practice_id) {
      return json({ error: 'practice_id is required' }, 400);
    }

    console.info(`[rematch-deposits] Starting for practice=${practice_id}${eob_document_id ? ` doc=${eob_document_id}` : ''}`);

    // ── Step 1: Get current check totals from BigQuery ──────────────────────
    // Use eob_payment_items VIEW — it derives check_number (from summary_total.remark_code)
    // and check_total_amount (from summary_total.paid_amount) correctly.
    // GROUP BY check_number + take MAX(check_total_amount) to handle streaming-buffer
    // duplicates where old and new rows co-exist with different totals.
    const docFilter = eob_document_id
      ? `AND eob_document_id = '${eob_document_id}'`
      : '';

    // Query eob_line_items directly (inline the eob_payment_items view logic)
    // to avoid a dependency on the view that may not exist after BQ housekeeping.
    // For each document, the summary_total row (highest page_number) carries the
    // check number (remark_code) and check total (paid_amount). We join it onto
    // every non-summary row so we can GROUP BY check_number.
    const checkRows = await bqQuery(
      await getGoogleAccessToken(JSON.parse(GCP_SA_JSON_STR.trim())),
      `
        SELECT
          check_number,
          MAX(CAST(check_total_amount AS FLOAT64)) AS check_total,
          ANY_VALUE(eob_doc_id)                    AS eob_doc_id
        FROM (
          SELECT
            li.eob_document_id  AS eob_doc_id,
            st.remark_code      AS check_number,
            st.paid_amount      AS check_total_amount
          FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\` li
          LEFT JOIN (
            SELECT
              eob_document_id,
              remark_code,
              paid_amount,
              ROW_NUMBER() OVER (PARTITION BY eob_document_id ORDER BY page_number DESC) AS rn
            FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\`
            WHERE line_type = 'summary_total'
          ) st ON li.eob_document_id = st.eob_document_id AND st.rn = 1
          WHERE li.line_type    != 'summary_total'
            AND li.practice_id   = '${practice_id}'
            ${docFilter}
        )
        WHERE check_number       IS NOT NULL
          AND check_total_amount IS NOT NULL
        GROUP BY check_number
      `,
    );

    if (checkRows.length === 0) {
      console.warn(`[rematch-deposits] No check totals found in BQ for practice=${practice_id}${eob_document_id ? ` doc=${eob_document_id}` : ''}`);
      return json({
        updated: 0,
        matched: 0,
        discrepancies: 0,
        unchanged: 0,
        warning: 'No check totals found in BigQuery — document may still be processing or streaming buffer not yet cleared',
      });
    }

    // Build lookup: checkNumber → { checkTotal, docId }
    const checkMap = new Map<string, { checkTotal: number; docId: string }>();
    for (const row of checkRows) {
      if (row.check_number) {
        checkMap.set(row.check_number.trim(), {
          checkTotal: parseFloat(row.check_total) || 0,
          docId: row.eob_doc_id || '',
        });
      }
    }
    console.info(`[rematch-deposits] Loaded ${checkMap.size} check totals from BQ`);

    // ── Step 2: Fetch bank deposits from Supabase ───────────────────────────
    let depositsQuery = supabase
      .from('bank_deposits')
      .select('id, check_number, amount, match_status, match_delta, matched_eob_document_id')
      .eq('practice_id', practice_id);

    if (eob_document_id) {
      // When scoped to a doc: match either by matched_eob_document_id OR by check_number
      // (the deposit might have been unmatched previously, so matched_eob_document_id may be null)
      depositsQuery = depositsQuery.or(
        `matched_eob_document_id.eq.${eob_document_id},matched_eob_document_id.is.null`,
      );
    }

    const { data: deposits, error: depositsErr } = await depositsQuery;
    if (depositsErr) throw new Error(`Supabase deposits query failed: ${depositsErr.message}`);
    if (!deposits || deposits.length === 0) {
      return json({ updated: 0, matched: 0, discrepancies: 0, unchanged: 0, warning: 'No deposits found for this practice' });
    }

    // ── Step 3: Recompute and update changed rows ───────────────────────────
    let updated = 0, matched = 0, discrepancies = 0, unchanged = 0;
    const details: any[] = [];

    for (const dep of deposits) {
      const checkNum = dep.check_number?.trim();
      if (!checkNum) continue; // skip deposits with no check number

      const bqMatch = checkMap.get(checkNum);
      if (!bqMatch) continue; // check number not in BQ — leave as-is

      const newDelta  = Math.round((Number(dep.amount) - bqMatch.checkTotal) * 100) / 100;
      const newStatus = Math.abs(newDelta) < 0.01 ? 'matched' : 'discrepancy';
      const oldDelta  = dep.match_delta === null ? null : Number(dep.match_delta);
      const oldStatus = dep.match_status;

      // Only update if something actually changed
      const deltaChanged  = oldDelta !== newDelta;
      const statusChanged = oldStatus !== newStatus;
      if (!deltaChanged && !statusChanged) {
        unchanged++;
        continue;
      }

      const { error: updateErr } = await supabase
        .from('bank_deposits')
        .update({
          match_delta:              newDelta,
          match_status:             newStatus,
          matched_eob_document_id:  bqMatch.docId || dep.matched_eob_document_id,
        })
        .eq('id', dep.id);

      if (updateErr) {
        console.error(`[rematch-deposits] Update failed for deposit ${dep.id}: ${updateErr.message}`);
        continue;
      }

      updated++;
      if (newStatus === 'matched')     matched++;
      if (newStatus === 'discrepancy') discrepancies++;

      details.push({
        deposit_id:     dep.id,
        check_number:   checkNum,
        bank_amount:    Number(dep.amount),
        bq_check_total: bqMatch.checkTotal,
        old_delta:      oldDelta,
        new_delta:      newDelta,
        old_status:     oldStatus,
        new_status:     newStatus,
      });

      console.info(`[rematch-deposits] Updated ${checkNum}: delta ${oldDelta} → ${newDelta}, status ${oldStatus} → ${newStatus}`);
    }

    console.info(`[rematch-deposits] Done: updated=${updated} matched=${matched} discrepancies=${discrepancies} unchanged=${unchanged}`);

    return json({ updated, matched, discrepancies, unchanged, details });

  } catch (err: any) {
    console.error(`[rematch-deposits] Error: ${err.message}`);
    return json({ error: 'Rematch failed', details: err.message }, 500);
  }
});

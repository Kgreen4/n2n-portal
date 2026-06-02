// rematch-deposits — Refreshes bank_deposits match data from live BigQuery.
//
// After a document is reprocessed, BigQuery data changes but bank_deposits.match_delta
// is a static value written at CSV-upload time. This function re-queries BQ and updates
// match_delta / match_status for any deposits whose matched_eob_document_id is found in BQ.
//
// Primary path  (Path A): query eob_line_items summary_total rows by matched_eob_document_id.
//   Works even when remark_code (check_number) is NULL in BQ.
// Fallback path (Path B): derive check_number from summary_total.remark_code and match
//   deposits by check_number. Used when matched_eob_document_id is not set on the deposit.
//
// Input:  { practice_id: string, eob_document_id?: string }
//   - practice_id is required (scopes the bank_deposits query)
//   - eob_document_id is optional; if supplied, only deposits linked to that doc are rematched
//
// Output: { updated: number, matched: number, discrepancies: number, unchanged: number, details: [...] }

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL              = Deno.env.get('SUPABASE_URL')!;
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

    const gToken = await getGoogleAccessToken(JSON.parse(GCP_SA_JSON_STR.trim()));

    // ── Step 1: Fetch bank deposits from Supabase ───────────────────────────
    let depositsQuery = supabase
      .from('bank_deposits')
      .select('id, check_number, amount, match_status, match_delta, matched_eob_document_id')
      .eq('practice_id', practice_id);

    if (eob_document_id) {
      // When scoped to a doc: match either by matched_eob_document_id OR by check_number
      depositsQuery = depositsQuery.or(
        `matched_eob_document_id.eq.${eob_document_id},matched_eob_document_id.is.null`,
      );
    }

    const { data: deposits, error: depositsErr } = await depositsQuery;
    if (depositsErr) throw new Error(`Supabase deposits query failed: ${depositsErr.message}`);
    if (!deposits || deposits.length === 0) {
      return json({ updated: 0, matched: 0, discrepancies: 0, unchanged: 0, warning: 'No deposits found for this practice' });
    }

    console.info(`[rematch-deposits] Found ${deposits.length} deposits in Supabase`);

    // ── Step 2a: Path A — query BQ by matched_eob_document_id ──────────────
    // This is the PRIMARY path. Works even when remark_code (check_number) is
    // NULL in BQ summary_total rows. Uses the document ID already stored in
    // bank_deposits.matched_eob_document_id to look up the current check total.
    const matchedDocIds = [...new Set(
      deposits
        .filter(d => d.matched_eob_document_id)
        .map(d => d.matched_eob_document_id as string)
    )];

    // docCheckMap: eob_document_id → check_total
    const docCheckMap = new Map<string, number>();

    if (matchedDocIds.length > 0) {
      const inClause = matchedDocIds.map(id => `'${id}'`).join(', ');
      try {
        const docRows = await bqQuery(gToken, `
          SELECT
            eob_document_id,
            MAX(CAST(paid_amount AS FLOAT64)) AS check_total
          FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\`
          WHERE line_type = 'summary_total'
            AND eob_document_id IN (${inClause})
          GROUP BY eob_document_id
        `);
        for (const row of docRows) {
          if (row.eob_document_id && row.check_total !== null && row.check_total !== undefined) {
            docCheckMap.set(row.eob_document_id, parseFloat(row.check_total) || 0);
          }
        }
        console.info(`[rematch-deposits] Path A: loaded ${docCheckMap.size} check totals from BQ by document ID`);
      } catch (e: any) {
        console.warn(`[rematch-deposits] Path A BQ query failed: ${e.message}`);
      }
    }

    // ── Step 2b: Path B — derive check_number from summary_total.remark_code ──
    // FALLBACK path for deposits without matched_eob_document_id.
    // Groups by check_number (remark_code on summary_total rows).
    const unmatchedDeposits = deposits.filter(d => !d.matched_eob_document_id && d.check_number?.trim());
    const docFilter = eob_document_id ? `AND eob_document_id = '${eob_document_id}'` : '';

    // checkMap: check_number → { checkTotal, docId }
    const checkMap = new Map<string, { checkTotal: number; docId: string }>();

    if (unmatchedDeposits.length > 0 || docCheckMap.size === 0) {
      // Run Path B if there are unmatched deposits, or as a safety net if Path A found nothing
      try {
        const checkRows = await bqQuery(gToken, `
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
        `);

        for (const row of checkRows) {
          if (row.check_number) {
            checkMap.set(row.check_number.trim(), {
              checkTotal: parseFloat(row.check_total) || 0,
              docId: row.eob_doc_id || '',
            });
          }
        }
        console.info(`[rematch-deposits] Path B: loaded ${checkMap.size} check totals from BQ by check_number`);
      } catch (e: any) {
        console.warn(`[rematch-deposits] Path B BQ query skipped: ${e.message}`);
      }
    }

    if (docCheckMap.size === 0 && checkMap.size === 0) {
      console.warn(`[rematch-deposits] No check totals found via either path for practice=${practice_id}`);
      return json({
        updated: 0,
        matched: 0,
        discrepancies: 0,
        unchanged: 0,
        warning: 'No check totals found in BigQuery — documents may still be processing or have no summary totals',
      });
    }

    // ── Step 3: Recompute and update changed rows ───────────────────────────
    let updated = 0, matched = 0, discrepancies = 0, unchanged = 0;
    const details: any[] = [];

    for (const dep of deposits) {
      // Determine BQ check total — Path A first, then Path B fallback
      let bqCheckTotal: number | undefined;
      let resolvedDocId = dep.matched_eob_document_id || '';

      // Path A: match by matched_eob_document_id
      if (dep.matched_eob_document_id && docCheckMap.has(dep.matched_eob_document_id)) {
        bqCheckTotal = docCheckMap.get(dep.matched_eob_document_id);
      }

      // Path B: fall back to check_number
      if (bqCheckTotal === undefined) {
        const checkNum = dep.check_number?.trim();
        if (checkNum) {
          const checkMatch = checkMap.get(checkNum);
          if (checkMatch) {
            bqCheckTotal = checkMatch.checkTotal;
            resolvedDocId = checkMatch.docId || dep.matched_eob_document_id || '';
          }
        }
      }

      if (bqCheckTotal === undefined) continue; // no BQ data for this deposit

      const newDelta  = Math.round((Number(dep.amount) - bqCheckTotal) * 100) / 100;
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
          match_delta:             newDelta,
          match_status:            newStatus,
          matched_eob_document_id: resolvedDocId || dep.matched_eob_document_id,
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
        deposit_id:      dep.id,
        check_number:    dep.check_number || null,
        eob_document_id: resolvedDocId,
        bank_amount:     Number(dep.amount),
        bq_check_total:  bqCheckTotal,
        old_delta:       oldDelta,
        new_delta:       newDelta,
        old_status:      oldStatus,
        new_status:      newStatus,
        path:            dep.matched_eob_document_id && docCheckMap.has(dep.matched_eob_document_id) ? 'A' : 'B',
      });

      console.info(`[rematch-deposits] Updated deposit ${dep.id} (${dep.check_number || 'no check#'}): delta ${oldDelta} → ${newDelta}, status ${oldStatus} → ${newStatus}`);
    }

    console.info(`[rematch-deposits] Done: updated=${updated} matched=${matched} discrepancies=${discrepancies} unchanged=${unchanged}`);

    return json({ updated, matched, discrepancies, unchanged, details });

  } catch (err: any) {
    console.error(`[rematch-deposits] Error: ${err.message}`);
    return json({ error: 'Rematch failed', details: err.message }, 500);
  }
});

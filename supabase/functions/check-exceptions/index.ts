// check-exceptions — Evaluates a completed document for data quality exceptions.
// Queries BigQuery for reconciliation status, missing claim IDs, low confidence,
// and found revenue (incentive_bonus). Updates eob_documents in Postgres with
// review_status, review_reasons, and has_found_revenue.

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// BigQuery config
const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';

// ──────────────────────────────────────────────────────────────
// GCP Auth (same pattern as generate-835 / eob-worker)
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
// BQ query helper
// ──────────────────────────────────────────────────────────────
async function bqQuery(gToken: string, sql: string): Promise<any[]> {
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql, useLegacySql: false })
  });
  const result = await resp.json();
  if (!resp.ok) throw new Error(`BQ query failed: ${JSON.stringify(result)}`);
  const schema = result.schema?.fields || [];
  return (result.rows || []).map((row: any) => {
    const obj: any = {};
    row.f.forEach((field: any, i: number) => { obj[schema[i].name] = field.v; });
    return obj;
  });
}

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  try {
    const { eob_document_id } = await req.json();
    if (!eob_document_id) {
      return new Response(JSON.stringify({ error: 'eob_document_id is required' }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.info(`[check-exceptions] Evaluating document ${eob_document_id}`);

    // Get GCP token
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    const gToken = await getGoogleAccessToken(sa);

    const reasons: string[] = [];
    let hasFoundRevenue = false;

    // ── Check 1: Reconciliation status (math variance / no check total) ──
    const reconRows = await bqQuery(gToken, `
      SELECT reconciliation_status, reconciliation_delta
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_reconciliation\`
      WHERE eob_document_id = '${eob_document_id}'
      LIMIT 1
    `);

    if (reconRows.length > 0) {
      const recon = reconRows[0];
      if (recon.reconciliation_status === 'unbalanced') {
        reasons.push('math_variance');
        console.info(`[check-exceptions] math_variance: delta=${recon.reconciliation_delta}`);
      }
      if (recon.reconciliation_status === 'no_check_total') {
        reasons.push('no_check_total');
        console.info(`[check-exceptions] no_check_total`);
      }
    } else {
      // No reconciliation row means no line items at all — unusual
      console.warn(`[check-exceptions] No reconciliation row found for ${eob_document_id}`);
    }

    // ── Check 2: Missing claim numbers on medical_service lines ──
    const missingClaimRows = await bqQuery(gToken, `
      SELECT COUNT(*) as cnt
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_payment_items\`
      WHERE eob_document_id = '${eob_document_id}'
        AND line_type = 'medical_service'
        AND claim_number IS NULL
    `);

    if (missingClaimRows.length > 0 && parseInt(missingClaimRows[0].cnt) > 0) {
      reasons.push('missing_claim_id');
      console.info(`[check-exceptions] missing_claim_id: ${missingClaimRows[0].cnt} lines`);
    }

    // ── Check 3: Low confidence items (below 85) ──
    const lowConfRows = await bqQuery(gToken, `
      SELECT COUNT(*) as cnt, MIN(confidence_score) as min_score
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_payment_items\`
      WHERE eob_document_id = '${eob_document_id}'
        AND confidence_score IS NOT NULL
        AND confidence_score < 85
    `);

    if (lowConfRows.length > 0 && parseInt(lowConfRows[0].cnt) > 0) {
      reasons.push('low_confidence');
      console.info(`[check-exceptions] low_confidence: ${lowConfRows[0].cnt} items, min=${lowConfRows[0].min_score}`);
    }

    // ── Check 4: Partial failure (from Postgres) ──
    const { data: doc } = await supabase
      .from('eob_documents')
      .select('status')
      .eq('id', eob_document_id)
      .single();

    if (doc?.status === 'partial_failure') {
      reasons.push('partial_failure');
      console.info(`[check-exceptions] partial_failure`);
    }

    // ── Check 5: Found Revenue (incentive_bonus line items) ──
    const revenueRows = await bqQuery(gToken, `
      SELECT COUNT(*) as cnt, SUM(CAST(paid_amount AS FLOAT64)) as total_revenue
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_payment_items\`
      WHERE eob_document_id = '${eob_document_id}'
        AND line_type = 'incentive_bonus'
    `);

    if (revenueRows.length > 0 && parseInt(revenueRows[0].cnt) > 0) {
      hasFoundRevenue = true;
      console.info(`[check-exceptions] Found Revenue! ${revenueRows[0].cnt} bonus items, $${revenueRows[0].total_revenue}`);
    }

    // ── Update Postgres ──
    const reviewStatus = reasons.length > 0 ? 'needs_review' : 'clear';

    const { error: updateErr } = await supabase
      .from('eob_documents')
      .update({
        review_status: reviewStatus,
        review_reasons: reasons,
        has_found_revenue: hasFoundRevenue,
        updated_at: new Date().toISOString(),
      })
      .eq('id', eob_document_id);

    if (updateErr) {
      throw new Error(`Postgres update failed: ${updateErr.message}`);
    }

    const result = {
      eob_document_id,
      review_status: reviewStatus,
      review_reasons: reasons,
      has_found_revenue: hasFoundRevenue,
    };

    console.info(`[check-exceptions] Result:`, JSON.stringify(result));

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    console.error('[check-exceptions] Error:', err.message);
    return new Response(JSON.stringify({ error: 'Exception check failed', details: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
});

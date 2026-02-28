// fetch-line-items — Returns extracted line items from BigQuery for a document.
// Used by the document detail page to display items for review/editing.
// Queries the eob_payment_items view (excludes summary_total rows).

import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;

// BigQuery config
const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';

// ──────────────────────────────────────────────────────────────
// GCP Auth
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

    console.info(`[fetch-line-items] Fetching items for document ${eob_document_id}`);

    // Get GCP token
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    const gToken = await getGoogleAccessToken(sa);

    // Query eob_payment_items view — excludes summary_total rows, enriched with check info
    const items = await bqQuery(gToken, `
      SELECT
        eob_document_id,
        page_number,
        patient_name,
        member_id,
        date_of_service,
        cpt_code,
        cpt_description,
        billed_amount,
        allowed_amount,
        paid_amount,
        adjustment_amount,
        patient_responsibility,
        deductible_amount,
        coinsurance_amount,
        copay_amount,
        contractual_adjustment,
        claim_status,
        remark_code,
        remark_reason,
        rendering_provider_npi,
        line_type,
        claim_number,
        payment_date,
        payer_name,
        payer_id,
        confidence_score,
        non_covered_amount,
        remark_description,
        check_number,
        check_total_amount
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_payment_items\`
      WHERE eob_document_id = '${eob_document_id}'
      ORDER BY page_number, patient_name, date_of_service, cpt_code
    `);

    console.info(`[fetch-line-items] Returned ${items.length} items`);

    return new Response(JSON.stringify({ items, count: items.length }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    console.error('[fetch-line-items] Error:', err.message);
    return new Response(JSON.stringify({ error: 'Failed to fetch line items', details: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
});

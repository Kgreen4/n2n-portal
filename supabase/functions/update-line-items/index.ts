// update-line-items — Updates extracted line items in BigQuery for Fix-and-Post workflow.
// Accepts an array of field-level updates keyed by composite row identity
// (eob_document_id + page_number + patient_name + cpt_code + date_of_service).
// After updates, re-fires check-exceptions to re-evaluate review_status.

import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;
const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

// BigQuery config
const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';
const BQ_TABLE = 'eob_line_items';

// Allowed editable fields and their BQ types
const EDITABLE_FIELDS: Record<string, 'STRING' | 'FLOAT64' | 'INT64' | 'DATE'> = {
  paid_amount: 'FLOAT64',
  billed_amount: 'FLOAT64',
  allowed_amount: 'FLOAT64',
  adjustment_amount: 'FLOAT64',
  patient_responsibility: 'FLOAT64',
  deductible_amount: 'FLOAT64',
  coinsurance_amount: 'FLOAT64',
  copay_amount: 'FLOAT64',
  contractual_adjustment: 'FLOAT64',
  claim_number: 'STRING',
  claim_status: 'STRING',
  remark_code: 'STRING',
  remark_reason: 'STRING',
  cpt_code: 'STRING',
  cpt_description: 'STRING',
  patient_name: 'STRING',
  member_id: 'STRING',
  date_of_service: 'DATE',
  rendering_provider_npi: 'STRING',
  payer_name: 'STRING',
  payer_id: 'STRING',
  payment_date: 'DATE',
  line_type: 'STRING',
};

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
// BQ DML helper (for UPDATE statements — returns numDmlAffectedRows)
// ──────────────────────────────────────────────────────────────
async function bqDml(gToken: string, sql: string): Promise<number> {
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql, useLegacySql: false })
  });
  const result = await resp.json();
  if (!resp.ok) throw new Error(`BQ DML failed: ${JSON.stringify(result)}`);
  return parseInt(result.numDmlAffectedRows || '0');
}

// ──────────────────────────────────────────────────────────────
// Escape string for BigQuery SQL (prevent injection)
// ──────────────────────────────────────────────────────────────
function escBQ(val: string): string {
  return val.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

// ──────────────────────────────────────────────────────────────
// Build SET clause for a single update
// ──────────────────────────────────────────────────────────────
function buildSetClause(fields: Record<string, any>): string {
  const parts: string[] = [];
  for (const [field, value] of Object.entries(fields)) {
    const bqType = EDITABLE_FIELDS[field];
    if (!bqType) continue; // skip unknown fields

    if (value === null || value === undefined || value === '') {
      parts.push(`${field} = NULL`);
    } else if (bqType === 'FLOAT64' || bqType === 'INT64') {
      const num = parseFloat(String(value));
      if (isNaN(num)) throw new Error(`Invalid numeric value for ${field}: ${value}`);
      parts.push(`${field} = ${num}`);
    } else if (bqType === 'DATE') {
      // Expect YYYY-MM-DD format
      parts.push(`${field} = DATE '${escBQ(String(value))}'`);
    } else {
      parts.push(`${field} = '${escBQ(String(value))}'`);
    }
  }
  return parts.join(', ');
}

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
interface LineItemUpdate {
  // Composite key to identify the row
  eob_document_id: string;
  page_number: number;
  patient_name: string;
  cpt_code: string;
  date_of_service: string;
  // Fields to update
  fields: Record<string, any>;
}

Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  try {
    const { eob_document_id, updates } = await req.json() as {
      eob_document_id: string;
      updates: LineItemUpdate[];
    };

    if (!eob_document_id) {
      return new Response(JSON.stringify({ error: 'eob_document_id is required' }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    if (!updates || !Array.isArray(updates) || updates.length === 0) {
      return new Response(JSON.stringify({ error: 'updates array is required and must not be empty' }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.info(`[update-line-items] Processing ${updates.length} updates for document ${eob_document_id}`);

    // Get GCP token
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    const gToken = await getGoogleAccessToken(sa);

    let totalAffected = 0;
    const results: { index: number; affected: number; error?: string }[] = [];

    // Execute each update as a separate DML statement
    for (let i = 0; i < updates.length; i++) {
      const upd = updates[i];
      try {
        // Validate fields
        const unknownFields = Object.keys(upd.fields).filter(f => !EDITABLE_FIELDS[f]);
        if (unknownFields.length > 0) {
          results.push({ index: i, affected: 0, error: `Unknown fields: ${unknownFields.join(', ')}` });
          continue;
        }

        const setClause = buildSetClause(upd.fields);
        if (!setClause) {
          results.push({ index: i, affected: 0, error: 'No valid fields to update' });
          continue;
        }

        // Build WHERE clause using composite key
        const whereParts = [
          `eob_document_id = '${escBQ(eob_document_id)}'`,
          `page_number = ${parseInt(String(upd.page_number))}`,
        ];

        // patient_name and cpt_code — handle nulls
        if (upd.patient_name) {
          whereParts.push(`patient_name = '${escBQ(upd.patient_name)}'`);
        } else {
          whereParts.push(`patient_name IS NULL`);
        }

        if (upd.cpt_code) {
          whereParts.push(`cpt_code = '${escBQ(upd.cpt_code)}'`);
        } else {
          whereParts.push(`cpt_code IS NULL`);
        }

        if (upd.date_of_service) {
          whereParts.push(`date_of_service = DATE '${escBQ(upd.date_of_service)}'`);
        } else {
          whereParts.push(`date_of_service IS NULL`);
        }

        const sql = `
          UPDATE \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\`
          SET ${setClause}
          WHERE ${whereParts.join(' AND ')}
        `;

        console.info(`[update-line-items] DML #${i}: ${sql.trim().substring(0, 200)}...`);
        const affected = await bqDml(gToken, sql);
        totalAffected += affected;
        results.push({ index: i, affected });

      } catch (err) {
        console.error(`[update-line-items] DML #${i} failed:`, err.message);
        results.push({ index: i, affected: 0, error: err.message });
      }
    }

    console.info(`[update-line-items] Total rows affected: ${totalAffected}`);

    // Re-fire check-exceptions to re-evaluate review_status after edits
    fetch(`${SUPABASE_URL}/functions/v1/check-exceptions`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ eob_document_id })
    }).then(r => console.info(`[update-line-items] check-exceptions re-fired: ${r.status}`))
      .catch(err => console.warn('[update-line-items] check-exceptions fire failed:', err.message));

    return new Response(JSON.stringify({
      eob_document_id,
      total_updates: updates.length,
      total_rows_affected: totalAffected,
      results
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    console.error('[update-line-items] Error:', err.message);
    return new Response(JSON.stringify({ error: 'Update failed', details: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
});

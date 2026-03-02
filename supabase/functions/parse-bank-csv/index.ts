// parse-bank-csv — Parses a bank statement CSV, inserts deposits, and auto-matches
// against EOB check totals by check_number. Called from the Settings page.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;

const BQ_PROJECT = 'cardio-metrics-dev';
const BQ_DATASET = 'billing_audit_practice_test';

// ── GCP Auth (shared pattern) ──
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
    exp: now + 3600, iat: now - 30,
  };
  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(payload));
  const dataToSign = new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`);
  const pem = sa.private_key.replace(/\\n/g, '\n');
  const binaryKey = atob(pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, ''));
  const keyBuffer = new Uint8Array(binaryKey.length);
  for (let i = 0; i < binaryKey.length; i++) keyBuffer[i] = binaryKey.charCodeAt(i);
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8', keyBuffer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', cryptoKey, dataToSign);
  const jwt = `${encodedHeader}.${encodedPayload}.${base64url(new Uint8Array(signature))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt }),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`GCP Auth Failed: ${JSON.stringify(data)}`);
  return data.access_token;
}

async function bqQuery(gToken: string, sql: string): Promise<any[]> {
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${BQ_PROJECT}/queries`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql, useLegacySql: false }),
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

// ── CSV Parser ──
function parseCsvRows(csvText: string): Array<{ date: string; check_number: string; amount: number; description: string }> {
  const lines = csvText.trim().split(/\r?\n/);
  if (lines.length < 2) throw new Error('CSV must have a header row and at least one data row');

  // Parse header — use includes() for flexible matching (handles "Transaction Date", etc.)
  const header = lines[0].toLowerCase().split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const dateIdx = header.findIndex(h => h.includes('date') && !h.includes('balance'));
  const checkIdx = header.findIndex(h => h === 'check_number' || h === 'check_no' || h === 'check#' || h === 'check' || h === 'reference');
  const amountIdx = header.findIndex(h => h === 'amount' || h.includes('amount') || h === 'credit');
  const descIdx = header.findIndex(h => h.includes('description') || h === 'memo' || h === 'details' || h === 'payee');

  if (dateIdx === -1) throw new Error('CSV missing required "date" column. Found headers: ' + header.join(', '));
  if (amountIdx === -1) throw new Error('CSV missing required "amount" column. Found headers: ' + header.join(', '));

  const rows: Array<{ date: string; check_number: string; amount: number; description: string }> = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // CSV split: handle quoted fields with commas inside (e.g., "$1,386.79")
    const cols: string[] = [];
    let inQuotes = false;
    let current = '';
    for (const ch of line) {
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === ',' && !inQuotes) { cols.push(current.trim()); current = ''; continue; }
      current += ch;
    }
    cols.push(current.trim());

    const rawDate = cols[dateIdx] || '';
    const rawAmount = cols[amountIdx] || '';
    const rawDesc = descIdx >= 0 ? (cols[descIdx] || '') : '';

    // Skip PENDING rows
    if (rawDate.toUpperCase().includes('PENDING')) continue;

    // Parse date: accept YYYY-MM-DD, MM/DD/YYYY, M/D/YYYY
    let parsedDate = rawDate;
    const mdyMatch = rawDate.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (mdyMatch) {
      parsedDate = `${mdyMatch[3]}-${mdyMatch[1].padStart(2, '0')}-${mdyMatch[2].padStart(2, '0')}`;
    }

    // Parse amount: strip $, commas, spaces. Parens = negative: ($25) → -25
    const cleanAmount = rawAmount.replace(/[$,\s]/g, '').replace(/^\((.+)\)$/, '-$1');
    const amount = parseFloat(cleanAmount);
    if (isNaN(amount) || amount <= 0) continue; // Skip negative/zero (withdrawals, checks written)

    // Extract check/reference number from description if no dedicated column
    let checkNumber = '';
    if (checkIdx >= 0) {
      checkNumber = cols[checkIdx] || '';
    } else {
      // PNC-style: "DEPOSIT xxxxx0077" → extract trailing digits after xxxxx
      const depositRef = rawDesc.match(/DEPOSIT\s+x+(\d+)/i);
      // "CHECK 5277 xxxxx5405" → extract check number
      const checkRef = rawDesc.match(/CHECK\s+(\d+)/i);
      // Generic: any trailing number after last space
      const trailingRef = rawDesc.match(/\b(\d{4,})\s*$/);

      if (depositRef) checkNumber = depositRef[1];
      else if (checkRef) checkNumber = checkRef[1];
      else if (trailingRef) checkNumber = trailingRef[1];
    }

    rows.push({
      date: parsedDate,
      check_number: checkNumber,
      amount,
      description: rawDesc,
    });
  }

  return rows;
}

// ── Main Handler ──
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  try {
    const { practice_id, csv_content, source_file } = await req.json();
    if (!practice_id || !csv_content) {
      return new Response(JSON.stringify({ error: 'practice_id and csv_content required' }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    console.info(`[parse-bank-csv] Parsing CSV for practice ${practice_id}`);

    // 1. Parse CSV rows
    const rows = parseCsvRows(csv_content);
    console.info(`[parse-bank-csv] Parsed ${rows.length} deposit rows`);

    if (rows.length === 0) {
      return new Response(JSON.stringify({ success: true, inserted: 0, matched: 0, discrepancies: 0, unmatched: 0 }), {
        status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // 2. Get user ID from auth header
    const authHeader = req.headers.get('authorization') || '';
    const supabaseAuth = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    let uploadedBy: string | null = null;
    if (authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '');
      const { data: { user } } = await supabaseAuth.auth.getUser(token);
      uploadedBy = user?.id || null;
    }

    // 3. Get GCP token for BigQuery matching
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    const gToken = await getGoogleAccessToken(sa);

    // 4. Fetch all check totals for this practice from BigQuery
    const checkTotals = await bqQuery(gToken, `
      SELECT DISTINCT
        check_number,
        check_total_amount,
        eob_document_id
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\`
      WHERE check_number IS NOT NULL
        AND check_total_amount IS NOT NULL
        AND eob_document_id IN (
          SELECT id FROM EXTERNAL_QUERY("projects/cardio-metrics-dev/locations/us/connections/supabase-connection", "SELECT id::text FROM eob_documents WHERE practice_id = '${practice_id}'")
        )
    `).catch(() => {
      // Fallback: query without external connection (simpler approach)
      return [];
    });

    // Build a lookup map: check_number → { amount, doc_id }
    // If external query fails, try a simpler approach
    let checkMap = new Map<string, { amount: number; docId: string }>();

    if (checkTotals.length === 0) {
      // Simpler fallback: get all check totals from BQ for this practice
      const allChecks = await bqQuery(gToken, `
        SELECT
          check_number,
          MAX(CAST(check_total_amount AS FLOAT64)) as check_total,
          ANY_VALUE(eob_document_id) as eob_document_id
        FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\`
        WHERE check_number IS NOT NULL AND check_total_amount IS NOT NULL
        GROUP BY check_number
      `);
      for (const row of allChecks) {
        if (row.check_number) {
          checkMap.set(row.check_number.trim(), {
            amount: parseFloat(row.check_total) || 0,
            docId: row.eob_document_id || '',
          });
        }
      }
    } else {
      for (const row of checkTotals) {
        if (row.check_number) {
          checkMap.set(row.check_number.trim(), {
            amount: parseFloat(row.check_total_amount) || 0,
            docId: row.eob_document_id || '',
          });
        }
      }
    }

    console.info(`[parse-bank-csv] Loaded ${checkMap.size} EOB check totals for matching`);

    // 5. Insert deposits and auto-match
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    let matched = 0, discrepancies = 0, unmatched = 0;

    const depositsToInsert = rows.map(row => {
      const checkNum = row.check_number.trim();
      const eobMatch = checkNum ? checkMap.get(checkNum) : undefined;

      let match_status = 'unmatched';
      let match_delta: number | null = null;
      let matched_eob_document_id: string | null = null;

      if (eobMatch) {
        matched_eob_document_id = eobMatch.docId || null;
        match_delta = Math.round((row.amount - eobMatch.amount) * 100) / 100;
        if (Math.abs(match_delta) < 0.01) {
          match_status = 'matched';
          matched++;
        } else {
          match_status = 'discrepancy';
          discrepancies++;
        }
      } else {
        unmatched++;
      }

      return {
        practice_id,
        deposit_date: row.date,
        check_number: checkNum || null,
        amount: row.amount,
        description: row.description || null,
        matched_eob_document_id,
        match_status,
        match_delta,
        source_file: source_file || null,
        uploaded_by: uploadedBy,
      };
    });

    const { error: insertError } = await supabase
      .from('bank_deposits')
      .insert(depositsToInsert);

    if (insertError) throw new Error(`Insert failed: ${insertError.message}`);

    console.info(`[parse-bank-csv] Inserted ${rows.length} deposits: ${matched} matched, ${discrepancies} discrepancies, ${unmatched} unmatched`);

    return new Response(JSON.stringify({
      success: true,
      inserted: rows.length,
      matched,
      discrepancies,
      unmatched,
    }), {
      status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });

  } catch (err) {
    console.error('[parse-bank-csv] Error:', err.message);
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }
});

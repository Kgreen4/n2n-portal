// generate-835 — Generates an ANSI X12 835 (5010A1) Electronic Remittance Advice
// from extracted EOB data in BigQuery + practice metadata in Postgres.
//
// Supports two modes:
//   1. Single:  { eob_document_id, practice_id }       → one 835 file (one ST/SE)
//   2. Batch:   { eob_document_ids[], practice_id }     → batched 835 (multiple ST/SE in one ISA/GS)

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
// GCP Auth (same as eob-worker)
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
// BigQuery Query Helper
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
// Helpers
// ──────────────────────────────────────────────────────────────
function pad(s: string, len: number): string {
  return (s + ' '.repeat(len)).substring(0, len);
}

function formatDate8(dateStr: string | null): string {
  if (!dateStr) return formatDate8(new Date().toISOString().split('T')[0]);
  return dateStr.replace(/-/g, '');  // YYYY-MM-DD → YYYYMMDD
}

function formatTime4(): string {
  const d = new Date();
  return String(d.getHours()).padStart(2, '0') + String(d.getMinutes()).padStart(2, '0');
}

function formatAmount(n: number | null): string {
  if (n === null || n === undefined) return '0';
  return n.toFixed(2);
}

/** Map our claim_status to 835 CLP02 status codes */
function clpStatusCode(status: string | null): string {
  switch ((status || '').toLowerCase()) {
    case 'paid': return '1';           // Processed as Primary
    case 'denied': return '4';         // Denied
    case 'adjusted': return '22';      // Reversal of Previous Payment
    case 'partially paid': return '2'; // Processed as Secondary
    case 'incentive paid': return '1';
    default: return '1';
  }
}

/** Parse remark_code like "CO-45" or "45" into group + reason */
function parseRemarkCode(code: string | null): { group: string; reason: string } | null {
  if (!code) return null;
  const match = code.match(/^(CO|PR|OA|PI|CR)-?(\d+)$/i);
  if (match) return { group: match[1].toUpperCase(), reason: match[2] };
  // If just a number, default to CO (Contractual Obligation)
  if (/^\d+$/.test(code)) return { group: 'CO', reason: code };
  return null;
}

/** Split "LAST, FIRST" or "FIRST LAST" into [last, first] */
function splitName(name: string | null): [string, string] {
  if (!name) return ['UNKNOWN', 'UNKNOWN'];
  const trimmed = name.trim();
  if (trimmed.includes(',')) {
    const [last, ...rest] = trimmed.split(',');
    return [last.trim(), rest.join(',').trim() || 'UNKNOWN'];
  }
  const parts = trimmed.split(/\s+/);
  if (parts.length === 1) return [parts[0], 'UNKNOWN'];
  return [parts[parts.length - 1], parts.slice(0, -1).join(' ')];
}

/** Validate UUID format to prevent SQL injection in IN clauses */
function isValidUUID(id: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
}

// ──────────────────────────────────────────────────────────────
// Transaction Body Builder
// Builds the inner X12 segments for a single ST/SE transaction
// (BPR through CLP/SVC/CAS loops). Does NOT include ST or SE.
// ──────────────────────────────────────────────────────────────
interface TransactionData {
  practice: {
    name: string; tax_id: string; npi: string;
    address_line1?: string; address_line2?: string;
    city?: string; state?: string; zip?: string;
  };
  bqRows: any[];
  checkInfo: any;
  payerName: string;
  payerId: string;
  paymentDate: string | null;
  checkNumber: string;
  checkTotal: number;
}

function buildTransactionBody(data: TransactionData): string[] {
  const segments: string[] = [];
  const date8 = formatDate8(data.paymentDate);
  const taxIdClean = data.practice.tax_id.replace(/[^0-9]/g, '');
  const payerIdClean = data.payerId.replace(/[^0-9A-Za-z]/g, '');

  // BPR - Financial Information
  const paymentMethod = (data.checkInfo.cpt_description || '').toLowerCase().includes('eft') ? 'ACH' : 'CHK';
  const bprMethod = paymentMethod === 'ACH' ? 'ACH*CTX*CCP' : 'CHK';
  segments.push(`BPR*I*${formatAmount(data.checkTotal)}*C*${bprMethod}************${date8}~`);

  // TRN - Reassociation Trace Number
  segments.push(`TRN*1*${data.checkNumber}*1${payerIdClean}~`);

  // DTM - Production Date
  segments.push(`DTM*405*${date8}~`);

  // N1 - Payer Identification (1000A loop)
  segments.push(`N1*PR*${data.payerName.substring(0, 60)}*XV*${payerIdClean}~`);

  // N1 - Payee Identification (1000B loop)
  segments.push(`N1*PE*${data.practice.name.substring(0, 60)}*XX*${data.practice.npi}~`);
  if (data.practice.address_line1) {
    segments.push(`N3*${data.practice.address_line1}${data.practice.address_line2 ? '*' + data.practice.address_line2 : ''}~`);
  }
  if (data.practice.city && data.practice.state && data.practice.zip) {
    segments.push(`N4*${data.practice.city}*${data.practice.state}*${data.practice.zip}~`);
  }
  segments.push(`REF*TJ*${taxIdClean}~`);

  // Group line items by claim
  const claimMap = new Map<string, any[]>();
  data.bqRows.forEach((row: any) => {
    const key = row.claim_number || `${row.patient_name}_${row.member_id}`;
    if (!claimMap.has(key)) claimMap.set(key, []);
    claimMap.get(key)!.push(row);
  });

  // CLP loops - one per claim
  for (const [claimKey, lines] of claimMap) {
    const firstLine = lines[0];
    const claimNumber = firstLine.claim_number || claimKey;
    const [lastName, firstName] = splitName(firstLine.patient_name);

    // Aggregate claim-level amounts
    const totalBilled = lines.reduce((s: number, l: any) => s + (parseFloat(l.billed_amount) || 0), 0);
    const totalPaid = lines.reduce((s: number, l: any) => s + (parseFloat(l.paid_amount) || 0), 0);

    // Determine dominant status for the claim
    const statuses = lines.map((l: any) => l.claim_status);
    const claimStatus = statuses.includes('Denied') ? 'Denied' :
                        statuses.includes('Adjusted') ? 'Adjusted' :
                        statuses.includes('Partially Paid') ? 'Partially Paid' : 'Paid';

    // CLP - Claim Payment Information
    segments.push(
      `CLP*${claimNumber}*${clpStatusCode(claimStatus)}*${formatAmount(totalBilled)}*${formatAmount(totalPaid)}**MC*${firstLine.member_id || ''}~`
    );

    // NM1 - Patient Name (QC = Patient)
    segments.push(
      `NM1*QC*1*${lastName}*${firstName}****MI*${firstLine.member_id || ''}~`
    );

    // SVC loops - one per service line
    for (const line of lines) {
      const billedAmt = parseFloat(line.billed_amount) || 0;
      const paidAmt = parseFloat(line.paid_amount) || 0;

      // SVC - Service Payment Information
      if (line.cpt_code && line.cpt_code !== 'MIPS_BONUS') {
        segments.push(
          `SVC*HC:${line.cpt_code}*${formatAmount(billedAmt)}*${formatAmount(paidAmt)}~`
        );
      } else {
        segments.push(
          `SVC*HC:99999*${formatAmount(billedAmt)}*${formatAmount(paidAmt)}~`
        );
      }

      // DTM - Service Date
      if (line.date_of_service) {
        segments.push(`DTM*472*${formatDate8(line.date_of_service)}~`);
      }

      // CAS - Adjustment Segments (multi-CAS for granular financial breakdowns)
      const contractualAdj = parseFloat(line.contractual_adjustment) || 0;
      const deductible = parseFloat(line.deductible_amount) || 0;
      const coinsurance = parseFloat(line.coinsurance_amount) || 0;
      const copay = parseFloat(line.copay_amount) || 0;
      const hasGranular = contractualAdj > 0.005 || deductible > 0.005
                          || coinsurance > 0.005 || copay > 0.005;

      if (hasGranular) {
        // Emit individual CAS segments per adjustment category
        if (contractualAdj > 0.005) {
          segments.push(`CAS*CO*45*${formatAmount(contractualAdj)}~`);
        }
        if (deductible > 0.005) {
          segments.push(`CAS*PR*1*${formatAmount(deductible)}~`);
        }
        if (coinsurance > 0.005) {
          segments.push(`CAS*PR*2*${formatAmount(coinsurance)}~`);
        }
        if (copay > 0.005) {
          segments.push(`CAS*PR*3*${formatAmount(copay)}~`);
        }
        // Remainder: if total adjustment > sum of granular parts, emit catch-all
        const granularSum = contractualAdj + deductible + coinsurance + copay;
        const totalAdj = parseFloat(line.adjustment_amount) || (billedAmt - paidAmt);
        const remainder = totalAdj - granularSum;
        if (remainder > 0.005) {
          const remark = parseRemarkCode(line.remark_code);
          const group = remark?.group || 'OA';
          const reason = remark?.reason || '23';
          segments.push(`CAS*${group}*${reason}*${formatAmount(remainder)}~`);
        }
      } else {
        // Legacy fallback: single CAS for older data without granular fields
        const adjAmount = parseFloat(line.adjustment_amount) || (billedAmt - paidAmt);
        if (adjAmount > 0.005) {
          const remark = parseRemarkCode(line.remark_code);
          const group = remark?.group || 'CO';
          const reason = remark?.reason || '45';
          segments.push(`CAS*${group}*${reason}*${formatAmount(adjAmount)}~`);
        }
      }

      // AMT - Allowed Amount
      if (line.allowed_amount) {
        segments.push(`AMT*B6*${formatAmount(parseFloat(line.allowed_amount))}~`);
      }
    }
  }

  return segments;
}

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  try {
    const body = await req.json();
    const { practice_id } = body;

    // Determine mode: single or batch
    const isBatch = Array.isArray(body.eob_document_ids) && body.eob_document_ids.length > 0;
    const docIds: string[] = isBatch
      ? body.eob_document_ids
      : body.eob_document_id
        ? [body.eob_document_id]
        : [];

    if (docIds.length === 0) {
      return new Response(JSON.stringify({ error: 'eob_document_id or eob_document_ids is required' }), {
        status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Validate UUIDs to prevent SQL injection in IN clause
    for (const id of docIds) {
      if (!isValidUUID(id)) {
        return new Response(JSON.stringify({ error: `Invalid document ID: ${id}` }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }
    }

    // 1. Fetch practice metadata from Postgres
    const { data: practice, error: practiceErr } = await supabase
      .from('practices')
      .select('name, tax_id, npi, address_line1, address_line2, city, state, zip')
      .eq('id', practice_id)
      .single();

    if (practiceErr || !practice) {
      return new Response(JSON.stringify({ error: 'Practice not found', details: practiceErr?.message }), {
        status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    if (!practice.tax_id || !practice.npi) {
      return new Response(JSON.stringify({
        error: 'Practice profile incomplete',
        message: 'Tax ID and NPI are required for 835 generation. Please update your practice settings.'
      }), { status: 422, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
    }

    // 2. Auth to GCP (once for entire batch)
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    const gToken = await getGoogleAccessToken(sa);

    // 3. Batch-optimized BigQuery queries — fetch ALL docs in one shot
    const idList = docIds.map(id => `'${id}'`).join(', ');

    const allItems = await bqQuery(gToken, `
      SELECT * FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_payment_items\`
      WHERE eob_document_id IN (${idList})
      ORDER BY eob_document_id, page_number, patient_name, claim_number
    `);

    const allSummaries = await bqQuery(gToken, `
      SELECT eob_document_id, remark_code, paid_amount, cpt_description, payment_date, payer_name, payer_id
      FROM \`${BQ_PROJECT}.${BQ_DATASET}.eob_line_items\`
      WHERE eob_document_id IN (${idList}) AND line_type = 'summary_total'
      ORDER BY eob_document_id, page_number DESC
    `);

    // Group items and summaries by document ID
    const itemsByDoc = new Map<string, any[]>();
    for (const row of allItems) {
      const id = row.eob_document_id;
      if (!itemsByDoc.has(id)) itemsByDoc.set(id, []);
      itemsByDoc.get(id)!.push(row);
    }

    // For summaries, take the first one per doc (ordered by page_number DESC)
    const summaryByDoc = new Map<string, any>();
    for (const row of allSummaries) {
      const id = row.eob_document_id;
      if (!summaryByDoc.has(id)) summaryByDoc.set(id, row);
    }

    // 4. Build transaction sets — one ST/SE per document
    const allTransactionSegments: string[] = [];
    let transactionCount = 0;
    let totalClaimCount = 0;
    let firstPayerIdClean = '999999999';

    // Per-document summary stats for Export History dashboard
    const docStats = new Map<string, { total_paid: number; patient_resp: number; claim_count: number }>();

    for (const docId of docIds) {
      const bqRows = itemsByDoc.get(docId) || [];
      if (bqRows.length === 0) {
        // In batch mode, skip empty docs. In single mode, error out.
        if (!isBatch) {
          return new Response(JSON.stringify({
            error: 'No line items found',
            message: 'This document has no extracted payment items to generate an 835 file.'
          }), { status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
        }
        console.info(`[generate-835] Skipping ${docId} — no line items`);
        continue;
      }

      const checkInfo = summaryByDoc.get(docId) || {};
      const checkNumber = checkInfo.remark_code || 'UNKNOWN';
      const checkTotal = parseFloat(checkInfo.paid_amount || '0');
      const paymentDate = checkInfo.payment_date || bqRows[0]?.payment_date || null;
      const payerName = checkInfo.payer_name || bqRows[0]?.payer_name || 'UNKNOWN PAYER';
      const payerId = checkInfo.payer_id || bqRows[0]?.payer_id || '999999999';

      // Compute per-doc summary stats for Export History
      const docTotalPaid = bqRows.reduce((s: number, r: any) => s + (parseFloat(r.paid_amount) || 0), 0);
      const docPatientResp = bqRows.reduce((s: number, r: any) => s + (parseFloat(r.patient_responsibility) || 0), 0);
      const claimKeys = new Set(bqRows.map((r: any) => r.claim_number || `${r.patient_name}_${r.member_id}`));
      docStats.set(docId, { total_paid: docTotalPaid, patient_resp: docPatientResp, claim_count: claimKeys.size });

      // Capture first payer ID for ISA/GS envelope
      if (transactionCount === 0) {
        firstPayerIdClean = payerId.replace(/[^0-9A-Za-z]/g, '');
      }

      // Build inner segments (BPR through CLP/SVC/CAS)
      const bodySegments = buildTransactionBody({
        practice, bqRows, checkInfo,
        payerName, payerId, paymentDate, checkNumber, checkTotal,
      });

      // Wrap in ST/SE
      transactionCount++;
      const stNumber = String(transactionCount).padStart(4, '0');
      const seCount = bodySegments.length + 2; // +2 for ST and SE themselves
      allTransactionSegments.push(`ST*835*${stNumber}*005010X221A1~`);
      allTransactionSegments.push(...bodySegments);
      allTransactionSegments.push(`SE*${seCount}*${stNumber}~`);
    }

    if (transactionCount === 0) {
      return new Response(JSON.stringify({
        error: 'No exportable data',
        message: 'None of the selected documents have extracted payment items.'
      }), { status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
    }

    // 5. Wrap in ISA/GS/GE/IEA envelope
    const controlNumber = String(Date.now()).slice(-9).padStart(9, '0');
    const groupControl = String(Date.now()).slice(-4);
    const now = new Date();
    const envDate8 = formatDate8(now.toISOString().split('T')[0]);
    const time4 = formatTime4();
    const taxIdClean = practice.tax_id.replace(/[^0-9]/g, '');

    const envelope: string[] = [];

    // ISA - Interchange Control Header
    envelope.push(
      `ISA*00*${pad('', 10)}*00*${pad('', 10)}*ZZ*${pad(firstPayerIdClean, 15)}*ZZ*${pad(taxIdClean, 15)}*${envDate8.substring(2)}*${time4}*^*00501*${controlNumber}*0*P*:~`
    );

    // GS - Functional Group Header
    envelope.push(
      `GS*HP*${firstPayerIdClean}*${taxIdClean}*${envDate8}*${time4}*${groupControl}*X*005010X221A1~`
    );

    // All ST/SE transaction sets
    envelope.push(...allTransactionSegments);

    // GE - Functional Group Trailer (number of transaction sets)
    envelope.push(`GE*${transactionCount}*${groupControl}~`);

    // IEA - Interchange Control Trailer
    envelope.push(`IEA*1*${controlNumber}~`);

    const x12Content = envelope.join('\n');

    // 6. Build filename
    let fileName: string;
    if (isBatch) {
      const dateStr = now.toISOString().split('T')[0];
      fileName = `batch-835-${dateStr}-${transactionCount}docs.835`;
    } else {
      const docResult = await supabase
        .from('eob_documents')
        .select('file_name')
        .eq('id', docIds[0])
        .single();
      const baseName = (docResult.data?.file_name || docIds[0]).replace(/\.pdf$/i, '');
      fileName = `${baseName}.835`;
    }

    // 7. Stamp export state + summary stats on each exported document
    const batchId = crypto.randomUUID();
    const exportedAt = new Date().toISOString();
    const stampResults = await Promise.all(
      docIds.filter(id => docStats.has(id)).map(docId => {
        const stats = docStats.get(docId)!;
        return supabase
          .from('eob_documents')
          .update({
            last_exported_at: exportedAt,
            export_batch_id: batchId,
            export_total_paid: stats.total_paid,
            export_total_patient_resp: stats.patient_resp,
            export_claim_count: stats.claim_count,
          })
          .eq('id', docId);
      })
    );
    const stampErrors = stampResults.filter(r => r.error);
    if (stampErrors.length > 0) {
      console.warn('[generate-835] Failed to stamp some docs:', stampErrors.map(r => r.error!.message));
      // Non-fatal: still return the 835 file even if stamping fails
    }

    console.info(`[generate-835] Generated ${isBatch ? 'batch' : 'single'}: ${transactionCount} transactions, ${envelope.length} segments, batchId=${batchId}`);

    return new Response(x12Content, {
      status: 200,
      headers: {
        'Content-Type': 'text/plain',
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'X-835-Segments': String(envelope.length),
        'X-835-Transactions': String(transactionCount),
        'X-835-Batch-Id': batchId,
        ...corsHeaders,
      }
    });

  } catch (err) {
    console.error('[generate-835] Error:', err.message);
    return new Response(JSON.stringify({ error: 'Failed to generate 835', details: err.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
});

// eob-worker.js — Per-page EOB extraction worker
// Downloads a single PDF page from Supabase Storage, calls Gemini 2.0 Flash
// for extraction, and inserts structured line items into BigQuery.

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

// ── Hybrid PDF Mode: unpdf for digital-native text extraction ─────────────────
// unpdf is edge-compatible and works without canvas or fs.
// We import dynamically at module load so a failure never crashes the worker —
// pages fall back to base64-PDF vision mode automatically.
// See: https://github.com/unjs/unpdf
type PdfExtractFn = (buf: Uint8Array, opts?: { mergePages?: boolean }) => Promise<{ totalPages: number; text: string | string[] }>;
let pdfExtractText: PdfExtractFn | null = null;
try {
  const mod = await import('npm:unpdf@0.11.0');
  pdfExtractText = mod.extractText ?? null;
  if (pdfExtractText) {
    console.info('[eob-worker] unpdf loaded — hybrid PDF text extraction enabled');
  }
} catch (_importErr) {
  console.warn('[eob-worker] unpdf not available — all pages will use vision mode');
}
// Threshold: pages with fewer "meaningful" characters fall back to vision mode.
// Typical digital-native EOB page yields 500-2000 chars; scanned yields <50.
const DIGITAL_NATIVE_MIN_CHARS = 150;
// Numeric density guard: if >35% of meaningful chars are digits, the page is a
// financial claim grid (amounts, DOS dates, claim IDs packed in columns). unpdf
// loses the column alignment, producing a number stream Gemini can't parse.
// Force VISION mode so Gemini sees the actual visual table structure.
const FINANCIAL_GRID_DIGIT_RATIO = 0.35;

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
  // Already in YYYY-MM-DD format — validate components before passing through
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [year, month, day] = str.split('-').map(Number);
    if (month < 1 || month > 12 || day < 1 || day > 31 || year < 1900 || year > 2100) {
      return null; // Malformed date (e.g., month=25) — don't pass garbage to BigQuery
    }
    return str;
  }
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
// Post-processing enrichment: deterministic fallbacks for fields
// Gemini may miss or inconsistently populate.
// ──────────────────────────────────────────────────────────────
const REMARK_CODE_DESCRIPTIONS: Record<string, string> = {
  // Common CARC/RARC codes
  '935': 'Payment adjusted based on patient payment option/election',
  'CO-4': 'The procedure code is inconsistent with the modifier used',
  'CO-45': 'Charge exceeds fee schedule/maximum allowable',
  'CO-97': 'Benefit included in payment for another service',
  'PR-1': 'Deductible amount',
  'PR-2': 'Coinsurance amount',
  'PR-3': 'Copay amount',
  'OA-23': 'Impact of prior payer adjudication',
  // BCBS-style group codes (with and without suffix)
  'B19-PR': 'Non-covered charge(s) — patient responsibility',
  'B19': 'Non-covered charge(s)',
  'PXN-PR': 'Payment adjusted based on plan provisions',
  'PXN': 'Payment adjusted based on plan provisions',
  'OCR-OA': 'Other carrier coinsurance adjustment',
  'OCR': 'Other carrier coinsurance adjustment',
  // Additional common codes
  'CO-16': 'Claim/service lacks information needed for adjudication',
  'CO-18': 'Exact duplicate claim/service',
  'CO-50': 'Non-covered service — not deemed a medical necessity',
  'PR-96': 'Non-covered charge(s)',
  'OA-18': 'Exact duplicate claim/service',
  'CO-253': 'Sequestration — reduction in federal spending',
};

function enrichExtractedItems(items: any[]): any[] {
  return items.map(item => {
    // ── Multi-code remark description enrichment ──
    // Handles single codes ("B19-PR") and multi-codes ("B19-PR, PXN-PR")
    if (item.remark_code && (!item.remark_description || item.remark_description === '-' || item.remark_description === null)) {
      const rawCodes = (item.remark_code || '').trim();
      // Split on comma, slash, or semicolon separators
      const codes = rawCodes.split(/[,;\/]+/).map((c: string) => c.trim()).filter(Boolean);
      const descriptions: string[] = [];

      for (const code of codes) {
        // Try exact match first, then try without suffix (-PR, -OA, -CO)
        const desc = REMARK_CODE_DESCRIPTIONS[code]
          || REMARK_CODE_DESCRIPTIONS[code.replace(/-(PR|OA|CO)$/i, '')]
          || null;
        if (desc) descriptions.push(desc);
      }

      if (descriptions.length > 0) {
        item.remark_description = descriptions.join(' | ');
      }
    }

    // ── Infer claim_status if missing on non-summary lines ──
    if (item.line_type !== 'summary_total' && (!item.claim_status || item.claim_status === '-' || item.claim_status === null)) {
      const paid = parseFloat(item.paid_amount) || 0;
      const allowed = parseFloat(item.allowed_amount) || 0;
      if (paid <= 0) {
        item.claim_status = 'Denied';
      } else if (allowed > 0 && paid < allowed) {
        item.claim_status = 'Partially Paid';
      } else if (paid > 0) {
        item.claim_status = 'Paid';
      }
    }

    return item;
  });
}

// ──────────────────────────────────────────────────────────────
// Deduplication: merge rows extracted twice (summary + detail)
// ──────────────────────────────────────────────────────────────
function deduplicateItems(items: any[]): any[] {
  if (items.length <= 1) return items;

  // Build composite key for each item (excludes claim_number since subtotal
  // rows lack it, causing false key mismatches with their detail counterparts)
  const keyOf = (it: any) => [
    (it.patient_name || '').trim().toUpperCase(),
    (it.cpt_code || '').trim().toUpperCase(),
    (it.date_of_service || '').trim(),
    String(parseCurrency(it.paid_amount) ?? ''),
  ].join('|');

  // Count non-null fields as a quality score
  const quality = (it: any) => {
    let score = 0;
    const fields = [
      'billed_amount', 'allowed_amount', 'paid_amount', 'contractual_adjustment',
      'deductible_amount', 'coinsurance_amount', 'copay_amount', 'non_covered_amount',
      'patient_responsibility', 'adjustment_amount', 'remark_code', 'remark_description',
      'claim_number', 'member_id', 'cpt_description', 'rendering_provider_npi',
      'payer_name', 'payment_date',
    ];
    for (const f of fields) {
      if (it[f] !== null && it[f] !== undefined && it[f] !== '') score++;
    }
    // Bonus for higher confidence
    score += (parseInt(it.confidence_score) || 0) / 100;
    return score;
  };

  // Group by composite key, keep the highest-quality row per group
  const groups = new Map<string, any>();
  for (const item of items) {
    // Deduplicate summary_total rows by paid_amount — Gemini sometimes creates
    // multiple rows for the same check (one with the correct insurance payer + check#,
    // and a spurious duplicate without a check# or using the practice name as payer).
    // Group by normalized paid_amount; keep the highest-quality row (which will be the
    // one with a non-null remark_code / check number, scored higher by quality()).
    if (item.line_type === 'summary_total') {
      // Key on BOTH amount AND check/EFT number (remark_code) so that two checks
      // in the same PDF with the same dollar amount are NOT collapsed into one row.
      // A bare amount-only key caused multi-check PDFs with equal amounts to merge,
      // losing a check entry and widening the reconciliation gap artificially.
      const summaryAmt = String(parseCurrency(item.paid_amount) ?? item.paid_amount ?? '__noamt');
      const summaryChk = String(item.remark_code ?? '__nochk').trim();
      const summaryKey = `__summary_${summaryAmt}_${summaryChk}`;
      const existing = groups.get(summaryKey);
      if (!existing) {
        groups.set(summaryKey, item);
      } else {
        const [winner, donor] = quality(item) >= quality(existing) ? [item, existing] : [existing, item];
        const merged = { ...winner };
        for (const [field, val] of Object.entries(donor)) {
          if ((merged[field] === null || merged[field] === undefined || merged[field] === '') &&
              val !== null && val !== undefined && val !== '') {
            merged[field] = val;
          }
        }
        const winnerConf = parseInt(winner.confidence_score) || 0;
        const donorConf = parseInt(donor.confidence_score) || 0;
        merged.confidence_score = Math.max(winnerConf, donorConf);
        groups.set(summaryKey, merged);
      }
      continue;
    }

    const k = keyOf(item);
    if (!k || k === '||||') {
      // No meaningful key — keep as-is
      groups.set(`__nokey_${crypto.randomUUID()}`, item);
      continue;
    }

    const existing = groups.get(k);
    if (!existing) {
      groups.set(k, item);
    } else {
      // Merge: start with the higher-quality row, fill in blanks from the other
      const [winner, donor] = quality(item) >= quality(existing) ? [item, existing] : [existing, item];
      const merged = { ...winner };
      for (const [field, val] of Object.entries(donor)) {
        if ((merged[field] === null || merged[field] === undefined || merged[field] === '') &&
            val !== null && val !== undefined && val !== '') {
          merged[field] = val;
        }
      }
      // Take the higher confidence score
      const winnerConf = parseInt(winner.confidence_score) || 0;
      const donorConf = parseInt(donor.confidence_score) || 0;
      merged.confidence_score = Math.max(winnerConf, donorConf);
      groups.set(k, merged);
    }
  }

  return Array.from(groups.values());
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
- remark_code: ALL CARC/RARC remark/reason codes for this line, comma-separated if multiple (e.g., "CO-45", "PR-1, OA-23", "B19-PR, PXN-PR"). Extract EVERY code shown for the line, not just the first one. Common code prefixes: CO = Contractual Obligation, PR = Patient Responsibility, OA = Other Adjustment. If a code appears with a group prefix (e.g., "B19-PR" means group B19 with PR responsibility), keep the full code as printed. For summary totals, use the check number or EFT trace number if available
- remark_reason: Text explanation for the remark, adjustment, or denial. For MIPS bonuses, include the Claim ID here if available. For summary totals, include the payer name or payment method
- claim_status: Status of the claim. Look for explicit labels like "Paid", "Denied", "Adjusted", "Partially Paid", "Processed", or similar text. If no explicit status text is printed on the EOB, INFER the status from the financial fields: if paid_amount > 0 and paid_amount >= allowed_amount use "Paid"; if paid_amount > 0 but paid_amount < allowed_amount use "Partially Paid"; if paid_amount = 0 or null use "Denied"; if adjustment_amount > 0 and paid_amount > 0 use "Adjusted". For MIPS bonuses, use "Incentive Paid". For summary totals, use "Summary". NEVER return null for claim_status on medical_service lines — always infer if not explicitly shown.
- claim_number: The payer's claim control number / ICN (Internal Claim Number). This is the reference number assigned by the insurance company to this specific claim. Often printed as "Claim #", "ICN", "DCN", "Claim Reference", or "Reference #" on the EOB. Use null if not visible.
- payment_date: Date the check or EFT was issued (format: YYYY-MM-DD). Usually printed on the check stub, payment summary, or in the page header as "Payment Date", "Check Date", or "Date Issued". Apply the same date to all items on the page if it appears only in the header. Use null if not visible on this page.
- payer_name: Name of the insurance company / payer issuing this payment. Look in page headers, footers, letterhead, or the "From" / "Payer" section. Apply to all items on the page. Use null if not visible. CRITICAL: payer_name is ALWAYS the insurance company (e.g., "BlueCross BlueShield of Arizona", "Aetna", "UnitedHealthcare"). It is NEVER the medical practice, physician group, clinic, or hospital that is receiving the payment. If the document shows a "Pay to:", "Payee:", "Remit to:", or "Provider:" field with the practice/clinic name, that is the check RECIPIENT — ignore it for payer_name.
- payer_id: Payer identifier number (if visible). Sometimes shown as "Payer ID", "Plan ID", or near the payer name. Use null if not visible.
- adjustment_amount: For adjustment or denial lines, the TOTAL dollar amount of the adjustment (numeric, no $ sign). This is typically billed_amount minus paid_amount, or the specific denied/adjusted/contractual amount shown on the line. Use null for fully paid lines or if no adjustment amount is shown.
- deductible_amount: The portion of patient responsibility attributed to the deductible (numeric, no $ sign). On many EOBs this appears in a "Deductible" or "Ded" column, or within a combined "Not Covered Ded-Coin-Inst" breakdown. Use null if not broken out separately on the EOB.
- coinsurance_amount: The portion of patient responsibility attributed to coinsurance (numeric, no $ sign). Often shown as "Coinsurance", "Coins", "Co-Ins" column, or within the combined "Not Covered Ded-Coin-Inst" breakdown. Use null if not broken out separately.
- copay_amount: The portion of patient responsibility attributed to the copay (numeric, no $ sign). Often shown as "Copay" or "Co-Pay" column. Use null if not broken out separately.
- contractual_adjustment: The contractual write-off amount — typically billed_amount minus allowed_amount, representing the amount the provider agreed to waive per their payer contract (numeric, no $ sign). Usually labeled "Above Allowed Amount", "Contractual", or the CO-45 amount on the EOB. If not explicitly shown but billed_amount and allowed_amount are both present, calculate as billed_amount minus allowed_amount. Use null if neither value is present.
- non_covered_amount: Amount not covered by the plan (numeric, no $ sign). Often shown as "Non-Cvrd Amount", "Non-Covered", "NCov", or "Not Covered Amount" on the EOB. This is DISTINCT from the contractual adjustment — it represents the portion of the charge that the plan does not cover beyond the contractual write-off. Use null if not shown.
- remark_description: Human-readable description(s) of ALL remark/reason codes for this line. If the EOB prints a legend, footnote, or code explanation section (often at the bottom of the page), use those descriptions verbatim. If the line has multiple codes, provide all descriptions separated by " | " (e.g., "Non-covered charge(s) | Payment adjusted based on plan provisions"). If the EOB does not print descriptions but shows codes, provide the standard CARC/RARC description. Common codes: CO-4 = "The procedure code is inconsistent with the modifier used", CO-45 = "Charge exceeds fee schedule/maximum allowable", CO-97 = "The benefit for this service is included in the payment/allowance for another service", PR-1 = "Deductible amount", PR-2 = "Coinsurance amount", PR-3 = "Copay amount", OA-23 = "The impact of prior payer(s) adjudication", 935 = "Payment adjusted based on patient payment option/election", B19-PR = "Non-covered charge(s) — patient responsibility", PXN-PR = "Payment adjusted based on plan provisions", OCR-OA = "Other carrier coinsurance adjustment". Use null if no remark code is present.
- confidence_score: Your confidence (0-100) that this line item was extracted correctly. 100 = all fields clearly printed and unambiguous. 80-99 = most fields clear, minor ambiguity on one field. 50-79 = some fields guessed or partially visible. Below 50 = significant uncertainty, OCR artifacts, or fields inferred from context. Consider: text clarity, field alignment on the page, whether amounts are clearly tied to the correct patient/CPT line.

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
- If the page shows a check total, EFT total, or provider payment summary, extract EXACTLY ONE summary_total item per check or EFT payment. Do NOT create multiple summary_total rows for the same check.
- Set paid_amount to the total check/EFT amount.
- Set remark_code to the check number or EFT trace number (e.g., "CHK-12345" or "EFT-98765"). This is the most important field on the summary page — look carefully for it.
- Set payer_name to the insurance company name (e.g., "BlueCross BlueShield of Arizona"). NEVER use the medical practice or provider name as payer_name, even if the check is made out to the practice. The "Pay to" / "Payee" / "Remit to" field shows who is RECEIVING the check — do not create a separate summary_total row for the payee.
- Set payment_date to the check/EFT issue date if visible.
- If the page lists both individual claim lines AND a total, extract BOTH: the individual lines as "medical_service" and the total as "summary_total".
- Do NOT double-count: the summary_total represents the check total, not an additional payment.

IMPORTANT — Multi-Check Sections (single PDF containing multiple checks or EFTs):
- A single EOB PDF may contain multiple distinct check or EFT payment sections. Each section begins with its own check number, check date, and check amount, followed by the claim lines it covers.
- When extracting medical_service claim lines, tag EACH line with the check_number (remark_code) from the section header that immediately precedes it — NOT from the overall document header or from a different section.
- The active check_number for any claim line is determined by the most recent check/EFT section header appearing ABOVE that line in the document. When you encounter a new section header (new check number), update the active check_number for all subsequent lines until the next section header.
- Never inherit a check_number from a different section into another section's claim lines, even if they appear on the same page.
- For summary_total items: create exactly ONE summary_total row per distinct check or EFT. Use that check's unique number as remark_code and that check's total as paid_amount. Do not create a single summary_total for the grand total across all checks unless there is no per-check breakdown.
- Example: if page 2 has "Check #0231499940 — $71.20" followed by claims A and B, then page 6 has "Check #0231499945 — $97.56" followed by claim C, then claim C's remark_code must be "0231499945" and the summary_total for claim C's section must also use "0231499945" — not "0231499940".

IMPORTANT — Subtotal / Per-Claim Totals (DO NOT extract):
- Many EOBs print a subtotal box below individual claim lines showing "Benefits Paid", "Total Paid", "Claim Total", or a bare dollar amount. These subtotal boxes merely restate the paid_amount from the detail line above. Do NOT extract these as separate items.
- If a row has the SAME paid_amount as a detail line already extracted AND lacks its own unique CPT/HCPCS code or unique date_of_service, it is a subtotal — skip it entirely.
- The summary_total line_type is ONLY for the overall check or EFT payment total (the amount of the entire check), NOT for per-claim subtotals.
- When in doubt: if a box says "Benefits Paid" or "Total Paid" directly beneath a single claim's detail lines, and the amount matches, do NOT create a new item for it.

IMPORTANT — Financial Breakdown:
- patient_responsibility should equal deductible_amount + coinsurance_amount + copay_amount (when all three are present).
- contractual_adjustment is ONLY the contractual/write-off portion (CO-45), NOT total adjustments. adjustment_amount is the TOTAL of all adjustments combined.
- If the EOB only shows a lump "patient responsibility" without breaking it into deductible/coinsurance/copay, set patient_responsibility to that amount and leave deductible_amount, coinsurance_amount, and copay_amount as null.
- For BCBS-style EOBs: "Above Allow Amt" maps to contractual_adjustment, "Not Covered Ded-Coin-Inst" is the sum of deductible + coinsurance amounts, "Patient Resp" maps to patient_responsibility.

IMPORTANT — Legend / Footnote Code Explanations:
- Many EOBs print a legend, footnote, or "Code Explanation" section at the bottom of the page that defines the remark codes used on that page (e.g., "B19-PR: Non-covered charge(s)", "PXN-PR: Payment adjusted based on plan provisions").
- ALWAYS check the bottom of the page for these explanations and use them to fill remark_description for EVERY line item that references those codes.
- If a code like "B19-PR" has a suffix indicating responsibility type (PR = Patient Responsibility, OA = Other Adjustment, CO = Contractual Obligation), this tells you which financial column the associated amount belongs to:
  • PR suffix → the amount relates to patient_responsibility
  • OA suffix → the amount relates to coinsurance_amount or adjustment_amount
  • CO suffix → the amount relates to contractual_adjustment
- When you see a "Patient Resp" or "Patient Responsibility" column on the EOB, map that value to the patient_responsibility field.

IMPORTANT — MCO / Managed Care Remittance Formats (Mercy Care, Aetna, UnitedHealthcare, Humana, etc.):
Many Managed Care Organizations use a compact grid-style remittance layout that differs significantly from standard BCBS-style EOBs. Follow these rules when you encounter MCO-style documents:

1. GRID-STYLE CLAIM TABLES: MCO remittances often pack multiple claims into a dense columnar grid. Each row in the grid is a separate claim and must be extracted as a separate medical_service item. Common column headers include: "Patient Name", "Claim Number", "Date of Service", "DOS", "Proc", "Billed", "Allowed", "Paid", "Patient Resp", "Adj Reason", "Remark". Map each column to the appropriate field. Do NOT skip rows because they appear compressed — extract every claim row.

2. TWO TYPES OF MCO PAGES — CLAIM-DETAIL vs. CHECK STUB:
   MCO documents contain two distinct page types for each physical payment. You MUST distinguish them:

   TYPE A — CLAIM-DETAIL PAGE: Contains a grid/table of individual medical service rows for different patients or claims (multiple rows with patient names, CPT codes, dates of service, billed/allowed/paid amounts per claim line). These pages show the breakdown of HOW the check total was calculated. Rule: Extract ONLY medical_service items from these pages. Do NOT emit a summary_total row, even if an EFT trace number or check number appears somewhere on the page as a header or reference.

   TYPE B — CHECK STUB / COVER PAGE: Contains only a payment summary — a check or EFT number, a total payment amount, a payment date, and possibly a list of patient names with their per-patient subtotals — but NO CPT codes and NO individual service-level claim rows. Rule: Extract EXACTLY ONE summary_total item from these pages. Set remark_code = the official check or EFT number. Set paid_amount = the total check amount (NOT a per-patient subtotal). Set payment_date = the check date.

   NEVER emit a summary_total on a Type A page. The authoritative check total comes from the Type B check stub page only.

3. EFT TRACE NUMBERS ON CLAIM-DETAIL PAGES: A Type A claim-detail page may display an EFT trace number (typically a 7-digit number, e.g., "7901273") at the top as a reference header. This trace number is NOT a check — it is a reference that links the claim rows on this page to their associated payment. Do NOT extract this trace number as a summary_total. Extract only the medical_service rows below it.

4. DEDICATED CHECK STUB PAGES (Type B): When a page shows ONLY payment totals with no CPT codes or individual claim service rows, this is the authoritative check stub. Extract ONE summary_total item. If the page shows both a "Check #" (or official check number) and an "EFT Trace #" for the same payment, use the Check # as remark_code. If only a trace # is present, use it with an "EFT-" prefix.

5. PER-PATIENT SUBTOTALS ON CHECK STUB PAGES: A Type B check stub page may list each patient's name alongside their share of the check total. These per-patient amounts are subtotals — do NOT extract them individually as summary_total items. Extract EXACTLY ONE summary_total with the full check total amount.

6. MCO ADJUSTMENT CODES: MCO remittances may use adjustment reason codes formatted as plain numbers (e.g., "45", "97", "1") without the standard "CO-" / "PR-" prefix. These are still CARC codes. When you see bare numeric codes like "45", "97", "4", "16", "18", "22", treat them as their CARC equivalents (CO-45, CO-97, CO-4, CO-16, CO-18, CO-22) and include them in remark_code. Map their standard descriptions to remark_description.

7. PROVIDER GROUPINGS: Some MCO EOBs group claims by rendering provider or by provider NPI, with the provider name appearing as a section header above a block of claim rows. Apply that provider name and NPI to all claim rows in that section — treat it like a page header for NPI/provider assignment purposes (same "Header Memory" rule above).

8. MERCY CARE / ARIZONA-SPECIFIC MCO: Mercy Care remittances may show the member's Mercy Care ID (a long numeric string starting with the state prefix) in a "Member ID" column. Use that as member_id. The "Auth #" or "Auth Number" column is NOT the claim_number — that is a prior authorization number. Use the "Claim #" or "Reference #" column for claim_number.

Other instructions:
- Extract EVERY line item on the page, do not skip any
- If a field is not present, set it to null
- Return amounts as numbers without dollar signs or commas
- If the page has absolutely no extractable data (blank page, signature-only page), return: {"items": []}`;


// ──────────────────────────────────────────────────────────────
// Main Worker Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);
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
    // Store raw bytes for dual use: text extraction attempt + base64 vision fallback
    const pdfBytes = new Uint8Array(await fileBlob.arrayBuffer());
    const base64PDF = uint8ToBase64(pdfBytes);   // always computed; used only in vision mode
    console.info(`[eob-worker] Downloaded page ${job.page_number} for doc ${job.eob_document_id} (${pdfBytes.length} bytes)`);

    // STEP 1.5: DETECT DIGITAL-NATIVE vs SCANNED PAGE (hybrid mode)
    // If unpdf successfully extracts meaningful text, we send text to Gemini instead of
    // the raw base64 PDF — dramatically reducing token cost for digital-native EOBs
    // (Mercy Care, Aetna, most BCBS). Scanned pages fall back to vision automatically.
    let pageTextContent: string | null = null;
    let extractionMode: 'text' | 'vision' = 'vision';

    if (pdfExtractText) {
      try {
        const result = await pdfExtractText(pdfBytes, { mergePages: true });
        const rawText = typeof result.text === 'string'
          ? result.text
          : (result.text as string[]).join('\n');

        // Count meaningful characters (letters, digits, common EOB symbols)
        const meaningfulChars = (rawText.match(/[a-zA-Z0-9$.,\-\/:()|]/g) || []).length;
        const digitChars = (rawText.match(/\d/g) || []).length;
        const digitDensity = meaningfulChars > 0 ? digitChars / meaningfulChars : 0;
        const isFinancialGrid = digitDensity > FINANCIAL_GRID_DIGIT_RATIO;

        if (meaningfulChars >= DIGITAL_NATIVE_MIN_CHARS && !isFinancialGrid) {
          // Digital-native prose page (low digit density) — text extraction is reliable
          pageTextContent = rawText.trim();
          extractionMode = 'text';
          console.info(`[eob-worker] page ${job.page_number}: digital-native prose (${meaningfulChars} chars, ${Math.round(digitDensity * 100)}% digits) → TEXT mode`);
        } else if (isFinancialGrid) {
          // Financial claim grid: unpdf loses column alignment → Gemini can't map amounts.
          // Always use VISION so Gemini sees the visual table structure.
          console.info(`[eob-worker] page ${job.page_number}: financial grid (${Math.round(digitDensity * 100)}% digits, ${meaningfulChars} chars) → VISION mode (column layout preserved)`);
        } else {
          console.info(`[eob-worker] page ${job.page_number}: scanned/sparse (${meaningfulChars} chars) → VISION mode`);
        }
      } catch (pdfErr: any) {
        // Non-fatal: any extraction error falls through to vision
        console.warn(`[eob-worker] PDF text extraction failed, using vision: ${pdfErr.message}`);
      }
    }

    // STEP 2: AUTHENTICATE to GCP
    const gToken = await getGoogleAccessToken(sa);

    // STEP 3: CALL GEMINI 2.0 FLASH (Vertex AI) — per-page extraction with retry
    // TEXT MODE: send extracted text inline → far fewer tokens, lower cost
    // VISION MODE: send raw base64 PDF → handles scanned / image-based pages
    const VERTEX_URL = `https://us-central1-aiplatform.googleapis.com/v1/projects/${sa.project_id}/locations/us-central1/publishers/google/models/gemini-2.0-flash-001:generateContent`;

    const geminiParts: Array<{ text: string } | { inlineData: { mimeType: string; data: string } }> =
      extractionMode === 'text' && pageTextContent
        ? [
            { text: GEMINI_PROMPT },
            { text: `\n\nEOB PAGE TEXT (extracted from digital PDF — treat this as the complete page content):\n\`\`\`\n${pageTextContent}\n\`\`\`` }
          ]
        : [
            { text: GEMINI_PROMPT },
            { inlineData: { mimeType: 'application/pdf', data: base64PDF } }
          ];

    const geminiBody = JSON.stringify({
      contents: [{ "role": "user", "parts": geminiParts }],
      generationConfig: { "responseMimeType": "application/json", "maxOutputTokens": 8192 }
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
    const finishReason = aiData.candidates[0]?.finishReason;

    // Warn if Gemini truncated its output
    if (finishReason === 'MAX_TOKENS') {
      console.warn(`[eob-worker] Gemini truncated response (MAX_TOKENS) on page ${job.page_number} — attempting partial JSON recovery`);
    }

    // Parse JSON with repair fallback for truncated/malformed Gemini responses
    let parsed: any;
    try {
      // Strip markdown code fences (safety net — shouldn't appear with responseMimeType=json)
      const cleaned = rawText.trim().replace(/^```(?:json)?\n?/, '').replace(/\n?```$/m, '');
      parsed = JSON.parse(cleaned);
    } catch (parseErr: any) {
      console.warn(`[eob-worker] JSON parse failed on page ${job.page_number} (len=${rawText.length}, finishReason=${finishReason}): ${parseErr.message}`);
      console.warn(`[eob-worker] rawText first 400 chars: ${rawText.substring(0, 400)}`);

      // Truncation repair: find the last complete JSON object and close the array/root object
      let repaired: any = null;
      const cleaned = rawText.trim().replace(/^```(?:json)?\n?/, '').replace(/\n?```$/m, '');
      const lastClose = cleaned.lastIndexOf('}');
      if (lastClose > 0) {
        // Try closing the items array + root object after the last complete item
        for (const suffix of [']}', ']\n}']) {
          try {
            repaired = JSON.parse(cleaned.substring(0, lastClose + 1) + suffix);
            console.warn(`[eob-worker] Partial recovery: ${repaired.items?.length ?? 0} items from page ${job.page_number}`);
            break;
          } catch { /* try next */ }
        }
      }

      if (!repaired) {
        throw new Error(`Gemini response unparseable (len=${rawText.length}, finishReason=${finishReason}): ${parseErr.message}`);
      }
      parsed = repaired;
    }

    const rawExtracted = parsed.items || parsed.line_items || [];
    console.info(`[eob-worker] Gemini extracted ${rawExtracted.length} raw line items from page ${job.page_number} [${extractionMode} mode]`);

    // STEP 3B: DEDUPLICATE — merge rows with same composite key
    // Gemini sometimes extracts the same service line twice (once from summary, once from detail).
    // We merge duplicates by keeping the most-populated row (highest confidence, most non-null fields).
    const deduped = deduplicateItems(rawExtracted);
    if (deduped.length < rawExtracted.length) {
      console.info(`[eob-worker] Dedup: merged ${rawExtracted.length} → ${deduped.length} items on page ${job.page_number}`);
    }

    // STEP 3C: ENRICH — fill deterministic fallbacks (claim_status inference, remark descriptions)
    const extracted = enrichExtractedItems(deduped);

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
          // Financial breakdown (Phase 7)
          deductible_amount: parseCurrency(it.deductible_amount),
          coinsurance_amount: parseCurrency(it.coinsurance_amount),
          copay_amount: parseCurrency(it.copay_amount),
          contractual_adjustment: parseCurrency(it.contractual_adjustment),
          // Phase 10: RCM Intelligence — non-covered amount + remark descriptions
          non_covered_amount: parseCurrency(it.non_covered_amount),
          remark_description: it.remark_description || null,
          // Confidence scoring (Error Inbox Phase 0)
          confidence_score: parseInt(it.confidence_score) || null,
          created_at: now,
        }
      }));

      try {
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
          console.warn(`[eob-worker] BigQuery insert failed (HTTP ${bqResp.status}) — non-fatal, Supabase still receives data: ${JSON.stringify(bqResult)}`);
        } else if (bqResult.insertErrors && bqResult.insertErrors.length > 0) {
          console.warn(`[eob-worker] BigQuery partial insert errors (non-fatal, likely schema mismatch): ${JSON.stringify(bqResult.insertErrors[0])}`);
        } else {
          console.info(`[eob-worker] Inserted ${rows.length} rows into BigQuery for page ${job.page_number}`);
        }
      } catch (bqEx: any) {
        console.warn(`[eob-worker] BigQuery insert exception (non-fatal): ${bqEx.message}`);
      }
    }

    // STEP 4c: PERSIST TO SUPABASE eob_line_items (for in-app reporting)
    if (extracted.length > 0) {
      try {
        // Delete existing rows first for idempotency on re-runs
        await supabase
          .from('eob_line_items')
          .delete()
          .eq('eob_document_id', job.eob_document_id)
          .eq('page_number', job.page_number);

        const supabaseRows = extracted.map((it: any) => ({
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
          remark_description: it.remark_description || null,
          claim_status: it.claim_status || null,
          claim_number: it.claim_number || null,
          payment_date: formatBQDate(it.payment_date),
          payer_name: it.payer_name || null,
          payer_id: it.payer_id || null,
          adjustment_amount: parseCurrency(it.adjustment_amount),
          deductible_amount: parseCurrency(it.deductible_amount),
          coinsurance_amount: parseCurrency(it.coinsurance_amount),
          copay_amount: parseCurrency(it.copay_amount),
          contractual_adjustment: parseCurrency(it.contractual_adjustment),
          non_covered_amount: parseCurrency(it.non_covered_amount),
          confidence_score: parseInt(it.confidence_score) || null,
        }));

        const { error: sbErr } = await supabase
          .from('eob_line_items')
          .insert(supabaseRows);

        if (sbErr) {
          console.warn(`[eob-worker] Supabase eob_line_items insert failed (non-fatal): ${sbErr.message}`);
        } else {
          console.info(`[eob-worker] Inserted ${supabaseRows.length} rows into Supabase eob_line_items for page ${job.page_number}`);
        }
      } catch (sbEx: any) {
        console.warn(`[eob-worker] Supabase eob_line_items exception (non-fatal): ${sbEx.message}`);
      }
    }

    // STEP 5: FINALIZE — mark job as succeeded with audit data
    await supabase.rpc('succeed_eob_page_job', {
      p_page_job_id: job.id,
      p_items_extracted: extracted.length,
      p_gemini_response_type: extracted.length > 0 ? 'items_found' : 'empty_items_array',
      p_gemini_raw_response: aiData
    });

    // STEP 6: CHECK EXCEPTIONS — fire if document reached terminal state
    // Non-blocking: if check-exceptions fails, it doesn't block the worker response
    const { data: docStatus } = await supabase
      .from('eob_documents')
      .select('status')
      .eq('id', job.eob_document_id)
      .single();

    if (docStatus && ['completed', 'partial_failure'].includes(docStatus.status)) {
      const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
      const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
      fetch(`${SUPABASE_URL}/functions/v1/check-exceptions`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ eob_document_id: job.eob_document_id })
      }).then(r => console.info(`[eob-worker] check-exceptions fired: ${r.status}`))
        .catch(err => console.warn('[eob-worker] check-exceptions fire failed:', err.message));
    }

    return new Response(JSON.stringify({
      status: 'succeeded',
      count: extracted.length,
      page_number: job.page_number,
      eob_document_id: job.eob_document_id,
      extraction_mode: extractionMode   // 'text' | 'vision' — for cost analysis
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

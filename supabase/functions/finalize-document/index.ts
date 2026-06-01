// finalize-document — Document-Level Context Stitching.
// Called by check-exceptions after all pages complete. Resolves source_check_number
// for every row in eob_line_items using an order-agnostic page→check map.
//
// Algorithm:
//   PASS 1 — Accumulate: collect check/EFT identifiers from all summary_total rows
//             across the entire document (any page order).
//   PASS 2 — Resolve: for each row, assign the nearest-preceding check page's
//             identifier. Rows that appear before any check stub (backward-layout
//             EOBs where the summary page is at the end) fall back to the first
//             check found in the document.
//   FALLBACK — If the document contains zero valid check identifiers, exit without
//               modification (preserves current default behaviour for continuation
//               grids and scanned-only packets with no check stub).

import { createClient } from "npm:@supabase/supabase-js@2.39.7";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ──────────────────────────────────────────────────────────────
// Check identifier detection
// Distinguishes payment check/EFT numbers from CARC/RARC adjustment codes.
//
// CARC/RARC patterns (always rejected):
//   CO-45, PR-1, OA-23, N-30, MA18, W1, CR-1, PI-42, B14
//
// Valid check/EFT patterns (accepted):
//   Pure digit strings ≥4 digits:     "1234567890"
//   CHK-/EFT-/ACH-/TRN-/TRACE- prefix: "CHK-98765", "EFT-1234567890123"
//   "CHECK " or "EFT " prefix:         "CHECK 123456"
// ──────────────────────────────────────────────────────────────
const ADJUSTMENT_CODE_RE = /^(CO|PR|OA|PI|CR|N|MA|M|W|B|NC)\d*-?\d+$/i;

function isValidCheckId(s: string | null | undefined): boolean {
  if (!s) return false;
  const t = s.trim();
  if (!t || t.toLowerCase() === 'unknown' || t === '(Unknown)') return false;
  if (ADJUSTMENT_CODE_RE.test(t)) return false;
  // Accept: 4+ digit string (possibly with hyphens/spaces typical in check numbers),
  // or an explicit check/EFT prefix.
  return /^\d{4,}/.test(t) || /^(CHK|EFT|ACH|TRN|TRACE|CHECK)[-\s]/i.test(t);
}

// Prefer remark_code on summary_total rows (that's where Gemini puts check/EFT numbers).
// Fall back to claim_number if remark_code isn't a valid check identifier.
function extractCheckId(remark_code: string | null, claim_number: string | null): string | null {
  if (isValidCheckId(remark_code)) return remark_code!.trim();
  if (isValidCheckId(claim_number)) return claim_number!.trim();
  return null;
}

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  const json = (body: any, status = 200) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });

  try {
    const { eob_document_id } = await req.json();
    if (!eob_document_id) return json({ error: 'eob_document_id is required' }, 400);

    console.info(`[finalize-document] Starting check stitching for ${eob_document_id}`);

    // ── PASS 1: Fetch all rows ordered by page ─────────────────────────────
    const { data: rows, error: fetchErr } = await supabase
      .from('eob_line_items')
      .select('id, page_number, line_type, remark_code, claim_number, source_check_number')
      .eq('eob_document_id', eob_document_id)
      .order('page_number', { ascending: true });

    if (fetchErr) throw new Error(`Failed to fetch rows: ${fetchErr.message}`);

    if (!rows || rows.length === 0) {
      console.info(`[finalize-document] No rows found for ${eob_document_id} — nothing to stitch`);
      return json({ status: 'no_rows', eob_document_id, stitched: 0 });
    }

    // ── PASS 1: Accumulate check identifiers from summary_total rows ───────
    // pageCheckMap: page_number → canonical check/EFT identifier
    const pageCheckMap = new Map<number, string>();
    const allCheckIds: string[] = [];

    for (const row of rows) {
      if (row.line_type === 'summary_total') {
        const checkId = extractCheckId(row.remark_code, row.claim_number);
        if (checkId) {
          pageCheckMap.set(row.page_number, checkId);
          if (!allCheckIds.includes(checkId)) allCheckIds.push(checkId);
        }
      }
    }

    // ── FALLBACK: No check identifiers found anywhere ──────────────────────
    if (allCheckIds.length === 0) {
      console.info(`[finalize-document] No check identifiers found in ${eob_document_id} — skipping stitching`);
      return json({ status: 'no_checks_found', eob_document_id, stitched: 0 });
    }

    console.info(`[finalize-document] ${allCheckIds.length} check identifier(s) on pages [${[...pageCheckMap.keys()].sort((a,b)=>a-b).join(', ')}]`);

    // Sorted check pages for nearest-preceding lookup
    const checkPages = [...pageCheckMap.keys()].sort((a, b) => a - b);

    // First check in document — used as fallback for backward-layout rows
    // (service lines that appear before any check stub page)
    const firstCheck = pageCheckMap.get(checkPages[0])!;

    // ── PASS 2: Resolve source_check_number for every row ─────────────────
    // For each row find the nearest PRECEDING check page (≤ row.page_number).
    // If no preceding check page exists, use firstCheck (backward-layout fallback).
    const updates: Array<{ id: string; source_check_number: string }> = [];

    for (const row of rows) {
      let resolvedCheck: string | null = null;

      // Binary-search style: walk backwards through sorted checkPages
      for (let i = checkPages.length - 1; i >= 0; i--) {
        if (checkPages[i] <= row.page_number) {
          resolvedCheck = pageCheckMap.get(checkPages[i])!;
          break;
        }
      }

      // Backward-layout fallback: this row's page precedes all check stubs
      if (!resolvedCheck) resolvedCheck = firstCheck;

      // Enqueue update only when value is missing or has changed
      if (row.source_check_number !== resolvedCheck) {
        updates.push({ id: row.id, source_check_number: resolvedCheck });
      }
    }

    if (updates.length === 0) {
      console.info(`[finalize-document] All rows already stitched for ${eob_document_id}`);
      return json({ status: 'already_stitched', eob_document_id, stitched: 0 });
    }

    // ── PASS 2 COMMIT: Batch UPDATE grouped by check number ────────────────
    // Group IDs by their resolved check so we issue one UPDATE per unique check.
    // Cap each call at 50 IDs to avoid PostgREST URL length limits.
    const BATCH_SIZE = 50;
    let totalUpdated = 0;
    let errors = 0;

    // Group updates by resolved check number
    const byCheck = new Map<string, string[]>();
    for (const u of updates) {
      const list = byCheck.get(u.source_check_number) || [];
      list.push(u.id);
      byCheck.set(u.source_check_number, list);
    }

    for (const [checkNum, ids] of byCheck) {
      // Chunk ids to stay within URL length limits
      for (let j = 0; j < ids.length; j += BATCH_SIZE) {
        const idChunk = ids.slice(j, j + BATCH_SIZE);
        const { error: updateErr } = await supabase
          .from('eob_line_items')
          .update({ source_check_number: checkNum })
          .in('id', idChunk);

        if (updateErr) {
          console.error(`[finalize-document] Update failed (check=${checkNum}): ${updateErr.message}`);
          errors++;
        } else {
          totalUpdated += idChunk.length;
        }
      }
    }

    console.info(`[finalize-document] Done — ${totalUpdated} rows stitched, ${errors} errors — document ${eob_document_id}`);

    return json({
      status: 'stitched',
      eob_document_id,
      stitched: totalUpdated,
      check_ids_found: allCheckIds,
      errors,
    });

  } catch (err: any) {
    console.error(`[finalize-document] Error: ${err.message}`);
    return json({ error: 'Finalize failed', details: err.message }, 500);
  }
});

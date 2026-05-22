// eob-enqueue.js — PDF splitting orchestrator
// Downloads a multi-page EOB PDF from one of three sources:
//   1. Signed URL (signed_pdf_url)
//   2. GCS bucket/object (gcs_bucket + gcs_object_name)
//   3. Supabase Storage (storage_bucket + storage_path) — for frontend uploads
// Splits into individual pages, uploads each to Supabase Storage, enqueues page
// jobs, and triggers eob-worker for each page (awaited in batches of 3).

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { PDFDocument } from "npm:pdf-lib@1.17.1";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";
import { getGoogleAccessToken, moveToProcessedFolder } from "../_shared/gcp-auth.ts";

const MAX_PAGES_PER_DOC = 500;
const STORAGE_BUCKET = "eob-pages";
// Worker triggering: fire in batches to avoid Gemini 429 rate limits
// Each batch fires concurrently, with delays between batches

function json(data: unknown, status = 200, extraHeaders: Record<string, string> = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  });
}

// ──────────────────────────────────────────────────────────────
// Helper: sleep for rate limiting
// ──────────────────────────────────────────────────────────────
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === "OPTIONS") return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  if (req.method !== "POST") return json({ error: "Use POST" }, 405, corsHeaders);

  // Env check
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("[eob-enqueue] missing env");
    return json({ error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Parse body and validate
  let body: any;
  try {
    body = await req.json();
  } catch (e) {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const practice_id = body?.practice_id;
  const eob_document_id = body?.eob_document_id;
  const signed_pdf_url = body?.signed_pdf_url;
  const gcs_bucket = body?.gcs_bucket;
  const gcs_object_name = body?.gcs_object_name;
  // Supabase Storage source (frontend uploads)
  const storage_bucket = body?.storage_bucket;
  const storage_path = body?.storage_path;
  // Google Drive source (n8n folder watcher)
  const gdrive_file_id = body?.gdrive_file_id;

  // Require practice_id + eob_document_id + one of four source modes
  if (!practice_id || !eob_document_id) {
    return json({ error: "Missing practice_id or eob_document_id" }, 400);
  }
  if (!signed_pdf_url && !(gcs_bucket && gcs_object_name) && !(storage_bucket && storage_path) && !gdrive_file_id) {
    return json({ error: "Missing PDF source: provide signed_pdf_url, (gcs_bucket + gcs_object_name), (storage_bucket + storage_path), or gdrive_file_id" }, 400);
  }

  console.info("[eob-enqueue] start", { practice_id, eob_document_id });

  // Look up file_name from eob_documents for denormalization into page jobs + worker payload
  let file_name: string | null = null;
  try {
    const { data: docRow, error: docErr } = await supabase
      .from("eob_documents")
      .select("file_name")
      .eq("id", eob_document_id)
      .single();
    if (!docErr && docRow?.file_name) {
      file_name = docRow.file_name;
    }
    console.info("[eob-enqueue] file_name for denormalization:", file_name);
  } catch (e) {
    console.warn("[eob-enqueue] file_name lookup failed (non-fatal):", e);
  }

  // Helper to refund credits on error (best-effort)
  const refundCredits = async () => {
    try {
      await supabase.rpc("refund_parsing_credit", { p_practice_id: practice_id });
    } catch (e) {
      console.error("[eob-enqueue] refund_parsing_credit failed", e);
    }
  };

  // ──────────────────────────────────────────────────────────────
  // 1) Download PDF — from Supabase Storage, signed URL, or GCS
  // ──────────────────────────────────────────────────────────────
  let pdfBytes: Uint8Array;
  try {
    if (gdrive_file_id) {
      // Source mode 4: Google Drive (n8n folder watcher)
      console.info("[eob-enqueue] downloading from Google Drive:", gdrive_file_id);
      const GCP_SA_JSON_STR = Deno.env.get("GCP_SA_JSON");
      if (!GCP_SA_JSON_STR) {
        return json({ error: "Google Drive download requested but GCP_SA_JSON not configured" }, 500);
      }
      const sa = JSON.parse(GCP_SA_JSON_STR.trim());
      const gToken = await getGoogleAccessToken(sa);
      const driveUrl = `https://www.googleapis.com/drive/v3/files/${gdrive_file_id}?alt=media`;
      const driveResp = await fetch(driveUrl, {
        method: "GET",
        headers: { "Authorization": `Bearer ${gToken}` },
      });
      if (!driveResp.ok) {
        const errText = await driveResp.text();
        console.error("[eob-enqueue] Google Drive download failed:", driveResp.status, errText);
        return json({ error: "Failed to download from Google Drive", status: driveResp.status, details: errText }, 400);
      }
      pdfBytes = new Uint8Array(await driveResp.arrayBuffer());
      console.info("[eob-enqueue] Google Drive download complete:", pdfBytes.length, "bytes");
    } else if (storage_bucket && storage_path) {
      // Source mode 3: Supabase Storage (frontend uploads)
      console.info("[eob-enqueue] downloading from Supabase Storage:", storage_bucket, storage_path);
      const { data: fileBlob, error: storageErr } = await supabase.storage
        .from(storage_bucket)
        .download(storage_path);

      if (storageErr || !fileBlob) {
        console.error("[eob-enqueue] Supabase Storage download error:", storageErr);
        return json({ error: "Failed to download PDF from Supabase Storage", error_code: "file_not_found", details: storageErr?.message }, 400);
      }
      pdfBytes = new Uint8Array(await fileBlob.arrayBuffer());
    } else {
      // Source mode 1 (signed URL) or 2 (GCS)
      let downloadUrl = signed_pdf_url;
      let downloadHeaders: Record<string, string> = {};

      // If no signed URL, build GCS download URL with GCP auth
      if (!downloadUrl && gcs_bucket && gcs_object_name) {
        const GCP_SA_JSON_STR = Deno.env.get("GCP_SA_JSON");
        if (!GCP_SA_JSON_STR) {
          return json({ error: "GCS download requested but GCP_SA_JSON not configured" }, 500);
        }
        const sa = JSON.parse(GCP_SA_JSON_STR.trim());
        const gToken = await getGoogleAccessToken(sa);
        downloadUrl = `https://storage.googleapis.com/storage/v1/b/${gcs_bucket}/o/${encodeURIComponent(gcs_object_name)}?alt=media`;
        downloadHeaders = { 'Authorization': `Bearer ${gToken}` };
        console.info("[eob-enqueue] downloading from GCS:", gcs_bucket, gcs_object_name);
      }

      const resp = await fetch(downloadUrl, { method: "GET", headers: downloadHeaders });
      if (!resp.ok) {
        console.error("[eob-enqueue] fetch pdf failed", resp.status, resp.statusText);
        return json({ error: "Failed to fetch PDF", error_code: "file_not_found", status: resp.status, statusText: resp.statusText }, 400);
      }
      pdfBytes = new Uint8Array(await resp.arrayBuffer());
    }
  } catch (e) {
    console.error("[eob-enqueue] fetch error", e);
    return json({ error: "Failed to download PDF", error_code: "storage_error" }, 400);
  }

  // ──────────────────────────────────────────────────────────────
  // 1b) File size guard — reject files > 25MB before any processing
  // ──────────────────────────────────────────────────────────────
  const MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024; // 25MB
  if (pdfBytes.length > MAX_FILE_SIZE_BYTES) {
    const sizeMb = (pdfBytes.length / 1024 / 1024).toFixed(1);
    console.warn(`[eob-enqueue] file too large: ${sizeMb}MB exceeds 25MB limit`);
    await supabase.from("eob_documents")
      .update({ status: "failed", error_message: `File is ${sizeMb}MB, exceeds the 25MB maximum` })
      .eq("id", eob_document_id);
    try {
      await supabase.from("pipeline_events").insert({
        practice_id,
        event_type: "processing_error",
        file_name,
        details: { error_code: "FILE_TOO_LARGE", file_size_mb: sizeMb },
        source: gdrive_file_id ? "folder_watcher" : (storage_bucket ? "manual_upload" : "gcs"),
      });
    } catch (e) { console.warn("[eob-enqueue] pipeline_events insert failed:", e); }
    return json({ error: `File exceeds 25MB size limit (${sizeMb}MB)`, error_code: "FILE_TOO_LARGE" }, 413);
  }

  // ──────────────────────────────────────────────────────────────
  // ──────────────────────────────────────────────────────────────
  // 1d) Archive original to eob-uploads for non-storage sources
  //     Ensures reprocess-document can always find the original PDF.
  // ──────────────────────────────────────────────────────────────
  const isFromEobUploads = storage_bucket === "eob-uploads";
  if (!isFromEobUploads && pdfBytes.length > 0) {
    try {
      const timestamp = Date.now();
      const safeName = (file_name || "document.pdf").replace(/[^a-zA-Z0-9._-]/g, "_");
      const archivePath = `${practice_id}/${timestamp}_${safeName}`;

      const { error: archiveErr } = await supabase.storage
        .from("eob-uploads")
        .upload(archivePath, pdfBytes, { contentType: "application/pdf", upsert: false });

      if (archiveErr) {
        console.warn("[eob-enqueue] archive to eob-uploads failed (non-fatal):", archiveErr.message);
      } else {
        console.info("[eob-enqueue] archived original to eob-uploads:", archivePath);
        // Update file_path so reprocess-document can find the original
        const { error: fpErr } = await supabase
          .from("eob_documents")
          .update({ file_path: archivePath })
          .eq("id", eob_document_id);

        if (fpErr) {
          console.warn("[eob-enqueue] file_path update failed (non-fatal):", fpErr.message);
        } else {
          console.info("[eob-enqueue] updated file_path to:", archivePath);
        }
      }
    } catch (e) {
      console.warn("[eob-enqueue] archive failed (non-fatal):", e);
    }
  }

  // 2) Load PDF, count pages, validate
  let pdfDoc: PDFDocument;
  let totalPages: number;
  try {
    pdfDoc = await PDFDocument.load(pdfBytes, { ignoreEncryption: true });
    totalPages = pdfDoc.getPageCount();
  } catch (e) {
    console.error("[eob-enqueue] pdf load error", e);
    return json({ error: "Invalid PDF or unable to parse" }, 400);
  }

  if (!Number.isFinite(totalPages) || totalPages < 1) {
    return json({ error: "PDF has no pages / invalid PDF" }, 400);
  }

  if (totalPages > MAX_PAGES_PER_DOC) {
    return json({ error: `PDF exceeds maximum limit of ${MAX_PAGES_PER_DOC} pages.` }, 413);
  }

  console.info("[eob-enqueue] pdf pages", { eob_document_id, totalPages });

  // 2b) Soft page cap — enforce plan_max_pages_per_doc
  //     Process only the first N pages; user sees data + an upsell banner instead of an error.
  const pagesActual = totalPages; // original count before any cap
  let pagesCapped = false;
  try {
    const { data: practiceRow } = await supabase
      .from("practices")
      .select("plan_max_pages_per_doc")
      .eq("id", practice_id)
      .single();

    const maxPages: number = practiceRow?.plan_max_pages_per_doc ?? 10;
    if (totalPages > maxPages) {
      console.info(`[eob-enqueue] soft page cap: capping ${totalPages} pages to ${maxPages} (plan limit)`);
      totalPages = maxPages;
      pagesCapped = true;
    }
  } catch (e) {
    console.warn("[eob-enqueue] plan tier lookup failed (non-fatal), using full page count:", e);
  }

  // 3) Attempt to reserve/charge credits atomically for totalPages
  try {
    const { data: creditOk, error: creditErr } = await supabase.rpc("use_parsing_credit", {
      p_practice_id: practice_id,
      p_amount: totalPages,
    });

    if (creditErr) {
      console.error("[eob-enqueue] credit rpc error", creditErr);
      return json({ error: "Credit RPC failed", detail: creditErr.message }, 402);
    }

    if (!creditOk) {
      return json({ error: "Insufficient credits" }, 402);
    }
  } catch (e) {
    console.error("[eob-enqueue] credit rpc thrown", e);
    return json({ error: "Credit RPC failed" }, 500);
  }

  const nowIso = new Date().toISOString();

  // 4) Update eob_documents status to queued
  try {
    const { error: docErr } = await supabase
      .from("eob_documents")
      .update({
        status: "queued",
        updated_at: nowIso,
        error_message: null,
        total_pages: totalPages,
        ...(pagesCapped ? { pages_capped: true, pages_actual: pagesActual } : {}),
      })
      .eq("id", eob_document_id);

    if (docErr) {
      console.error("[eob-enqueue] update eob_documents error", docErr);
      await refundCredits();
      return json({ error: "Failed to update eob_documents", detail: docErr.message }, 500);
    }
  } catch (e) {
    console.error("[eob-enqueue] update eob_documents thrown", e);
    await refundCredits();
    return json({ error: "Failed to update eob_documents" }, 500);
  }

  // 5) For idempotency: fetch existing object list for the document
  let existingObjects = new Set<string>();
  try {
    const { data: listData, error: listErr } = await supabase.storage
      .from(STORAGE_BUCKET)
      .list(eob_document_id, { limit: 1000 });

    if (listErr) {
      console.warn("[eob-enqueue] storage.list error - falling back to per-page checks", listErr);
      existingObjects = new Set();
    } else if (Array.isArray(listData)) {
      for (const obj of listData) {
        if (obj?.name) existingObjects.add(obj.name);
      }
    }
  } catch (e) {
    console.warn("[eob-enqueue] storage.list thrown - proceeding with per-page checks", e);
  }

  // ──────────────────────────────────────────────────────────────
  // 6) PHASE 1: Split, upload, and enqueue ALL page jobs first
  //    (no worker triggers yet — just get all jobs into the DB)
  // ──────────────────────────────────────────────────────────────
  const enqueuedJobs: Array<{ jobId: string; pageNumber: number }> = [];

  // ── STAGE A: Build all single-page PDFs (CPU-bound, synchronous) ──
  // Keeping this sequential avoids saturating Deno's V8 heap with 60+ PDFs in memory at once.
  type PageItem = { pageNumber: number; pageName: string; pagePath: string; bytes: Uint8Array | null };
  const pageItems: PageItem[] = [];
  try {
    for (let i = 0; i < totalPages; i++) {
      const pageNumber = i + 1;
      const pageName = `page-${String(pageNumber).padStart(3, "0")}.pdf`;
      const pagePath = `${eob_document_id}/${pageName}`;

      if (existingObjects.has(pageName)) {
        console.info(`[eob-enqueue] page ${pageNumber} already uploaded — skipping PDF creation`);
        pageItems.push({ pageNumber, pageName, pagePath, bytes: null }); // null = skip upload
      } else {
        const newPdf = await PDFDocument.create();
        const [pg] = await newPdf.copyPages(pdfDoc, [i]);
        newPdf.addPage(pg);
        const pageBytes = await newPdf.save();
        pageItems.push({ pageNumber, pageName, pagePath, bytes: pageBytes });
      }
    }
    console.info(`[eob-enqueue] Stage A complete: built ${pageItems.length} page PDFs`);
  } catch (e) {
    console.error("[eob-enqueue] Stage A (PDF split) error", e);
    await refundCredits();
    return json({ error: "PDF split phase error" }, 500);
  }

  // ── STAGE B: Upload all pages in parallel batches of 10 ──
  // 62 pages serial ≈ 124s (hits 150s timeout); 10-concurrent batches ≈ 14s.
  const UPLOAD_BATCH = 10;
  try {
    for (let b = 0; b < pageItems.length; b += UPLOAD_BATCH) {
      const batch = pageItems.slice(b, b + UPLOAD_BATCH).filter(item => item.bytes !== null);
      if (batch.length === 0) continue;

      await Promise.all(
        batch.map(async ({ pageNumber, pagePath, bytes }) => {
          const { error: uploadErr } = await supabase.storage
            .from(STORAGE_BUCKET)
            .upload(pagePath, bytes!, { contentType: "application/pdf", upsert: true });
          if (uploadErr) {
            console.error("[eob-enqueue] upload error", { pagePath, message: uploadErr.message });
            throw new Error(`Failed to upload page ${pageNumber}: ${uploadErr.message}`);
          }
          console.info(`[eob-enqueue] uploaded ${pagePath}`);
        })
      );

      const batchNum = Math.floor(b / UPLOAD_BATCH) + 1;
      const batchEnd = Math.min(b + UPLOAD_BATCH, pageItems.length);
      console.info(`[eob-enqueue] Stage B batch ${batchNum} complete (pages ${b + 1}–${batchEnd})`);
    }
    console.info(`[eob-enqueue] Stage B complete: all ${totalPages} pages uploaded`);
  } catch (e: any) {
    console.error("[eob-enqueue] Stage B (upload) error", e);
    await refundCredits();
    return json({ error: e.message || "Upload phase error" }, 500);
  }

  // ── STAGE C: Enqueue all page jobs in parallel batches of 5 ──
  const ENQUEUE_BATCH = 5;
  try {
    for (let b = 0; b < pageItems.length; b += ENQUEUE_BATCH) {
      const batch = pageItems.slice(b, b + ENQUEUE_BATCH);

      await Promise.all(
        batch.map(async ({ pageNumber, pagePath }) => {
          // Idempotency: skip if job already exists
          try {
            const { data: existingJob } = await supabase
              .from("eob_page_jobs")
              .select("id,status")
              .eq("eob_document_id", eob_document_id)
              .eq("page_number", pageNumber)
              .limit(1);
            if (Array.isArray(existingJob) && existingJob.length > 0) {
              console.info(`[eob-enqueue] job already exists for page ${pageNumber}, skipping`);
              return;
            }
          } catch (e) {
            console.warn(`[eob-enqueue] job existence check thrown for page ${pageNumber}`, e);
          }

          const { data: enqueueResult, error: enqueueErr } = await supabase.rpc("enqueue_eob_page_job", {
            p_eob_document_id: eob_document_id,
            p_practice_id: practice_id,
            p_page_number: pageNumber,
            p_total_pages: totalPages,
            p_page_storage_bucket: STORAGE_BUCKET,
            p_page_storage_path: pagePath,
            p_run_after: nowIso,
            p_file_name: file_name,
          });

          if (enqueueErr) {
            console.error("[eob-enqueue] enqueue_eob_page_job rpc error", { pageNumber, enqueueErr });
            throw new Error(`Failed to enqueue page ${pageNumber}: ${enqueueErr.message}`);
          }

          const jobId: string | null = enqueueResult?.id || enqueueResult || null;
          console.info(`[eob-enqueue] enqueued job for page ${pageNumber}, jobId: ${jobId}`);
          if (jobId) enqueuedJobs.push({ jobId, pageNumber });
        })
      );

      const batchNum = Math.floor(b / ENQUEUE_BATCH) + 1;
      console.info(`[eob-enqueue] Stage C batch ${batchNum} complete`);
    }
    console.info(`[eob-enqueue] Stage C complete: ${enqueuedJobs.length} jobs enqueued`);
  } catch (e: any) {
    console.error("[eob-enqueue] Stage C (enqueue) error", e);
    await refundCredits();
    return json({ error: e.message || "Enqueue phase error" }, 500);
  }

  console.info(`[eob-enqueue] Phase 1 complete: ${enqueuedJobs.length} jobs enqueued for ${totalPages} pages`);

  // ──────────────────────────────────────────────────────────────
  // 7) Return HTTP response immediately.
  //    Workers fire in the background via EdgeRuntime.waitUntil so
  //    large documents (60-150 pages) don't hit the 150s function
  //    timeout. The document stays in "queued" state while workers
  //    run; succeed_eob_page_job() auto-transitions it to
  //    "completed" / "partial_failure" when all pages finish.
  // ──────────────────────────────────────────────────────────────
  // Capture all variables needed by the background closure
  const _enqueuedJobs   = enqueuedJobs;
  const _supabaseUrl    = SUPABASE_URL!;
  const _serviceKey     = SUPABASE_SERVICE_ROLE_KEY!;
  const _docId          = eob_document_id;
  const _practiceId     = practice_id;
  const _fileName       = file_name;

  const backgroundTask = (async () => {
    // Phase 2: fire workers — fire-and-forget (no await on responses).
    //
    // WHY fire-and-forget:
    //   Awaiting each worker response serializes execution. For a 112-page doc
    //   at ~20s/page in batches of 3, that's ~870 seconds of background runtime —
    //   far beyond what EdgeRuntime.waitUntil can sustain. Only ~8 workers fire
    //   before the budget runs out, leaving 100+ jobs stuck in "queued".
    //
    //   Workers are independent edge function invocations that update their own
    //   job/document status via succeed_eob_page_job / fail_eob_page_job RPCs.
    //   eob-enqueue does NOT need to observe their completion.
    //
    // Staggering (500ms between batches of 5) prevents a simultaneous burst of
    // 100+ Gemini calls. Workers retry on transient rate-limit errors.
    const FIRE_BATCH = 5;
    const FIRE_DELAY_MS = 500;

    try {
      for (let b = 0; b < _enqueuedJobs.length; b += FIRE_BATCH) {
        const batch = _enqueuedJobs.slice(b, b + FIRE_BATCH);

        for (const { jobId, pageNumber } of batch) {
          // Intentionally NOT awaited — worker runs independently
          fetch(`${_supabaseUrl}/functions/v1/eob-worker`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${_serviceKey}`,
            },
            body: JSON.stringify({
              job: {
                id: jobId,
                eob_document_id: _docId,
                page_number: pageNumber,
                practice_id: _practiceId,
                file_name: _fileName,
              }
            }),
          }).catch(err => console.warn(`[eob-enqueue] worker trigger failed page ${pageNumber}: ${err?.message}`));
        }

        const batchNum = Math.floor(b / FIRE_BATCH) + 1;
        const batchEnd = Math.min(b + FIRE_BATCH, _enqueuedJobs.length);
        console.info(`[eob-enqueue] fired worker batch ${batchNum} (pages ${b + 1}–${batchEnd})`);

        if (b + FIRE_BATCH < _enqueuedJobs.length) {
          await sleep(FIRE_DELAY_MS);
        }
      }
      console.info(`[eob-enqueue] Phase 2 complete: fired ${_enqueuedJobs.length} workers (fire-and-forget)`);
    } catch (e) {
      console.warn("[eob-enqueue] worker phase error (non-fatal):", e);
    }

    // Nudge document to "processing" now that all workers are in flight.
    // Workers will transition the document to completed/failed/partial_failure
    // via their RPC calls as they finish — we just need to clear "queued".
    try {
      const { data: docCheck } = await supabase
        .from("eob_documents")
        .select("status")
        .eq("id", _docId)
        .single();

      if (docCheck?.status !== "completed" && docCheck?.status !== "partial_failure" && docCheck?.status !== "failed") {
        await supabase
          .from("eob_documents")
          .update({ status: "processing", updated_at: new Date().toISOString() })
          .eq("id", _docId);
        console.info(`[eob-enqueue] nudged document to 'processing'`);
      }
    } catch (e) {
      console.warn("[eob-enqueue] status nudge failed (non-fatal):", e);
    }

    // NOTE: Drive file move to Processed folder is intentionally omitted here.
    // With fire-and-forget workers, the document won't be "completed" yet when
    // this background task finishes (~12s). The eob-sweeper handles post-completion
    // cleanup including the Drive file move.

    console.info(`[eob-enqueue] background task done: ${_enqueuedJobs.length} workers fired for doc ${_docId}`);
  })();

  // Register background task — keeps running after HTTP response returns
  // @ts-ignore — EdgeRuntime is a global in the Supabase Deno runtime
  EdgeRuntime.waitUntil(backgroundTask);

  // 10) Immediate response — caller gets 200 right away; document is "queued"
  return json({
    success: true,
    eob_document_id,
    practice_id,
    total_pages: totalPages,
    jobs_enqueued: enqueuedJobs.length,
    message: `Split ${totalPages} pages into ${enqueuedJobs.length} jobs. Workers processing in background.`,
  });
});

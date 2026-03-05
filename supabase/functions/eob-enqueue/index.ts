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
// GCP Authentication (for GCS download when using bucket/object path)
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
    scope: "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/drive",
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
  // 1b) Archive original to eob-uploads for non-storage sources
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
      .update({ status: "queued", updated_at: nowIso, error_message: null, total_pages: totalPages })
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

  try {
    for (let i = 0; i < totalPages; i++) {
      const pageNumber = i + 1;
      const pageName = `page-${String(pageNumber).padStart(3, "0")}.pdf`;
      const pagePath = `${eob_document_id}/${pageName}`;

      // ── A. Split & Upload page ──
      if (existingObjects.has(pageName)) {
        console.info(`[eob-enqueue] skipping upload for ${pagePath} (exists)`);
      } else {
        // Physically create single-page PDF
        const newPdf = await PDFDocument.create();
        const [page] = await newPdf.copyPages(pdfDoc, [i]);
        newPdf.addPage(page);
        const pageBytes = await newPdf.save();

        // Upload to eob-pages bucket
        const { error: uploadErr } = await supabase.storage
          .from(STORAGE_BUCKET)
          .upload(pagePath, pageBytes, { contentType: "application/pdf", upsert: true });

        if (uploadErr) {
          console.error("[eob-enqueue] upload error", { pagePath, message: uploadErr.message });
          await refundCredits();
          return json({ error: `Failed to upload page ${pageNumber}` }, 500);
        }

        console.info(`[eob-enqueue] uploaded ${pagePath}`);
      }

      // ── B. Check for existing job (idempotency) ──
      try {
        const { data: existingJob, error: jobCheckErr } = await supabase
          .from("eob_page_jobs")
          .select("id,status")
          .eq("eob_document_id", eob_document_id)
          .eq("page_number", pageNumber)
          .limit(1);

        if (jobCheckErr) {
          console.warn("[eob-enqueue] job existence check error", jobCheckErr);
        } else if (Array.isArray(existingJob) && existingJob.length > 0) {
          console.info(`[eob-enqueue] job already exists for page ${pageNumber}, skipping enqueue`);
          continue;
        }
      } catch (e) {
        console.warn("[eob-enqueue] job existence check thrown", e);
      }

      // ── C. Enqueue page job via RPC ──
      let jobId: string | null = null;
      try {
        const { data: enqueueResult, error: enqueueErr } = await supabase.rpc("enqueue_eob_page_job", {
          p_eob_document_id: eob_document_id,
          p_practice_id: practice_id,
          p_page_number: pageNumber,
          p_total_pages: totalPages,
          p_page_storage_bucket: STORAGE_BUCKET,
          p_page_storage_path: pagePath,
          p_run_after: nowIso,
          p_file_name: file_name,  // denormalized from eob_documents
        });

        if (enqueueErr) {
          console.error("[eob-enqueue] enqueue_eob_page_job rpc error", enqueueErr);
          await refundCredits();
          return json({ error: "Failed to enqueue page job", detail: enqueueErr.message }, 500);
        }

        // Capture the job ID returned by the RPC (if it returns one)
        jobId = enqueueResult?.id || enqueueResult || null;
        console.info(`[eob-enqueue] enqueued job for ${pagePath}, jobId: ${jobId}`);
        enqueuedJobs.push({ jobId: jobId!, pageNumber });
      } catch (e) {
        console.error("[eob-enqueue] enqueue rpc thrown", e);
        await refundCredits();
        return json({ error: "Failed to enqueue page job" }, 500);
      }
    } // end enqueue loop
  } catch (e) {
    console.error("[eob-enqueue] enqueue phase error", e);
    await refundCredits();
    return json({ error: "Enqueue phase error" }, 500);
  }

  console.info(`[eob-enqueue] Phase 1 complete: ${enqueuedJobs.length} jobs enqueued for ${totalPages} pages`);

  // ──────────────────────────────────────────────────────────────
  // 7) PHASE 2: Fire workers in batches, AWAIT each batch fully.
  //    Previously used AbortController fire-and-forget, but the 3s
  //    abort often fired before the request reached the Supabase
  //    gateway, leaving pages stuck as "queued" forever.
  //    Now we await the full worker response. Each worker takes
  //    5-10s (Gemini), so batches of 3 with delays keeps us well
  //    within the 150s edge function timeout.
  //    trigger-eob-parser already returned to the caller, so the
  //    user isn't waiting for this.
  // ──────────────────────────────────────────────────────────────
  const BATCH_SIZE = 3;          // fire 3 workers at a time (conservative to avoid Gemini 429)
  const BATCH_DELAY_MS = 3000;   // wait 3s between batches to respect Gemini rate limits
  const workerResults: Array<{ page: number; status: string; items?: number }> = [];

  try {
    for (let b = 0; b < enqueuedJobs.length; b += BATCH_SIZE) {
      const batch = enqueuedJobs.slice(b, b + BATCH_SIZE);

      const batchPromises = batch.map(async ({ jobId, pageNumber }) => {
        const workerPayload = {
          job: {
            id: jobId,
            eob_document_id: eob_document_id,
            page_number: pageNumber,
            practice_id: practice_id,
            file_name: file_name,  // denormalized for BQ inserts
          }
        };
        try {
          const response = await fetch(`${SUPABASE_URL}/functions/v1/eob-worker`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            },
            body: JSON.stringify(workerPayload),
          });
          const result = await response.json();
          if (response.ok) {
            console.info(`[eob-enqueue] worker succeeded for page ${pageNumber}: ${result.count} items`);
            return { page: pageNumber, status: 'succeeded', items: result.count || 0 };
          } else {
            console.warn(`[eob-enqueue] worker returned error for page ${pageNumber}: ${result.details || result.error}`);
            return { page: pageNumber, status: 'worker_error' };
          }
        } catch (e: any) {
          console.warn(`[eob-enqueue] worker fetch failed for page ${pageNumber}: ${e.message}`);
          return { page: pageNumber, status: 'trigger_error' };
        }
      });

      const batchResults = await Promise.allSettled(batchPromises);
      for (const result of batchResults) {
        if (result.status === 'fulfilled') {
          workerResults.push(result.value);
        }
      }

      // Rate limit between batches
      if (b + BATCH_SIZE < enqueuedJobs.length) {
        await sleep(BATCH_DELAY_MS);
      }
    }
  } catch (e) {
    console.warn("[eob-enqueue] worker phase error (non-fatal):", e);
  }

  const succeededCount = workerResults.filter(r => r.status === 'succeeded').length;
  const totalItems = workerResults.reduce((sum, r) => sum + (r.items || 0), 0);
  console.info(`[eob-enqueue] Phase 2 complete: ${succeededCount}/${workerResults.length} workers succeeded, ${totalItems} items extracted`);

  // 8) Final status check — only update to "processing" if workers haven't
  //    already completed the document (succeed_eob_page_job auto-sets "completed")
  try {
    const { data: docCheck } = await supabase
      .from("eob_documents")
      .select("status")
      .eq("id", eob_document_id)
      .single();

    if (docCheck?.status !== "completed" && docCheck?.status !== "partial_failure" && docCheck?.status !== "failed") {
      await supabase
        .from("eob_documents")
        .update({ status: "processing", updated_at: new Date().toISOString() })
        .eq("id", eob_document_id);
    }
  } catch (e) {
    console.warn("[eob-enqueue] final eob_documents status check failed", e);
  }

  // ──────────────────────────────────────────────────────────────
  // 9) POST-PROCESSING: Move Google Drive file to "Processed" folder
  //    Only runs when source was Google Drive AND document completed.
  //    Non-fatal: if the move fails, processing results are preserved.
  // ──────────────────────────────────────────────────────────────
  if (gdrive_file_id) {
    try {
      // Re-read definitive document status (succeed_eob_page_job may have updated it)
      const { data: finalDoc } = await supabase
        .from("eob_documents")
        .select("status")
        .eq("id", eob_document_id)
        .single();

      if (finalDoc?.status === "completed") {
        // Check practice settings for auto_move_processed
        const { data: ps } = await supabase
          .from("practice_settings")
          .select("auto_move_processed, gdrive_folder_id, gdrive_processed_folder_id")
          .eq("practice_id", practice_id)
          .single();

        if (ps?.auto_move_processed && ps?.gdrive_folder_id) {
          const GCP_SA_JSON_STR = Deno.env.get("GCP_SA_JSON");
          if (GCP_SA_JSON_STR) {
            const sa = JSON.parse(GCP_SA_JSON_STR.trim());
            const gToken = await getGoogleAccessToken(sa);
            const sourceFolderId = ps.gdrive_folder_id;
            let processedFolderId = ps.gdrive_processed_folder_id;

            // Auto-discover or create the "Processed" subfolder
            if (!processedFolderId) {
              const searchQ = encodeURIComponent(
                `name='Processed' and '${sourceFolderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`
              );
              const searchResp = await fetch(
                `https://www.googleapis.com/drive/v3/files?q=${searchQ}&fields=files(id,name)&supportsAllDrives=true&includeItemsFromAllDrives=true`,
                { headers: { "Authorization": `Bearer ${gToken}` } }
              );
              const searchData = await searchResp.json();

              if (searchData.files?.length > 0) {
                processedFolderId = searchData.files[0].id;
                console.info("[eob-enqueue] found existing Processed folder:", processedFolderId);
              } else {
                // Create the "Processed" subfolder
                const createResp = await fetch(
                  "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true",
                  {
                    method: "POST",
                    headers: { "Authorization": `Bearer ${gToken}`, "Content-Type": "application/json" },
                    body: JSON.stringify({
                      name: "Processed",
                      mimeType: "application/vnd.google-apps.folder",
                      parents: [sourceFolderId],
                    }),
                  }
                );
                const createData = await createResp.json();
                if (createResp.ok && createData.id) {
                  processedFolderId = createData.id;
                  console.info("[eob-enqueue] created Processed folder:", processedFolderId);
                } else {
                  console.warn("[eob-enqueue] failed to create Processed folder:", createData);
                }
              }

              // Cache the discovered/created folder ID
              if (processedFolderId) {
                await supabase
                  .from("practice_settings")
                  .update({ gdrive_processed_folder_id: processedFolderId })
                  .eq("practice_id", practice_id);
              }
            }

            // Move the file: remove from source parent, add to Processed parent
            if (processedFolderId) {
              const fileMetaResp = await fetch(
                `https://www.googleapis.com/drive/v3/files/${gdrive_file_id}?fields=parents&supportsAllDrives=true`,
                { headers: { "Authorization": `Bearer ${gToken}` } }
              );
              const fileMeta = await fileMetaResp.json();
              const currentParents = (fileMeta.parents || []).join(",");

              const moveResp = await fetch(
                `https://www.googleapis.com/drive/v3/files/${gdrive_file_id}?addParents=${processedFolderId}&removeParents=${currentParents}&supportsAllDrives=true`,
                {
                  method: "PATCH",
                  headers: { "Authorization": `Bearer ${gToken}`, "Content-Type": "application/json" },
                  body: JSON.stringify({}),
                }
              );

              if (moveResp.ok) {
                console.info(`[eob-enqueue] moved file ${gdrive_file_id} to Processed folder`);
              } else {
                const moveErr = await moveResp.text();
                console.warn(`[eob-enqueue] move to Processed failed (non-fatal): ${moveResp.status} ${moveErr}`);
              }
            }
          }
        }
      } else {
        console.info(`[eob-enqueue] skipping move — document status is ${finalDoc?.status}, not completed`);
      }
    } catch (e) {
      console.warn("[eob-enqueue] post-processing move failed (non-fatal):", e);
    }
  }

  // 10) Return success summary
  console.info(`[eob-enqueue] complete: ${totalPages} pages, ${succeededCount}/${workerResults.length} workers succeeded, ${totalItems} items`);
  return json({
    success: true,
    eob_document_id,
    practice_id,
    total_pages: totalPages,
    jobs_enqueued: enqueuedJobs.length,
    workers_completed: workerResults.length,
    workers_succeeded: succeededCount,
    total_items_extracted: totalItems,
    message: `Split ${totalPages} pages, ${succeededCount}/${workerResults.length} workers succeeded, ${totalItems} items extracted.`,
  });
});

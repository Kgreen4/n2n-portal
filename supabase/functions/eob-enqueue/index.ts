// eob-enqueue.js — PDF splitting orchestrator
// Downloads a multi-page EOB PDF (from signed URL or GCS), splits into individual
// pages, uploads each to Supabase Storage, enqueues page jobs, and fire-and-forget
// triggers eob-worker for each page with a 500ms delay for rate limiting.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { PDFDocument } from "npm:pdf-lib@1.17.1";

const MAX_PAGES_PER_DOC = 500;
const STORAGE_BUCKET = "eob-pages";
const WORKER_DELAY_MS = 500; // Delay between worker triggers to avoid Gemini 429

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
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
// Helper: sleep for rate limiting
// ──────────────────────────────────────────────────────────────
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// ──────────────────────────────────────────────────────────────
// Main Handler
// ──────────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
      },
    });
  }

  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

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

  // Require practice_id + eob_document_id + (signed_pdf_url OR gcs_bucket+gcs_object_name)
  if (!practice_id || !eob_document_id) {
    return json({ error: "Missing practice_id or eob_document_id" }, 400);
  }
  if (!signed_pdf_url && !(gcs_bucket && gcs_object_name)) {
    return json({ error: "Missing signed_pdf_url or (gcs_bucket + gcs_object_name)" }, 400);
  }

  console.info("[eob-enqueue] start", { practice_id, eob_document_id });

  // Helper to refund credits on error (best-effort)
  const refundCredits = async () => {
    try {
      await supabase.rpc("refund_parsing_credit", { p_practice_id: practice_id });
    } catch (e) {
      console.error("[eob-enqueue] refund_parsing_credit failed", e);
    }
  };

  // ──────────────────────────────────────────────────────────────
  // 1) Download PDF — from signed URL or GCS bucket/object path
  // ──────────────────────────────────────────────────────────────
  let pdfBytes: Uint8Array;
  try {
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
      return json({ error: "Failed to fetch PDF", status: resp.status, statusText: resp.statusText }, 400);
    }
    pdfBytes = new Uint8Array(await resp.arrayBuffer());
  } catch (e) {
    console.error("[eob-enqueue] fetch error", e);
    return json({ error: "Failed to download PDF" }, 400);
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
      .update({ status: "queued", updated_at: nowIso, error_message: null })
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
  // 6) Process pages: split, upload, enqueue, and fire workers
  // ──────────────────────────────────────────────────────────────
  const workerTriggers: Array<{ page: number; status: string }> = [];

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
        });

        if (enqueueErr) {
          console.error("[eob-enqueue] enqueue_eob_page_job rpc error", enqueueErr);
          await refundCredits();
          return json({ error: "Failed to enqueue page job", detail: enqueueErr.message }, 500);
        }

        // Capture the job ID returned by the RPC (if it returns one)
        jobId = enqueueResult?.id || enqueueResult || null;
        console.info(`[eob-enqueue] enqueued job for ${pagePath}, jobId: ${jobId}`);
      } catch (e) {
        console.error("[eob-enqueue] enqueue rpc thrown", e);
        await refundCredits();
        return json({ error: "Failed to enqueue page job" }, 500);
      }

      // ── D. Trigger eob-worker for this page ──
      // We await the fetch to ensure the request is delivered before the
      // edge function runtime exits. We don't wait for the worker to finish
      // processing — just confirm the trigger was accepted (HTTP 2xx).
      const workerPayload = {
        job: {
          id: jobId,
          eob_document_id: eob_document_id,
          page_number: pageNumber,
          practice_id: practice_id,  // Pass directly so worker skips DB lookup
        }
      };

      try {
        const wResp = await fetch(`${SUPABASE_URL}/functions/v1/eob-worker`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          },
          body: JSON.stringify(workerPayload),
        });
        // Read and discard the response body so the connection is released,
        // but don't block on the worker's full processing time.
        // Note: Supabase edge functions respond immediately with the return value,
        // so this await is fast — the worker runs its Gemini call synchronously
        // before returning.
        const wResult = await wResp.text();
        console.info(`[eob-enqueue] worker page ${pageNumber}: HTTP ${wResp.status}`);
        workerTriggers.push({ page: pageNumber, status: wResp.ok ? 'triggered' : 'trigger_error' });
      } catch (e) {
        console.warn(`[eob-enqueue] worker trigger failed for page ${pageNumber}:`, e.message);
        workerTriggers.push({ page: pageNumber, status: 'trigger_error' });
      }

      // ── E. Rate limiting: 500ms delay between worker triggers ──
      if (i < totalPages - 1) {
        await sleep(WORKER_DELAY_MS);
      }

    } // end pages loop
  } catch (e) {
    console.error("[eob-enqueue] processing loop error", e);
    await refundCredits();
    return json({ error: "Processing error" }, 500);
  }

  // 7) Final status check — only update to "processing" if workers haven't
  //    already completed the document (succeed_eob_page_job auto-sets "completed")
  try {
    const { data: docCheck } = await supabase
      .from("eob_documents")
      .select("status")
      .eq("id", eob_document_id)
      .single();

    if (docCheck?.status !== "completed") {
      await supabase
        .from("eob_documents")
        .update({ status: "processing", updated_at: new Date().toISOString() })
        .eq("id", eob_document_id);
    }
  } catch (e) {
    console.warn("[eob-enqueue] final eob_documents status check failed", e);
  }

  // 8) Return success summary
  console.info(`[eob-enqueue] complete: ${totalPages} pages split, uploaded, enqueued, and workers triggered`);
  return json({
    success: true,
    eob_document_id,
    practice_id,
    total_pages: totalPages,
    workers_triggered: workerTriggers.length,
    message: `Split ${totalPages} pages, uploaded to storage, enqueued jobs, and triggered ${workerTriggers.length} workers.`,
  });
});

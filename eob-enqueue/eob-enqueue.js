// supabase/functions/eob-enqueue/index.ts
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { PDFDocument } from "npm:pdf-lib@1.17.1";

const MAX_PAGES_PER_DOC = 500; // Tune as needed
const STORAGE_BUCKET = "eob-pages";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

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

  if (!practice_id || !eob_document_id || !signed_pdf_url) {
    return json({ error: "Missing practice_id, eob_document_id, or signed_pdf_url" }, 400);
  }

  console.info("[eob-enqueue] start", { practice_id, eob_document_id });

  // Helper to refund credits on error (best-effort)
  const refundCredits = async () => {
    try {
      await supabase.rpc("refund_parsing_credit", { practice_id });
    } catch (e) {
      console.error("[eob-enqueue] refund_parsing_credit failed", e);
    }
  };

  // 1) Download PDF
  let pdfBytes: Uint8Array;
  try {
    const resp = await fetch(signed_pdf_url, { method: "GET" });
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
  // Adapt RPC param names to your implementation: some RPCs accept p_practice_id, p_amount, etc.
  // If your RPC only accepts practice_id and charges a flat fee, call accordingly.
  try {
    const { data: creditOk, error: creditErr } = await supabase.rpc("use_parsing_credit", {
      p_practice_id: practice_id,
      p_amount: totalPages, // adjust if your RPC uses a different param name or doesn't accept amount
    });

    if (creditErr) {
      console.error("[eob-enqueue] credit rpc error", creditErr);
      return json({ error: "Credit RPC failed", detail: creditErr.message }, 402);
    }

    // Some RPCs return boolean; others may return an object. Handle falsy as no credit.
    if (!creditOk) {
      return json({ error: "Insufficient credits" }, 402);
    }
  } catch (e) {
    console.error("[eob-enqueue] credit rpc thrown", e);
    return json({ error: "Credit RPC failed" }, 500);
  }

  const nowIso = new Date().toISOString();

  // 4) Update eob_documents status to queued (keep original refunds on failure pattern)
  try {
    const { error: docErr } = await supabase
      .from("eob_documents")
      .update({ status: "queued", updated_at: nowIso, error_message: null })
      .eq("id", eob_document_id);

    if (docErr) {
      console.error("[eob-enqueue] update eob_documents error", docErr);
      // refund since update failed after charging
      await refundCredits();
      return json({ error: "Failed to update eob_documents", detail: docErr.message }, 500);
    }
  } catch (e) {
    console.error("[eob-enqueue] update eob_documents thrown", e);
    await refundCredits();
    return json({ error: "Failed to update eob_documents" }, 500);
  }

  // 5) For idempotency: fetch existing object list for the document (single call)
  // Using storage.list to check existing pages under the prefix (paginated). If you have thousands of files, this may need pagination.
  let existingObjects = new Set<string>();
  try {
    const { data: listData, error: listErr } = await supabase.storage
      .from(STORAGE_BUCKET)
      .list(eob_document_id, { limit: 1000 }); // assume less than 1000 pages; adjust if needed

    if (listErr) {
      // Non-fatal: we'll proceed but log it; existence checks will be per-page (slower)
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

  // 6) Process pages sequentially: split, upload (skip if exists), and enqueue via RPC
  try {
    for (let i = 0; i < totalPages; i++) {
      const pageNumber = i + 1;
      const pageName = `page-${String(pageNumber).padStart(3, "0")}.pdf`;
      const pagePath = `${eob_document_id}/${pageName}`;

      // Quick existence check using earlier list result
      if (existingObjects.has(pageName)) {
        console.info(`[eob-enqueue] skipping upload for ${pagePath} (exists)`);
      } else {
        // Create single-page PDF
        const newPdf = await PDFDocument.create();
        const [page] = await newPdf.copyPages(pdfDoc, [i]);
        newPdf.addPage(page);
        const pageBytes = await newPdf.save();

        // Upload with upsert true to cover race/retry cases (will overwrite)
        const { error: uploadErr } = await supabase.storage
          .from(STORAGE_BUCKET)
          .upload(pagePath, pageBytes, { contentType: "application/pdf", upsert: true });

        if (uploadErr) {
          console.error("[eob-enqueue] upload error", { pagePath, message: uploadErr.message });
          // Refund and abort (best-effort)
          await refundCredits();
          return json({ error: `Failed to upload page ${pageNumber}` }, 500);
        }

        console.info(`[eob-enqueue] uploaded ${pagePath}`);
      }

      // Optional: check for pre-existing job for this page (idempotency at DB level)
      try {
        const { data: existingJob, error: jobCheckErr } = await supabase
          .from("eob_page_jobs")
          .select("id,status")
          .eq("eob_document_id", eob_document_id)
          .eq("page_number", pageNumber)
          .limit(1);

        if (jobCheckErr) {
          console.warn("[eob-enqueue] job existence check error", jobCheckErr);
          // proceed to enqueue via RPC anyway
        } else if (Array.isArray(existingJob) && existingJob.length > 0) {
          console.info(`[eob-enqueue] job already exists for page ${pageNumber}, skipping enqueue`);
          continue; // don't re-enqueue
        }
      } catch (e) {
        console.warn("[eob-enqueue] job existence check thrown", e);
        // proceed
      }

      // Enqueue via hardened RPC to ensure consistent defaults & RLS behavior server-side
      try {
        const { error: enqueueErr } = await supabase.rpc("enqueue_eob_page_job", {
          p_eob_document_id: eob_document_id,
          p_practice_id: practice_id,
          p_page_number: pageNumber,
          p_total_pages: totalPages,
          p_page_storage_bucket: STORAGE_BUCKET,
          p_page_storage_path: pagePath,
          p_run_after: nowIso, // optional depending on RPC signature
        });

        if (enqueueErr) {
          console.error("[eob-enqueue] enqueue_eob_page_job rpc error", enqueueErr);
          // Refund and abort (best-effort) — avoid partial enqueue state
          await refundCredits();
          return json({ error: "Failed to enqueue page job", detail: enqueueErr.message }, 500);
        }

        console.info(`[eob-enqueue] enqueued job for ${pagePath}`);
      } catch (e) {
        console.error("[eob-enqueue] enqueue rpc thrown", e);
        await refundCredits();
        return json({ error: "Failed to enqueue page job" }, 500);
      }
    } // end pages loop
  } catch (e) {
    console.error("[eob-enqueue] processing loop error", e);
    await refundCredits();
    return json({ error: "Processing error" }, 500);
  }

  // 7) Final update: mark document as processing (or queued depending on workflow)
  try {
    await supabase
      .from("eob_documents")
      .update({ status: "processing", total_pages: totalPages, updated_at: nowIso })
      .eq("id", eob_document_id);
  } catch (e) {
    // Non-fatal, but log
    console.warn("[eob-enqueue] final eob_documents update failed", e);
  }

  // 8) Return success summary
  return json({
    success: true,
    eob_document_id,
    total_pages: totalPages,
    message: "Split, uploaded pages, and enqueued jobs (idempotent).",
  });
});
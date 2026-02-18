import { PDFDocument } from "npm:pdf-lib@1.17.1";
import { createClient } from "npm:@supabase/supabase-js@2.30.0";

// Minimal JSON helper
function json(resBody: unknown, status = 200) {
  return new Response(JSON.stringify(resBody), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

console.info("split-pdf-pages function starting");

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' } });
  }
  if (req.method !== "POST") return json({ error: "POST only" }, 405);

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return json({ error: "Missing Supabase environment variables" }, 500);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

  try {
    const body = await req.json();
    const { practice_id, eob_document_id, signed_pdf_url } = body ?? {};

    if (!practice_id || !eob_document_id || !signed_pdf_url) {
      return json({ error: "Missing practice_id, eob_document_id, signed_pdf_url" }, 400);
    }

    // 1) Credit check/charge via RPC
    const { data: ok, error: creditErr } = await supabase.rpc("use_parsing_credit", { practice_id });
    if (creditErr) {
      console.error("use_parsing_credit RPC error:", creditErr);
      return json({ error: creditErr.message }, 500);
    }
    if (!ok) return json({ error: "Insufficient credits" }, 402);

    // 2) Mark doc queued
    const { error: updErr } = await supabase
      .from("eob_documents")
      .update({ status: "queued", error_message: null, updated_at: new Date().toISOString() })
      .eq("id", eob_document_id);
    if (updErr) {
      console.error("update eob_documents error:", updErr);
      return json({ error: updErr.message }, 500);
    }

    // 3) Download PDF
    const resp = await fetch(signed_pdf_url);
    if (!resp.ok) throw new Error(`Failed to fetch PDF: ${resp.status} ${resp.statusText}`);
    const pdfBytes = new Uint8Array(await resp.arrayBuffer());

    // 4) Count pages
    const pdfDoc = await PDFDocument.load(pdfBytes);
    const totalPages = pdfDoc.getPageCount();

    // 5) Create one job per page
    const jobs = Array.from({ length: totalPages }, (_, i) => ({
      practice_id,
      eob_document_id,
      page_number: i + 1,
      total_pages: totalPages,
      status: "queued",
      run_after: new Date().toISOString(),
      max_attempts: 5,
    }));

    const { error: jobErr } = await supabase.from("eob_page_jobs").insert(jobs);
    if (jobErr) {
      console.error("insert jobs error:", jobErr);
      throw jobErr;
    }

    return json({ success: true, eob_document_id, total_pages: totalPages });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("handler error:", msg);
    return json({ error: msg }, 500);
  }
});
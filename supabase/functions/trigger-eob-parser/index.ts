// trigger-eob-parser.js — Entry point / dispatcher
// Creates the eob_documents record, logs the handoff in eob_processing_logs,
// then calls eob-enqueue (Supabase edge function) which handles:
//   - Credit charging (per-page, based on actual PDF page count)
//   - PDF splitting into individual pages
//   - Uploading pages to Supabase Storage
//   - Enqueueing page jobs
//   - Firing eob-worker for each page
//
// Supports two PDF source modes:
//   1. GCS:              { gcs_bucket, gcs_object_name }     — Google Cloud Storage
//   2. Supabase Storage: { storage_bucket, storage_path }    — Supabase Storage (frontend uploads)
//
// This function does NOT charge credits — eob-enqueue does, because only
// it knows the actual page count after loading the PDF.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const DEFAULT_GCS_BUCKET = "cardio-metrics-eob-uploads";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

Deno.serve(async (req) => {
  // ── CORS preflight ──
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

  // ── Env check ──
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("[trigger-eob-parser] missing env");
    return json({ error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  try {
    // ── Parse request body ──
    let body: any;
    try {
      body = await req.json();
    } catch (e) {
      return json({ error: "Invalid JSON body" }, 400);
    }

    const practice_id = body?.practice_id;
    const uploaded_by = body?.uploaded_by || null;  // auth.uid() from frontend

    // Source mode 1: GCS (legacy / n8n / GCS bucket watcher)
    const gcs_object_name = body?.gcs_object_name;
    const gcs_bucket = body?.gcs_bucket || DEFAULT_GCS_BUCKET;

    // Source mode 2: Supabase Storage (frontend uploads)
    const storage_bucket = body?.storage_bucket || null;
    const storage_path = body?.storage_path || null;

    const has_gcs = !!gcs_object_name;
    const has_storage = !!storage_bucket && !!storage_path;

    if (!practice_id) {
      return json({ error: "Missing practice_id" }, 400);
    }
    if (!has_gcs && !has_storage) {
      return json({ error: "Must provide either gcs_object_name or (storage_bucket + storage_path)" }, 400);
    }

    // Derive file_name and file_path from whichever source is provided
    const file_path = has_storage ? storage_path : gcs_object_name;
    const file_name = file_path!.split("/").pop();

    console.info("[trigger-eob-parser] start", {
      practice_id,
      source: has_storage ? "supabase_storage" : "gcs",
      file_path,
    });

    // ──────────────────────────────────────────────────────────────
    // 1) Create eob_documents entry (status: 'pending')
    //    eob-enqueue will advance to 'queued' → 'processing'
    // ──────────────────────────────────────────────────────────────
    const { data: docData, error: docError } = await supabase
      .from("eob_documents")
      .insert({
        practice_id,
        file_path,
        file_name,
        status: "pending",
        ...(uploaded_by && { uploaded_by }),
      })
      .select()
      .single();

    if (docError) {
      console.error("[trigger-eob-parser] document creation error:", docError);
      return json({ error: "Failed to create document", details: docError.message }, 500);
    }

    const eob_document_id = docData.id;
    console.info("[trigger-eob-parser] created eob_document", { eob_document_id });

    // ──────────────────────────────────────────────────────────────
    // 2) Create processing log — audit trail for when PDF was
    //    officially handed off to the worker pipeline
    // ──────────────────────────────────────────────────────────────
    const { error: logError } = await supabase
      .from("eob_processing_logs")
      .insert({
        practice_id,
        eob_document_id,
        gcs_object_name: file_path,  // use file_path regardless of source
        status: "pending",
        credits_used: 0,  // actual credits charged by eob-enqueue (per-page)
      });

    if (logError) {
      console.error("[trigger-eob-parser] log creation error:", logError);
      // Mark doc as failed since we can't track it
      await supabase
        .from("eob_documents")
        .update({ status: "failed", error_message: `Log creation failed: ${logError.message}` })
        .eq("id", eob_document_id);
      return json({ error: "Failed to create processing log", details: logError.message }, 500);
    }

    // ──────────────────────────────────────────────────────────────
    // 3) Call eob-enqueue edge function
    //    Passes GCS metadata so eob-enqueue can download directly
    //    from Google Cloud Storage using its own GCP auth.
    // ──────────────────────────────────────────────────────────────
    const enqueuePayload: Record<string, unknown> = {
      practice_id,
      eob_document_id,
    };

    if (has_storage) {
      // Supabase Storage source (frontend uploads)
      enqueuePayload.storage_bucket = storage_bucket;
      enqueuePayload.storage_path = storage_path;
    } else {
      // GCS source (n8n / bucket watcher)
      enqueuePayload.gcs_bucket = gcs_bucket;
      enqueuePayload.gcs_object_name = gcs_object_name;
    }

    console.info("[trigger-eob-parser] calling eob-enqueue", enqueuePayload);

    const enqueueResponse = await fetch(
      `${SUPABASE_URL}/functions/v1/eob-enqueue`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        },
        body: JSON.stringify(enqueuePayload),
      }
    );

    if (!enqueueResponse.ok) {
      const errBody = await enqueueResponse.text();
      console.error("[trigger-eob-parser] eob-enqueue failed:", enqueueResponse.status, errBody);

      // Mark doc + log as failed
      const failMessage = `eob-enqueue failed (HTTP ${enqueueResponse.status}): ${errBody}`;

      await supabase
        .from("eob_documents")
        .update({ status: "failed", error_message: failMessage })
        .eq("id", eob_document_id);

      await supabase
        .from("eob_processing_logs")
        .update({
          status: "failed",
          error_message: failMessage,
          processing_completed_at: new Date().toISOString(),
        })
        .eq("eob_document_id", eob_document_id);

      return json({ error: "eob-enqueue call failed", details: failMessage }, 500);
    }

    const enqueueResult = await enqueueResponse.json();
    console.info("[trigger-eob-parser] eob-enqueue succeeded:", enqueueResult);

    // ──────────────────────────────────────────────────────────────
    // 4) Update processing log to 'processing'
    //    The PDF has been accepted by eob-enqueue, pages are being
    //    split and workers are being fired.
    // ──────────────────────────────────────────────────────────────
    await supabase
      .from("eob_processing_logs")
      .update({
        status: "processing",
        credits_used: enqueueResult?.total_pages || 0,
      })
      .eq("eob_document_id", eob_document_id);

    // ──────────────────────────────────────────────────────────────
    // 5) Get remaining credits for the response
    // ──────────────────────────────────────────────────────────────
    const { data: profile } = await supabase
      .from("practice_credits")
      .select("credits_remaining")
      .eq("practice_id", practice_id)
      .single();

    // ──────────────────────────────────────────────────────────────
    // 6) Return success
    // ──────────────────────────────────────────────────────────────
    return json({
      success: true,
      eob_document_id,
      total_pages: enqueueResult?.total_pages || null,
      workers_triggered: enqueueResult?.workers_triggered || null,
      credits_remaining: profile?.credits_remaining ?? 0,
      status: "processing",
    });

  } catch (error) {
    console.error("[trigger-eob-parser] unhandled error:", error);
    return json({ error: error.message || "Unknown error" }, 500);
  }
});

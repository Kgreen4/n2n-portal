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
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const DEFAULT_GCS_BUCKET = "cardio-metrics-eob-uploads";

Deno.serve(async (req) => {
  // ── CORS preflight ──
  if (req.method === "OPTIONS") return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  function json(data: unknown, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

  // ── Env check ──
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("[trigger-eob-parser] missing env");
    return json({ error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }

  // ── Auth verification (dual-client pattern) ──
  // Verify the caller's JWT when an Authorization header is present.
  // GCS/n8n callers use the service_role_key directly; frontend callers
  // send the user's JWT via supabase.functions.invoke().
  const authHeader = req.headers.get("Authorization");
  let uploaded_by: string | null = null;

  if (authHeader && SUPABASE_ANON_KEY) {
    // Check if this is a user JWT (not the service role key)
    const token = authHeader.replace("Bearer ", "");
    if (token !== SUPABASE_SERVICE_ROLE_KEY) {
      const authClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
        global: { headers: { Authorization: authHeader } },
      });
      const { data: { user }, error: authError } = await authClient.auth.getUser();
      if (authError || !user) {
        console.error("[trigger-eob-parser] auth failed:", authError?.message);
        return json({ error: "Unauthorized" }, 401);
      }
      uploaded_by = user.id;
      console.info("[trigger-eob-parser] authenticated user:", user.id);
    }
  }

  // Service role client for all DB writes (bypasses RLS)
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
    // Use verified user ID from JWT if available, fall back to body field (legacy/n8n)
    if (!uploaded_by) uploaded_by = body?.uploaded_by || null;

    // Source mode 1: GCS (legacy / n8n / GCS bucket watcher)
    const gcs_object_name = body?.gcs_object_name;
    const gcs_bucket = body?.gcs_bucket || DEFAULT_GCS_BUCKET;

    // Source mode 2: Supabase Storage (frontend uploads)
    const storage_bucket = body?.storage_bucket || null;
    const storage_path = body?.storage_path || null;

    // Original file name (from frontend) — preserves spaces, #, etc.
    const original_file_name = body?.original_file_name || null;

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
    // Prefer original_file_name (from frontend) to preserve human-readable name
    // with spaces, #, etc. Fall back to deriving from storage path (GCS/n8n callers)
    const file_name = original_file_name || file_path!.split("/").pop();

    console.info("[trigger-eob-parser] start", {
      practice_id,
      source: has_storage ? "supabase_storage" : "gcs",
      file_path,
    });

    // ──────────────────────────────────────────────────────────────
    // 1) Duplicate detection — block re-upload of same file
    //    (allows re-upload only if previous attempt failed)
    // ──────────────────────────────────────────────────────────────
    const { data: existing } = await supabase
      .from("eob_documents")
      .select("id, status, created_at")
      .eq("practice_id", practice_id)
      .eq("file_name", file_name)
      .neq("status", "failed")
      .maybeSingle();

    if (existing) {
      console.warn("[trigger-eob-parser] duplicate upload blocked:", { file_name, existing_id: existing.id, status: existing.status });
      return json({
        error: "duplicate_upload",
        message: `This file has already been uploaded${existing.status === "completed" ? " and processed" : " and is currently " + existing.status}.`,
        existing_document_id: existing.id,
        existing_status: existing.status,
        uploaded_at: existing.created_at,
      }, 409);
    }

    // ──────────────────────────────────────────────────────────────
    // 2) Create eob_documents entry (status: 'pending')
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
    // 3) Create processing log — audit trail for when PDF was
    //    officially handed off to the worker pipeline
    // ──────────────────────────────────────────────────────────────
    const { error: logError } = await supabase
      .from("eob_processing_logs")
      .insert({
        practice_id,
        eob_document_id,
        gcs_object_name: file_path,  // use file_path regardless of source
        file_name,                   // denormalized for traceability
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
    // 4) Call eob-enqueue edge function
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

    console.info("[trigger-eob-parser] calling eob-enqueue (non-blocking)", enqueuePayload);

    // Non-blocking: race eob-enqueue against an 8-second timeout.
    // eob-enqueue runs as its own edge function invocation — it continues
    // processing even after we return to the frontend. This prevents the
    // frontend from blocking while workers process (especially for large docs).
    const ENQUEUE_TIMEOUT_MS = 8000;
    const enqueuePromise = fetch(
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
    const timeoutPromise = new Promise<Response>((resolve) =>
      setTimeout(() => resolve(new Response(JSON.stringify({ timeout: true }), { status: 202 })), ENQUEUE_TIMEOUT_MS)
    );

    const enqueueResponse = await Promise.race([enqueuePromise, timeoutPromise]);
    let enqueueResult: any = null;
    let enqueueTimedOut = false;

    if (enqueueResponse.status === 202) {
      // Timed out waiting — eob-enqueue is still running in the background
      enqueueTimedOut = true;
      console.info("[trigger-eob-parser] eob-enqueue still running (returned to frontend early)");
    } else if (!enqueueResponse.ok) {
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
    } else {
      enqueueResult = await enqueueResponse.json();
      console.info("[trigger-eob-parser] eob-enqueue completed:", enqueueResult);
    }

    // ──────────────────────────────────────────────────────────────
    // 5) Update processing log to 'processing'
    //    The PDF has been accepted by eob-enqueue (or is still being processed).
    // ──────────────────────────────────────────────────────────────
    await supabase
      .from("eob_processing_logs")
      .update({
        status: "processing",
        credits_used: enqueueResult?.total_pages || 0,
      })
      .eq("eob_document_id", eob_document_id);

    // ──────────────────────────────────────────────────────────────
    // 6) Get remaining credits for the response
    // ──────────────────────────────────────────────────────────────
    const { data: profile } = await supabase
      .from("practice_credits")
      .select("credits_remaining")
      .eq("practice_id", practice_id)
      .single();

    // ──────────────────────────────────────────────────────────────
    // 7) Return success
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

// eob-sweeper.js — Recovery sweeper for stuck/retryable page jobs
// Handles three scenarios:
//   1. Stuck "queued" jobs — created > 5 min ago, worker never fired
//   2. "retryable" jobs — worker failed but has retries remaining
//   3. Orphaned documents — all page jobs terminal but doc still "processing"
//
// Fires workers ONE at a time with delays to avoid Gemini 429.
// Designed to be called by n8n scheduled workflow every 5 minutes,
// or manually via curl for testing.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const WORKER_DELAY_MS = 5000; // 5s between individual worker calls (conservative)

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

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

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return json({ error: "Missing env" }, 500);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const summary = {
    stuck_queued: { found: 0, fired: 0, succeeded: 0 },
    retryable: { found: 0, fired: 0, succeeded: 0 },
    orphaned_docs: { found: 0, completed: 0, partial_failure: 0, failed: 0 },
    credits_refunded: 0,
  };

  try {
    // ──────────────────────────────────────────────────────────────
    // 1) Stuck "queued" jobs — created > 5 min ago, never picked up
    // ──────────────────────────────────────────────────────────────
    const { data: stuckJobs, error: stuckErr } = await supabase
      .from("eob_page_jobs")
      .select("id, eob_document_id, page_number, practice_id")
      .eq("status", "queued")
      .lt("created_at", new Date(Date.now() - 5 * 60 * 1000).toISOString())
      .limit(10);

    if (stuckErr) {
      console.error("[eob-sweeper] stuck query error:", stuckErr);
    } else if (stuckJobs && stuckJobs.length > 0) {
      summary.stuck_queued.found = stuckJobs.length;
      console.info(`[eob-sweeper] found ${stuckJobs.length} stuck queued jobs`);

      for (const job of stuckJobs) {
        const result = await fireWorker(
          SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
          job.id, job.eob_document_id, job.page_number, job.practice_id
        );
        summary.stuck_queued.fired++;
        if (result === 'succeeded') summary.stuck_queued.succeeded++;
        await sleep(WORKER_DELAY_MS);
      }
    }

    // ──────────────────────────────────────────────────────────────
    // 2) "retryable" jobs — worker failed but has retries remaining
    //    Only pick up jobs that haven't been touched in 2+ minutes
    //    (to avoid re-firing something that's already being retried)
    // ──────────────────────────────────────────────────────────────
    const { data: retryableJobs, error: retryErr } = await supabase
      .from("eob_page_jobs")
      .select("id, eob_document_id, page_number, practice_id, attempt_count")
      .eq("status", "retryable")
      .lt("updated_at", new Date(Date.now() - 2 * 60 * 1000).toISOString())
      .limit(10);

    if (retryErr) {
      console.error("[eob-sweeper] retryable query error:", retryErr);
    } else if (retryableJobs && retryableJobs.length > 0) {
      summary.retryable.found = retryableJobs.length;
      console.info(`[eob-sweeper] found ${retryableJobs.length} retryable jobs`);

      for (const job of retryableJobs) {
        // Reset status to queued so the worker picks it up cleanly
        await supabase
          .from("eob_page_jobs")
          .update({ status: "queued", updated_at: new Date().toISOString() })
          .eq("id", job.id);

        const result = await fireWorker(
          SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
          job.id, job.eob_document_id, job.page_number, job.practice_id
        );
        summary.retryable.fired++;
        if (result === 'succeeded') summary.retryable.succeeded++;
        await sleep(WORKER_DELAY_MS);
      }
    }

    // ──────────────────────────────────────────────────────────────
    // 3) Orphaned documents — all page jobs terminal but doc still
    //    "processing" or "queued". This can happen if eob-enqueue
    //    crashed mid-batch or the final status update was missed.
    // ──────────────────────────────────────────────────────────────
    const { data: orphanedDocs, error: orphanErr } = await supabase
      .from("eob_documents")
      .select("id, total_pages, practice_id")
      .in("status", ["processing", "queued"])
      .lt("updated_at", new Date(Date.now() - 5 * 60 * 1000).toISOString())
      .limit(10);

    if (orphanErr) {
      console.error("[eob-sweeper] orphaned docs query error:", orphanErr);
    } else if (orphanedDocs && orphanedDocs.length > 0) {
      for (const doc of orphanedDocs) {
        // Check if all page jobs are in terminal state
        const { data: pageJobs, error: pjErr } = await supabase
          .from("eob_page_jobs")
          .select("status, items_extracted")
          .eq("eob_document_id", doc.id);

        if (pjErr || !pageJobs) continue;

        const totalJobs = pageJobs.length;
        if (totalJobs === 0) continue; // No page jobs yet — not orphaned, maybe still enqueueing

        const terminalJobs = pageJobs.filter(j =>
          j.status === 'succeeded' || j.status === 'failed'
        ).length;
        const succeededJobs = pageJobs.filter(j => j.status === 'succeeded').length;
        const totalItems = pageJobs
          .filter(j => j.status === 'succeeded')
          .reduce((sum, j) => sum + (j.items_extracted || 0), 0);

        // Only finalize if ALL jobs are terminal
        if (terminalJobs < (doc.total_pages || totalJobs)) continue;

        summary.orphaned_docs.found++;
        console.info(`[eob-sweeper] orphaned doc ${doc.id}: ${succeededJobs}/${totalJobs} succeeded`);

        if (succeededJobs === totalJobs) {
          // All succeeded
          await supabase
            .from("eob_documents")
            .update({
              status: "completed",
              items_extracted: totalItems,
              updated_at: new Date().toISOString(),
            })
            .eq("id", doc.id);
          await supabase
            .from("eob_processing_logs")
            .update({ status: "completed", processing_completed_at: new Date().toISOString() })
            .eq("eob_document_id", doc.id);
          summary.orphaned_docs.completed++;
        } else if (succeededJobs > 0) {
          // Partial failure
          await supabase
            .from("eob_documents")
            .update({
              status: "partial_failure",
              items_extracted: totalItems,
              error_code: "partial_failure",
              error_message: `${succeededJobs} of ${totalJobs} pages processed. ${totalJobs - succeededJobs} pages had errors.`,
              updated_at: new Date().toISOString(),
            })
            .eq("id", doc.id);
          await supabase
            .from("eob_processing_logs")
            .update({ status: "partial_failure", processing_completed_at: new Date().toISOString() })
            .eq("eob_document_id", doc.id);
          summary.orphaned_docs.partial_failure++;
        } else {
          // All failed — refund credits
          await supabase
            .from("eob_documents")
            .update({
              status: "failed",
              items_extracted: 0,
              error_code: "all_pages_failed",
              error_message: `All ${totalJobs} pages failed extraction.`,
              updated_at: new Date().toISOString(),
            })
            .eq("id", doc.id);
          await supabase
            .from("eob_processing_logs")
            .update({
              status: "failed",
              error_message: `All ${totalJobs} pages failed extraction.`,
              processing_completed_at: new Date().toISOString(),
            })
            .eq("eob_document_id", doc.id);

          // Refund credits for completely failed documents
          // refund_parsing_credit refunds 1 credit per call, so loop
          try {
            const refundAmount = doc.total_pages || totalJobs;
            for (let i = 0; i < refundAmount; i++) {
              await supabase.rpc("refund_parsing_credit", {
                p_practice_id: doc.practice_id,
              });
            }
            summary.credits_refunded += refundAmount;
            console.info(`[eob-sweeper] refunded ${refundAmount} credits for doc ${doc.id}`);
          } catch (e) {
            console.error(`[eob-sweeper] credit refund failed for doc ${doc.id}:`, e);
          }

          summary.orphaned_docs.failed++;
        }
      }
    }

    console.info("[eob-sweeper] sweep complete:", JSON.stringify(summary));
    return json({ success: true, ...summary });

  } catch (error: any) {
    console.error("[eob-sweeper] unhandled error:", error);
    return json({ error: error.message || "Unknown error" }, 500);
  }
});

// ──────────────────────────────────────────────────────────────
// Helper: Fire a single worker and await the result
// ──────────────────────────────────────────────────────────────
async function fireWorker(
  supabaseUrl: string,
  serviceRoleKey: string,
  jobId: string,
  eobDocumentId: string,
  pageNumber: number,
  practiceId: string,
): Promise<'succeeded' | 'worker_error' | 'fetch_error'> {
  const workerPayload = {
    job: {
      id: jobId,
      eob_document_id: eobDocumentId,
      page_number: pageNumber,
      practice_id: practiceId,
    },
  };

  try {
    const response = await fetch(`${supabaseUrl}/functions/v1/eob-worker`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${serviceRoleKey}`,
      },
      body: JSON.stringify(workerPayload),
    });
    const result = await response.json();
    if (response.ok) {
      console.info(`[eob-sweeper] worker succeeded for page ${pageNumber} (job ${jobId}): ${result.count} items`);
      return 'succeeded';
    } else {
      console.warn(`[eob-sweeper] worker error for page ${pageNumber}: ${result.details || result.error}`);
      return 'worker_error';
    }
  } catch (e: any) {
    console.warn(`[eob-sweeper] worker fetch failed for page ${pageNumber}: ${e.message}`);
    return 'fetch_error';
  }
}

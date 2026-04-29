import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";
import { getGoogleAccessToken } from "../_shared/gcp-auth.ts";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  function json(data: unknown, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  const GCP_SA_JSON_STR = Deno.env.get("GCP_SA_JSON");

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return json({ error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }
  if (!GCP_SA_JSON_STR) {
    return json({ error: "GCP_SA_JSON not configured" }, 500);
  }

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const practice_id = body?.practice_id;
  if (!practice_id) return json({ error: "Missing practice_id" }, 400);

  // Optional date filter — only pick up files created on/after this date.
  // Accepts "YYYY-MM-DD" or full ISO datetime. When omitted, all files are scanned.
  const after_date: string | null = body?.after_date || null;
  const afterDateTime = after_date
    ? (after_date.includes("T") ? after_date : `${after_date}T00:00:00Z`)
    : null;

  // When true, bypass the "COMPLETED" filename filter and rely solely on the
  // duplicate check against eob_documents. Useful for catch-up runs where the
  // night crew marked files before the pipeline had a chance to process them.
  const include_completed: boolean = body?.include_completed === true;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // 1. Look up configured Drive folder for this practice
  const { data: ps, error: psError } = await supabase
    .from("practice_settings")
    .select("gdrive_folder_id, gdrive_folder_name")
    .eq("practice_id", practice_id)
    .single();

  if (psError || !ps?.gdrive_folder_id) {
    console.warn("[scan-drive-folder] no folder configured:", psError?.message);
    return json({ error: "No Drive folder configured for this practice" }, 400);
  }

  const folderId = ps.gdrive_folder_id;

  // 2. Get GCP access token
  let gToken: string;
  try {
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    gToken = await getGoogleAccessToken(sa);
  } catch (err) {
    console.error("[scan-drive-folder] GCP auth failed:", err);
    return json({ error: "GCP authentication failed" }, 500);
  }

  // 3. List all PDFs in the folder (paginated)
  const driveFiles: Array<{ id: string; name: string }> = [];
  let pageToken: string | undefined;

  do {
    let driveQuery = `'${folderId}' in parents AND mimeType='application/pdf' AND trashed=false`;
    if (afterDateTime) driveQuery += ` AND createdTime > '${afterDateTime}'`;
    const q = encodeURIComponent(driveQuery);
    const url =
      `https://www.googleapis.com/drive/v3/files` +
      `?q=${q}` +
      `&fields=nextPageToken,files(id,name)` +
      `&corpora=allDrives` +
      `&supportsAllDrives=true` +
      `&includeItemsFromAllDrives=true` +
      `&pageSize=1000` +
      (pageToken ? `&pageToken=${pageToken}` : "");

    const resp = await fetch(url, { headers: { Authorization: `Bearer ${gToken}` } });
    if (!resp.ok) {
      const errText = await resp.text();
      console.error("[scan-drive-folder] Drive list error:", resp.status, errText);
      return json({ error: "Failed to list Drive folder", details: errText }, 500);
    }
    const data = await resp.json();
    driveFiles.push(...(data.files || []));
    pageToken = data.nextPageToken;
  } while (pageToken);

  console.info(`[scan-drive-folder] found ${driveFiles.length} PDFs in folder ${folderId}${afterDateTime ? ` after ${afterDateTime}` : ""}${include_completed ? " (include_completed=true)" : ""}`);

  // 4. Split into COMPLETED (skip unless include_completed) vs candidates
  const completedFiles = include_completed
    ? []
    : driveFiles.filter(f => f.name.toUpperCase().includes("COMPLETED"));
  const candidateFiles = include_completed
    ? driveFiles
    : driveFiles.filter(f => !f.name.toUpperCase().includes("COMPLETED"));

  // 5. Check which candidates are already in eob_documents (non-failed)
  let alreadyProcessedNames = new Set<string>();
  if (candidateFiles.length > 0) {
    const { data: existing } = await supabase
      .from("eob_documents")
      .select("file_name")
      .eq("practice_id", practice_id)
      .in("file_name", candidateFiles.map(f => f.name))
      .neq("status", "failed");

    (existing || []).forEach(d => alreadyProcessedNames.add(d.file_name));
  }

  const newFiles = candidateFiles.filter(f => !alreadyProcessedNames.has(f.name));
  const duplicateFiles = candidateFiles.filter(f => alreadyProcessedNames.has(f.name));

  console.info(`[scan-drive-folder] ${completedFiles.length} COMPLETED skipped, ${duplicateFiles.length} duplicate, ${newFiles.length} new`);

  // 6. Trigger processing for each new file (sequential to avoid overloading workers)
  const triggered: string[] = [];
  const triggerErrors: Array<{ name: string; error: string }> = [];

  for (const file of newFiles) {
    try {
      const resp = await fetch(
        `${SUPABASE_URL}/functions/v1/trigger-eob-parser`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          },
          body: JSON.stringify({
            practice_id,
            gdrive_file_id: file.id,
            original_file_name: file.name,
          }),
        }
      );
      if (resp.ok || resp.status === 409) {
        // 409 = duplicate detected at trigger level (race condition) — still counts as handled
        triggered.push(file.name);
        console.info(`[scan-drive-folder] triggered: ${file.name}`);
      } else {
        const errText = await resp.text();
        console.error(`[scan-drive-folder] trigger failed for "${file.name}":`, resp.status, errText);
        triggerErrors.push({ name: file.name, error: errText });
      }
    } catch (err: any) {
      console.error(`[scan-drive-folder] trigger exception for "${file.name}":`, err);
      triggerErrors.push({ name: file.name, error: err.message || "Unknown error" });
    }
  }

  return json({
    folder_name: ps.gdrive_folder_name,
    after_date: after_date || null,
    found: driveFiles.length,
    skipped_completed: completedFiles.length,
    already_processed: duplicateFiles.length,
    triggered: triggered.length,
    errors: triggerErrors.length,
    files: [
      ...triggered.map(n => ({ name: n, status: "triggered" as const })),
      ...duplicateFiles.map(f => ({ name: f.name, status: "duplicate" as const })),
      ...completedFiles.map(f => ({ name: f.name, status: "skipped_completed" as const })),
      ...triggerErrors.map(e => ({ name: e.name, status: "error" as const, error: e.error })),
    ],
  });
});

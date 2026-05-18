// resolve-drive-folder — Lookup a Google Drive folder's name and verify access.
// Called by the Settings page when the user pastes a folder ID.
// Returns { id, name } on success so the UI can auto-fill the display name.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
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

  const GCP_SA_JSON_STR = Deno.env.get("GCP_SA_JSON");
  if (!GCP_SA_JSON_STR) return json({ error: "GCP_SA_JSON not configured" }, 500);

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const folder_id = (body?.folder_id || "").trim();
  if (!folder_id) return json({ error: "Missing folder_id" }, 400);

  let gToken: string;
  try {
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());
    gToken = await getGoogleAccessToken(sa);
  } catch (err) {
    console.error("[resolve-drive-folder] GCP auth failed:", err);
    return json({ error: "GCP authentication failed" }, 500);
  }

  // Fetch folder metadata from Drive API
  const url =
    `https://www.googleapis.com/drive/v3/files/${folder_id}` +
    `?fields=id,name,mimeType` +
    `&supportsAllDrives=true`;

  const resp = await fetch(url, {
    headers: { Authorization: `Bearer ${gToken}` },
  });

  if (resp.status === 404) {
    return json({ error: "Folder not found or not accessible to the service account" }, 404);
  }
  if (!resp.ok) {
    const errText = await resp.text();
    console.error("[resolve-drive-folder] Drive API error:", resp.status, errText);
    return json({ error: `Drive API error: ${resp.status}` }, 502);
  }

  const data = await resp.json();

  if (data.mimeType !== "application/vnd.google-apps.folder") {
    return json({ error: "The ID provided is not a folder" }, 400);
  }

  return json({ id: data.id, name: data.name });
});

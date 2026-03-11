// google-drive-setup — One-time Google OAuth + Drive folder sharing
// Called during onboarding after Google OAuth consent.
//
// Body params:
//   { code: string, folder_id: string, practice_id: string }
//   code       — OAuth authorization code from Google's callback
//   folder_id  — Google Drive folder ID the user selected
//   practice_id — Supabase practice ID to save settings for
//
// Flow:
//   1. Exchange OAuth code for access_token
//   2. GET folder name from Drive API
//   3. Share folder with service account (writer access)
//   4. UPSERT practice_settings with folder info + watcher_enabled=true
//   5. Return { success: true, folder_name }
//
// NOTE: OAuth tokens are NOT stored — used once and discarded.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const GOOGLE_OAUTH_CLIENT_ID = Deno.env.get("GOOGLE_OAUTH_CLIENT_ID")!;
const GOOGLE_OAUTH_CLIENT_SECRET = Deno.env.get("GOOGLE_OAUTH_CLIENT_SECRET")!;
const GOOGLE_SERVICE_ACCOUNT_EMAIL = Deno.env.get("GOOGLE_SERVICE_ACCOUNT_EMAIL")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Must match the redirect_uri used when initiating OAuth on the frontend
const REDIRECT_URI = "https://n2n-portal.vercel.app/onboarding/callback";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return corsResponse(req);
  const corsHeaders = getCorsHeaders(req);

  function json(data: unknown, status = 200) {
    return new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  try {
    if (req.method !== "POST") return json({ error: "Use POST" }, 405);

    if (!GOOGLE_OAUTH_CLIENT_ID || !GOOGLE_OAUTH_CLIENT_SECRET || !GOOGLE_SERVICE_ACCOUNT_EMAIL) {
      return json({ error: "Google OAuth not configured" }, 503);
    }

    // Verify the user's Supabase JWT
    const authHeader = req.headers.get("Authorization");
    if (!authHeader) return json({ error: "Unauthorized" }, 401);

    const authClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      global: { headers: { Authorization: authHeader } },
    });
    const { data: { user }, error: userError } = await authClient.auth.getUser();
    if (userError || !user) {
      console.error("[google-drive-setup] Auth error:", userError);
      return json({ error: "Unauthorized" }, 401);
    }

    let body: any;
    try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

    const { code, folder_id, practice_id } = body;
    if (!code || !folder_id || !practice_id) {
      return json({ error: "Missing code, folder_id, or practice_id" }, 400);
    }

    // Verify this practice belongs to the user
    const adminClient = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    const { data: link } = await adminClient
      .from("practice_users")
      .select("practice_id")
      .eq("user_id", user.id)
      .eq("practice_id", practice_id)
      .single();
    if (!link) return json({ error: "Practice not found or unauthorized" }, 403);

    // ── Step 1: Exchange OAuth code for access_token ──────────────────────────
    const tokenResp = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_OAUTH_CLIENT_ID,
        client_secret: GOOGLE_OAUTH_CLIENT_SECRET,
        redirect_uri: REDIRECT_URI,
        grant_type: "authorization_code",
      }),
    });
    const tokenData = await tokenResp.json();
    if (!tokenResp.ok) {
      console.error("[google-drive-setup] Token exchange error:", JSON.stringify(tokenData));
      return json({ error: "Failed to exchange OAuth code — please try again", details: tokenData.error_description }, 400);
    }
    const accessToken = tokenData.access_token;

    // ── Step 2: Get folder name ───────────────────────────────────────────────
    const folderResp = await fetch(
      `https://www.googleapis.com/drive/v3/files/${folder_id}?fields=name,mimeType`,
      { headers: { Authorization: `Bearer ${accessToken}` } }
    );
    const folderData = await folderResp.json();
    if (!folderResp.ok) {
      console.error("[google-drive-setup] Folder fetch error:", JSON.stringify(folderData));
      const detail = folderData.error?.message ?? "Unknown error";
      return json({
        error: `Could not access folder — make sure you granted Google Drive access and the folder exists. (${detail})`,
      }, 400);
    }
    const folderName: string = folderData.name ?? "PAYMENTS";

    // ── Step 3: Share folder with service account ─────────────────────────────
    const permResp = await fetch(
      `https://www.googleapis.com/drive/v3/files/${folder_id}/permissions`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          role: "writer",
          type: "user",
          emailAddress: GOOGLE_SERVICE_ACCOUNT_EMAIL,
        }),
      }
    );
    const permData = await permResp.json();
    if (!permResp.ok) {
      console.error("[google-drive-setup] Permission error:", JSON.stringify(permData));
      const detail = permData.error?.message ?? "Unknown error";
      return json({ error: `Failed to share folder: ${detail}` }, 500);
    }

    // ── Step 4: Save to practice_settings ────────────────────────────────────
    const { error: upsertError } = await adminClient
      .from("practice_settings")
      .upsert(
        {
          practice_id,
          gdrive_folder_id: folder_id,
          gdrive_folder_name: folderName,
          watcher_enabled: true,
        },
        { onConflict: "practice_id" }
      );

    if (upsertError) {
      console.error("[google-drive-setup] Settings upsert error:", upsertError);
      return json({ error: "Failed to save Drive settings", details: upsertError.message }, 500);
    }

    console.info("[google-drive-setup] success:", { practice_id, folder_id, folderName });
    return json({ success: true, folder_name: folderName });

  } catch (err: any) {
    console.error("[google-drive-setup] Unhandled:", err?.message, err?.stack);
    return json({ error: "Internal server error", details: err?.message }, 500);
  }
});

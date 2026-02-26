// create-practice.js — Self-service practice onboarding
// Verifies the user's JWT, creates a practice with a slug,
// links the user as 'owner', and grants 50 starter credits.
// Called from the frontend setup wizard after email verification.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
    },
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

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_SERVICE_ROLE_KEY) {
    return json({ error: "Missing environment variables" }, 500);
  }

  try {
    // 1. Verify the user making the request using their JWT
    const authClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
      global: { headers: { Authorization: req.headers.get("Authorization")! } },
    });

    const { data: { user }, error: userError } = await authClient.auth.getUser();
    if (userError || !user) {
      console.error("[create-practice] auth error:", userError);
      return json({ error: "Unauthorized" }, 401);
    }

    // 2. Parse request body
    const { practiceName } = await req.json();
    if (!practiceName || typeof practiceName !== "string" || practiceName.trim().length === 0) {
      return json({ error: "Practice name is required" }, 400);
    }

    const trimmedName = practiceName.trim();

    // 3. Generate a URL-friendly slug
    const slug =
      trimmedName
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)+/g, "") +
      "-" +
      Math.floor(Math.random() * 1000);

    console.info("[create-practice] creating practice:", { userId: user.id, name: trimmedName, slug });

    // 4. Use service role client to bypass RLS for setup operations
    const adminClient = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // 4A. Check if user already has a practice (prevent duplicates)
    const { data: existingLink } = await adminClient
      .from("practice_users")
      .select("practice_id")
      .eq("user_id", user.id)
      .limit(1);

    if (existingLink && existingLink.length > 0) {
      return json({ error: "You already belong to a practice" }, 409);
    }

    // 4B. Create the practice
    const { data: practice, error: practiceError } = await adminClient
      .from("practices")
      .insert({ name: trimmedName, slug })
      .select("id")
      .single();

    if (practiceError) {
      console.error("[create-practice] insert practice error:", practiceError);
      return json({ error: "Failed to create practice", details: practiceError.message }, 500);
    }

    // 4C. Link the user as owner
    const { error: linkError } = await adminClient
      .from("practice_users")
      .insert({ user_id: user.id, practice_id: practice.id, role: "owner" });

    if (linkError) {
      console.error("[create-practice] link user error:", linkError);
      // Cleanup: delete the orphaned practice
      await adminClient.from("practices").delete().eq("id", practice.id);
      return json({ error: "Failed to link user to practice", details: linkError.message }, 500);
    }

    // 4D. Grant starter credits
    const STARTER_CREDITS = 50;
    const { error: creditsError } = await adminClient
      .from("practice_credits")
      .insert({ practice_id: practice.id, credits_remaining: STARTER_CREDITS, practice_name: trimmedName });

    if (creditsError) {
      console.error("[create-practice] credits error:", creditsError);
      // Non-fatal — practice is created, credits can be added later
    }

    console.info("[create-practice] success:", { practiceId: practice.id, userId: user.id, credits: STARTER_CREDITS });

    return json({
      success: true,
      practiceId: practice.id,
      slug,
      creditsGranted: STARTER_CREDITS,
    });
  } catch (error: any) {
    console.error("[create-practice] unhandled error:", error);
    return json({ error: error.message || "Unknown error" }, 500);
  }
});

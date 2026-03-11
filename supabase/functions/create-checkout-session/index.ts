// create-checkout-session — Creates a Stripe Checkout session
// Used by the /billing page to initiate plan subscriptions or credit pack purchases.
//
// Body params:
//   { practice_id, price_id, mode: 'subscription' | 'payment', credits_to_add?: number }
//
// Returns: { url: string } — redirect the user to this Stripe-hosted checkout URL.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Map Stripe price IDs to plan details (credits/month, max pages per doc)
// These are filled in by the webhook after checkout completes — but we include
// metadata in the session so the webhook knows what was purchased.
const PLAN_CONFIG: Record<string, { tier: string; credits: number; maxPages: number }> = {
  [Deno.env.get("STRIPE_STARTER_PRICE_ID") ?? ""]:      { tier: "starter",      credits: 500,  maxPages: 50  },
  [Deno.env.get("STRIPE_PRO_PRICE_ID") ?? ""]:          { tier: "professional", credits: 2000, maxPages: 150 },
  [Deno.env.get("STRIPE_BOOST100_PRICE_ID") ?? ""]:     { tier: "",             credits: 100,  maxPages: 0   },
  [Deno.env.get("STRIPE_BOOST500_PRICE_ID") ?? ""]:     { tier: "",             credits: 500,  maxPages: 0   },
};

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
    if (!STRIPE_SECRET_KEY) return json({ error: "Stripe not configured" }, 503);

    let body: any;
    try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

    const { practice_id, price_id, mode = "subscription" } = body;
    if (!practice_id || !price_id) return json({ error: "Missing practice_id or price_id" }, 400);

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Get or create Stripe customer for this practice
    const { data: practice, error: dbErr } = await supabase
      .from("practices")
      .select("id, name, stripe_customer_id")
      .eq("id", practice_id)
      .single();

    // PGRST116 = "not found" from .single() — treat as 404, not a real DB error
    if (dbErr && dbErr.code !== "PGRST116") {
      console.error("[create-checkout-session] DB error:", dbErr.code, dbErr.message);
      return json({ error: "Database error", details: dbErr.message }, 500);
    }
    if (!practice) return json({ error: "Practice not found" }, 404);

    let customerId = practice.stripe_customer_id;

    // Create Stripe customer if not yet linked
    if (!customerId) {
      const customerResp = await fetch("https://api.stripe.com/v1/customers", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${STRIPE_SECRET_KEY}`,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          name: practice.name,
          "metadata[practice_id]": practice_id,
        } as any).toString(),
      });
      const customer = await customerResp.json();
      if (!customerResp.ok) {
        console.error("[create-checkout-session] Stripe customer error:", JSON.stringify(customer));
        return json({ error: "Failed to create Stripe customer", details: customer }, 500);
      }
      customerId = customer.id;
      await supabase.from("practices").update({ stripe_customer_id: customerId }).eq("id", practice_id);
    }

    const planConfig = PLAN_CONFIG[price_id];
    const origin = req.headers.get("origin") || "https://n2n-portal.vercel.app";

    // Build Checkout session
    const sessionParams = new URLSearchParams({
      customer: customerId,
      mode,
      "line_items[0][price]": price_id,
      "line_items[0][quantity]": "1",
      success_url: `${origin}/billing?checkout=success`,
      cancel_url: `${origin}/billing?checkout=cancelled`,
      "metadata[practice_id]": practice_id,
      "metadata[purchase_type]": mode === "subscription" ? "subscription" : "credit_pack",
      "metadata[price_id]": price_id,
      ...(planConfig?.credits ? { [`metadata[credits_to_add]`]: String(planConfig.credits) } : {}),
      ...(planConfig?.tier ? { [`metadata[plan_tier]`]: planConfig.tier } : {}),
      ...(planConfig?.maxPages ? { [`metadata[max_pages_per_doc]`]: String(planConfig.maxPages) } : {}),
    });

    const sessionResp = await fetch("https://api.stripe.com/v1/checkout/sessions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${STRIPE_SECRET_KEY}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: sessionParams.toString(),
    });
    const session = await sessionResp.json();

    if (!sessionResp.ok) {
      console.error("[create-checkout-session] Stripe session error:", JSON.stringify(session));
      return json({ error: "Failed to create checkout session", details: session }, 500);
    }

    return json({ url: session.url });
  } catch (err: any) {
    console.error("[create-checkout-session] Unhandled exception:", err?.message, err?.stack);
    return json({ error: "Internal server error", details: err?.message }, 500);
  }
});

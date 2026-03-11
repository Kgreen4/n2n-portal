// create-portal-session — Creates a Stripe Customer Portal session
// Allows existing subscribers to manage their plan, payment method, and invoices.
//
// Body: { practice_id }
// Returns: { url: string } — redirect to Stripe-hosted portal

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { getCorsHeaders, corsResponse } from "../_shared/cors.ts";

const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

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
  if (!STRIPE_SECRET_KEY) return json({ error: "Stripe not configured" }, 503);

  let body: any;
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { practice_id } = body;
  if (!practice_id) return json({ error: "Missing practice_id" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  const { data: practice } = await supabase
    .from("practices")
    .select("stripe_customer_id")
    .eq("id", practice_id)
    .single();

  if (!practice?.stripe_customer_id) {
    return json({ error: "No billing account found. Please subscribe to a plan first." }, 404);
  }

  const origin = req.headers.get("origin") || "https://n2n-portal.vercel.app";

  const portalResp = await fetch("https://api.stripe.com/v1/billing_portal/sessions", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${STRIPE_SECRET_KEY}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      customer: practice.stripe_customer_id,
      return_url: `${origin}/billing`,
    }).toString(),
  });

  const portal = await portalResp.json();
  if (!portalResp.ok) {
    console.error("[create-portal-session] Stripe error:", portal);
    return json({ error: "Failed to create portal session", details: portal }, 500);
  }

  return json({ url: portal.url });
});

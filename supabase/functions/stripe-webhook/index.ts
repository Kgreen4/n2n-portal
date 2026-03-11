// stripe-webhook — Handles Stripe events to manage practice subscriptions and credits.
//
// Events handled:
//   checkout.session.completed  → link Stripe customer + set plan OR add credit pack credits
//   invoice.payment_succeeded   → reset credits to monthly plan amount (subscription renewal)
//   customer.subscription.updated → update plan tier/limits
//   customer.subscription.deleted → downgrade to trial

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY")!;
const STRIPE_WEBHOOK_SECRET = Deno.env.get("STRIPE_WEBHOOK_SECRET")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Plan config — mirror of create-checkout-session
const PLAN_CONFIG: Record<string, { tier: string; credits: number; maxPages: number }> = {
  [Deno.env.get("STRIPE_STARTER_PRICE_ID") ?? ""]:  { tier: "starter",      credits: 500,  maxPages: 50  },
  [Deno.env.get("STRIPE_PRO_PRICE_ID") ?? ""]:      { tier: "professional", credits: 2000, maxPages: 150 },
};

// ── Stripe signature verification (manual HMAC-SHA256, no Stripe SDK needed) ──
async function verifyStripeSignature(payload: string, sigHeader: string, secret: string): Promise<boolean> {
  try {
    const parts = sigHeader.split(",");
    const timestampPart = parts.find(p => p.startsWith("t="));
    const sigPart = parts.find(p => p.startsWith("v1="));
    if (!timestampPart || !sigPart) return false;

    const timestamp = timestampPart.slice(2);
    const expectedSig = sigPart.slice(3);

    const signedPayload = `${timestamp}.${payload}`;
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw", encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig = await crypto.subtle.sign("HMAC", key, encoder.encode(signedPayload));
    const computedSig = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, "0")).join("");

    return computedSig === expectedSig;
  } catch (e) {
    console.error("[stripe-webhook] signature verification error:", e);
    return false;
  }
}

Deno.serve(async (req) => {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const payload = await req.text();
  const sigHeader = req.headers.get("stripe-signature") ?? "";

  if (STRIPE_WEBHOOK_SECRET) {
    const valid = await verifyStripeSignature(payload, sigHeader, STRIPE_WEBHOOK_SECRET);
    if (!valid) {
      console.error("[stripe-webhook] invalid signature");
      return new Response("Invalid signature", { status: 400 });
    }
  } else {
    console.warn("[stripe-webhook] STRIPE_WEBHOOK_SECRET not set — skipping signature check (dev only)");
  }

  let event: any;
  try {
    event = JSON.parse(payload);
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  console.info("[stripe-webhook] event:", event.type);

  try {
    switch (event.type) {
      // ── Checkout completed ──────────────────────────────────
      case "checkout.session.completed": {
        const session = event.data.object;
        const meta = session.metadata ?? {};
        const practiceId = meta.practice_id;
        const purchaseType = meta.purchase_type; // 'subscription' or 'credit_pack'
        const customerId = session.customer;

        if (!practiceId) {
          console.warn("[stripe-webhook] checkout.session.completed: no practice_id in metadata");
          break;
        }

        if (purchaseType === "subscription") {
          const tier = meta.plan_tier ?? "starter";
          const credits = parseInt(meta.credits_to_add ?? "500");
          const maxPages = parseInt(meta.max_pages_per_doc ?? "50");
          const subscriptionId = session.subscription;

          await supabase.from("practices").update({
            stripe_customer_id: customerId,
            stripe_subscription_id: subscriptionId,
            plan_tier: tier,
            plan_credits_per_month: credits,
            plan_max_pages_per_doc: maxPages,
            subscription_status: "active",
          }).eq("id", practiceId);

          // Set initial credit balance (first month)
          await supabase.from("practice_credits")
            .update({ credits_remaining: credits })
            .eq("practice_id", practiceId);

          console.info(`[stripe-webhook] subscription activated: practice=${practiceId} tier=${tier} credits=${credits}`);
        } else if (purchaseType === "credit_pack") {
          const creditsToAdd = parseInt(meta.credits_to_add ?? "0");
          if (creditsToAdd > 0) {
            // ADD to existing balance (do not reset)
            const { data: current } = await supabase
              .from("practice_credits")
              .select("credits_remaining")
              .eq("practice_id", practiceId)
              .single();

            const newBalance = (current?.credits_remaining ?? 0) + creditsToAdd;
            await supabase.from("practice_credits")
              .update({ credits_remaining: newBalance })
              .eq("practice_id", practiceId);

            console.info(`[stripe-webhook] credit pack added: practice=${practiceId} +${creditsToAdd} → ${newBalance}`);
          }
        }
        break;
      }

      // ── Monthly subscription renewal ────────────────────────
      case "invoice.payment_succeeded": {
        const invoice = event.data.object;
        const customerId = invoice.customer;
        const subscriptionId = invoice.subscription;

        // Only reset credits for subscription renewals (not first payment — handled by checkout.session.completed)
        if (!subscriptionId || invoice.billing_reason !== "subscription_cycle") break;

        // Look up practice by customer ID
        const { data: practice } = await supabase
          .from("practices")
          .select("id, plan_credits_per_month")
          .eq("stripe_customer_id", customerId)
          .single();

        if (!practice) {
          console.warn("[stripe-webhook] invoice.payment_succeeded: practice not found for customer", customerId);
          break;
        }

        const credits = practice.plan_credits_per_month ?? 500;
        await supabase.from("practice_credits")
          .update({ credits_remaining: credits })
          .eq("practice_id", practice.id);

        console.info(`[stripe-webhook] monthly renewal: practice=${practice.id} credits reset to ${credits}`);
        break;
      }

      // ── Plan upgrade / downgrade ────────────────────────────
      case "customer.subscription.updated": {
        const sub = event.data.object;
        const customerId = sub.customer;
        const priceId = sub.items?.data?.[0]?.price?.id;

        const config = priceId ? PLAN_CONFIG[priceId] : null;
        if (!config) break;

        const { data: practice } = await supabase
          .from("practices")
          .select("id")
          .eq("stripe_customer_id", customerId)
          .single();

        if (!practice) break;

        await supabase.from("practices").update({
          stripe_subscription_id: sub.id,
          plan_tier: config.tier,
          plan_credits_per_month: config.credits,
          plan_max_pages_per_doc: config.maxPages,
          subscription_status: sub.status,
        }).eq("id", practice.id);

        console.info(`[stripe-webhook] subscription updated: practice=${practice.id} tier=${config.tier}`);
        break;
      }

      // ── Cancellation / expiry ───────────────────────────────
      case "customer.subscription.deleted": {
        const sub = event.data.object;
        const customerId = sub.customer;

        const { data: practice } = await supabase
          .from("practices")
          .select("id")
          .eq("stripe_customer_id", customerId)
          .single();

        if (!practice) break;

        // Downgrade to trial limits
        await supabase.from("practices").update({
          plan_tier: "trial",
          plan_credits_per_month: 50,
          plan_max_pages_per_doc: 10,
          subscription_status: "canceled",
          stripe_subscription_id: null,
        }).eq("id", practice.id);

        // Reset credits to trial allowance
        await supabase.from("practice_credits")
          .update({ credits_remaining: 50 })
          .eq("practice_id", practice.id);

        console.info(`[stripe-webhook] subscription canceled: practice=${practice.id} → downgraded to trial`);
        break;
      }

      default:
        console.info(`[stripe-webhook] unhandled event type: ${event.type}`);
    }
  } catch (err: any) {
    console.error("[stripe-webhook] handler error:", err.message);
    return new Response(JSON.stringify({ error: "Handler error", details: err.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }

  return new Response(JSON.stringify({ received: true }), {
    headers: { "Content-Type": "application/json" },
  });
});

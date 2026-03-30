// get-practice-summary — read-only KPI rollup for a practice
// Queries BigQuery dedup views and returns a flat metrics object.
// Deployed with --no-verify-jwt (called server-to-server by dashboard / n8n).
// All metrics are resilient to empty/missing tables — returns 0/null, never throws.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { getGoogleAccessToken } from "../_shared/gcp-auth.ts";

const GCP_PROJECT  = "cardio-metrics-dev";
const BQ_DATASET   = "billing_audit_practice_test";
const BQ_SCOPE     = "https://www.googleapis.com/auth/bigquery.readonly";
const BQ_QUERY_URL = `https://bigquery.googleapis.com/bigquery/v2/projects/${GCP_PROJECT}/queries`;
const TIMEOUT_MS   = 10_000;

// ── Types ────────────────────────────────────────────────────────────────────

interface PracticeSummary {
  total_ar_balance:                  number;
  claims_this_month:                 number;
  paid_this_month:                   number;
  denial_rate_pct:                   number;
  charge_capture_rate_pct:           number;
  days_in_ar_avg:                    number;
  avg_submission_lag_days:           number;
  avg_primary_collection_lag_days:   number;
  last_updated:                      string;
}

interface BQQueryParam {
  name:           string;
  parameterType:  { type: string };
  parameterValue: { value: string };
}

interface BQQueryBody {
  query:           string;
  useLegacySql:    boolean;
  timeoutMs:       number;
  queryParameters: BQQueryParam[];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function zeroSummary(): PracticeSummary {
  return {
    total_ar_balance:                0,
    claims_this_month:               0,
    paid_this_month:                 0,
    denial_rate_pct:                 0,
    charge_capture_rate_pct:         0,
    days_in_ar_avg:                  0,
    avg_submission_lag_days:         0,
    avg_primary_collection_lag_days: 0,
    last_updated:                    new Date().toISOString(),
  };
}

/**
 * Execute a single BigQuery synchronous query.
 * Returns the first row's first field value as a number, or null on any error.
 */
async function runBQScalar(
  token:       string,
  query:       string,
  practiceId:  string,
): Promise<number | null> {
  const body: BQQueryBody = {
    query,
    useLegacySql:    false,
    timeoutMs:       TIMEOUT_MS,
    queryParameters: [
      {
        name:           "practice_id",
        parameterType:  { type: "STRING" },
        parameterValue: { value: practiceId },
      },
    ],
  };

  try {
    const resp = await fetch(BQ_QUERY_URL, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type":  "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      console.warn(`[get-practice-summary] BQ query failed (${resp.status}):`, errText.slice(0, 400));
      return null;
    }

    const data = await resp.json();

    // jobComplete = false means the query timed out — treat as null
    if (!data.jobComplete) {
      console.warn("[get-practice-summary] BQ query timed out, jobComplete=false");
      return null;
    }

    const value = data.rows?.[0]?.f?.[0]?.v;
    if (value === null || value === undefined || value === "") return null;

    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  } catch (err) {
    console.warn("[get-practice-summary] BQ fetch error:", String(err));
    return null;
  }
}

// ── Query definitions ────────────────────────────────────────────────────────

// Use parameterized queries (@practice_id) and query the dedup VIEWS exclusively.

const Q_TOTAL_AR_BALANCE = `
  SELECT COALESCE(SUM(billed_amount), 0)
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
    AND LOWER(claim_status) NOT IN ('paid')
`;

const Q_CLAIMS_THIS_MONTH = `
  SELECT COUNT(DISTINCT claim_number)
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
    AND DATE_TRUNC(DATE(ingested_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
`;

const Q_PAID_THIS_MONTH = `
  SELECT COALESCE(SUM(paid_amount), 0)
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
    AND check_date IS NOT NULL
    AND DATE_TRUNC(DATE(check_date), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
`;

// denial_rate_pct: denied rows / all rows * 100
const Q_DENIAL_RATE = `
  SELECT
    CASE WHEN COUNT(*) = 0 THEN 0
         ELSE ROUND(
           COUNTIF(LOWER(claim_status) IN ('denied', 'rejected')) * 100.0 / COUNT(*),
           2
         )
    END
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
`;

// charge_capture_rate_pct:
//   distinct (cpt_code, date_of_service) matched in EOB / distinct in charge report * 100
const Q_CHARGE_CAPTURE = `
  WITH eob_pairs AS (
    SELECT DISTINCT cpt_code, date_of_service
    FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
    WHERE practice_id = @practice_id
      AND cpt_code IS NOT NULL
      AND date_of_service IS NOT NULL
  ),
  charge_pairs AS (
    SELECT DISTINCT cpt_code, date_of_service
    FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_charge_report\`
    WHERE practice_id = @practice_id
      AND cpt_code IS NOT NULL
      AND date_of_service IS NOT NULL
  )
  SELECT
    CASE WHEN (SELECT COUNT(*) FROM charge_pairs) = 0 THEN 0
         ELSE ROUND(
           (SELECT COUNT(*) FROM eob_pairs) * 100.0 /
           (SELECT COUNT(*) FROM charge_pairs),
           2
         )
    END
`;

// days_in_ar_avg: for unpaid claims, average age since date_of_service
const Q_DAYS_IN_AR = `
  SELECT COALESCE(
    AVG(DATE_DIFF(CURRENT_DATE(), DATE(date_of_service), DAY)),
    0
  )
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
    AND LOWER(claim_status) NOT IN ('paid')
    AND date_of_service IS NOT NULL
`;

// avg_submission_lag_days: from charge report — date_submitted minus date_of_service
const Q_SUBMISSION_LAG = `
  SELECT COALESCE(
    AVG(DATE_DIFF(DATE(date_submitted), DATE(date_of_service), DAY)),
    0
  )
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_charge_report\`
  WHERE practice_id = @practice_id
    AND date_submitted IS NOT NULL
    AND date_of_service IS NOT NULL
    AND DATE_DIFF(DATE(date_submitted), DATE(date_of_service), DAY) >= 0
`;

// avg_primary_collection_lag_days: payment_date minus date_of_service for paid claims
const Q_PRIMARY_COLLECTION_LAG = `
  SELECT COALESCE(
    AVG(DATE_DIFF(DATE(payment_date), DATE(date_of_service), DAY)),
    0
  )
  FROM \`${GCP_PROJECT}.${BQ_DATASET}.view_eob_line_items\`
  WHERE practice_id = @practice_id
    AND LOWER(claim_status) = 'paid'
    AND payment_date IS NOT NULL
    AND date_of_service IS NOT NULL
    AND DATE_DIFF(DATE(payment_date), DATE(date_of_service), DAY) >= 0
`;

// ── Main handler ─────────────────────────────────────────────────────────────

Deno.serve(async (req) => {
  if (req.method !== "POST") return json({ error: "Use POST" }, 405);

  const GCP_SA_JSON = Deno.env.get("GCP_SA_JSON");

  // Parse request body
  let practice_id: string;
  try {
    const body = await req.json();
    practice_id = body?.practice_id;
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  if (!practice_id) {
    return json({ error: "practice_id is required" }, 400);
  }

  // If no GCP credentials, return zeros with a warning rather than failing
  if (!GCP_SA_JSON) {
    console.warn("[get-practice-summary] GCP_SA_JSON not set — returning zero summary");
    return json({
      ...zeroSummary(),
      warning: "GCP_SA_JSON not configured — metrics unavailable",
    });
  }

  // Obtain access token once; share across all parallel queries
  let token: string;
  try {
    const sa = JSON.parse(GCP_SA_JSON);
    token = await getGoogleAccessToken(sa, BQ_SCOPE);
  } catch (err) {
    console.error("[get-practice-summary] GCP auth failed:", String(err));
    return json({
      ...zeroSummary(),
      warning: "GCP authentication failed — metrics unavailable",
    });
  }

  // Run all seven scalar queries in parallel
  const [
    totalArBalance,
    claimsThisMonth,
    paidThisMonth,
    denialRatePct,
    chargeCapturePct,
    daysInArAvg,
    submissionLagDays,
    primaryCollectionLagDays,
  ] = await Promise.all([
    runBQScalar(token, Q_TOTAL_AR_BALANCE,       practice_id),
    runBQScalar(token, Q_CLAIMS_THIS_MONTH,       practice_id),
    runBQScalar(token, Q_PAID_THIS_MONTH,         practice_id),
    runBQScalar(token, Q_DENIAL_RATE,             practice_id),
    runBQScalar(token, Q_CHARGE_CAPTURE,          practice_id),
    runBQScalar(token, Q_DAYS_IN_AR,              practice_id),
    runBQScalar(token, Q_SUBMISSION_LAG,          practice_id),
    runBQScalar(token, Q_PRIMARY_COLLECTION_LAG,  practice_id),
  ]);

  const summary: PracticeSummary = {
    total_ar_balance:                round2(totalArBalance),
    claims_this_month:               Math.round(claimsThisMonth ?? 0),
    paid_this_month:                 round2(paidThisMonth),
    denial_rate_pct:                 round2(denialRatePct),
    charge_capture_rate_pct:         round2(chargeCapturePct),
    days_in_ar_avg:                  round2(daysInArAvg),
    avg_submission_lag_days:         round2(submissionLagDays),
    avg_primary_collection_lag_days: round2(primaryCollectionLagDays),
    last_updated:                    new Date().toISOString(),
  };

  console.info(
    `[get-practice-summary] practice=${practice_id}`,
    JSON.stringify(summary),
  );

  return json(summary);
});

// ── Utility ──────────────────────────────────────────────────────────────────

function round2(v: number | null): number {
  if (v === null || v === undefined || isNaN(v)) return 0;
  return Math.round(v * 100) / 100;
}

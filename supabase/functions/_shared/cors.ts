// Shared CORS utility for all edge functions.
// Restricts Access-Control-Allow-Origin to known frontend domains
// instead of wildcard "*". Update ALLOWED_ORIGINS when adding new
// deployment targets (e.g., custom domain).

const ALLOWED_ORIGINS = [
  'https://n2n-portal.vercel.app',   // Vercel production
  'http://localhost:3000',            // local dev
];

/**
 * Returns CORS headers scoped to the request's origin.
 * If the origin isn't in the allow-list, defaults to the first entry
 * (production) so the response still includes a valid header but the
 * browser will block the cross-origin request.
 */
export function getCorsHeaders(req: Request) {
  const origin = req.headers.get('origin') || '';
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };
}

/**
 * Returns a 204 CORS preflight response. Use at the top of every
 * Deno.serve handler: `if (req.method === 'OPTIONS') return corsResponse(req);`
 */
export function corsResponse(req: Request) {
  return new Response(null, { status: 204, headers: getCorsHeaders(req) });
}

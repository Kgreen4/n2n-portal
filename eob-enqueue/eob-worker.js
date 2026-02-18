import { createClient } from "npm:@supabase/supabase-js@2.39.7";
const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const GCP_SA_JSON_STR = Deno.env.get('GCP_SA_JSON')!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function uint8ToBase64(bytes: Uint8Array): string {
  const CHUNK = 0x8000;
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += CHUNK) {
    parts.push(String.fromCharCode(...bytes.subarray(i, i + CHUNK)));
  }
  return btoa(parts.join(''));
}

const base64url = (buf: Uint8Array | string) => {
  const base64 = typeof buf === 'string' ? btoa(buf) : uint8ToBase64(buf);
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

async function getGoogleAccessToken(sa: any) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now - 30
  };
  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(payload));
  const dataToSign = new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`);
  const pem = sa.private_key.replace(/\\n/g, '\n');
  const binaryKey = atob(pem.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n/g, ''));
  const keyBuffer = new Uint8Array(binaryKey.length);
  for (let i = 0; i < binaryKey.length; i++) {
    keyBuffer[i] = binaryKey.charCodeAt(i);
  }
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8', keyBuffer.buffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', cryptoKey, dataToSign);
  const jwt = `${encodedHeader}.${encodedPayload}.${base64url(new Uint8Array(signature))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt })
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`GCP Auth Failed: ${JSON.stringify(data)}`);
  return data.access_token;
}

Deno.serve(async (req) => {
  try {
    const payload = await req.json();
    const job = payload.job || payload;
    const sa = JSON.parse(GCP_SA_JSON_STR.trim());

    // Look up practice_id
    const { data: docRow, error: docErr } = await supabase
      .from('eob_documents')
      .select('practice_id')
      .eq('id', job.eob_document_id)
      .single();
    if (docErr) throw new Error(`Doc lookup failed: ${docErr.message}`);
    const practice_id = docRow.practice_id;

    // STEP 1: DOWNLOAD PDF
    const pageStr = String(job.page_number).padStart(3, '0');
    const filePath = `${job.eob_document_id}/page-${pageStr}.pdf`;
    const { data: fileBlob, error: dlErr } = await supabase.storage.from('eob-pages').download(filePath);
    if (dlErr) throw new Error(dlErr.message);
    const base64PDF = uint8ToBase64(new Uint8Array(await fileBlob.arrayBuffer()));

    // STEP 2: AUTHENTICATE
    const gToken = await getGoogleAccessToken(sa);

    // STEP 3: CALL VERTEX AI
    const VERTEX_URL = `https://us-central1-aiplatform.googleapis.com/v1/projects/${sa.project_id}/locations/us-central1/publishers/google/models/gemini-2.0-flash-exp:generateContent`;
    const aiResp = await fetch(VERTEX_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ "role": "user", "parts": [
          { "text": "Extract medical line items from this BCBS Arizona EOB. Return JSON with an 'items' array. Use keys: patient_name, cpt_code, billed_amount, paid_amount." },
          { "inlineData": { "mimeType": "application/pdf", "data": base64PDF } }
        ]}],
        generationConfig: { "responseMimeType": "application/json" }
      })
    });
    const aiData = await aiResp.json();

    // Handle empty/blocked Gemini responses
    if (!aiData.candidates || !aiData.candidates[0]?.content?.parts?.[0]?.text) {
      await supabase.rpc('succeed_eob_page_job', { p_page_job_id: job.id });
      return new Response(JSON.stringify({ status: 'succeeded', count: 0 }), { status: 200 });
    }

    const rawText = aiData.candidates[0].content.parts[0].text;
    const extracted = JSON.parse(rawText).items || [];

    // STEP 4: PERSIST — with error checking and explicit column mapping
    if (extracted.length > 0) {
      const { error: insertErr } = await supabase.from('eob_line_items').insert(
        extracted.map((it: any) => ({
          eob_document_id: job.eob_document_id,
          page_number: job.page_number,
          practice_id: practice_id,
          patient_name: it.patient_name || null,
          cpt_code: it.cpt_code || null,
          billed_amount: parseFloat(String(it.billed_amount).replace(/[$,]/g, '')) || null,
          paid_amount: parseFloat(String(it.paid_amount).replace(/[$,]/g, '')) || null,
        }))
      );
      if (insertErr) throw new Error(`Insert failed: ${insertErr.message}`);
    }

    // STEP 5: FINALIZE
    await supabase.rpc('succeed_eob_page_job', { p_page_job_id: job.id });
    return new Response(JSON.stringify({ status: 'succeeded', count: extracted.length }), { status: 200 });
  } catch (err) {
    return new Response(JSON.stringify({ error: "Operation Failed", details: err.message }), { status: 500 });
  }
});

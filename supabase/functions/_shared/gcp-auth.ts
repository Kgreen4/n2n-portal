// Shared GCP authentication helpers
// Used by eob-enqueue, trigger-eob-parser, reprocess-document, eob-worker

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

export async function getGoogleAccessToken(
  sa: { client_email: string; private_key: string },
  scope = "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/drive"
): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope,
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now - 30,
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

/**
 * Move a Google Drive file to the "Processed" subfolder.
 * Auto-discovers or creates the Processed folder if not cached.
 * Returns the processed folder ID (for caching) or null on failure.
 */
export async function moveToProcessedFolder(
  gToken: string,
  fileId: string,
  sourceFolderId: string,
  cachedProcessedFolderId: string | null,
): Promise<string | null> {
  let processedFolderId = cachedProcessedFolderId;

  // Auto-discover or create the "Processed" subfolder
  if (!processedFolderId) {
    const searchQ = encodeURIComponent(
      `name='Processed' and '${sourceFolderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`
    );
    const searchResp = await fetch(
      `https://www.googleapis.com/drive/v3/files?q=${searchQ}&fields=files(id,name)&supportsAllDrives=true&includeItemsFromAllDrives=true`,
      { headers: { "Authorization": `Bearer ${gToken}` } }
    );
    const searchData = await searchResp.json();

    if (searchData.files?.length > 0) {
      processedFolderId = searchData.files[0].id;
      console.info("[gcp-auth] found existing Processed folder:", processedFolderId);
    } else {
      // Create the "Processed" subfolder
      const createResp = await fetch(
        "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true",
        {
          method: "POST",
          headers: { "Authorization": `Bearer ${gToken}`, "Content-Type": "application/json" },
          body: JSON.stringify({
            name: "Processed",
            mimeType: "application/vnd.google-apps.folder",
            parents: [sourceFolderId],
          }),
        }
      );
      const createData = await createResp.json();
      if (createResp.ok && createData.id) {
        processedFolderId = createData.id;
        console.info("[gcp-auth] created Processed folder:", processedFolderId);
      } else {
        console.warn("[gcp-auth] failed to create Processed folder:", createData);
        return null;
      }
    }
  }

  // Get the file's current parent(s)
  const fileMetaResp = await fetch(
    `https://www.googleapis.com/drive/v3/files/${fileId}?fields=parents&supportsAllDrives=true`,
    { headers: { "Authorization": `Bearer ${gToken}` } }
  );
  const fileMeta = await fileMetaResp.json();
  const currentParents = (fileMeta.parents || []).join(",");

  // Move the file
  const moveResp = await fetch(
    `https://www.googleapis.com/drive/v3/files/${fileId}?addParents=${processedFolderId}&removeParents=${currentParents}&supportsAllDrives=true`,
    {
      method: "PATCH",
      headers: { "Authorization": `Bearer ${gToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({}),
    }
  );

  if (moveResp.ok) {
    console.info(`[gcp-auth] moved file ${fileId} to Processed folder`);
    return processedFolderId;
  } else {
    const moveErr = await moveResp.text();
    console.warn(`[gcp-auth] move to Processed failed: ${moveResp.status} ${moveErr}`);
    return processedFolderId; // Return the folder ID even if move failed (folder exists)
  }
}

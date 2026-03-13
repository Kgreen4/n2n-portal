'use client'

// Google OAuth callback page for the onboarding wizard.
// Google redirects here after the user grants Drive access:
//   /onboarding/callback?code=<auth_code>&state=<base64_json>
//
// This page:
//   1. Decodes the state param to get practiceId + folderId
//   2. Calls the google-drive-setup edge function with the auth code
//   3. Redirects to /onboarding?step=3 on success, or /onboarding?error=... on failure

import { useEffect, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { Suspense } from 'react'

const SUPABASE_FUNCTIONS_URL = `${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1`
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? ''

function CallbackHandler() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const [statusText, setStatusText] = useState('Connecting your Google Drive…')
  const [isError, setIsError] = useState(false)

  useEffect(() => {
    handleOAuthCallback()
  }, [])

  async function handleOAuthCallback() {
    const code = searchParams.get('code')
    const state = searchParams.get('state')
    const oauthError = searchParams.get('error')

    // Google returned an error (e.g. user denied access)
    if (oauthError) {
      const msg = oauthError === 'access_denied'
        ? 'You declined Google Drive access. Please try again.'
        : `Google authorization error: ${oauthError}`
      setIsError(true)
      setStatusText(msg)
      setTimeout(() => {
        router.push(`/onboarding?error=${encodeURIComponent(msg)}`)
      }, 2500)
      return
    }

    if (!code || !state) {
      const msg = 'Invalid callback parameters. Please try again.'
      setIsError(true)
      setStatusText(msg)
      setTimeout(() => router.push(`/onboarding?error=${encodeURIComponent(msg)}`), 2500)
      return
    }

    // Decode the state param to get practiceId + folderId
    let practiceId: string
    let folderId: string
    try {
      const decoded = JSON.parse(atob(state))
      practiceId = decoded.practiceId
      folderId = decoded.folderId
      if (!practiceId || !folderId) throw new Error('Missing fields in state')
    } catch {
      const msg = 'Invalid OAuth state. Please restart the onboarding flow.'
      setIsError(true)
      setStatusText(msg)
      setTimeout(() => router.push(`/onboarding?error=${encodeURIComponent(msg)}`), 2500)
      return
    }

    // Get a fresh session (refreshSession avoids stale/corrupt cached tokens)
    const supabase = createClient()
    const { data: { session }, error: refreshError } = await supabase.auth.refreshSession()
    if (refreshError || !session) {
      router.push('/login')
      return
    }

    // Call the edge function to exchange the code + share the folder
    try {
      const resp = await fetch(`${SUPABASE_FUNCTIONS_URL}/google-drive-setup`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'apikey': SUPABASE_ANON_KEY,
          'Authorization': `Bearer ${session.access_token}`,
        },
        body: JSON.stringify({ code, folder_id: folderId, practice_id: practiceId }),
      })

      const data = await resp.json()
      if (!resp.ok) {
        throw new Error(data.error || 'Failed to connect Google Drive')
      }

      // Success — redirect to step 3 of the wizard
      const folderName = encodeURIComponent(data.folder_name ?? 'PAYMENTS')
      router.push(`/onboarding?step=3&folder_name=${folderName}`)
    } catch (err: any) {
      const msg = err.message || 'An unexpected error occurred'
      setIsError(true)
      setStatusText(msg)
      setTimeout(() => {
        router.push(`/onboarding?error=${encodeURIComponent(msg)}`)
      }, 3000)
    }
  }

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-100 p-10 max-w-sm w-full text-center">
      {!isError ? (
        <>
          <div className="w-14 h-14 mx-auto mb-5 flex items-center justify-center">
            <svg className="w-12 h-12 animate-spin text-blue-500" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
          </div>
          <h2 className="text-lg font-semibold text-gray-900">{statusText}</h2>
          <p className="mt-1 text-sm text-gray-500">This will only take a moment.</p>
        </>
      ) : (
        <>
          <div className="w-14 h-14 mx-auto mb-5 bg-red-100 rounded-full flex items-center justify-center">
            <svg className="w-7 h-7 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </div>
          <h2 className="text-lg font-semibold text-gray-900">Connection failed</h2>
          <p className="mt-2 text-sm text-red-600">{statusText}</p>
          <p className="mt-3 text-xs text-gray-400">Redirecting you back…</p>
        </>
      )}
    </div>
  )
}

export default function OnboardingCallbackPage() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100 flex items-center justify-center p-4">
      <Suspense fallback={
        <div className="bg-white rounded-2xl shadow-lg border border-gray-100 p-10 max-w-sm w-full text-center">
          <svg className="w-12 h-12 animate-spin text-blue-500 mx-auto" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
        </div>
      }>
        <CallbackHandler />
      </Suspense>
    </div>
  )
}

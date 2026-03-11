'use client'

// Onboarding wizard — 4-step flow for new practices
// Step 1: Enter practice name  → calls create-practice edge function
// Step 2: Connect Google Drive → Google OAuth → /onboarding/callback handles the exchange
// Step 3: Confirmation         → shown after callback redirects back with ?step=3
//
// State management across the OAuth redirect:
//   practiceId + folderId are encoded into the OAuth `state` param (base64 JSON).
//   The callback page decodes state, calls google-drive-setup, then redirects
//   to /onboarding?step=3&folder_name=<name>.

import { Suspense, useEffect, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'

const SUPABASE_FUNCTIONS_URL = `${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1`
const GOOGLE_OAUTH_CLIENT_ID = process.env.NEXT_PUBLIC_GOOGLE_OAUTH_CLIENT_ID ?? ''

/** Extract the folder ID from a Google Drive URL */
function extractFolderId(url: string): string | null {
  const match = url.match(/\/folders\/([a-zA-Z0-9_-]+)/)
  return match ? match[1] : null
}

type Step = 1 | 2 | 3

function OnboardingWizard() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const supabase = createClient()

  const [step, setStep] = useState<Step>(1)
  const [practiceName, setPracticeName] = useState('')
  const [practiceId, setPracticeId] = useState<string | null>(null)
  const [driveUrl, setDriveUrl] = useState('')
  const [folderName, setFolderName] = useState<string | null>(null)
  const [trialEndsAt, setTrialEndsAt] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [initializing, setInitializing] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const stepParam = searchParams.get('step')
    const oauthError = searchParams.get('error')
    const connectedFolderName = searchParams.get('folder_name')

    if (stepParam === '3') {
      // Arrived from /onboarding/callback after successful Drive connection
      setStep(3)
      if (connectedFolderName) setFolderName(decodeURIComponent(connectedFolderName))
      fetchTrialEndDate()
      setInitializing(false)
      return
    }

    if (oauthError) {
      // Returned from OAuth with an error — show step 2 with error message
      setError(decodeURIComponent(oauthError))
      setInitializing(false)
      checkForExistingPractice(true) // puts us on step 2 if practice exists
      return
    }

    // Normal load — check if user already has a practice
    checkForExistingPractice(false)
  }, [])

  async function checkForExistingPractice(skipDashboardRedirect: boolean) {
    const { data: { user } } = await supabase.auth.getUser()
    if (!user) {
      router.push('/login')
      return
    }

    const { data: links } = await supabase
      .from('practice_users')
      .select('practice_id')
      .eq('user_id', user.id)
      .limit(1)

    if (!links || links.length === 0) {
      // No practice yet — start at step 1
      setInitializing(false)
      return
    }

    // Has a practice — check if Drive is connected
    const pid = links[0].practice_id
    setPracticeId(pid)

    const { data: settings } = await supabase
      .from('practice_settings')
      .select('gdrive_folder_id, watcher_enabled')
      .eq('practice_id', pid)
      .maybeSingle()

    if (settings?.gdrive_folder_id && settings?.watcher_enabled) {
      // Fully onboarded — go to dashboard (unless we're handling an OAuth error)
      if (!skipDashboardRedirect) {
        router.push('/dashboard')
        return
      }
    }

    // Has practice but Drive not connected — resume at step 2
    setStep(2)
    setInitializing(false)
  }

  async function fetchTrialEndDate() {
    const { data: { user } } = await supabase.auth.getUser()
    if (!user) return

    const { data: link } = await supabase
      .from('practice_users')
      .select('practice_id')
      .eq('user_id', user.id)
      .limit(1)
      .maybeSingle()

    if (!link) return

    const { data: practice } = await supabase
      .from('practices')
      .select('trial_ends_at')
      .eq('id', link.practice_id)
      .single()

    if (practice?.trial_ends_at) setTrialEndsAt(practice.trial_ends_at)
  }

  async function handleCreatePractice(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    const { data: { session } } = await supabase.auth.getSession()
    if (!session) {
      setError('Your session has expired. Please log in again.')
      setLoading(false)
      return
    }

    try {
      const resp = await fetch(`${SUPABASE_FUNCTIONS_URL}/create-practice`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${session.access_token}`,
        },
        body: JSON.stringify({ practiceName: practiceName.trim() }),
      })
      const data = await resp.json()
      if (!resp.ok) throw new Error(data.error || 'Failed to create practice')

      setPracticeId(data.practiceId)
      setStep(2)
    } catch (err: any) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  function handleConnectDrive() {
    const folderId = extractFolderId(driveUrl)
    if (!folderId) {
      setError('Please paste a valid Google Drive folder URL, e.g. https://drive.google.com/drive/folders/…')
      return
    }
    if (!practiceId) {
      setError('Practice ID missing. Please refresh the page and try again.')
      return
    }
    if (!GOOGLE_OAUTH_CLIENT_ID) {
      setError('Google OAuth is not configured. Please contact support.')
      return
    }

    // Encode practiceId + folderId into the OAuth state param so the callback
    // page can retrieve them after the redirect (no sessionStorage needed).
    const statePayload = btoa(JSON.stringify({ practiceId, folderId }))

    const redirectUri = `${window.location.origin}/onboarding/callback`
    const oauthUrl = new URL('https://accounts.google.com/o/oauth2/v2/auth')
    oauthUrl.searchParams.set('client_id', GOOGLE_OAUTH_CLIENT_ID)
    oauthUrl.searchParams.set('redirect_uri', redirectUri)
    oauthUrl.searchParams.set('response_type', 'code')
    oauthUrl.searchParams.set('scope', 'https://www.googleapis.com/auth/drive')
    oauthUrl.searchParams.set('access_type', 'online')
    oauthUrl.searchParams.set('prompt', 'consent')
    oauthUrl.searchParams.set('state', statePayload)

    window.location.href = oauthUrl.toString()
  }

  const folderId = extractFolderId(driveUrl)

  if (initializing) {
    return (
      <div className="flex flex-col items-center justify-center gap-3">
        <svg className="h-8 w-8 animate-spin text-blue-500" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        <p className="text-sm text-gray-500">Loading…</p>
      </div>
    )
  }

  return (
    <div className="w-full max-w-md">
      {/* Step progress indicator */}
      <div className="flex items-center justify-center mb-8">
        {([1, 2, 3] as Step[]).map((s) => (
          <div key={s} className="flex items-center">
            <div
              className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-semibold transition-colors ${
                step > s
                  ? 'bg-green-500 text-white'
                  : step === s
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-500'
              }`}
            >
              {step > s ? (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
              ) : (
                s
              )}
            </div>
            {s < 3 && (
              <div className={`w-20 h-0.5 mx-1 transition-colors ${step > s ? 'bg-green-500' : 'bg-gray-200'}`} />
            )}
          </div>
        ))}
      </div>

      <div className="bg-white rounded-2xl shadow-lg border border-gray-100 p-8">

        {/* ── Step 1: Practice Details ──────────────────────────────────────── */}
        {step === 1 && (
          <>
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-gray-900">Welcome to Clarix</h1>
              <p className="mt-1 text-sm text-gray-500">
                Let's get your practice set up — takes under 5 minutes.
              </p>
            </div>

            <form onSubmit={handleCreatePractice} className="space-y-5">
              <div>
                <label htmlFor="practiceName" className="block text-sm font-medium text-gray-700 mb-1">
                  Practice Name
                </label>
                <input
                  id="practiceName"
                  type="text"
                  required
                  autoFocus
                  value={practiceName}
                  onChange={(e) => setPracticeName(e.target.value)}
                  placeholder="e.g. Arizona Heart Specialists"
                  disabled={loading}
                  className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-50"
                />
              </div>

              {error && (
                <div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-sm text-red-700">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={loading || !practiceName.trim()}
                className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-semibold text-white hover:bg-blue-500 disabled:opacity-50 transition-colors"
              >
                {loading ? 'Setting up…' : 'Continue →'}
              </button>
            </form>
          </>
        )}

        {/* ── Step 2: Connect Google Drive ─────────────────────────────────── */}
        {step === 2 && (
          <>
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-gray-900">Connect Google Drive</h1>
              <p className="mt-1 text-sm text-gray-500">
                Paste the URL of the Drive folder where you store EOB PDFs.
                We'll connect it automatically — no manual sharing needed.
              </p>
            </div>

            <div className="space-y-4">
              <div>
                <label htmlFor="driveUrl" className="block text-sm font-medium text-gray-700 mb-1">
                  Google Drive Folder URL
                </label>
                <input
                  id="driveUrl"
                  type="text"
                  value={driveUrl}
                  onChange={(e) => { setDriveUrl(e.target.value); setError(null) }}
                  placeholder="https://drive.google.com/drive/folders/…"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                {folderId && (
                  <p className="mt-1.5 text-xs font-medium text-green-600 flex items-center gap-1">
                    <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                    Folder ID detected
                  </p>
                )}
              </div>

              <div className="rounded-lg bg-blue-50 border border-blue-100 px-4 py-3 text-xs text-blue-700 leading-relaxed">
                <strong className="block mb-1">What happens next</strong>
                You'll sign in with Google and grant Clarix access to your Drive. We'll automatically share
                the folder with our secure processing service — you won't need to touch Drive settings again.
              </div>

              {error && (
                <div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-sm text-red-700">
                  {error}
                </div>
              )}

              <button
                onClick={handleConnectDrive}
                disabled={!folderId}
                className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-semibold text-white hover:bg-blue-500 disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
                  <path d="M19.35 10.04C18.67 6.59 15.64 4 12 4 9.11 4 6.6 5.64 5.35 8.04 2.34 8.36 0 10.91 0 14c0 3.31 2.69 6 6 6h13c2.76 0 5-2.24 5-5 0-2.64-2.05-4.78-4.65-4.96z"/>
                </svg>
                Authorize Google Drive Access →
              </button>
            </div>
          </>
        )}

        {/* ── Step 3: Trial Started ─────────────────────────────────────────── */}
        {step === 3 && (
          <div className="text-center">
            <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-5">
              <svg className="w-8 h-8 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
              </svg>
            </div>

            <h1 className="text-2xl font-bold text-gray-900 mb-2">You're all set!</h1>

            <div className="space-y-1 mb-5">
              {folderName && (
                <p className="text-sm text-gray-700">
                  ✓ <strong>{folderName}</strong> connected to Clarix
                </p>
              )}
              <p className="text-sm text-gray-700">
                ✓ 7-day free trial started
                {trialEndsAt && (
                  <> — ends <strong>
                    {new Date(trialEndsAt).toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}
                  </strong></>
                )}
              </p>
            </div>

            <div className="rounded-lg bg-indigo-50 border border-indigo-100 px-4 py-4 text-sm text-indigo-800 text-left mb-6">
              <strong className="block mb-2">🚀 Get your first EOB in under 2 minutes</strong>
              <ol className="space-y-1 list-decimal list-inside text-indigo-700">
                <li>Drop an EOB PDF into your connected Google Drive folder</li>
                <li>Clarix detects it and starts processing automatically</li>
                <li>View extracted data on your dashboard</li>
              </ol>
            </div>

            <button
              onClick={() => router.push('/dashboard')}
              className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-semibold text-white hover:bg-blue-500 transition-colors"
            >
              Go to Dashboard →
            </button>
          </div>
        )}
      </div>

      {/* Step labels */}
      <div className="flex justify-between mt-3 text-xs text-gray-400 px-1">
        <span>Practice details</span>
        <span className="text-center">Connect Drive</span>
        <span>Free trial</span>
      </div>
    </div>
  )
}

export default function OnboardingPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-screen">
        <svg className="h-8 w-8 animate-spin text-blue-500" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      </div>
    }>
      <OnboardingWizard />
    </Suspense>
  )
}

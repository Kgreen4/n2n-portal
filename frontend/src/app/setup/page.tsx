'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'

export default function SetupPracticePage() {
  const router = useRouter()
  const [practiceName, setPracticeName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  const handleSetup = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    setError(null)

    const supabase = createClient()
    const { data: { session } } = await supabase.auth.getSession()

    if (!session) {
      setError("You must be logged in.")
      setIsLoading(false)
      return
    }

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1/create-practice`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${session.access_token}`
        },
        body: JSON.stringify({ practiceName: practiceName.trim() })
      })

      const result = await response.json()

      if (!response.ok) throw new Error(result.error || 'Failed to create practice')

      // Refresh to update server components with new RLS access, then route to dashboard
      router.push('/dashboard')
      router.refresh()
    } catch (err: any) {
      setError(err.message)
      setIsLoading(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-gray-50 px-4">
      <div className="w-full max-w-md space-y-8 rounded-xl bg-white p-8 shadow-lg border border-gray-100">
        <div className="text-center">
          <h2 className="text-3xl font-bold tracking-tight text-gray-900">Welcome!</h2>
          <p className="mt-2 text-sm text-gray-600">Let's set up your practice to start extracting EOBs.</p>
        </div>

        <form onSubmit={handleSetup} className="mt-8 space-y-6">
          <div>
            <label htmlFor="practiceName" className="block text-sm font-medium text-gray-700">Practice Name</label>
            <input
              id="practiceName"
              type="text"
              required
              disabled={isLoading}
              value={practiceName}
              onChange={(e) => setPracticeName(e.target.value)}
              className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:bg-gray-100 disabled:text-gray-500"
              placeholder="e.g. Arizona Heart Specialists"
            />
          </div>

          <div aria-live="polite">
            {error && <div className="text-sm text-red-600 bg-red-50 p-3 rounded-md" role="alert">{error}</div>}
          </div>

          <button
            type="submit"
            disabled={isLoading}
            className="flex w-full justify-center rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-blue-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600 disabled:opacity-50"
          >
            {isLoading ? 'Creating Workspace...' : 'Complete Setup'}
          </button>
        </form>
      </div>
    </div>
  )
}

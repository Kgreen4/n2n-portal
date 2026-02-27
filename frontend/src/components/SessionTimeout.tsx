'use client'

import { useEffect } from 'react'
import { createClient } from '@/lib/supabase/client'

// HIPAA: Auto-logout after 15 minutes of inactivity
const TIMEOUT_MS = 15 * 60 * 1000

export default function SessionTimeout() {
  useEffect(() => {
    const supabase = createClient()
    let timeout: ReturnType<typeof setTimeout>

    const resetTimer = () => {
      clearTimeout(timeout)
      timeout = setTimeout(async () => {
        await supabase.auth.signOut()
        window.location.href = '/login?reason=timeout'
      }, TIMEOUT_MS)
    }

    const events = ['mousedown', 'keypress', 'scroll', 'touchstart'] as const
    events.forEach(e => window.addEventListener(e, resetTimer))
    resetTimer()

    return () => {
      clearTimeout(timeout)
      events.forEach(e => window.removeEventListener(e, resetTimer))
    }
  }, [])

  return null // Invisible component — only handles side effects
}

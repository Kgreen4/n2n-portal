'use client'

// /setup is superseded by the new onboarding wizard at /onboarding.
// Redirect any users who navigate here directly.
import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function SetupPracticePage() {
  const router = useRouter()
  useEffect(() => {
    router.replace('/onboarding')
  }, [router])
  return null
}

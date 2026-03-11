'use client'

import { useEffect, useState } from 'react'
import { useSearchParams } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'

interface PracticeInfo {
  id: string
  plan_tier: string
  plan_credits_per_month: number
  stripe_customer_id: string | null
  subscription_status: string | null
  trial_ends_at: string | null
}

interface Credits {
  credits_remaining: number
}

// Static lookup map — Next.js can only inline NEXT_PUBLIC_* vars at build time
// when accessed via literal keys (process.env.NEXT_PUBLIC_FOO), not dynamic keys.
const STRIPE_PRICE_IDS: Record<string, string> = {
  NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID:  process.env.NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID  ?? '',
  NEXT_PUBLIC_STRIPE_PRO_PRICE_ID:      process.env.NEXT_PUBLIC_STRIPE_PRO_PRICE_ID      ?? '',
  NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID: process.env.NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID ?? '',
  NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID: process.env.NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID ?? '',
}

const TIER_LABELS: Record<string, string> = {
  trial: 'Trial',
  starter: 'Starter',
  professional: 'Professional',
  enterprise: 'Enterprise',
}

const TIER_COLORS: Record<string, string> = {
  trial: 'bg-gray-100 text-gray-700',
  starter: 'bg-blue-100 text-blue-700',
  professional: 'bg-purple-100 text-purple-700',
  enterprise: 'bg-amber-100 text-amber-700',
}

const PLANS = [
  {
    id: 'starter',
    name: 'Starter',
    price: '$99',
    period: '/month',
    pages: '500 pages/mo',
    maxPerDoc: 'Up to 50 pages per EOB',
    users: 'Up to 3 users',
    features: ['Portal dashboard', 'CSV download', 'Google Drive watcher'],
    priceEnvKey: 'NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID',
  },
  {
    id: 'professional',
    name: 'Professional',
    price: '$299',
    period: '/month',
    pages: '2,000 pages/mo',
    maxPerDoc: 'Up to 150 pages per EOB',
    users: 'Unlimited users',
    features: ['Everything in Starter', '835 EDI export', 'Bank reconciliation'],
    highlight: true,
    priceEnvKey: 'NEXT_PUBLIC_STRIPE_PRO_PRICE_ID',
  },
]

const BOOSTS = [
  { id: 'boost100', name: 'Boost Pack 100', price: '$20', pages: '100 extra pages', priceEnvKey: 'NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID' },
  { id: 'boost500', name: 'Boost Pack 500', price: '$89', pages: '500 extra pages', priceEnvKey: 'NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID' },
]

export default function BillingPage() {
  const supabase = createClient()
  const searchParams = useSearchParams()

  const [practice, setPractice] = useState<PracticeInfo | null>(null)
  const [credits, setCredits] = useState<Credits | null>(null)
  const [loading, setLoading] = useState(true)
  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const checkoutStatus = searchParams.get('checkout')

  useEffect(() => {
    async function load() {
      // Get current user's practice
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) return

      const { data: link } = await supabase
        .from('practice_users')
        .select('practice_id')
        .eq('user_id', user.id)
        .single()

      if (!link) return

      const [practiceResult, creditsResult] = await Promise.all([
        supabase
          .from('practices')
          .select('id, plan_tier, plan_credits_per_month, stripe_customer_id, subscription_status, trial_ends_at')
          .eq('id', link.practice_id)
          .single(),
        supabase
          .from('practice_credits')
          .select('credits_remaining')
          .eq('practice_id', link.practice_id)
          .single(),
      ])

      if (practiceResult.data) setPractice(practiceResult.data)
      if (creditsResult.data) setCredits(creditsResult.data)
      setLoading(false)
    }
    load()
  }, [])

  async function handleCheckout(priceId: string, mode: 'subscription' | 'payment', creditsToAdd?: number) {
    if (!practice) return
    setActionLoading(priceId)
    setError(null)

    try {
      const { data, error: fnErr } = await supabase.functions.invoke('create-checkout-session', {
        body: { practice_id: practice.id, price_id: priceId, mode, credits_to_add: creditsToAdd },
      })

      if (fnErr || data?.error) {
        let errMsg: string = data?.error || fnErr?.message || 'Failed to start checkout'
        if (fnErr) {
          try {
            const body = await (fnErr as any).context?.json?.()
            if (body?.error) errMsg = body.error
            if (body?.details) console.error('[checkout] Stripe details:', JSON.stringify(body.details))
          } catch {}
        }
        setError(errMsg)
        return
      }

      if (data?.url) {
        window.location.href = data.url
      }
    } finally {
      setActionLoading(null)
    }
  }

  async function handleManageBilling() {
    if (!practice) return
    setActionLoading('portal')
    setError(null)

    try {
      const { data, error: fnErr } = await supabase.functions.invoke('create-portal-session', {
        body: { practice_id: practice.id },
      })

      if (fnErr || data?.error) {
        setError(data?.message || fnErr?.message || 'Failed to open billing portal')
        return
      }

      if (data?.url) {
        window.location.href = data.url
      }
    } finally {
      setActionLoading(null)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-24">
        <svg className="h-6 w-6 animate-spin text-gray-400" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      </div>
    )
  }

  const tier = practice?.plan_tier ?? 'trial'
  const monthlyAllowance = practice?.plan_credits_per_month ?? 50
  const remaining = credits?.credits_remaining ?? 0
  const usedPct = Math.min(100, Math.round(((monthlyAllowance - remaining) / monthlyAllowance) * 100))
  const hasSubscription = !!practice?.stripe_customer_id && tier !== 'trial'

  // Trial countdown — only shown when on the trial tier
  const trialDaysLeft = (tier === 'trial' && practice?.trial_ends_at)
    ? Math.max(0, Math.ceil((new Date(practice.trial_ends_at).getTime() - Date.now()) / (24 * 60 * 60 * 1000)))
    : null
  const trialExpired = trialDaysLeft === 0

  return (
    <div className="max-w-4xl">
      <h1 className="text-2xl font-bold text-gray-900">Billing & Plan</h1>
      <p className="mt-1 text-sm text-gray-500">Manage your subscription, credits, and plan tier.</p>

      {/* Checkout status banners */}
      {checkoutStatus === 'success' && (
        <div className="mt-4 rounded-md bg-green-50 border border-green-200 p-4 text-sm text-green-800">
          Payment successful! Your plan has been updated. Credits have been added to your account.
        </div>
      )}
      {checkoutStatus === 'cancelled' && (
        <div className="mt-4 rounded-md bg-gray-50 border border-gray-200 p-4 text-sm text-gray-700">
          Checkout was cancelled. Your plan has not changed.
        </div>
      )}
      {trialExpired && (
        <div className="mt-4 rounded-md bg-red-50 border border-red-200 p-4 text-sm text-red-800">
          <strong>Your free trial has ended.</strong> Upgrade below to continue processing EOBs.
        </div>
      )}
      {error && (
        <div className="mt-4 rounded-md bg-red-50 border border-red-200 p-4 text-sm text-red-800">{error}</div>
      )}

      {/* Current plan card */}
      <div className="mt-6 rounded-lg border border-gray-200 bg-white p-6 shadow-sm">
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h2 className="text-base font-semibold text-gray-900">Current Plan</h2>
              <span className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${TIER_COLORS[tier] ?? TIER_COLORS.trial}`}>
                {TIER_LABELS[tier] ?? tier}
              </span>
              {trialDaysLeft !== null && (
                <span className={`rounded-full px-2.5 py-0.5 text-xs font-semibold ${
                  trialExpired
                    ? 'bg-red-100 text-red-700'
                    : trialDaysLeft <= 3
                    ? 'bg-orange-100 text-orange-700'
                    : 'bg-blue-100 text-blue-700'
                }`}>
                  {trialExpired
                    ? 'Trial ended'
                    : `${trialDaysLeft} day${trialDaysLeft === 1 ? '' : 's'} left in free trial`}
                </span>
              )}
            </div>
            <p className="mt-1 text-sm text-gray-500">
              {monthlyAllowance.toLocaleString()} pages included per month
            </p>
          </div>
          {hasSubscription && (
            <button
              onClick={handleManageBilling}
              disabled={actionLoading === 'portal'}
              className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
            >
              {actionLoading === 'portal' ? 'Loading...' : 'Manage billing'}
            </button>
          )}
        </div>

        {/* Credit usage bar */}
        <div className="mt-4">
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>{remaining.toLocaleString()} pages remaining</span>
            <span>{monthlyAllowance.toLocaleString()} monthly allowance</span>
          </div>
          <div className="h-2 w-full rounded-full bg-gray-100 overflow-hidden">
            <div
              className={`h-2 rounded-full transition-all ${remaining / monthlyAllowance < 0.2 ? 'bg-red-500' : 'bg-blue-500'}`}
              style={{ width: `${Math.min(100, Math.max(2, 100 - usedPct))}%` }}
            />
          </div>
          {remaining / monthlyAllowance < 0.2 && (
            <p className="mt-1 text-xs text-red-600">Running low on pages — consider a Boost Pack or plan upgrade.</p>
          )}
        </div>
      </div>

      {/* Plan upgrade cards */}
      {tier !== 'professional' && tier !== 'enterprise' && (
        <div className="mt-6">
          <h2 className="text-base font-semibold text-gray-900">Upgrade your plan</h2>
          <div className="mt-3 grid gap-4 sm:grid-cols-2">
            {PLANS.filter(p => p.id !== tier).map(plan => {
              const priceId = STRIPE_PRICE_IDS[plan.priceEnvKey] ?? ''
              return (
                <div
                  key={plan.id}
                  className={`relative rounded-lg border p-5 ${plan.highlight ? 'border-purple-300 bg-purple-50' : 'border-gray-200 bg-white'}`}
                >
                  {plan.highlight && (
                    <span className="absolute -top-2.5 left-4 rounded-full bg-purple-600 px-2.5 py-0.5 text-xs font-semibold text-white">
                      Most popular
                    </span>
                  )}
                  <div className="flex items-baseline gap-1">
                    <span className="text-2xl font-bold text-gray-900">{plan.price}</span>
                    <span className="text-sm text-gray-500">{plan.period}</span>
                  </div>
                  <p className="mt-0.5 font-semibold text-gray-900">{plan.name}</p>
                  <ul className="mt-3 space-y-1.5 text-sm text-gray-600">
                    <li className="font-medium text-gray-800">{plan.pages}</li>
                    <li>{plan.maxPerDoc}</li>
                    <li>{plan.users}</li>
                    {plan.features.map(f => (
                      <li key={f} className="flex items-center gap-1.5">
                        <svg className="h-4 w-4 shrink-0 text-green-500" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                        </svg>
                        {f}
                      </li>
                    ))}
                  </ul>
                  <button
                    onClick={() => handleCheckout(priceId, 'subscription')}
                    disabled={!priceId || actionLoading === priceId}
                    className={`mt-4 w-full rounded-md px-4 py-2 text-sm font-semibold text-white shadow-sm disabled:opacity-50 ${
                      plan.highlight ? 'bg-purple-600 hover:bg-purple-500' : 'bg-blue-600 hover:bg-blue-500'
                    }`}
                  >
                    {actionLoading === priceId ? 'Redirecting...' : `Upgrade to ${plan.name}`}
                  </button>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Boost packs */}
      <div className="mt-6">
        <h2 className="text-base font-semibold text-gray-900">Need more pages this month?</h2>
        <p className="mt-0.5 text-sm text-gray-500">One-time credit packs — never expire.</p>
        <div className="mt-3 grid gap-3 sm:grid-cols-2">
          {BOOSTS.map(boost => {
            const priceId = STRIPE_PRICE_IDS[boost.priceEnvKey] ?? ''
            const creditsMap: Record<string, number> = { boost100: 100, boost500: 500 }
            return (
              <div key={boost.id} className="flex items-center justify-between rounded-lg border border-gray-200 bg-white p-4">
                <div>
                  <p className="font-semibold text-gray-900">{boost.name}</p>
                  <p className="text-sm text-gray-500">{boost.pages}</p>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-lg font-bold text-gray-900">{boost.price}</span>
                  <button
                    onClick={() => handleCheckout(priceId, 'payment', creditsMap[boost.id])}
                    disabled={!priceId || actionLoading === priceId}
                    className="rounded-md bg-gray-900 px-3 py-1.5 text-sm font-semibold text-white hover:bg-gray-700 disabled:opacity-50"
                  >
                    {actionLoading === priceId ? '...' : 'Buy'}
                  </button>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Enterprise CTA */}
      {tier !== 'enterprise' && (
        <div className="mt-6 rounded-lg border border-gray-200 bg-gray-50 p-5 text-center">
          <p className="font-semibold text-gray-900">Need more? Talk to us about Enterprise.</p>
          <p className="mt-1 text-sm text-gray-500">Unlimited pages, HIPAA BAA, dedicated support, and custom SLA.</p>
          <a
            href="mailto:hello@n2nportal.com"
            className="mt-3 inline-block rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 hover:bg-gray-100"
          >
            Contact us
          </a>
        </div>
      )}
    </div>
  )
}

'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'

type Period = '7d' | '30d' | '90d' | 'all'

interface LineItem {
  id: string
  eob_document_id: string
  file_name: string | null
  line_type: string | null
  patient_name: string | null
  date_of_service: string | null
  cpt_code: string | null
  cpt_description: string | null
  billed_amount: number | null
  allowed_amount: number | null
  paid_amount: number | null
  patient_responsibility: number | null
  claim_status: string | null
  payer_name: string | null
  remark_code: string | null
  remark_description: string | null
  payment_date: string | null
  confidence_score: number | null
  created_at: string
}

interface PayerRow {
  payer: string
  claims: number
  billed: number
  paid: number
  denied: number
}

interface StatusRow {
  status: string
  count: number
  paid: number
}

const fmt = (n: number) =>
  n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })

const pct = (num: number, den: number) =>
  den === 0 ? '—' : `${((num / den) * 100).toFixed(1)}%`

export default function ReportsPage() {
  const supabase = createClient()
  const [period, setPeriod] = useState<Period>('30d')
  const [items, setItems] = useState<LineItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const PAGE_SIZE = 25

  useEffect(() => {
    let cancelled = false

    async function load() {
      setLoading(true)
      setError(null)
      setPage(0)

      const { data: { user } } = await supabase.auth.getUser()
      if (!user) return

      const { data: link } = await supabase
        .from('practice_users')
        .select('practice_id')
        .eq('user_id', user.id)
        .limit(1)
        .single()
      if (!link) return

      let query = supabase
        .from('eob_line_items')
        .select('*')
        .eq('practice_id', link.practice_id)
        .neq('line_type', 'summary_total')
        .order('created_at', { ascending: false })
        .limit(2000)

      if (period !== 'all') {
        const days = period === '7d' ? 7 : period === '30d' ? 30 : 90
        const since = new Date(Date.now() - days * 86400_000).toISOString()
        query = query.gte('created_at', since)
      }

      const { data, error: err } = await query
      if (cancelled) return
      if (err) { setError(err.message); setLoading(false); return }
      setItems(data ?? [])
      setLoading(false)
    }

    load()
    return () => { cancelled = true }
  }, [period])

  // ── Aggregations ──────────────────────────────────────────────
  const medicalItems = items.filter(i => i.line_type === 'medical_service' || i.line_type === null)

  const totalBilled = medicalItems.reduce((s, i) => s + (i.billed_amount ?? 0), 0)
  const totalPaid   = medicalItems.reduce((s, i) => s + (i.paid_amount   ?? 0), 0)
  const totalClaims = medicalItems.length
  const deniedCount = medicalItems.filter(i =>
    (i.claim_status ?? '').toLowerCase().includes('denied') ||
    (i.claim_status ?? '').toLowerCase().includes('rejected')
  ).length

  // Payer breakdown
  const payerMap = new Map<string, PayerRow>()
  for (const i of medicalItems) {
    const payer = i.payer_name || '(Unknown Payer)'
    const row = payerMap.get(payer) ?? { payer, claims: 0, billed: 0, paid: 0, denied: 0 }
    row.claims++
    row.billed += i.billed_amount ?? 0
    row.paid   += i.paid_amount   ?? 0
    const isDenied = (i.claim_status ?? '').toLowerCase().includes('denied') ||
                     (i.claim_status ?? '').toLowerCase().includes('rejected')
    if (isDenied) row.denied++
    payerMap.set(payer, row)
  }
  const payerRows = Array.from(payerMap.values()).sort((a, b) => b.billed - a.billed)

  // Status breakdown
  const statusMap = new Map<string, StatusRow>()
  for (const i of medicalItems) {
    const status = i.claim_status || '(No Status)'
    const row = statusMap.get(status) ?? { status, count: 0, paid: 0 }
    row.count++
    row.paid += i.paid_amount ?? 0
    statusMap.set(status, row)
  }
  const statusRows = Array.from(statusMap.values()).sort((a, b) => b.count - a.count)

  // Filtered + paginated items for the table
  const filtered = items.filter(i => {
    if (!search) return true
    const q = search.toLowerCase()
    return (
      (i.patient_name ?? '').toLowerCase().includes(q) ||
      (i.payer_name ?? '').toLowerCase().includes(q) ||
      (i.cpt_code ?? '').toLowerCase().includes(q) ||
      (i.file_name ?? '').toLowerCase().includes(q) ||
      (i.claim_status ?? '').toLowerCase().includes(q)
    )
  })
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE)
  const pageItems  = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  const statusColor = (s: string | null) => {
    const lower = (s ?? '').toLowerCase()
    if (lower.includes('paid') && !lower.includes('partially')) return 'bg-green-100 text-green-800'
    if (lower.includes('partial')) return 'bg-yellow-100 text-yellow-800'
    if (lower.includes('denied') || lower.includes('rejected')) return 'bg-red-100 text-red-800'
    return 'bg-gray-100 text-gray-600'
  }

  const PERIOD_LABELS: Record<Period, string> = {
    '7d': 'Last 7 days', '30d': 'Last 30 days', '90d': 'Last 90 days', 'all': 'All time'
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Reports & Insights</h1>
          <p className="mt-1 text-sm text-gray-500">
            Extracted line items from processed EOB documents
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">Period:</span>
          <select
            value={period}
            onChange={e => setPeriod(e.target.value as Period)}
            className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm text-gray-700 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            {(Object.entries(PERIOD_LABELS) as [Period, string][]).map(([val, label]) => (
              <option key={val} value={val}>{label}</option>
            ))}
          </select>
        </div>
      </div>

      {error && (
        <div className="mt-4 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {loading ? (
        <div className="mt-12 flex items-center justify-center gap-3 text-gray-400">
          <svg className="h-5 w-5 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          <span className="text-sm">Loading line items…</span>
        </div>
      ) : items.length === 0 ? (
        <div className="mt-12 rounded-xl border border-gray-200 bg-white p-12 text-center shadow-sm">
          <svg className="mx-auto h-12 w-12 text-gray-300" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
          </svg>
          <p className="mt-3 text-sm font-medium text-gray-900">No line items found</p>
          <p className="mt-1 text-sm text-gray-500">
            Process some EOB documents to see reporting data here.
            {period !== 'all' && ' Try expanding the period filter.'}
          </p>
        </div>
      ) : (
        <>
          {/* KPI Cards */}
          <div className="mt-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Total Billed</p>
              <p className="mt-1 text-2xl font-bold text-gray-900">{fmt(totalBilled)}</p>
              <p className="mt-1 text-xs text-gray-400">{totalClaims.toLocaleString()} claim lines</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Total Paid</p>
              <p className="mt-1 text-2xl font-bold text-green-700">{fmt(totalPaid)}</p>
              <p className="mt-1 text-xs text-gray-400">Collection rate: {pct(totalPaid, totalBilled)}</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Denial Rate</p>
              <p className={`mt-1 text-2xl font-bold ${deniedCount / Math.max(totalClaims, 1) > 0.05 ? 'text-red-600' : 'text-gray-900'}`}>
                {pct(deniedCount, totalClaims)}
              </p>
              <p className="mt-1 text-xs text-gray-400">{deniedCount} denied of {totalClaims}</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Avg Per Claim</p>
              <p className="mt-1 text-2xl font-bold text-gray-900">
                {totalClaims > 0 ? fmt(totalPaid / totalClaims) : '—'}
              </p>
              <p className="mt-1 text-xs text-gray-400">paid per line item</p>
            </div>
          </div>

          {/* Payer Breakdown + Status Distribution */}
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            {/* Payer Breakdown */}
            <div className="rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-gray-100 px-5 py-4">
                <h2 className="text-sm font-semibold text-gray-900">Payer Breakdown</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 text-left">
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Claims</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Billed</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Paid</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Coll%</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {payerRows.map(row => (
                      <tr key={row.payer} className="hover:bg-gray-50">
                        <td className="px-4 py-2.5 text-gray-800 max-w-[160px] truncate" title={row.payer}>
                          {row.payer}
                        </td>
                        <td className="px-4 py-2.5 text-gray-600 text-right">{row.claims}</td>
                        <td className="px-4 py-2.5 text-gray-600 text-right">{fmt(row.billed)}</td>
                        <td className="px-4 py-2.5 text-gray-800 font-medium text-right">{fmt(row.paid)}</td>
                        <td className={`px-4 py-2.5 text-right font-medium ${row.billed > 0 && row.paid / row.billed < 0.6 ? 'text-red-600' : 'text-green-700'}`}>
                          {pct(row.paid, row.billed)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Claim Status */}
            <div className="rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-gray-100 px-5 py-4">
                <h2 className="text-sm font-semibold text-gray-900">Claim Status</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 text-left">
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Status</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Count</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Share</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Amount Paid</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {statusRows.map(row => (
                      <tr key={row.status} className="hover:bg-gray-50">
                        <td className="px-4 py-2.5">
                          <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusColor(row.status)}`}>
                            {row.status}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 text-gray-600 text-right">{row.count}</td>
                        <td className="px-4 py-2.5 text-gray-500 text-right">{pct(row.count, totalClaims)}</td>
                        <td className="px-4 py-2.5 text-gray-800 font-medium text-right">{fmt(row.paid)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Line Items Table */}
          <div className="mt-6 rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
            <div className="flex items-center justify-between border-b border-gray-100 px-5 py-4">
              <h2 className="text-sm font-semibold text-gray-900">
                Line Items
                <span className="ml-2 text-xs font-normal text-gray-400">
                  {filtered.length.toLocaleString()} of {items.length.toLocaleString()}
                </span>
              </h2>
              <input
                type="search"
                placeholder="Search patient, payer, CPT…"
                value={search}
                onChange={e => { setSearch(e.target.value); setPage(0) }}
                className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 w-56"
              />
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 text-left border-b border-gray-100">
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Patient</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">DOS</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">CPT</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Status</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Billed</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Paid</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Remark</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {pageItems.map(i => (
                    <tr key={i.id} className="hover:bg-gray-50">
                      <td className="px-4 py-2.5 max-w-[140px] truncate text-gray-800" title={i.patient_name ?? ''}>
                        {i.patient_name || <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5 max-w-[160px] truncate text-gray-600" title={i.payer_name ?? ''}>
                        {i.payer_name || <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap">
                        {i.date_of_service || <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-gray-700">
                        {i.cpt_code || <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5">
                        <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusColor(i.claim_status)}`}>
                          {i.claim_status || '—'}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-right text-gray-600 whitespace-nowrap">
                        {i.billed_amount != null ? fmt(i.billed_amount) : <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5 text-right font-medium text-gray-800 whitespace-nowrap">
                        {i.paid_amount != null ? fmt(i.paid_amount) : <span className="text-gray-300">—</span>}
                      </td>
                      <td className="px-4 py-2.5 max-w-[140px] truncate text-xs text-gray-500" title={i.remark_code ?? ''}>
                        {i.remark_code || <span className="text-gray-300">—</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between border-t border-gray-100 px-5 py-3">
                <p className="text-xs text-gray-500">
                  Showing {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, filtered.length)} of {filtered.length}
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={() => setPage(p => Math.max(0, p - 1))}
                    disabled={page === 0}
                    className="rounded px-3 py-1 text-xs border border-gray-200 disabled:opacity-40 hover:bg-gray-50"
                  >
                    Previous
                  </button>
                  <button
                    onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                    disabled={page === totalPages - 1}
                    className="rounded px-3 py-1 text-xs border border-gray-200 disabled:opacity-40 hover:bg-gray-50"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </div>

          <p className="mt-4 text-xs text-gray-400 text-center">
            Showing up to 2,000 line items · {PERIOD_LABELS[period]} · medical service lines only
          </p>
        </>
      )}
    </div>
  )
}

'use client'

import { useState, useEffect } from 'react'
import { createClient } from '@/lib/supabase/client'
import Link from 'next/link'

interface BankDeposit {
  id: string
  deposit_date: string
  check_number: string | null
  amount: number
  description: string | null
  payer_name: string | null
  matched_eob_document_id: string | null
  match_status: string
  match_delta: number | null
  source_file: string | null
  created_at: string
}

export default function ReconciliationPage() {
  const supabase = createClient()
  const [deposits, setDeposits] = useState<BankDeposit[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<'all' | 'matched' | 'discrepancy' | 'unmatched'>('all')
  const [rematching, setRematching] = useState(false)
  const [rematchResult, setRematchResult] = useState<string | null>(null)
  const [practiceId, setPracticeId] = useState<string | null>(null)

  useEffect(() => {
    async function load() {
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) return

      const { data: link } = await supabase
        .from('practice_users')
        .select('practice_id')
        .eq('user_id', user.id)
        .single()

      if (!link) return

      setPracticeId(link.practice_id)

      const { data } = await supabase
        .from('bank_deposits')
        .select('*')
        .eq('practice_id', link.practice_id)
        .order('deposit_date', { ascending: false })
        .order('created_at', { ascending: false })

      setDeposits(data || [])
      setLoading(false)
    }
    load()
  }, [supabase])

  async function handleRematch() {
    if (!practiceId || rematching) return
    setRematching(true)
    setRematchResult(null)
    try {
      const { data: { session } } = await supabase.auth.getSession()
      const resp = await fetch(
        `${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1/rematch-deposits`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${session?.access_token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ practice_id: practiceId }),
        }
      )
      const result = await resp.json()
      if (!resp.ok) throw new Error(result.error || 'Rematch failed')
      const msg = result.updated === 0
        ? `No changes — ${result.unchanged ?? 0} deposits already current`
        : `Updated ${result.updated} deposit${result.updated !== 1 ? 's' : ''}: ${result.matched} matched, ${result.discrepancies} discrepancies`
      setRematchResult(msg)
      // Reload deposits to show updated values
      const { data } = await supabase
        .from('bank_deposits')
        .select('*')
        .eq('practice_id', practiceId)
        .order('deposit_date', { ascending: false })
        .order('created_at', { ascending: false })
      setDeposits(data || [])
    } catch (err: any) {
      setRematchResult(`Error: ${err.message}`)
    } finally {
      setRematching(false)
    }
  }

  const matchedCount = deposits.filter(d => d.match_status === 'matched').length
  const discrepancyCount = deposits.filter(d => d.match_status === 'discrepancy').length
  const unmatchedCount = deposits.filter(d => d.match_status === 'unmatched').length
  const totalAmount = deposits.reduce((s, d) => s + Number(d.amount), 0)
  const matchedAmount = deposits.filter(d => d.match_status === 'matched').reduce((s, d) => s + Number(d.amount), 0)

  const filtered = filter === 'all' ? deposits : deposits.filter(d => d.match_status === filter)

  const formatCurrency = (n: number | null) => {
    if (n === null || n === undefined) return '-'
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(n)
  }

  const formatDate = (d: string) => {
    if (!d) return '-'
    const date = new Date(d + 'T00:00:00')
    return date.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: '2-digit' })
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin h-8 w-8 border-4 border-blue-600 border-t-transparent rounded-full" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Bank Reconciliation</h1>
          <p className="mt-1 text-sm text-gray-500">
            Match bank deposits against EOB payment check totals.
          </p>
        </div>
        <div className="flex items-center gap-3">
          {rematchResult && (
            <span className={`text-xs font-medium px-3 py-1.5 rounded-lg ${
              rematchResult.startsWith('Error')
                ? 'bg-red-50 text-red-700 border border-red-200'
                : 'bg-blue-50 text-blue-700 border border-blue-200'
            }`}>
              {rematchResult}
            </span>
          )}
          {deposits.length > 0 && (
            <button
              onClick={handleRematch}
              disabled={rematching}
              className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-medium rounded-lg transition-colors"
            >
              {rematching ? (
                <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
              ) : (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
                </svg>
              )}
              {rematching ? 'Refreshing…' : 'Refresh Matching'}
            </button>
          )}
          <Link
            href="/settings"
            className="inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white text-sm font-medium rounded-lg transition-colors"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
            </svg>
            Upload CSV
          </Link>
        </div>
      </div>

      {/* Summary Cards */}
      {deposits.length > 0 && (
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Deposits</p>
            <p className="mt-1 text-2xl font-bold text-gray-900">{deposits.length}</p>
            <p className="text-xs text-gray-500">{formatCurrency(totalAmount)}</p>
          </div>
          <button
            onClick={() => setFilter(filter === 'matched' ? 'all' : 'matched')}
            className={`text-left border rounded-xl p-4 transition-colors ${filter === 'matched' ? 'bg-green-50 border-green-300' : 'bg-white border-gray-200 hover:bg-green-50'}`}
          >
            <p className="text-xs font-medium text-green-600 uppercase tracking-wider">Matched</p>
            <p className="mt-1 text-2xl font-bold text-green-700">{matchedCount}</p>
            <p className="text-xs text-green-600">{formatCurrency(matchedAmount)}</p>
          </button>
          <button
            onClick={() => setFilter(filter === 'discrepancy' ? 'all' : 'discrepancy')}
            className={`text-left border rounded-xl p-4 transition-colors ${filter === 'discrepancy' ? 'bg-amber-50 border-amber-300' : 'bg-white border-gray-200 hover:bg-amber-50'}`}
          >
            <p className="text-xs font-medium text-amber-600 uppercase tracking-wider">Discrepancies</p>
            <p className="mt-1 text-2xl font-bold text-amber-700">{discrepancyCount}</p>
          </button>
          <button
            onClick={() => setFilter(filter === 'unmatched' ? 'all' : 'unmatched')}
            className={`text-left border rounded-xl p-4 transition-colors ${filter === 'unmatched' ? 'bg-red-50 border-red-300' : 'bg-white border-gray-200 hover:bg-red-50'}`}
          >
            <p className="text-xs font-medium text-red-600 uppercase tracking-wider">Unmatched</p>
            <p className="mt-1 text-2xl font-bold text-red-700">{unmatchedCount}</p>
          </button>
          <div className="bg-white border border-gray-200 rounded-xl p-4">
            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Match Rate</p>
            <p className="mt-1 text-2xl font-bold text-gray-900">
              {deposits.length > 0 ? Math.round((matchedCount / deposits.length) * 100) : 0}%
            </p>
          </div>
        </div>
      )}

      {/* Deposits Table */}
      {deposits.length === 0 ? (
        <div className="bg-white border border-gray-200 rounded-xl p-12 text-center">
          <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" strokeWidth="1" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 21v-8.25M15.75 21v-8.25M8.25 21v-8.25M3 9l9-6 9 6m-1.5 12V10.332A48.36 48.36 0 0012 9.75c-2.551 0-5.056.2-7.5.582V21M3 21h18M12 6.75h.008v.008H12V6.75z" />
          </svg>
          <h3 className="mt-4 text-sm font-medium text-gray-900">No bank deposits yet</h3>
          <p className="mt-2 text-sm text-gray-500">
            Upload a bank statement CSV from the{' '}
            <Link href="/settings" className="text-blue-600 hover:text-blue-500 font-medium">Settings</Link>
            {' '}page to start reconciling.
          </p>
        </div>
      ) : (
        <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-900">
              {filter === 'all' ? 'All Deposits' : `${filter.charAt(0).toUpperCase() + filter.slice(1)} Deposits`}
              <span className="ml-2 text-xs font-normal text-gray-500">{filtered.length} rows</span>
            </h3>
            {filter !== 'all' && (
              <button onClick={() => setFilter('all')} className="text-xs text-blue-600 hover:text-blue-500">
                Show all
              </button>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">Date</th>
                  <th className="px-4 py-2 text-left font-medium">Check #</th>
                  <th className="px-4 py-2 text-right font-medium">Bank Amt</th>
                  <th className="px-4 py-2 text-right font-medium">EOB Amt</th>
                  <th className="px-4 py-2 text-center font-medium">Status</th>
                  <th className="px-4 py-2 text-right font-medium">Delta</th>
                  <th className="px-4 py-2 text-left font-medium">Description</th>
                  <th className="px-4 py-2 text-left font-medium">Source</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filtered.map(dep => {
                  const eobAmt = dep.match_status !== 'unmatched' && dep.match_delta !== null
                    ? Number(dep.amount) - Number(dep.match_delta)
                    : null;

                  return (
                    <tr key={dep.id} className="hover:bg-gray-50">
                      <td className="px-4 py-2.5 text-gray-900 whitespace-nowrap">
                        {formatDate(dep.deposit_date)}
                      </td>
                      <td className="px-4 py-2.5 text-gray-900 font-mono text-xs whitespace-nowrap">
                        {dep.check_number || '-'}
                      </td>
                      <td className="px-4 py-2.5 text-right text-gray-900 font-medium whitespace-nowrap">
                        {formatCurrency(Number(dep.amount))}
                      </td>
                      <td className="px-4 py-2.5 text-right text-gray-600 whitespace-nowrap">
                        {eobAmt !== null ? formatCurrency(eobAmt) : '-'}
                      </td>
                      <td className="px-4 py-2.5 text-center whitespace-nowrap">
                        {dep.match_status === 'matched' && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">
                            Matched
                          </span>
                        )}
                        {dep.match_status === 'discrepancy' && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-amber-100 text-amber-700">
                            Discrepancy
                          </span>
                        )}
                        {dep.match_status === 'unmatched' && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">
                            No EOB
                          </span>
                        )}
                      </td>
                      <td className={`px-4 py-2.5 text-right whitespace-nowrap font-medium ${
                        dep.match_delta && Math.abs(Number(dep.match_delta)) >= 0.01
                          ? 'text-amber-600' : 'text-gray-400'
                      }`}>
                        {dep.match_delta !== null && Math.abs(Number(dep.match_delta)) >= 0.01
                          ? (Number(dep.match_delta) > 0 ? '+' : '') + formatCurrency(Number(dep.match_delta))
                          : '-'}
                      </td>
                      <td className="px-4 py-2.5 text-gray-500 truncate max-w-[200px]" title={dep.description || ''}>
                        {dep.description || '-'}
                      </td>
                      <td className="px-4 py-2.5 text-gray-400 text-xs truncate max-w-[120px]" title={dep.source_file || ''}>
                        {dep.source_file || '-'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

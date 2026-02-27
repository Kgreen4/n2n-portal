'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { logAuditEvent } from '@/lib/audit'

interface Document {
  id: string
  file_name: string
  status: string
  total_pages: number | null
  items_extracted: number
  created_at: string
  practice_id: string
  last_exported_at: string | null
  export_batch_id: string | null
  export_total_paid: number | null
  export_total_patient_resp: number | null
  export_claim_count: number | null
}

interface BatchGroup {
  batchId: string
  exportedAt: string
  docs: Document[]
  totalPaid: number
  totalPatientResp: number
  claimCount: number
}

interface Props {
  documents: Document[]
  practiceId: string
}

export default function DocumentsClient({ documents, practiceId }: Props) {
  const supabase = createClient()
  const router = useRouter()
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [downloading, setDownloading] = useState(false)
  const [downloadError, setDownloadError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'ready' | 'history'>('ready')
  const [collapsedBatches, setCollapsedBatches] = useState<Set<string>>(new Set())

  // Partition documents into Ready (unexported) and History (exported)
  const readyDocs = documents.filter(d => !d.last_exported_at)
  const historyDocs = documents.filter(d => !!d.last_exported_at)
    .sort((a, b) => new Date(b.last_exported_at!).getTime() - new Date(a.last_exported_at!).getTime())

  // Group history docs by export_batch_id
  const batchGroups: BatchGroup[] = (() => {
    const groupMap = new Map<string, Document[]>()
    for (const doc of historyDocs) {
      const key = doc.export_batch_id || doc.id // fallback for docs without batch_id
      if (!groupMap.has(key)) groupMap.set(key, [])
      groupMap.get(key)!.push(doc)
    }
    return Array.from(groupMap.entries()).map(([batchId, docs]) => ({
      batchId,
      exportedAt: docs[0].last_exported_at!,
      docs,
      totalPaid: docs.reduce((s, d) => s + (d.export_total_paid || 0), 0),
      totalPatientResp: docs.reduce((s, d) => s + (d.export_total_patient_resp || 0), 0),
      claimCount: docs.reduce((s, d) => s + (d.export_claim_count || 0), 0),
    }))
  })()

  const visibleDocs = activeTab === 'ready' ? readyDocs : historyDocs

  // Only completed/partial_failure documents can be selected for 835 export
  const exportableDocs = readyDocs.filter(
    d => d.status === 'completed' || d.status === 'partial_failure'
  )
  const exportableIds = new Set(exportableDocs.map(d => d.id))

  // Only show checkboxes for unexported ready docs
  const showCheckboxes = activeTab === 'ready'

  function toggleSelect(id: string) {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleSelectAll() {
    if (selectedIds.size === exportableDocs.length) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(exportableDocs.map(d => d.id)))
    }
  }

  const allExportableSelected = exportableDocs.length > 0
    && selectedIds.size === exportableDocs.length

  function toggleBatchCollapse(batchId: string) {
    setCollapsedBatches(prev => {
      const next = new Set(prev)
      if (next.has(batchId)) next.delete(batchId)
      else next.add(batchId)
      return next
    })
  }

  async function handleBatchExport() {
    if (selectedIds.size === 0) return
    setDownloading(true)
    setDownloadError(null)

    try {
      const ids = Array.from(selectedIds)

      // Single-doc uses backward-compatible path; batch uses array path
      const body = ids.length === 1
        ? { eob_document_id: ids[0], practice_id: practiceId }
        : { eob_document_ids: ids, practice_id: practiceId }

      const { data, error } = await supabase.functions.invoke('generate-835', { body })

      if (error) {
        let msg = error.message
        try {
          const errorBody = await (error as any).context?.json?.()
          if (errorBody?.message) msg = errorBody.message
        } catch { /* ignore parse errors */ }
        setDownloadError(msg || 'Failed to generate 835 file')
        return
      }

      // Handle various response formats
      let content: string
      if (typeof data === 'string') {
        content = data
      } else if (data instanceof Blob) {
        content = await data.text()
      } else if (typeof data === 'object' && data?.error) {
        setDownloadError(data.message || data.error)
        return
      } else {
        content = String(data)
      }

      // Build filename
      const fileName = ids.length === 1
        ? `${(documents.find(d => d.id === ids[0])?.file_name || ids[0]).replace(/\.pdf$/i, '')}.835`
        : `batch-835-${new Date().toISOString().split('T')[0]}-${ids.length}docs.835`

      // Trigger browser download
      const blob = new Blob([content], { type: 'text/plain' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = fileName
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)

      // Audit log — track every export for HIPAA compliance
      logAuditEvent(supabase, {
        action: 'document.export',
        resourceType: 'eob_document',
        metadata: { doc_count: ids.length, file_name: fileName },
      })

      // Auto-clear selection and refresh page data
      // (server re-fetches; exported docs now have last_exported_at → move to History tab)
      setSelectedIds(new Set())
      router.refresh()
    } catch (err: any) {
      setDownloadError(err.message || 'Unexpected error')
    } finally {
      setDownloading(false)
    }
  }

  // Clear selection when switching tabs
  function switchTab(tab: 'ready' | 'history') {
    setActiveTab(tab)
    setSelectedIds(new Set())
    setDownloadError(null)
  }

  function formatCurrency(amount: number): string {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount)
  }

  function formatExportDateTime(dateStr: string): string {
    const d = new Date(dateStr)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) +
      ' at ' + d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
  }

  function formatExportDateShort(dateStr: string): string {
    const d = new Date(dateStr)
    return `${d.getMonth() + 1}/${d.getDate()}`
  }

  // Render a single document row (shared between Ready and History tabs)
  function renderDocRow(doc: Document, showCheckbox: boolean) {
    const isExportable = exportableIds.has(doc.id)
    const isSelected = selectedIds.has(doc.id)
    return (
      <tr key={doc.id} className={`hover:bg-gray-50 ${isSelected ? 'bg-green-50' : ''}`}>
        {showCheckbox && (
          <td className="px-4 py-4">
            {isExportable ? (
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => toggleSelect(doc.id)}
                className="h-4 w-4 rounded border-gray-300 text-green-600 focus:ring-green-500"
              />
            ) : (
              <input
                type="checkbox"
                disabled
                className="h-4 w-4 rounded border-gray-200 text-gray-300 cursor-not-allowed"
              />
            )}
          </td>
        )}
        <td className="px-6 py-4 text-sm font-medium text-gray-900 max-w-xs truncate">
          {doc.file_name || doc.id.substring(0, 8)}
        </td>
        <td className="px-6 py-4">
          <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${
            doc.status === 'completed' ? 'bg-green-100 text-green-800' :
            doc.status === 'processing' ? 'bg-yellow-100 text-yellow-800' :
            doc.status === 'failed' ? 'bg-red-100 text-red-800' :
            doc.status === 'partial_failure' ? 'bg-orange-100 text-orange-800' :
            'bg-gray-100 text-gray-800'
          }`}>
            {doc.status}
          </span>
        </td>
        <td className="px-6 py-4 text-sm text-gray-500">{doc.total_pages ?? '-'}</td>
        <td className="px-6 py-4 text-sm text-gray-500">{doc.items_extracted ?? 0}</td>
        <td className="px-6 py-4 text-sm text-gray-500">
          {activeTab === 'history' && doc.last_exported_at
            ? new Date(doc.last_exported_at).toLocaleDateString()
            : new Date(doc.created_at).toLocaleDateString()
          }
        </td>
        <td className="px-6 py-4 text-sm">
          <Link
            href={`/documents/${doc.id}`}
            className="text-blue-600 hover:text-blue-500 font-medium"
          >
            View →
          </Link>
        </td>
      </tr>
    )
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Documents</h1>
          <p className="mt-1 text-sm text-gray-500">
            {activeTab === 'ready'
              ? selectedIds.size > 0
                ? `${selectedIds.size} document${selectedIds.size > 1 ? 's' : ''} selected for Ethizo batch. Ready to export?`
                : 'Select completed documents and click Batch Export to generate a combined 835 file for EMR import.'
              : 'Previously exported batches grouped by export date. Click a batch to expand documents.'
            }
          </p>
        </div>

        {/* Batch Export Button — visible when 1+ selected on Ready tab */}
        {showCheckboxes && selectedIds.size > 0 && (
          <button
            onClick={handleBatchExport}
            disabled={downloading}
            className="inline-flex items-center rounded-md bg-green-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {downloading ? (
              <>
                <svg className="mr-2 h-4 w-4 animate-spin" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Generating...
              </>
            ) : (
              <>
                <svg className="mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
                </svg>
                {selectedIds.size === 1
                  ? 'Export 835'
                  : `Batch Export 835 (${selectedIds.size})`
                }
              </>
            )}
          </button>
        )}
      </div>

      {/* Tabs */}
      <div className="mt-4 border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => switchTab('ready')}
            className={`py-3 px-1 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'ready'
                ? 'border-green-600 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Ready to Export
            {readyDocs.filter(d => d.status === 'completed' || d.status === 'partial_failure').length > 0 && (
              <span className={`ml-2 inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                activeTab === 'ready' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'
              }`}>
                {readyDocs.filter(d => d.status === 'completed' || d.status === 'partial_failure').length}
              </span>
            )}
          </button>
          <button
            onClick={() => switchTab('history')}
            className={`py-3 px-1 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'history'
                ? 'border-green-600 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Export History
            {batchGroups.length > 0 && (
              <span className={`ml-2 inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                activeTab === 'history' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'
              }`}>
                {batchGroups.length}
              </span>
            )}
          </button>
        </nav>
      </div>

      {downloadError && (
        <div className="mt-4 rounded-md bg-red-50 border border-red-200 p-4">
          <p className="text-sm text-red-700">{downloadError}</p>
        </div>
      )}

      {/* Ready Tab — flat table with checkboxes */}
      {activeTab === 'ready' && (
        <div className="mt-4 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left">
                  <input
                    type="checkbox"
                    checked={allExportableSelected}
                    onChange={toggleSelectAll}
                    disabled={exportableDocs.length === 0}
                    className="h-4 w-4 rounded border-gray-300 text-green-600 focus:ring-green-500"
                    title="Select all completed documents"
                  />
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">File Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Status</th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Pages</th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Items</th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Uploaded</th>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {readyDocs.map((doc) => renderDocRow(doc, true))}
              {readyDocs.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-6 py-8 text-center text-sm text-gray-500">
                    No documents ready for export. <Link href="/upload" className="text-blue-600 hover:text-blue-500">Upload an EOB</Link> to get started.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* History Tab — grouped by batch with summary rows */}
      {activeTab === 'history' && (
        <div className="mt-4 space-y-4">
          {batchGroups.length === 0 && (
            <div className="rounded-xl border border-gray-200 bg-white shadow-sm px-6 py-8 text-center text-sm text-gray-500">
              No documents have been exported yet.
            </div>
          )}

          {batchGroups.map((batch) => {
            const isCollapsed = collapsedBatches.has(batch.batchId)
            return (
              <div key={batch.batchId} className="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
                {/* Batch Summary Header — always visible, clickable to expand/collapse */}
                <button
                  onClick={() => toggleBatchCollapse(batch.batchId)}
                  className="w-full bg-gray-50 hover:bg-gray-100 transition-colors px-6 py-4 text-left"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      {/* Chevron */}
                      <svg
                        className={`h-4 w-4 text-gray-400 transition-transform ${isCollapsed ? '' : 'rotate-90'}`}
                        fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor"
                      >
                        <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                      </svg>
                      {/* Batch info */}
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold text-gray-900">
                            Batch Export — {batch.docs.length} document{batch.docs.length !== 1 ? 's' : ''}
                          </span>
                          <span className="inline-flex items-center text-xs text-green-600 font-medium">
                            <svg className="mr-0.5 h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                            </svg>
                            Exported
                          </span>
                        </div>
                        <p className="text-xs text-gray-500 mt-0.5">
                          {formatExportDateTime(batch.exportedAt)}
                        </p>
                      </div>
                    </div>

                    {/* Summary stats */}
                    <div className="flex items-center gap-6 text-sm">
                      <div className="text-right">
                        <span className="text-xs text-gray-500 uppercase tracking-wide">Total Paid</span>
                        <p className="font-semibold text-gray-900">{formatCurrency(batch.totalPaid)}</p>
                      </div>
                      <div className="text-right">
                        <span className="text-xs text-gray-500 uppercase tracking-wide">Patient Resp</span>
                        <p className="font-semibold text-gray-900">{formatCurrency(batch.totalPatientResp)}</p>
                      </div>
                      <div className="text-right">
                        <span className="text-xs text-gray-500 uppercase tracking-wide">Claims</span>
                        <p className="font-semibold text-gray-900">{batch.claimCount}</p>
                      </div>
                    </div>
                  </div>
                </button>

                {/* Expanded document rows */}
                {!isCollapsed && (
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50/50">
                      <tr>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">File Name</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">Status</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">Paid</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">Pat Resp</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">Claims</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400">Items</th>
                        <th className="px-6 py-2 text-left text-xs font-medium uppercase tracking-wider text-gray-400"></th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {batch.docs.map((doc) => (
                        <tr key={doc.id} className="hover:bg-gray-50">
                          <td className="px-6 py-3 text-sm font-medium text-gray-900 max-w-xs truncate">
                            {doc.file_name || doc.id.substring(0, 8)}
                          </td>
                          <td className="px-6 py-3">
                            <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${
                              doc.status === 'completed' ? 'bg-green-100 text-green-800' :
                              doc.status === 'partial_failure' ? 'bg-orange-100 text-orange-800' :
                              'bg-gray-100 text-gray-800'
                            }`}>
                              {doc.status}
                            </span>
                          </td>
                          <td className="px-6 py-3 text-sm text-gray-700 font-medium">
                            {doc.export_total_paid != null ? formatCurrency(doc.export_total_paid) : '-'}
                          </td>
                          <td className="px-6 py-3 text-sm text-gray-700 font-medium">
                            {doc.export_total_patient_resp != null ? formatCurrency(doc.export_total_patient_resp) : '-'}
                          </td>
                          <td className="px-6 py-3 text-sm text-gray-500">
                            {doc.export_claim_count ?? '-'}
                          </td>
                          <td className="px-6 py-3 text-sm text-gray-500">{doc.items_extracted ?? 0}</td>
                          <td className="px-6 py-3 text-sm">
                            <Link
                              href={`/documents/${doc.id}`}
                              className="text-blue-600 hover:text-blue-500 font-medium"
                            >
                              View →
                            </Link>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

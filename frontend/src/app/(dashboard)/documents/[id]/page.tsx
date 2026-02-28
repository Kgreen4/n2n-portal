'use client'

import { useParams, useRouter } from 'next/navigation'
import { useEffect, useState, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { logAuditEvent } from '@/lib/audit'

// ─── Types ───────────────────────────────────────────────────

interface Document {
  id: string
  file_name: string
  status: string
  total_pages: number | null
  items_extracted: number
  created_at: string
  practice_id: string
  review_status: string | null
  review_reasons: string[] | null
  has_found_revenue: boolean
  last_exported_at: string | null
  export_batch_id: string | null
}

interface PageJob {
  page_number: number
  status: string
  items_extracted: number | null
  error_message: string | null
}

interface LineItem {
  eob_document_id: string
  page_number: string
  patient_name: string | null
  member_id: string | null
  date_of_service: string | null
  cpt_code: string | null
  cpt_description: string | null
  billed_amount: string | null
  allowed_amount: string | null
  paid_amount: string | null
  adjustment_amount: string | null
  patient_responsibility: string | null
  deductible_amount: string | null
  coinsurance_amount: string | null
  copay_amount: string | null
  contractual_adjustment: string | null
  claim_status: string | null
  remark_code: string | null
  remark_reason: string | null
  rendering_provider_npi: string | null
  line_type: string | null
  claim_number: string | null
  payment_date: string | null
  payer_name: string | null
  payer_id: string | null
  confidence_score: string | null
  non_covered_amount: string | null
  remark_description: string | null
  check_number: string | null
  check_total_amount: string | null
}

// Editable fields per line item — keyed by a composite string
type EditMap = Record<string, Record<string, string>>

// ─── Reason Badge Config ─────────────────────────────────────

const reasonBadges: Record<string, { label: string; className: string }> = {
  math_variance: { label: 'Math Variance', className: 'bg-orange-100 text-orange-800' },
  missing_claim_id: { label: 'Missing Claim ID', className: 'bg-red-100 text-red-800' },
  no_check_total: { label: 'No Check Total', className: 'bg-yellow-100 text-yellow-800' },
  partial_failure: { label: 'Partial Failure', className: 'bg-red-100 text-red-800' },
  low_confidence: { label: 'Low Confidence', className: 'bg-purple-100 text-purple-800' },
}

// Compose a row key for edit tracking
function rowKey(item: LineItem, idx: number): string {
  return `${item.page_number}|${item.patient_name || ''}|${item.cpt_code || ''}|${item.date_of_service || ''}|${idx}`
}

// ─── Main Component ──────────────────────────────────────────

export default function DocumentDetailPage() {
  const params = useParams()
  const router = useRouter()
  const docId = params.id as string
  const supabase = createClient()

  // Document & page jobs
  const [doc, setDoc] = useState<Document | null>(null)
  const [pageJobs, setPageJobs] = useState<PageJob[]>([])
  const [loading, setLoading] = useState(true)

  // Download 835
  const [downloading, setDownloading] = useState(false)
  const [downloadError, setDownloadError] = useState<string | null>(null)

  // Re-process
  const [reprocessing, setReprocessing] = useState(false)
  const [showReprocessConfirm, setShowReprocessConfirm] = useState(false)

  // Line items
  const [lineItems, setLineItems] = useState<LineItem[]>([])
  const [lineItemsLoading, setLineItemsLoading] = useState(false)
  const [lineItemsError, setLineItemsError] = useState<string | null>(null)

  // Edit mode
  const [editMode, setEditMode] = useState(false)
  const [edits, setEdits] = useState<EditMap>({})
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [saveSuccess, setSaveSuccess] = useState(false)

  // Review actions
  const [resolving, setResolving] = useState(false)

  // Export lock
  const [unlocking, setUnlocking] = useState(false)

  // Side-by-side: selected page for PDF viewer
  const [selectedPage, setSelectedPage] = useState(1)
  const [pdfUrl, setPdfUrl] = useState<string | null>(null)

  // ─── Fetch Document + Page Jobs ────────────────────────────

  useEffect(() => {
    async function fetchData() {
      const [docResult, jobsResult] = await Promise.all([
        supabase
          .from('eob_documents')
          .select('id, file_name, status, total_pages, items_extracted, created_at, practice_id, review_status, review_reasons, has_found_revenue, last_exported_at, export_batch_id')
          .eq('id', docId)
          .single(),
        supabase
          .from('eob_page_jobs')
          .select('page_number, status, items_extracted, error_message')
          .eq('eob_document_id', docId)
          .order('page_number'),
      ])

      if (docResult.data) {
        setDoc(docResult.data as Document)
        // Audit log — track document view
        logAuditEvent(supabase, {
          action: 'document.view',
          resourceType: 'eob_document',
          resourceId: docId,
        })
      }
      if (jobsResult.data) setPageJobs(jobsResult.data)
      setLoading(false)
    }
    fetchData()
  }, [docId])

  // ─── Fetch Line Items ──────────────────────────────────────

  const fetchLineItems = useCallback(async () => {
    setLineItemsLoading(true)
    setLineItemsError(null)
    try {
      const { data, error } = await supabase.functions.invoke('fetch-line-items', {
        body: { eob_document_id: docId },
      })
      if (error) throw new Error(error.message)
      if (data?.error) throw new Error(data.details || data.error)
      setLineItems(data.items || [])
    } catch (err: any) {
      setLineItemsError(err.message || 'Failed to fetch line items')
    } finally {
      setLineItemsLoading(false)
    }
  }, [docId])

  useEffect(() => {
    if (doc && ['completed', 'partial_failure'].includes(doc.status)) {
      fetchLineItems()
    }
  }, [doc, fetchLineItems])

  // ─── PDF Signed URL ────────────────────────────────────────

  useEffect(() => {
    async function getSignedUrl() {
      const pageStr = String(selectedPage).padStart(3, '0')
      const path = `${docId}/page-${pageStr}.pdf`
      const { data } = await supabase.storage
        .from('eob-pages')
        .createSignedUrl(path, 3600)
      setPdfUrl(data?.signedUrl || null)
    }
    if (editMode && doc) {
      getSignedUrl()
    }
  }, [editMode, selectedPage, docId, doc])

  // ─── Download 835 Handler ─────────────────────────────────

  async function handleDownload835() {
    if (!doc) return
    setDownloading(true)
    setDownloadError(null)

    try {
      const { data, error } = await supabase.functions.invoke('generate-835', {
        body: { eob_document_id: doc.id, practice_id: doc.practice_id },
      })

      if (error) {
        const msg = typeof data === 'object' && data?.message ? data.message : error.message
        setDownloadError(msg || 'Failed to generate 835 file')
        return
      }

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

      const baseName = (doc.file_name || doc.id).replace(/\.pdf$/i, '')
      const fileName = `${baseName}.835`
      const blob = new Blob([content], { type: 'text/plain' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = fileName
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)

      // Refresh document state (generate-835 now stamps last_exported_at)
      const { data: freshDoc } = await supabase
        .from('eob_documents')
        .select('id, file_name, status, total_pages, items_extracted, created_at, practice_id, review_status, review_reasons, has_found_revenue, last_exported_at, export_batch_id')
        .eq('id', docId)
        .single()
      if (freshDoc) setDoc(freshDoc as Document)
    } catch (err: any) {
      setDownloadError(err.message || 'Unexpected error')
    } finally {
      setDownloading(false)
    }
  }

  // ─── Re-process Handler ──────────────────────────────────────

  async function handleReprocess() {
    if (!doc) return
    setReprocessing(true)
    setShowReprocessConfirm(false)

    try {
      const { data, error } = await supabase.functions.invoke('reprocess-document', {
        body: { eob_document_id: doc.id },
      })

      if (error) {
        const msg = typeof data === 'object' && data?.message ? data.message : error.message
        alert(`Re-process failed: ${msg}`)
        setReprocessing(false)
        return
      }

      // Audit log
      logAuditEvent(supabase, {
        action: 'document.reprocess',
        resourceType: 'eob_document',
        resourceId: doc.id,
        metadata: { file_name: doc.file_name },
      })

      // Redirect back to documents list — document will show as 'pending'
      router.push('/documents')
    } catch (err: any) {
      alert(`Re-process failed: ${err.message}`)
      setReprocessing(false)
    }
  }

  // ─── Edit Handlers ─────────────────────────────────────────

  function handleFieldChange(key: string, field: string, value: string) {
    setEdits(prev => ({
      ...prev,
      [key]: { ...(prev[key] || {}), [field]: value },
    }))
    setSaveSuccess(false)
  }

  function getEditValue(key: string, field: string, original: string | null): string {
    return edits[key]?.[field] ?? original ?? ''
  }

  function hasEdits(): boolean {
    return Object.keys(edits).length > 0
  }

  async function handleSave() {
    if (!hasEdits()) return
    setSaving(true)
    setSaveError(null)
    setSaveSuccess(false)

    try {
      // Build updates array from edits
      const updates = Object.entries(edits).map(([key, fields]) => {
        // Parse composite key: "page|patient|cpt|dos|idx"
        const parts = key.split('|')
        // Find the original line item by index (last part of key)
        const idx = parseInt(parts[4])
        const item = lineItems[idx]
        return {
          eob_document_id: docId,
          page_number: parseInt(item.page_number),
          patient_name: item.patient_name || '',
          cpt_code: item.cpt_code || '',
          date_of_service: item.date_of_service || '',
          fields,
        }
      })

      const { data, error } = await supabase.functions.invoke('update-line-items', {
        body: { eob_document_id: docId, updates },
      })

      if (error) throw new Error(error.message)
      if (data?.error) throw new Error(data.details || data.error)

      setSaveSuccess(true)
      setEdits({})

      // Audit log — track edit for HIPAA compliance
      logAuditEvent(supabase, {
        action: 'document.edit',
        resourceType: 'eob_document',
        resourceId: docId,
        metadata: { fields_updated: updates.length },
      })

      // Refresh line items + document (check-exceptions may have updated review_status)
      await fetchLineItems()
      const { data: freshDoc } = await supabase
        .from('eob_documents')
        .select('id, file_name, status, total_pages, items_extracted, created_at, practice_id, review_status, review_reasons, has_found_revenue, last_exported_at, export_batch_id')
        .eq('id', docId)
        .single()
      if (freshDoc) setDoc(freshDoc as Document)
    } catch (err: any) {
      setSaveError(err.message || 'Failed to save changes')
    } finally {
      setSaving(false)
    }
  }

  function handleCancelEdit() {
    setEdits({})
    setEditMode(false)
    setSaveError(null)
    setSaveSuccess(false)
  }

  // ─── Mark as Resolved ──────────────────────────────────────

  async function handleMarkResolved() {
    if (!doc) return
    setResolving(true)
    try {
      const { error } = await supabase
        .from('eob_documents')
        .update({ review_status: 'resolved', updated_at: new Date().toISOString() })
        .eq('id', docId)
      if (error) throw error
      setDoc({ ...doc, review_status: 'resolved' })
    } catch (err: any) {
      alert('Failed to mark as resolved: ' + err.message)
    } finally {
      setResolving(false)
    }
  }

  // ─── Unlock for Re-export ─────────────────────────────────

  async function handleUnlockForReexport() {
    if (!doc) return
    setUnlocking(true)
    try {
      const { error } = await supabase
        .from('eob_documents')
        .update({ last_exported_at: null, export_batch_id: null, updated_at: new Date().toISOString() })
        .eq('id', docId)
      if (error) throw error
      setDoc({ ...doc, last_exported_at: null, export_batch_id: null })

      // Audit log — track unlock for re-export
      logAuditEvent(supabase, {
        action: 'document.unlock',
        resourceType: 'eob_document',
        resourceId: docId,
      })
    } catch (err: any) {
      alert('Failed to unlock: ' + err.message)
    } finally {
      setUnlocking(false)
    }
  }

  // ─── Loading / Error States ────────────────────────────────

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
      </div>
    )
  }

  if (!doc) {
    return (
      <div className="text-center py-12">
        <p className="text-gray-500">Document not found.</p>
        <button onClick={() => router.push('/documents')} className="mt-4 text-blue-600 hover:text-blue-500">
          Back to documents
        </button>
      </div>
    )
  }

  const succeededPages = pageJobs.filter(j => j.status === 'succeeded').length
  const failedPages = pageJobs.filter(j => j.status === 'failed').length
  const reviewReasons = (doc.review_reasons || []) as string[]
  const isTerminal = ['completed', 'partial_failure'].includes(doc.status)
  const needsReview = doc.review_status === 'needs_review'
  const isExported = !!doc.last_exported_at

  // Unique pages in line items for page navigation
  const lineItemPages = [...new Set(lineItems.map(i => parseInt(i.page_number)))].sort((a, b) => a - b)

  // Filter line items for selected page in edit mode
  const visibleItems = editMode
    ? lineItems.filter(i => parseInt(i.page_number) === selectedPage)
    : lineItems

  // ─── Render ────────────────────────────────────────────────

  return (
    <div>
      {/* Back button */}
      <button
        onClick={() => router.push('/documents')}
        className="mb-4 text-sm text-gray-500 hover:text-gray-700"
      >
        Back to documents
      </button>

      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold text-gray-900 break-all">{doc.file_name || doc.id}</h1>
            {doc.has_found_revenue && (
              <span className="inline-flex items-center rounded-full bg-green-100 px-2.5 py-1 text-xs font-semibold text-green-800 ring-1 ring-green-600/20 ring-inset">
                Found Revenue
              </span>
            )}
          </div>
          <p className="mt-1 text-sm text-gray-500">
            Uploaded {new Date(doc.created_at).toLocaleString()}
          </p>
        </div>

        <div className="flex items-center gap-2">
          {/* Re-process Button — available for terminal states */}
          {isTerminal && (
            <button
              onClick={() => setShowReprocessConfirm(true)}
              disabled={reprocessing}
              className="inline-flex items-center rounded-md bg-amber-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-amber-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {reprocessing ? (
                <>
                  <svg className="mr-2 h-4 w-4 animate-spin" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  Re-processing...
                </>
              ) : (
                <>
                  <svg className="mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182" />
                  </svg>
                  Re-process
                </>
              )}
            </button>
          )}
          {needsReview && (
            <button
              onClick={handleMarkResolved}
              disabled={resolving}
              className="inline-flex items-center rounded-md bg-gray-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-gray-500 disabled:opacity-50"
            >
              {resolving ? 'Resolving...' : 'Mark as Resolved'}
            </button>
          )}
          {isTerminal && (
            <button
              onClick={handleDownload835}
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
                  Download 835
                </>
              )}
            </button>
          )}
        </div>
      </div>

      {/* Re-process Confirmation Dialog */}
      {showReprocessConfirm && (
        <div className="mt-4 rounded-md bg-amber-50 border border-amber-200 p-4">
          <div className="flex items-start gap-3">
            <svg className="h-5 w-5 text-amber-600 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
            </svg>
            <div className="flex-1">
              <h3 className="text-sm font-semibold text-amber-800">Re-process this document?</h3>
              <p className="mt-1 text-sm text-amber-700">
                This will delete all extracted data and re-run extraction with the latest prompt settings.
                Any manual edits will be lost. Export history will be cleared.
              </p>
              <div className="mt-3 flex gap-2">
                <button
                  onClick={handleReprocess}
                  className="inline-flex items-center rounded-md bg-amber-600 px-3 py-1.5 text-sm font-semibold text-white shadow-sm hover:bg-amber-500"
                >
                  Yes, Re-process
                </button>
                <button
                  onClick={() => setShowReprocessConfirm(false)}
                  className="inline-flex items-center rounded-md bg-white px-3 py-1.5 text-sm font-semibold text-gray-700 shadow-sm ring-1 ring-gray-300 hover:bg-gray-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {downloadError && (
        <div className="mt-4 rounded-md bg-red-50 border border-red-200 p-4">
          <p className="text-sm text-red-700">{downloadError}</p>
        </div>
      )}

      {/* Review Reason Badges */}
      {reviewReasons.length > 0 && (
        <div className="mt-4 flex items-center gap-2">
          <span className="text-xs font-medium text-gray-500 uppercase">Review Reasons:</span>
          {reviewReasons.map((reason) => {
            const badge = reasonBadges[reason] || { label: reason, className: 'bg-gray-100 text-gray-800' }
            return (
              <span key={reason} className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${badge.className}`}>
                {badge.label}
              </span>
            )
          })}
          {doc.review_status === 'resolved' && (
            <span className="inline-flex rounded-full bg-green-100 px-2 py-0.5 text-xs font-semibold text-green-800">
              Resolved
            </span>
          )}
        </div>
      )}

      {/* Export Lock Banner */}
      {isExported && (
        <div className="mt-4 rounded-md bg-yellow-50 border border-yellow-200 p-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <svg className="h-5 w-5 text-yellow-600 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M16.5 10.5V6.75a4.5 4.5 0 10-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 002.25-2.25v-6.75a2.25 2.25 0 00-2.25-2.25H6.75a2.25 2.25 0 00-2.25 2.25v6.75a2.25 2.25 0 002.25 2.25z" />
            </svg>
            <p className="text-sm text-yellow-800">
              <strong>Exported</strong> on {new Date(doc.last_exported_at!).toLocaleDateString()} at {new Date(doc.last_exported_at!).toLocaleTimeString()}.
              Editing is locked to preserve the audit trail.
            </p>
          </div>
          <button
            onClick={handleUnlockForReexport}
            disabled={unlocking}
            className="ml-4 shrink-0 inline-flex items-center rounded-md bg-yellow-600 px-3 py-1.5 text-sm font-semibold text-white shadow-sm hover:bg-yellow-500 disabled:opacity-50"
          >
            {unlocking ? 'Unlocking...' : 'Unlock for Re-export'}
          </button>
        </div>
      )}

      {/* Status Cards */}
      <div className="mt-6 grid gap-4 sm:grid-cols-4">
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-xs font-medium text-gray-500 uppercase">Status</p>
          <span className={`mt-1 inline-flex rounded-full px-2 py-1 text-sm font-semibold ${
            doc.status === 'completed' ? 'bg-green-100 text-green-800' :
            doc.status === 'processing' ? 'bg-yellow-100 text-yellow-800' :
            doc.status === 'failed' || doc.status === 'partial_failure' ? 'bg-red-100 text-red-800' :
            'bg-gray-100 text-gray-800'
          }`}>
            {doc.status === 'partial_failure' ? 'Partial Failure' : doc.status}
          </span>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-xs font-medium text-gray-500 uppercase">Pages</p>
          <p className="mt-1 text-2xl font-bold text-gray-900">{succeededPages}/{doc.total_pages ?? '?'}</p>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-xs font-medium text-gray-500 uppercase">Items Extracted</p>
          <p className="mt-1 text-2xl font-bold text-gray-900">{doc.items_extracted ?? 0}</p>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="text-xs font-medium text-gray-500 uppercase">Errors</p>
          <p className={`mt-1 text-2xl font-bold ${failedPages > 0 ? 'text-red-600' : 'text-gray-900'}`}>
            {failedPages}
          </p>
        </div>
      </div>

      {/* Page Jobs Table */}
      <div className="mt-6 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Page Processing Details</h2>
        </div>
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Page</th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Status</th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Items</th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Error</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {pageJobs.map((job) => (
              <tr key={job.page_number}>
                <td className="px-6 py-3 text-sm text-gray-900">{job.page_number}</td>
                <td className="px-6 py-3">
                  <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${
                    job.status === 'succeeded' ? 'bg-green-100 text-green-800' :
                    job.status === 'failed' ? 'bg-red-100 text-red-800' :
                    job.status === 'queued' ? 'bg-gray-100 text-gray-800' :
                    'bg-yellow-100 text-yellow-800'
                  }`}>
                    {job.status}
                  </span>
                </td>
                <td className="px-6 py-3 text-sm text-gray-500">{job.items_extracted ?? '-'}</td>
                <td className="px-6 py-3 text-sm text-red-500 max-w-md truncate">{job.error_message || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ─── Line Items Section ─────────────────────────────── */}
      {isTerminal && (
        <div className="mt-8">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-900">
              Line Items
              {lineItems.length > 0 && (
                <span className="ml-2 text-sm font-normal text-gray-500">({lineItems.length} items)</span>
              )}
            </h2>
            {lineItems.length > 0 && !editMode && !isExported && (
              <button
                onClick={() => { setEditMode(true); setSelectedPage(lineItemPages[0] || 1) }}
                className="inline-flex items-center rounded-md bg-blue-600 px-3 py-1.5 text-sm font-semibold text-white shadow-sm hover:bg-blue-500"
              >
                <svg className="mr-1.5 h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0115.75 21H5.25A2.25 2.25 0 013 18.75V8.25A2.25 2.25 0 015.25 6H10" />
                </svg>
                Edit / Review
              </button>
            )}
          </div>

          {lineItemsLoading && (
            <div className="flex items-center justify-center py-8">
              <div className="h-6 w-6 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
              <span className="ml-3 text-sm text-gray-500">Loading line items from BigQuery...</span>
            </div>
          )}

          {lineItemsError && (
            <div className="rounded-md bg-red-50 border border-red-200 p-4 mb-4">
              <p className="text-sm text-red-700">{lineItemsError}</p>
            </div>
          )}

          {/* ─── EDIT MODE: Side-by-Side Layout ─────────────── */}
          {editMode && (
            <div>
              {/* Toolbar */}
              <div className="flex items-center justify-between rounded-t-xl border border-gray-200 bg-gray-50 px-4 py-3">
                <div className="flex items-center gap-3">
                  <span className="text-sm font-medium text-gray-700">Page:</span>
                  {lineItemPages.map(pg => (
                    <button
                      key={pg}
                      onClick={() => setSelectedPage(pg)}
                      className={`rounded-md px-3 py-1 text-sm font-medium ${
                        pg === selectedPage
                          ? 'bg-blue-600 text-white'
                          : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-100'
                      }`}
                    >
                      {pg}
                    </button>
                  ))}
                </div>
                <div className="flex items-center gap-2">
                  {saveError && <span className="text-sm text-red-600">{saveError}</span>}
                  {saveSuccess && <span className="text-sm text-green-600">Saved!</span>}
                  <button
                    onClick={handleCancelEdit}
                    className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={handleSave}
                    disabled={saving || !hasEdits()}
                    className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-semibold text-white shadow-sm hover:bg-blue-500 disabled:opacity-50"
                  >
                    {saving ? 'Saving...' : 'Save Changes'}
                  </button>
                </div>
              </div>

              {/* Instructions banner */}
              <div className="border-x border-gray-200 bg-blue-50 px-4 py-2 flex items-center gap-2">
                <svg className="h-4 w-4 text-blue-500 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
                </svg>
                <span className="text-xs text-blue-700">
                  Click any <strong>blue field</strong> on the right to edit it. Compare against the original PDF on the left, then click <strong>Save Changes</strong>.
                </span>
              </div>

              {/* Split Panes */}
              <div className="flex border-x border-b border-gray-200 rounded-b-xl overflow-hidden" style={{ height: '70vh' }}>
                {/* Left: PDF Viewer */}
                <div className="w-1/2 border-r border-gray-200 bg-gray-100">
                  {pdfUrl ? (
                    <iframe
                      src={pdfUrl}
                      className="h-full w-full"
                      title={`Page ${selectedPage}`}
                    />
                  ) : (
                    <div className="flex h-full items-center justify-center text-sm text-gray-400">
                      Loading PDF...
                    </div>
                  )}
                </div>

                {/* Right: Editable Line Items */}
                <div className="w-1/2 overflow-auto bg-white">
                  <table className="min-w-full divide-y divide-gray-200 text-xs">
                    <thead className="bg-gray-50 sticky top-0">
                      <tr>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">Patient</th>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">CPT</th>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">DOS</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Billed</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Allowed</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Paid</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Adjustment</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Deduct</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">CoIns</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Copay</th>
                        <th className="px-3 py-2 text-right font-medium text-gray-500">Non-Cvrd</th>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">Claim #</th>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">Remark</th>
                        <th className="px-3 py-2 text-left font-medium text-gray-500">Remark Desc</th>
                        <th className="px-3 py-2 text-center font-medium text-gray-500">Conf</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {visibleItems.map((item, idx) => {
                        const globalIdx = lineItems.indexOf(item)
                        const key = rowKey(item, globalIdx)
                        const conf = item.confidence_score ? parseInt(item.confidence_score) : null
                        const isLowConf = conf !== null && conf < 85
                        const isRevenue = item.line_type === 'incentive_bonus'

                        return (
                          <tr
                            key={key}
                            className={`${isRevenue ? 'border-l-4 border-l-green-500 bg-green-50/30' : ''} ${isLowConf ? 'bg-purple-50/30' : ''}`}
                          >
                            <td className="px-3 py-2 max-w-[120px]">
                              <input
                                type="text"
                                value={getEditValue(key, 'patient_name', item.patient_name)}
                                onChange={e => handleFieldChange(key, 'patient_name', e.target.value)}
                                className="w-full rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2">
                              <input
                                type="text"
                                value={getEditValue(key, 'cpt_code', item.cpt_code)}
                                onChange={e => handleFieldChange(key, 'cpt_code', e.target.value)}
                                className="w-20 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2">
                              <input
                                type="text"
                                value={getEditValue(key, 'date_of_service', item.date_of_service)}
                                onChange={e => handleFieldChange(key, 'date_of_service', e.target.value)}
                                className="w-24 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'billed_amount', item.billed_amount)}
                                onChange={e => handleFieldChange(key, 'billed_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'allowed_amount', item.allowed_amount)}
                                onChange={e => handleFieldChange(key, 'allowed_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <div className="flex items-center justify-end gap-1">
                                <input
                                  type="text"
                                  value={getEditValue(key, 'paid_amount', item.paid_amount)}
                                  onChange={e => handleFieldChange(key, 'paid_amount', e.target.value)}
                                  className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                                />
                                {isRevenue && (
                                  <span className="whitespace-nowrap rounded bg-green-100 px-1 py-0.5 text-[10px] font-semibold text-green-700">
                                    Revenue
                                  </span>
                                )}
                              </div>
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'contractual_adjustment', item.contractual_adjustment)}
                                onChange={e => handleFieldChange(key, 'contractual_adjustment', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'deductible_amount', item.deductible_amount)}
                                onChange={e => handleFieldChange(key, 'deductible_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'coinsurance_amount', item.coinsurance_amount)}
                                onChange={e => handleFieldChange(key, 'coinsurance_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'copay_amount', item.copay_amount)}
                                onChange={e => handleFieldChange(key, 'copay_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <input
                                type="text"
                                value={getEditValue(key, 'non_covered_amount', item.non_covered_amount)}
                                onChange={e => handleFieldChange(key, 'non_covered_amount', e.target.value)}
                                className="w-16 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 text-right focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2">
                              <input
                                type="text"
                                value={getEditValue(key, 'claim_number', item.claim_number)}
                                onChange={e => handleFieldChange(key, 'claim_number', e.target.value)}
                                className="w-24 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 max-w-[80px] truncate text-gray-500">
                              {item.remark_code || '-'}
                            </td>
                            <td className="px-3 py-2">
                              <input
                                type="text"
                                value={getEditValue(key, 'remark_description', item.remark_description)}
                                onChange={e => handleFieldChange(key, 'remark_description', e.target.value)}
                                className="w-48 rounded border border-blue-300 bg-blue-50/50 px-1.5 py-1 text-xs font-medium text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:bg-white"
                              />
                            </td>
                            <td className="px-3 py-2 text-center">
                              {conf !== null ? (
                                <span className={`inline-flex rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${
                                  isLowConf ? 'bg-purple-100 text-purple-800' : 'bg-gray-100 text-gray-600'
                                }`}>
                                  {conf}
                                </span>
                              ) : (
                                <span className="text-gray-300">-</span>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                      {visibleItems.length === 0 && (
                        <tr>
                          <td colSpan={15} className="px-3 py-6 text-center text-gray-400">
                            No items on this page.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* ─── READ-ONLY MODE: Full Line Items Table ──────── */}
          {!editMode && lineItems.length > 0 && (
            <div className="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">Pg</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">Patient</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">DOS</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">CPT</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Billed</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Allowed</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Adjustment</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Deduct</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">CoIns</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Copay</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Non-Cvrd</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Pat Resp</th>
                      <th className="px-4 py-3 text-right font-medium text-gray-500 uppercase">Paid</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">Claim #</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">Status</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase">Remark</th>
                      <th className="px-4 py-3 text-left font-medium text-gray-500 uppercase min-w-[200px]">Remark Desc</th>
                      <th className="px-4 py-3 text-center font-medium text-gray-500 uppercase">Conf</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {lineItems.map((item, idx) => {
                      const conf = item.confidence_score ? parseInt(item.confidence_score) : null
                      const isLowConf = conf !== null && conf < 85
                      const isRevenue = item.line_type === 'incentive_bonus'

                      return (
                        <tr
                          key={idx}
                          className={`hover:bg-gray-50 ${isRevenue ? 'border-l-4 border-l-green-500 bg-green-50/30' : ''} ${isLowConf ? 'bg-purple-50/30' : ''}`}
                        >
                          <td className="px-4 py-2 text-gray-500">{item.page_number}</td>
                          <td className="px-4 py-2 font-medium text-gray-900 max-w-[150px] truncate">{item.patient_name || '-'}</td>
                          <td className="px-4 py-2 text-gray-500">{item.date_of_service || '-'}</td>
                          <td className="px-4 py-2 font-mono text-gray-900">{item.cpt_code || '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.billed_amount ? `$${parseFloat(item.billed_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.allowed_amount ? `$${parseFloat(item.allowed_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.contractual_adjustment ? `$${parseFloat(item.contractual_adjustment).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.deductible_amount ? `$${parseFloat(item.deductible_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.coinsurance_amount ? `$${parseFloat(item.coinsurance_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.copay_amount ? `$${parseFloat(item.copay_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.non_covered_amount ? `$${parseFloat(item.non_covered_amount).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right text-gray-500">{item.patient_responsibility ? `$${parseFloat(item.patient_responsibility).toFixed(2)}` : '-'}</td>
                          <td className="px-4 py-2 text-right">
                            <div className="flex items-center justify-end gap-1">
                              <span className="font-medium text-gray-900">
                                {item.paid_amount ? `$${parseFloat(item.paid_amount).toFixed(2)}` : '-'}
                              </span>
                              {isRevenue && (
                                <span className="rounded bg-green-100 px-1.5 py-0.5 text-[10px] font-semibold text-green-700">
                                  Found Revenue
                                </span>
                              )}
                            </div>
                          </td>
                          <td className="px-4 py-2 font-mono text-gray-700">{item.claim_number || '-'}</td>
                          <td className="px-4 py-2">
                            <span className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                              item.claim_status === 'Paid' ? 'bg-green-100 text-green-800' :
                              item.claim_status === 'Denied' ? 'bg-red-100 text-red-800' :
                              item.claim_status === 'Adjusted' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-gray-100 text-gray-800'
                            }`}>
                              {item.claim_status || '-'}
                            </span>
                          </td>
                          <td className="px-4 py-2 text-gray-500 max-w-[100px] truncate" title={item.remark_reason || ''}>
                            {item.remark_code || '-'}
                          </td>
                          <td className="px-4 py-2 text-gray-500 min-w-[200px] max-w-[300px]">
                            <span className="block text-xs leading-snug whitespace-normal">{item.remark_description || '-'}</span>
                          </td>
                          <td className="px-4 py-2 text-center">
                            {conf !== null ? (
                              <span className={`inline-flex rounded-full px-1.5 py-0.5 text-[10px] font-semibold ${
                                isLowConf ? 'bg-purple-100 text-purple-800' : 'bg-gray-100 text-gray-600'
                              }`}>
                                {conf}
                              </span>
                            ) : (
                              <span className="text-gray-300">-</span>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!lineItemsLoading && lineItems.length === 0 && !lineItemsError && (
            <div className="rounded-xl border border-gray-200 bg-white p-8 text-center text-sm text-gray-400">
              No line items found in BigQuery for this document.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

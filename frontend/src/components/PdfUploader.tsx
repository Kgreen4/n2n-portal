'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { logAuditEvent } from '@/lib/audit'

/* ── Types ─────────────────────────────────────────────────── */
type JobStatus = 'queued' | 'uploading' | 'triggering' | 'processing' | 'completed' | 'failed'

interface UploadJob {
  id: string
  file: File
  status: JobStatus
  error: string | null
  docId: string | null
  totalPages: number | null
  pagesCompleted: number
  itemsExtracted: number
}

const MAX_CONCURRENT = 2

function validatePdf(file: File): string | null {
  const isPdf = file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')
  if (!isPdf) return 'Please upload a valid PDF document.'
  if (file.size > 50 * 1024 * 1024) return 'File size must be under 50MB.'
  return null
}

/* ── Component ─────────────────────────────────────────────── */
export default function PdfUploader({ practiceId }: { practiceId: string }) {
  const router = useRouter()
  const supabase = createClient()
  const [jobs, setJobs] = useState<UploadJob[]>([])
  const [isDragOver, setIsDragOver] = useState(false)
  const [globalError, setGlobalError] = useState<string | null>(null)
  const processingRef = useRef(false)
  const channelsRef = useRef<Map<string, ReturnType<typeof supabase.channel>>>(new Map())

  /* ── Helpers ──────────────────────────────────────────────── */
  const updateJob = useCallback((jobId: string, patch: Partial<UploadJob>) => {
    setJobs(prev => prev.map(j => j.id === jobId ? { ...j, ...patch } : j))
  }, [])

  const activeCount = jobs.filter(j =>
    j.status === 'uploading' || j.status === 'triggering' || j.status === 'processing'
  ).length

  const allTerminal = jobs.length > 0 && jobs.every(j =>
    j.status === 'completed' || j.status === 'failed'
  )

  const hasQueued = jobs.some(j => j.status === 'queued')
  const showDropZone = jobs.length === 0 || allTerminal || hasQueued

  /* ── Add files to queue ──────────────────────────────────── */
  const addFiles = useCallback((fileList: FileList | File[]) => {
    setGlobalError(null)
    const newJobs: UploadJob[] = []
    const errors: string[] = []

    for (const file of Array.from(fileList)) {
      const err = validatePdf(file)
      if (err) {
        errors.push(`${file.name}: ${err}`)
        continue
      }
      newJobs.push({
        id: crypto.randomUUID(),
        file,
        status: 'queued',
        error: null,
        docId: null,
        totalPages: null,
        pagesCompleted: 0,
        itemsExtracted: 0,
      })
    }

    if (errors.length > 0 && newJobs.length === 0) {
      setGlobalError(errors.join('\n'))
      return
    }

    setJobs(prev => [...prev, ...newJobs])
  }, [])

  /* ── Upload a single job ─────────────────────────────────── */
  const processJob = useCallback(async (job: UploadJob) => {
    // 1. Upload to storage
    updateJob(job.id, { status: 'uploading' })

    try {
      const safeFileName = job.file.name.replace(/[^a-zA-Z0-9.-]/g, '_')
      const filePath = `${practiceId}/${Date.now()}_${safeFileName}`

      const { error: uploadError } = await supabase.storage
        .from('eob-uploads')
        .upload(filePath, job.file)

      if (uploadError) throw new Error(`Storage upload failed: ${uploadError.message}`)

      // 2. Trigger pipeline
      updateJob(job.id, { status: 'triggering' })

      const { data: triggerData, error: triggerError } = await supabase.functions.invoke(
        'trigger-eob-parser',
        {
          body: {
            practice_id: practiceId,
            storage_bucket: 'eob-uploads',
            storage_path: filePath,
            original_file_name: job.file.name,
          },
        }
      )

      if (triggerError) {
        let errorMessage = triggerError.message
        try {
          const errorBody = await (triggerError as any).context?.json?.()
          if (errorBody?.message) errorMessage = errorBody.message
        } catch { /* ignore */ }
        throw new Error(errorMessage)
      }
      if (!triggerData?.success) {
        throw new Error(triggerData?.error || 'Pipeline returned an unexpected error')
      }

      const eobDocId = triggerData.eob_document_id
      if (!eobDocId) throw new Error('No document ID returned from pipeline')

      updateJob(job.id, {
        status: 'processing',
        docId: eobDocId,
        totalPages: triggerData?.total_pages || null,
      })

      // 3. Audit log
      logAuditEvent(supabase, {
        action: 'document.upload',
        resourceType: 'eob_document',
        resourceId: eobDocId,
        metadata: { file_name: job.file.name },
      })

      // 4. Subscribe to Realtime for progress
      const channel = supabase
        .channel(`doc-${eobDocId}`)
        .on(
          'postgres_changes',
          {
            event: 'UPDATE',
            schema: 'public',
            table: 'eob_documents',
            filter: `id=eq.${eobDocId}`,
          },
          (payload) => {
            const newRow = payload.new as {
              status: string
              total_pages: number | null
              items_extracted: number
              pages_completed: number
            }

            setJobs(prev => prev.map(j => {
              if (j.id !== job.id) return j
              const isTerminal = ['completed', 'failed', 'partial_failure'].includes(newRow.status)
              return {
                ...j,
                totalPages: newRow.total_pages ?? j.totalPages,
                pagesCompleted: newRow.pages_completed ?? j.pagesCompleted,
                itemsExtracted: newRow.items_extracted ?? j.itemsExtracted,
                status: isTerminal
                  ? (newRow.status === 'failed' ? 'failed' : 'completed')
                  : j.status,
              }
            }))

            // Clean up channel on terminal state
            if (['completed', 'failed', 'partial_failure'].includes(newRow.status)) {
              supabase.removeChannel(channel)
              channelsRef.current.delete(job.id)
            }
          }
        )
        .subscribe()

      channelsRef.current.set(job.id, channel)

    } catch (err: any) {
      updateJob(job.id, {
        status: 'failed',
        error: err.message || 'Upload failed',
      })
    }
  }, [practiceId, supabase, updateJob])

  /* ── Queue processor — picks up queued jobs respecting concurrency ── */
  useEffect(() => {
    if (processingRef.current) return

    const queued = jobs.filter(j => j.status === 'queued')
    const active = jobs.filter(j =>
      j.status === 'uploading' || j.status === 'triggering' || j.status === 'processing'
    )

    const slotsAvailable = MAX_CONCURRENT - active.length
    if (slotsAvailable <= 0 || queued.length === 0) return

    processingRef.current = true
    const toStart = queued.slice(0, slotsAvailable)

    // Fire uploads without awaiting (they manage their own state)
    for (const job of toStart) {
      processJob(job)
    }

    processingRef.current = false
  }, [jobs, processJob])

  /* ── Cleanup channels on unmount ── */
  useEffect(() => {
    return () => {
      for (const channel of channelsRef.current.values()) {
        supabase.removeChannel(channel)
      }
      channelsRef.current.clear()
    }
  }, [supabase])

  /* ── Event handlers ──────────────────────────────────────── */
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      addFiles(e.target.files)
      // Reset the input so the same files can be re-selected
      e.target.value = ''
    }
  }

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
    if (e.dataTransfer.files.length > 0) {
      addFiles(e.dataTransfer.files)
    }
  }, [addFiles])

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
  }, [])

  const retryJob = useCallback((jobId: string) => {
    setJobs(prev => prev.map(j =>
      j.id === jobId ? { ...j, status: 'queued' as JobStatus, error: null, docId: null, totalPages: null, pagesCompleted: 0, itemsExtracted: 0 } : j
    ))
  }, [])

  const removeJob = useCallback((jobId: string) => {
    const channel = channelsRef.current.get(jobId)
    if (channel) {
      supabase.removeChannel(channel)
      channelsRef.current.delete(jobId)
    }
    setJobs(prev => prev.filter(j => j.id !== jobId))
  }, [supabase])

  const resetAll = useCallback(() => {
    for (const channel of channelsRef.current.values()) {
      supabase.removeChannel(channel)
    }
    channelsRef.current.clear()
    setJobs([])
    setGlobalError(null)
  }, [supabase])

  /* ── Render helpers ──────────────────────────────────────── */
  const statusConfig: Record<JobStatus, { label: string; color: string; icon: string }> = {
    queued:     { label: 'Queued',     color: 'text-gray-500 bg-gray-100',   icon: '🕐' },
    uploading:  { label: 'Uploading',  color: 'text-blue-700 bg-blue-100',   icon: '📤' },
    triggering: { label: 'Starting',   color: 'text-blue-700 bg-blue-100',   icon: '⚡' },
    processing: { label: 'Processing', color: 'text-amber-700 bg-amber-100', icon: '⏳' },
    completed:  { label: 'Complete',   color: 'text-green-700 bg-green-100', icon: '✅' },
    failed:     { label: 'Failed',     color: 'text-red-700 bg-red-100',     icon: '❌' },
  }

  const getProgress = (job: UploadJob): number => {
    if (job.status === 'completed') return 100
    if (job.status === 'queued') return 0
    if (job.status === 'uploading') return 5
    if (job.status === 'triggering') return 10
    if (!job.totalPages || job.totalPages === 0) return 15
    // Pages completed / total pages, scaled between 15-100%
    return Math.min(100, 15 + (job.pagesCompleted / job.totalPages) * 85)
  }

  /* ── Render ──────────────────────────────────────────────── */
  return (
    <div className="max-w-2xl mx-auto mt-8 space-y-6">
      {/* Drop zone — always visible when no jobs or can add more */}
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        className={`bg-white border-2 border-dashed rounded-xl p-8 text-center transition-colors ${
          isDragOver
            ? 'border-blue-500 bg-blue-50'
            : 'border-gray-300 hover:bg-gray-50'
        }`}
      >
        <input
          type="file"
          id="file-upload"
          accept=".pdf,application/pdf"
          multiple
          className="hidden"
          onChange={handleFileChange}
        />
        <label
          htmlFor="file-upload"
          className="cursor-pointer flex flex-col items-center justify-center space-y-3"
        >
          <svg className="w-10 h-10 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"
              d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
          </svg>
          <span className="text-sm font-medium text-blue-600">
            Click or drag PDFs to upload
          </span>
          <span className="text-xs text-gray-500">
            Multiple files supported &middot; PDF up to 50MB each
          </span>
        </label>
      </div>

      {/* Global error */}
      {globalError && (
        <div className="text-sm text-red-600 bg-red-50 p-3 rounded-lg border border-red-200" role="alert">
          {globalError}
        </div>
      )}

      {/* Job queue */}
      {jobs.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-900">
              Upload Queue
              <span className="ml-2 text-xs font-normal text-gray-500">
                {jobs.filter(j => j.status === 'completed').length}/{jobs.length} complete
              </span>
            </h3>
            {allTerminal && (
              <button
                onClick={resetAll}
                className="text-xs text-gray-500 hover:text-gray-700"
              >
                Clear all
              </button>
            )}
          </div>

          <div className="divide-y divide-gray-100">
            {jobs.map(job => {
              const cfg = statusConfig[job.status]
              const progress = getProgress(job)
              const isActive = job.status === 'uploading' || job.status === 'triggering' || job.status === 'processing'

              return (
                <div key={job.id} className="px-5 py-4">
                  {/* Top row: filename + status badge + remove */}
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2 min-w-0 flex-1">
                      <span className="text-base leading-none">{cfg.icon}</span>
                      <span className="text-sm font-medium text-gray-900 truncate" title={job.file.name}>
                        {job.file.name}
                      </span>
                      <span className="text-xs text-gray-400 flex-shrink-0">
                        {(job.file.size / (1024 * 1024)).toFixed(1)} MB
                      </span>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0 ml-3">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
                        {cfg.label}
                      </span>
                      {(job.status === 'queued' || job.status === 'failed' || job.status === 'completed') && (
                        <button
                          onClick={() => removeJob(job.id)}
                          className="text-gray-400 hover:text-gray-600 p-0.5"
                          title="Remove"
                        >
                          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                          </svg>
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Progress bar */}
                  <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-700 ease-out ${
                        job.status === 'completed' ? 'bg-green-500' :
                        job.status === 'failed' ? 'bg-red-400' :
                        'bg-blue-500'
                      } ${isActive ? 'animate-pulse' : ''}`}
                      style={{ width: `${progress}%` }}
                    />
                  </div>

                  {/* Bottom row: detail text */}
                  <div className="mt-1.5 flex items-center justify-between">
                    <span className="text-xs text-gray-500">
                      {job.status === 'queued' && 'Waiting in queue...'}
                      {job.status === 'uploading' && 'Uploading to storage...'}
                      {job.status === 'triggering' && 'Starting extraction pipeline...'}
                      {job.status === 'processing' && job.totalPages && (
                        <>
                          {job.pagesCompleted}/{job.totalPages} pages
                          {job.itemsExtracted > 0 && ` \u00B7 ${job.itemsExtracted} items extracted`}
                        </>
                      )}
                      {job.status === 'processing' && !job.totalPages && 'Splitting pages...'}
                      {job.status === 'completed' && (
                        <>
                          {job.totalPages} page{(job.totalPages ?? 0) > 1 ? 's' : ''} processed
                          {job.itemsExtracted > 0 && ` \u00B7 ${job.itemsExtracted} line items`}
                        </>
                      )}
                      {job.status === 'failed' && (
                        <span className="text-red-600">{job.error || 'Processing failed'}</span>
                      )}
                    </span>

                    {/* Retry button for failed jobs */}
                    {job.status === 'failed' && (
                      <button
                        onClick={() => retryJob(job.id)}
                        className="text-xs font-medium text-blue-600 hover:text-blue-500"
                      >
                        Retry
                      </button>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Action buttons */}
      {allTerminal && jobs.length > 0 && (
        <div className="flex justify-center">
          <button
            onClick={() => { router.push('/documents'); router.refresh() }}
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg shadow-sm transition-colors"
          >
            View All Documents
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
            </svg>
          </button>
        </div>
      )}
    </div>
  )
}

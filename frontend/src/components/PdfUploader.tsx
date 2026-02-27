'use client'

import { useState, useEffect, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { logAuditEvent } from '@/lib/audit'

type UploadStatus = 'idle' | 'uploading' | 'triggering' | 'processing' | 'completed' | 'failed'

function validatePdf(file: File): string | null {
  const isPdf = file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')
  if (!isPdf) return 'Please upload a valid PDF document.'
  if (file.size > 50 * 1024 * 1024) return 'File size must be under 50MB.'
  return null
}

export default function PdfUploader({ practiceId }: { practiceId: string }) {
  const router = useRouter()
  const supabase = createClient()
  const [file, setFile] = useState<File | null>(null)
  const [status, setStatus] = useState<UploadStatus>('idle')
  const [error, setError] = useState<string | null>(null)
  const [docInfo, setDocInfo] = useState<{
    id: string
    totalPages: number | null
    status: string
    itemsExtracted: number
  } | null>(null)
  const [isDragOver, setIsDragOver] = useState(false)

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0]
    setError(null)
    if (!selected) return

    console.log('[PdfUploader] file selected via picker:', {
      name: selected.name, type: selected.type, size: selected.size,
    })

    const validationError = validatePdf(selected)
    if (validationError) {
      setError(validationError)
      return
    }

    setFile(selected)
  }

  // Core upload logic — accepts file directly to avoid React state batching issues
  const startUpload = useCallback(async (uploadFile: File) => {
    if (!uploadFile || !practiceId) return
    setStatus('uploading')
    setError(null)

    try {
      // 1. Sanitize filename and create storage path
      const safeFileName = uploadFile.name.replace(/[^a-zA-Z0-9.-]/g, '_')
      const filePath = `${practiceId}/${Date.now()}_${safeFileName}`

      console.log('[PdfUploader] uploading to storage:', filePath)

      // 2. Upload to Supabase Storage
      const { error: uploadError } = await supabase.storage
        .from('eob-uploads')
        .upload(filePath, uploadFile)

      if (uploadError) {
        console.error('[PdfUploader] storage upload error:', uploadError)
        throw new Error(`Storage upload failed: ${uploadError.message}`)
      }

      console.log('[PdfUploader] storage upload succeeded, triggering pipeline...')

      // 3. Trigger the backend pipeline via Edge Function
      //    The user's JWT is sent automatically by supabase.functions.invoke()
      //    and trigger-eob-parser verifies it internally (dual-client pattern).
      setStatus('triggering')

      const { data: triggerData, error: triggerError } = await supabase.functions.invoke(
        'trigger-eob-parser',
        {
          body: {
            practice_id: practiceId,
            storage_bucket: 'eob-uploads',
            storage_path: filePath,
            original_file_name: uploadFile.name,  // preserve human-readable name with spaces, #, etc.
          },
        }
      )

      // Robust error handling: log everything for debugging
      console.log('[PdfUploader] trigger response:', { triggerData, triggerError })

      if (triggerError) {
        console.error('[PdfUploader] trigger error:', triggerError)
        // Try to extract the actual error message from the response body
        let errorMessage = triggerError.message
        try {
          const errorBody = await (triggerError as any).context?.json?.()
          if (errorBody?.message) errorMessage = errorBody.message
          else if (errorBody?.error === 'duplicate_upload') errorMessage = errorBody.message || 'This file has already been uploaded.'
        } catch { /* ignore parse errors */ }
        throw new Error(errorMessage)
      }
      if (!triggerData?.success) {
        console.error('[PdfUploader] trigger returned failure:', triggerData)
        throw new Error(triggerData?.error || 'Pipeline returned an unexpected error')
      }

      // 4. Extract document ID from response and start listening
      const eobDocId = triggerData.eob_document_id
      if (!eobDocId) throw new Error('No document ID returned from pipeline')

      console.log('[PdfUploader] pipeline started, document ID:', eobDocId)

      setDocInfo({
        id: eobDocId,
        totalPages: triggerData?.total_pages || null,
        status: triggerData?.status || 'processing',
        itemsExtracted: 0,
      })
      setStatus('processing')

      // Audit log — track upload for HIPAA compliance
      logAuditEvent(supabase, {
        action: 'document.upload',
        resourceType: 'eob_document',
        resourceId: eobDocId,
        metadata: { file_name: file?.name ?? 'unknown' },
      })
    } catch (err: any) {
      console.error('[PdfUploader] upload failed:', err)
      setError(err.message || 'An unexpected error occurred during upload.')
      setStatus('failed')
    }
  }, [practiceId, supabase])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
    setError(null)

    const dropped = e.dataTransfer.files?.[0]
    if (!dropped) return

    console.log('[PdfUploader] file dropped:', {
      name: dropped.name, type: dropped.type, size: dropped.size,
    })

    const validationError = validatePdf(dropped)
    if (validationError) {
      setError(validationError)
      return
    }

    // Auto-start upload on drop — pass file directly to avoid state batching delay
    setFile(dropped)
    startUpload(dropped)
  }, [startUpload])

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
  }, [])

  // Subscribe to Realtime updates when we have a document ID
  useEffect(() => {
    if (!docInfo?.id) return

    const channel = supabase
      .channel(`doc-${docInfo.id}`)
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'eob_documents',
          filter: `id=eq.${docInfo.id}`,
        },
        (payload) => {
          const newRow = payload.new as {
            status: string
            total_pages: number | null
            items_extracted: number
          }

          console.log('[PdfUploader] realtime update:', newRow)

          setDocInfo((prev) =>
            prev
              ? {
                  ...prev,
                  status: newRow.status,
                  totalPages: newRow.total_pages,
                  itemsExtracted: newRow.items_extracted ?? 0,
                }
              : prev
          )

          if (['completed', 'failed', 'partial_failure'].includes(newRow.status)) {
            if (newRow.status === 'completed' || newRow.status === 'partial_failure') {
              setStatus('completed')
            } else {
              setStatus('failed')
            }
          }
        }
      )
      .subscribe()

    return () => {
      supabase.removeChannel(channel)
    }
  }, [docInfo?.id, supabase])

  // Auto-redirect after completion
  useEffect(() => {
    if (status === 'completed') {
      const timer = setTimeout(() => {
        router.push('/documents')
        router.refresh()
      }, 3000)
      return () => clearTimeout(timer)
    }
  }, [status, router])

  // Button click handler — uses file from state
  const handleButtonClick = () => {
    if (file) startUpload(file)
  }

  const isProcessing = status === 'uploading' || status === 'triggering' || status === 'processing'

  return (
    <div className="max-w-xl mx-auto mt-8 bg-white p-8 border border-gray-200 rounded-xl shadow-sm">
      <h2 className="text-xl font-semibold text-gray-900 mb-6">Upload EOB Document</h2>

      {/* File picker / drag-and-drop zone */}
      {status === 'idle' && (
        <>
          <div
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            className={`border-2 border-dashed rounded-lg p-10 text-center transition-colors ${
              isDragOver
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-300 hover:bg-gray-50'
            }`}
          >
            <input
              type="file"
              id="file-upload"
              accept=".pdf,application/pdf"
              className="hidden"
              onChange={handleFileChange}
            />
            <label
              htmlFor="file-upload"
              className="cursor-pointer flex flex-col items-center justify-center space-y-3"
            >
              <svg
                className="w-10 h-10 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                />
              </svg>
              <span className="text-sm font-medium text-blue-600">
                {file ? file.name : 'Click or drag a PDF to upload'}
              </span>
              {!file && <span className="text-xs text-gray-500">PDF up to 50MB</span>}
              {file && (
                <span className="text-xs text-gray-500">
                  {(file.size / (1024 * 1024)).toFixed(1)} MB
                </span>
              )}
            </label>
          </div>

          <div aria-live="polite" className="mt-4">
            {error && (
              <div className="text-sm text-red-600 bg-red-50 p-3 rounded-md" role="alert">
                {error}
              </div>
            )}
          </div>

          <button
            onClick={handleButtonClick}
            disabled={!file}
            className={`mt-6 w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white transition-colors ${
              file
                ? 'bg-blue-600 hover:bg-blue-700 animate-pulse'
                : 'bg-gray-300 cursor-not-allowed'
            }`}
          >
            Upload and Process
          </button>
        </>
      )}

      {/* Processing status */}
      {isProcessing && (
        <div className="text-center space-y-4">
          <div className="flex justify-center">
            <svg
              className="animate-spin h-10 w-10 text-blue-600"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
            >
              <circle
                className="opacity-25"
                cx="12"
                cy="12"
                r="10"
                stroke="currentColor"
                strokeWidth="4"
              />
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              />
            </svg>
          </div>
          <p className="text-sm font-medium text-gray-900">
            {status === 'uploading' && 'Uploading PDF to storage...'}
            {status === 'triggering' && 'Starting extraction pipeline...'}
            {status === 'processing' && 'Extracting EOB data with AI...'}
          </p>
          {docInfo?.totalPages && (
            <p className="text-xs text-gray-500">
              Processing {docInfo.totalPages} page{docInfo.totalPages > 1 ? 's' : ''}
              {docInfo.itemsExtracted > 0 && ` — ${docInfo.itemsExtracted} items extracted so far`}
            </p>
          )}
        </div>
      )}

      {/* Completed */}
      {status === 'completed' && docInfo && (
        <div className="text-center space-y-4">
          <div className="flex justify-center">
            <svg className="h-12 w-12 text-green-500" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p className="text-sm font-medium text-gray-900">
            Extraction {docInfo.status === 'partial_failure' ? 'partially completed' : 'complete'}!
          </p>
          <p className="text-xs text-gray-500">
            {docInfo.totalPages} page{(docInfo.totalPages ?? 0) > 1 ? 's' : ''} processed
            {docInfo.itemsExtracted > 0 && ` — ${docInfo.itemsExtracted} line items extracted`}
          </p>
          <p className="text-xs text-gray-400">Redirecting to documents...</p>
        </div>
      )}

      {/* Failed */}
      {status === 'failed' && (
        <div className="text-center space-y-4">
          <div className="flex justify-center">
            <svg className="h-12 w-12 text-red-500" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 9.75l4.5 4.5m0-4.5l-4.5 4.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p className="text-sm font-medium text-red-600">
            {error || 'Processing failed. Please try again.'}
          </p>
          <button
            onClick={() => {
              setStatus('idle')
              setFile(null)
              setError(null)
              setDocInfo(null)
            }}
            className="mt-2 text-sm font-medium text-blue-600 hover:text-blue-500"
          >
            Try again
          </button>
        </div>
      )}
    </div>
  )
}

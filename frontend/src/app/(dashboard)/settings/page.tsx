'use client'

import { useState, useEffect } from 'react'
import { createClient } from '@/lib/supabase/client'

interface PracticeSettings {
  practice_id: string
  gdrive_folder_id: string | null
  gdrive_folder_name: string | null
  watcher_enabled: boolean
  watcher_interval_minutes: number
  auto_move_processed: boolean
}

export default function SettingsPage() {
  const supabase = createClient()
  const [practiceId, setPracticeId] = useState<string | null>(null)
  const [settings, setSettings] = useState<PracticeSettings | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  // Bank CSV state
  const [csvFile, setCsvFile] = useState<File | null>(null)
  const [csvUploading, setCsvUploading] = useState(false)
  const [csvResult, setCsvResult] = useState<{ success: boolean; message: string } | null>(null)

  // Scan folder state
  const [scanning, setScanning] = useState(false)
  const [scanDate, setScanDate] = useState<string>(new Date().toISOString().split('T')[0])
  const [includeCompleted, setIncludeCompleted] = useState(false)
  const [scanResult, setScanResult] = useState<{
    folder_name: string | null
    after_date: string | null
    found: number
    skipped_completed: number
    already_processed: number
    triggered: number
    errors: number
  } | null>(null)
  const [scanError, setScanError] = useState<string | null>(null)

  // Form fields
  const [folderId, setFolderId] = useState('')
  const [folderName, setFolderName] = useState('')
  const [interval, setInterval] = useState(5)
  const [autoMove, setAutoMove] = useState(true)
  const [watcherEnabled, setWatcherEnabled] = useState(false)

  // Load practice ID and settings
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

      const { data: s } = await supabase
        .from('practice_settings')
        .select('*')
        .eq('practice_id', link.practice_id)
        .single()

      if (s) {
        setSettings(s)
        setFolderId(s.gdrive_folder_id || '')
        setFolderName(s.gdrive_folder_name || '')
        setInterval(s.watcher_interval_minutes || 5)
        setAutoMove(s.auto_move_processed ?? true)
        setWatcherEnabled(s.watcher_enabled ?? false)
      }
      setLoading(false)
    }
    load()
  }, [supabase])

  const handleSave = async () => {
    if (!practiceId) return
    setSaving(true)
    setSaved(false)

    const { error } = await supabase
      .from('practice_settings')
      .upsert({
        practice_id: practiceId,
        gdrive_folder_id: folderId || null,
        gdrive_folder_name: folderName || null,
        watcher_enabled: watcherEnabled,
        watcher_interval_minutes: interval,
        auto_move_processed: autoMove,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'practice_id' })

    setSaving(false)
    if (!error) {
      setSaved(true)
      setTimeout(() => setSaved(false), 3000)
    }
  }

  const handleScanFolder = async () => {
    if (!practiceId) return
    setScanning(true)
    setScanResult(null)
    setScanError(null)

    try {
      const body: Record<string, unknown> = { practice_id: practiceId }
      if (scanDate) body.after_date = scanDate
      if (includeCompleted) body.include_completed = true
      const { data, error } = await supabase.functions.invoke('scan-drive-folder', { body })
      if (error) throw new Error(error.message)
      setScanResult(data)
    } catch (err: any) {
      setScanError(err.message || 'Scan failed')
    } finally {
      setScanning(false)
    }
  }

  const handleCsvUpload = async () => {
    if (!csvFile || !practiceId) return
    setCsvUploading(true)
    setCsvResult(null)

    try {
      const text = await csvFile.text()

      const { data, error } = await supabase.functions.invoke('parse-bank-csv', {
        body: { practice_id: practiceId, csv_content: text, source_file: csvFile.name },
      })

      if (error) throw new Error(error.message)
      if (!data?.success) throw new Error(data?.error || 'Unknown error')

      setCsvResult({
        success: true,
        message: `Imported ${data.inserted} deposits. ${data.matched} matched, ${data.discrepancies} discrepancies, ${data.unmatched} unmatched.`,
      })
      setCsvFile(null)
    } catch (err: any) {
      setCsvResult({ success: false, message: err.message || 'Upload failed' })
    } finally {
      setCsvUploading(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin h-8 w-8 border-4 border-blue-600 border-t-transparent rounded-full" />
      </div>
    )
  }

  return (
    <div className="space-y-8 max-w-2xl">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Practice Settings</h1>
        <p className="mt-1 text-sm text-gray-500">
          Configure folder watching and bank reconciliation for your practice.
        </p>
      </div>

      {/* EOB Folder Watcher */}
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6 space-y-5">
        <div className="flex items-center gap-3">
          <svg className="w-6 h-6 text-blue-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z" />
          </svg>
          <h2 className="text-lg font-semibold text-gray-900">EOB Folder Watcher</h2>
        </div>
        <p className="text-sm text-gray-500">
          Connect a Google Shared Drive folder. New PDFs will be automatically ingested every few minutes via n8n.
        </p>

        <div className="grid grid-cols-1 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Google Drive Folder ID
            </label>
            <input
              type="text"
              value={folderId}
              onChange={e => setFolderId(e.target.value)}
              placeholder="e.g. 1A2B3C4D5E6F7G8H9I0J"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-base text-gray-900 placeholder:text-gray-400 focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-400">
              Find this in your Google Drive folder URL after /folders/
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Folder Display Name
            </label>
            <input
              type="text"
              value={folderName}
              onChange={e => setFolderName(e.target.value)}
              placeholder="e.g. EOB Inbox"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-base text-gray-900 placeholder:text-gray-400 focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Poll Interval (minutes)
              </label>
              <select
                value={interval}
                onChange={e => setInterval(Number(e.target.value))}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-base text-gray-900 focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              >
                <option value={2}>2 minutes</option>
                <option value={5}>5 minutes</option>
                <option value={10}>10 minutes</option>
                <option value={15}>15 minutes</option>
                <option value={30}>30 minutes</option>
              </select>
            </div>

            <div className="flex items-end pb-1">
              <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  checked={autoMove}
                  onChange={e => setAutoMove(e.target.checked)}
                  className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                Auto-move to "Processed"
              </label>
            </div>
          </div>

          <div className="flex items-center justify-between pt-2 border-t border-gray-100">
            <label className="flex items-center gap-3 cursor-pointer">
              <div className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${watcherEnabled ? 'bg-blue-600' : 'bg-gray-300'}`}>
                <input
                  type="checkbox"
                  checked={watcherEnabled}
                  onChange={e => setWatcherEnabled(e.target.checked)}
                  className="sr-only"
                />
                <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${watcherEnabled ? 'translate-x-6' : 'translate-x-1'}`} />
              </div>
              <span className="text-sm font-medium text-gray-700">
                {watcherEnabled ? 'Watcher Enabled' : 'Watcher Disabled'}
              </span>
            </label>

            <div className="flex items-center gap-3">
              {saved && (
                <span className="text-sm text-green-600 font-medium">Saved!</span>
              )}
              <button
                onClick={handleSave}
                disabled={saving}
                className="inline-flex items-center px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-medium rounded-lg transition-colors"
              >
                {saving ? 'Saving...' : 'Save Settings'}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Scan & Process Folder */}
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6 space-y-4">
        <div className="flex items-center gap-3">
          <svg className="w-6 h-6 text-indigo-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
          <h2 className="text-lg font-semibold text-gray-900">Scan & Process Folder</h2>
        </div>
        <p className="text-sm text-gray-500">
          Scan the configured Drive folder for unprocessed PDFs and trigger extraction.
          By default, files with <span className="font-mono text-xs bg-gray-100 px-1 rounded">COMPLETED</span> in
          the name are skipped — use the override below for catch-up runs.
        </p>

        <div className="space-y-3">
          <div className="flex items-center gap-3">
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-gray-600">Only files created on or after</label>
              <div className="flex items-center gap-2">
                <input
                  type="date"
                  value={scanDate}
                  onChange={e => setScanDate(e.target.value)}
                  className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm text-gray-900 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                />
                <button
                  onClick={() => setScanDate('')}
                  className="text-xs text-gray-400 hover:text-gray-600 underline"
                >
                  Clear (scan all)
                </button>
              </div>
            </div>
          </div>

          <label className="flex items-center gap-2 cursor-pointer w-fit">
            <input
              type="checkbox"
              checked={includeCompleted}
              onChange={e => setIncludeCompleted(e.target.checked)}
              className="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
            />
            <span className="text-sm text-gray-700">
              Include <span className="font-mono text-xs bg-gray-100 px-1 rounded">COMPLETED</span> files
              <span className="ml-1 text-gray-400 text-xs">(catch-up run — duplicates are still skipped)</span>
            </span>
          </label>

          <div className="flex items-center gap-3">
            <button
              onClick={handleScanFolder}
              disabled={scanning || !folderId}
              className="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition-colors"
            >
              {scanning ? (
                <>
                  <span className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full" />
                  Scanning…
                </>
              ) : (
                <>
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />
                  </svg>
                  Scan &amp; Process Folder
                </>
              )}
            </button>
            {!folderId && (
              <p className="text-xs text-gray-400">Save a folder ID above to enable scanning.</p>
            )}
          </div>
        </div>

        {scanResult && (
          <div className="bg-indigo-50 border border-indigo-200 rounded-lg p-4 text-sm space-y-1">
            <p className="font-medium text-indigo-900">
              Scan complete{scanResult.folder_name ? ` — ${scanResult.folder_name}` : ''}
              {scanResult.after_date && (
                <span className="font-normal text-indigo-600"> · files from {scanResult.after_date} onward</span>
              )}
            </p>
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-indigo-700">
              <span>{scanResult.found} PDF{scanResult.found !== 1 ? 's' : ''} found</span>
              {scanResult.skipped_completed > 0 && (
                <span className="text-amber-600">{scanResult.skipped_completed} COMPLETED skipped</span>
              )}
              {scanResult.already_processed > 0 && (
                <span className="text-gray-500">{scanResult.already_processed} already processed</span>
              )}
              <span className="text-green-700 font-medium">{scanResult.triggered} triggered ✓</span>
              {scanResult.errors > 0 && (
                <span className="text-red-600">{scanResult.errors} error{scanResult.errors !== 1 ? 's' : ''}</span>
              )}
            </div>
          </div>
        )}

        {scanError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
            {scanError}
          </div>
        )}
      </div>

      {/* Bank Reconciliation CSV Upload */}
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6 space-y-5">
        <div className="flex items-center gap-3">
          <svg className="w-6 h-6 text-green-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 21v-8.25M15.75 21v-8.25M8.25 21v-8.25M3 9l9-6 9 6m-1.5 12V10.332A48.36 48.36 0 0012 9.75c-2.551 0-5.056.2-7.5.582V21M3 21h18M12 6.75h.008v.008H12V6.75z" />
          </svg>
          <h2 className="text-lg font-semibold text-gray-900">Bank Reconciliation</h2>
        </div>
        <p className="text-sm text-gray-500">
          Upload a bank statement CSV to reconcile EOB payments against actual deposits. The system will auto-match by check number.
        </p>

        <div className="bg-gray-50 rounded-lg p-4 text-xs text-gray-600 space-y-1">
          <p className="font-medium text-gray-700">Expected CSV format:</p>
          <code className="block bg-white border border-gray-200 rounded p-2 font-mono">
            date,check_number,amount,description<br/>
            2026-01-21,91938141,175.49,UHC CLAIM PAYMENT<br/>
            2026-01-21,CK847291,77.98,MANHATTAN LIFE INS
          </code>
          <p>Columns: <strong>date</strong> (YYYY-MM-DD or MM/DD/YYYY), <strong>check_number</strong>, <strong>amount</strong>, <strong>description</strong> (optional)</p>
        </div>

        <div className="flex items-center gap-3">
          <input
            type="file"
            accept=".csv,text/csv"
            onChange={e => { setCsvFile(e.target.files?.[0] || null); setCsvResult(null) }}
            className="block w-full text-base text-gray-700 file:mr-3 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-base file:font-medium file:bg-green-50 file:text-green-700 hover:file:bg-green-100 file:cursor-pointer"
          />
          <button
            onClick={handleCsvUpload}
            disabled={!csvFile || csvUploading}
            className="inline-flex items-center px-4 py-2 bg-green-600 hover:bg-green-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors whitespace-nowrap"
          >
            {csvUploading ? 'Importing...' : 'Import CSV'}
          </button>
        </div>

        {csvResult && (
          <div className={`text-sm p-3 rounded-lg ${csvResult.success ? 'bg-green-50 text-green-700 border border-green-200' : 'bg-red-50 text-red-700 border border-red-200'}`}>
            {csvResult.message}
          </div>
        )}
      </div>

      {/* Folder Structure Guide */}
      <div className="bg-gray-50 border border-gray-200 rounded-xl p-6 space-y-3">
        <h3 className="text-sm font-semibold text-gray-700">Recommended Folder Structure</h3>
        <pre className="text-xs text-gray-600 font-mono leading-relaxed">
{`EOB Inbox/                     <- Watched folder
  2026-03-01/                  <- Optional date subfolders
    BCBS_Louisiana_EOB.pdf
    UHC_Texas_Check.pdf
  2026-03-02/
    State_Farm_EOB.pdf
  Processed/                   <- Auto-created, files moved here
  (any PDF at root level is also picked up)`}
        </pre>
        <p className="text-xs text-gray-500">
          The n8n watcher scans recursively, excluding the "Processed" subfolder. Date subfolders are optional.
        </p>
      </div>
    </div>
  )
}

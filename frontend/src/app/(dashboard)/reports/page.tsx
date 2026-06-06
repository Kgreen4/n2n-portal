'use client'

import { useEffect, useRef, useState } from 'react'
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
  adjustment_amount: number | null
  contractual_adjustment: number | null
  deductible_amount: number | null
  coinsurance_amount: number | null
  copay_amount: number | null
  non_covered_amount: number | null
  claim_status: string | null
  claim_number: string | null
  payer_name: string | null
  remark_code: string | null
  source_check_number: string | null
  eob_payment_id: string | null
  remark_reason: string | null
  remark_description: string | null
  payment_date: string | null
  confidence_score: number | null
  created_at: string
}

interface EobPayment {
  id: string
  eob_document_id: string
  practice_id: string
  check_number: string | null
  payment_date: string | null
  payer_name: string | null
  payer_id: string | null
  check_amount: number | null
}

interface CheckGapRow {
  checkNumber: string
  paymentDate: string | null
  payerName: string | null
  checkAmount: number
  extractedPaid: number
  gap: number
  sourceDoc: string
  docId: string
}

interface PayerRow {
  payer: string
  claims: number
  billed: number
  paid: number
  denied: number
  adjustment: number
}

interface StatusRow {
  status: string
  count: number
  paid: number
}

interface DenialRow {
  code: string
  description: string
  count: number
  billed: number
}

interface DepositRow {
  payer: string
  check_number: string
  payment_date: string | null
  amount: number
  source_doc: string
}

interface DenialByPayerRow {
  payer: string
  count: number
  billed: number
}

interface DocGapRow {
  sourceDoc: string
  docId: string | null   // eob_document_id — null if not resolvable from line items
  depositAmount: number
  extractedPaid: number
  gap: number
}

const fmt = (n: number) =>
  n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 })

const fmtDec = (n: number) =>
  n.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 })

const pct = (num: number, den: number) =>
  den === 0 ? '—' : `${((num / den) * 100).toFixed(1)}%`

const fmtDate = (d: string | null) => {
  if (!d) return null
  // Handle both "YYYY-MM-DD" and ISO timestamps
  return d.length > 10 ? d.slice(0, 10) : d
}

const normalizePayer = (name: string | null): string =>
  name ? name.trim().replace(/\s+/g, ' ') : '(Unknown Payer)'

// ── Levenshtein distance (Wagner-Fischer, O(n) space) ───────────────────────
// Used to detect OCR-mangled check number variants within the same document
// (e.g. "0000414227" vs "0000444227" — one digit misread by Gemini).
// Both strings are typically ≤ 15 chars so the O(m×n) cell cost is negligible.
function levenshtein(a: string, b: string): number {
  if (a === b) return 0
  const m = a.length, n = b.length
  if (m === 0) return n
  if (n === 0) return m
  // dp[j] = edit distance between a[0..i-1] and b[0..j-1]
  const dp: number[] = Array.from({ length: n + 1 }, (_, j) => j)
  for (let i = 1; i <= m; i++) {
    let prev = dp[0]
    dp[0] = i
    for (let j = 1; j <= n; j++) {
      const temp = dp[j]
      dp[j] = a[i - 1] === b[j - 1] ? prev : 1 + Math.min(prev, dp[j - 1], dp[j])
      prev = temp
    }
  }
  return dp[n]
}

export default function ReportsPage() {
  const supabase = createClient()
  const [period, setPeriod] = useState<Period>('30d')
  const [items, setItems] = useState<LineItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const PAGE_SIZE = 25
  // Increment to force the data useEffect to re-run after a reprocess is triggered
  const [refreshKey, setRefreshKey] = useState(0)
  const [reprocessingDocs, setReprocessingDocs] = useState<Set<string>>(new Set())
  const [reprocessedDocs, setReprocessedDocs] = useState<Set<string>>(new Set())
  const [reprocessError, setReprocessError] = useState<string | null>(null)
  // Live status from eob_documents — polled while any doc is in-flight
  const [docStatuses, setDocStatuses] = useState<Map<string, string>>(new Map())
  const prevDocStatusesRef = useRef<Map<string, string>>(new Map())
  const [eobPayments, setEobPayments] = useState<EobPayment[]>([])

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

      // Load eob_payments for check-level gap analysis (hierarchical ingestion path)
      {
        let pq = supabase
          .from('eob_payments')
          .select('*')
          .eq('practice_id', link.practice_id)
          .order('created_at', { ascending: false })
          .limit(500)
        if (period !== 'all') {
          const days = period === '7d' ? 7 : period === '30d' ? 30 : 90
          pq = pq.gte('created_at', new Date(Date.now() - days * 86400_000).toISOString())
        }
        const { data: pmtData } = await pq
        if (!cancelled) setEobPayments(pmtData ?? [])
      }

      setLoading(false)
    }

    load()
    return () => { cancelled = true }
  // refreshKey is intentionally included: incrementing it re-runs load() after a reprocess
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [period, refreshKey])

  // ── Live document-status polling ──────────────────────────────────────────
  // After every items reload, fetch status for all docs present in
  // the data, then poll every 5 s while any doc is queued or processing.
  // When a doc transitions to finished, increment refreshKey so the gap table
  // re-derives from the freshly extracted line items.
  useEffect(() => {
    // Reset transition tracker on every items reload to avoid false re-triggers
    prevDocStatusesRef.current = new Map()

    const docIds = Array.from(new Set(
      items.map(i => i.eob_document_id).filter((id): id is string => !!id)
    ))
    if (docIds.length === 0) {
      setDocStatuses(new Map())
      return
    }

    let cancelled = false
    let pollTimer: ReturnType<typeof setTimeout> | null = null

    async function fetchStatuses() {
      if (cancelled) return
      const { data } = await supabase
        .from('eob_documents')
        .select('id, status')
        .in('id', docIds)
      if (cancelled || !data) return

      const newMap = new Map<string, string>()
      for (const row of data) newMap.set(row.id, row.status ?? '')

      // Detect transitions from in-progress → finished so we can reload line items
      let anyJustFinished = false
      for (const [id, status] of newMap) {
        const prev = prevDocStatusesRef.current.get(id) ?? ''
        if ((prev === 'queued' || prev === 'processing') &&
            status !== 'queued' && status !== 'processing') {
          anyJustFinished = true
        }
      }

      setDocStatuses(newMap)
      prevDocStatusesRef.current = newMap

      if (anyJustFinished) {
        // Reload line items — the items useEffect will restart this poll cycle
        setRefreshKey(k => k + 1)
        return
      }

      const anyInProgress = data.some(
        r => r.status === 'queued' || r.status === 'processing'
      )
      if (anyInProgress) {
        pollTimer = setTimeout(fetchStatuses, 5000)
      }
    }

    fetchStatuses()
    return () => {
      cancelled = true
      if (pollTimer) clearTimeout(pollTimer)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items])

  // ── Aggregations ──────────────────────────────────────────────
  //
  // medicalItems is built in four steps to handle Provider Summary Roster pages
  // (e.g. BCBS pages 106-110) that appear at the end of a payment block.
  // These roster pages duplicate claim-level totals already captured in the
  // per-CPT detail pages above them. We must not double-count them.
  //
  // STEP A — Raw medical_service pool (identical to the old single-line filter)
  const rawMedicalItems = items.filter(i => i.line_type === 'medical_service' || i.line_type === null)

  // STEP B — Roster rows (populated ONLY after eob-worker is reprocessed with the
  // roster_summary prompt; empty for all legacy data extracted before this change)
  const rosterItems = items.filter(i => i.line_type === 'roster_summary')

  // STEP C — Pre-reprocess safety-net dedup ──────────────────────────────────
  // Before the file is reprocessed with the new prompt, roster pages are still in
  // rawMedicalItems as "thin" rows: claim_number + paid_amount, but no CPT code
  // and no date_of_service. Drop any thin row when a richer row already exists
  // for the same (eob_document_id, claim_number) pair within the document.
  // "Rich" = has a real CPT code (not "SUMMARY") AND a date_of_service.
  // "Thin" = missing CPT code or date_of_service (the summary-roster profile).
  //
  // This safely no-ops on claims that genuinely lack DOS (unusual but possible)
  // because those claims will have no matching rich row and pass through unchanged.
  const claimsWithDetail = new Set<string>()
  for (const item of rawMedicalItems) {
    if (!item.claim_number || !item.eob_document_id) continue
    const isRich = item.cpt_code && item.cpt_code !== 'SUMMARY' && item.date_of_service
    if (isRich) claimsWithDetail.add(`${item.eob_document_id}||${item.claim_number}`)
  }

  const dedupedMedicalItems = rawMedicalItems.filter(item => {
    if (!item.claim_number || !item.eob_document_id) return true  // no key → can't dedup, keep
    const key = `${item.eob_document_id}||${item.claim_number}`
    if (!claimsWithDetail.has(key)) return true  // no rich row exists → keep this row
    // A rich row exists for this claim. Keep ONLY if this row is also rich.
    const isRich = item.cpt_code && item.cpt_code !== 'SUMMARY' && item.date_of_service
    return !!isRich
  })

  // STEP D — Post-reprocess roster_summary cross-reference ───────────────────
  // Once the document is reprocessed with the new prompt, roster pages produce
  // roster_summary rows instead of medical_service rows. Cross-reference them:
  //   • Roster row matches a detail row   → verify amounts, DROP roster row
  //   • Roster row has NO matching detail → promote to medical_service (backfill)
  //
  // Pre-build a sum-of-paid-amounts map per (doc + claim_number) for variance checks.
  // A single claim can have multiple CPT lines; the roster total = sum of all lines.
  const detailPaidSumByKey = new Map<string, number>()
  for (const item of dedupedMedicalItems) {
    if (!item.claim_number || !item.eob_document_id) continue
    const key = `${item.eob_document_id}||${item.claim_number}`
    detailPaidSumByKey.set(key, (detailPaidSumByKey.get(key) ?? 0) + (item.paid_amount ?? 0))
  }

  const backfillItems: LineItem[] = []
  for (const roster of rosterItems) {
    if (!roster.claim_number || !roster.eob_document_id) continue
    const key = `${roster.eob_document_id}||${roster.claim_number}`
    const detailSum = detailPaidSumByKey.get(key)
    if (detailSum !== undefined) {
      // Detail row exists — validate amounts and drop roster row
      const rosterPaid = roster.paid_amount ?? 0
      const variance = Math.abs(detailSum - rosterPaid)
      if (variance > 0.01) {
        console.warn(
          `[Reports Reconcile] Amount variance on claim# "${roster.claim_number}": ` +
          `detail total=${fmtDec(detailSum)} vs roster=${fmtDec(rosterPaid)} ` +
          `(Δ${fmtDec(variance)}) | doc=${roster.eob_document_id}`
        )
      }
      // Drop the roster row — detail is the authoritative source
    } else {
      // No detail row found — backfill this claim to plug the revenue gap
      console.warn(
        `[Reports Reconcile] Backfilling orphan claim# "${roster.claim_number}" ` +
        `from roster_summary (no detail row found) | paid=${fmtDec(roster.paid_amount ?? 0)} ` +
        `| doc=${roster.eob_document_id}`
      )
      backfillItems.push({ ...roster, line_type: 'medical_service' })
    }
  }

  const medicalItems = [...dedupedMedicalItems, ...backfillItems]

  const totalBilled      = medicalItems.reduce((s, i) => s + (i.billed_amount ?? 0), 0)
  const totalPaid        = medicalItems.reduce((s, i) => s + (i.paid_amount   ?? 0), 0)
  const totalAdjustment  = medicalItems.reduce((s, i) =>
    s + (i.contractual_adjustment ?? i.adjustment_amount ?? 0), 0)
  const totalClaims      = medicalItems.length
  const deniedCount      = medicalItems.filter(i =>
    (i.claim_status ?? '').toLowerCase().includes('denied') ||
    (i.claim_status ?? '').toLowerCase().includes('rejected')
  ).length

  // Payer breakdown
  const payerMap = new Map<string, PayerRow>()
  for (const i of medicalItems) {
    const payer = i.payer_name || '(Unknown Payer)'
    const row = payerMap.get(payer) ?? { payer, claims: 0, billed: 0, paid: 0, denied: 0, adjustment: 0 }
    row.claims++
    row.billed     += i.billed_amount ?? 0
    row.paid       += i.paid_amount   ?? 0
    row.adjustment += i.contractual_adjustment ?? i.adjustment_amount ?? 0
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

  // Denial management — group denied items by remark code
  const deniedItems = medicalItems.filter(i =>
    (i.claim_status ?? '').toLowerCase().includes('denied') ||
    (i.claim_status ?? '').toLowerCase().includes('rejected')
  )
  const denialMap = new Map<string, DenialRow>()
  for (const i of deniedItems) {
    const code = i.remark_code || '(No Code)'
    const description = i.remark_description || i.remark_reason || '—'
    const key = code
    const row = denialMap.get(key) ?? { code, description, count: 0, billed: 0 }
    row.count++
    row.billed += i.billed_amount ?? 0
    // Update description if we have a better one
    if (description !== '—' && row.description === '—') row.description = description
    denialMap.set(key, row)
  }
  const denialRows = Array.from(denialMap.values()).sort((a, b) => b.billed - a.billed)

  // Denial by payer — group denied items by payer for Table B
  const denialByPayerMap = new Map<string, DenialByPayerRow>()
  for (const i of deniedItems) {
    const payer = normalizePayer(i.payer_name)
    const row = denialByPayerMap.get(payer) ?? { payer, count: 0, billed: 0 }
    row.count++
    row.billed += i.billed_amount ?? 0
    denialByPayerMap.set(payer, row)
  }
  const denialByPayerRows = Array.from(denialByPayerMap.values()).sort((a, b) => b.billed - a.billed)

  // Deposit summary — from summary_total rows (check/EFT cover pages captured by eob-worker).
  //
  // Cross-page extraction produces several inflation/dedup artifacts that must be collapsed:
  //   1. Per-page / per-section subtotals: a large EOB prints a running subtotal on every page
  //      under the same check number. Fix: per unique check number keep only the MAX amount.
  //   2. Payer-name spelling variants produce duplicate rows for the same check. Fix: dedup by
  //      normalized check number alone (not payer+checkNum).
  //   3. Dual-numbered payer payments (e.g. Mercy Care/Aetna) show a primary check row (with
  //      payment_date) and an EFT-trace reference row (no date). Fix: drop null-date rows.
  //   4. "CHK-XXXXXXXX" vs "XXXXXXXX" prefix variants: Gemini sometimes prefixes check numbers
  //      with "CHK-". Strip it before using as a dedup key so both forms collapse to one entry.
  //   5. Cross-document check collisions: the same check number can appear in multiple documents
  //      (different PDFs that each contain a cover-page reference to the same remittance).
  //      The global deposit table dedups to one entry per physical check (correct for totals).
  //      The per-document gap analysis uses a SEPARATE per-document dedup so each document's
  //      deposit credit is computed from the checks found *within* that document, not stolen
  //      by another document that happens to share the same check number.

  // Helper: strip "CHK-" prefix (case-insensitive) before using as a dedup key.
  const normalizeCheckNum = (s: string | null | undefined): string =>
    s ? s.trim().replace(/^CHK-/i, '') : ''

  const rawDepositItems = items.filter(i => i.line_type === 'summary_total' && (i.paid_amount ?? 0) > 0)

  // Step 1 — drop null-date rows (EFT trace references, cover-page stubs without a date).
  const datedDepositItems = rawDepositItems.filter(i => i.payment_date != null)

  // Step 2a — GLOBAL dedup by normalized check number.
  // One physical check = one entry in the deposit table. When two rows share a normalized
  // check number, keep the one with the highest amount; tie-break on non-null payer name.
  const globalCheckMap = new Map<string, LineItem>()
  for (const i of datedDepositItems) {
    const checkNum = normalizeCheckNum(i.source_check_number || i.remark_code)
    if (!checkNum) continue  // no check number — handled below in nullCheckItems
    const existing = globalCheckMap.get(checkNum)
    if (!existing) {
      globalCheckMap.set(checkNum, i)
    } else {
      const iAmt = i.paid_amount ?? 0
      const eAmt = existing.paid_amount ?? 0
      const iHasPayer = i.payer_name != null && i.payer_name.trim() !== ''
      const eHasPayer = existing.payer_name != null && existing.payer_name.trim() !== ''
      if (iAmt > eAmt || (iAmt === eAmt && iHasPayer && !eHasPayer)) {
        globalCheckMap.set(checkNum, i)
      }
    }
  }

  // OCR fuzzy collapse — global pass ──────────────────────────────────────────
  // After the standard exact dedup, scan all pairs in globalCheckMap for OCR
  // misreads: two entries from the *same* eob_document that are all-digit strings
  // of similar length with Levenshtein distance ≤ 2.  The lower-amount entry is
  // the phantom; collapse it into the higher-amount (authoritative) entry.
  //
  // Guards applied before measuring distance:
  //   • Same eob_document_id — never collapse checks from different source files
  //   • Both strings purely numeric — avoids fuzzy-matching "EFT-7901273" etc.
  //   • Length difference ≤ 2 — avoids matching short codes vs. long check numbers
  //   • Levenshtein ≤ 2 — catches single-digit OCR misreads (e.g. 4→1)
  {
    const globalKeys = Array.from(globalCheckMap.keys())
    for (let gi = 0; gi < globalKeys.length; gi++) {
      for (let gj = gi + 1; gj < globalKeys.length; gj++) {
        const keyA = globalKeys[gi], keyB = globalKeys[gj]
        const itemA = globalCheckMap.get(keyA)
        const itemB = globalCheckMap.get(keyB)
        if (!itemA || !itemB) continue                                 // already collapsed
        if (itemA.eob_document_id !== itemB.eob_document_id) continue  // different docs
        // Sanitize before numeric test: OCR sometimes injects non-digit chars
        // (e.g. "?" for a misread digit, as in "00025888?2" → "000258882").
        // Strip them so the purely-numeric guard and Levenshtein distance both
        // operate on the clean digit string rather than rejecting the entry.
        const keyAClean = keyA.replace(/[^a-zA-Z0-9]/g, '')
        const keyBClean = keyB.replace(/[^a-zA-Z0-9]/g, '')
        if (!/^\d+$/.test(keyAClean) || !/^\d+$/.test(keyBClean)) continue   // non-numeric
        if (Math.abs(keyAClean.length - keyBClean.length) > 2) continue       // length mismatch
        const dist = levenshtein(keyAClean, keyBClean)
        if (dist > 2) continue
        // Collapse: drop the lower-amount entry as the OCR phantom
        const amtA = itemA.paid_amount ?? 0
        const amtB = itemB.paid_amount ?? 0
        const [keepKey, dropKey] = amtA >= amtB ? [keyA, keyB] : [keyB, keyA]
        const keepAmt = Math.max(amtA, amtB)
        const dropAmt = Math.min(amtA, amtB)
        console.warn(
          `[Reports Dedup] Collapsed OCR-variant check# "${dropKey}" (${fmtDec(dropAmt)})` +
          ` → canonical "${keepKey}" (${fmtDec(keepAmt)})` +
          ` | doc=${itemA.eob_document_id} | Levenshtein dist=${dist}`
        )
        globalCheckMap.delete(dropKey)
      }
    }
  }

  // Step 2b — PER-DOCUMENT dedup by (file_name + normalized check number).
  // Used exclusively for the per-document gap analysis below.
  // Each document independently keeps the MAX amount for each check number it references,
  // so a document's deposit credit is never stolen by another document that shares the
  // same check number string.
  const perDocCheckMap = new Map<string, LineItem>()
  for (const i of datedDepositItems) {
    const checkNum = normalizeCheckNum(i.source_check_number || i.remark_code)
    const doc = i.file_name || ''
    if (!checkNum) continue
    const key = `${doc}||${checkNum}`
    const existing = perDocCheckMap.get(key)
    if (!existing || (i.paid_amount ?? 0) > (existing.paid_amount ?? 0)) {
      perDocCheckMap.set(key, i)
    }
  }

  // OCR fuzzy collapse — per-document pass ─────────────────────────────────────
  // Mirror of the global pass above, applied to perDocCheckMap so the gap analysis
  // also collapses phantom deposits.  Keys here are "fileName||checkNum"; split on
  // the last "||" to isolate checkNum and only compare within the same file.
  // (The global pass already emitted the console.warn; no duplicate log here.)
  {
    const pdKeys = Array.from(perDocCheckMap.keys())
    for (let pi = 0; pi < pdKeys.length; pi++) {
      for (let pj = pi + 1; pj < pdKeys.length; pj++) {
        const keyA = pdKeys[pi], keyB = pdKeys[pj]
        const sepA = keyA.lastIndexOf('||'), sepB = keyB.lastIndexOf('||')
        if (sepA < 0 || sepB < 0) continue
        const fileA = keyA.slice(0, sepA), checkA = keyA.slice(sepA + 2)
        const fileB = keyB.slice(0, sepB), checkB = keyB.slice(sepB + 2)
        if (fileA !== fileB) continue                                   // different docs
        const itemA = perDocCheckMap.get(keyA)
        const itemB = perDocCheckMap.get(keyB)
        if (!itemA || !itemB) continue                                  // already collapsed
        // Same OCR sanitization as the global pass above — strip non-alphanumeric
        // chars before the numeric guard so "00025888?2" collapses with "0002588872".
        const checkAClean = checkA.replace(/[^a-zA-Z0-9]/g, '')
        const checkBClean = checkB.replace(/[^a-zA-Z0-9]/g, '')
        if (!/^\d+$/.test(checkAClean) || !/^\d+$/.test(checkBClean)) continue   // non-numeric
        if (Math.abs(checkAClean.length - checkBClean.length) > 2) continue       // length mismatch
        const dist = levenshtein(checkAClean, checkBClean)
        if (dist > 2) continue
        const amtA = itemA.paid_amount ?? 0
        const amtB = itemB.paid_amount ?? 0
        const dropKey = amtA >= amtB ? keyB : keyA
        perDocCheckMap.delete(dropKey)
      }
    }
  }

  // Null-check# rows that have a date (rare; can't dedup without a key — keep all)
  const nullCheckItems = datedDepositItems.filter(i => !normalizeCheckNum(i.source_check_number || i.remark_code))

  const depositItems = [...globalCheckMap.values(), ...nullCheckItems]
  const depositMap = new Map<string, DepositRow>()
  for (const i of depositItems) {
    const checkNum = normalizeCheckNum(i.source_check_number || i.remark_code) || '(Unknown)'
    const payer = normalizePayer(i.payer_name)
    const key = `${payer}||${checkNum}`
    const row = depositMap.get(key)
    if (!row) {
      depositMap.set(key, {
        payer,
        check_number: checkNum,
        payment_date: i.payment_date,
        amount: i.paid_amount ?? 0,
        source_doc: i.file_name || '',
      })
    } else {
      // After globalCheckMap dedup, same payer+checkNum collisions are rare; keep the larger amount
      row.amount = Math.max(row.amount, i.paid_amount ?? 0)
      if (!row.payment_date && i.payment_date) row.payment_date = i.payment_date
      if (!row.source_doc && i.file_name) row.source_doc = i.file_name
    }
  }
  const isUnknownDeposit = (r: DepositRow) =>
    r.payer === '(Unknown Payer)' || r.check_number === '(Unknown)'
  const depositRows = Array.from(depositMap.values()).sort((a, b) => {
    const aU = isUnknownDeposit(a), bU = isUnknownDeposit(b)
    if (aU !== bU) return aU ? -1 : 1
    return a.payer.localeCompare(b.payer)
  })
  const depositTotal = depositRows.reduce((s, r) => s + r.amount, 0)

  // Build file_name → eob_document_id lookup so the gap table can wire up Reprocess.
  // Use the first eob_document_id seen for each file_name (all rows for a given
  // document share the same eob_document_id).
  const fileNameToDocId = new Map<string, string>()
  for (const item of items) {
    if (item.file_name && item.eob_document_id && !fileNameToDocId.has(item.file_name)) {
      fileNameToDocId.set(item.file_name, item.eob_document_id)
    }
  }

  // Reverse lookup: eob_document_id → file_name (needed for check-level gap sourceDoc)
  const docIdToFileName = new Map<string, string>()
  for (const item of items) {
    if (item.eob_document_id && item.file_name && !docIdToFileName.has(item.eob_document_id)) {
      docIdToFileName.set(item.eob_document_id, item.file_name)
    }
  }

  // Per-document gap: for each source document that has a summary_total deposit,
  // compare the check total to the sum of extracted medical_service claim lines.
  // A gap means that document has checks/EFTs that weren't fully reconciled to
  // individual claim lines — reprocessing the document closes the gap.
  //
  // IMPORTANT: uses perDocCheckMap (not depositRows / globalCheckMap) so that each
  // document's deposit credit is based on the checks found within *that document*,
  // not stolen by another doc that shares the same check number in the global dedup.
  const docGapRows: DocGapRow[] = (() => {
    const depositByDoc = new Map<string, number>()
    for (const i of perDocCheckMap.values()) {
      const doc = i.file_name || '(unknown)'
      depositByDoc.set(doc, (depositByDoc.get(doc) || 0) + (i.paid_amount ?? 0))
    }
    // Also credit null-check# items to their source document
    for (const i of nullCheckItems) {
      const doc = i.file_name || '(unknown)'
      depositByDoc.set(doc, (depositByDoc.get(doc) || 0) + (i.paid_amount ?? 0))
    }
    const paidByDoc = new Map<string, number>()
    for (const item of medicalItems) {
      const doc = item.file_name || '(unknown)'
      paidByDoc.set(doc, (paidByDoc.get(doc) || 0) + (item.paid_amount || 0))
    }
    return Array.from(depositByDoc.entries())
      .map(([sourceDoc, depositAmount]) => ({
        sourceDoc,
        docId: fileNameToDocId.get(sourceDoc) || null,
        depositAmount,
        extractedPaid: paidByDoc.get(sourceDoc) || 0,
        gap: depositAmount - (paidByDoc.get(sourceDoc) || 0),
      }))
      .filter(r => Math.abs(r.gap) > 1)
      .sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap))
  })()

  // Check-level gap: for each eob_payment row, compare its check_amount to the sum of
  // extracted claim lines that carry matching eob_payment_id (hierarchical ingestion path).
  // Documents that were ingested before the hierarchical model produce no eob_payments rows
  // and fall through to legacyDocGapRows below.
  const checkGapRows: CheckGapRow[] = (() => {
    const paidByPaymentId = new Map<string, number>()
    for (const item of medicalItems) {
      if (!item.eob_payment_id) continue
      paidByPaymentId.set(
        item.eob_payment_id,
        (paidByPaymentId.get(item.eob_payment_id) ?? 0) + (item.paid_amount ?? 0)
      )
    }
    return eobPayments
      .filter(p => (p.check_amount ?? 0) > 0)
      .map(p => {
        const extractedPaid = paidByPaymentId.get(p.id) ?? 0
        const checkAmount = p.check_amount ?? 0
        return {
          checkNumber: p.check_number ?? '(Unknown)',
          paymentDate: p.payment_date,
          payerName: p.payer_name,
          checkAmount,
          extractedPaid,
          gap: checkAmount - extractedPaid,
          sourceDoc: docIdToFileName.get(p.eob_document_id) ?? '(unknown)',
          docId: p.eob_document_id,
        }
      })
      .filter(r => Math.abs(r.gap) > 1)
      .sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap))
  })()

  // Legacy fallback: document-level gap for docs that predate the hierarchical model
  const docsWithCheckCoverage = new Set(eobPayments.map(p => p.eob_document_id))
  const legacyDocGapRows = docGapRows.filter(
    r => !r.docId || !docsWithCheckCoverage.has(r.docId)
  )

  // Filtered + paginated items for the table (exclude summary_total rows — those appear in Deposit Summary;
  // exclude roster_summary rows — aggregate roster pages are cross-referenced into medicalItems above)
  const serviceItems = items.filter(i => i.line_type !== 'summary_total' && i.line_type !== 'roster_summary')
  const filtered = serviceItems.filter(i => {
    if (!search) return true
    const q = search.toLowerCase()
    return (
      (i.patient_name    ?? '').toLowerCase().includes(q) ||
      (i.payer_name      ?? '').toLowerCase().includes(q) ||
      (i.cpt_code        ?? '').toLowerCase().includes(q) ||
      (i.file_name       ?? '').toLowerCase().includes(q) ||
      (i.claim_status    ?? '').toLowerCase().includes(q) ||
      (i.remark_code     ?? '').toLowerCase().includes(q) ||
      (i.claim_number    ?? '').toLowerCase().includes(q)
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

  async function handleReprocess(docId: string) {
    setReprocessingDocs(prev => new Set(prev).add(docId))
    setReprocessError(null)
    try {
      const { error } = await supabase.functions.invoke('reprocess-document', {
        body: { eob_document_id: docId },
      })
      if (error) {
        let msg = error.message
        try {
          const errorBody = await (error as any).context?.json?.()
          if (errorBody?.error) {
            msg = errorBody.error
            if (errorBody?.details) msg += `: ${errorBody.details}`
          }
        } catch { /* ignore parse errors */ }
        setReprocessError(msg || 'Failed to reprocess document')
      } else {
        // Mark as queued — the gap will close once the background job finishes.
        // Increment refreshKey so the useEffect re-fetches line items.
        setReprocessedDocs(prev => new Set(prev).add(docId))
        setRefreshKey(k => k + 1)
      }
    } catch (err: any) {
      setReprocessError(err.message || 'Unexpected error during reprocess')
    } finally {
      setReprocessingDocs(prev => {
        const next = new Set(prev)
        next.delete(docId)
        return next
      })
    }
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
              <p className="mt-1 text-2xl font-bold text-gray-900">{fmtDec(totalBilled)}</p>
              <p className="mt-1 text-xs text-gray-400">{totalClaims.toLocaleString()} claim lines</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Total Paid</p>
              <p className="mt-1 text-2xl font-bold text-green-700">{fmtDec(totalPaid)}</p>
              <p className="mt-1 text-xs text-gray-400">Collection rate: {pct(totalPaid, totalBilled)}</p>
              <p className="mt-1 text-xs text-gray-400 italic">Extracted claim lines only — see Deposit Summary for check totals</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Denial Rate</p>
              <p className={`mt-1 text-2xl font-bold ${deniedCount / Math.max(totalClaims, 1) > 0.05 ? 'text-red-600' : 'text-gray-900'}`}>
                {pct(deniedCount, totalClaims)}
              </p>
              <p className="mt-1 text-xs text-gray-400">{deniedCount} denied of {totalClaims}</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">Contractual Adj.</p>
              <p className="mt-1 text-2xl font-bold text-orange-600">
                {totalAdjustment > 0 ? fmtDec(totalAdjustment) : '—'}
              </p>
              <p className="mt-1 text-xs text-gray-400">write-downs where captured</p>
            </div>
          </div>

          {/* ── Check-Level Reconciliation Gap (hierarchical ingestion path) ──
              Shown when eob_payments rows exist for the practice. Each row is one
              check/EFT whose extracted claim total doesn't match the check amount.
              Provides check-level precision vs. the legacy document-level fallback.
          ──────────────────────────────────────────────────────────────────── */}
          {checkGapRows.length > 0 && (
            <div className="mt-6 rounded-xl border border-amber-200 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-amber-200 px-5 py-4 bg-amber-50 flex items-start justify-between gap-4">
                <div>
                  <h2 className="text-sm font-semibold text-amber-900 flex items-center gap-1.5">
                    <svg className="h-4 w-4 text-amber-600 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                    </svg>
                    Check-Level Reconciliation Gap — {fmtDec(checkGapRows.reduce((s, r) => s + Math.abs(r.gap), 0))}
                  </h2>
                  <p className="mt-0.5 text-xs text-amber-700">
                    {checkGapRows.length} check{checkGapRows.length !== 1 ? 's' : ''} where the remittance check amount
                    doesn&apos;t match the sum of extracted claim lines linked to that check.
                    Reprocess the source document to close the gap.
                  </p>
                </div>
                <span className="shrink-0 inline-flex items-center rounded-full bg-amber-100 border border-amber-200 px-2.5 py-0.5 text-xs font-semibold text-amber-800">
                  {checkGapRows.length} check{checkGapRows.length !== 1 ? 's' : ''}
                </span>
              </div>

              {reprocessError && (
                <div className="border-b border-red-100 bg-red-50 px-5 py-2.5 text-xs text-red-700 flex items-center gap-2">
                  <svg className="h-3.5 w-3.5 shrink-0 text-red-500" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9.303 3.376c.866 1.5-.217 3.374-1.948 3.374H4.645c-1.73 0-2.813-1.874-1.948-3.374L10.051 3.378c.866-1.5 3.032-1.5 3.898 0l7.354 12.748zM12 15.75h.007v.008H12v-.008z" />
                  </svg>
                  {reprocessError}
                  <button onClick={() => setReprocessError(null)} className="ml-auto text-red-400 hover:text-red-600">✕</button>
                </div>
              )}

              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left bg-gray-50 border-b border-gray-100">
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500">Check / EFT #</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500">Check Date</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500">Source Document</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Check Amount</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Extracted Paid</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-amber-600 text-right">Gap</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {checkGapRows.map((row, idx) => {
                      const isSubmitting   = reprocessingDocs.has(row.docId)
                      const wasJustQueued  = reprocessedDocs.has(row.docId)
                      const dbStatus       = docStatuses.get(row.docId) ?? ''
                      const isQueuedDB     = dbStatus === 'queued'
                      const isProcessingDB = dbStatus === 'processing'
                      return (
                        <tr key={idx} className="hover:bg-amber-50/40">
                          <td className="px-5 py-2.5 font-mono text-blue-700">{row.checkNumber}</td>
                          <td className="px-5 py-2.5 text-gray-600 max-w-[160px] truncate" title={row.payerName ?? ''}>
                            {row.payerName || <span className="text-gray-300">—</span>}
                          </td>
                          <td className="px-5 py-2.5 text-gray-500 whitespace-nowrap">
                            {fmtDate(row.paymentDate) || <span className="text-gray-300">—</span>}
                          </td>
                          <td className="px-5 py-2.5 text-gray-400 max-w-[200px] truncate" title={row.sourceDoc}>
                            {row.sourceDoc || '(unknown)'}
                          </td>
                          <td className="px-5 py-2.5 text-right text-gray-600">{fmtDec(row.checkAmount)}</td>
                          <td className="px-5 py-2.5 text-right text-gray-600">{fmtDec(row.extractedPaid)}</td>
                          <td className="px-5 py-2.5 text-right font-semibold text-amber-700">{fmtDec(row.gap)}</td>
                          <td className="px-5 py-2.5 text-right">
                            {isSubmitting ? (
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-gray-500">
                                <svg className="h-3 w-3 animate-spin" viewBox="0 0 24 24" fill="none">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                                Submitting…
                              </span>
                            ) : isProcessingDB ? (
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-blue-700">
                                <svg className="h-3 w-3 animate-spin" viewBox="0 0 24 24" fill="none">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                                Processing…
                              </span>
                            ) : dbStatus === 'completed' || dbStatus === 'needs_review' ? (
                              <div className="inline-flex flex-col items-end gap-1">
                                <span className="inline-flex items-center gap-1 text-xs font-medium text-green-700">
                                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                                  </svg>
                                  Extracted
                                </span>
                                <button
                                  onClick={() => handleReprocess(row.docId)}
                                  className="text-[10px] text-gray-400 hover:text-amber-700 underline underline-offset-2"
                                  title="Re-extract this document again"
                                >
                                  re-run
                                </button>
                              </div>
                            ) : dbStatus === 'failed' ? (
                              <button
                                onClick={() => handleReprocess(row.docId)}
                                className="inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium bg-white border border-red-300 text-red-700 hover:bg-red-50 shadow-sm transition-colors"
                                title="Processing failed — click to retry"
                              >
                                ⚠ Failed — Retry
                              </button>
                            ) : isQueuedDB || wasJustQueued ? (
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-amber-700">
                                <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
                                </svg>
                                Queued
                              </span>
                            ) : (
                              <button
                                onClick={() => handleReprocess(row.docId)}
                                className="inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium bg-white border border-amber-300 text-amber-800 hover:bg-amber-50 shadow-sm transition-colors"
                                title="Re-extract this document to close the gap"
                              >
                                ↺ Reprocess
                              </button>
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

          {/* ── Reconciliation Gap Analysis (legacy — document level) ────────
              Shown for documents that predate the hierarchical ingestion model.
              Each row identifies a source document where the remittance cover-page
              check total doesn't match the sum of extracted claim lines, and
              exposes a one-click Reprocess action to re-extract and close the gap.
          ──────────────────────────────────────────────────────────── */}
          {legacyDocGapRows.length > 0 && (
            <div className="mt-6 rounded-xl border border-amber-200 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-amber-200 px-5 py-4 bg-amber-50 flex items-start justify-between gap-4">
                <div>
                  <h2 className="text-sm font-semibold text-amber-900 flex items-center gap-1.5">
                    <svg className="h-4 w-4 text-amber-600 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                    </svg>
                    Reconciliation Gap — {fmtDec(legacyDocGapRows.reduce((s, r) => s + Math.abs(r.gap), 0))}
                  </h2>
                  <p className="mt-0.5 text-xs text-amber-700">
                    {legacyDocGapRows.length} document{legacyDocGapRows.length !== 1 ? 's' : ''} where the remittance check total
                    doesn&apos;t match the sum of extracted claim lines. Reprocess to close the gap, or investigate
                    if the document has no individual claim lines (e.g. a check-only stub with no EOB pages).
                  </p>
                </div>
                <span className="shrink-0 inline-flex items-center rounded-full bg-amber-100 border border-amber-200 px-2.5 py-0.5 text-xs font-semibold text-amber-800">
                  {legacyDocGapRows.length} file{legacyDocGapRows.length !== 1 ? 's' : ''}
                </span>
              </div>

              {reprocessError && (
                <div className="border-b border-red-100 bg-red-50 px-5 py-2.5 text-xs text-red-700 flex items-center gap-2">
                  <svg className="h-3.5 w-3.5 shrink-0 text-red-500" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9.303 3.376c.866 1.5-.217 3.374-1.948 3.374H4.645c-1.73 0-2.813-1.874-1.948-3.374L10.051 3.378c.866-1.5 3.032-1.5 3.898 0l7.354 12.748zM12 15.75h.007v.008H12v-.008z" />
                  </svg>
                  {reprocessError}
                  <button onClick={() => setReprocessError(null)} className="ml-auto text-red-400 hover:text-red-600">✕</button>
                </div>
              )}

              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left bg-gray-50 border-b border-gray-100">
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500">Source Document</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Deposit Total</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Extracted Paid</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-amber-600 text-right">Gap</th>
                      <th className="px-5 py-2.5 text-xs font-medium text-gray-500 text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {legacyDocGapRows.map((row, idx) => {
                      // isSubmitting — API call is in-flight (instant local feedback)
                      const isSubmitting   = row.docId ? reprocessingDocs.has(row.docId) : false
                      // wasJustQueued — API returned success but DB status not yet polled
                      const wasJustQueued  = row.docId ? reprocessedDocs.has(row.docId) : false
                      // Live DB status — authoritative once the first poll fires
                      const dbStatus       = row.docId ? (docStatuses.get(row.docId) ?? '') : ''
                      const isQueuedDB     = dbStatus === 'queued'
                      const isProcessingDB = dbStatus === 'processing'
                      return (
                        <tr key={idx} className="hover:bg-amber-50/40">
                          <td className="px-5 py-2.5 text-gray-800 max-w-[320px] truncate" title={row.sourceDoc}>
                            {row.sourceDoc || '(unknown)'}
                          </td>
                          <td className="px-5 py-2.5 text-right text-gray-600">{fmtDec(row.depositAmount)}</td>
                          <td className="px-5 py-2.5 text-right text-gray-600">{fmtDec(row.extractedPaid)}</td>
                          <td className="px-5 py-2.5 text-right font-semibold text-amber-700">{fmtDec(row.gap)}</td>
                          <td className="px-5 py-2.5 text-right">
                            {!row.docId ? (
                              <span className="text-gray-300 text-xs">—</span>
                            ) : isSubmitting ? (
                              // API call in-flight — instant spinner before DB confirms
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-gray-500">
                                <svg className="h-3 w-3 animate-spin" viewBox="0 0 24 24" fill="none">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                                Submitting…
                              </span>
                            ) : isProcessingDB ? (
                              // DB confirmed actively processing
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-blue-700">
                                <svg className="h-3 w-3 animate-spin" viewBox="0 0 24 24" fill="none">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                                Processing…
                              </span>
                            ) : dbStatus === 'completed' || dbStatus === 'needs_review' ? (
                              // DB confirmed extraction finished — authoritative, beats wasJustQueued
                              <div className="inline-flex flex-col items-end gap-1">
                                <span className="inline-flex items-center gap-1 text-xs font-medium text-green-700">
                                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                                  </svg>
                                  Extracted
                                </span>
                                <button
                                  onClick={() => handleReprocess(row.docId!)}
                                  className="text-[10px] text-gray-400 hover:text-amber-700 underline underline-offset-2"
                                  title="Re-extract this document again"
                                >
                                  re-run
                                </button>
                              </div>
                            ) : dbStatus === 'failed' ? (
                              // DB confirmed failed — authoritative, beats wasJustQueued
                              <button
                                onClick={() => handleReprocess(row.docId!)}
                                className="inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium bg-white border border-red-300 text-red-700 hover:bg-red-50 shadow-sm transition-colors"
                                title="Processing failed — click to retry"
                              >
                                ⚠ Failed — Retry
                              </button>
                            ) : isQueuedDB || wasJustQueued ? (
                              // DB confirmed queued (or just submitted locally before first poll)
                              <span className="inline-flex items-center gap-1.5 text-xs font-medium text-amber-700">
                                <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
                                </svg>
                                Queued
                              </span>
                            ) : (
                              // No status yet — offer initial reprocess
                              <button
                                onClick={() => handleReprocess(row.docId!)}
                                className="inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium bg-white border border-amber-300 text-amber-800 hover:bg-amber-50 shadow-sm transition-colors"
                                title="Re-extract this document to close the gap"
                              >
                                ↺ Reprocess
                              </button>
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

          {/* Deposit Summary */}
          {depositRows.length > 0 && (
            <div className="mt-6 rounded-xl border border-blue-100 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-blue-100 px-5 py-4 bg-blue-50 flex items-center justify-between">
                <div>
                  <h2 className="text-sm font-semibold text-blue-900">Deposit Summary</h2>
                  <p className="mt-0.5 text-xs text-blue-600">
                    {depositRows.length} check{depositRows.length !== 1 ? 's' : ''} / EFT{depositRows.length !== 1 ? 's' : ''} · Total expected deposit: {fmtDec(depositTotal)}
                  </p>
                  {/* Explain discrepancy between check totals and claim-line totals */}
                  <p className="mt-1 text-xs text-blue-500">
                    Check/EFT totals are read from the remittance cover page and represent the full deposit amount.
                    The <strong>Total Paid</strong> KPI above sums individual extracted claim lines — these differ when
                    not all EOB pages were processed or some claims weren&apos;t captured as individual lines.
                  </p>
                  {Math.abs(depositTotal - totalPaid) > 1 && (
                    <p className="mt-1 text-xs text-amber-700 font-medium">
                      ⚠ Gap of {fmtDec(Math.abs(depositTotal - totalPaid))} — see Reconciliation Gap Analysis above.
                    </p>
                  )}
                </div>
                <svg className="h-5 w-5 text-blue-400" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18.75a60.07 60.07 0 0115.797 2.101c.727.198 1.453-.342 1.453-1.096V18.75M3.75 4.5v.75A.75.75 0 013 6h-.75m0 0v-.375c0-.621.504-1.125 1.125-1.125H20.25M2.25 6v9m18-10.5v.75c0 .414.336.75.75.75h.75m-1.5-1.5h.375c.621 0 1.125.504 1.125 1.125v9.75c0 .621-.504 1.125-1.125 1.125h-.375m1.5-1.5H21a.75.75 0 00-.75.75v.75m0 0H3.75m0 0h-.375a1.125 1.125 0 01-1.125-1.125V15m1.5 1.5v-.75A.75.75 0 003 15h-.75M15 10.5a3 3 0 11-6 0 3 3 0 016 0zm3 0h.008v.008H18V10.5zm-12 0h.008v.008H6V10.5z" />
                </svg>
              </div>
              <div className="max-h-[480px] overflow-y-auto overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 z-10">
                    <tr className="bg-gray-50 text-left border-b border-gray-100">
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Check / EFT #</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Check/EFT Date</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Source Document</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Amount</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {depositRows.map((row, idx) => {
                      const isUnknown = isUnknownDeposit(row)
                      return (
                        <tr key={idx} className={isUnknown ? 'bg-amber-50 hover:bg-amber-100' : 'hover:bg-gray-50'}>
                          <td className="px-4 py-2.5 max-w-[180px] truncate" title={row.payer}>
                            {isUnknown ? (
                              <span className="flex items-center gap-1.5 text-amber-800 font-medium">
                                <svg className="h-3.5 w-3.5 text-amber-500 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                                </svg>
                                {row.payer}
                              </span>
                            ) : (
                              <span className="text-gray-800">{row.payer}</span>
                            )}
                          </td>
                          <td className={`px-4 py-2.5 font-mono text-xs ${isUnknown ? 'text-amber-700' : 'text-blue-700'}`}>
                            {row.check_number}
                          </td>
                          <td className="px-4 py-2.5 text-gray-500 text-xs whitespace-nowrap">
                            {fmtDate(row.payment_date) || <span className="text-gray-300">—</span>}
                          </td>
                          <td className="px-4 py-2.5 text-xs text-gray-400 max-w-[200px] truncate" title={row.source_doc}>
                            {row.source_doc || <span className="text-gray-300">—</span>}
                          </td>
                          <td className="px-4 py-2.5 text-right font-semibold text-gray-900 whitespace-nowrap">
                            {fmtDec(row.amount)}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                  <tfoot>
                    <tr className="bg-blue-50 border-t border-blue-100">
                      <td className="px-4 py-2.5 text-xs font-semibold text-blue-900" colSpan={4}>
                        Total Expected Deposit
                      </td>
                      <td className="px-4 py-2.5 text-right text-sm font-bold text-blue-900 whitespace-nowrap">
                        {fmtDec(depositTotal)}
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            </div>
          )}

          {/* Payer Breakdown + Status Distribution */}
          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            {/* Payer Breakdown */}
            <div className="rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-gray-100 px-5 py-4">
                <h2 className="text-sm font-semibold text-gray-900">Payer Breakdown</h2>
                <p className="mt-0.5 text-xs text-gray-400">Adj. = contractual write-down where captured by EOB</p>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 text-left">
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Claims</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Billed</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Paid</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Adj.</th>
                      <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Denied</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {payerRows.map(row => (
                      <tr key={row.payer} className="hover:bg-gray-50">
                        <td className="px-4 py-2.5 text-gray-800 max-w-[140px] truncate" title={row.payer}>
                          {row.payer}
                        </td>
                        <td className="px-4 py-2.5 text-gray-600 text-right">{row.claims}</td>
                        <td className="px-4 py-2.5 text-gray-600 text-right">{fmt(row.billed)}</td>
                        <td className="px-4 py-2.5 text-gray-800 font-medium text-right">{fmt(row.paid)}</td>
                        <td className="px-4 py-2.5 text-orange-600 text-right">
                          {row.adjustment > 0 ? fmt(row.adjustment) : <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 text-right">
                          {row.denied > 0 ? (
                            <span className="text-red-600 font-medium">{row.denied}</span>
                          ) : (
                            <span className="text-gray-300">0</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="border-t border-gray-50 px-4 py-2.5 bg-gray-50 text-xs text-gray-400">
                Note: check/EFT numbers are not captured from paper EOBs — they appear only on the remittance cover page, not individual claim lines.
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

          {/* Denial Management */}
          {denialRows.length > 0 && (
            <div className="mt-6 rounded-xl border border-red-100 bg-white shadow-sm overflow-hidden">
              <div className="border-b border-red-100 px-5 py-4 bg-red-50">
                <h2 className="text-sm font-semibold text-red-900">Denial Management</h2>
                <p className="mt-0.5 text-xs text-red-600">
                  {deniedCount} denied claim lines · {pct(deniedCount, totalClaims)} denial rate ·{' '}
                  {fmt(deniedItems.reduce((s, i) => s + (i.billed_amount ?? 0), 0))} at risk
                </p>
              </div>
              <div className="grid lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100">
                {/* Table A — Top Denial Codes (Pareto by $ at risk) */}
                <div>
                  <div className="px-4 py-2.5 border-b border-gray-100 bg-gray-50/60">
                    <p className="text-xs font-semibold text-gray-700">
                      By Denial Code{' '}
                      <span className="font-normal text-gray-400">ranked by $ at risk</span>
                    </p>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-gray-50 text-left border-b border-gray-100">
                          <th className="px-4 py-2 text-xs font-medium text-gray-500">Code</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500">Description</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500 text-right">Count</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500 text-right">$ at Risk</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-50">
                        {denialRows.map(row => (
                          <tr key={row.code} className="hover:bg-gray-50">
                            <td className="px-4 py-2 font-mono text-xs text-gray-700 whitespace-nowrap">
                              {row.code}
                            </td>
                            <td className="px-4 py-2 text-gray-600 text-xs max-w-[200px] truncate" title={row.description}>
                              {row.description}
                            </td>
                            <td className="px-4 py-2 text-right">
                              <span className="inline-flex items-center justify-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-bold text-red-700">
                                {row.count}
                              </span>
                            </td>
                            <td className="px-4 py-2 text-red-600 font-medium text-right text-xs whitespace-nowrap">
                              {fmt(row.billed)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
                {/* Table B — Denials by Payer */}
                <div>
                  <div className="px-4 py-2.5 border-b border-gray-100 bg-gray-50/60">
                    <p className="text-xs font-semibold text-gray-700">
                      By Payer{' '}
                      <span className="font-normal text-gray-400">ranked by $ at risk</span>
                    </p>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-gray-50 text-left border-b border-gray-100">
                          <th className="px-4 py-2 text-xs font-medium text-gray-500">Payer</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500 text-right">Denials</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500 text-right">Share</th>
                          <th className="px-4 py-2 text-xs font-medium text-gray-500 text-right">$ at Risk</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-50">
                        {denialByPayerRows.map(row => (
                          <tr key={row.payer} className="hover:bg-gray-50">
                            <td className="px-4 py-2 text-gray-800 text-xs max-w-[160px] truncate" title={row.payer}>
                              {row.payer}
                            </td>
                            <td className="px-4 py-2 text-right">
                              <span className="inline-flex items-center justify-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-bold text-red-700">
                                {row.count}
                              </span>
                            </td>
                            <td className="px-4 py-2 text-gray-500 text-right text-xs">
                              {pct(row.count, deniedCount)}
                            </td>
                            <td className="px-4 py-2 text-red-600 font-medium text-right text-xs whitespace-nowrap">
                              {fmt(row.billed)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Line Items Table */}
          <div className="mt-6 rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
            <div className="flex items-center justify-between border-b border-gray-100 px-5 py-4">
              <h2 className="text-sm font-semibold text-gray-900">
                Line Items
                <span className="ml-2 text-xs font-normal text-gray-400">
                  {filtered.length.toLocaleString()} of {serviceItems.length.toLocaleString()}
                </span>
              </h2>
              <div className="relative w-72">
                <span className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
                  <svg className="h-4 w-4 text-slate-400" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-4.35-4.35m0 0A7.5 7.5 0 104.5 4.5a7.5 7.5 0 0012.15 12.15z" />
                  </svg>
                </span>
                <input
                  type="search"
                  placeholder="Search patient, payer, CPT, file…"
                  value={search}
                  onChange={e => { setSearch(e.target.value); setPage(0) }}
                  className="w-full rounded-lg border border-slate-300 bg-white pl-9 pr-3 py-1.5 text-sm text-gray-800 shadow-sm placeholder:text-slate-400 focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:shadow-md transition-shadow"
                />
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 text-left border-b border-gray-100">
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Patient</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Payer</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">DOS</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Paid Date</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">CPT</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Status</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Billed</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500 text-right">Paid</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Remark</th>
                    <th className="px-4 py-2.5 text-xs font-medium text-gray-500">Source File</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {pageItems.map(i => {
                    const remarkTitle = [i.remark_code, i.remark_description || i.remark_reason]
                      .filter(Boolean).join(' — ')
                    return (
                      <tr key={i.id} className="hover:bg-gray-50">
                        <td className="px-4 py-2.5 max-w-[120px] truncate text-gray-800" title={i.patient_name ?? ''}>
                          {i.patient_name || <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 max-w-[130px] truncate text-gray-600" title={i.payer_name ?? ''}>
                          {i.payer_name || <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap text-xs">
                          {fmtDate(i.date_of_service) || <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 text-gray-500 whitespace-nowrap text-xs">
                          {fmtDate(i.payment_date) || <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 font-mono text-xs text-gray-700" title={i.cpt_description ?? ''}>
                          {i.cpt_code || <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5">
                          <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusColor(i.claim_status)}`}>
                            {i.claim_status || '—'}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 text-right text-gray-600 whitespace-nowrap text-xs">
                          {i.billed_amount != null ? fmtDec(i.billed_amount) : <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 text-right font-medium text-gray-800 whitespace-nowrap text-xs">
                          {i.paid_amount != null ? fmtDec(i.paid_amount) : <span className="text-gray-300">—</span>}
                        </td>
                        <td className="px-4 py-2.5 max-w-[180px] text-xs text-gray-500" title={remarkTitle || undefined}>
                          {i.remark_code ? (
                            <span>
                              <span className="font-mono text-gray-700">{i.remark_code}</span>
                              {(i.remark_description || i.remark_reason) && (
                                <span className="ml-1 text-gray-400 truncate block max-w-[160px]">
                                  {i.remark_description || i.remark_reason}
                                </span>
                              )}
                            </span>
                          ) : (
                            <span className="text-gray-300">—</span>
                          )}
                        </td>
                        <td className="px-4 py-2.5 max-w-[140px] text-xs text-gray-400 truncate" title={i.file_name ?? ''}>
                          {i.file_name || <span className="text-gray-300">—</span>}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between border-t border-gray-100 px-5 py-3">
                <p className="text-xs text-gray-500">
                  Showing {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, filtered.length)} of {filtered.length.toLocaleString()}
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={() => setPage(p => Math.max(0, p - 1))}
                    disabled={page === 0}
                    className="rounded px-3 py-1.5 text-xs font-medium border border-gray-300 text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    ← Previous
                  </button>
                  <span className="flex items-center px-2 text-xs text-gray-500">
                    {page + 1} / {totalPages}
                  </span>
                  <button
                    onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                    disabled={page === totalPages - 1}
                    className="rounded px-3 py-1.5 text-xs font-medium border border-gray-300 text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    Next →
                  </button>
                </div>
              </div>
            )}
          </div>

          <p className="mt-4 text-xs text-gray-400 text-center">
            Showing up to 2,000 records · {PERIOD_LABELS[period]} · check/EFT totals in Deposit Summary above
          </p>
        </>
      )}
    </div>
  )
}

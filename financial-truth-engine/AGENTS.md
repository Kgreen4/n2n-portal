# AGENTS.md — Financial Truth Engine

**Scope:** `financial-truth-engine/` only.
**Status:** Active separate initiative. The legacy EOB extraction project is frozen / reference-only.

This file is the standing contract for any future Codex / Claude Code / agent run that
touches this folder. Read it before doing anything.

---

## Hard Rules

1. **This effort is separate from the current EOB extraction project.**
   The legacy document-first EOB pipeline (GitLab `kgreen41-eob/cardio-metrics-saas`,
   and the `eob_*` / `eob_line_items` / `eob_payments` tables) is frozen. The Financial
   Truth Engine (FTE) is a clean, ledger-centered rebuild.

2. **Work only under `financial-truth-engine/` unless explicitly instructed otherwise.**
   Do not modify files outside this folder. Do not modify existing EOB extraction code,
   edge functions, migrations, or frontend.

3. **Do not reference or migrate old extracted EOB rows as truth.**
   No `INSERT ... SELECT` from `eob_*` tables. No foreign keys to legacy tables. The
   `fte_` schema must stand alone. Old extracted line items, payment rows, reconciliation
   patches, and one-off manual fixes are NOT financial truth and must not be carried forward.

4. **Never commit PHI or sensitive source material.** This repository is public.
   Do not commit:
   - raw EOB / ERA PDFs
   - production database exports
   - screenshots of live EOBs
   - patient names
   - member IDs
   - real claim numbers
   Use **synthetic or redacted fixtures only**. Real PDFs live in private, PHI-safe storage
   and are referenced by internal fixture ID only.

5. **AI observations are not financial truth.**
   Extraction produces *observations* (visible facts with confidence + evidence references),
   never final financial answers. Observations must never directly mutate
   `fte_financial_positions`.

6. **Claim events and financial positions must be deterministic and auditable.**
   `fte_financial_positions` are derived/materialized from `fte_claim_events` by
   deterministic reconciliation — not entered as source truth, not computed by AI.

7. **All financial conclusions must link back to evidence and/or observations.**
   Every `fte_claim_event` must be traceable through `fte_event_evidence` to at least one
   `fte_evidence` and/or `fte_observation` row. No evidence link → it goes to
   `fte_review_queue`, not into the ledger as fact.

8. **Make uncertainty explicit.** Conflicting, low-confidence, late/retry, duplicate,
   summary-row, and unbalanced cases are routed to `fte_review_queue`. They are never
   silently overwritten or patched.

---

## Architecture Principle

```text
Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
```

The PDF is evidence, not truth. AI outputs observations only. Deterministic reconciliation
creates financial truth. Analytics and recommendations reason from the ledger.

---

## Why This Is Separate (Design Intent)

The legacy EOB project asked AI to decide final financial truth directly from a PDF, then
accumulated incremental prompt patches and manual DB corrections to fix the resulting
reconciliation gaps. That is fragile: a single mis-extracted line (e.g. a CO-45 contractual
adjustment landing in `paid_amount`, or a late page-63 retry contradicting an earlier read)
silently corrupted the financial result.

The FTE inverts this. Evidence is immutable. AI only records what it sees, with confidence.
Reconciliation is deterministic and auditable, and every dollar in a financial position is
traceable to evidence. Contradictions surface in a review queue instead of corrupting truth.

Keep it that way.

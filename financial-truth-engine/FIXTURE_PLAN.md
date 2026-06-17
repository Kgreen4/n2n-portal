# Financial Truth Engine — Fixture Plan

**Status:** Initial hard-case fixture selection  
**Created:** 2026-06-17  
**Important:** Do not commit live EOB PDFs, PHI, patient data, raw remittance files, or production exports to this public repository.

---

## Purpose

The Financial Truth Engine needs a small set of difficult, representative test cases before building broad extraction, UI, or analytics.

The fixture strategy is not to migrate old EOB data. The strategy is to preserve a few known hard source examples and use them to prove that the new architecture can produce an auditable claim ledger from messy evidence.

---

## Primary Hard Fixture

### Fixture ID

`ccdbe216`

### Description

BCBS AZ multiple-payment EOB, 112 pages.

### Why This Is The Primary Fixture

This is the hardest known case and should be the first proof case for the Financial Truth Engine architecture.

Known characteristics:

- Largest known document: 112 pages.
- Largest original gap: `$4,720.98` before fixes.
- Required work across multiple sessions.
- Still had remaining open gaps during legacy EOB work.
- Required every category of legacy fix:
  - prompt change
  - OCR sanitization
  - consolidation logic
  - manual database correction

Known failure modes observed in the legacy EOB project:

- OCR phantom duplicate check/reference issue.
- Section-delimiter double counting.
- Null-check crossbleed across multiple check sections.
- Spurious summary row for `$1,479.08`.
- Late page retry anomaly on page 63 where a delayed page job created a contradiction between raw AI response and database items.

### What The New Architecture Must Prove

For this fixture, the Financial Truth Engine should prove that:

1. The PDF is stored as evidence, not treated as truth.
2. Page-level artifacts become evidence records.
3. AI extracts observations only.
4. Duplicate, conflicting, and late observations can coexist without corrupting financial truth.
5. Reconciliation creates claim events and financial positions deterministically.
6. Ambiguous or contradictory observations are routed to review instead of silently patched.
7. Every financial conclusion links back to evidence or observations.

---

## Secondary Regression Fixture

### Fixture ID

`96c5c357`

### Description

Arizona Priority Care EOB.

### Why This Is Secondary

This was the next most difficult known case after `ccdbe216`.

Known characteristics:

- Largest single-check gap: `$1,248.11`.
- Case 2 gap: `$966.20`, still technically open in the legacy work pending retry.

### Use

Use this after the primary fixture passes to make sure the architecture generalizes beyond the BCBS AZ multiple-payment case.

---

## Fixture Storage Policy

Because this repository is public:

- Do not commit raw PDFs.
- Do not commit patient names.
- Do not commit member IDs.
- Do not commit claim numbers unless synthetic or redacted.
- Do not commit raw payer remittance text if it contains PHI.
- Do not commit screenshots of live EOBs.

Allowed in repo:

- Synthetic fixtures.
- Redacted fixture metadata.
- Test cases using fake patient/member/claim IDs.
- Expected behavior descriptions.
- Failure-mode descriptions.
- SQL/unit tests that use synthetic data.

Recommended private storage:

- Store live PDFs in private Supabase Storage, encrypted cloud storage, or another controlled PHI-safe environment.
- Reference them by internal fixture ID only.

---

## First Prototype Target

The first working prototype should run this flow against `ccdbe216` using private/redacted source evidence:

```text
source PDF
  -> document evidence
  -> page evidence
  -> observations
  -> claim identity candidates
  -> claim events
  -> event/evidence links
  -> financial positions
  -> review queue for conflicts
  -> evidence-backed explanation
```

---

## Success Criteria

The first prototype succeeds when it can demonstrate:

- Observations from page 63 or other late/retry pages do not directly mutate financial truth.
- Conflicting observations are retained and reviewed, not overwritten.
- Summary rows can be identified as non-transaction observations or routed to review.
- Multiple payment sections do not crossbleed into the same claim/payment event without evidence.
- The ledger can explain what it believes and why.
- The ledger can explain what it does not know and why.

---

## Codex Instruction

Codex should not use live PHI or raw PDFs in this repo.

Codex should create synthetic fixture rows that simulate the known failure modes above. The real PDF can be tested later in a secure environment once the schema and prototype are ready.

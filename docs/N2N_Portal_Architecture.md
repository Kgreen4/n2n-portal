# N2N Portal — Architecture & Developer Handoff

> **Last updated:** March 2026
> **Project status:** Production (billing live, first paying client active)
> **Working name:** N2N Portal (possible rebrand: Clarix)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack Summary](#2-tech-stack-summary)
3. [System Architecture Diagram](#3-system-architecture-diagram)
4. [Frontend](#4-frontend)
5. [Supabase Edge Functions](#5-supabase-edge-functions)
6. [Database Schema (Postgres)](#6-database-schema-postgres)
7. [BigQuery Schema](#7-bigquery-schema)
8. [n8n Workflow — Trizetto ERA Sync *(In Progress / Not Yet Live)*](#8-n8n-workflow--trizetto-era-sync-in-progress--not-yet-live)
9. [Subscription & Credit Model](#9-subscription--credit-model)
10. [Environment Variables Reference](#10-environment-variables-reference)
11. [Deployment Procedures](#11-deployment-procedures)
12. [Scheduled Jobs](#12-scheduled-jobs)
13. [Common Pitfalls & Gotchas](#13-common-pitfalls--gotchas)

---

## 1. Project Overview

N2N Portal is a multi-tenant SaaS platform for medical practices that automates Explanation of Benefits (EOB) extraction from PDF documents. Practices upload multi-page EOB PDFs (from their Google Drive, direct upload, or GCS), which are split into individual pages, processed by Gemini AI to extract structured claim data, and stored in BigQuery for review and reconciliation.

### Key Identifiers

| Resource | Value |
|----------|-------|
| Supabase project ref | `jdmyjdvricpyrsfchakk` |
| Supabase URL | `https://jdmyjdvricpyrsfchakk.supabase.co` |
| GCP project | `cardio-metrics-dev` |
| BigQuery dataset | `billing_audit_practice_test` |
| Vercel URL | https://n2n-portal.vercel.app |
| GCP service account | `n8n-backend@cardio-metrics-dev.iam.gserviceaccount.com` |
| Google OAuth client | `71932304669-aha3mq2adt7o6s1fvgs5pdjkebnj3kn7.apps.googleusercontent.com` |
| First paying client | Dr. Ravi |

---

## 2. Tech Stack Summary

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Frontend** | Next.js (App Router) | 14 / v16.1.6 pkg | Practice management UI |
| **UI Framework** | React | 19.2.3 | Component rendering |
| **Styling** | TailwindCSS | 4 | Utility-first CSS |
| **Backend runtime** | Deno | 2 | Edge Function runtime |
| **Backend platform** | Supabase Edge Functions | Latest | 16 serverless API handlers |
| **Primary database** | PostgreSQL | 17 | Metadata, jobs, practices, subscriptions |
| **Data warehouse** | Google BigQuery | GCP | Extracted EOB line items |
| **File storage** | Supabase Storage + GCS | S3-compatible | PDFs, split pages, archives |
| **AI extraction** | Gemini 2.0 Flash (Vertex AI) | Multimodal | OCR + structured data extraction |
| **PDF processing** | pdf-lib | 1.17.1 | Split multi-page PDFs |
| **Billing** | Stripe | Test mode | Subscriptions + credit packs |
| **Auth** | Supabase Auth + Google OAuth 2.0 | — | User identity + Drive integration |
| **Scheduling** | pg_cron + pg_net | PostgreSQL extensions | Recovery sweeper every 5 min |
| **Workflow automation** | n8n | Self-hosted on GCP VM | File watching + ERA sync |
| **Frontend deployment** | Vercel | — | CDN, edge rendering |
| **Backend deployment** | Supabase CLI | v2.78.1+ | Deploy edge functions |

---

## 3. System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│                                                                     │
│  [Practice Google Drive]   [Frontend Upload]   [GCS Bucket]        │
│         │                        │                    │             │
│         └───────────────┬────────┘                    │             │
│                         │                             │             │
└─────────────────────────┼─────────────────────────────┼─────────────┘
                          │                             │
                          ▼                             ▼
              ┌─────────────────────┐      ┌──────────────────────┐
              │  n8n (GCP VM)       │      │  Frontend (Next.js)  │
              │  - Google Drive     │      │  /upload page        │
              │    file watcher     │      │  POST signed URL     │
              └────────┬────────────┘      └──────────┬───────────┘
                       │                              │
                       └───────────────┬──────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │  trigger-eob-parser          │
                         │  (Edge Function)             │
                         │  - Creates eob_documents row │
                         │  - Logs to processing_logs   │
                         │  - Calls eob-enqueue         │
                         └─────────────┬───────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │  eob-enqueue                 │
                         │  (Edge Function)             │
                         │  - Downloads PDF             │
                         │  - Splits pages (pdf-lib)    │
                         │  - Uploads pages to Storage  │
                         │  - Creates eob_page_jobs     │
                         │  - Charges credits per page  │
                         │  - Fires eob-worker × N      │
                         └─────────────┬───────────────┘
                                       │ (batches of 3)
                              ┌────────┴────────┐
                              │                 │
                              ▼                 ▼
               ┌──────────────────┐  ┌──────────────────┐
               │  eob-worker      │  │  eob-worker       │  ···
               │  (page 1)        │  │  (page 2)         │
               │  - Calls Gemini  │  │  - Calls Gemini   │
               │  - Parses JSON   │  │  - Parses JSON    │
               │  - Inserts BQ    │  │  - Inserts BQ     │
               └────────┬─────────┘  └────────┬──────────┘
                        └────────────┬─────────┘
                                     │
                                     ▼
                         ┌─────────────────────────────┐
                         │  BigQuery: eob_line_items    │
                         │  (one row per claim line)    │
                         └─────────────┬───────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │  check-exceptions            │
                         │  - Queries BigQuery          │
                         │  - Sets review_status        │
                         │  - Flags needs_review        │
                         └─────────────┬───────────────┘
                                       │
                          ┌────────────┴────────────────┐
                          │                             │
                          ▼                             ▼
             ┌─────────────────────┐      ┌──────────────────────┐
             │  Frontend: /inbox   │      │  Frontend: /documents│
             │  (needs review)     │      │  (all documents)     │
             └─────────────────────┘      └──────────────────────┘
                          │
                          ▼
             ┌─────────────────────────────┐
             │  update-line-items           │
             │  - Edit claims in BigQuery   │
             │  - Re-run check-exceptions   │
             └─────────────────────────────┘

RECOVERY (every 5 min via pg_cron):
   ┌─────────────────────────────┐
   │  eob-sweeper                │
   │  - Retries stuck/failed     │
   │    page jobs                │
   │  - Refunds failed credits   │
   └─────────────────────────────┘
```

---

## 4. Frontend

**Framework:** Next.js 14 App Router, deployed on Vercel
**Root:** `frontend/`
**Deployment command (run from project root):** `npx vercel --prod`

### Route Structure

```
frontend/src/app/
├── (auth)/
│   ├── login/page.tsx              Login form
│   ├── signup/page.tsx             New account signup
│   └── layout.tsx
│
├── (dashboard)/
│   ├── layout.tsx                  Sidebar + practice context provider
│   ├── dashboard/page.tsx          Home — activity feed + stats
│   ├── inbox/page.tsx              Review queue (needs_review docs)
│   ├── documents/
│   │   ├── page.tsx                Document list with filter/sort
│   │   ├── DocumentsClient.tsx     Client component for filtering
│   │   └── [id]/page.tsx           Document detail — line item editor
│   ├── upload/page.tsx             Direct PDF upload UI
│   ├── reconciliation/page.tsx     Bank CSV upload + EOB matching
│   ├── settings/page.tsx           Practice settings, Drive config
│   └── billing/page.tsx            Stripe billing: plan + credit packs
│
├── onboarding/
│   ├── page.tsx                    4-step setup wizard
│   ├── callback/page.tsx           Google OAuth callback handler
│   └── layout.tsx
│
├── auth/
│   ├── callback/route.ts           Supabase Auth exchange (OAuth)
│   └── signout/route.ts            Logout endpoint
│
├── setup/page.tsx                  Pre-onboarding: links user to practice
├── page.tsx                        Landing / redirect to dashboard
└── layout.tsx                      Root layout, global styles
```

### Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| `SessionTimeout` | `src/components/SessionTimeout.tsx` | Logs out after 30 min inactivity |
| `PdfUploader` | `src/components/PdfUploader.tsx` | Multi-file PDF upload with progress |

### Key Libraries

- `@supabase/ssr` — server-side Supabase client (Next.js App Router)
- `@stripe/stripe-js` — Stripe.js for checkout redirects
- TailwindCSS 4 — utility CSS

### Environment Variables (Frontend)

Set in Vercel project settings AND `.env.local` for local dev:

```
NEXT_PUBLIC_SUPABASE_URL
NEXT_PUBLIC_SUPABASE_ANON_KEY
NEXT_PUBLIC_GOOGLE_OAUTH_CLIENT_ID
NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID
NEXT_PUBLIC_STRIPE_PRO_PRICE_ID
NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID
NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID
```

> **Gotcha:** `NEXT_PUBLIC_*` vars must be accessed with literal string keys in code — not via dynamic `process.env[varName]`. Next.js performs static replacement at build time.

---

## 5. Supabase Edge Functions

All 16 functions live in `supabase/functions/<name>/index.ts`.
Runtime: Deno 2. Deployed via `supabase functions deploy`.

### Critical: `--no-verify-jwt` Flag

Functions called server-to-server (no user JWT) **must** be deployed with `--no-verify-jwt`. Deploying without it re-enables gateway JWT verification and causes `401 Invalid JWT` errors on all server-to-server calls.

| Function | Needs `--no-verify-jwt`? | Called by |
|----------|--------------------------|-----------|
| `eob-enqueue` | **YES** | `trigger-eob-parser` |
| `eob-sweeper` | **YES** | pg_cron (Postgres) |
| `generate-835` | **YES** | Server-to-server |
| `ingest-era-data` | **YES** | n8n workflow |
| `stripe-webhook` | **YES** | Stripe |
| `trigger-eob-parser` | **YES** | n8n + frontend |
| `create-checkout-session` | No | Frontend (user JWT) |
| `create-portal-session` | No | Frontend (user JWT) |
| `create-practice` | No | Frontend (user JWT) |
| `google-drive-setup` | No | Frontend (user JWT) |

### Deploy command example

```bash
supabase functions deploy eob-enqueue --no-verify-jwt
supabase functions deploy create-practice  # no flag needed
```

---

### Function Reference

#### `trigger-eob-parser`
**Role:** Entry point for all EOB processing. Creates the document record and hands off to `eob-enqueue`.

- Creates `eob_documents` row (`status: 'pending'`)
- Logs to `eob_processing_logs`
- Supports 3 PDF sources: GCS bucket, Supabase Storage, Google Drive
- Dual-auth: accepts user JWT (frontend uploads) or service role key (n8n)
- Does **not** charge credits (only `eob-enqueue` knows actual page count)

---

#### `eob-enqueue`
**Role:** PDF splitting orchestrator. The credit-charging step.

- Downloads PDF from source (GCS, Supabase Storage, Google Drive, signed URL)
- Splits into pages using `pdf-lib`
- Uploads each page to Supabase Storage: `eob-pages/{doc_id}/page-{n}.pdf`
- Creates `eob_page_jobs` records (one per page, status: `queued`)
- Charges `credits` against `practices` table (based on actual page count)
- Fires `eob-worker` for each page in **batches of 3** with rate-limit delays
- Constants: `MAX_PAGES_PER_DOC = 500`, storage bucket: `"eob-pages"`

---

#### `eob-worker`
**Role:** Single-page extraction worker. Calls Gemini and writes to BigQuery.

- Downloads page PDF from Supabase Storage
- Authenticates to GCP via JWT from `GCP_SA_JSON` service account
- Calls **Gemini 2.0 Flash** (Vertex AI, multimodal) with extraction prompt
- Extracts 13 fields per claim line: patient_name, member_id, date_of_service, cpt_code, cpt_description, billed/allowed/paid amounts, patient_responsibility, rendering_provider_npi, denial_code, denial_reason, claim_status
- Post-processing: currency parsing, date normalization, remark code lookup table, claim status inference
- Inserts rows into BigQuery `eob_line_items`
- Updates job status: `queued` → `processing` → `completed` / `failed`
- Handles Gemini 429 rate limits with exponential backoff
- Handles empty/blank pages gracefully (0 items, no error)
- Max output tokens: 8192

---

#### `eob-sweeper`
**Role:** Recovery sweeper for stuck or failed page jobs.

- Handles 3 scenarios:
  1. Stuck `queued` jobs (created >5 min ago, never started)
  2. `retryable` jobs (failed but retries remaining)
  3. Orphaned documents (all page jobs terminal but doc still `processing`)
- Fires workers staggered 2 seconds apart (returns <30s, fire-and-forget)
- Refunds credits for permanently failed jobs
- Called every 5 minutes by pg_cron (see [Scheduled Jobs](#12-scheduled-jobs))

---

#### `check-exceptions`
**Role:** Post-extraction data quality evaluation.

- Queries BigQuery for the completed document
- Checks: unmatched claims, missing claim IDs, low confidence, found revenue
- Updates `eob_documents`: `review_status`, `review_reasons`, `has_found_revenue`
- Sets `needs_review = true` when issues found (appears in `/inbox`)

---

#### `update-line-items`
**Role:** Fix-and-Post — edit extracted line items in BigQuery.

- Accepts array of field updates keyed by composite row identity
- Editable fields: paid/billed/allowed amounts, claim status, CPT code, patient name, etc.
- Validates and performs UPDATE in BigQuery
- Re-fires `check-exceptions` after changes

---

#### `fetch-line-items`
**Role:** Read line items from BigQuery for the document detail page.

- Queries `eob_payment_items` view (excludes `summary_total` rows)
- Used by frontend `/documents/[id]`

---

#### `reprocess-document`
**Role:** Full re-extraction of a document.

- Verifies practice ownership via `practice_users`
- Deletes existing BigQuery rows for document
- Deletes page jobs from Postgres
- Resets document to `pending`
- Re-calls `eob-enqueue` (preserving original PDF source metadata)

---

#### `create-checkout-session`
**Role:** Start Stripe Checkout for plan or credit pack purchase.

- Body: `{ practice_id, price_id, mode: 'subscription'|'payment', credits_to_add? }`
- Gets or creates Stripe customer for practice
- Returns `{ url }` — Stripe-hosted checkout URL

---

#### `stripe-webhook`
**Role:** Handles all Stripe billing events.

| Event | Action |
|-------|--------|
| `checkout.session.completed` | Link Stripe customer, set plan or add credits |
| `invoice.payment_succeeded` | Reset credits to monthly plan amount |
| `customer.subscription.updated` | Update plan tier and limits |
| `customer.subscription.deleted` | Downgrade to trial |

- Verifies Stripe webhook signature (manual HMAC-SHA256)
- Updates `practices`: `stripe_customer_id`, `stripe_subscription_id`, `plan_tier`, `credits`

---

#### `create-portal-session`
**Role:** Stripe Customer Portal — manage plan, payment, invoices.

- Body: `{ practice_id }`
- Returns `{ url }` — Stripe-hosted portal URL

---

#### `create-practice`
**Role:** Self-service practice creation during onboarding.

- Verifies user JWT
- Creates `practices` row with auto-generated slug
- Links user as `owner` in `practice_users`
- Grants 50 starter credits, sets `trial_ends_at = now() + 7 days`
- Returns `{ practice_id, name, slug }`

---

#### `google-drive-setup`
**Role:** One-time Google OAuth exchange + Drive folder sharing.

- Body: `{ code, folder_id, practice_id }`
- Exchanges OAuth code for access token (token used once, not stored)
- Gets folder name from Drive API
- Shares folder with service account (writer access)
- Upserts `practice_settings`: `folder_id`, `folder_name`, `watcher_enabled = true`

---

#### `parse-bank-csv`
**Role:** Bank statement reconciliation.

- Parses bank CSV uploads
- Inserts deposits to `bank_deposits` table
- Auto-matches against EOB check totals by `check_number`

---

#### `ingest-era-data` *(In Progress — see Section 8)*
**Role:** Receives normalized ERA line items from n8n Google Drive watcher workflow (biller drops ERA files into Drive; n8n parses and calls this function).

- Inserts to BigQuery `eob_line_items` with `source_type: "trizetto_era"`
- Deduplicates by `{payer_id}|{claim_number}|{cpt_code}|{date_of_service}`
- Batches inserts in chunks of 500
- Logs results to `pipeline_events`

---

#### `generate-835`
**Role:** Generates ANSI X12 835 EDI output from extracted line items.

- Queries BigQuery for completed document
- Produces standard 835 remittance file format

---

## 6. Database Schema (Postgres)

Managed by Supabase. Migrations in `supabase/migrations/`.

### Core Tables

#### `practices`
Multi-tenant root. One row per medical practice.

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid | PK |
| `name` | text | Practice display name |
| `slug` | text | URL-safe unique identifier |
| `stripe_customer_id` | text | Stripe customer |
| `stripe_subscription_id` | text | Active subscription |
| `plan_tier` | text | `trial` / `starter` / `professional` |
| `credits` | integer | Remaining page credits |
| `trial_ends_at` | timestamptz | 7-day trial expiry (set at signup) |
| `created_at` | timestamptz | — |

#### `practice_users`
Links users to practices with roles.

| Column | Type | Notes |
|--------|------|-------|
| `practice_id` | uuid | FK → practices |
| `user_id` | uuid | FK → auth.users |
| `role` | text | `owner` / `admin` / `user` |

#### `eob_documents`
One row per uploaded EOB PDF.

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid | PK |
| `practice_id` | uuid | FK → practices |
| `uploaded_by` | uuid | FK → auth.users (null for n8n uploads) |
| `file_name` | text | Original PDF filename |
| `status` | text | `pending` / `processing` / `completed` / `failed` |
| `review_status` | text | `ok` / `needs_review` / `reviewed` |
| `review_reasons` | text[] | List of flagged issues |
| `needs_review` | boolean | Inbox flag |
| `has_found_revenue` | boolean | Bonus incentive flag |
| `page_count` | integer | Total pages (set by eob-enqueue) |
| `credits_charged` | integer | Credits deducted |
| `source_type` | text | `gdrive` / `upload` / `gcs` |
| `gdrive_file_id` | text | Google Drive file ID (if applicable) |
| `created_at` | timestamptz | — |

#### `eob_page_jobs`
Per-page extraction job tracking.

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid | PK |
| `eob_document_id` | uuid | FK → eob_documents |
| `practice_id` | uuid | FK → practices |
| `page_number` | integer | 1-indexed |
| `status` | text | `queued` / `processing` / `completed` / `failed` / `retryable` |
| `retry_count` | integer | Max 3 retries |
| `error_message` | text | Last error (if failed) |
| `created_at` | timestamptz | — |
| `updated_at` | timestamptz | — |

#### `practice_settings`
One row per practice. Google Drive watcher config.

| Column | Type | Notes |
|--------|------|-------|
| `practice_id` | uuid | PK, FK → practices |
| `folder_id` | text | Google Drive folder ID for EOB PDFs |
| `folder_name` | text | Display name for EOB folder |
| `watcher_enabled` | boolean | Whether n8n watches the EOB folder |
| `era_folder_id` | text | Google Drive folder ID for ERA files *(to be added)* |
| `era_folder_name` | text | Display name for ERA folder *(to be added)* |
| `era_watcher_enabled` | boolean | Whether n8n watches the ERA folder *(to be added)* |

#### `eob_processing_logs`
Audit trail for trigger-eob-parser calls.

#### `bank_deposits`
Bank statement deposits for reconciliation.

| Column | Type | Notes |
|--------|------|-------|
| `practice_id` | uuid | FK → practices |
| `check_number` | text | Matched against EOB check totals |
| `amount` | numeric | Deposit amount |
| `deposit_date` | date | — |
| `matched_eob_id` | uuid | Matched eob_document (if reconciled) |

#### `pipeline_events`
Audit trail for all pipeline sync runs (ERA ingestion, errors).

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid | PK |
| `practice_id` | uuid | FK → practices |
| `event_type` | text | e.g., `era_sync` |
| `source_system` | text | e.g., `trizetto_era` |
| `records_processed` | integer | — |
| `records_inserted` | integer | — |
| `records_skipped` | integer | Duplicates |
| `error_message` | text | Null if success |
| `metadata` | jsonb | Extra context |
| `created_at` | timestamptz | — |

### Views

#### `practice_members`
Created in `20260313_practice_members_view.sql`. Joins `practice_users` + `auth.users` + `practices`. Useful for access audits in Supabase Table Editor.

### Financial Tables *(In Progress — not yet live)*

Created in `20260320_financial_pipeline.sql`:

| Table | Purpose |
|-------|---------|
| `qb_monthly_summary` | QuickBooks P&L by month (income, expenses, net) |
| `qs_billing_summary` | Ethizo/QuickSight billing actuals (encounters, charges, AR aging) |
| `dashboard_config` | User-adjustable projection settings (growth rate, salaries, rent, etc.) |

### Migrations

| File | Purpose |
|------|---------|
| `20260311_phase15b_trial_ends_at.sql` | Adds `trial_ends_at` to practices |
| `20260313_practice_members_view.sql` | Creates `practice_members` view |
| `20260314_schedule_eob_sweeper.sql` | Schedules eob-sweeper via pg_cron |
| `20260320_financial_pipeline.sql` | Financial analytics tables *(not yet live)* |

Apply migrations: `supabase db push --linked`

> **Gotcha:** `supabase db execute` is not available in CLI v2.78.1. Run SQL directly in Supabase Dashboard → SQL Editor.

---

## 7. BigQuery Schema

**GCP project:** `cardio-metrics-dev`
**Dataset:** `billing_audit_practice_test`
**Primary table:** `eob_line_items`

### `eob_line_items` Table

| Column | Type | Notes |
|--------|------|-------|
| `id` | STRING | UUID, dedup key |
| `eob_document_id` | STRING | Links to Postgres `eob_documents.id` |
| `practice_id` | STRING | Multi-tenant partition key |
| `page_number` | INTEGER | Which page this was extracted from |
| `patient_name` | STRING | — |
| `member_id` | STRING | Insurance member ID |
| `date_of_service` | STRING | Normalized to YYYY-MM-DD |
| `cpt_code` | STRING | Procedure code |
| `cpt_description` | STRING | Procedure description |
| `billed_amount` | FLOAT | — |
| `allowed_amount` | FLOAT | — |
| `paid_amount` | FLOAT | — |
| `patient_responsibility` | FLOAT | — |
| `rendering_provider_npi` | STRING | — |
| `denial_code` | STRING | e.g., `CO-45`, `PR-1` |
| `denial_reason` | STRING | Human-readable denial explanation |
| `claim_status` | STRING | `Paid` / `Partially Paid` / `Denied` |
| `remark_code` | STRING | ERA only |
| `remark_description` | STRING | ERA only |
| `payer_id` | STRING | ERA only |
| `payer_name` | STRING | ERA only |
| `check_number` | STRING | ERA only |
| `check_date` | STRING | ERA only |
| `source_type` | STRING | `pdf_parser` or `trizetto_era` |
| `created_at` / `ingested_at` | TIMESTAMP | — |

### Dedup Keys

- PDF extraction: `{eob_document_id}_p{pageNum}_{rowIdx}`
- ERA ingestion: `{payer_id}|{claim_number}|{cpt_code}|{date_of_service}`

### View: `eob_payment_items`

Excludes `summary_total` rows from `eob_line_items`. Used by `fetch-line-items` function for the frontend document detail page.

### Authentication

BigQuery is accessed via GCP service account JWT auth. The `GCP_SA_JSON` secret contains the full service account JSON. Functions exchange it for a short-lived OAuth access token, then call the BigQuery REST API directly (no BigQuery SDK needed in Deno).

---

## 8. ERA File Ingestion via Google Drive *(In Progress / Not Yet Live)*

> The `supabase/functions/ingest-era-data/` edge function exists in the codebase but is **not yet committed or deployed to production**. The n8n workflow (`n8n/trizetto-era-sync.json`) that was originally drafted for a direct API connection to Trizetto is **not being pursued** — it should be discarded.

### Revised Approach

Instead of a direct API connection to Trizetto/Change Healthcare, ERA files will be sourced manually by the biller:

1. **Biller exports ERA file(s)** from Trizetto (scheduled download or manual export)
2. **Biller places file(s)** into a **dedicated ERA Google Drive folder** (separate from the EOB PDF folder)
3. **n8n Google Drive watcher** detects new file(s) in the ERA folder
4. **n8n workflow** reads, parses, and normalizes the ERA file contents
5. **`ingest-era-data` edge function** receives the normalized line items and inserts into BigQuery

Both ERA-sourced data and PDF-extracted data write to the same BigQuery `eob_line_items` table, distinguished by `source_type` (`"trizetto_era"` vs `"pdf_parser"`).

### ERA File Formats

ERA files from Trizetto are typically one of:
- **ANSI X12 835** — standard EDI remittance format (most common)
- **CSV/Excel export** — if biller uses a Trizetto portal report instead

> **Developer note:** The n8n workflow will need to be built fresh to handle file detection and parsing. The existing `trizetto-era-sync.json` (API-based) should be deleted from the `n8n/` folder as it no longer reflects the intended approach.

### What Still Needs to Be Built

| Step | Work Required |
|------|--------------|
| n8n Google Drive watcher for ERA folder | New workflow (separate folder from EOB PDFs, or subfolder) |
| ERA file parser (835 EDI or CSV) | n8n Code node or dedicated parser |
| Transform to `ingest-era-data` shape | Normalize fields to match `eob_line_items` schema |
| `ingest-era-data` edge function | Already drafted — review and deploy with `--no-verify-jwt` |
| Practice Drive folder config for ERA | Separate Google Drive folder from EOB PDFs — store `era_folder_id` in `practice_settings` |

### `ingest-era-data` Edge Function (existing draft)

- Receives normalized ERA line items from n8n
- Inserts to BigQuery `eob_line_items` with `source_type: "trizetto_era"`
- Deduplicates by `{payer_id}|{claim_number}|{cpt_code}|{date_of_service}`
- Batches inserts in chunks of 500
- Logs results to `pipeline_events`
- Must be deployed with `--no-verify-jwt` (called server-to-server by n8n)

---

## 9. Subscription & Credit Model

### Tiers

| Tier | Price | Credits/month | Max pages per EOB |
|------|-------|--------------|-------------------|
| Trial | Free | 50 (one-time) | 10 |
| Starter | $99/mo | 500 | 50 |
| Professional | $299/mo | 2,000 | 150 |

### Credit Packs (one-time purchases)

| Pack | Price | Credits |
|------|-------|---------|
| Boost 100 | ~$10 | 100 |
| Boost 500 | ~$40 | 500 |

### Stripe Price IDs (test mode)

| Item | Price ID |
|------|---------|
| Starter subscription | `price_1T9YSv0JXufBkchGE2BB40w3` |
| Professional subscription | `price_1T9YTk0JXufBkchGaQlXJG58` |
| Boost 100 | `price_1T9YUI0JXufBkchGOGwuJIhc` |
| Boost 500 | `price_1T9YUh0JXufBkchGOfe66wc1` |

### Credit Logic

- Credits are **charged in `eob-enqueue`** — after loading the PDF and knowing the actual page count
- `trigger-eob-parser` does not charge credits (it doesn't know page count yet)
- Credits are refunded for permanently failed page jobs (by `eob-sweeper`)
- Monthly plan credits are **reset** (not accumulated) on each `invoice.payment_succeeded` Stripe event

---

## 10. Environment Variables Reference

### Supabase Edge Functions (set via `supabase secrets set`)

| Variable | Purpose |
|----------|---------|
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key — bypasses RLS |
| `SUPABASE_ANON_KEY` | Public client key |
| `GCP_SA_JSON` | Full GCP service account JSON (string) — used for Vertex AI + BigQuery auth |
| `STRIPE_SECRET_KEY` | Stripe test/prod secret key |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret |
| `STRIPE_STARTER_PRICE_ID` | See Section 9 |
| `STRIPE_PRO_PRICE_ID` | See Section 9 |
| `STRIPE_BOOST100_PRICE_ID` | See Section 9 |
| `STRIPE_BOOST500_PRICE_ID` | See Section 9 |
| `GOOGLE_OAUTH_CLIENT_ID` | Google OAuth 2.0 client ID |
| `GOOGLE_OAUTH_CLIENT_SECRET` | Google OAuth 2.0 client secret |
| `GOOGLE_SERVICE_ACCOUNT_EMAIL` | `n8n-backend@cardio-metrics-dev.iam.gserviceaccount.com` |

### Frontend (Vercel environment variables + `.env.local` for local dev)

| Variable | Purpose |
|----------|---------|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase URL (client-side) |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Supabase anon key (client-side) |
| `NEXT_PUBLIC_GOOGLE_OAUTH_CLIENT_ID` | Google OAuth client ID for Drive setup |
| `NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID` | Shown in billing UI |
| `NEXT_PUBLIC_STRIPE_PRO_PRICE_ID` | Shown in billing UI |
| `NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID` | Shown in billing UI |
| `NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID` | Shown in billing UI |

> **Adding `NEXT_PUBLIC_*` vars to Vercel preview environments requires the `--value` flag:**
> ```bash
> vercel env add NEXT_PUBLIC_GOOGLE_OAUTH_CLIENT_ID preview --value "your-value"
> ```

### n8n Environment Variables

| Variable | Purpose |
|----------|---------|
| `SUPABASE_SERVICE_ROLE_KEY` | For calling edge functions server-to-server |

---

## 11. Deployment Procedures

### Frontend (Vercel)

```bash
# Run from PROJECT ROOT (not frontend/ subdirectory)
# Vercel has rootDirectory: frontend already configured
npx vercel --prod
```

### Supabase Edge Functions

```bash
# Deploy a single function (no JWT flag needed)
supabase functions deploy create-practice

# Deploy a function that requires no-verify-jwt (server-to-server)
supabase functions deploy eob-enqueue --no-verify-jwt
supabase functions deploy eob-sweeper --no-verify-jwt
supabase functions deploy trigger-eob-parser --no-verify-jwt
supabase functions deploy stripe-webhook --no-verify-jwt
supabase functions deploy generate-835 --no-verify-jwt
supabase functions deploy ingest-era-data --no-verify-jwt  # when ready

# Deploy all functions (be careful — must add --no-verify-jwt per function)
```

### Database Migrations

```bash
supabase db push --linked
```

Or run SQL directly in **Supabase Dashboard → SQL Editor** (required for CLI v2.78.1 which lacks `supabase db execute`).

### Supabase Secrets

```bash
supabase secrets set GCP_SA_JSON='{"type":"service_account",...}'
supabase secrets set STRIPE_SECRET_KEY=sk_test_...
# etc.
```

---

## 12. Scheduled Jobs

### EOB Sweeper (pg_cron)

**Schedule:** Every 5 minutes
**Defined in:** `supabase/migrations/20260314_schedule_eob_sweeper.sql`

The sweeper runs inside Postgres via the `pg_cron` and `pg_net` extensions (available on all Supabase projects). It makes an HTTP POST to the `eob-sweeper` edge function.

```sql
-- Check if scheduled job exists
SELECT * FROM cron.job WHERE jobname = 'eob-sweeper';

-- Manually trigger (for testing)
SELECT net.http_post(
  url := 'https://jdmyjdvricpyrsfchakk.supabase.co/functions/v1/eob-sweeper',
  headers := '{"Authorization": "Bearer <service_role_key>", "Content-Type": "application/json"}'::jsonb,
  body := '{}'::jsonb
);
```

**What the sweeper does:**
1. Finds stuck `queued` jobs (created >5 min ago, never picked up) → retries
2. Finds `retryable` jobs (worker failed, retries remaining) → retries
3. Finds orphaned documents (all jobs terminal but doc still `processing`) → marks completed/failed
4. Refunds credits for permanently failed jobs

---

## 13. Common Pitfalls & Gotchas

### Critical

- **`--no-verify-jwt` must be specified on every redeploy** for server-to-server functions. Redeploying without the flag re-enables JWT verification and breaks all n8n → Supabase calls with `401 Invalid JWT`. This caused a live demo failure. Always check the table in Section 5.

- **Vercel deploy from project root, not `frontend/`** — The Vercel project has `rootDirectory: frontend` configured. Running `vercel --prod` from inside `frontend/` results in a double-nested path error.

### Database / API

- **`supabase db execute` is not available in CLI v2.78.1.** Use `supabase db push --linked` for migrations, or run SQL in the Supabase Dashboard → SQL Editor.

- **PostgREST error `PGRST116`** = "The result contains 0 rows" from `.single()`. This is expected behavior when a row doesn't exist — not a real server error. Handle it gracefully.

### Frontend

- **`NEXT_PUBLIC_*` vars require literal key access.** Next.js replaces them at build time via static analysis. `process.env[dynamicKey]` will return `undefined`.

- **`useSearchParams` requires `<Suspense>` wrapper** in Next.js 13/14 App Router. Omitting it causes a build error.

### Google APIs

- **Google Drive Permissions API** requires the `https://www.googleapis.com/auth/drive` scope — not the more limited `drive.file` scope. Using `drive.file` causes permission errors when sharing folders.

### Stripe

- **Webhook signature verification** is done manually (HMAC-SHA256) in `stripe-webhook/index.ts` since the Stripe Node.js SDK is not available in Deno. Ensure `STRIPE_WEBHOOK_SECRET` is set correctly.

### Gemini / AI

- **Gemini 429 rate limits** are common under load. `eob-worker` handles them with exponential backoff. `eob-enqueue` batches worker calls in groups of 3 with delays to reduce rate limit pressure.

- **maxOutputTokens is set to 8192** in `eob-worker`. Increasing EOB complexity may require tuning this, but higher values increase latency.

- **JSON repair logic** is implemented in `eob-worker` to handle malformed Gemini responses (truncated JSON, trailing text). If extraction quality degrades, check the Gemini response format first.

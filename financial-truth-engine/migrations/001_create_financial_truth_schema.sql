-- =============================================================================
-- Financial Truth Engine (FTE) — Ledger Schema
-- Migration: 001_create_financial_truth_schema.sql
-- Created: 2026-06-17
--
-- WHY THIS IS SEPARATE FROM THE OLD EOB ARCHITECTURE
-- -----------------------------------------------------------------------------
-- The legacy EOB project (eob_documents / eob_line_items / eob_payments) asked
-- AI to decide final financial truth directly from a PDF, then patched the
-- resulting reconciliation gaps with prompt edits and manual DB corrections.
-- That coupling made a single mis-extraction able to silently corrupt the
-- financial result.
--
-- The Financial Truth Engine is ledger-centered:
--     Evidence -> Observations -> Claim Ledger -> Reconciliation -> Intelligence
--
--   * Evidence is immutable (append-only). The PDF is evidence, NOT truth.
--   * Observations are AI-visible facts with confidence + evidence references.
--     They are NOT financial truth and must never directly mutate positions.
--   * Claim events are deterministic, auditable financial events.
--   * Financial positions are DERIVED/materialized from claim events.
--   * Every financial conclusion links back to evidence and/or observations.
--   * Ambiguity (conflicts, low confidence, late/retry, summary rows, unbalanced
--     positions) is routed to a review queue, never silently overwritten.
--
-- All tables are prefixed `fte_` so this schema stays cleanly isolated even if
-- temporarily deployed into the same Supabase project as the legacy EOB tables.
-- This migration intentionally references NO legacy `eob_*` table.
-- =============================================================================

begin;

create extension if not exists pgcrypto;  -- gen_random_uuid()

-- -----------------------------------------------------------------------------
-- Shared helpers
-- -----------------------------------------------------------------------------

-- Maintains updated_at on UPDATE.
create or replace function fte_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- RLS tenant-scope helper (STUB — deny-by-default).
-- Returns the practice_ids the current authenticated user may access.
-- Wire this to your real membership lookup before loading real data, e.g.:
--     select practice_id from practice_members where user_id = auth.uid();
-- Until then it returns no rows, so RLS denies all access to non-superusers.
-- (Supabase `service_role` has BYPASSRLS and is unaffected; that is how the
--  migration, fixtures, and validation scripts run.)
create or replace function fte_accessible_practice_ids()
returns setof uuid
language sql
stable
as $$
  select null::uuid where false;
$$;


-- =============================================================================
-- Layer 1: practices (tenant root)
-- =============================================================================
create table fte_practices (
  id          uuid primary key default gen_random_uuid(),
  name        text not null,
  external_ref text,                 -- optional synthetic/redacted external key
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);
comment on table fte_practices is
  'Tenant root for the Financial Truth Engine. Every tenant-scoped fte_ table references this.';
comment on column fte_practices.external_ref is
  'Optional synthetic/redacted external identifier. Never store PHI here.';


-- =============================================================================
-- Layer 1: evidence (immutable source artifacts)
--   document / page / OCR text / ERA-835 segment / check-payment / payer-export
--   Append-only: NO updated_at, NO UPDATE policy. Corrections become new
--   observations or events, never mutations of evidence.
-- =============================================================================
create table fte_evidence (
  id                 uuid primary key default gen_random_uuid(),
  practice_id        uuid not null references fte_practices(id) on delete restrict,
  parent_evidence_id uuid references fte_evidence(id) on delete restrict,  -- page -> document
  evidence_type      text not null check (evidence_type in (
                        'document',          -- whole source artifact (e.g. a 112-page PDF)
                        'page',              -- one page of a document
                        'ocr_text',          -- OCR/text extraction of a page or region
                        'era_835_segment',   -- a parsed ERA / 835 segment
                        'check_payment',     -- a check / EFT stub artifact
                        'payer_export'       -- payer portal / export artifact
                      )),
  fixture_id         text,           -- internal fixture/source id (e.g. 'ccdbe216'); never PHI
  source_uri         text,           -- pointer to PRIVATE storage; never inline PHI content
  content_hash       text,           -- hash of source bytes for integrity / dedup
  page_number        integer,        -- 1-based page index for page/ocr evidence
  raw_text           text,           -- synthetic/redacted text only (OCR/segment evidence)
  metadata           jsonb not null default '{}'::jsonb,
  created_at         timestamptz not null default now()
  -- intentionally NO updated_at: evidence is immutable / append-only.
);
comment on table fte_evidence is
  'Immutable, append-only source artifacts and derived evidence units. The PDF is evidence, '
  'not truth. Never UPDATE rows here; record corrections as new observations or claim events.';
comment on column fte_evidence.parent_evidence_id is
  'Self-reference: a page references its document, an ocr_text references its page, etc.';
comment on column fte_evidence.source_uri is
  'Reference to PHI-safe private storage (e.g. private Supabase Storage). Do not inline PHI.';
comment on column fte_evidence.raw_text is
  'Synthetic or redacted text only. Real OCR text containing PHI must not be stored in this repo''s DB.';


-- =============================================================================
-- Layer 2: observations (AI-visible facts — NOT truth)
-- =============================================================================
create table fte_observations (
  id                uuid primary key default gen_random_uuid(),
  practice_id       uuid not null references fte_practices(id) on delete restrict,
  evidence_id       uuid not null references fte_evidence(id) on delete restrict,
  observation_type  text not null check (observation_type in (
                        'payment','adjustment','contractual_adjustment',
                        'patient_responsibility','denial','allowed_amount','billed_amount',
                        'claim_identifier','payer_claim_identifier','check_eft_identifier',
                        'carc','rarc','service_date','cpt','modifier','payer','provider',
                        'summary_total','other'
                     )),
  amount            numeric(14,2),
  amount_type       text check (amount_type in (
                        'billed','allowed','paid','contractual_adjustment',
                        'patient_responsibility','denied','other'
                     )),
  claim_identifier  text,
  payer_claim_identifier text,
  payer_name        text,
  provider_name     text,
  service_date      date,
  cpt_code          text,
  modifiers         text[],
  carc_code         text,
  rarc_code         text,
  check_eft_identifier text,
  confidence_score  numeric(5,4) check (confidence_score is null
                        or (confidence_score >= 0 and confidence_score <= 1)),
  raw_value         text,           -- value exactly as seen on the evidence
  normalized_value  text,           -- normalized/cleaned representation
  page_number       integer,
  bounding_box      jsonb,          -- {x,y,w,h,page} region metadata where useful
  is_summary_row    boolean not null default false,  -- aggregate/summary, NOT a transaction
  is_superseded     boolean not null default false,  -- kept for audit; never deleted on conflict
  metadata          jsonb not null default '{}'::jsonb,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);
comment on table fte_observations is
  'AI-extracted visible facts from evidence. Observations are NOT final financial truth and '
  'must never directly update fte_financial_positions. Conflicting/duplicate/summary/late '
  'observations are retained (flagged), not overwritten.';
comment on column fte_observations.is_summary_row is
  'True when this observation is an aggregate/summary figure, not a per-claim transaction. '
  'Summary rows must not be reconciled as transaction truth.';
comment on column fte_observations.is_superseded is
  'Soft flag for audit. A later/corrected observation never deletes an earlier one; both are kept.';
comment on column fte_observations.confidence_score is
  'AI confidence in [0,1]. Low-confidence observations should be routed to fte_review_queue.';


-- =============================================================================
-- Layer 3: claims (claim identity)
-- =============================================================================
create table fte_claims (
  id                      uuid primary key default gen_random_uuid(),
  practice_id             uuid not null references fte_practices(id) on delete restrict,
  internal_claim_id       text,        -- stable internal id (human-readable)
  claim_number            text,        -- provider claim number (synthetic/redacted)
  payer_claim_number      text,        -- payer-assigned claim number (synthetic/redacted)
  patient_identifier_hash text,        -- HASH or synthetic placeholder ONLY — never raw PHI
  service_date_start      date,
  service_date_end        date,
  payer_name              text,
  status                  text not null default 'open' check (status in (
                            'open','in_review','reconciled','closed'
                          )),
  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);
comment on table fte_claims is
  'Claim identity. Patient identification is stored only as a hash or synthetic placeholder; '
  'never store raw patient names / member IDs.';
comment on column fte_claims.patient_identifier_hash is
  'One-way hash or synthetic placeholder. Raw PHI patient/member identifiers are prohibited.';


-- =============================================================================
-- Layer 3: claim_events (auditable financial events)
-- =============================================================================
create table fte_claim_events (
  id                   uuid primary key default gen_random_uuid(),
  practice_id          uuid not null references fte_practices(id) on delete restrict,
  claim_id             uuid not null references fte_claims(id) on delete cascade,
  event_type           text not null check (event_type in (
                          'claim_submitted','claim_adjudicated','payment_applied',
                          'contractual_adjustment_applied','patient_responsibility_assigned',
                          'denial_posted','short_pay_detected','appeal_filed',
                          'recovery_received','write_off_approved'
                       )),
  event_date           date,
  amount               numeric(14,2),
  amount_type          text check (amount_type in (
                          'billed','allowed','paid','contractual_adjustment',
                          'patient_responsibility','denied','recovery','write_off','other'
                       )),
  payer_name           text,
  carc_code            text,
  rarc_code            text,
  reason_category      text,
  confidence_score     numeric(5,4) check (confidence_score is null
                          or (confidence_score >= 0 and confidence_score <= 1)),
  reconciliation_status text not null default 'pending' check (reconciliation_status in (
                          'pending','reconciled','ambiguous','unbalanced'
                        )),
  metadata             jsonb not null default '{}'::jsonb,
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);
comment on table fte_claim_events is
  'Deterministic, auditable financial events derived by reconciliation. Each event MUST be '
  'linkable to supporting evidence/observations via fte_event_evidence. Financial positions '
  'are materialized from these events.';


-- =============================================================================
-- Layer 3: event_evidence (audit trail: event -> evidence and/or observation)
-- =============================================================================
create table fte_event_evidence (
  id             uuid primary key default gen_random_uuid(),
  practice_id    uuid not null references fte_practices(id) on delete restrict,
  claim_event_id uuid not null references fte_claim_events(id) on delete cascade,
  evidence_id    uuid references fte_evidence(id) on delete restrict,
  observation_id uuid references fte_observations(id) on delete restrict,
  link_role      text not null default 'supports' check (link_role in (
                    'supports','derived_from','contradicts','contextual'
                  )),
  created_at     timestamptz not null default now(),
  -- A link must point at evidence and/or an observation. This is the audit spine.
  constraint fte_event_evidence_target_present
    check (evidence_id is not null or observation_id is not null)
);
comment on table fte_event_evidence is
  'Audit trail linking every claim event back to its supporting (or contradicting) evidence '
  'and/or observations. A financial conclusion without such a link is not allowed in the ledger.';


-- =============================================================================
-- Layer 3: financial_positions (DERIVED per-claim materialized state)
-- =============================================================================
create table fte_financial_positions (
  id                            uuid primary key default gen_random_uuid(),
  practice_id                   uuid not null references fte_practices(id) on delete restrict,
  claim_id                      uuid not null unique references fte_claims(id) on delete cascade,
  billed_amount                 numeric(14,2),
  allowed_amount                numeric(14,2),
  contractual_adjustment_amount numeric(14,2),
  paid_amount                   numeric(14,2),
  denied_amount                 numeric(14,2),
  patient_responsibility_amount numeric(14,2),
  recoverable_amount            numeric(14,2),
  open_balance_amount           numeric(14,2),
  position_confidence_score     numeric(5,4) check (position_confidence_score is null
                                   or (position_confidence_score >= 0 and position_confidence_score <= 1)),
  reconciliation_status         text not null default 'incomplete' check (reconciliation_status in (
                                   'balanced','unbalanced','in_review','incomplete'
                                 )),
  last_reconciled_at            timestamptz,
  created_at                    timestamptz not null default now(),
  updated_at                    timestamptz not null default now()
);
comment on table fte_financial_positions is
  'DERIVED/materialized current financial state per claim. Computed deterministically from '
  'fte_claim_events — NOT entered as source truth and NOT computed from AI observations. '
  'Exactly one position per claim (unique claim_id).';


-- =============================================================================
-- Layer 4: denial_knowledge (editable CARC/RARC/payer intelligence)
--   practice_id NULL = global/default rule; non-NULL = practice/payer override.
-- =============================================================================
create table fte_denial_knowledge (
  id                   uuid primary key default gen_random_uuid(),
  practice_id          uuid references fte_practices(id) on delete cascade, -- NULL = global default
  carc_code            text,
  rarc_code            text,
  category             text,
  subcategory          text,
  payer_name           text,           -- NULL = applies to all payers
  recoverable          boolean not null default false,
  default_action       text,
  default_owner        text,
  appeal_window_days   integer,
  evidence_requirements text,
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);
comment on table fte_denial_knowledge is
  'Editable CARC/RARC/payer intelligence. practice_id IS NULL denotes a global default rule; '
  'a non-null practice_id denotes a practice/payer-specific override.';
comment on column fte_denial_knowledge.practice_id is
  'NULL = global default rule (readable by all tenants). Non-null = tenant-specific override.';


-- =============================================================================
-- Layer 4: contract_terms (expected payer behavior)
-- =============================================================================
create table fte_contract_terms (
  id                     uuid primary key default gen_random_uuid(),
  practice_id            uuid not null references fte_practices(id) on delete cascade,
  payer_name             text not null,
  cpt_code               text,
  modifiers              text[],
  effective_start        date,
  effective_end          date,
  expected_allowed_amount numeric(14,2),
  expected_allowed_pct   numeric(6,3),  -- e.g. 80.000 (% of billed) where amount unknown
  source_metadata        jsonb not null default '{}'::jsonb,
  created_at             timestamptz not null default now(),
  updated_at             timestamptz not null default now()
);
comment on table fte_contract_terms is
  'Expected payer behavior per CPT/modifier and effective window. Used by reasoning to detect '
  'contract variance and underpayment against fte_financial_positions.';


-- =============================================================================
-- Review / exception handling: review_queue
-- =============================================================================
create table fte_review_queue (
  id              uuid primary key default gen_random_uuid(),
  practice_id     uuid not null references fte_practices(id) on delete cascade,
  claim_id        uuid references fte_claims(id) on delete cascade,
  claim_event_id  uuid references fte_claim_events(id) on delete cascade,
  observation_id  uuid references fte_observations(id) on delete cascade,
  evidence_id     uuid references fte_evidence(id) on delete cascade,
  reason          text not null check (reason in (
                    'low_confidence_observation','conflicting_observations',
                    'missing_evidence_link','unbalanced_financial_position',
                    'suspected_duplicate','suspected_summary_row',
                    'late_retry_page_contradiction'
                  )),
  status          text not null default 'open' check (status in ('open','resolved','dismissed')),
  details         jsonb not null default '{}'::jsonb,
  resolution_note text,
  resolved_by     text,
  resolved_at     timestamptz,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);
comment on table fte_review_queue is
  'Makes uncertainty explicit. Conflicting/low-confidence/late-retry/duplicate/summary/'
  'unbalanced cases are routed here instead of silently overwriting truth. Reviewer fixes '
  'become new observations or events, not mutations of evidence.';


-- =============================================================================
-- Audit/execution metadata: analysis_runs
-- =============================================================================
create table fte_analysis_runs (
  id            uuid primary key default gen_random_uuid(),
  practice_id   uuid not null references fte_practices(id) on delete cascade,
  run_type      text not null,             -- e.g. 'reconcile_claim','ingest_evidence','seed_fixture'
  fixture_id    text,                       -- fixture/source identifier (e.g. 'ccdbe216')
  status        text not null default 'pending' check (status in (
                  'pending','running','succeeded','failed'
                )),
  inputs_hash   text,
  summary       text,
  error_details text,
  started_at    timestamptz,
  finished_at   timestamptz,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
comment on table fte_analysis_runs is
  'Execution/audit metadata for reconciliation and ingestion runs. Records what was processed, '
  'the result, and any error for traceability.';


-- =============================================================================
-- updated_at triggers (tables that have updated_at)
-- =============================================================================
create trigger trg_fte_practices_updated_at            before update on fte_practices            for each row execute function fte_set_updated_at();
create trigger trg_fte_observations_updated_at         before update on fte_observations         for each row execute function fte_set_updated_at();
create trigger trg_fte_claims_updated_at               before update on fte_claims               for each row execute function fte_set_updated_at();
create trigger trg_fte_claim_events_updated_at         before update on fte_claim_events         for each row execute function fte_set_updated_at();
create trigger trg_fte_financial_positions_updated_at  before update on fte_financial_positions  for each row execute function fte_set_updated_at();
create trigger trg_fte_denial_knowledge_updated_at     before update on fte_denial_knowledge     for each row execute function fte_set_updated_at();
create trigger trg_fte_contract_terms_updated_at       before update on fte_contract_terms       for each row execute function fte_set_updated_at();
create trigger trg_fte_review_queue_updated_at         before update on fte_review_queue         for each row execute function fte_set_updated_at();
create trigger trg_fte_analysis_runs_updated_at        before update on fte_analysis_runs        for each row execute function fte_set_updated_at();
-- (fte_evidence has no updated_at by design — immutable.)


-- =============================================================================
-- Indexes
--   Goals: claim lookup, payer lookup, evidence lookup, event reconstruction.
-- =============================================================================

-- evidence lookup
create index idx_fte_evidence_practice           on fte_evidence (practice_id);
create index idx_fte_evidence_type               on fte_evidence (practice_id, evidence_type);
create index idx_fte_evidence_parent             on fte_evidence (parent_evidence_id);
create index idx_fte_evidence_fixture            on fte_evidence (fixture_id);

-- observation -> evidence lookup + claim/payer/check lookup
create index idx_fte_obs_practice                on fte_observations (practice_id);
create index idx_fte_obs_evidence                on fte_observations (evidence_id);
create index idx_fte_obs_claim_ident             on fte_observations (practice_id, claim_identifier);
create index idx_fte_obs_payer                   on fte_observations (practice_id, payer_name);
create index idx_fte_obs_check                   on fte_observations (practice_id, check_eft_identifier);
create index idx_fte_obs_type                    on fte_observations (practice_id, observation_type);

-- claim lookup
create index idx_fte_claims_practice             on fte_claims (practice_id);
create index idx_fte_claims_claim_number         on fte_claims (practice_id, claim_number);
create index idx_fte_claims_payer_claim_number   on fte_claims (practice_id, payer_claim_number);
create index idx_fte_claims_payer                on fte_claims (practice_id, payer_name);
create index idx_fte_claims_status               on fte_claims (practice_id, status);

-- event reconstruction (per claim, chronological)
create index idx_fte_events_claim_date           on fte_claim_events (claim_id, event_date);
create index idx_fte_events_practice             on fte_claim_events (practice_id);
create index idx_fte_events_payer                on fte_claim_events (practice_id, payer_name);
create index idx_fte_events_recon_status         on fte_claim_events (reconciliation_status);

-- audit trail traversal
create index idx_fte_event_evidence_event        on fte_event_evidence (claim_event_id);
create index idx_fte_event_evidence_evidence     on fte_event_evidence (evidence_id);
create index idx_fte_event_evidence_observation  on fte_event_evidence (observation_id);

-- financial positions
create index idx_fte_positions_practice          on fte_financial_positions (practice_id);
create index idx_fte_positions_recon_status      on fte_financial_positions (practice_id, reconciliation_status);

-- knowledge / contracts
create index idx_fte_denial_carc                 on fte_denial_knowledge (carc_code, rarc_code);
create index idx_fte_denial_payer                on fte_denial_knowledge (payer_name);
create index idx_fte_contract_payer_cpt          on fte_contract_terms (practice_id, payer_name, cpt_code);

-- review / runs
create index idx_fte_review_practice_status      on fte_review_queue (practice_id, status, reason);
create index idx_fte_review_claim                on fte_review_queue (claim_id);
create index idx_fte_runs_practice               on fte_analysis_runs (practice_id, status);
create index idx_fte_runs_fixture                on fte_analysis_runs (fixture_id);


-- =============================================================================
-- Row Level Security
--   Enabled on every tenant-scoped table BEFORE any real data is loaded.
--   Policies below are deny-by-default stubs keyed on practice membership via
--   fte_accessible_practice_ids(). Supabase service_role (BYPASSRLS) is used for
--   migrations, fixtures, and validation, so it is unaffected by these stubs.
--   Wire fte_accessible_practice_ids() to a real membership lookup before
--   exposing this schema to end-user (anon/authenticated) traffic.
-- =============================================================================

alter table fte_practices            enable row level security;
alter table fte_evidence             enable row level security;
alter table fte_observations         enable row level security;
alter table fte_claims               enable row level security;
alter table fte_claim_events         enable row level security;
alter table fte_event_evidence       enable row level security;
alter table fte_financial_positions  enable row level security;
alter table fte_denial_knowledge     enable row level security;
alter table fte_contract_terms       enable row level security;
alter table fte_review_queue         enable row level security;
alter table fte_analysis_runs        enable row level security;

-- practices: a user may see only the practices they belong to.
-- Intentionally NO insert/update/delete policy. Practice creation and management
-- is handled exclusively by service_role (onboarding flow) or a future explicit
-- admin policy. Non-BYPASSRLS users cannot modify the practices table until such
-- a policy is added.
create policy fte_practices_select on fte_practices
  for select using (id in (select fte_accessible_practice_ids()));

-- evidence: append-only. SELECT + INSERT policies only — NO update/delete policy
-- (immutability enforced at the policy layer as well as by convention).
create policy fte_evidence_select on fte_evidence
  for select using (practice_id in (select fte_accessible_practice_ids()));
create policy fte_evidence_insert on fte_evidence
  for insert with check (practice_id in (select fte_accessible_practice_ids()));

-- Generic tenant-scoped read/write stubs for the remaining tables.
-- observations
create policy fte_observations_rw on fte_observations
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- claims
create policy fte_claims_rw on fte_claims
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- claim_events
create policy fte_claim_events_rw on fte_claim_events
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- event_evidence
create policy fte_event_evidence_rw on fte_event_evidence
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- financial_positions
create policy fte_financial_positions_rw on fte_financial_positions
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- contract_terms
create policy fte_contract_terms_rw on fte_contract_terms
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- review_queue
create policy fte_review_queue_rw on fte_review_queue
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
-- analysis_runs
create policy fte_analysis_runs_rw on fte_analysis_runs
  for all using (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));

-- denial_knowledge:
--   SELECT: global rows (practice_id IS NULL) are readable by all tenant users;
--           tenant-specific overrides follow practice membership.
--   INSERT/UPDATE/DELETE: restricted to tenant-scoped rows only
--           (practice_id IN accessible set). Global rows (practice_id IS NULL)
--           are intentionally NOT writable by ordinary tenant users. Global
--           denial knowledge must be seeded/updated by service_role, postgres,
--           or a future explicit admin policy — never by tenant-level auth.
create policy fte_denial_knowledge_select on fte_denial_knowledge
  for select using (
    practice_id is null
    or practice_id in (select fte_accessible_practice_ids())
  );
create policy fte_denial_knowledge_tenant_insert on fte_denial_knowledge
  for insert
  with check (practice_id in (select fte_accessible_practice_ids()));
create policy fte_denial_knowledge_tenant_update on fte_denial_knowledge
  for update
  using  (practice_id in (select fte_accessible_practice_ids()))
  with check (practice_id in (select fte_accessible_practice_ids()));
create policy fte_denial_knowledge_tenant_delete on fte_denial_knowledge
  for delete
  using  (practice_id in (select fte_accessible_practice_ids()));

-- =============================================================================
-- End of migration 001.
-- =============================================================================

commit;

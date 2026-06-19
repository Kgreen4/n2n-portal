-- =============================================================================
-- Financial Truth Engine — Schema Validation
-- tests/validate_schema.sql
--
-- Asserts the structural guarantees of migration 001. Self-contained:
--   * Catalog checks (RLS, FK isolation) read system catalogs — no data needed.
--   * Behavioral checks INSERT synthetic rows under a throwaway test practice,
--     assert invariants, then ROLLBACK so nothing persists.
--
-- Run AFTER applying migrations/001_create_financial_truth_schema.sql:
--     psql "$DATABASE_URL" -f tests/validate_schema.sql
--
-- Output: NOTICE lines for each passing check; any failure RAISEs EXCEPTION and
-- aborts (the wrapping transaction rolls back regardless).
-- Run as a role that can insert (Supabase service_role / postgres; both BYPASSRLS).
-- =============================================================================

begin;

-- -----------------------------------------------------------------------------
-- Check 1: RLS is enabled on every fte_ table.
-- -----------------------------------------------------------------------------
do $$
declare
  missing text;
begin
  select string_agg(c.relname, ', ')
    into missing
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'r'
    and c.relname like 'fte\_%'
    and c.relrowsecurity = false;

  if missing is not null then
    raise exception 'FAIL [RLS]: RLS not enabled on: %', missing;
  end if;
  raise notice 'PASS [1/6] RLS enabled on all fte_ tables';
end $$;

-- -----------------------------------------------------------------------------
-- Check 2: no fte_ table has a foreign key to a non-fte_ table
--          (proves isolation from legacy eob_* and any other schema).
-- -----------------------------------------------------------------------------
do $$
declare
  bad text;
begin
  select string_agg(format('%s.%s -> %s', src.relname, con.conname, tgt.relname), '; ')
    into bad
  from pg_constraint con
  join pg_class src on src.oid = con.conrelid
  join pg_class tgt on tgt.oid = con.confrelid
  where con.contype = 'f'
    and src.relname like 'fte\_%'
    and tgt.relname not like 'fte\_%';

  if bad is not null then
    raise exception 'FAIL [ISOLATION]: fte_ tables reference non-fte_ tables: %', bad;
  end if;
  raise notice 'PASS [2/6] no fte_ FK references any non-fte_ (eob_/other) table';
end $$;

-- -----------------------------------------------------------------------------
-- Check 3: every claim event can link to evidence AND/OR an observation,
--          and the audit constraint forbids a link pointing at neither.
-- -----------------------------------------------------------------------------
do $$
declare
  has_constraint boolean;
begin
  select exists (
    select 1 from pg_constraint
    where conname = 'fte_event_evidence_target_present'
  ) into has_constraint;

  if not has_constraint then
    raise exception 'FAIL [AUDIT]: fte_event_evidence audit constraint missing';
  end if;
  raise notice 'PASS [3/6] event_evidence audit link constraint present';
end $$;

-- -----------------------------------------------------------------------------
-- Check 4: financial positions are claim-scoped (unique claim_id) and
--          practice-scoped (NOT NULL practice_id FK).
-- -----------------------------------------------------------------------------
do $$
declare
  has_unique boolean;
  practice_notnull boolean;
begin
  select exists (
    select 1
    from pg_constraint con
    join pg_class c on c.oid = con.conrelid
    where c.relname = 'fte_financial_positions'
      and con.contype = 'u'
      and (select array_agg(att.attname::text order by att.attname)
             from unnest(con.conkey) k
             join pg_attribute att on att.attrelid = con.conrelid and att.attnum = k)
          = array['claim_id']
  ) into has_unique;

  select a.attnotnull
    into practice_notnull
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  where c.relname = 'fte_financial_positions' and a.attname = 'practice_id';

  if not has_unique then
    raise exception 'FAIL [POSITION]: fte_financial_positions missing unique(claim_id)';
  end if;
  if not coalesce(practice_notnull, false) then
    raise exception 'FAIL [POSITION]: fte_financial_positions.practice_id must be NOT NULL';
  end if;
  raise notice 'PASS [4/6] financial_positions are claim-scoped (unique) and practice-scoped';
end $$;

-- -----------------------------------------------------------------------------
-- Check 5 (behavioral): inserting an observation does NOT create a financial
--          position, AND review_queue can capture the required reason types.
--          Rolled back at end of file.
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice  uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_evidence  uuid;
  v_obs       uuid;
  v_claim     uuid;
  v_event     uuid;
  position_count integer;
  r text;
begin
  insert into fte_practices (id, name) values (v_practice, 'VALIDATION TEMP PRACTICE');

  insert into fte_evidence (practice_id, evidence_type, fixture_id, raw_text)
    values (v_practice, 'page', 'validation', '[SYNTHETIC] validation page')
    returning id into v_evidence;

  -- Insert an observation. This must NOT auto-create any financial position.
  insert into fte_observations (practice_id, evidence_id, observation_type, amount, amount_type, confidence_score)
    values (v_practice, v_evidence, 'payment', 100.00, 'paid', 0.90)
    returning id into v_obs;

  select count(*) into position_count
    from fte_financial_positions where practice_id = v_practice;
  if position_count <> 0 then
    raise exception 'FAIL [OBS->POSITION]: observation insert created % financial position(s); observations must not mutate truth', position_count;
  end if;

  -- A claim + event + audit link can be built, and a derived position is explicit.
  insert into fte_claims (practice_id, claim_number, payer_name, status)
    values (v_practice, 'VAL-CLM-1', 'Validation Payer', 'in_review')
    returning id into v_claim;

  insert into fte_claim_events (practice_id, claim_id, event_type, amount, amount_type, reconciliation_status)
    values (v_practice, v_claim, 'payment_applied', 100.00, 'paid', 'reconciled')
    returning id into v_event;

  -- event_evidence must accept a link to evidence and/or observation.
  insert into fte_event_evidence (practice_id, claim_event_id, evidence_id, observation_id, link_role)
    values (v_practice, v_event, v_evidence, v_obs, 'supports');

  -- the audit constraint must REJECT a link to neither.
  begin
    insert into fte_event_evidence (practice_id, claim_event_id, evidence_id, observation_id)
      values (v_practice, v_event, null, null);
    raise exception 'FAIL [AUDIT]: event_evidence accepted a link with no evidence and no observation';
  exception when check_violation then
    null; -- expected
  end;

  -- review_queue must capture each required reason type.
  foreach r in array array[
    'low_confidence_observation','conflicting_observations','missing_evidence_link',
    'unbalanced_financial_position','suspected_duplicate','suspected_summary_row',
    'late_retry_page_contradiction'
  ] loop
    insert into fte_review_queue (practice_id, reason, details)
      values (v_practice, r, jsonb_build_object('check', r));
  end loop;

  raise notice 'PASS [5/6] observation insert created 0 positions; audit link enforced; review_queue captures all 7 reasons';
end $$;

-- -----------------------------------------------------------------------------
-- Check 6 (behavioral): a financial position is claim+practice scoped and is
--          stored independently of observations (derived layer).
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_claim    uuid;
begin
  select id into v_claim from fte_claims where practice_id = v_practice limit 1;

  insert into fte_financial_positions
    (practice_id, claim_id, billed_amount, paid_amount, open_balance_amount,
     reconciliation_status, last_reconciled_at)
    values (v_practice, v_claim, 100.00, 100.00, 0.00, 'balanced', now());

  if not exists (
    select 1 from fte_financial_positions
    where practice_id = v_practice and claim_id = v_claim
  ) then
    raise exception 'FAIL [POSITION]: derived position not stored as claim+practice scoped';
  end if;
  raise notice 'PASS [6/6] derived financial position is claim+practice scoped';
end $$;

-- Discard all validation inserts. Catalog checks above persist nothing.
rollback;

\echo ''
\echo '==================================================================='
\echo ' FTE schema validation complete — all checks passed if no EXCEPTION'
\echo ' was raised above. (All validation inserts were rolled back.)'
\echo '==================================================================='

-- =============================================================================
-- Financial Truth Engine — Schema Validation
-- tests/validate_schema.sql
--
-- Asserts the structural guarantees of migrations 001, 002, and 012. Self-contained:
--   * Catalog checks (RLS, FK isolation) read system catalogs — no data needed.
--   * Behavioral checks INSERT synthetic rows under a throwaway test practice,
--     assert invariants, then ROLLBACK so nothing persists.
--
-- Run AFTER applying all migrations in order:
--     psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
--     psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql
--     psql "$DATABASE_URL" -f tests/validate_schema.sql
--
-- Output: NOTICE lines for each passing check; any failure RAISEs EXCEPTION and
-- aborts (the wrapping transaction rolls back regardless).
-- Run as a role that can insert (Supabase service_role / postgres; both BYPASSRLS).
-- =============================================================================

begin;

-- -----------------------------------------------------------------------------
-- Check 1: RLS is enabled on every fte_ table (including fte_review_resolutions).
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
  raise notice 'PASS [1/20] RLS enabled on all fte_ tables (incl. fte_review_resolutions)';
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
  raise notice 'PASS [2/20] no fte_ FK references any non-fte_ (eob_/other) table';
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
  raise notice 'PASS [3/20] event_evidence audit link constraint present';
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
  raise notice 'PASS [4/20] financial_positions are claim-scoped (unique) and practice-scoped';
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

  raise notice 'PASS [5/20] observation insert created 0 positions; audit link enforced; review_queue captures all 7 reasons';
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
  raise notice 'PASS [6/20] derived financial position is claim+practice scoped';
end $$;

-- =============================================================================
-- Migration 002 checks — fte_review_resolutions
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check 7 (catalog): fte_review_resolutions exists with all 22 required columns,
--          and the target_type CHECK constraint is present.
-- -----------------------------------------------------------------------------
do $$
declare
  missing_cols           text;
  has_target_type_check  boolean;
  target_type_nullable   text;
  has_target_present     boolean;
begin
  select string_agg(required_col, ', ' order by required_col)
    into missing_cols
  from unnest(array[
    'id', 'practice_id', 'claim_id', 'observation_id', 'evidence_id',
    'action', 'target_type', 'target_event_type', 'target_review_reason',
    'target_claim_number', 'source_review_queue_id', 'source_claim_event_id',
    'source_position_id', 'resolved_by', 'resolved_at', 'notes',
    'corrected_value', 'corrected_identifier', 'is_superseded',
    'metadata', 'created_at', 'updated_at'
  ]) as required_col
  where required_col not in (
    select column_name
    from information_schema.columns
    where table_schema = 'public' and table_name = 'fte_review_resolutions'
  );

  if missing_cols is not null then
    raise exception 'FAIL [RESOLUTIONS COLS]: fte_review_resolutions missing columns: %', missing_cols;
  end if;

  -- target_type CHECK constraint must be present on the table.
  select exists (
    select 1 from pg_constraint con
    join pg_class c on c.oid = con.conrelid
    where c.relname = 'fte_review_resolutions'
      and con.contype = 'c'
      and pg_get_constraintdef(con.oid) like '%target_type%'
  ) into has_target_type_check;

  if not has_target_type_check then
    raise exception
      'FAIL [RESOLUTIONS TARGET_TYPE]: target_type CHECK constraint missing on '
      'fte_review_resolutions';
  end if;

  -- target_type must be NOT NULL.
  select is_nullable
    into target_type_nullable
  from information_schema.columns
  where table_schema = 'public'
    and table_name   = 'fte_review_resolutions'
    and column_name  = 'target_type';

  if target_type_nullable <> 'NO' then
    raise exception
      'FAIL [RESOLUTIONS TARGET_TYPE NOTNULL]: target_type must be NOT NULL, '
      'got is_nullable=%', target_type_nullable;
  end if;

  -- fte_review_resolutions_target_present must exist.
  select exists (
    select 1 from pg_constraint
    where conname  = 'fte_review_resolutions_target_present'
      and contype  = 'c'
  ) into has_target_present;

  if not has_target_present then
    raise exception
      'FAIL [RESOLUTIONS TARGET_PRESENT]: fte_review_resolutions_target_present '
      'CHECK constraint missing — at least one of claim_id, observation_id, '
      'evidence_id, source_review_queue_id, source_claim_event_id, '
      'source_position_id must be non-null';
  end if;

  raise notice
    'PASS [7/20] fte_review_resolutions: 22 required columns present; '
    'target_type NOT NULL and CHECK present; '
    'target-present CHECK (fte_review_resolutions_target_present) present';
end $$;

-- -----------------------------------------------------------------------------
-- Check 8 (catalog): stable FK constraints exist on practice_id, claim_id,
--          observation_id, and evidence_id, each pointing to the correct table.
-- -----------------------------------------------------------------------------
do $$
declare
  missing_fks text;
begin
  select string_agg(expected.col || ' -> ' || expected.tgt, ', ' order by expected.col)
    into missing_fks
  from (
    values
      ('practice_id',    'fte_practices'),
      ('claim_id',       'fte_claims'),
      ('observation_id', 'fte_observations'),
      ('evidence_id',    'fte_evidence')
  ) as expected(col, tgt)
  where not exists (
    select 1
    from pg_constraint con
    join pg_class src on src.oid = con.conrelid
    join pg_class tgt on tgt.oid = con.confrelid
    join pg_attribute att on att.attrelid = con.conrelid
                         and att.attnum = any(con.conkey)
    where con.contype = 'f'
      and src.relname = 'fte_review_resolutions'
      and tgt.relname = expected.tgt
      and att.attname = expected.col
  );

  if missing_fks is not null then
    raise exception 'FAIL [RESOLUTIONS STABLE FK]: missing FK constraints: %', missing_fks;
  end if;
  raise notice 'PASS [8/20] stable FK constraints present on practice_id, claim_id, observation_id, evidence_id';
end $$;

-- -----------------------------------------------------------------------------
-- Check 9 (catalog): no FK from fte_review_resolutions points to any volatile
--          Phase-0 table. fte_review_queue, fte_claim_events, and
--          fte_financial_positions are deleted and regenerated by the reconciler
--          on every call. A hard FK to these tables would either block Phase 0's
--          DELETE (RESTRICT) or destroy reviewer history (CASCADE).
-- -----------------------------------------------------------------------------
do $$
declare
  bad_targets text;
begin
  select string_agg(tgt.relname, ', ' order by tgt.relname)
    into bad_targets
  from pg_constraint con
  join pg_class src on src.oid = con.conrelid
  join pg_class tgt on tgt.oid = con.confrelid
  where con.contype = 'f'
    and src.relname = 'fte_review_resolutions'
    and tgt.relname in (
      'fte_review_queue', 'fte_claim_events', 'fte_financial_positions'
    );

  if bad_targets is not null then
    raise exception
      'FAIL [RESOLUTIONS VOLATILE FK]: FK constraints found pointing to volatile '
      'Phase-0 tables (would block DELETE or cascade reviewer history): %', bad_targets;
  end if;
  raise notice
    'PASS [9/20] no FK from fte_review_resolutions to fte_review_queue, '
    'fte_claim_events, or fte_financial_positions';
end $$;

-- -----------------------------------------------------------------------------
-- Check 10 (behavioral): a valid synthetic insert succeeds (is_superseded
--           defaults false, all 3 action vocabulary categories are accepted).
--           Demonstrates reviewer decisions persist inside the transaction.
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice   uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_claim      uuid;
  v_obs        uuid;
  v_superseded boolean;
begin
  select id into v_claim from fte_claims       where practice_id = v_practice limit 1;
  select id into v_obs   from fte_observations where practice_id = v_practice limit 1;

  -- Valid: payment/event-level action — confirm is_superseded defaults false.
  insert into fte_review_resolutions (
    practice_id, claim_id, observation_id,
    action, target_type, target_event_type, resolved_by
  ) values (
    v_practice, v_claim, v_obs,
    'confirm_payment_event', 'payment_event', 'payment_applied', 'test_runner'
  ) returning is_superseded into v_superseded;

  if v_superseded is distinct from false then
    raise exception
      'FAIL [RESOLUTIONS DEFAULT]: is_superseded should default to false, got %',
      v_superseded;
  end if;

  -- Valid: observation-level action — reject_observation requires observation_id
  -- (migration 003 constraint fte_review_resolutions_obs_action_needs_obs_id); the
  -- volatile snapshot UUID additionally satisfies the target-present CHECK.
  insert into fte_review_resolutions (practice_id, action, target_type, observation_id, source_claim_event_id)
    values (v_practice, 'reject_observation', 'observation', v_obs, 'eeeeeeee-0000-4000-8000-0000000000ce');

  -- Valid: position-level action — confirm_short_pay requires a stable claim anchor
  -- (migration 006 constraint fte_review_resolutions_confirm_shortpay_needs_claim_id);
  -- the volatile snapshot UUID additionally satisfies the target-present CHECK.
  insert into fte_review_resolutions (practice_id, action, target_type, claim_id, source_position_id)
    values (v_practice, 'confirm_short_pay', 'position', v_claim, 'eeeeeeee-0000-4000-8000-0000000000c0');

  -- Missing all targets beyond practice_id — must be rejected by target-present CHECK.
  begin
    insert into fte_review_resolutions (practice_id, action, target_type)
      values (v_practice, 'confirm_observation', 'observation');
    raise exception
      'FAIL [RESOLUTIONS TARGET_PRESENT BEHAVIOR]: row with no targets was accepted; '
      'fte_review_resolutions_target_present CHECK missing or incomplete';
  exception when check_violation then
    null; -- expected
  end;

  -- Invalid action — include valid target_type and snapshot target so action CHECK fires.
  begin
    insert into fte_review_resolutions (practice_id, action, target_type, source_claim_event_id)
      values (v_practice, 'not_a_real_action', 'observation', 'eeeeeeee-0000-4000-8000-0000000000ce');
    raise exception
      'FAIL [RESOLUTIONS ACTION CHECK]: invalid action was accepted; '
      'CHECK constraint missing or incomplete';
  exception when check_violation then
    null; -- expected
  end;

  raise notice
    'PASS [10/20] valid synthetic inserts succeeded (all 3 action categories, with '
    'volatile snapshot targets); is_superseded defaults false; '
    'target-absent row rejected (check_violation); invalid action rejected (check_violation)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 11 (behavioral): invalid target_type is rejected by CHECK constraint;
--           invalid non-object metadata (JSON array) is rejected by CHECK constraint.
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
begin
  -- Invalid target_type — include a snapshot target so the target_type CHECK fires cleanly.
  begin
    insert into fte_review_resolutions (practice_id, action, target_type, source_claim_event_id)
      values (v_practice, 'confirm_observation', 'not_a_valid_type', 'eeeeeeee-0000-4000-8000-0000000000ce');
    raise exception
      'FAIL [RESOLUTIONS TARGET_TYPE CHECK]: invalid target_type was accepted; '
      'CHECK constraint missing or incomplete';
  exception when check_violation then
    null; -- expected
  end;

  -- Invalid non-object metadata (array) — include valid target_type and snapshot target
  -- so the metadata CHECK fires (not NOT NULL or target-present).
  begin
    insert into fte_review_resolutions (practice_id, action, target_type, source_claim_event_id, metadata)
      values (v_practice, 'confirm_observation', 'observation', 'eeeeeeee-0000-4000-8000-0000000000ce', '[1,2,3]'::jsonb);
    raise exception
      'FAIL [RESOLUTIONS METADATA CHECK]: non-object JSON metadata was accepted; '
      'check (jsonb_typeof(metadata) = ''object'') constraint missing';
  exception when check_violation then
    null; -- expected
  end;

  raise notice
    'PASS [11/20] invalid target_type rejected (check_violation); '
    'invalid non-object metadata (array) rejected (check_violation)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 12 (catalog): metadata column has a jsonb_typeof(metadata) = 'object'
--           CHECK constraint, ensuring only JSON objects are accepted.
-- -----------------------------------------------------------------------------
do $$
declare
  has_metadata_check boolean;
begin
  select exists (
    select 1 from pg_constraint con
    join pg_class c on c.oid = con.conrelid
    where c.relname = 'fte_review_resolutions'
      and con.contype = 'c'
      and pg_get_constraintdef(con.oid) like '%jsonb_typeof%'
      and pg_get_constraintdef(con.oid) like '%metadata%'
  ) into has_metadata_check;

  if not has_metadata_check then
    raise exception
      'FAIL [RESOLUTIONS METADATA CATALOG]: jsonb_typeof(metadata) = ''object'' '
      'CHECK constraint not found on fte_review_resolutions';
  end if;
  raise notice
    'PASS [12/20] metadata CHECK (jsonb_typeof(metadata) = ''object'') constraint '
    'present in catalog';
end $$;

-- -----------------------------------------------------------------------------
-- Check 13 (catalog): all 4 expected indexes exist on fte_review_resolutions,
--           including the audit/chronological idx_fte_review_resolutions_resolved_at.
-- -----------------------------------------------------------------------------
do $$
declare
  missing_indexes text;
begin
  select string_agg(expected_idx, ', ' order by expected_idx)
    into missing_indexes
  from unnest(array[
    'idx_fte_resolutions_practice_active',
    'idx_fte_resolutions_claim_action',
    'idx_fte_resolutions_observation_action',
    'idx_fte_review_resolutions_resolved_at'
  ]) as expected_idx
  where expected_idx not in (
    select indexname from pg_indexes where tablename = 'fte_review_resolutions'
  );

  if missing_indexes is not null then
    raise exception
      'FAIL [RESOLUTIONS INDEXES]: missing expected indexes: %', missing_indexes;
  end if;
  raise notice
    'PASS [13/20] all 4 indexes present on fte_review_resolutions '
    '(practice_active, claim_action, observation_action, resolved_at)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 14 (catalog): RLS is explicitly confirmed on fte_review_resolutions.
--           Check 1 covers all fte_ tables generically; this check makes the
--           coverage unambiguous for the new table.
-- -----------------------------------------------------------------------------
do $$
declare
  has_rls boolean;
begin
  select c.relrowsecurity
    into has_rls
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relname = 'fte_review_resolutions';

  if not coalesce(has_rls, false) then
    raise exception
      'FAIL [RESOLUTIONS RLS]: RLS not enabled on fte_review_resolutions';
  end if;
  raise notice
    'PASS [14/20] RLS explicitly confirmed on fte_review_resolutions '
    '(policy: fte_review_resolutions_rw)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 15 (catalog): updated_at trigger exists on fte_review_resolutions.
-- -----------------------------------------------------------------------------
do $$
declare
  has_trigger boolean;
begin
  select exists (
    select 1
    from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where c.relname = 'fte_review_resolutions'
      and t.tgname = 'trg_fte_review_resolutions_updated_at'
      and not t.tgisinternal
  ) into has_trigger;

  if not has_trigger then
    raise exception
      'FAIL [RESOLUTIONS TRIGGER]: trg_fte_review_resolutions_updated_at '
      'not found on fte_review_resolutions';
  end if;
  raise notice 'PASS [15/20] updated_at trigger present on fte_review_resolutions';
end $$;

-- =============================================================================
-- Migration 012 checks — denial lifecycle schema support (Task 017B)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check 16 (catalog): the denial-lifecycle columns added by migration 012 exist:
--          fte_review_resolutions.lifecycle_amount and
--          fte_financial_positions.recovered_amount / written_off_amount.
-- -----------------------------------------------------------------------------
do $$
declare
  missing_cols text;
begin
  select string_agg(t.tbl || '.' || t.col, ', ' order by t.tbl, t.col)
    into missing_cols
  from (values
    ('fte_review_resolutions',  'lifecycle_amount'),
    ('fte_financial_positions', 'recovered_amount'),
    ('fte_financial_positions', 'written_off_amount')
  ) as t(tbl, col)
  where not exists (
    select 1 from information_schema.columns c
    where c.table_schema = 'public' and c.table_name = t.tbl and c.column_name = t.col
  );

  if missing_cols is not null then
    raise exception 'FAIL [LIFECYCLE COLS]: missing migration-012 columns: %', missing_cols;
  end if;
  raise notice
    'PASS [16/20] denial-lifecycle columns present '
    '(review_resolutions.lifecycle_amount; positions.recovered_amount/written_off_amount)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 17 (behavioral): the three new lifecycle actions are accepted with valid
--          shapes — file_appeal (no amount), record_recovery (positive amount),
--          approve_write_off (positive amount).
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_claim    uuid;
begin
  select id into v_claim from fte_claims where practice_id = v_practice limit 1;

  insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by)
    values (v_practice, v_claim, 'file_appeal', 'claim', null, 'test_runner');

  insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by)
    values (v_practice, v_claim, 'record_recovery', 'claim', 100.00, 'test_runner');

  insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount, resolved_by)
    values (v_practice, v_claim, 'approve_write_off', 'claim', 50.00, 'test_runner');

  raise notice
    'PASS [17/20] lifecycle actions accepted (file_appeal no-amount; '
    'record_recovery/approve_write_off positive amount)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 18 (behavioral): invalid lifecycle shapes are rejected by CHECK —
--          record_recovery / approve_write_off with NULL or non-positive amount,
--          and file_appeal carrying an amount.
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_claim    uuid;
begin
  select id into v_claim from fte_claims where practice_id = v_practice limit 1;

  begin
    insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount)
      values (v_practice, v_claim, 'record_recovery', 'claim', null);
    raise exception 'FAIL [LIFECYCLE SHAPE]: record_recovery accepted NULL lifecycle_amount';
  exception when check_violation then null; end;

  begin
    insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount)
      values (v_practice, v_claim, 'record_recovery', 'claim', 0);
    raise exception 'FAIL [LIFECYCLE SHAPE]: record_recovery accepted non-positive lifecycle_amount';
  exception when check_violation then null; end;

  begin
    insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount)
      values (v_practice, v_claim, 'approve_write_off', 'claim', null);
    raise exception 'FAIL [LIFECYCLE SHAPE]: approve_write_off accepted NULL lifecycle_amount';
  exception when check_violation then null; end;

  begin
    insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount)
      values (v_practice, v_claim, 'approve_write_off', 'claim', -5);
    raise exception 'FAIL [LIFECYCLE SHAPE]: approve_write_off accepted negative lifecycle_amount';
  exception when check_violation then null; end;

  begin
    insert into fte_review_resolutions (practice_id, claim_id, action, target_type, lifecycle_amount)
      values (v_practice, v_claim, 'file_appeal', 'claim', 10.00);
    raise exception 'FAIL [LIFECYCLE SHAPE]: file_appeal accepted a non-NULL lifecycle_amount';
  exception when check_violation then null; end;

  raise notice
    'PASS [18/20] invalid lifecycle shapes rejected '
    '(recovery/write_off NULL|<=0; file_appeal with amount)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 19 (behavioral): pre-existing resolution actions remain valid after the
--          action-vocabulary CHECK was rebuilt — regression on mark_duplicate
--          (migration 003 / Task 009C shape must be unbroken).
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_evidence uuid;
  v_obs      uuid;
  v_obs2     uuid;
begin
  select id into v_obs from fte_observations where practice_id = v_practice limit 1;
  select evidence_id into v_evidence from fte_observations where id = v_obs;

  insert into fte_observations (practice_id, evidence_id, observation_type, amount, amount_type, confidence_score)
    values (v_practice, v_evidence, 'payment', 100.00, 'paid', 0.90)
    returning id into v_obs2;

  -- mark_duplicate must still be accepted with its migration-003 shape.
  insert into fte_review_resolutions (practice_id, action, target_type, observation_id, target_observation_id)
    values (v_practice, 'mark_duplicate', 'observation', v_obs, v_obs2);

  raise notice
    'PASS [19/20] pre-existing actions still valid after vocabulary rebuild (mark_duplicate accepted)';
end $$;

-- -----------------------------------------------------------------------------
-- Check 20 (behavioral): the new position columns reject negative values.
-- -----------------------------------------------------------------------------
do $$
declare
  v_practice uuid := 'ffffffff-0000-4000-8000-0000000000aa';
  v_claim2   uuid;
begin
  insert into fte_claims (practice_id, claim_number, payer_name, status)
    values (v_practice, 'VAL-CLM-2', 'Validation Payer', 'in_review')
    returning id into v_claim2;

  begin
    insert into fte_financial_positions (practice_id, claim_id, recovered_amount, reconciliation_status)
      values (v_practice, v_claim2, -1.00, 'in_review');
    raise exception 'FAIL [LIFECYCLE POSITION]: recovered_amount accepted a negative value';
  exception when check_violation then null; end;

  begin
    insert into fte_financial_positions (practice_id, claim_id, written_off_amount, reconciliation_status)
      values (v_practice, v_claim2, -1.00, 'in_review');
    raise exception 'FAIL [LIFECYCLE POSITION]: written_off_amount accepted a negative value';
  exception when check_violation then null; end;

  raise notice
    'PASS [20/20] new position columns reject negative values (recovered_amount, written_off_amount)';
end $$;

-- Discard all validation inserts. Catalog checks above persist nothing.
rollback;

\echo ''
\echo '==================================================================='
\echo ' FTE schema validation complete — all checks passed if no EXCEPTION'
\echo ' was raised above. (All validation inserts were rolled back.)'
\echo '==================================================================='

-- =============================================================================
-- Financial Truth Engine — Full Validation Runner
-- tests/run_all_validations.sql
--
-- PSQL ONLY — not for the Supabase SQL Editor.
-- The \i metacommands below are psql-specific and will cause a syntax error
-- if pasted into the Supabase SQL Editor.
-- Supabase users: follow tests/RUNBOOK.md for the manual paste-and-run sequence.
--
-- Usage (from the repo root, after first-time setup):
--
--   psql "$DATABASE_URL" -f tests/run_all_validations.sql
--
-- Prerequisites (first-time setup — run once per disposable DB):
--   psql "$DATABASE_URL" -f migrations/001_create_financial_truth_schema.sql
--   psql "$DATABASE_URL" -f migrations/002_add_review_resolutions.sql
--   psql "$DATABASE_URL" -f migrations/003_add_observation_resolution_target.sql
--   psql "$DATABASE_URL" -f migrations/004_corrected_value_constraints.sql
--   psql "$DATABASE_URL" -f migrations/005_dismiss_short_pay_constraints.sql
--   psql "$DATABASE_URL" -f migrations/006_confirm_short_pay_constraints.sql
--   psql "$DATABASE_URL" -f migrations/007_request_more_evidence_constraints.sql
--   psql "$DATABASE_URL" -f migrations/008_mark_position_needs_correction_constraints.sql
--   psql "$DATABASE_URL" -f migrations/009_mark_position_resolved_constraints.sql
--   psql "$DATABASE_URL" -f migrations/010_reject_payment_event_constraints.sql
--   psql "$DATABASE_URL" -f migrations/011_assert_check_identity_constraints.sql
--   psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
--   psql "$DATABASE_URL" -f reconciler/fte_explain_claim.sql
--   psql "$DATABASE_URL" -f reconciler/fte_mock_extract_observations.sql
--
-- Migrations are one-time DDL. Do not include them here — rerunning them
-- against an already-migrated DB causes duplicate-object errors.
-- The reconciler functions (CREATE OR REPLACE) are safe to rerun; do so
-- whenever fte_reconcile.sql or fte_explain_claim.sql changes.
--
-- Expected output: 298 PASS NOTICE lines across twenty-three suites.
-- A FAIL raises an EXCEPTION that aborts the current suite's transaction.
-- Subsequent \i calls still execute — scroll up to find any EXCEPTION output.
--
-- No credentials or connection strings are stored here.
-- All fixtures are synthetic. No PHI. No production data.
-- =============================================================================

\echo ''
\echo '=== FTE Validation Runner ==='
\echo ''

-- ---------------------------------------------------------------------------
-- Fixtures (commit — not wrapped in ROLLBACK)
-- ---------------------------------------------------------------------------

\echo 'Loading synthetic fixture: ccdbe216'
\i fixtures/synthetic_ccdbe216_failure_modes.sql

\echo 'Loading synthetic fixture: 96c5c357'
\i fixtures/synthetic_96c5c357_failure_modes.sql

\echo 'Loading synthetic fixture: phase3a_extraction'
\i fixtures/synthetic_phase3a_extraction_fixture.sql

\echo 'Loading synthetic fixture: phase3b_mock_extractor'
\i fixtures/synthetic_phase3b_mock_extractor_fixture.sql

-- ---------------------------------------------------------------------------
-- Validation suites (each wraps in ROLLBACK — nothing persists)
-- ---------------------------------------------------------------------------

\echo ''
\echo '--- validate_schema ---'
\i tests/validate_schema.sql

\echo ''
\echo '--- validate_reconciler ---'
\i tests/validate_reconciler.sql

\echo ''
\echo '--- validate_reconciler_incomplete_status ---'
\i tests/validate_reconciler_incomplete_status.sql

\echo ''
\echo '--- validate_reconciler_e2 ---'
\i tests/validate_reconciler_e2.sql

\echo ''
\echo '--- validate_reconciler_denial ---'
\i tests/validate_reconciler_denial.sql

\echo ''
\echo '--- validate_reconciler_recoverable ---'
\i tests/validate_reconciler_recoverable.sql

\echo ''
\echo '--- validate_reconciler_denial_lifecycle ---'
\i tests/validate_reconciler_denial_lifecycle.sql

\echo ''
\echo '--- validate_review_resolution ---'
\i tests/validate_review_resolution.sql

\echo ''
\echo '--- validate_observation_resolution ---'
\i tests/validate_observation_resolution.sql

\echo ''
\echo '--- validate_corrected_value ---'
\i tests/validate_corrected_value.sql

\echo ''
\echo '--- validate_corrected_value_supersession ---'
\i tests/validate_corrected_value_supersession.sql

\echo ''
\echo '--- validate_corrected_contractual_adjustment ---'
\i tests/validate_corrected_contractual_adjustment.sql

\echo ''
\echo '--- validate_corrected_billed_amount ---'
\i tests/validate_corrected_billed_amount.sql

\echo ''
\echo '--- validate_dismiss_short_pay ---'
\i tests/validate_dismiss_short_pay.sql

\echo ''
\echo '--- validate_confirm_short_pay ---'
\i tests/validate_confirm_short_pay.sql

\echo ''
\echo '--- validate_request_more_evidence ---'
\i tests/validate_request_more_evidence.sql

\echo ''
\echo '--- validate_mark_position_needs_correction ---'
\i tests/validate_mark_position_needs_correction.sql

\echo ''
\echo '--- validate_mark_position_resolved ---'
\i tests/validate_mark_position_resolved.sql

\echo ''
\echo '--- validate_reject_payment_event ---'
\i tests/validate_reject_payment_event.sql

\echo ''
\echo '--- validate_assert_check_identity ---'
\i tests/validate_assert_check_identity.sql

\echo ''
\echo '--- validate_extraction_pipeline ---'
\i tests/validate_extraction_pipeline.sql

\echo ''
\echo '--- validate_explain_claim ---'
\i tests/validate_explain_claim.sql

\echo ''
\echo '--- validate_mock_extraction ---'
\i tests/validate_mock_extraction.sql

\echo ''
\echo '=== All suites complete. Expected: 298 PASS checks. ==='
\echo ''

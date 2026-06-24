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
--   psql "$DATABASE_URL" -f reconciler/fte_reconcile.sql
--
-- Migrations are one-time DDL. Do not include them here — rerunning them
-- against an already-migrated DB causes duplicate-object errors.
-- The reconciler (CREATE OR REPLACE FUNCTION) is safe to rerun; do so
-- whenever fte_reconcile.sql changes.
--
-- Expected output: 81 PASS NOTICE lines across nine suites.
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
\echo '=== All suites complete. Expected: 81 PASS checks. ==='
\echo ''

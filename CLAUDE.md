# N2N Portal — Claude Code Project Context

## Current Active Lane
- Active workstream: Financial Truth Engine (`financial-truth-engine/`)
- Current checkpoint: complete through Task 015A / PR #46 (E1/E2/denial/recoverable/explain/fixture-hardening arcs merged)
- Current main HEAD: 3d9ac76
- Validation baseline: 279 total PASS = 264 counted + 15 validate_schema
- Open blockers: none
- Next planned task: none scheduled — awaiting next written task spec
- Do not start a task without a written task spec and Keith approval

## Hard Safety Rails
- Do not touch legacy EOB extraction code unless explicitly instructed
- Do not connect FTE tasks to the legacy EOB Supabase project
- Do not use production data, PHI, raw real PDFs, real patient identifiers, real payer exports, or real check numbers in FTE fixtures/tests
- Do not ask for or expose credentials, DB URLs, project URLs, service-role keys, anon keys, API keys, or connection strings
- Use synthetic fixtures and disposable Supabase validation only
- No live AI calls unless a future approved task explicitly allows it
- No Edge Functions or UI unless a future approved task explicitly allows it

## Git / Remote Rules
- Work from clean `main`
- Use feature branches
- Push explicitly with `git push github HEAD:<branch>`
- Do not use plain `git push`
- Do not push to GitLab/origin for FTE tasks
- Do not merge before validation and review

## Canonical FTE Files
- `financial-truth-engine/NEXT_STEPS.md` — roadmap and immediate next action
- `financial-truth-engine/README.md` — current capabilities and validation table
- `financial-truth-engine/README_SCHEMA.md` — schema notes
- `financial-truth-engine/tests/RUNBOOK.md` — validation order and SQL Editor caveats
- `financial-truth-engine/tests/run_all_validations.sql` — full validation runner
- `financial-truth-engine/reconciler/fte_reconcile.sql` — deterministic reconciler
- `financial-truth-engine/reconciler/fte_explain_claim.sql` — deterministic claim explanation function
- `financial-truth-engine/fixtures/` — synthetic fixtures only

## FTE Current State
- Ledger schema complete through migrations 001–011 (no migrations added since)
- `fte_reconcile_practice` deterministic 9-phase reconciler complete
- Reviewer resolution actions complete through Task 005H / docs repair 005I
- Phase 3A synthetic extraction fixture + validation complete
- `fte_explain_claim` complete
- Merged accounting/reporting arcs: E1 incomplete-status (#39), validate_schema repair (#40), E2 reconciler accounting (#41), E2 scalar-selector hardening (#42), core denial accounting (#43), recoverable_amount overlay (#44), explain ledger-field surfacing (#45), fixture observation-insert hardening (#46 / Task 015A)
- Validation baseline: 279 total PASS = 264 counted + 15 validate_schema
- Persistent 006L/009C fixture reconcile succeeded; baseline locked; idempotency proven
- Open blockers: none
- Derived outputs are volatile: `fte_financial_positions`, `fte_claim_events`, `fte_review_queue`
- Durable reviewer decision history is `fte_review_resolutions`

## Supabase Validation Notes
- Use disposable Supabase only
- SQL Editor does not support psql `\i`
- For SQL Editor, load/register scripts separately and run validation bodies from `BEGIN;`
- `Success. No rows returned` can mean all assertion-based validations passed
- Do not paste DB URLs or credentials into chat

## Context Management Habits
- Use `/context` before large reads
- Use `/compact focus on current FTE task` at natural checkpoints
- Use `/clear` only when starting unrelated work
- Use `/mcp` to disable unused MCP servers/tools for the session
- Delegate large file scans to the `explorer` subagent when possible

## How To Work
- Read only the files needed for the current task
- Prefer diffs and targeted searches over broad file dumps
- Keep implementation slices small
- Before committing, report `git diff --check`, `git diff --stat`, changed files, safety grep results, and `git status --short`

"""
fte_extract_observations.py — Financial Truth Engine observation extractor.

Task 006J pre-gate scope: script skeleton, dry-run only.
No INSERT path is implemented. Non-dry-run execution exits nonzero.
No live AI call is made in any mode in Task 006J.

Usage:
    python fte_extract_observations.py \
        --practice-id <uuid> \
        [--evidence-id <uuid>] \
        --provider <provider-name> \
        --model <provider-model-id> \
        --db-env DATABASE_URL \
        --dry-run

Real provider adapters and the INSERT path are implemented in Task 006K
after explicit B1/B2/B3 approvals from Keith.
"""

import argparse
import os
import sys

import psycopg2

from providers import get_adapter

SYNTHETIC_PREFIX = "[SYNTHETIC]"

OBSERVATION_CONTRACT_FIELDS = [
    "observation_type",
    "amount",
    "claim_identifier",
    "payer_name",
    "confidence_score",
    "raw_value",
    "normalized_value",
]

VALID_OBSERVATION_TYPES = {"billed_amount", "contractual_adjustment", "payment"}

# Inline synthetic mock response used only for parser unit-path testing (no AI call).
_MOCK_RESPONSE_SAMPLE = {
    "observations": [
        {
            "observation_type": "billed_amount",
            "amount": "150.00",
            "claim_identifier": "CLAIM-0001",
            "payer_name": "SYNTHETIC-PAYER",
            "service_date": "2026-01-01",
            "cpt_code": "99213",
            "check_eft_identifier": None,
            "confidence_score": 0.95,
            "raw_value": "$150.00",
            "normalized_value": "150.00",
            "is_summary_row": False,
        }
    ]
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="FTE observation extractor — pre-gate dry-run only (Task 006J)."
    )
    parser.add_argument("--practice-id", required=True, help="Target practice UUID")
    parser.add_argument("--evidence-id", default=None, help="Single evidence row UUID (optional)")
    parser.add_argument("--provider", required=True, help="Provider name (pre-gate: stub)")
    parser.add_argument("--model", required=True, help="Model identifier (pre-gate: stub-model)")
    parser.add_argument(
        "--db-env",
        required=True,
        metavar="VAR_NAME",
        help="Name of the environment variable holding the DB connection string",
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry-run mode (required in Task 006J)")
    return parser.parse_args()


def resolve_db_url(env_var_name: str) -> str:
    value = os.environ.get(env_var_name)
    if not value:
        print(f"ERROR: environment variable '{env_var_name}' is not set or is empty.", file=sys.stderr)
        sys.exit(1)
    return value


def connect(db_url: str):
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as exc:
        print(f"ERROR: DB connection failed: {exc}", file=sys.stderr)
        sys.exit(1)


def select_evidence(conn, practice_id: str, evidence_id: str | None) -> list[dict]:
    query = """
        SELECT id, page_number, raw_text
        FROM fte_evidence
        WHERE practice_id = %s
          AND evidence_type = 'page'
          AND raw_text IS NOT NULL
    """
    params = [practice_id]
    if evidence_id:
        query += " AND id = %s"
        params.append(evidence_id)
    query += " ORDER BY page_number"

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [{"id": str(r[0]), "page_number": r[1], "raw_text": r[2]} for r in rows]


def check_deid_guardrail(row: dict) -> bool:
    return row["raw_text"].startswith(SYNTHETIC_PREFIX)


def assemble_prompt(row: dict) -> str:
    return (
        f"Extract financial observations from the following EOB page text.\n"
        f"Return a JSON object with an 'observations' array. "
        f"Each observation must include: observation_type, amount, claim_identifier, "
        f"payer_name, service_date, cpt_code, check_eft_identifier, confidence_score, "
        f"raw_value, normalized_value, is_summary_row.\n\n"
        f"Text:\n{row['raw_text']}"
    )


def prompt_preview(prompt: str) -> str:
    max_chars = 120
    if len(prompt) <= max_chars:
        return prompt
    return prompt[:max_chars] + " [...]"


def parse_response(response: dict) -> tuple[list[dict], list[str]]:
    observations = []
    errors = []

    if not isinstance(response, dict) or "observations" not in response:
        errors.append("Response missing 'observations' key")
        return observations, errors

    for i, obs in enumerate(response["observations"]):
        missing = [f for f in OBSERVATION_CONTRACT_FIELDS if f not in obs or obs[f] is None]
        if missing:
            errors.append(f"Observation {i}: missing fields {missing}")
            continue
        if obs["observation_type"] not in VALID_OBSERVATION_TYPES:
            errors.append(f"Observation {i}: invalid observation_type '{obs['observation_type']}'")
            continue
        score = obs.get("confidence_score")
        if not isinstance(score, (int, float)) or not (0.0 <= score <= 1.0):
            errors.append(f"Observation {i}: confidence_score '{score}' out of range [0.00, 1.00]")
            continue
        observations.append(obs)

    return observations, errors


def check_idempotency(conn, practice_id: str, evidence_id: str, claim_identifier: str, extractor: str) -> bool:
    query = """
        SELECT COUNT(*)
        FROM fte_observations
        WHERE practice_id = %s
          AND evidence_id = %s
          AND claim_identifier = %s
          AND metadata->>'extractor' = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, [practice_id, evidence_id, claim_identifier, extractor])
        count = cur.fetchone()[0]
    return count >= 3


def main():
    args = parse_args()

    if not args.dry_run:
        print(
            "ERROR: Live run disabled until Task 006K after B1/B2/B3 approvals.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[DRY-RUN] env var resolved:       {args.db_env} — present" if os.environ.get(args.db_env)
          else f"[DRY-RUN] env var resolved:       {args.db_env} — MISSING")
    if not os.environ.get(args.db_env):
        print(f"ERROR: environment variable '{args.db_env}' is not set.", file=sys.stderr)
        sys.exit(1)

    db_url = resolve_db_url(args.db_env)
    conn = connect(db_url)
    print("[DRY-RUN] DB connection:           OK")

    evidence_rows = select_evidence(conn, args.practice_id, args.evidence_id)
    print(f"[DRY-RUN] evidence rows selected:  {len(evidence_rows)}")

    if not evidence_rows:
        print("[DRY-RUN] No evidence rows found. Exiting.")
        conn.close()
        sys.exit(0)

    # De-identification guardrail — fail closed
    for row in evidence_rows:
        if not check_deid_guardrail(row):
            print(
                f"[DRY-RUN] de-id guardrail:         FAIL — "
                f"evidence_id={row['id']} page={row['page_number']} missing {SYNTHETIC_PREFIX!r} prefix",
                file=sys.stderr,
            )
            print("ERROR: raw_text does not satisfy de-identification guardrail. Exiting.", file=sys.stderr)
            conn.close()
            sys.exit(1)

    print("[DRY-RUN] de-id guardrail:         PASS — all rows have [SYNTHETIC] prefix")

    adapter = get_adapter(args.provider)
    extractor_label = f"{args.provider}/{args.model}"

    pages_processed = 0
    pages_skipped = 0
    would_insert_count = 0

    for row in evidence_rows:
        prompt = assemble_prompt(row)
        print(
            f"[DRY-RUN] prompt assembled:        "
            f"evidence_id={row['id']} page={row['page_number']} "
            f"preview={prompt_preview(prompt)!r}"
        )

        # Adapter is loaded but extract() is never called in dry-run mode.
        print(f"[DRY-RUN] adapter:                 StubAdapter — extract() not called")

        # Parser smoke-test against inline synthetic sample (no AI call).
        parsed_obs, parse_errors = parse_response(_MOCK_RESPONSE_SAMPLE)
        if parse_errors:
            print(
                f"[DRY-RUN] parser check (synthetic): WARN — {parse_errors}",
                file=sys.stderr,
            )
        else:
            print(f"[DRY-RUN] parser check (synthetic): PASS — {len(parsed_obs)} obs valid")

        # Idempotency check (read-only; no writes in Task 006J).
        already_extracted = False
        if parsed_obs:
            claim_id = parsed_obs[0].get("claim_identifier", "UNKNOWN")
            already_extracted = check_idempotency(conn, args.practice_id, row["id"], claim_id, extractor_label)

        if already_extracted:
            print(f"[DRY-RUN] idempotency:             SKIP — already extracted")
            pages_skipped += 1
            continue

        would_insert_count += len(parsed_obs)
        pages_processed += 1

    print(f"[DRY-RUN] insert path:             DISABLED — Task 006K required")
    print(f"[DRY-RUN] pages processed:         {pages_processed}")
    print(f"[DRY-RUN] pages skipped:           {pages_skipped}")
    print(f"[DRY-RUN] observations would insert: {would_insert_count}")
    print("[DRY-RUN] COMPLETE — no rows written")

    conn.close()
    sys.exit(0)


if __name__ == "__main__":
    main()

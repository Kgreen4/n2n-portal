"""
preflight.py — fail-closed pre-call guardrails for Task 006K.

These checks gate whether an extraction request is allowed to proceed.
Task 006K scope: these functions are pure and side-effect-free; they make
no network calls and no DB writes. They are exercised only against
synthetic fixtures and mocked data in this task.
"""

import re

APPROVED_BATCH_LABEL = "AZHS_DEID_TEST_BATCH_001"
DEID_PREFIX = f"[DEIDENTIFIED: {APPROVED_BATCH_LABEL}]"
SOURCE_URI_PREFIX = f"private://fte/de-identified/{APPROVED_BATCH_LABEL}/"

# Synthetic identifiers approved for this batch (B1 correction).
SYNTHETIC_ID_PATTERN = re.compile(r"^SYN-[A-Z0-9]+(-[A-Z0-9]+)*$")

# A real-format claim/check number in this batch's context is treated as a
# long run of digits (and optional separators) that does NOT match the
# synthetic pattern above. This is a conservative heuristic, not a perfect
# detector — it fails closed (flags as real) rather than fails open.
_DIGIT_RUN = re.compile(r"\d{6,}")

# B3_RUNTIME_DRAFT_004 expected runtime configuration.
# top_p intentionally excluded: gpt-5.5 rejects it (HTTP 400), confirmed
# via one live call under Task 006L-B.
EXPECTED_RUNTIME_CONFIG = {
    "provider": "openai",
    "api_surface": "responses",
    "model": "gpt-5.5",
    "tools": [],
    "tool_choice": "none",
    "previous_response_id": None,
}


def check_batch_label(batch_label: str) -> bool:
    return batch_label == APPROVED_BATCH_LABEL


def check_deid_prefix(raw_text: str) -> bool:
    return isinstance(raw_text, str) and raw_text.startswith(DEID_PREFIX)


def check_source_uri(source_uri: str, record_id: str) -> bool:
    return source_uri == f"{SOURCE_URI_PREFIX}{record_id}"


def is_synthetic_identifier(value: str) -> bool:
    return isinstance(value, str) and bool(SYNTHETIC_ID_PATTERN.match(value))


def looks_like_real_format_identifier(value: str) -> bool:
    """
    Fail-closed heuristic: True means this value looks like it could be a
    real (non-synthetic) claim or check/EFT identifier and must block
    processing. Synthetic IDs (SYN-*) are explicitly excluded.
    """
    if not isinstance(value, str):
        return False
    if is_synthetic_identifier(value):
        return False
    return bool(_DIGIT_RUN.search(value))


def check_runtime_config(config: dict) -> list[str]:
    """
    Compares a runtime config dict against B3_RUNTIME_DRAFT_003 expected
    values. Returns a list of mismatch error strings; empty list means OK.
    """
    errors: list[str] = []
    for key, expected in EXPECTED_RUNTIME_CONFIG.items():
        actual = config.get(key, "<missing>")
        if actual != expected:
            errors.append(f"runtime_config.{key} mismatch: expected {expected!r}, got {actual!r}")

    if "max_output_tokens" not in config or not isinstance(config.get("max_output_tokens"), int):
        errors.append("runtime_config.max_output_tokens must be explicitly set to an int")

    if "top_p" in config:
        errors.append(
            "runtime_config.top_p must be omitted for gpt-5.5 (unsupported "
            "parameter — confirmed via live 400 under Task 006L-B)"
        )

    if "reasoning_effort" not in config:
        errors.append("runtime_config.reasoning_effort must be explicitly selected")

    return errors


def check_prompt_match(prompt_text: str, expected_prompt_text: str) -> bool:
    return prompt_text == expected_prompt_text


def run_all_preflight_checks(
    *,
    batch_label: str,
    raw_text: str,
    source_uri: str,
    record_id: str,
    claim_identifier: str,
    check_eft_identifier: str | None,
    prompt_text: str,
    expected_prompt_text: str,
    runtime_config: dict,
) -> list[str]:
    """
    Runs every Task 006K preflight check and returns a list of failure
    reasons. An empty list means all checks passed. Any non-empty list
    means fail-closed — the caller must not proceed to call the adapter.
    """
    errors: list[str] = []

    if not check_batch_label(batch_label):
        errors.append(f"batch_label mismatch: expected '{APPROVED_BATCH_LABEL}', got '{batch_label}'")

    if not check_deid_prefix(raw_text):
        errors.append(f"raw_text missing required prefix '{DEID_PREFIX}'")

    if not check_source_uri(source_uri, record_id):
        errors.append(
            f"source_uri mismatch: expected '{SOURCE_URI_PREFIX}{record_id}', got '{source_uri}'"
        )

    if looks_like_real_format_identifier(claim_identifier):
        errors.append(f"claim_identifier '{claim_identifier}' looks real-format, not synthetic")

    if check_eft_identifier is not None and looks_like_real_format_identifier(check_eft_identifier):
        errors.append(f"check_eft_identifier '{check_eft_identifier}' looks real-format, not synthetic")

    if not check_prompt_match(prompt_text, expected_prompt_text):
        errors.append("prompt_text does not match approved B2_PROMPT_DRAFT_001 text")

    errors.extend(check_runtime_config(runtime_config))

    return errors

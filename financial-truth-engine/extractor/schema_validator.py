"""
schema_validator.py — local structured-output validation for B2_PROMPT_DRAFT_001
responses.

Task 006K scope: this validates mocked/test responses only. No live AI call
is made anywhere in this module or its callers within Task 006K.

Implemented without the `jsonschema` package (not present in this environment)
so the check list below is a hand-rolled equivalent of the rules in
schemas/b2_response_schema.json, kept in sync with that file deliberately.
"""

EXPECTED_SCHEMA_VERSION = "B2_PROMPT_DRAFT_001"

_SAFETY_FLAG_KEYS = [
    "phi_detected",
    "credentials_detected",
    "database_url_detected",
    "project_url_detected",
    "production_data_detected",
    "payer_export_detected",
    "raw_pdf_detected",
    "other_disallowed_content_detected",
]

_EXTRACTED_ITEM_REQUIRED_KEYS = [
    "field_name",
    "field_category",
    "raw_value",
    "normalized_value",
    "unit_or_currency",
    "date_context",
    "source_evidence_quote",
    "ambiguity_note",
    "confidence",
]

_NON_EXTRACTED_REQUIRED_KEYS = ["observation", "reason_not_extracted"]

_VALID_STATUS = {"ok", "safety_blocked"}
_VALID_CONFIDENCE = {"low", "medium", "high"}

_TOP_LEVEL_REQUIRED_KEYS = [
    "output_schema_version",
    "batch_label",
    "artifact_label",
    "status",
    "safety_flags",
    "extracted_items",
    "non_extracted_observations",
    "extraction_limits",
]


def validate_b2_response(response: dict) -> tuple[bool, list[str]]:
    """
    Validate a (mocked) response dict against the B2_PROMPT_DRAFT_001 schema.
    Returns (is_valid, errors). Any non-empty errors list means fail-closed.
    """
    errors: list[str] = []

    if not isinstance(response, dict):
        return False, ["response is not a JSON object"]

    for key in _TOP_LEVEL_REQUIRED_KEYS:
        if key not in response:
            errors.append(f"missing required top-level key: '{key}'")

    extra_top_level = set(response.keys()) - set(_TOP_LEVEL_REQUIRED_KEYS)
    if extra_top_level:
        errors.append(f"unexpected top-level key(s) (additionalProperties: false): {sorted(extra_top_level)}")

    if errors:
        return False, errors

    if response["output_schema_version"] != EXPECTED_SCHEMA_VERSION:
        errors.append(
            f"output_schema_version mismatch: expected "
            f"'{EXPECTED_SCHEMA_VERSION}', got '{response['output_schema_version']}'"
        )

    if not isinstance(response["batch_label"], str):
        errors.append("batch_label must be a string")

    if not isinstance(response["artifact_label"], str):
        errors.append("artifact_label must be a string")

    if response["status"] not in _VALID_STATUS:
        errors.append(f"status must be one of {_VALID_STATUS}, got '{response['status']}'")

    safety_flags = response["safety_flags"]
    if not isinstance(safety_flags, dict):
        errors.append("safety_flags must be an object")
    else:
        for key in _SAFETY_FLAG_KEYS:
            if key not in safety_flags:
                errors.append(f"safety_flags missing key: '{key}'")
            elif not isinstance(safety_flags[key], bool):
                errors.append(f"safety_flags.{key} must be boolean")
        extra_safety_flags = set(safety_flags.keys()) - set(_SAFETY_FLAG_KEYS)
        if extra_safety_flags:
            errors.append(
                f"unexpected safety_flags key(s) (additionalProperties: false): {sorted(extra_safety_flags)}"
            )

    extracted_items = response["extracted_items"]
    if not isinstance(extracted_items, list):
        errors.append("extracted_items must be an array")
    else:
        for i, item in enumerate(extracted_items):
            if not isinstance(item, dict):
                errors.append(f"extracted_items[{i}] is not an object")
                continue
            for key in _EXTRACTED_ITEM_REQUIRED_KEYS:
                if key not in item:
                    errors.append(f"extracted_items[{i}] missing key: '{key}'")
            extra_item_keys = set(item.keys()) - set(_EXTRACTED_ITEM_REQUIRED_KEYS)
            if extra_item_keys:
                errors.append(
                    f"extracted_items[{i}] unexpected key(s) (additionalProperties: false): "
                    f"{sorted(extra_item_keys)}"
                )
            if "confidence" in item and item["confidence"] not in _VALID_CONFIDENCE:
                errors.append(
                    f"extracted_items[{i}].confidence must be one of "
                    f"{_VALID_CONFIDENCE}, got '{item['confidence']}'"
                )

    non_extracted = response["non_extracted_observations"]
    if not isinstance(non_extracted, list):
        errors.append("non_extracted_observations must be an array")
    else:
        for i, item in enumerate(non_extracted):
            if not isinstance(item, dict):
                errors.append(f"non_extracted_observations[{i}] is not an object")
                continue
            for key in _NON_EXTRACTED_REQUIRED_KEYS:
                if key not in item:
                    errors.append(f"non_extracted_observations[{i}] missing key: '{key}'")
            extra_non_extracted_keys = set(item.keys()) - set(_NON_EXTRACTED_REQUIRED_KEYS)
            if extra_non_extracted_keys:
                errors.append(
                    f"non_extracted_observations[{i}] unexpected key(s) (additionalProperties: false): "
                    f"{sorted(extra_non_extracted_keys)}"
                )

    extraction_limits = response["extraction_limits"]
    if not isinstance(extraction_limits, list) or not all(
        isinstance(x, str) for x in extraction_limits
    ):
        errors.append("extraction_limits must be an array of strings")

    return (len(errors) == 0), errors

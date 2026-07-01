"""
fixtures.py — synthetic, de-identified test fixtures for Task 006K.

All content here is synthetic. No real claim numbers, check/EFT numbers,
patient identifiers, or production data. Conforms to B1 AZHS_DEID_TEST_BATCH_001
constraints: payer name retained, claim/check identifiers are synthetic
(SYN-* format) only.
"""

BATCH_LABEL = "AZHS_DEID_TEST_BATCH_001"

DEID_PAGE_001_RAW_TEXT = (
    "[DEIDENTIFIED: AZHS_DEID_TEST_BATCH_001]\n"
    "Payer: BlueCross BlueShield of Arizona\n"
    "Claim #: SYN-AZ-CLAIM-0001\n"
    "EFT/CHECK#: SYN-AZ-CHECK-0001\n"
    "Payment Summary: Billed $150.00, Allowed $120.00, Paid $96.00\n"
)

DEID_PAGE_002_RAW_TEXT = (
    "[DEIDENTIFIED: AZHS_DEID_TEST_BATCH_001]\n"
    "Payer: BlueCross BlueShield of Arizona\n"
    "Claim #: SYN-AZ-CLAIM-0001\n"
    "Claim Detail Line: CPT 99213, Billed $150.00, Adjustment $30.00\n"
)

VALID_B2_RESPONSE = {
    "output_schema_version": "B2_PROMPT_DRAFT_001",
    "batch_label": BATCH_LABEL,
    "artifact_label": "page_001",
    "status": "ok",
    "safety_flags": {
        "phi_detected": False,
        "credentials_detected": False,
        "database_url_detected": False,
        "project_url_detected": False,
        "production_data_detected": False,
        "payer_export_detected": False,
        "raw_pdf_detected": False,
        "other_disallowed_content_detected": False,
    },
    "extracted_items": [
        {
            "field_name": "billed_amount",
            "field_category": "financial",
            "raw_value": "$150.00",
            "normalized_value": "150.00",
            "unit_or_currency": "USD",
            "date_context": None,
            "source_evidence_quote": "Billed $150.00",
            "ambiguity_note": None,
            "confidence": "high",
        }
    ],
    "non_extracted_observations": [],
    "extraction_limits": [
        "Only explicitly supported values were extracted.",
        "No outside knowledge was used.",
        "No database insert, provider call, or production action was performed.",
    ],
}

SAFETY_BLOCKED_B2_RESPONSE = {
    "output_schema_version": "B2_PROMPT_DRAFT_001",
    "batch_label": BATCH_LABEL,
    "artifact_label": "page_001",
    "status": "safety_blocked",
    "safety_flags": {
        "phi_detected": True,
        "credentials_detected": False,
        "database_url_detected": False,
        "project_url_detected": False,
        "production_data_detected": False,
        "payer_export_detected": False,
        "raw_pdf_detected": False,
        "other_disallowed_content_detected": False,
    },
    "extracted_items": [],
    "non_extracted_observations": [
        {
            "observation": "Extraction stopped because disallowed content may be present.",
            "reason_not_extracted": "Safety boundary triggered.",
        }
    ],
    "extraction_limits": [
        "Extraction was blocked.",
        "No outside knowledge was used.",
        "No database insert, provider call, or production action was performed.",
    ],
}

MALFORMED_B2_RESPONSE = {
    "output_schema_version": "B2_PROMPT_DRAFT_001",
    "batch_label": BATCH_LABEL,
    # missing artifact_label, status, etc. — intentionally malformed
}

# B3_RUNTIME_DRAFT_004: top_p omitted (gpt-5.5 rejects it with HTTP 400).
RUNTIME_CONFIG_VALID = {
    "provider": "openai",
    "api_surface": "responses",
    "model": "gpt-5.5",
    "tools": [],
    "tool_choice": "none",
    "previous_response_id": None,
    "max_output_tokens": 2000,
    "reasoning_effort": "medium",
}

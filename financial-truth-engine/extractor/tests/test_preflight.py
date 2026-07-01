import unittest

from preflight import (
    check_batch_label,
    check_deid_prefix,
    check_source_uri,
    is_synthetic_identifier,
    looks_like_real_format_identifier,
    check_runtime_config,
    check_prompt_match,
    run_all_preflight_checks,
)
from tests.fixtures import DEID_PAGE_001_RAW_TEXT, RUNTIME_CONFIG_VALID


class TestBatchAndPrefixChecks(unittest.TestCase):
    def test_correct_batch_label_passes(self):
        self.assertTrue(check_batch_label("AZHS_DEID_TEST_BATCH_001"))

    def test_wrong_batch_label_fails(self):
        self.assertFalse(check_batch_label("AZHS_DEID_TEST_BATCH_002"))

    def test_prefix_present_passes(self):
        self.assertTrue(check_deid_prefix(DEID_PAGE_001_RAW_TEXT))

    def test_prefix_missing_fails(self):
        self.assertFalse(check_deid_prefix("no prefix here"))

    def test_wrong_prefix_fails_closed(self):
        self.assertFalse(check_deid_prefix("[SYNTHETIC] some text"))


class TestSourceUri(unittest.TestCase):
    def test_correct_source_uri_passes(self):
        self.assertTrue(
            check_source_uri(
                "private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
                "page_001",
            )
        )

    def test_wrong_record_id_fails(self):
        self.assertFalse(
            check_source_uri(
                "private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
                "page_002",
            )
        )

    def test_wrong_batch_in_uri_fails(self):
        self.assertFalse(
            check_source_uri(
                "private://fte/de-identified/AZHS_DEID_TEST_BATCH_002/page_001",
                "page_001",
            )
        )


class TestIdentifierGuardrail(unittest.TestCase):
    def test_synthetic_claim_id_recognized(self):
        self.assertTrue(is_synthetic_identifier("SYN-AZ-CLAIM-0001"))

    def test_synthetic_check_id_recognized(self):
        self.assertTrue(is_synthetic_identifier("SYN-AZ-CHECK-0001"))

    def test_synthetic_id_not_flagged_as_real(self):
        self.assertFalse(looks_like_real_format_identifier("SYN-AZ-CLAIM-0001"))

    def test_real_format_long_digit_run_flagged(self):
        # Generic placeholder digit run (not a real claim number) — exercises
        # the "looks real, not synthetic" detector only.
        self.assertTrue(looks_like_real_format_identifier("111122223333444"))

    def test_real_format_check_number_flagged(self):
        # Generic placeholder digit run (not a real check/EFT number).
        self.assertTrue(looks_like_real_format_identifier("9988776655"))

    def test_short_non_identifier_text_not_flagged(self):
        self.assertFalse(looks_like_real_format_identifier("CPT 99213"))


class TestRuntimeConfig(unittest.TestCase):
    def test_valid_config_passes(self):
        errors = check_runtime_config(RUNTIME_CONFIG_VALID)
        self.assertEqual(errors, [])

    def test_wrong_model_fails(self):
        bad = {**RUNTIME_CONFIG_VALID, "model": "gpt-4o"}
        errors = check_runtime_config(bad)
        self.assertTrue(any("model" in e for e in errors))

    def test_tools_not_empty_fails(self):
        bad = {**RUNTIME_CONFIG_VALID, "tools": ["some_tool"]}
        errors = check_runtime_config(bad)
        self.assertTrue(any("tools" in e for e in errors))

    def test_previous_response_id_set_fails(self):
        bad = {**RUNTIME_CONFIG_VALID, "previous_response_id": "resp_123"}
        errors = check_runtime_config(bad)
        self.assertTrue(any("previous_response_id" in e for e in errors))

    def test_missing_max_output_tokens_fails(self):
        bad = dict(RUNTIME_CONFIG_VALID)
        del bad["max_output_tokens"]
        errors = check_runtime_config(bad)
        self.assertTrue(any("max_output_tokens" in e for e in errors))

    def test_missing_reasoning_effort_fails(self):
        bad = dict(RUNTIME_CONFIG_VALID)
        del bad["reasoning_effort"]
        errors = check_runtime_config(bad)
        self.assertTrue(any("reasoning_effort" in e for e in errors))

    def test_top_p_present_in_config_fails(self):
        # gpt-5.5 rejects top_p with HTTP 400 (confirmed live, Task 006L-B).
        # It must be omitted from runtime config entirely, not pinned.
        bad = {**RUNTIME_CONFIG_VALID, "top_p": 1.0}
        errors = check_runtime_config(bad)
        self.assertTrue(any("top_p" in e for e in errors))


class TestPromptMatch(unittest.TestCase):
    def test_matching_prompt_passes(self):
        self.assertTrue(check_prompt_match("exact text", "exact text"))

    def test_mismatched_prompt_fails(self):
        self.assertFalse(check_prompt_match("modified text", "exact text"))


class TestRunAllPreflightChecks(unittest.TestCase):
    def test_all_pass_returns_no_errors(self):
        errors = run_all_preflight_checks(
            batch_label="AZHS_DEID_TEST_BATCH_001",
            raw_text=DEID_PAGE_001_RAW_TEXT,
            source_uri="private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
            record_id="page_001",
            claim_identifier="SYN-AZ-CLAIM-0001",
            check_eft_identifier="SYN-AZ-CHECK-0001",
            prompt_text="approved prompt",
            expected_prompt_text="approved prompt",
            runtime_config=RUNTIME_CONFIG_VALID,
        )
        self.assertEqual(errors, [])

    def test_real_format_claim_id_fails_closed(self):
        errors = run_all_preflight_checks(
            batch_label="AZHS_DEID_TEST_BATCH_001",
            raw_text=DEID_PAGE_001_RAW_TEXT,
            source_uri="private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
            record_id="page_001",
            claim_identifier="111122223333444",
            check_eft_identifier="SYN-AZ-CHECK-0001",
            prompt_text="approved prompt",
            expected_prompt_text="approved prompt",
            runtime_config=RUNTIME_CONFIG_VALID,
        )
        self.assertTrue(any("claim_identifier" in e for e in errors))

    def test_real_format_check_id_fails_closed(self):
        errors = run_all_preflight_checks(
            batch_label="AZHS_DEID_TEST_BATCH_001",
            raw_text=DEID_PAGE_001_RAW_TEXT,
            source_uri="private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
            record_id="page_001",
            claim_identifier="SYN-AZ-CLAIM-0001",
            check_eft_identifier="9988776655",
            prompt_text="approved prompt",
            expected_prompt_text="approved prompt",
            runtime_config=RUNTIME_CONFIG_VALID,
        )
        self.assertTrue(any("check_eft_identifier" in e for e in errors))

    def test_missing_prefix_fails_closed(self):
        errors = run_all_preflight_checks(
            batch_label="AZHS_DEID_TEST_BATCH_001",
            raw_text="no prefix here",
            source_uri="private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
            record_id="page_001",
            claim_identifier="SYN-AZ-CLAIM-0001",
            check_eft_identifier=None,
            prompt_text="approved prompt",
            expected_prompt_text="approved prompt",
            runtime_config=RUNTIME_CONFIG_VALID,
        )
        self.assertTrue(any("prefix" in e for e in errors))

    def test_prompt_mismatch_fails_closed(self):
        errors = run_all_preflight_checks(
            batch_label="AZHS_DEID_TEST_BATCH_001",
            raw_text=DEID_PAGE_001_RAW_TEXT,
            source_uri="private://fte/de-identified/AZHS_DEID_TEST_BATCH_001/page_001",
            record_id="page_001",
            claim_identifier="SYN-AZ-CLAIM-0001",
            check_eft_identifier=None,
            prompt_text="tampered prompt",
            expected_prompt_text="approved prompt",
            runtime_config=RUNTIME_CONFIG_VALID,
        )
        self.assertTrue(any("prompt_text" in e for e in errors))


if __name__ == "__main__":
    unittest.main()

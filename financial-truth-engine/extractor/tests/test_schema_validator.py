import unittest

from schema_validator import validate_b2_response
from tests.fixtures import (
    VALID_B2_RESPONSE,
    SAFETY_BLOCKED_B2_RESPONSE,
    MALFORMED_B2_RESPONSE,
)


class TestSchemaValidator(unittest.TestCase):
    def test_valid_response_passes(self):
        is_valid, errors = validate_b2_response(VALID_B2_RESPONSE)
        self.assertTrue(is_valid)
        self.assertEqual(errors, [])

    def test_safety_blocked_response_is_schema_valid(self):
        # safety_blocked is a structurally valid response; the adapter
        # layer (not the schema validator) is responsible for halting on it.
        is_valid, errors = validate_b2_response(SAFETY_BLOCKED_B2_RESPONSE)
        self.assertTrue(is_valid)
        self.assertEqual(errors, [])

    def test_malformed_response_fails_closed(self):
        is_valid, errors = validate_b2_response(MALFORMED_B2_RESPONSE)
        self.assertFalse(is_valid)
        self.assertTrue(len(errors) > 0)

    def test_non_dict_response_fails_closed(self):
        is_valid, errors = validate_b2_response("not a dict")
        self.assertFalse(is_valid)
        self.assertIn("response is not a JSON object", errors)

    def test_wrong_schema_version_fails(self):
        bad = dict(VALID_B2_RESPONSE)
        bad["output_schema_version"] = "WRONG_VERSION"
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(any("output_schema_version mismatch" in e for e in errors))

    def test_invalid_confidence_value_fails(self):
        bad = {
            **VALID_B2_RESPONSE,
            "extracted_items": [
                {**VALID_B2_RESPONSE["extracted_items"][0], "confidence": "extremely_high"}
            ],
        }
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(any("confidence" in e for e in errors))

    def test_missing_safety_flag_key_fails(self):
        bad = dict(VALID_B2_RESPONSE)
        flags = dict(bad["safety_flags"])
        del flags["phi_detected"]
        bad["safety_flags"] = flags
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(any("phi_detected" in e for e in errors))

    def test_extra_top_level_key_fails_closed(self):
        bad = dict(VALID_B2_RESPONSE)
        bad["unexpected_field"] = "should not be here"
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(any("unexpected top-level key" in e and "unexpected_field" in e for e in errors))

    def test_extra_safety_flags_key_fails_closed(self):
        bad = dict(VALID_B2_RESPONSE)
        flags = dict(bad["safety_flags"])
        flags["unexpected_flag"] = True
        bad["safety_flags"] = flags
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(any("unexpected safety_flags key" in e and "unexpected_flag" in e for e in errors))

    def test_extra_extracted_items_key_fails_closed(self):
        bad = dict(VALID_B2_RESPONSE)
        item = dict(bad["extracted_items"][0])
        item["unexpected_item_field"] = "nope"
        bad["extracted_items"] = [item]
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(
            any("extracted_items[0] unexpected key" in e and "unexpected_item_field" in e for e in errors)
        )

    def test_extra_non_extracted_observations_key_fails_closed(self):
        bad = dict(VALID_B2_RESPONSE)
        bad["non_extracted_observations"] = [
            {
                "observation": "x",
                "reason_not_extracted": "y",
                "unexpected_obs_field": "nope",
            }
        ]
        is_valid, errors = validate_b2_response(bad)
        self.assertFalse(is_valid)
        self.assertTrue(
            any(
                "non_extracted_observations[0] unexpected key" in e and "unexpected_obs_field" in e
                for e in errors
            )
        )


if __name__ == "__main__":
    unittest.main()

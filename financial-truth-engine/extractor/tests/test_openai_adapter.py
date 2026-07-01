import unittest

from providers.openai_adapter import OpenAIAdapter, OpenAIAdapterError, MODEL_ID
from tests.fixtures import VALID_B2_RESPONSE, SAFETY_BLOCKED_B2_RESPONSE, MALFORMED_B2_RESPONSE

SYSTEM_PROMPT = "You are the N2N Financial Truth Engine extraction reviewer."
JSON_SCHEMA = {"type": "object"}  # placeholder schema object for request construction tests


class _FakeResponsesNamespace:
    """Mimics client.responses.create(**kwargs) without any real network call."""

    def __init__(self, fixed_output: dict, raise_exc: Exception | None = None):
        self._fixed_output = fixed_output
        self._raise_exc = raise_exc
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        if self._raise_exc is not None:
            raise self._raise_exc
        return {"output": self._fixed_output}


class _FakeClient:
    def __init__(self, fixed_output: dict, raise_exc: Exception | None = None):
        self.responses = _FakeResponsesNamespace(fixed_output, raise_exc)


def _build_adapter(client):
    return OpenAIAdapter(
        client,
        system_prompt=SYSTEM_PROMPT,
        json_schema=JSON_SCHEMA,
        max_output_tokens=2000,
        reasoning_effort="medium",
    )


class TestAdapterConstruction(unittest.TestCase):
    def test_requires_explicit_client(self):
        with self.assertRaises(ValueError):
            OpenAIAdapter(
                None,
                system_prompt=SYSTEM_PROMPT,
                json_schema=JSON_SCHEMA,
                max_output_tokens=2000,
                reasoning_effort="medium",
            )


class TestRequestConstruction(unittest.TestCase):
    def setUp(self):
        self.client = _FakeClient(VALID_B2_RESPONSE)
        self.adapter = _build_adapter(self.client)

    def test_build_request_shape(self):
        request = self.adapter.build_request("user prompt text", MODEL_ID)
        self.assertEqual(request["model"], MODEL_ID)
        self.assertEqual(request["tools"], [])
        self.assertEqual(request["tool_choice"], "none")
        self.assertEqual(request["max_output_tokens"], 2000)
        self.assertNotIn("top_p", request)
        self.assertEqual(request["reasoning"], {"effort": "medium"})
        self.assertNotIn("previous_response_id", request)
        self.assertEqual(request["input"][0]["role"], "system")
        self.assertEqual(request["input"][0]["content"], SYSTEM_PROMPT)
        self.assertEqual(request["input"][1]["role"], "user")
        self.assertEqual(request["input"][1]["content"], "user prompt text")
        self.assertEqual(request["text"]["format"]["type"], "json_schema")
        self.assertTrue(request["text"]["format"]["strict"])

    def test_wrong_model_rejected(self):
        with self.assertRaises(OpenAIAdapterError):
            self.adapter.build_request("prompt", "gpt-4o")

    def test_prompt_hash_deterministic(self):
        h1 = self.adapter.prompt_hash("same text")
        h2 = self.adapter.prompt_hash("same text")
        h3 = self.adapter.prompt_hash("different text")
        self.assertEqual(h1, h2)
        self.assertNotEqual(h1, h3)


class TestExtractFailClosed(unittest.TestCase):
    def test_valid_response_returns_output(self):
        client = _FakeClient(VALID_B2_RESPONSE)
        adapter = _build_adapter(client)
        result = adapter.extract("user prompt", MODEL_ID)
        self.assertEqual(result["status"], "ok")
        # Confirm exactly one mocked call was made, no network involved.
        self.assertIsNotNone(client.responses.last_kwargs)

    def test_safety_blocked_raises(self):
        client = _FakeClient(SAFETY_BLOCKED_B2_RESPONSE)
        adapter = _build_adapter(client)
        with self.assertRaises(OpenAIAdapterError) as ctx:
            adapter.extract("user prompt", MODEL_ID)
        self.assertIn("safety_blocked", str(ctx.exception))

    def test_malformed_response_raises(self):
        client = _FakeClient(MALFORMED_B2_RESPONSE)
        adapter = _build_adapter(client)
        with self.assertRaises(OpenAIAdapterError) as ctx:
            adapter.extract("user prompt", MODEL_ID)
        self.assertIn("schema validation failed", str(ctx.exception))

    def test_client_exception_raises_adapter_error(self):
        client = _FakeClient(VALID_B2_RESPONSE, raise_exc=ConnectionError("simulated network failure"))
        adapter = _build_adapter(client)
        with self.assertRaises(OpenAIAdapterError) as ctx:
            adapter.extract("user prompt", MODEL_ID)
        self.assertIn("adapter call failed", str(ctx.exception))

    def test_incomplete_output_raises(self):
        class _IncompleteResponsesNamespace:
            def create(self, **kwargs):
                return {
                    "output": VALID_B2_RESPONSE,
                    "incomplete_details": {"reason": "max_output_tokens"},
                }

        class _IncompleteClient:
            def __init__(self):
                self.responses = _IncompleteResponsesNamespace()

        adapter = _build_adapter(_IncompleteClient())
        with self.assertRaises(OpenAIAdapterError) as ctx:
            adapter.extract("user prompt", MODEL_ID)
        self.assertIn("incomplete output", str(ctx.exception))

    def test_no_retry_on_failure(self):
        """
        Adapter must not retry internally — a single failed extract() call
        results in exactly one mocked client invocation.
        """
        client = _FakeClient(SAFETY_BLOCKED_B2_RESPONSE)
        adapter = _build_adapter(client)
        call_count = {"n": 0}
        original_create = client.responses.create

        def counting_create(**kwargs):
            call_count["n"] += 1
            return original_create(**kwargs)

        client.responses.create = counting_create

        with self.assertRaises(OpenAIAdapterError):
            adapter.extract("user prompt", MODEL_ID)

        self.assertEqual(call_count["n"], 1)


if __name__ == "__main__":
    unittest.main()

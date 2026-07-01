"""
openai_adapter.py — OpenAI Responses API adapter (Task 006K).

Task 006K scope: this adapter implements request construction, strict
structured-output configuration, and fail-closed response handling. It is
exercised in this task ONLY against an injected mock client and synthetic
fixtures.

Live-call safety by construction:
- OpenAIAdapter never constructs a real OpenAI client itself. A `client`
  object exposing a `responses.create(**kwargs)` method MUST be passed in
  by the caller. Task 006K's dry-run/test code paths only ever inject a
  mock client; no caller code added in this task supplies a real OpenAI
  SDK client. Wiring a real client to this adapter is explicitly deferred
  to a future Task 006L, which requires its own separate written approval.
- No `previous_response_id` is ever set (single-turn/stateless only).
- `tools` is always `[]` and `tool_choice` is always `"none"`.
"""

import hashlib

from .base import BaseAdapter
from schema_validator import validate_b2_response

MODEL_ID = "gpt-5.5"
API_SURFACE = "responses"
PROVIDER_NAME = "openai"


class OpenAIAdapterError(Exception):
    """Raised for any fail-closed condition in OpenAIAdapter.extract()."""


class OpenAIAdapter(BaseAdapter):
    def __init__(
        self,
        client,
        *,
        system_prompt: str,
        json_schema: dict,
        max_output_tokens: int,
        reasoning_effort: str,
    ):
        """
        client: object exposing `responses.create(**kwargs) -> dict`.
            Task 006K never passes a real OpenAI SDK client here — only a
            mock, in tests. Required (no default) so this adapter cannot
            silently fall back to constructing a live client.
        system_prompt: exact B2_PROMPT_DRAFT_001 system prompt text.
        json_schema: B2_PROMPT_DRAFT_001 response JSON Schema (strict mode).
        max_output_tokens, reasoning_effort: pinned per
            B3_RUNTIME_DRAFT_004 — must be explicitly supplied, no defaults.

        `top_p` is intentionally not an accepted parameter here: the
        gpt-5.5 Responses API rejects it with HTTP 400 ("Unsupported
        parameter: 'top_p'"), confirmed by a live call under Task 006L-B.
        It must be omitted from the request body, not sent as null.
        """
        if client is None:
            raise ValueError(
                "OpenAIAdapter requires an explicit client. "
                "Task 006K does not authorize constructing a live OpenAI client."
            )
        self._client = client
        self._system_prompt = system_prompt
        self._json_schema = json_schema
        self._max_output_tokens = max_output_tokens
        self._reasoning_effort = reasoning_effort

    def build_request(self, prompt: str, model: str) -> dict:
        """
        Builds the exact request payload that would be sent to the
        Responses API. Pure function — makes no network call.
        """
        if model != MODEL_ID:
            raise OpenAIAdapterError(
                f"model mismatch: B3_RUNTIME_DRAFT_003 requires '{MODEL_ID}', got '{model}'"
            )

        return {
            "model": model,
            "input": [
                {"role": "system", "content": self._system_prompt},
                {"role": "user", "content": prompt},
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "b2_prompt_draft_001_response",
                    "schema": self._json_schema,
                    "strict": True,
                }
            },
            "tools": [],
            "tool_choice": "none",
            "max_output_tokens": self._max_output_tokens,
            "reasoning": {"effort": self._reasoning_effort},
            # Single-turn/stateless: previous_response_id intentionally
            # never set on this request.
        }

    def prompt_hash(self, prompt: str) -> str:
        return hashlib.sha256(prompt.encode("utf-8")).hexdigest()

    def extract(self, prompt: str, model: str) -> dict:
        """
        Sends the constructed request via the injected client and applies
        fail-closed validation to the result. Raises OpenAIAdapterError on
        any disallowed condition; never retries automatically.
        """
        request = self.build_request(prompt, model)

        try:
            raw_response = self._client.responses.create(**request)
        except Exception as exc:
            raise OpenAIAdapterError(f"adapter call failed: {exc}") from exc

        if not isinstance(raw_response, dict):
            raise OpenAIAdapterError("response is not a dict — fail closed")

        incomplete_details = raw_response.get("incomplete_details")
        if incomplete_details is not None:
            reason = incomplete_details.get("reason") if isinstance(incomplete_details, dict) else incomplete_details
            raise OpenAIAdapterError(
                f"incomplete output (reason={reason!r}) — treated as failed extraction, fail closed"
            )

        output = raw_response.get("output")
        if output is None:
            raise OpenAIAdapterError("response missing 'output' — fail closed")

        is_valid, errors = validate_b2_response(output)
        if not is_valid:
            raise OpenAIAdapterError(f"schema validation failed — fail closed: {errors}")

        if output.get("status") == "safety_blocked":
            raise OpenAIAdapterError("status=safety_blocked — fail closed, human review required")

        return output

from .base import BaseAdapter


class StubAdapter(BaseAdapter):
    def extract(self, prompt: str, model: str) -> dict:
        raise NotImplementedError(
            "StubAdapter does not make live AI calls. "
            "Approve B3 and implement a real provider adapter."
        )

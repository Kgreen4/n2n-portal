from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    @abstractmethod
    def extract(self, prompt: str, model: str) -> dict:
        """
        Send prompt to the AI provider and return a structured response dict.
        The response must be parseable against the FTE observation contract columns.
        Raises NotImplementedError for stub/pre-gate adapters.
        """

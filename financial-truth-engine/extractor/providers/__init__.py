from .stub import StubAdapter

REGISTRY = {
    "stub": StubAdapter,
}


def get_adapter(provider_name: str):
    if provider_name not in REGISTRY:
        raise ValueError(
            f"Unknown provider '{provider_name}'. "
            f"Available pre-gate providers: {list(REGISTRY.keys())}. "
            "Real provider adapters are added in Task 006K after B3 approval."
        )
    return REGISTRY[provider_name]()

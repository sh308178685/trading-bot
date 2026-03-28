"""Factory helpers for exchange adapters."""

from __future__ import annotations

from .bitget import BitgetExchangeAdapter


def create_exchange_adapter(config: dict[str, object]) -> BitgetExchangeAdapter:
    exchange_name = str(config.get("exchange", "bitget")).strip().lower()
    if exchange_name == "bitget":
        return BitgetExchangeAdapter(config)
    raise ValueError(f"Unsupported exchange: {exchange_name}")

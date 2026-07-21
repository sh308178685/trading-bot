"""Shared runtime config loader with environment overrides."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_FILE = ROOT / "config" / "config.json"
ENV_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "dashboardUsername": ("MARTIN_DASHBOARD_USERNAME",),
    "dashboardPassword": ("MARTIN_DASHBOARD_PASSWORD",),
    "dashboardSecret": ("MARTIN_DASHBOARD_SECRET",),
}
EXCHANGE_CREDENTIAL_ENV_KEYS: dict[str, dict[str, tuple[str, ...]]] = {
    "bitget": {
        "apiKey": ("BITGET_API_KEY", "MARTIN_BITGET_API_KEY", "MARTIN_API_KEY"),
        "secretKey": (
            "BITGET_SECRET_KEY",
            "BITGET_API_SECRET",
            "MARTIN_BITGET_SECRET_KEY",
            "MARTIN_API_SECRET",
        ),
        "passphrase": (
            "BITGET_PASSPHRASE",
            "BITGET_API_PASSPHRASE",
            "MARTIN_BITGET_PASSPHRASE",
            "MARTIN_API_PASSPHRASE",
        ),
    },
    "weex": {
        "apiKey": ("WEEX_API_KEY", "MARTIN_WEEX_API_KEY", "MARTIN_API_KEY"),
        "secretKey": (
            "WEEX_SECRET_KEY",
            "WEEX_API_SECRET",
            "MARTIN_WEEX_SECRET_KEY",
            "MARTIN_API_SECRET",
        ),
        "passphrase": (
            "WEEX_PASSPHRASE",
            "WEEX_API_PASSPHRASE",
            "MARTIN_WEEX_PASSPHRASE",
            "MARTIN_API_PASSPHRASE",
        ),
    },
}


def load_json(path: str | Path, default: Any) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        return default


def _resolve_env_value(keys: tuple[str, ...]) -> str | None:
    for env_key in keys:
        value = os.getenv(env_key)
        if value not in (None, ""):
            return value
    return None


def apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    resolved = dict(config)
    for config_key, env_keys in ENV_CONFIG_KEYS.items():
        env_value = _resolve_env_value(env_keys)
        if env_value is not None:
            resolved[config_key] = env_value
    exchange_name = str(resolved.get("exchange", "bitget")).strip().lower()
    credential_keys = EXCHANGE_CREDENTIAL_ENV_KEYS.get(exchange_name, {})
    for config_key, env_keys in credential_keys.items():
        env_value = _resolve_env_value(env_keys)
        if env_value is not None:
            resolved[config_key] = env_value
    return resolved


def load_runtime_config(
    config_path: str | Path | None = None,
    default: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = dict(default or {})
    raw = load_json(config_path or DEFAULT_CONFIG_FILE, {})
    if isinstance(raw, dict):
        base.update(raw)
    return apply_env_overrides(base)

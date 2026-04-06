from __future__ import annotations

import json
from typing import Mapping, Sequence

from app.common.dto import normalize_symbol


def normalize_entity_keys(
    entity_keys: Mapping[str, object] | None,
    *,
    symbol: str | None = None,
    required_keys: Sequence[str] | None = None,
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in (entity_keys or {}).items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized[key] = normalize_symbol(text) if key == "symbol" else text
    if symbol is not None:
        normalized["symbol"] = normalize_symbol(symbol)
    missing = [key for key in (required_keys or ()) if key not in normalized]
    if missing:
        raise ValueError(f"missing entity keys: {', '.join(missing)}")
    return normalized


def entity_scope(entity_keys: Mapping[str, object] | None, *, symbol: str | None = None) -> str:
    normalized = normalize_entity_keys(entity_keys, symbol=symbol)
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_entity_scope(scope: str) -> dict[str, str]:
    if not scope:
        return {}
    payload = json.loads(scope)
    if not isinstance(payload, dict):
        raise ValueError("entity scope payload must be a JSON object")
    return normalize_entity_keys(payload)


def primary_symbol(entity_keys: Mapping[str, object] | None, *, fallback_symbol: str | None = None) -> str:
    normalized = normalize_entity_keys(entity_keys, symbol=fallback_symbol)
    symbol = normalized.get("symbol")
    if not symbol:
        raise ValueError("entity scope requires symbol")
    return symbol

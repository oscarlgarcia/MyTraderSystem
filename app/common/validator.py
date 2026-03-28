"""
Validadores centrales para DTOs y payloads de eventos.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def ensure_aware_utc(ts: datetime) -> None:
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        raise ValueError("timestamp must be timezone-aware UTC")


def validate_price_size(price: float, size: float) -> None:
    if price < 0:
        raise ValueError("price must be non-negative")
    if size < 0:
        raise ValueError("size must be non-negative")


def validate_market_payload(symbol: str, event_ts: datetime, price: float, size: float) -> None:
    ensure_aware_utc(event_ts)
    validate_price_size(price, size)
    if not symbol or not isinstance(symbol, str):
        raise ValueError("symbol must be non-empty string")

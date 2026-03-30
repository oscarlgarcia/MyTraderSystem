"""
Minimal adapters for Binance-style trade/kline streams (testnet/spot).

Designed to be dependency-light and easily mockable for tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Callable, Dict, Tuple

from app.common.dto import MarketEvent, normalize_symbol


def _key(event: MarketEvent) -> Tuple[str, datetime, float, float, str]:
    return (event.symbol, event.event_ts, event.price, event.size, event.source)


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _validate_positive(value: float, name: str) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def normalize_trade(payload: dict) -> MarketEvent:
    """
    Normalize Binance trade payload to MarketEvent.
    Expected keys: s (symbol), E (event time), p (price), q (qty)
    """
    symbol = normalize_symbol(str(payload["s"]))
    event_ts = _ts_from_ms(int(payload["E"]))
    price = float(payload["p"])
    size = float(payload["q"])
    _validate_positive(price, "price")
    _validate_positive(size, "size")
    return MarketEvent(symbol=symbol, event_ts=event_ts, price=price, size=size, source="trade")


def normalize_kline(payload: dict) -> MarketEvent:
    """
    Normalize Binance kline payload to MarketEvent (use close price).
    Expected keys: s (symbol), E (event time), k->{c (close), q (volume)}.
    """
    k = payload["k"]
    symbol = normalize_symbol(str(payload["s"]))
    event_ts = _ts_from_ms(int(payload["E"]))
    price = float(k["c"])
    size = float(k["q"])
    _validate_positive(price, "price")
    _validate_positive(size, "size")
    return MarketEvent(symbol=symbol, event_ts=event_ts, price=price, size=size, source="kline")


NORMALIZERS: Dict[str, Callable[[dict], MarketEvent]] = {
    "trade": normalize_trade,
    "kline": normalize_kline,
}


def register_normalizer(event_type: str, fn: Callable[[dict], MarketEvent]) -> None:
    NORMALIZERS[event_type] = fn


def build_streams(symbols: Iterable[str]) -> List[str]:
    seen = set()
    syms = []
    for s in symbols:
        norm = normalize_symbol(s).lower()
        if norm in seen:
            continue
        seen.add(norm)
        syms.append(norm)
    return [f"{sym}@trade" for sym in syms] + [f"{sym}@kline_1m" for sym in syms]


def build_ws_url(ws_base: str, symbols: Iterable[str]) -> str:
    streams = "/".join(build_streams(symbols))
    if not ws_base.endswith("/stream"):
        base = ws_base.rstrip("/") + "/stream"
    else:
        base = ws_base
    return f"{base}?streams={streams}"


def parse_message(msg: str) -> MarketEvent:
    """
    Parse raw WS message string into MarketEvent, deciding by stream type.
    Supports Binance aggregate stream messages with 'stream' and 'data'.
    """
    payload = json.loads(msg)
    data = payload.get("data", payload)
    stream = payload.get("stream", "")
    event_type = data.get("e")
    if not event_type:
        event_type = "kline" if "kline" in stream else "trade"
    handler = NORMALIZERS.get(event_type)
    if handler is None:
        raise KeyError(f"Unknown event type: {event_type}")
    return handler(data)

"""
Minimal adapters for Binance-style trade/kline streams (testnet/spot).

Designed to be dependency-light and easily mockable for tests.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable, List, Callable, Dict, Tuple

from app.common.dto import MarketEvent, normalize_symbol
from app.common import validator
from app.ingestion.dedup import identity_from_event


def _key(event: MarketEvent) -> Tuple[str, datetime, float, float, str]:
    return identity_from_event(event)


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _validate_positive(value: float, name: str) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _require_keys(payload: dict, required: tuple[str, ...]) -> None:
    missing = [key for key in required if key not in payload]
    if missing:
        raise KeyError(",".join(missing))


def validate_trade_payload(payload: dict) -> None:
    _require_keys(payload, ("s", "E", "p", "q"))
    int(payload["E"])
    price = float(payload["p"])
    size = float(payload["q"])
    _validate_positive(price, "price")
    _validate_positive(size, "size")


def validate_kline_payload(payload: dict) -> None:
    _require_keys(payload, ("s", "E", "k"))
    if not isinstance(payload["k"], dict):
        raise ValueError("k must be a dict")
    _require_keys(payload["k"], ("c", "q"))
    int(payload["E"])
    price = float(payload["k"]["c"])
    size = float(payload["k"]["q"])
    _validate_positive(price, "price")
    _validate_positive(size, "size")


def _validate_market_event(event: MarketEvent) -> None:
    validator.validate_market_payload(event.symbol, event.event_ts, event.price, event.size)


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
PAYLOAD_VALIDATORS: Dict[str, Callable[[dict], None]] = {
    "trade": validate_trade_payload,
    "kline": validate_kline_payload,
}
STREAM_BUILDERS: Dict[str, Callable[[str], str]] = {
    "trade": lambda symbol: f"{symbol}@trade",
    "kline": lambda symbol: f"{symbol}@kline_1m",
}
DEFAULT_STREAM_TYPES: Tuple[str, ...] = ("trade", "kline")


def register_normalizer(event_type: str, fn: Callable[[dict], MarketEvent]) -> None:
    NORMALIZERS[event_type] = fn


def register_payload_validator(event_type: str, fn: Callable[[dict], None]) -> None:
    PAYLOAD_VALIDATORS[event_type] = fn


def register_stream_builder(stream_type: str, fn: Callable[[str], str]) -> None:
    STREAM_BUILDERS[stream_type] = fn


def build_streams(symbols: Iterable[str], stream_types: Iterable[str] | None = None) -> List[str]:
    seen = set()
    syms = []
    for symbol in symbols:
        norm = normalize_symbol(symbol).lower()
        if norm in seen:
            continue
        seen.add(norm)
        syms.append(norm)
    stream_type_list = list(stream_types) if stream_types is not None else list(DEFAULT_STREAM_TYPES)
    streams: List[str] = []
    for stream_type in stream_type_list:
        builder = STREAM_BUILDERS.get(stream_type)
        if builder is None:
            raise KeyError(f"Unknown stream type: {stream_type}")
        for symbol in syms:
            streams.append(builder(symbol))
    return streams


def build_ws_url(ws_base: str, symbols: Iterable[str], stream_types: Iterable[str] | None = None) -> str:
    streams = "/".join(build_streams(symbols, stream_types=stream_types))
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
    payload_validator = PAYLOAD_VALIDATORS.get(event_type)
    if payload_validator is not None:
        payload_validator(data)
    handler = NORMALIZERS.get(event_type)
    if handler is None:
        raise KeyError(f"Unknown event type: {event_type}")
    event = handler(data)
    _validate_market_event(event)
    return event

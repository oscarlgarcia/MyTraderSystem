"""
Minimal adapters for Binance-style trade/kline streams (testnet/spot).

Designed to be dependency-light and easily mockable for tests.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable, List, Callable, Dict, Collection, Tuple

from app.common.dto import MarketEvent, normalize_symbol
from app.ingestion.dedup import EventIdentity, identity_from_event
from app.marketdata.connectors.binance import (
    BinanceBarNormalizer,
    BinanceBookTickerNormalizer,
    BinanceTradeNormalizer,
    build_binance_stream,
    normalize_binance_event,
)
from app.marketdata.models import BarEvent, BookEvent, IngestionEvent, TradeEvent, ensure_legacy_market_event
from app.marketdata.validators import (
    validate_book_payload,
    validate_ingestion_event,
    validate_kline_payload,
    validate_trade_payload,
)


def _key(event: IngestionEvent) -> EventIdentity:
    return identity_from_event(event)


def normalize_trade_typed(
    payload: dict,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
) -> TradeEvent:
    return BinanceTradeNormalizer.normalize_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
    )


def normalize_trade(payload: dict) -> MarketEvent:
    """
    Normalize Binance trade payload to MarketEvent.
    Expected keys: s (symbol), E (event time), p (price), q (qty)
    """
    return ensure_legacy_market_event(normalize_trade_typed(payload))


def normalize_kline_typed(
    payload: dict,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
    interval: str | None = None,
) -> BarEvent:
    event = BinanceBarNormalizer.normalize_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
        interval=interval,
    )
    return event


def normalize_book_typed(
    payload: dict,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
) -> BookEvent:
    return BinanceBookTickerNormalizer.normalize_typed(
        payload,
        venue=venue,
        receive_ts=receive_ts,
        process_ts=process_ts,
    )


def normalize_book(payload: dict) -> MarketEvent:
    return ensure_legacy_market_event(normalize_book_typed(payload))


def normalize_kline(payload: dict) -> MarketEvent:
    """
    Normalize Binance kline payload to MarketEvent (use close price).
    Expected keys: s (symbol), E (event time), k->{c (close), q (quote volume)}.
    """
    return ensure_legacy_market_event(normalize_kline_typed(payload))


NORMALIZERS: Dict[str, Callable[[dict], IngestionEvent]] = {
    "trade": normalize_trade,
    "kline": normalize_kline,
    "book": normalize_book,
}
PAYLOAD_VALIDATORS: Dict[str, Callable[[dict], None]] = {
    "trade": validate_trade_payload,
    "kline": validate_kline_payload,
    "book": validate_book_payload,
}
STREAM_BUILDERS: Dict[str, Callable[[str], str]] = {
    "trade": lambda symbol: build_binance_stream("trade", symbol),
    "kline": lambda symbol: build_binance_stream("kline", symbol),
    "book": lambda symbol: build_binance_stream("book", symbol),
}
DEFAULT_STREAM_TYPES: Tuple[str, ...] = ("trade", "kline")


def canonical_event_type(event_type: str | None, *, stream: str = "") -> str:
    event_type_text = str(event_type or "").strip()
    lowered = event_type_text.lower()
    stream_lower = str(stream).lower()
    if lowered == "aggtrade" or "@aggtrade" in stream_lower:
        return "trade"
    if lowered == "bookticker" or "@bookticker" in stream_lower:
        return "book"
    if event_type_text:
        return event_type_text
    return "kline" if "kline" in stream_lower else "trade"


def register_normalizer(event_type: str, fn: Callable[[dict], IngestionEvent]) -> None:
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


def parse_message_parts(msg: str) -> tuple[dict, dict, str, str]:
    payload = json.loads(msg)
    data = payload.get("data", payload)
    stream = payload.get("stream", "")
    event_type = canonical_event_type(data.get("e"), stream=stream)
    return payload, data, stream, event_type


def parse_typed_message(
    msg: str,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
    allowed_event_types: Collection[str] | None = None,
) -> IngestionEvent:
    payload, data, stream, event_type = parse_message_parts(msg)
    del payload, stream
    if allowed_event_types is not None and event_type not in allowed_event_types:
        raise KeyError(f"Unknown event type: {event_type}")
    if event_type == "trade":
        return normalize_binance_event(
            event_type,
            data,
            venue=venue,
            receive_ts=receive_ts,
            process_ts=process_ts,
        )
    if event_type == "kline":
        return normalize_binance_event(
            event_type,
            data,
            venue=venue,
            receive_ts=receive_ts,
            process_ts=process_ts,
        )
    if event_type == "book":
        return normalize_binance_event(
            event_type,
            data,
            venue=venue,
            receive_ts=receive_ts,
            process_ts=process_ts,
        )
    payload_validator = PAYLOAD_VALIDATORS.get(event_type)
    if payload_validator is not None:
        payload_validator(data)
    handler = NORMALIZERS.get(event_type)
    if handler is None:
        raise KeyError(f"Unknown event type: {event_type}")
    event = handler(data)
    validate_ingestion_event(event)
    return event


def parse_message(msg: str) -> IngestionEvent:
    """
    Parse raw WS message string into MarketEvent, deciding by stream type.
    Supports Binance aggregate stream messages with 'stream' and 'data'.
    """
    return ensure_legacy_market_event(parse_typed_message(msg))

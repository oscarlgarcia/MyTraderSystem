"""
Minimal adapters for Binance-style trade/kline streams (testnet/spot).

Designed to be dependency-light and easily mockable for tests.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable, List, Callable, Dict, Collection

from app.common.dto import MarketEvent, normalize_symbol
from app.ingestion.dedup import EventIdentity, identity_from_event
from app.marketdata.instruments import resolve_instrument
from app.marketdata.models import BarEvent, IngestionEvent, TradeEvent, ensure_legacy_market_event
from app.marketdata.normalization import stamp_normalizer_version
from app.marketdata.validators import (
    validate_ingestion_event,
    validate_kline_payload,
    validate_trade_payload,
)


def _key(event: IngestionEvent) -> EventIdentity:
    return identity_from_event(event)


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _process_ts(process_ts: datetime | None = None) -> datetime:
    return process_ts or datetime.now(timezone.utc)


def _trade_exchange_ts(payload: dict) -> datetime:
    return _ts_from_ms(int(payload["E"]))


def _kline_exchange_ts(payload: dict) -> datetime:
    kline = payload["k"]
    if "T" in kline:
        return _ts_from_ms(int(kline["T"]))
    return _ts_from_ms(int(payload["E"]))


def _instrument_metadata(symbol: str, venue: str) -> dict[str, str]:
    return resolve_instrument(symbol, venue=venue).as_metadata()


def normalize_trade_typed(
    payload: dict,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
) -> TradeEvent:
    validate_trade_payload(payload)
    event = TradeEvent(
        symbol=normalize_symbol(str(payload["s"])),
        exchange_ts=_trade_exchange_ts(payload),
        receive_ts=receive_ts,
        process_ts=_process_ts(process_ts),
        venue=venue,
        source_id=str(payload.get("t")) if payload.get("t") is not None else None,
        metadata=stamp_normalizer_version(_instrument_metadata(str(payload["s"]), venue)),
        price=float(payload["p"]),
        size=float(payload["q"]),
        trade_id=str(payload.get("t")) if payload.get("t") is not None else None,
        side="sell" if payload.get("m") else "buy" if payload.get("m") is not None else None,
    )
    validate_ingestion_event(event)
    return event


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
    validate_kline_payload(payload)
    k = payload["k"]
    event = BarEvent(
        symbol=normalize_symbol(str(payload["s"])),
        exchange_ts=_kline_exchange_ts(payload),
        receive_ts=receive_ts,
        process_ts=_process_ts(process_ts),
        venue=venue,
        source_id=str(k.get("t")) if k.get("t") is not None else None,
        metadata=stamp_normalizer_version(_instrument_metadata(str(payload["s"]), venue)),
        open=float(k.get("o", k["c"])),
        high=float(k.get("h", k["c"])),
        low=float(k.get("l", k["c"])),
        close=float(k["c"]),
        volume=float(k["q"]),
        interval=interval or str(k.get("i", "1m")),
        open_ts=_ts_from_ms(int(k["t"])) if k.get("t") is not None else None,
        close_ts=_ts_from_ms(int(k["T"])) if k.get("T") is not None else None,
    )
    validate_ingestion_event(event)
    return event


def normalize_kline(payload: dict) -> MarketEvent:
    """
    Normalize Binance kline payload to MarketEvent (use close price).
    Expected keys: s (symbol), E (event time), k->{c (close), q (volume)}.
    """
    return ensure_legacy_market_event(normalize_kline_typed(payload))


NORMALIZERS: Dict[str, Callable[[dict], IngestionEvent]] = {
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
    event_type = data.get("e")
    if not event_type:
        event_type = "kline" if "kline" in stream else "trade"
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
        return normalize_trade_typed(
            data,
            venue=venue,
            receive_ts=receive_ts,
            process_ts=process_ts,
        )
    if event_type == "kline":
        return normalize_kline_typed(
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

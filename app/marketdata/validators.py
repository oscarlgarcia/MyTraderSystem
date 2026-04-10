"""
Explicit validators for canonical market data events and Binance-style payloads.

This module keeps validation rules close to the typed market data contract while
still supporting the legacy ``MarketEvent`` migration path.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

from app.common import validator as legacy_validator
from app.common.dto import MarketEvent
from app.marketdata.models import (
    BarEvent,
    BookEvent,
    IngestionEvent,
    TradeEvent,
    legacy_market_event_to_bar,
    legacy_market_event_to_trade,
)

MAX_FUTURE_SKEW = timedelta(minutes=5)


def _utc_now(now: datetime | None = None) -> datetime:
    return now or datetime.now(timezone.utc)


def _require_keys(payload: dict[str, Any], required: tuple[str, ...]) -> None:
    missing = [key for key in required if key not in payload]
    if missing:
        raise KeyError(",".join(missing))


def _validate_symbol(symbol: str) -> None:
    if not symbol or not isinstance(symbol, str):
        raise ValueError("symbol must be non-empty string")


def _validate_finite(value: float, name: str) -> None:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")


def _validate_non_negative_finite(value: float, name: str) -> None:
    _validate_finite(value, name)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _validate_timestamp_sane(ts: datetime, name: str, *, now: datetime | None = None) -> None:
    legacy_validator.ensure_aware_utc(ts)
    if ts > _utc_now(now) + MAX_FUTURE_SKEW:
        raise ValueError(f"{name} is too far in the future")


def _validate_optional_timestamp(ts: datetime | None, name: str, *, now: datetime | None = None) -> None:
    if ts is None:
        return
    _validate_timestamp_sane(ts, name, now=now)


def _validate_payload_timestamp_ms(raw_ms: Any, name: str, *, now: datetime | None = None) -> None:
    ts = datetime.fromtimestamp(int(raw_ms) / 1000, tz=timezone.utc)
    _validate_timestamp_sane(ts, name, now=now)


def validate_trade_payload(payload: dict[str, Any], *, now: datetime | None = None) -> None:
    _require_keys(payload, ("s", "E", "p", "q"))
    _validate_symbol(str(payload["s"]))
    _validate_payload_timestamp_ms(payload["E"], "E", now=now)
    _validate_non_negative_finite(float(payload["p"]), "price")
    _validate_non_negative_finite(float(payload["q"]), "size")


def validate_kline_payload(payload: dict[str, Any], *, now: datetime | None = None) -> None:
    _require_keys(payload, ("s", "E", "k"))
    _validate_symbol(str(payload["s"]))
    _validate_payload_timestamp_ms(payload["E"], "E", now=now)
    kline = payload["k"]
    if not isinstance(kline, dict):
        raise ValueError("k must be a dict")
    _require_keys(kline, ("c", "q"))
    _validate_non_negative_finite(float(kline["c"]), "close")
    _validate_non_negative_finite(float(kline["q"]), "volume")
    if {"o", "h", "l"} <= set(kline):
        open_price = float(kline["o"])
        high = float(kline["h"])
        low = float(kline["l"])
        close = float(kline["c"])
        _validate_non_negative_finite(open_price, "open")
        _validate_non_negative_finite(high, "high")
        _validate_non_negative_finite(low, "low")
        if low > high:
            raise ValueError("low cannot exceed high")
        if not (low <= open_price <= high):
            raise ValueError("open must be within [low, high]")
        if not (low <= close <= high):
            raise ValueError("close must be within [low, high]")
    if "t" in kline:
        _validate_payload_timestamp_ms(kline["t"], "k.t", now=now)
    if "T" in kline:
        _validate_payload_timestamp_ms(kline["T"], "k.T", now=now)
        if "t" in kline and int(kline["T"]) < int(kline["t"]):
            raise ValueError("k.T cannot be earlier than k.t")


def validate_book_payload(payload: dict[str, Any], *, now: datetime | None = None) -> None:
    _require_keys(payload, ("s", "u", "b", "B", "a", "A"))
    _validate_symbol(str(payload["s"]))
    if payload.get("E") not in (None, ""):
        _validate_payload_timestamp_ms(payload["E"], "E", now=now)
    for name, raw in {"bid_price": payload["b"], "bid_size": payload["B"], "ask_price": payload["a"], "ask_size": payload["A"]}.items():
        _validate_non_negative_finite(float(raw), name)
    if float(payload["a"]) < float(payload["b"]):
        raise ValueError("ask price cannot be below bid price")


def validate_trade_event(event: TradeEvent, *, now: datetime | None = None) -> None:
    _validate_symbol(event.symbol)
    _validate_timestamp_sane(event.exchange_ts, "exchange_ts", now=now)
    _validate_optional_timestamp(event.receive_ts, "receive_ts", now=now)
    _validate_optional_timestamp(event.process_ts, "process_ts", now=now)
    if event.receive_ts is not None and event.process_ts is not None and event.process_ts < event.receive_ts:
        raise ValueError("process_ts cannot be earlier than receive_ts")
    _validate_non_negative_finite(event.price, "price")
    _validate_non_negative_finite(event.size, "size")


def validate_bar_event(event: BarEvent, *, now: datetime | None = None) -> None:
    _validate_symbol(event.symbol)
    _validate_timestamp_sane(event.exchange_ts, "exchange_ts", now=now)
    _validate_optional_timestamp(event.receive_ts, "receive_ts", now=now)
    _validate_optional_timestamp(event.process_ts, "process_ts", now=now)
    _validate_optional_timestamp(event.open_ts, "open_ts", now=now)
    _validate_optional_timestamp(event.close_ts, "close_ts", now=now)
    if event.receive_ts is not None and event.process_ts is not None and event.process_ts < event.receive_ts:
        raise ValueError("process_ts cannot be earlier than receive_ts")
    if event.open_ts is not None and event.close_ts is not None and event.close_ts < event.open_ts:
        raise ValueError("close_ts cannot be earlier than open_ts")
    for name, value in {
        "open": event.open,
        "high": event.high,
        "low": event.low,
        "close": event.close,
        "volume": event.volume,
    }.items():
        _validate_non_negative_finite(value, name)
    if event.low > event.high:
        raise ValueError("low cannot exceed high")
    if not (event.low <= event.open <= event.high):
        raise ValueError("open must be within [low, high]")
    if not (event.low <= event.close <= event.high):
        raise ValueError("close must be within [low, high]")
    if not event.interval:
        raise ValueError("interval must be non-empty")


def validate_book_event(event: BookEvent, *, now: datetime | None = None) -> None:
    _validate_symbol(event.symbol)
    _validate_timestamp_sane(event.exchange_ts, "exchange_ts", now=now)
    _validate_optional_timestamp(event.receive_ts, "receive_ts", now=now)
    _validate_optional_timestamp(event.process_ts, "process_ts", now=now)
    if event.receive_ts is not None and event.process_ts is not None and event.process_ts < event.receive_ts:
        raise ValueError("process_ts cannot be earlier than receive_ts")
    for name, value in {
        "bid_price": event.bid_price,
        "bid_size": event.bid_size,
        "ask_price": event.ask_price,
        "ask_size": event.ask_size,
    }.items():
        _validate_non_negative_finite(value, name)
    if event.ask_price and event.bid_price and event.ask_price < event.bid_price:
        raise ValueError("ask_price cannot be below bid_price")


def validate_ingestion_event(event: IngestionEvent, *, now: datetime | None = None) -> None:
    if isinstance(event, TradeEvent):
        validate_trade_event(event, now=now)
        return
    if isinstance(event, BarEvent):
        validate_bar_event(event, now=now)
        return
    if isinstance(event, BookEvent):
        validate_book_event(event, now=now)
        return
    if not isinstance(event, MarketEvent):
        raise TypeError(f"unsupported ingestion event type: {type(event)!r}")
    if event.source == "trade":
        validate_trade_event(legacy_market_event_to_trade(event), now=now)
        return
    if event.source == "kline":
        validate_bar_event(
            legacy_market_event_to_bar(event, close_ts=event.event_ts),
            now=now,
        )
        return
    if event.source == "book":
        ask_price_raw = event.metadata.get("ask_price", event.price)
        ask_size_raw = event.metadata.get("ask_size", event.size)
        validate_book_event(
            BookEvent(
                symbol=event.symbol,
                exchange_ts=event.event_ts,
                venue=event.metadata.get("venue", "BINANCE"),
                bid_price=float(event.price),
                bid_size=float(event.size),
                ask_price=float(ask_price_raw),
                ask_size=float(ask_size_raw),
                sequence_id=event.metadata.get("sequence_id"),
                metadata=event.metadata,
            ),
            now=now,
        )
        return
    _validate_symbol(event.symbol)
    _validate_timestamp_sane(event.event_ts, "event_ts", now=now)
    _validate_non_negative_finite(event.price, "price")
    _validate_non_negative_finite(event.size, "size")

"""
Canonical typed market data models plus temporary legacy adapters.

`TradeEvent` and `BarEvent` are the only canonical event types currently
supported by ingestion/storage/runtime. `BookEvent` remains as an experimental
placeholder for future depth/quote work and is intentionally out of scope for
the supported ingestion surface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Literal, TypeAlias

from app.common.dto import MarketEvent, normalize_symbol
from app.common.validator import ensure_aware_utc, validate_price_size


def _validate_optional_ts(ts: datetime | None, name: str) -> None:
    if ts is None:
        return
    ensure_aware_utc(ts)


def _normalize_metadata(metadata: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in metadata.items()}


def _metadata_ts(metadata: dict[str, str], key: str) -> datetime | None:
    raw = metadata.get(key)
    if raw in (None, ""):
        return None
    return datetime.fromisoformat(raw)


BarVolumeKind: TypeAlias = Literal["base", "quote", "contracts"]


@dataclass(slots=True, kw_only=True)
class BaseMarketEvent:
    symbol: str
    exchange_ts: datetime
    provider_ts: datetime | None = None
    receive_ts: datetime | None = None
    process_ts: datetime | None = None
    venue: str = "BINANCE"
    source_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    event_type: ClassVar[str] = "base"
    source: ClassVar[str] = "base"

    def __post_init__(self) -> None:
        self.symbol = normalize_symbol(self.symbol)
        ensure_aware_utc(self.exchange_ts)
        _validate_optional_ts(self.provider_ts, "provider_ts")
        _validate_optional_ts(self.receive_ts, "receive_ts")
        _validate_optional_ts(self.process_ts, "process_ts")
        self.venue = str(self.venue).upper()
        if not self.venue:
            raise ValueError("venue must be non-empty")
        if self.source_id is not None:
            self.source_id = str(self.source_id)
        self.metadata = _normalize_metadata(self.metadata)

    @property
    def event_ts(self) -> datetime:
        # Transitional alias for the legacy ingestion stack.
        return self.exchange_ts

    @property
    def observation_ts(self) -> datetime:
        return self.exchange_ts

    @property
    def published_ts(self) -> datetime:
        return self.provider_ts or self.exchange_ts

    @property
    def available_ts(self) -> datetime:
        # Features can only consume data once it has been received/processed locally.
        return self.process_ts or self.receive_ts or self.published_ts

    @property
    def has_explicit_available_ts(self) -> bool:
        return any(ts is not None for ts in (self.provider_ts, self.receive_ts, self.process_ts)) or any(
            key in self.metadata for key in ("published_ts", "available_ts", "receive_ts", "process_ts")
        )


@dataclass(slots=True, kw_only=True)
class TradeEvent(BaseMarketEvent):
    price: float
    size: float
    trade_id: str | None = None
    side: Literal["buy", "sell"] | None = None

    event_type: ClassVar[str] = "trade"
    source: ClassVar[str] = "trade"

    def __post_init__(self) -> None:
        BaseMarketEvent.__post_init__(self)
        validate_price_size(self.price, self.size)
        if self.trade_id is not None:
            self.trade_id = str(self.trade_id)
        if self.side is not None and self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy, sell, or None")


@dataclass(slots=True, kw_only=True)
class BarEvent(BaseMarketEvent):
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_kind: BarVolumeKind = "quote"
    interval: str = "1m"
    open_ts: datetime | None = None
    close_ts: datetime | None = None

    event_type: ClassVar[str] = "bar"
    source: ClassVar[str] = "kline"

    def __post_init__(self) -> None:
        BaseMarketEvent.__post_init__(self)
        for value_name, value in {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }.items():
            if value < 0:
                raise ValueError(f"{value_name} must be non-negative")
        if self.low > self.high:
            raise ValueError("low cannot exceed high")
        if not (self.low <= self.open <= self.high):
            raise ValueError("open must be within [low, high]")
        if not (self.low <= self.close <= self.high):
            raise ValueError("close must be within [low, high]")
        if self.volume_kind not in {"base", "quote", "contracts"}:
            raise ValueError("volume_kind must be base, quote, or contracts")
        if not self.interval:
            raise ValueError("interval must be non-empty")
        _validate_optional_ts(self.open_ts, "open_ts")
        _validate_optional_ts(self.close_ts, "close_ts")

    @property
    def price(self) -> float:
        # Transitional alias for legacy consumers; bars map to close price.
        return self.close

    @property
    def size(self) -> float:
        # Transitional alias for legacy consumers; bars map to volume.
        return self.volume


@dataclass(slots=True, kw_only=True)
class BookEvent(BaseMarketEvent):
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    sequence_id: str | None = None

    event_type: ClassVar[str] = "book"
    source: ClassVar[str] = "book"

    def __post_init__(self) -> None:
        BaseMarketEvent.__post_init__(self)
        for value_name, value in {
            "bid_price": self.bid_price,
            "bid_size": self.bid_size,
            "ask_price": self.ask_price,
            "ask_size": self.ask_size,
        }.items():
            if value < 0:
                raise ValueError(f"{value_name} must be non-negative")
        if self.bid_price and self.ask_price and self.ask_price < self.bid_price:
            raise ValueError("ask_price cannot be below bid_price")
        if self.sequence_id is not None:
            self.sequence_id = str(self.sequence_id)

    @property
    def price(self) -> float:
        if self.bid_price and self.ask_price:
            return (self.bid_price + self.ask_price) / 2.0
        return self.ask_price or self.bid_price

    @property
    def size(self) -> float:
        return self.bid_size + self.ask_size


CanonicalMarketEvent: TypeAlias = TradeEvent | BarEvent | BookEvent
IngestionEvent: TypeAlias = MarketEvent | CanonicalMarketEvent
SUPPORTED_MARKETDATA_SOURCES: tuple[str, ...] = ("trade", "kline")
EXPERIMENTAL_MARKETDATA_SOURCES: tuple[str, ...] = ("book",)


def is_supported_marketdata_source(source: str) -> bool:
    return str(source).lower() in SUPPORTED_MARKETDATA_SOURCES


def legacy_market_event_to_trade(
    event: MarketEvent,
    *,
    venue: str = "BINANCE",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
    trade_id: str | None = None,
    side: Literal["buy", "sell"] | None = None,
) -> TradeEvent:
    if event.source != "trade":
        raise ValueError(f"legacy event source must be trade, got {event.source}")
    metadata = dict(event.metadata)
    return TradeEvent(
        symbol=event.symbol,
        exchange_ts=event.event_ts,
        provider_ts=event.published_ts or _metadata_ts(metadata, "provider_ts"),
        receive_ts=receive_ts or event.available_ts or _metadata_ts(metadata, "receive_ts"),
        process_ts=process_ts or event.processed_ts or _metadata_ts(metadata, "process_ts"),
        venue=metadata.get("venue", venue),
        source_id=metadata.get("source_id") or trade_id,
        metadata=metadata,
        price=event.price,
        size=event.size,
        trade_id=trade_id or metadata.get("trade_id"),
        side=side or metadata.get("side"),
    )


def legacy_market_event_to_bar(
    event: MarketEvent,
    *,
    venue: str = "BINANCE",
    interval: str = "1m",
    receive_ts: datetime | None = None,
    process_ts: datetime | None = None,
    open_ts: datetime | None = None,
    close_ts: datetime | None = None,
) -> BarEvent:
    if event.source != "kline":
        raise ValueError(f"legacy event source must be kline, got {event.source}")
    metadata = dict(event.metadata)
    return BarEvent(
        symbol=event.symbol,
        exchange_ts=event.event_ts,
        provider_ts=event.published_ts or _metadata_ts(metadata, "provider_ts"),
        receive_ts=receive_ts or event.available_ts or _metadata_ts(metadata, "receive_ts"),
        process_ts=process_ts or event.processed_ts or _metadata_ts(metadata, "process_ts"),
        venue=metadata.get("venue", venue),
        metadata=metadata,
        open=float(metadata.get("open", event.price)),
        high=float(metadata.get("high", event.price)),
        low=float(metadata.get("low", event.price)),
        close=event.price,
        volume=event.size,
        volume_kind=metadata.get("volume_kind", "quote"),
        interval=metadata.get("interval", interval),
        open_ts=open_ts or _metadata_ts(metadata, "open_ts"),
        close_ts=close_ts or _metadata_ts(metadata, "close_ts") or event.event_ts,
    )


def typed_event_to_legacy(event: CanonicalMarketEvent) -> MarketEvent:
    if isinstance(event, TradeEvent):
        metadata = dict(event.metadata)
        metadata.setdefault("venue", event.venue)
        if event.provider_ts is not None:
            metadata.setdefault("provider_ts", event.provider_ts.isoformat())
        if event.receive_ts is not None:
            metadata.setdefault("receive_ts", event.receive_ts.isoformat())
        if event.process_ts is not None:
            metadata.setdefault("process_ts", event.process_ts.isoformat())
        if event.trade_id is not None:
            metadata.setdefault("trade_id", event.trade_id)
        if event.source_id is not None:
            metadata.setdefault("source_id", event.source_id)
        if event.side is not None:
            metadata.setdefault("side", event.side)
        return MarketEvent(
            symbol=event.symbol,
            event_ts=event.exchange_ts,
            price=event.price,
            size=event.size,
            source="trade",
            metadata=metadata,
            published_ts=event.published_ts,
            available_ts=event.available_ts,
            processed_ts=event.process_ts or event.available_ts,
            observation_ts=event.observation_ts,
        )
    if isinstance(event, BarEvent):
        metadata = dict(event.metadata)
        metadata.setdefault("venue", event.venue)
        metadata.setdefault("interval", event.interval)
        if event.provider_ts is not None:
            metadata.setdefault("provider_ts", event.provider_ts.isoformat())
        if event.receive_ts is not None:
            metadata.setdefault("receive_ts", event.receive_ts.isoformat())
        if event.process_ts is not None:
            metadata.setdefault("process_ts", event.process_ts.isoformat())
        if event.open_ts is not None:
            metadata.setdefault("open_ts", event.open_ts.isoformat())
        if event.close_ts is not None:
            metadata.setdefault("close_ts", event.close_ts.isoformat())
        metadata.setdefault("open", str(event.open))
        metadata.setdefault("high", str(event.high))
        metadata.setdefault("low", str(event.low))
        metadata.setdefault("volume_kind", event.volume_kind)
        return MarketEvent(
            symbol=event.symbol,
            event_ts=event.close_ts or event.exchange_ts,
            price=event.close,
            size=event.volume,
            source="kline",
            metadata=metadata,
            published_ts=event.published_ts,
            available_ts=event.available_ts,
            processed_ts=event.process_ts or event.available_ts,
            observation_ts=event.observation_ts,
        )
    metadata = dict(event.metadata)
    metadata.setdefault("venue", event.venue)
    if event.provider_ts is not None:
        metadata.setdefault("provider_ts", event.provider_ts.isoformat())
    if event.receive_ts is not None:
        metadata.setdefault("receive_ts", event.receive_ts.isoformat())
    if event.process_ts is not None:
        metadata.setdefault("process_ts", event.process_ts.isoformat())
    if event.sequence_id is not None:
        metadata.setdefault("sequence_id", event.sequence_id)
    if event.source_id is not None:
        metadata.setdefault("source_id", event.source_id)
    metadata.setdefault("ask_price", str(event.ask_price))
    metadata.setdefault("ask_size", str(event.ask_size))
    return MarketEvent(
        symbol=event.symbol,
        event_ts=event.exchange_ts,
        price=event.bid_price,
        size=event.bid_size,
        source="book",
        metadata=metadata,
        published_ts=event.published_ts,
        available_ts=event.available_ts,
        processed_ts=event.process_ts or event.available_ts,
        observation_ts=event.observation_ts,
    )


def ensure_legacy_market_event(event: IngestionEvent) -> MarketEvent:
    if isinstance(event, MarketEvent):
        return event
    return typed_event_to_legacy(event)

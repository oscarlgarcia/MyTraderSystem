"""
Shared event identity and bounded deduplication helpers.

The goal is operational consistency, not exactly-once semantics.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Hashable, Iterable, Mapping, Protocol

from app.common.dto import MarketEvent
from app.marketdata.models import BaseMarketEvent, BookEvent, IngestionEvent, TradeEvent

EventIdentity = tuple[Hashable, ...]
IdentityBuilder = Callable[[str, datetime, float, float, str], EventIdentity]
NowFn = Callable[[], float]

DEFAULT_DEDUP_TTL_SECONDS = 300.0
DEFAULT_DEDUP_MAX_ENTRIES = 4096


class IdentityProvider(Protocol):
    def from_event(self, event: IngestionEvent) -> EventIdentity: ...

    def from_fields(
        self,
        *,
        symbol: str,
        event_ts: datetime,
        price: float,
        size: float,
        source: str,
        venue: str | None = None,
        metadata: Mapping[str, object] | None = None,
        source_id: str | None = None,
    ) -> EventIdentity: ...


def _default_identity_builder(
    symbol: str,
    event_ts: datetime,
    price: float,
    size: float,
    source: str,
) -> EventIdentity:
    return ("heuristic", symbol, event_ts, price, size, source)


def _string_or_none(value: object | None) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _metadata_mapping(metadata: Mapping[str, object] | object | None) -> Mapping[str, object]:
    if metadata is None:
        return {}
    if isinstance(metadata, Mapping):
        return metadata
    if isinstance(metadata, list):
        out: dict[str, object] = {}
        for item in metadata:
            if isinstance(item, tuple) and len(item) == 2:
                out[str(item[0])] = item[1]
        return out
    return {}


def _native_identity(
    *,
    venue: str,
    symbol: str,
    source: str,
    native_kind: str,
    native_id: str,
) -> EventIdentity:
    return ("native", venue.upper(), symbol, source, native_kind, native_id)


@dataclass(frozen=True, slots=True)
class NativeAwareIdentityProvider:
    fallback_builder: IdentityBuilder = _default_identity_builder

    def from_event(self, event: IngestionEvent) -> EventIdentity:
        if isinstance(event, TradeEvent):
            native = self._native_identity_from_fields(
                symbol=event.symbol,
                source=event.source,
                venue=event.venue,
                metadata=event.metadata,
                source_id=event.trade_id or event.source_id,
            )
            if native is not None:
                return native
        elif isinstance(event, BookEvent):
            native = self._native_identity_from_fields(
                symbol=event.symbol,
                source=event.source,
                venue=event.venue,
                metadata=event.metadata,
                source_id=event.sequence_id or event.source_id,
            )
            if native is not None:
                return native
        elif isinstance(event, BaseMarketEvent):
            native = self._native_identity_from_fields(
                symbol=event.symbol,
                source=event.source,
                venue=event.venue,
                metadata=event.metadata,
                source_id=event.source_id,
            )
            if native is not None:
                return native
        elif isinstance(event, MarketEvent):
            native = self._native_identity_from_fields(
                symbol=event.symbol,
                source=event.source,
                metadata=event.metadata,
            )
            if native is not None:
                return native
        return self.from_fields(
            symbol=event.symbol,
            event_ts=event.event_ts,
            price=event.price,
            size=event.size,
            source=event.source,
            venue=getattr(event, "venue", None),
            metadata=getattr(event, "metadata", None),
            source_id=getattr(event, "source_id", None),
        )

    def from_fields(
        self,
        *,
        symbol: str,
        event_ts: datetime,
        price: float,
        size: float,
        source: str,
        venue: str | None = None,
        metadata: Mapping[str, object] | None = None,
        source_id: str | None = None,
    ) -> EventIdentity:
        native = self._native_identity_from_fields(
            symbol=symbol,
            source=source,
            venue=venue,
            metadata=metadata,
            source_id=source_id,
        )
        if native is not None:
            return native
        return self.fallback_builder(symbol, event_ts, price, size, source)

    @staticmethod
    def _native_identity_from_fields(
        *,
        symbol: str,
        source: str,
        venue: str | None = None,
        metadata: Mapping[str, object] | None = None,
        source_id: str | None = None,
    ) -> EventIdentity | None:
        metadata = _metadata_mapping(metadata)
        venue_name = str(venue or metadata.get("venue", "BINANCE")).upper()
        trade_id = _string_or_none(metadata.get("trade_id"))
        sequence_id = _string_or_none(metadata.get("sequence_id"))
        native_source_id = _string_or_none(source_id or metadata.get("source_id"))
        if trade_id is not None:
            return _native_identity(
                venue=venue_name,
                symbol=symbol,
                source=source,
                native_kind="trade_id",
                native_id=trade_id,
            )
        if sequence_id is not None:
            return _native_identity(
                venue=venue_name,
                symbol=symbol,
                source=source,
                native_kind="sequence_id",
                native_id=sequence_id,
            )
        if native_source_id is not None:
            return _native_identity(
                venue=venue_name,
                symbol=symbol,
                source=source,
                native_kind="source_id",
                native_id=native_source_id,
            )
        return None


@dataclass(frozen=True, slots=True)
class BuilderIdentityProvider:
    builder: IdentityBuilder

    def from_event(self, event: IngestionEvent) -> EventIdentity:
        return self.from_fields(
            symbol=event.symbol,
            event_ts=event.event_ts,
            price=event.price,
            size=event.size,
            source=event.source,
            venue=getattr(event, "venue", None),
            metadata=getattr(event, "metadata", None),
            source_id=getattr(event, "source_id", None),
        )

    def from_fields(
        self,
        *,
        symbol: str,
        event_ts: datetime,
        price: float,
        size: float,
        source: str,
        venue: str | None = None,
        metadata: Mapping[str, object] | None = None,
        source_id: str | None = None,
    ) -> EventIdentity:
        del venue, metadata, source_id
        return self.builder(symbol, event_ts, price, size, source)


IDENTITY_PROVIDERS: dict[str, IdentityProvider] = {}
DEFAULT_IDENTITY_PROVIDER: IdentityProvider = NativeAwareIdentityProvider()


def register_identity_provider(source: str, provider: IdentityProvider) -> None:
    IDENTITY_PROVIDERS[source] = provider


def register_identity_builder(source: str, fn: IdentityBuilder) -> None:
    register_identity_provider(source, BuilderIdentityProvider(fn))


def identity_from_fields(
    *,
    symbol: str,
    event_ts: datetime,
    price: float,
    size: float,
    source: str,
    venue: str | None = None,
    metadata: Mapping[str, object] | None = None,
    source_id: str | None = None,
) -> EventIdentity:
    provider = IDENTITY_PROVIDERS.get(source, DEFAULT_IDENTITY_PROVIDER)
    return provider.from_fields(
        symbol=symbol,
        event_ts=event_ts,
        price=price,
        size=size,
        source=source,
        venue=venue,
        metadata=metadata,
        source_id=source_id,
    )


def identity_from_event(event: IngestionEvent) -> EventIdentity:
    provider = IDENTITY_PROVIDERS.get(event.source, DEFAULT_IDENTITY_PROVIDER)
    return provider.from_event(event)


def serialize_identity(key: EventIdentity) -> dict[str, object]:
    kind = str(key[0])
    if kind not in {"native", "heuristic"} and len(key) == 5:
        return {
            "kind": "heuristic",
            "symbol": str(key[0]),
            "event_ts": key[1].isoformat(),
            "price": float(key[2]),
            "size": float(key[3]),
            "source": str(key[4]),
        }
    if kind == "native":
        return {
            "kind": "native",
            "venue": str(key[1]),
            "symbol": str(key[2]),
            "source": str(key[3]),
            "native_kind": str(key[4]),
            "native_id": str(key[5]),
        }
    if kind == "heuristic":
        return {
            "kind": "heuristic",
            "symbol": str(key[1]),
            "event_ts": key[2].isoformat(),
            "price": float(key[3]),
            "size": float(key[4]),
            "source": str(key[5]),
        }
    raise ValueError(f"unsupported identity kind: {kind}")


def deserialize_identity(raw: Mapping[str, object]) -> EventIdentity:
    kind = str(raw["kind"])
    if kind == "native":
        return (
            "native",
            str(raw["venue"]),
            str(raw["symbol"]),
            str(raw["source"]),
            str(raw["native_kind"]),
            str(raw["native_id"]),
        )
    if kind == "heuristic":
        return (
            "heuristic",
            str(raw["symbol"]),
            datetime.fromisoformat(str(raw["event_ts"])),
            float(raw["price"]),
            float(raw["size"]),
            str(raw["source"]),
        )
    raise ValueError(f"unsupported identity kind: {kind}")


@dataclass(frozen=True, slots=True)
class DedupStateEntry:
    key: EventIdentity
    seen_at: float


@dataclass
class Deduplicator:
    ttl_seconds: float | None = DEFAULT_DEDUP_TTL_SECONDS
    max_entries: int = DEFAULT_DEDUP_MAX_ENTRIES
    now_fn: NowFn = time.time
    _entries: OrderedDict[EventIdentity, float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self._entries = OrderedDict()

    def clone_empty(self) -> "Deduplicator":
        return Deduplicator(
            ttl_seconds=self.ttl_seconds,
            max_entries=self.max_entries,
            now_fn=self.now_fn,
        )

    def is_duplicate(self, event: IngestionEvent, *, observed_at: float | None = None) -> bool:
        return self.contains_key(identity_from_event(event), observed_at=observed_at)

    def contains_key(self, key: EventIdentity, *, observed_at: float | None = None) -> bool:
        now = self.now_fn() if observed_at is None else observed_at
        self._evict_expired(now)
        if key not in self._entries:
            return False
        self._entries.move_to_end(key)
        self._entries[key] = now
        return True

    def remember(self, event: IngestionEvent, *, observed_at: float | None = None) -> None:
        self.remember_key(identity_from_event(event), observed_at=observed_at)

    def remember_key(self, key: EventIdentity, *, observed_at: float | None = None) -> None:
        now = self.now_fn() if observed_at is None else observed_at
        self._evict_expired(now)
        if key in self._entries:
            self._entries.move_to_end(key)
        self._entries[key] = now
        self._evict_capacity()

    def restore_entries(self, entries: Iterable[DedupStateEntry]) -> None:
        self._entries.clear()
        sorted_entries = sorted(entries, key=lambda entry: entry.seen_at)
        now = self.now_fn()
        for entry in sorted_entries:
            if self.ttl_seconds is not None and now - entry.seen_at > self.ttl_seconds:
                continue
            self._entries[entry.key] = entry.seen_at
        self._evict_capacity()

    def export_entries(self) -> tuple[DedupStateEntry, ...]:
        now = self.now_fn()
        self._evict_expired(now)
        return tuple(DedupStateEntry(key=key, seen_at=seen_at) for key, seen_at in self._entries.items())

    def __len__(self) -> int:
        return len(self._entries)

    def _evict_expired(self, now: float) -> None:
        if self.ttl_seconds is None:
            return
        while self._entries:
            first_key = next(iter(self._entries))
            first_seen = self._entries[first_key]
            if now - first_seen <= self.ttl_seconds:
                break
            self._entries.popitem(last=False)

    def _evict_capacity(self) -> None:
        while len(self._entries) > max(1, self.max_entries):
            self._entries.popitem(last=False)


def deduplicate_events(
    events: list[IngestionEvent],
    *,
    deduplicator: Deduplicator | None = None,
) -> tuple[list[IngestionEvent], int]:
    dedup = deduplicator or Deduplicator(ttl_seconds=None, max_entries=max(len(events), DEFAULT_DEDUP_MAX_ENTRIES))
    unique: list[IngestionEvent] = []
    dropped = 0
    for event in events:
        if dedup.is_duplicate(event):
            dropped += 1
            continue
        dedup.remember(event)
        unique.append(event)
    return unique, dropped

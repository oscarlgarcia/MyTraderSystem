"""
Shared event identity and bounded deduplication helpers.

The goal is operational consistency, not exactly-once semantics.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Hashable, Iterable

from app.common.dto import MarketEvent

EventIdentity = Hashable
IdentityBuilder = Callable[[str, datetime, float, float, str], EventIdentity]
NowFn = Callable[[], float]

DEFAULT_DEDUP_TTL_SECONDS = 300.0
DEFAULT_DEDUP_MAX_ENTRIES = 4096


def _default_identity_builder(
    symbol: str,
    event_ts: datetime,
    price: float,
    size: float,
    source: str,
) -> EventIdentity:
    return (symbol, event_ts, price, size, source)


IDENTITY_BUILDERS: dict[str, IdentityBuilder] = {}


def register_identity_builder(source: str, fn: IdentityBuilder) -> None:
    IDENTITY_BUILDERS[source] = fn


def identity_from_fields(
    *,
    symbol: str,
    event_ts: datetime,
    price: float,
    size: float,
    source: str,
) -> EventIdentity:
    builder = IDENTITY_BUILDERS.get(source, _default_identity_builder)
    return builder(symbol, event_ts, price, size, source)


def identity_from_event(event: MarketEvent) -> EventIdentity:
    return identity_from_fields(
        symbol=event.symbol,
        event_ts=event.event_ts,
        price=event.price,
        size=event.size,
        source=event.source,
    )


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

    def is_duplicate(self, event: MarketEvent, *, observed_at: float | None = None) -> bool:
        return self.contains_key(identity_from_event(event), observed_at=observed_at)

    def contains_key(self, key: EventIdentity, *, observed_at: float | None = None) -> bool:
        now = self.now_fn() if observed_at is None else observed_at
        self._evict_expired(now)
        if key not in self._entries:
            return False
        # Refresh the entry to keep immediate duplicate storms bounded by the same TTL window.
        self._entries.move_to_end(key)
        self._entries[key] = now
        return True

    def remember(self, event: MarketEvent, *, observed_at: float | None = None) -> None:
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
    events: list[MarketEvent],
    *,
    deduplicator: Deduplicator | None = None,
) -> tuple[list[MarketEvent], int]:
    dedup = deduplicator or Deduplicator(ttl_seconds=None, max_entries=max(len(events), DEFAULT_DEDUP_MAX_ENTRIES))
    unique: list[MarketEvent] = []
    dropped = 0
    for event in events:
        if dedup.is_duplicate(event):
            dropped += 1
            continue
        dedup.remember(event)
        unique.append(event)
    return unique, dropped

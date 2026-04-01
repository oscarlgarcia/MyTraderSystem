"""
Feed-specific recovery policies for ingestion gaps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Protocol

from app.common.dto import MarketEvent
from app.marketdata.models import BarEvent, IngestionEvent
from app.marketdata.temporal_state import TemporalPartitionKey

LIVE_RECOVERY_SCOPE = ("kline",)


class RecoveryPolicy(Protocol):
    name: str

    def can_recover(self, snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None) -> bool: ...

    def recover(
        self,
        snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
    ) -> Iterable[IngestionEvent]: ...


def _event_partition_matches(event: IngestionEvent, partition: TemporalPartitionKey) -> bool:
    venue = getattr(event, "venue", "BINANCE")
    stream_type = getattr(event, "source", getattr(event, "event_type", "trade"))
    return (
        str(venue).upper() == partition.venue
        and event.symbol == partition.symbol
        and str(stream_type) == partition.stream_type
    )


@dataclass(frozen=True, slots=True)
class TradeRecoveryPolicy:
    name: str = "trade_recovery_policy"

    def can_recover(self, snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None) -> bool:
        del snapshot_fn
        return False

    def recover(
        self,
        snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
    ) -> Iterable[IngestionEvent]:
        del snapshot_fn, partition
        return ()


@dataclass(frozen=True, slots=True)
class BarRecoveryPolicy:
    name: str = "bar_recovery_policy"

    def can_recover(self, snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None) -> bool:
        return snapshot_fn is not None

    def recover(
        self,
        snapshot_fn: Callable[[], Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
    ) -> Iterable[IngestionEvent]:
        if snapshot_fn is None:
            return ()
        recovered: list[IngestionEvent] = []
        for event in snapshot_fn():
            if not _event_partition_matches(event, partition):
                continue
            if isinstance(event, (BarEvent, MarketEvent)) and getattr(event, "source", None) == "kline":
                recovered.append(event)
        return recovered


def recovery_policy_for_event(event: IngestionEvent) -> RecoveryPolicy:
    source = getattr(event, "source", getattr(event, "event_type", "trade"))
    if source == "kline":
        return BarRecoveryPolicy()
    return TradeRecoveryPolicy()


def supports_live_recovery(feed_type: str) -> bool:
    return str(feed_type).strip().lower() in LIVE_RECOVERY_SCOPE

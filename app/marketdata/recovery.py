"""
Feed-specific recovery policies for ingestion gaps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import inspect
from typing import Callable, Iterable, Protocol

from app.common.dto import MarketEvent
from app.marketdata.gaps import GapObservation
from app.marketdata.models import BarEvent, IngestionEvent
from app.marketdata.temporal_state import TemporalPartitionKey

LIVE_RECOVERY_SCOPE = ("kline",)


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    partition: TemporalPartitionKey
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    interval: str | None = None
    limit: int | None = None
    cursor_kind: str | None = None
    cursor_value: str | None = None
    gap_seconds: float = 0.0
    missing_count: int = 0
    reason: str | None = None


class RecoveryPolicy(Protocol):
    name: str

    def can_recover(self, snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None) -> bool: ...

    def recover(
        self,
        snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
        request: RecoveryRequest | None = None,
    ) -> Iterable[IngestionEvent]: ...


def _interval_to_ms(interval: str) -> int:
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
    }
    return mapping.get(interval, 60_000)


def _interval_for_event(event: IngestionEvent) -> str | None:
    interval = getattr(event, "interval", None)
    if interval:
        return str(interval)
    metadata = getattr(event, "metadata", {})
    if isinstance(metadata, dict):
        raw = metadata.get("interval")
        if raw not in (None, ""):
            return str(raw)
    return None


def _snapshot_window_limit(*, previous_ts: datetime | None, current_ts: datetime, interval: str | None) -> int | None:
    if previous_ts is None or interval is None:
        return None
    interval_ms = _interval_to_ms(interval)
    delta_ms = max(0, int((current_ts - previous_ts).total_seconds() * 1000))
    # Request both edges plus the interior gap rows so dedup can safely remove overlap.
    return max(2, (delta_ms // interval_ms) + 1)


def build_recovery_request(
    event: IngestionEvent,
    *,
    partition: TemporalPartitionKey,
    previous_ts: datetime | None,
    gap_observation: GapObservation,
) -> RecoveryRequest | None:
    stream_type = getattr(event, "source", getattr(event, "event_type", "trade"))
    if str(stream_type) != "kline":
        return None
    interval = _interval_for_event(event) or "1m"
    return RecoveryRequest(
        partition=partition,
        start_ts=previous_ts,
        end_ts=event.event_ts,
        interval=interval,
        limit=_snapshot_window_limit(previous_ts=previous_ts, current_ts=event.event_ts, interval=interval),
        cursor_kind=None,
        cursor_value=None,
        gap_seconds=gap_observation.gap_seconds,
        missing_count=gap_observation.missing_count,
        reason=gap_observation.mode,
    )


def _call_snapshot_fn(
    snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None,
    *,
    request: RecoveryRequest | None,
) -> Iterable[IngestionEvent]:
    if snapshot_fn is None:
        return ()
    try:
        parameters = inspect.signature(snapshot_fn).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "request" in parameters or any(param.kind is inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
        return snapshot_fn(request=request)
    return snapshot_fn()


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

    def can_recover(self, snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None) -> bool:
        del snapshot_fn
        return False

    def recover(
        self,
        snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
        request: RecoveryRequest | None = None,
    ) -> Iterable[IngestionEvent]:
        del snapshot_fn, partition, request
        return ()


@dataclass(frozen=True, slots=True)
class BarRecoveryPolicy:
    name: str = "bar_recovery_policy"

    def can_recover(self, snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None) -> bool:
        return snapshot_fn is not None

    def recover(
        self,
        snapshot_fn: Callable[..., Iterable[IngestionEvent]] | None,
        *,
        partition: TemporalPartitionKey,
        request: RecoveryRequest | None = None,
    ) -> Iterable[IngestionEvent]:
        if snapshot_fn is None:
            return ()
        recovered: list[IngestionEvent] = []
        for event in _call_snapshot_fn(snapshot_fn, request=request):
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

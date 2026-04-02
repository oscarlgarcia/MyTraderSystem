"""
Feed-specific recovery policies for ingestion gaps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


@dataclass(frozen=True, slots=True)
class RecoveryVerification:
    exact: bool
    expected_rows: int
    received_rows: int
    missing_timestamps: tuple[datetime, ...] = ()
    unexpected_timestamps: tuple[datetime, ...] = ()
    duplicate_timestamps: tuple[datetime, ...] = ()


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


def _interval_to_delta(interval: str | None) -> timedelta:
    return timedelta(milliseconds=_interval_to_ms(interval or "1m"))


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


def _snapshot_window_limit(*, start_ts: datetime | None, end_ts: datetime | None, interval: str | None) -> int | None:
    if start_ts is None or end_ts is None or interval is None:
        return None
    interval_ms = _interval_to_ms(interval)
    delta_ms = max(0, int((end_ts - start_ts).total_seconds() * 1000))
    # Request both edges plus the interior gap rows so dedup can safely remove overlap.
    return max(2, (delta_ms // interval_ms) + 1)


def _ts_from_ms_str(raw: str | None) -> datetime | None:
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError, OSError):
        return None
    # Binance kline open times arrive as epoch-milliseconds. Small numeric
    # identifiers from legacy fixtures or other feeds are not timestamp cursors.
    if value < 946684800000:  # 2000-01-01T00:00:00Z
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _bar_open_ts(event: IngestionEvent) -> datetime | None:
    if isinstance(event, BarEvent) and event.open_ts is not None:
        return event.open_ts
    source_id = getattr(event, "source_id", None)
    if source_id not in (None, ""):
        parsed = _ts_from_ms_str(str(source_id))
        if parsed is not None:
            return parsed
    metadata = getattr(event, "metadata", {})
    if isinstance(metadata, dict):
        raw_open_ts = metadata.get("open_ts")
        if raw_open_ts not in (None, ""):
            try:
                return datetime.fromisoformat(str(raw_open_ts))
            except ValueError:
                return None
    return None


def _expected_window_timestamps(request: RecoveryRequest) -> tuple[datetime, ...]:
    if request.start_ts is None or request.end_ts is None or not request.interval:
        return ()
    if request.end_ts < request.start_ts:
        return ()
    step = _interval_to_delta(request.interval)
    current = request.start_ts
    expected: list[datetime] = []
    while current <= request.end_ts:
        expected.append(current)
        current += step
    return tuple(expected)


def build_recovery_request(
    event: IngestionEvent,
    *,
    partition: TemporalPartitionKey,
    previous_ts: datetime | None,
    gap_observation: GapObservation,
    previous_cursor_kind: str | None = None,
    previous_cursor_value: str | None = None,
) -> RecoveryRequest | None:
    stream_type = getattr(event, "source", getattr(event, "event_type", "trade"))
    if str(stream_type) != "kline":
        return None
    interval = _interval_for_event(event) or "1m"
    current_open_ts = _bar_open_ts(event)
    previous_open_ts = None
    if previous_cursor_kind == "source_id":
        previous_open_ts = _ts_from_ms_str(previous_cursor_value)
    if previous_open_ts is not None and current_open_ts is not None:
        start_ts = previous_open_ts
        end_ts = current_open_ts
        cursor_kind = "source_id"
        cursor_value = previous_cursor_value
    else:
        start_ts = previous_ts
        end_ts = event.event_ts
        cursor_kind = None
        cursor_value = None
    return RecoveryRequest(
        partition=partition,
        start_ts=start_ts,
        end_ts=end_ts,
        interval=interval,
        limit=_snapshot_window_limit(start_ts=start_ts, end_ts=end_ts, interval=interval),
        cursor_kind=cursor_kind,
        cursor_value=cursor_value,
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


def verify_recovery_window(
    *,
    partition: TemporalPartitionKey,
    request: RecoveryRequest | None,
    recovered_events: Iterable[IngestionEvent],
) -> RecoveryVerification:
    events = list(recovered_events)
    if request is None:
        return RecoveryVerification(exact=True, expected_rows=0, received_rows=len(events))
    if partition.stream_type != "kline":
        expected_rows = int(request.limit or 0)
        return RecoveryVerification(
            exact=len(events) >= expected_rows if expected_rows else True,
            expected_rows=expected_rows,
            received_rows=len(events),
        )

    expected = _expected_window_timestamps(request)
    actual = [timestamp for event in events if (timestamp := _bar_open_ts(event)) is not None]
    if not expected:
        return RecoveryVerification(exact=True, expected_rows=0, received_rows=len(actual))

    actual_sorted = tuple(sorted(actual))
    expected_set = set(expected)
    actual_set = set(actual_sorted)
    duplicates: list[datetime] = []
    seen: set[datetime] = set()
    for timestamp in actual_sorted:
        if timestamp in seen and timestamp not in duplicates:
            duplicates.append(timestamp)
        seen.add(timestamp)
    missing = tuple(timestamp for timestamp in expected if timestamp not in actual_set)
    unexpected = tuple(timestamp for timestamp in actual_sorted if timestamp not in expected_set)
    exact = not missing and not unexpected and actual_set == expected_set
    return RecoveryVerification(
        exact=exact,
        expected_rows=len(expected),
        received_rows=len(actual_sorted),
        missing_timestamps=missing,
        unexpected_timestamps=unexpected,
        duplicate_timestamps=tuple(duplicates),
    )

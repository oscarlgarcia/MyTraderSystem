"""
Temporal watermark and ordering state partitioned by venue, symbol, and stream type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.common.dto import MarketEvent
from app.marketdata.models import BaseMarketEvent, IngestionEvent


@dataclass(frozen=True, slots=True)
class TemporalPartitionKey:
    venue: str
    symbol: str
    stream_type: str

    def label(self) -> str:
        return f"{self.venue}:{self.symbol}:{self.stream_type}"


@dataclass(slots=True)
class TemporalStreamState:
    last_event_ts: datetime | None = None
    cursor_kind: str | None = None
    cursor_value: str | None = None
    messages_in_total: int = 0
    invalid_timestamp_total: int = 0
    duplicates_total: int = 0
    reconnects_total: int = 0
    heartbeat_missed_total: int = 0
    buffer_dropped_total: int = 0
    raw_write_latency: float = 0.0
    normalized_write_latency: float = 0.0
    exchange_receive_skew_seconds: float = 0.0
    receive_process_skew_seconds: float = 0.0
    recovery_window_rows_requested: int = 0
    recovery_window_rows_received: int = 0
    recovery_exactness_violation_total: int = 0
    last_recovery_request_start_ts: datetime | None = None
    last_recovery_request_end_ts: datetime | None = None
    last_recovery_cursor_before_kind: str | None = None
    last_recovery_cursor_before_value: str | None = None
    last_recovery_cursor_after_kind: str | None = None
    last_recovery_cursor_after_value: str | None = None
    last_recovery_rows_delivered: int = 0
    gap_detected: bool = False
    gap_irreparable: bool = False
    gaps_total: int = 0
    gap_irreparable_total: int = 0
    last_gap_detection_mode: str | None = None
    last_gap_missing_count: int = 0
    last_gap_seconds: float = 0.0
    last_event_gap_seconds: float = 0.0
    max_event_gap_seconds: float = 0.0
    late_events: int = 0
    out_of_order_events: int = 0
    late_events_dropped: int = 0
    last_late_seconds: float = 0.0
    max_late_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class CursorState:
    partition: TemporalPartitionKey
    last_event_ts: datetime | None = None
    cursor_kind: str | None = None
    cursor_value: str | None = None
    seen_entries: tuple[object, ...] = ()


@dataclass(slots=True)
class TemporalStateStore:
    states: dict[TemporalPartitionKey, TemporalStreamState] = field(default_factory=dict)

    def state_for_event(self, event: IngestionEvent) -> tuple[TemporalPartitionKey, TemporalStreamState]:
        key = temporal_partition_key(event)
        return key, self.states.setdefault(key, TemporalStreamState())

    def max_last_event_ts(self) -> datetime | None:
        timestamps = [state.last_event_ts for state in self.states.values() if state.last_event_ts is not None]
        if not timestamps:
            return None
        return max(timestamps)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        for key, state in self.states.items():
            payload[key.label()] = {
                "venue": key.venue,
                "symbol": key.symbol,
                "stream_type": key.stream_type,
                "messages_in_total": state.messages_in_total,
                "invalid_timestamp_total": state.invalid_timestamp_total,
                "duplicates_total": state.duplicates_total,
                "reconnects_total": state.reconnects_total,
                "heartbeat_missed_total": state.heartbeat_missed_total,
                "buffer_dropped_total": state.buffer_dropped_total,
                "raw_write_latency": state.raw_write_latency,
                "normalized_write_latency": state.normalized_write_latency,
                "exchange_receive_skew_seconds": state.exchange_receive_skew_seconds,
                "receive_process_skew_seconds": state.receive_process_skew_seconds,
                "recovery_window_rows_requested": state.recovery_window_rows_requested,
                "recovery_window_rows_received": state.recovery_window_rows_received,
                "recovery_exactness_violation_total": state.recovery_exactness_violation_total,
                "last_recovery_request_start_ts": state.last_recovery_request_start_ts.isoformat() if state.last_recovery_request_start_ts else None,
                "last_recovery_request_end_ts": state.last_recovery_request_end_ts.isoformat() if state.last_recovery_request_end_ts else None,
                "last_recovery_cursor_before_kind": state.last_recovery_cursor_before_kind,
                "last_recovery_cursor_before_value": state.last_recovery_cursor_before_value,
                "last_recovery_cursor_after_kind": state.last_recovery_cursor_after_kind,
                "last_recovery_cursor_after_value": state.last_recovery_cursor_after_value,
                "last_recovery_rows_delivered": state.last_recovery_rows_delivered,
                "last_event_ts": state.last_event_ts.isoformat() if state.last_event_ts else None,
                "cursor_kind": state.cursor_kind,
                "cursor_value": state.cursor_value,
                "gap_detected": state.gap_detected,
                "gap_irreparable": state.gap_irreparable,
                "gaps_total": state.gaps_total,
                "gap_irreparable_total": state.gap_irreparable_total,
                "last_gap_detection_mode": state.last_gap_detection_mode,
                "last_gap_missing_count": state.last_gap_missing_count,
                "last_gap_seconds": state.last_gap_seconds,
                "last_event_gap_seconds": state.last_event_gap_seconds,
                "max_event_gap_seconds": state.max_event_gap_seconds,
                "late_events": state.late_events,
                "out_of_order_events": state.out_of_order_events,
                "late_events_dropped": state.late_events_dropped,
                "last_late_seconds": state.last_late_seconds,
                "max_late_seconds": state.max_late_seconds,
            }
        return payload

    def restore_cursor_states(self, states: dict[str, CursorState]) -> None:
        for cursor_state in states.values():
            self.states[cursor_state.partition] = TemporalStreamState(
                last_event_ts=cursor_state.last_event_ts,
                cursor_kind=cursor_state.cursor_kind,
                cursor_value=cursor_state.cursor_value,
            )


def temporal_partition_key(event: IngestionEvent) -> TemporalPartitionKey:
    if isinstance(event, BaseMarketEvent):
        venue = event.venue
    elif isinstance(event, MarketEvent):
        venue = str(event.metadata.get("venue", "BINANCE")).upper()
    else:
        venue = "BINANCE"
    return TemporalPartitionKey(
        venue=str(venue).upper(),
        symbol=event.symbol,
        stream_type=event.source,
    )


def cursor_from_event(event: IngestionEvent) -> tuple[str | None, str | None]:
    if isinstance(event, BaseMarketEvent):
        metadata = event.metadata
        aggregate_trade_id = metadata.get("aggregate_trade_id")
        if aggregate_trade_id not in (None, ""):
            return "aggregate_trade_id", str(aggregate_trade_id)
        if getattr(event, "trade_id", None):
            return "trade_id", str(getattr(event, "trade_id"))
        if getattr(event, "sequence_id", None):
            return "sequence_id", str(getattr(event, "sequence_id"))
        if event.source_id:
            return "source_id", str(event.source_id)
    elif isinstance(event, MarketEvent):
        metadata = event.metadata
    else:
        metadata = {}
    for key in ("aggregate_trade_id", "trade_id", "sequence_id", "source_id"):
        value = metadata.get(key)
        if value not in (None, ""):
            return key, str(value)
    return None, None

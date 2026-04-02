"""
Resilient stream loop with simple reconnect/backoff and snapshot re-sync.

Designed to stay lightweight and testable (no threads, no event loop management here).
"""

from __future__ import annotations

import math
import time
import logging
import random
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Deque, Iterable, List, Literal, Optional

from app.ingestion.client import _key
from app.ingestion.checkpoints import CheckpointState
from app.ingestion.dedup import DedupStateEntry, Deduplicator, EventIdentity
from app.ingestion.errors import IngestionError, classify_error
from app.marketdata.gaps import detect_gap
from app.marketdata.models import BaseMarketEvent, IngestionEvent
from app.marketdata.recovery import RecoveryPolicy, RecoveryRequest, build_recovery_request, recovery_policy_for_event
from app.marketdata.temporal_state import (
    CursorState,
    TemporalPartitionKey,
    TemporalStateStore,
    TemporalStreamState,
    cursor_from_event,
)
from app.observability.alerts import emit_operational_alert
from app.observability.logger import get_trace_id


SnapshotFn = Callable[..., Iterable[IngestionEvent]]
StreamFn = Callable[[], Iterable[IngestionEvent]]
Sleeper = Callable[[float], None]
RecoveryPolicyResolver = Callable[[IngestionEvent], RecoveryPolicy]
JitterFn = Callable[[float], float]
BackpressurePolicy = Literal["pause", "drop_oldest", "drop_newest", "fail"]
TemporalPolicy = Literal["accept", "drop", "fail"]


def default_retry_jitter(delay_seconds: float) -> float:
    return delay_seconds * random.uniform(0.9, 1.1)

@dataclass
class ResilienceMetrics:
    # events_in counts source/snapshot events observed before runner-level dedup.
    events_in: int = 0
    # events_out counts events delivered to the handler after runner-level dedup.
    events_out: int = 0
    # dedup_skipped counts duplicates filtered by the runner or snapshot re-sync path.
    dedup_skipped: int = 0
    reconnects: int = 0
    # legacy alias kept for compatibility; mirrors max_event_gap_seconds.
    last_lag_seconds: float = 0.0
    last_event_gap_seconds: float = 0.0
    max_event_gap_seconds: float = 0.0
    late_events: int = 0
    out_of_order_events: int = 0
    late_events_dropped: int = 0
    last_late_seconds: float = 0.0
    max_late_seconds: float = 0.0
    buffer_size: int = 0
    buffer_skipped: int = 0
    buffer_overflows: int = 0
    buffer_pauses: int = 0
    buffer_drop_oldest: int = 0
    buffer_drop_newest: int = 0
    buffer_failures: int = 0
    snapshot_runs: int = 0
    snapshot_rows: int = 0
    snapshot_duplicates_skipped: int = 0
    gaps_total: int = 0
    gap_irreparable_total: int = 0
    last_latency_seconds: float = 0.0
    max_latency_seconds: float = 0.0
    exchange_receive_skew_seconds: float = 0.0
    receive_process_skew_seconds: float = 0.0
    recovery_window_rows_requested: int = 0
    recovery_window_rows_received: int = 0
    recovery_exactness_violation_total: int = 0
    checkpoint_restores: int = 0
    recovery_audit_events_total: int = 0
    temporal_streams: dict[str, dict[str, object]] = field(default_factory=dict)


@dataclass
class ResilientRunner:
    stream_fn: StreamFn
    snapshot_fn: Optional[SnapshotFn] = None
    backoff_base: float = 1.0
    backoff_max: float = 8.0  # keep <10s per requirement
    lag_threshold_seconds: float = 5.0
    max_lag_seconds: float = 10.0
    max_buffer: int = 10_000
    backpressure_policy: BackpressurePolicy = "pause"
    temporal_policy: TemporalPolicy = "accept"
    backpressure_pause_seconds: float = 0.01
    read_burst_size: int = 64
    dedup_enabled: bool = True
    sleeper: Sleeper = time.sleep
    jitter_fn: JitterFn = default_retry_jitter
    recovery_policy_resolver: RecoveryPolicyResolver = recovery_policy_for_event
    metrics: ResilienceMetrics = field(default_factory=ResilienceMetrics)
    deduplicator: Deduplicator = field(default_factory=Deduplicator)
    temporal_state: TemporalStateStore = field(default_factory=TemporalStateStore)
    checkpoint_last_event_ts: Optional[datetime] = None
    stream_seen_entries: dict[TemporalPartitionKey, OrderedDict[EventIdentity, float]] = field(default_factory=dict)
    recovery_audit_events: list[dict[str, object]] = field(default_factory=list)

    @property
    def last_event_ts(self) -> Optional[datetime]:
        return self.temporal_state.max_last_event_ts() or self.checkpoint_last_event_ts

    def restore_checkpoint(self, state: CheckpointState | None) -> None:
        if state is None:
            return
        self.metrics.checkpoint_restores += 1
        self.checkpoint_last_event_ts = state.last_event_ts
        if state.stream_cursors:
            self.temporal_state.restore_cursor_states(state.stream_cursors)
            self.stream_seen_entries = {}
            restored_entries: list[DedupStateEntry] = []
            for cursor_state in state.stream_cursors.values():
                ordered = OrderedDict((entry.key, entry.seen_at) for entry in cursor_state.seen_entries)
                self.stream_seen_entries[cursor_state.partition] = ordered
                restored_entries.extend(cursor_state.seen_entries)
            self.deduplicator.restore_entries(restored_entries)
            return
        self.deduplicator.restore_entries(state.seen_entries)

    def export_checkpoint(self, *, metadata: dict[str, object] | None = None) -> CheckpointState:
        stream_cursors = {
            partition.label(): CursorState(
                partition=partition,
                last_event_ts=state.last_event_ts,
                cursor_kind=state.cursor_kind,
                cursor_value=state.cursor_value,
                seen_entries=tuple(
                    DedupStateEntry(key=key, seen_at=seen_at)
                    for key, seen_at in self.stream_seen_entries.get(partition, OrderedDict()).items()
                ),
            )
            for partition, state in self.temporal_state.states.items()
        }
        return CheckpointState(
            last_event_ts=self.last_event_ts,
            seen_entries=self.deduplicator.export_entries(),
            stream_cursors=stream_cursors,
            metadata=dict(metadata or {}),
        )

    def run(
        self,
        handler: Callable[[IngestionEvent], None],
        max_retries: Optional[int] = None,
        stop_on_complete: bool = False,
    ) -> None:
        backoff = self.backoff_base
        buffer: Deque[IngestionEvent] = deque()
        while True:
            handled_this_cycle = 0
            try:
                stream_iter = iter(self.stream_fn())
                stream_exhausted = False
                while not stream_exhausted:
                    read_this_burst = 0
                    while read_this_burst < max(1, self.read_burst_size):
                        try:
                            ev = next(stream_iter)
                        except StopIteration:
                            stream_exhausted = True
                            break
                        except RuntimeError as exc:
                            if "StopIteration" in str(exc):
                                stream_exhausted = True
                                break
                            raise
                        self._enqueue_event(buffer, ev, handler)
                        self.metrics.buffer_size = len(buffer)
                        read_this_burst += 1
                        backoff = self.backoff_base  # reset on success
                    handled_this_cycle += self._drain_buffer(buffer, handler)
                    self.metrics.buffer_size = len(buffer)
                if stop_on_complete:
                    # If we are in a finite run mode and consumed the stream (even empty), exit.
                    break
            except StopIteration:
                break
            except Exception as exc:
                # Generators that explicitly raise StopIteration in Python 3.11+ surface as
                # RuntimeError("generator raised StopIteration") due to PEP 479. Treat that
                # as a clean completion when stop_on_complete is requested.
                if stop_on_complete and isinstance(exc, RuntimeError) and "StopIteration" in str(exc):
                    break
                err = classify_error(exc, default_category="source")
                if not err.retryable:
                    raise err
                self.metrics.reconnects += 1
                if max_retries is not None and self.metrics.reconnects > max_retries:
                    raise err
                sleep_seconds = min(self.backoff_max, max(0.0, self.jitter_fn(min(backoff, self.backoff_max))))
                self.sleeper(sleep_seconds)
                backoff = min(backoff * 2, self.backoff_max)
                continue

            # Extra guard: if stop_on_complete is requested and no events were processed (empty stream),
            # avoid looping forever.
            if stop_on_complete and handled_this_cycle == 0 and not buffer:
                break

    def _enqueue_event(
        self,
        buffer: Deque[IngestionEvent],
        ev: IngestionEvent,
        handler: Callable[[IngestionEvent], None],
    ) -> None:
        if len(buffer) < self.max_buffer:
            buffer.append(ev)
            return
        self.metrics.buffer_overflows += 1
        dropped_partition_key, dropped_stream_state = self.temporal_state.state_for_event(ev)
        logger = logging.getLogger("ingest.resilience")
        if self.backpressure_policy == "pause":
            self.metrics.buffer_pauses += 1
            self._drain_buffer(buffer, handler, limit=1)
            self.sleeper(self.backpressure_pause_seconds)
            buffer.append(ev)
            logger.warning(
                "backpressure pause applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_pauses": self.metrics.buffer_pauses,
                },
            )
            return
        if self.backpressure_policy == "drop_oldest":
            if buffer:
                dropped = buffer.popleft()
                dropped_partition_key, dropped_stream_state = self.temporal_state.state_for_event(dropped)
            self.metrics.buffer_skipped += 1
            self.metrics.buffer_drop_oldest += 1
            dropped_stream_state.buffer_dropped_total += 1
            self._update_temporal_metrics(dropped_partition_key, dropped_stream_state)
            buffer.append(ev)
            logger.warning(
                "backpressure drop_oldest applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_skipped": self.metrics.buffer_skipped,
                    "buffer_drop_oldest": self.metrics.buffer_drop_oldest,
                    "venue": dropped_partition_key.venue,
                    "symbol": dropped_partition_key.symbol,
                    "stream_type": dropped_partition_key.stream_type,
                },
            )
            return
        if self.backpressure_policy == "drop_newest":
            self.metrics.buffer_skipped += 1
            self.metrics.buffer_drop_newest += 1
            dropped_stream_state.buffer_dropped_total += 1
            self._update_temporal_metrics(dropped_partition_key, dropped_stream_state)
            logger.warning(
                "backpressure drop_newest applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_skipped": self.metrics.buffer_skipped,
                    "buffer_drop_newest": self.metrics.buffer_drop_newest,
                    "venue": dropped_partition_key.venue,
                    "symbol": dropped_partition_key.symbol,
                    "stream_type": dropped_partition_key.stream_type,
                },
            )
            return
        self.metrics.buffer_failures += 1
        raise IngestionError(
            "sink",
            "transient",
            f"buffer overloaded with fail policy (size={len(buffer)}, max_buffer={self.max_buffer})",
        )

    def _drain_buffer(
        self,
        buffer: Deque[IngestionEvent],
        handler: Callable[[IngestionEvent], None],
        *,
        limit: int | None = None,
    ) -> int:
        handled = 0
        while buffer and (limit is None or handled < limit):
            current = buffer.popleft()
            try:
                self._process_event(current, handler)
            except StopIteration:
                raise
            except RuntimeError as exc:
                if "StopIteration" in str(exc):
                    raise StopIteration from exc
                raise classify_error(exc, default_category="sink") from exc
            except Exception as exc:
                raise classify_error(exc, default_category="sink") from exc
            handled += 1
        return handled

    def _process_event(self, ev: IngestionEvent, handler: Callable[[IngestionEvent], None]) -> None:
        if isinstance(ev, BaseMarketEvent) and ev.process_ts is None:
            ev.process_ts = datetime.now(timezone.utc)
        self.metrics.events_in += 1
        k = _key(ev)
        partition_key, stream_state = self.temporal_state.state_for_event(ev)
        stream_state.messages_in_total += 1
        if isinstance(ev, BaseMarketEvent):
            exchange_receive_skew = max(0.0, (ev.receive_ts - ev.exchange_ts).total_seconds()) if ev.receive_ts is not None else 0.0
            receive_process_skew = max(0.0, (ev.process_ts - ev.receive_ts).total_seconds()) if ev.receive_ts is not None and ev.process_ts is not None else 0.0
            stream_state.exchange_receive_skew_seconds = max(
                stream_state.exchange_receive_skew_seconds,
                exchange_receive_skew,
            )
            stream_state.receive_process_skew_seconds = max(
                stream_state.receive_process_skew_seconds,
                receive_process_skew,
            )
            self.metrics.exchange_receive_skew_seconds = max(
                self.metrics.exchange_receive_skew_seconds,
                exchange_receive_skew,
            )
            self.metrics.receive_process_skew_seconds = max(
                self.metrics.receive_process_skew_seconds,
                receive_process_skew,
            )
        self._update_temporal_metrics(partition_key, stream_state)
        recovery_policy = self.recovery_policy_resolver(ev)
        # dedup
        if self.dedup_enabled:
            if self.deduplicator.contains_key(k):
                self.metrics.dedup_skipped += 1
                stream_state.duplicates_total += 1
                self._update_temporal_metrics(partition_key, stream_state)
                return

        # gap detection
        previous_ts = stream_state.last_event_ts or self.checkpoint_last_event_ts
        if previous_ts:
            delta_seconds = (ev.event_ts - previous_ts).total_seconds()
            if delta_seconds < 0:
                if not self._handle_out_of_order(ev, partition_key, stream_state):
                    return
            else:
                gap_observation = detect_gap(
                    stream_state=stream_state,
                    event=ev,
                    lag_threshold_seconds=self.lag_threshold_seconds,
                    recovery_available=recovery_policy.can_recover(self.snapshot_fn),
                )
                if gap_observation.detected:
                    self._record_gap(partition_key, stream_state, gap_observation)
                stream_state.last_event_gap_seconds = delta_seconds
                if delta_seconds > stream_state.max_event_gap_seconds:
                    stream_state.max_event_gap_seconds = delta_seconds
                self.metrics.last_event_gap_seconds = delta_seconds
                if delta_seconds > self.metrics.max_event_gap_seconds:
                    self.metrics.max_event_gap_seconds = delta_seconds
                self.metrics.last_lag_seconds = self.metrics.max_event_gap_seconds
                self._update_temporal_metrics(partition_key, stream_state)

                if delta_seconds > self.lag_threshold_seconds and recovery_policy.can_recover(self.snapshot_fn):
                    recovery_request = build_recovery_request(
                        ev,
                        partition=partition_key,
                        previous_ts=previous_ts,
                        gap_observation=gap_observation,
                    )
                    self._resync(
                        handler,
                        partition_key=partition_key,
                        recovery_policy=recovery_policy,
                        request=recovery_request,
                    )
                    # The snapshot may have already delivered this event (or a duplicate),
                    # so skip if it's now seen to avoid double handling.
                    if self.deduplicator.contains_key(k):
                        self.metrics.snapshot_duplicates_skipped += 1
                        return
            if delta_seconds > self.max_lag_seconds:
                # log via handler extra if it accepts 'warning' pattern? we can't assume; emit via logging module
                logging.getLogger("ingest.resilience").warning(
                    "Event gap exceeds max_lag_seconds",
                    extra={
                        "event_gap_seconds": delta_seconds,
                        "max_lag_seconds": self.max_lag_seconds,
                        "temporal_partition": partition_key.label(),
                    },
                )

        handler(ev)
        self.metrics.events_out += 1
        if self.dedup_enabled:
            self.deduplicator.remember_key(k)
            self._remember_stream_key(partition_key, k)
        cursor_kind, cursor_value = cursor_from_event(ev)
        if cursor_kind is not None:
            stream_state.cursor_kind = cursor_kind
            stream_state.cursor_value = cursor_value
        stream_state.last_event_ts = max(stream_state.last_event_ts or ev.event_ts, ev.event_ts)
        self._update_temporal_metrics(partition_key, stream_state)

        # latency from event_ts to processing time
        now = datetime.now(timezone.utc)
        latency = max(0.0, (now - ev.event_ts).total_seconds())
        self.metrics.last_latency_seconds = latency
        if latency > self.metrics.max_latency_seconds:
            self.metrics.max_latency_seconds = latency

    def _resync(
        self,
        handler: Callable[[IngestionEvent], None],
        *,
        partition_key: TemporalPartitionKey,
        recovery_policy: RecoveryPolicy,
        request: RecoveryRequest | None,
    ) -> None:
        if not recovery_policy.can_recover(self.snapshot_fn):
            return
        logger = logging.getLogger("ingest.resilience")
        stream_state = self.temporal_state.states.setdefault(partition_key, TemporalStreamState())
        cursor_before_kind = stream_state.cursor_kind
        cursor_before_value = stream_state.cursor_value
        last_event_ts_before = stream_state.last_event_ts.isoformat() if stream_state.last_event_ts else None
        if request is not None:
            stream_state.last_recovery_request_start_ts = request.start_ts
            stream_state.last_recovery_request_end_ts = request.end_ts
            stream_state.last_recovery_cursor_before_kind = cursor_before_kind
            stream_state.last_recovery_cursor_before_value = cursor_before_value
        logger.info(
            "recovery started",
            extra={
                "stream_key": partition_key.label(),
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "recovery_policy": recovery_policy.name,
                "recovery_request_start_ts": request.start_ts.isoformat() if request and request.start_ts else None,
                "recovery_request_end_ts": request.end_ts.isoformat() if request and request.end_ts else None,
                "recovery_request_interval": request.interval if request else None,
                "recovery_request_limit": request.limit if request else None,
            },
        )
        self.metrics.snapshot_runs += 1
        requested_rows = max(0, int(request.limit)) if request is not None and request.limit is not None else 0
        if requested_rows:
            self.metrics.recovery_window_rows_requested += requested_rows
            stream_state.recovery_window_rows_requested += requested_rows
            self._update_temporal_metrics(partition_key, stream_state)
        recovered_rows = 0
        recovered_events: list[IngestionEvent] = []
        try:
            recovered_events = list(recovery_policy.recover(self.snapshot_fn, partition=partition_key, request=request))
            if request is not None and request.limit is not None:
                self.metrics.recovery_window_rows_received += len(recovered_events)
                stream_state.recovery_window_rows_received += len(recovered_events)
                if len(recovered_events) < request.limit:
                    self._record_recovery_exactness_violation(
                        partition_key,
                        stream_state,
                        requested_rows=request.limit,
                        received_rows=len(recovered_events),
                        request=request,
                    )
                else:
                    self._update_temporal_metrics(partition_key, stream_state)
            for ev in recovered_events:
                event_partition_key, stream_state = self.temporal_state.state_for_event(ev)
                self.metrics.snapshot_rows += 1
                self.metrics.events_in += 1
                stream_state.messages_in_total += 1
                k = _key(ev)
                if self.dedup_enabled and self.deduplicator.contains_key(k):
                    self.metrics.dedup_skipped += 1
                    self.metrics.snapshot_duplicates_skipped += 1
                    stream_state.duplicates_total += 1
                    self._update_temporal_metrics(event_partition_key, stream_state)
                    continue
                handler(ev)
                self.metrics.events_out += 1
                recovered_rows += 1
                if self.dedup_enabled:
                    self.deduplicator.remember_key(k)
                    self._remember_stream_key(event_partition_key, k)
                cursor_kind, cursor_value = cursor_from_event(ev)
                if cursor_kind is not None:
                    stream_state.cursor_kind = cursor_kind
                    stream_state.cursor_value = cursor_value
                stream_state.last_event_ts = max(stream_state.last_event_ts or ev.event_ts, ev.event_ts)
                self._update_temporal_metrics(event_partition_key, stream_state)
        except Exception:
            logger.error(
                "recovery failed",
                extra={
                    "stream_key": partition_key.label(),
                    "venue": partition_key.venue,
                    "symbol": partition_key.symbol,
                    "stream_type": partition_key.stream_type,
                    "recovery_policy": recovery_policy.name,
                },
            )
            raise
        stream_state.last_recovery_cursor_after_kind = stream_state.cursor_kind
        stream_state.last_recovery_cursor_after_value = stream_state.cursor_value
        stream_state.last_recovery_rows_delivered = recovered_rows
        recovery_audit_event = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "trace_id": get_trace_id(),
            "stream_key": partition_key.label(),
            "recovery_policy": recovery_policy.name,
            "checkpoint_last_event_ts": self.checkpoint_last_event_ts.isoformat() if self.checkpoint_last_event_ts else None,
            "last_event_ts_before": last_event_ts_before,
            "last_event_ts_after": stream_state.last_event_ts.isoformat() if stream_state.last_event_ts else None,
            "cursor_before_kind": cursor_before_kind,
            "cursor_before_value": cursor_before_value,
            "cursor_after_kind": stream_state.cursor_kind,
            "cursor_after_value": stream_state.cursor_value,
            "recovery_request_start_ts": request.start_ts.isoformat() if request and request.start_ts else None,
            "recovery_request_end_ts": request.end_ts.isoformat() if request and request.end_ts else None,
            "recovery_request_interval": request.interval if request else None,
            "recovery_window_rows_requested": requested_rows,
            "recovery_window_rows_received": len(recovered_events),
            "recovered_rows_delivered": recovered_rows,
            "recovery_exactness_violated": bool(
                request is not None and request.limit is not None and len(recovered_events) < request.limit
            ),
        }
        self.recovery_audit_events.append(recovery_audit_event)
        self.metrics.recovery_audit_events_total += 1
        logger.info(
            "recovery completed",
            extra={
                "stream_key": partition_key.label(),
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "recovery_policy": recovery_policy.name,
                "recovered_rows": recovered_rows,
                "cursor_before_kind": cursor_before_kind,
                "cursor_before_value": cursor_before_value,
                "cursor_after_kind": stream_state.cursor_kind,
                "cursor_after_value": stream_state.cursor_value,
                "recovery_window_rows_requested": requested_rows or None,
                "recovery_window_rows_received": len(recovered_events) if request is not None and request.limit is not None else None,
                "snapshot_duplicates_skipped": self.metrics.snapshot_duplicates_skipped,
            },
        )

    def _handle_out_of_order(
        self,
        ev: IngestionEvent,
        partition_key: TemporalPartitionKey,
        stream_state: TemporalStreamState,
    ) -> bool:
        previous_ts = stream_state.last_event_ts or self.checkpoint_last_event_ts
        if previous_ts is None:
            return True
        late_seconds = max(0.0, (previous_ts - ev.event_ts).total_seconds())
        stream_state.out_of_order_events += 1
        stream_state.late_events += 1
        stream_state.last_late_seconds = late_seconds
        if late_seconds > stream_state.max_late_seconds:
            stream_state.max_late_seconds = late_seconds
        self.metrics.out_of_order_events += 1
        self.metrics.late_events += 1
        self.metrics.last_late_seconds = late_seconds
        if late_seconds > self.metrics.max_late_seconds:
            self.metrics.max_late_seconds = late_seconds
        self._update_temporal_metrics(partition_key, stream_state)

        logger = logging.getLogger("ingest.resilience")
        logger.warning(
            "Out-of-order event detected",
            extra={
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "temporal_policy": self.temporal_policy,
                "late_seconds": late_seconds,
                "last_event_ts": previous_ts.isoformat(),
                "event_ts": ev.event_ts.isoformat(),
                "temporal_partition": partition_key.label(),
            },
        )

        if self.temporal_policy == "accept":
            return True
        if self.temporal_policy == "drop":
            stream_state.late_events_dropped += 1
            self.metrics.late_events_dropped += 1
            self._update_temporal_metrics(partition_key, stream_state)
            return False
        raise IngestionError(
            "validation",
            "permanent",
            f"out-of-order event detected (late_seconds={late_seconds:.6f})",
        )

    def _update_temporal_metrics(
        self,
        partition_key: TemporalPartitionKey,
        stream_state: TemporalStreamState,
    ) -> None:
        self.metrics.temporal_streams[partition_key.label()] = {
            "stream_key": partition_key.label(),
            "venue": partition_key.venue,
            "symbol": partition_key.symbol,
            "stream_type": partition_key.stream_type,
            "messages_in_total": stream_state.messages_in_total,
            "invalid_timestamp_total": stream_state.invalid_timestamp_total,
            "duplicates_total": stream_state.duplicates_total,
            "reconnects_total": stream_state.reconnects_total,
            "heartbeat_missed_total": stream_state.heartbeat_missed_total,
            "buffer_dropped_total": stream_state.buffer_dropped_total,
            "raw_write_latency": stream_state.raw_write_latency,
            "normalized_write_latency": stream_state.normalized_write_latency,
            "exchange_receive_skew_seconds": stream_state.exchange_receive_skew_seconds,
            "receive_process_skew_seconds": stream_state.receive_process_skew_seconds,
            "recovery_window_rows_requested": stream_state.recovery_window_rows_requested,
            "recovery_window_rows_received": stream_state.recovery_window_rows_received,
            "recovery_exactness_violation_total": stream_state.recovery_exactness_violation_total,
            "last_recovery_request_start_ts": (
                stream_state.last_recovery_request_start_ts.isoformat()
                if stream_state.last_recovery_request_start_ts
                else None
            ),
            "last_recovery_request_end_ts": (
                stream_state.last_recovery_request_end_ts.isoformat()
                if stream_state.last_recovery_request_end_ts
                else None
            ),
            "last_recovery_cursor_before_kind": stream_state.last_recovery_cursor_before_kind,
            "last_recovery_cursor_before_value": stream_state.last_recovery_cursor_before_value,
            "last_recovery_cursor_after_kind": stream_state.last_recovery_cursor_after_kind,
            "last_recovery_cursor_after_value": stream_state.last_recovery_cursor_after_value,
            "last_recovery_rows_delivered": stream_state.last_recovery_rows_delivered,
            "last_event_ts": stream_state.last_event_ts.isoformat() if stream_state.last_event_ts else None,
            "cursor_kind": stream_state.cursor_kind,
            "cursor_value": stream_state.cursor_value,
            "gap_detected": stream_state.gap_detected,
            "gap_irreparable": stream_state.gap_irreparable,
            "gaps_total": stream_state.gaps_total,
            "gap_irreparable_total": stream_state.gap_irreparable_total,
            "last_gap_detection_mode": stream_state.last_gap_detection_mode,
            "last_gap_missing_count": stream_state.last_gap_missing_count,
            "last_gap_seconds": stream_state.last_gap_seconds,
            "last_event_gap_seconds": stream_state.last_event_gap_seconds,
            "max_event_gap_seconds": stream_state.max_event_gap_seconds,
            "late_events": stream_state.late_events,
            "out_of_order_events": stream_state.out_of_order_events,
            "late_events_dropped": stream_state.late_events_dropped,
            "last_late_seconds": stream_state.last_late_seconds,
            "max_late_seconds": stream_state.max_late_seconds,
        }

    def _remember_stream_key(self, partition_key: TemporalPartitionKey, key: EventIdentity) -> None:
        now = self.deduplicator.now_fn()
        entries = self.stream_seen_entries.setdefault(partition_key, OrderedDict())
        if key in entries:
            entries.move_to_end(key)
        entries[key] = now
        ttl_seconds = self.deduplicator.ttl_seconds
        if ttl_seconds is not None:
            while entries:
                first_key = next(iter(entries))
                if now - entries[first_key] <= ttl_seconds:
                    break
                entries.popitem(last=False)
        max_entries = max(1, self.deduplicator.max_entries)
        while len(entries) > max_entries:
            entries.popitem(last=False)

    def _record_gap(self, partition_key: TemporalPartitionKey, stream_state: TemporalStreamState, observation) -> None:
        stream_state.gap_detected = True
        stream_state.gaps_total += 1
        stream_state.last_gap_detection_mode = observation.mode
        stream_state.last_gap_missing_count = observation.missing_count
        stream_state.last_gap_seconds = observation.gap_seconds
        self.metrics.gaps_total += 1
        if observation.irreparable:
            stream_state.gap_irreparable = True
            stream_state.gap_irreparable_total += 1
            self.metrics.gap_irreparable_total += 1
        logger = logging.getLogger("ingest.resilience")
        logger.warning(
            "gap detected",
            extra={
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "temporal_partition": partition_key.label(),
                "gap_detection_mode": observation.mode,
                "gap_missing_count": observation.missing_count,
                "gap_seconds": observation.gap_seconds,
                "gap_strong": observation.strong,
                "gap_irreparable": observation.irreparable,
            },
        )
        emit_operational_alert(
            logger,
            alert_type="gap_detected",
            observed=stream_state.gaps_total,
            extra={
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "temporal_partition": partition_key.label(),
                "gap_detection_mode": observation.mode,
                "gap_missing_count": observation.missing_count,
                "gap_seconds": observation.gap_seconds,
                "gap_strong": observation.strong,
                "gap_irreparable": observation.irreparable,
            },
        )
        if observation.irreparable:
            logger.error(
                "gap irreparable",
                extra={
                    "venue": partition_key.venue,
                    "symbol": partition_key.symbol,
                    "stream_type": partition_key.stream_type,
                    "temporal_partition": partition_key.label(),
                    "gap_detection_mode": observation.mode,
                    "gap_missing_count": observation.missing_count,
                    "gap_seconds": observation.gap_seconds,
                },
            )
            emit_operational_alert(
                logger,
                alert_type="gap_irreparable",
                observed=stream_state.gap_irreparable_total,
                extra={
                    "venue": partition_key.venue,
                    "symbol": partition_key.symbol,
                    "stream_type": partition_key.stream_type,
                    "temporal_partition": partition_key.label(),
                    "gap_detection_mode": observation.mode,
                    "gap_missing_count": observation.missing_count,
                    "gap_seconds": observation.gap_seconds,
                },
            )
        self._update_temporal_metrics(partition_key, stream_state)

    def _record_recovery_exactness_violation(
        self,
        partition_key: TemporalPartitionKey,
        stream_state: TemporalStreamState,
        *,
        requested_rows: int,
        received_rows: int,
        request: RecoveryRequest,
    ) -> None:
        stream_state.recovery_exactness_violation_total += 1
        stream_state.gap_irreparable = True
        stream_state.gap_irreparable_total += 1
        self.metrics.recovery_exactness_violation_total += 1
        self.metrics.gap_irreparable_total += 1
        logger = logging.getLogger("ingest.resilience")
        logger.error(
            "recovery exactness violation",
            extra={
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "temporal_partition": partition_key.label(),
                "recovery_request_start_ts": request.start_ts.isoformat() if request.start_ts else None,
                "recovery_request_end_ts": request.end_ts.isoformat() if request.end_ts else None,
                "recovery_request_interval": request.interval,
                "recovery_window_rows_requested": requested_rows,
                "recovery_window_rows_received": received_rows,
                "gap_seconds": request.gap_seconds,
                "missing_count": request.missing_count,
            },
        )
        emit_operational_alert(
            logger,
            alert_type="recovery_exactness_violation",
            observed=stream_state.recovery_exactness_violation_total,
            extra={
                "venue": partition_key.venue,
                "symbol": partition_key.symbol,
                "stream_type": partition_key.stream_type,
                "temporal_partition": partition_key.label(),
                "recovery_request_start_ts": request.start_ts.isoformat() if request.start_ts else None,
                "recovery_request_end_ts": request.end_ts.isoformat() if request.end_ts else None,
                "recovery_request_interval": request.interval,
                "recovery_window_rows_requested": requested_rows,
                "recovery_window_rows_received": received_rows,
                "gap_seconds": request.gap_seconds,
                "missing_count": request.missing_count,
            },
        )
        self._update_temporal_metrics(partition_key, stream_state)

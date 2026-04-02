"""
Historical-to-live handoff helpers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Protocol

from app.ingestion.checkpoints import CheckpointState
from app.ingestion.client import _key
from app.ingestion.errors import IngestionError
from app.ingestion.sources import Source, SourceStats
from app.marketdata.errors import CheckpointMismatchError
from app.marketdata.models import BarEvent, IngestionEvent
from app.marketdata.recovery import RecoveryRequest
from app.marketdata.temporal_state import CursorState, cursor_from_event, temporal_partition_key


class BootstrapEventsFn(Protocol):
    def __call__(self) -> Iterable[IngestionEvent]: ...


@dataclass(frozen=True, slots=True)
class HistoricalWindow:
    start_ts: datetime | None = None
    end_ts: datetime | None = None

    def contains(self, event: IngestionEvent) -> bool:
        if self.start_ts is not None and event.event_ts < self.start_ts:
            return False
        if self.end_ts is not None and event.event_ts > self.end_ts:
            return False
        return True


def windowed_bootstrap_events(
    events: Iterable[IngestionEvent],
    *,
    window: HistoricalWindow | None,
) -> list[IngestionEvent]:
    filtered = [event for event in events if window is None or window.contains(event)]
    filtered.sort(key=lambda event: event.event_ts)
    return filtered


@dataclass
class HandoffSource:
    live_source: Source
    bootstrap_fn: BootstrapEventsFn
    window: HistoricalWindow | None = None
    strict: bool = True
    validation_rows: int = 3
    post_validation_rows: int = 3
    stats: SourceStats = field(default_factory=SourceStats)
    checkpoint_state: CheckpointState | None = None

    def __post_init__(self) -> None:
        for name in (
            "handoff_bootstrap_rows",
            "handoff_overlap_dropped",
            "handoff_inconsistent",
            "handoff_post_validation_rows",
            "handoff_post_inconsistent",
        ):
            if not hasattr(self.stats, name):
                setattr(self.stats, name, 0)

    def attach_checkpoint_state(self, state: CheckpointState | None) -> None:
        self.checkpoint_state = state

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        logger = logging.getLogger("ingest.handoff")
        bootstrap_events = self._bootstrap_events()
        emitted_bootstrap: dict[str, IngestionEvent] = {}
        bootstrap_tails: dict[str, list[IngestionEvent]] = {}
        live_heads: dict[str, list[IngestionEvent]] = {}
        post_transition_windows: dict[str, list[IngestionEvent]] = {}
        seen_bootstrap_keys = set()
        checkpoint_keys = self._checkpoint_seen_keys()

        logger.info(
            "handoff bootstrap started",
            extra={
                "bootstrap_rows": len(bootstrap_events),
            },
        )
        for event in bootstrap_events:
            partition = temporal_partition_key(event).label()
            event_key = _key(event)
            if event_key in checkpoint_keys or self._is_covered_by_checkpoint(event):
                continue
            seen_bootstrap_keys.add(event_key)
            emitted_bootstrap[partition] = event
            tail = bootstrap_tails.setdefault(partition, [])
            tail.append(event)
            if len(tail) > max(1, self.validation_rows):
                del tail[0]
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            self.stats.snapshot_rows += 1
            self._record_stream_metric(event, "messages_in_total", 1)
            yield event
        logger.info(
            "handoff bootstrap completed",
            extra={
                "bootstrap_rows": len(bootstrap_events),
                "bootstrap_emitted": len(seen_bootstrap_keys),
            },
        )

        reconciled_partitions: set[str] = set()
        for event in self.live_source.stream(end_time=end_time):
            partition = temporal_partition_key(event).label()
            event_key = _key(event)
            if partition in bootstrap_tails and partition not in reconciled_partitions:
                live_head = live_heads.setdefault(partition, [])
                if len(live_head) < max(1, self.validation_rows):
                    live_head.append(event)
                self._assert_handoff_edge_parity(
                    bootstrap_tail=bootstrap_tails[partition],
                    live_head=live_head,
                )
            if event_key in seen_bootstrap_keys or event_key in checkpoint_keys:
                self.stats.handoff_overlap_dropped += 1
                self._record_stream_metric(event, "duplicates_total", 1)
                logger.info(
                    "handoff overlap dropped",
                    extra={
                        "venue": temporal_partition_key(event).venue,
                        "symbol": event.symbol,
                        "stream_type": event.source,
                    },
                )
                continue
            if partition in emitted_bootstrap and partition not in reconciled_partitions:
                self._assert_handoff_consistency(
                    bootstrap_event=emitted_bootstrap[partition],
                    live_event=event,
                )
                logger.info(
                    "handoff edge parity validated",
                    extra={
                        "venue": temporal_partition_key(event).venue,
                        "symbol": event.symbol,
                        "stream_type": event.source,
                        "bootstrap_tail_rows": len(bootstrap_tails.get(partition, [])),
                        "live_head_rows": len(live_heads.get(partition, [])),
                        "overlap_rows": _overlap_count(
                            bootstrap_tails.get(partition, []),
                            live_heads.get(partition, []),
                        ),
                    },
                )
                reconciled_partitions.add(partition)
            if partition in reconciled_partitions:
                self._validate_post_transition_window(
                    partition=partition,
                    bootstrap_tail=bootstrap_tails.get(partition, []),
                    post_transition_window=post_transition_windows.setdefault(partition, []),
                    live_event=event,
                )
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            self._record_stream_metric(event, "messages_in_total", 1)
            yield event

    def snapshot(self, request: RecoveryRequest | None = None) -> Iterable[IngestionEvent] | None:
        return self.live_source.snapshot(request=request)

    def _bootstrap_events(self) -> list[IngestionEvent]:
        self.stats.snapshot_runs += 1
        events = windowed_bootstrap_events(self.bootstrap_fn(), window=self.window)
        self.stats.handoff_bootstrap_rows = len(events)
        return events

    def _checkpoint_seen_keys(self) -> set[object]:
        if self.checkpoint_state is None:
            return set()
        return {entry.key for entry in self.checkpoint_state.seen_entries}

    def _is_covered_by_checkpoint(self, event: IngestionEvent) -> bool:
        if self.checkpoint_state is None:
            return False
        partition = temporal_partition_key(event)
        cursor_state = self.checkpoint_state.stream_cursors.get(partition.label())
        if cursor_state is None:
            return False
        event_kind, event_cursor = cursor_from_event(event)
        if (
            event_kind is not None
            and cursor_state.cursor_kind == event_kind
            and event_cursor is not None
            and _cursor_lte(event_cursor, cursor_state.cursor_value)
        ):
            return True
        if cursor_state.last_event_ts is None:
            return False
        return event.event_ts <= cursor_state.last_event_ts

    def _assert_handoff_consistency(self, *, bootstrap_event: IngestionEvent, live_event: IngestionEvent) -> None:
        if live_event.event_ts < bootstrap_event.event_ts:
            self._mark_handoff_inconsistent(
                f"live event regressed behind bootstrap watermark ({live_event.event_ts.isoformat()} < {bootstrap_event.event_ts.isoformat()})"
            )
            return
        bootstrap_kind, bootstrap_cursor = cursor_from_event(bootstrap_event)
        live_kind, live_cursor = cursor_from_event(live_event)
        if (
            bootstrap_kind is not None
            and bootstrap_kind == live_kind
            and bootstrap_cursor is not None
            and live_cursor is not None
            and _cursor_gap(bootstrap_cursor, live_cursor) > 0
        ):
            self._mark_handoff_inconsistent(
                f"handoff cursor gap detected for {bootstrap_kind} ({bootstrap_cursor} -> {live_cursor})"
            )

    def _assert_handoff_edge_parity(
        self,
        *,
        bootstrap_tail: list[IngestionEvent],
        live_head: list[IngestionEvent],
    ) -> None:
        if not bootstrap_tail or not live_head:
            return
        bootstrap_by_key = {_key(event): event for event in bootstrap_tail}
        latest_bootstrap = bootstrap_tail[-1]
        first_unmatched_live: IngestionEvent | None = None
        for event in live_head:
            bootstrap_event = bootstrap_by_key.get(_key(event))
            if bootstrap_event is None:
                if first_unmatched_live is None:
                    first_unmatched_live = event
                continue
            if event.event_ts != bootstrap_event.event_ts:
                self._mark_handoff_inconsistent(
                    "handoff identity overlap has mismatched timestamps "
                    f"({event.event_ts.isoformat()} != {bootstrap_event.event_ts.isoformat()})"
                )
                return
        if first_unmatched_live is None:
            return
        if first_unmatched_live.event_ts < latest_bootstrap.event_ts:
            self._mark_handoff_inconsistent(
                "live head regressed behind historical tail "
                f"({first_unmatched_live.event_ts.isoformat()} < {latest_bootstrap.event_ts.isoformat()})"
            )
            return
        bootstrap_kind, bootstrap_cursor = cursor_from_event(latest_bootstrap)
        live_kind, live_cursor = cursor_from_event(first_unmatched_live)
        if (
            bootstrap_kind is not None
            and bootstrap_kind == live_kind
            and bootstrap_cursor is not None
            and live_cursor is not None
            and _cursor_gap(bootstrap_cursor, live_cursor) > 0
        ):
            self._mark_handoff_inconsistent(
                "handoff head/tail parity detected cursor gap "
                f"for {bootstrap_kind} ({bootstrap_cursor} -> {live_cursor})"
            )

    def _validate_post_transition_window(
        self,
        *,
        partition: str,
        bootstrap_tail: list[IngestionEvent],
        post_transition_window: list[IngestionEvent],
        live_event: IngestionEvent,
    ) -> None:
        limit = max(1, self.post_validation_rows)
        previous_event = post_transition_window[-1] if post_transition_window else (bootstrap_tail[-1] if bootstrap_tail else None)
        if previous_event is not None:
            self.stats.handoff_post_validation_rows += 1
            self._assert_post_transition_continuity(
                partition=partition,
                previous_event=previous_event,
                current_event=live_event,
            )
        post_transition_window.append(live_event)
        if len(post_transition_window) > limit:
            del post_transition_window[0]

    def _assert_post_transition_continuity(
        self,
        *,
        partition: str,
        previous_event: IngestionEvent,
        current_event: IngestionEvent,
    ) -> None:
        if current_event.event_ts < previous_event.event_ts:
            self._mark_handoff_inconsistent(
                "handoff post-transition window regressed in time "
                f"for {partition} ({current_event.event_ts.isoformat()} < {previous_event.event_ts.isoformat()})",
                post_window=True,
            )
            return
        previous_kind, previous_cursor = cursor_from_event(previous_event)
        current_kind, current_cursor = cursor_from_event(current_event)
        if (
            previous_kind is not None
            and previous_kind == current_kind
            and previous_cursor is not None
            and current_cursor is not None
            and _cursor_gap(previous_cursor, current_cursor) > 0
        ):
            self._mark_handoff_inconsistent(
                "handoff post-transition window detected cursor gap "
                f"for {previous_kind} ({previous_cursor} -> {current_cursor})",
                post_window=True,
            )
            return
        if isinstance(previous_event, BarEvent) and isinstance(current_event, BarEvent):
            expected_interval_seconds = _bar_interval_seconds(previous_event)
            if expected_interval_seconds is not None:
                gap_seconds = (current_event.event_ts - previous_event.event_ts).total_seconds()
                tolerance = expected_interval_seconds * 0.5
                if gap_seconds > expected_interval_seconds + tolerance:
                    self._mark_handoff_inconsistent(
                        "handoff post-transition window detected bar gap "
                        f"for interval {previous_event.interval} ({gap_seconds:.3f}s > {expected_interval_seconds:.3f}s)",
                        post_window=True,
                    )

    def _mark_handoff_inconsistent(self, message: str, *, post_window: bool = False) -> None:
        self.stats.handoff_inconsistent += 1
        if post_window:
            self.stats.handoff_post_inconsistent += 1
        error = self._handoff_error(message)
        logging.getLogger("ingest.handoff").error(
            "handoff inconsistent",
            extra={
                "error": message,
                "error_type": getattr(error, "error_type", type(error).__name__),
                "handoff_post_window": post_window,
                **(error.as_context() if isinstance(error, CheckpointMismatchError) else {}),
            },
        )
        if self.strict:
            raise error

    def _handoff_error(self, message: str) -> IngestionError:
        if self.checkpoint_state is None or not self.checkpoint_state.stream_cursors:
            return IngestionError("validation", "permanent", message)
        first_state = next(iter(self.checkpoint_state.stream_cursors.values()))
        return CheckpointMismatchError(
            stream_key=first_state.partition.label(),
            checkpoint_cursor_kind=first_state.cursor_kind,
            checkpoint_cursor_value=first_state.cursor_value,
            checkpoint_last_event_ts=first_state.last_event_ts,
            reason=message,
        )

    def _record_stream_metric(self, event: IngestionEvent, key: str, delta: int) -> None:
        partition = temporal_partition_key(event)
        label = partition.label()
        metric = self.stats.stream_metrics.setdefault(
            label,
            {
                "venue": partition.venue,
                "symbol": partition.symbol,
                "stream_type": partition.stream_type,
                "messages_in_total": 0,
                "messages_invalid_total": 0,
                "duplicates_total": 0,
                "gaps_total": 0,
                "gap_irreparable_total": 0,
                "reconnects_total": 0,
                "heartbeat_missed_total": 0,
                "buffer_dropped_total": 0,
                "raw_write_latency": 0.0,
                "normalized_write_latency": 0.0,
            },
        )
        metric[key] = int(metric.get(key, 0)) + delta


def _cursor_gap(previous: str, current: str) -> int:
    try:
        previous_value = int(previous)
        current_value = int(current)
    except (TypeError, ValueError):
        return 0
    return max(0, current_value - previous_value - 1)


def _cursor_lte(current: str | None, checkpoint: str | None) -> bool:
    if current in (None, "") or checkpoint in (None, ""):
        return False
    try:
        return int(current) <= int(checkpoint)
    except (TypeError, ValueError):
        return str(current) <= str(checkpoint)


def _overlap_count(
    bootstrap_tail: list[IngestionEvent],
    live_head: list[IngestionEvent],
) -> int:
    if not bootstrap_tail or not live_head:
        return 0
    bootstrap_keys = {_key(event) for event in bootstrap_tail}
    return sum(1 for event in live_head if _key(event) in bootstrap_keys)


def _bar_interval_seconds(event: BarEvent) -> float | None:
    interval = str(event.interval).strip().lower()
    if not interval:
        return None
    units = {
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
        "d": 86400.0,
    }
    unit = interval[-1]
    magnitude = interval[:-1]
    if unit not in units:
        return None
    try:
        value = int(magnitude)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value * units[unit]

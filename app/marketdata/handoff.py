"""
Historical-to-live handoff helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Protocol

from app.ingestion.checkpoints import CheckpointState
from app.ingestion.client import _key
from app.ingestion.errors import IngestionError
from app.ingestion.sources import Source, SourceStats
from app.marketdata.models import IngestionEvent
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
    stats: SourceStats = field(default_factory=SourceStats)
    checkpoint_state: CheckpointState | None = None

    def __post_init__(self) -> None:
        for name in ("handoff_bootstrap_rows", "handoff_overlap_dropped", "handoff_inconsistent"):
            if not hasattr(self.stats, name):
                setattr(self.stats, name, 0)

    def attach_checkpoint_state(self, state: CheckpointState | None) -> None:
        self.checkpoint_state = state

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        bootstrap_events = self._bootstrap_events()
        emitted_bootstrap: dict[str, IngestionEvent] = {}
        seen_bootstrap_keys = set()
        checkpoint_keys = self._checkpoint_seen_keys()

        for event in bootstrap_events:
            partition = temporal_partition_key(event).label()
            event_key = _key(event)
            if event_key in checkpoint_keys or self._is_covered_by_checkpoint(event):
                continue
            seen_bootstrap_keys.add(event_key)
            emitted_bootstrap[partition] = event
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            self.stats.snapshot_rows += 1
            yield event

        reconciled_partitions: set[str] = set()
        for event in self.live_source.stream(end_time=end_time):
            partition = temporal_partition_key(event).label()
            event_key = _key(event)
            if event_key in seen_bootstrap_keys or event_key in checkpoint_keys:
                self.stats.handoff_overlap_dropped += 1
                continue
            if partition in emitted_bootstrap and partition not in reconciled_partitions:
                self._assert_handoff_consistency(
                    bootstrap_event=emitted_bootstrap[partition],
                    live_event=event,
                )
                reconciled_partitions.add(partition)
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            yield event

    def snapshot(self) -> Iterable[IngestionEvent] | None:
        return self.live_source.snapshot()

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

    def _mark_handoff_inconsistent(self, message: str) -> None:
        self.stats.handoff_inconsistent += 1
        if self.strict:
            raise IngestionError("validation", "permanent", message)


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

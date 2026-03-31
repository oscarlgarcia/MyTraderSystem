"""
Gap detection helpers for market data streams.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.marketdata.temporal_state import TemporalStreamState, cursor_from_event
from app.marketdata.models import IngestionEvent

GapDetectionMode = str


@dataclass(frozen=True, slots=True)
class GapObservation:
    detected: bool
    mode: GapDetectionMode | None = None
    missing_count: int = 0
    gap_seconds: float = 0.0
    strong: bool = False
    irreparable: bool = False


_SEQUENCE_CURSOR_KINDS = {"trade_id", "sequence_id"}


def _numeric_cursor(kind: str | None, value: str | None) -> int | None:
    if kind not in _SEQUENCE_CURSOR_KINDS or value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def detect_gap(
    *,
    stream_state: TemporalStreamState,
    event: IngestionEvent,
    lag_threshold_seconds: float,
    has_snapshot: bool,
) -> GapObservation:
    cursor_kind, cursor_value = cursor_from_event(event)
    current_cursor = _numeric_cursor(cursor_kind, cursor_value)
    previous_cursor = _numeric_cursor(stream_state.cursor_kind, stream_state.cursor_value)
    if previous_cursor is not None and current_cursor is not None and current_cursor > previous_cursor + 1:
        missing = current_cursor - previous_cursor - 1
        return GapObservation(
            detected=True,
            mode="sequence_gap_detection",
            missing_count=missing,
            strong=True,
            irreparable=not has_snapshot,
        )

    if stream_state.last_event_ts is None:
        return GapObservation(detected=False)
    gap_seconds = max(0.0, (event.event_ts - stream_state.last_event_ts).total_seconds())
    if gap_seconds > lag_threshold_seconds:
        return GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=gap_seconds,
            strong=False,
            irreparable=False,
        )
    return GapObservation(detected=False)

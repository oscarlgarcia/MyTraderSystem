"""
Gap detection helpers for market data streams.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.marketdata.temporal_state import TemporalStreamState, cursor_from_event
from app.marketdata.models import BarEvent, IngestionEvent

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
_INTERVAL_SECONDS = {
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86_400.0,
    "w": 604_800.0,
    "M": 2_592_000.0,
}


def _numeric_cursor(kind: str | None, value: str | None) -> int | None:
    if kind not in _SEQUENCE_CURSOR_KINDS or value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _expected_bar_cadence_seconds(event: IngestionEvent) -> float | None:
    if not isinstance(event, BarEvent):
        return None
    if event.open_ts is not None and event.close_ts is not None:
        window_seconds = max(0.0, (event.close_ts - event.open_ts).total_seconds())
        if window_seconds > 0.0:
            return window_seconds
    raw_interval = str(getattr(event, "interval", "") or "").strip()
    if len(raw_interval) < 2:
        return None
    try:
        interval_value = int(raw_interval[:-1])
    except ValueError:
        return None
    unit_seconds = _INTERVAL_SECONDS.get(raw_interval[-1])
    if unit_seconds is None:
        return None
    return float(interval_value) * unit_seconds


def _weak_gap_threshold_seconds(event: IngestionEvent, lag_threshold_seconds: float) -> float:
    del event
    return lag_threshold_seconds


def _bar_cursor_ts(kind: str | None, value: str | None) -> datetime | None:
    if kind != "source_id" or value in (None, ""):
        return None
    try:
        cursor_value = int(value)
    except (TypeError, ValueError):
        return None
    if cursor_value < 946684800000:
        return None
    try:
        return datetime.fromtimestamp(cursor_value / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def detect_gap(
    *,
    stream_state: TemporalStreamState,
    event: IngestionEvent,
    lag_threshold_seconds: float,
    recovery_available: bool,
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
            irreparable=not recovery_available,
        )

    if isinstance(event, BarEvent):
        previous_bar_cursor = _bar_cursor_ts(stream_state.cursor_kind, stream_state.cursor_value)
        current_bar_cursor = _bar_cursor_ts(cursor_kind, cursor_value)
        expected_cadence_seconds = _expected_bar_cadence_seconds(event)
        if (
            previous_bar_cursor is not None
            and current_bar_cursor is not None
            and expected_cadence_seconds is not None
            and expected_cadence_seconds > 0.0
        ):
            delta_seconds = max(0.0, (current_bar_cursor - previous_bar_cursor).total_seconds())
            if delta_seconds > expected_cadence_seconds + lag_threshold_seconds:
                missing = max(0, round(delta_seconds / expected_cadence_seconds) - 1)
                return GapObservation(
                    detected=True,
                    mode="bar_cursor_gap_detection",
                    missing_count=int(missing),
                    gap_seconds=delta_seconds,
                    strong=True,
                    irreparable=not recovery_available,
                )
            return GapObservation(detected=False)

    if stream_state.last_event_ts is None:
        return GapObservation(detected=False)
    gap_seconds = max(0.0, (event.event_ts - stream_state.last_event_ts).total_seconds())
    weak_gap_threshold_seconds = _weak_gap_threshold_seconds(
        event,
        lag_threshold_seconds=lag_threshold_seconds,
    )
    if gap_seconds > weak_gap_threshold_seconds:
        return GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=gap_seconds,
            strong=False,
            irreparable=False,
        )
    return GapObservation(detected=False)

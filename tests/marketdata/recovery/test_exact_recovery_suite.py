from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.ingestion.resilience import ResilientRunner
from app.marketdata.gaps import GapObservation
from app.marketdata.models import BarEvent
from app.marketdata.recovery import build_recovery_request
from app.marketdata.temporal_state import TemporalPartitionKey


def _bar(ts: datetime) -> BarEvent:
    return BarEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=str(int((ts - timedelta(minutes=1)).timestamp() * 1000)),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=5.0,
        interval="1m",
        open_ts=ts - timedelta(minutes=1),
        close_ts=ts,
    )


@pytest.mark.parametrize(
    ("cut_seconds", "resume_offset_minutes"),
    [
        (5, 2),
        (30, 2),
        (120, 3),
    ],
)
def test_exact_verified_kline_recovery_handles_controlled_cuts(
    cut_seconds: int,
    resume_offset_minutes: int,
) -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    handled: list[datetime] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar(base),
            _bar(base + timedelta(minutes=resume_offset_minutes)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _bar(base + timedelta(minutes=offset))
            for offset in range(0, resume_offset_minutes + 1)
        ],
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [
        base + timedelta(minutes=offset)
        for offset in range(0, resume_offset_minutes + 1)
    ], f"cut_seconds={cut_seconds}"
    assert runner.metrics.recovery_exactness_violation_total == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is False


def test_exact_verified_kline_recovery_tolerates_duplicates_during_catchup() -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    handled: list[datetime] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar(base),
            _bar(base + timedelta(minutes=3)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _bar(base),
            _bar(base + timedelta(minutes=1)),
            _bar(base + timedelta(minutes=1)),
            _bar(base + timedelta(minutes=2)),
            _bar(base + timedelta(minutes=3)),
        ],
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [
        base,
        base + timedelta(minutes=1),
        base + timedelta(minutes=2),
        base + timedelta(minutes=3),
    ]
    assert runner.metrics.recovery_exactness_violation_total == 0
    assert runner.metrics.snapshot_duplicates_skipped >= 2


def test_exact_verified_kline_recovery_rejects_partial_snapshot_window() -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar(base),
            _bar(base + timedelta(minutes=3)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _bar(base + timedelta(minutes=1)),
        ],
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.recovery_exactness_violation_total == 1
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is True


def test_exact_verified_kline_recovery_falls_back_when_cursor_state_is_mismatched() -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    current = _bar(base + timedelta(minutes=12))
    request = build_recovery_request(
        current,
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        previous_ts=base,
        previous_cursor_kind="source_id",
        previous_cursor_value="3",
        gap_observation=GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=720.0,
        ),
    )

    assert request is not None
    assert request.start_ts == base
    assert request.end_ts == base + timedelta(minutes=12)
    assert request.cursor_kind is None
    assert request.cursor_value is None
    assert request.limit == 13

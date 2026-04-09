from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.ingestion.resilience import ResilientRunner
from app.marketdata.gaps import GapObservation
from app.marketdata.models import BarEvent, TradeEvent
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


def _trade(ts: datetime, trade_id: str) -> TradeEvent:
    return TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=trade_id,
        price=100.0,
        size=1.0,
        trade_id=trade_id,
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


@pytest.mark.parametrize(
    ("cut_seconds", "resume_trade_id"),
    [
        (1, 3),
        (5, 4),
        (30, 6),
    ],
)
def test_exact_verified_trade_recovery_handles_controlled_cuts(
    cut_seconds: int,
    resume_trade_id: int,
) -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    handled: list[str] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            _trade(base + timedelta(seconds=cut_seconds), str(resume_trade_id)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _trade(base + timedelta(seconds=offset), str(offset + 1))
            for offset in range(1, max(1, resume_trade_id - 1))
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.trade_id or ""), stop_on_complete=True)

    assert handled == [str(index) for index in range(1, resume_trade_id + 1)]
    assert runner.metrics.recovery_exactness_violation_total == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert stream_metrics["gap_irreparable"] is False


def test_exact_verified_trade_recovery_tolerates_duplicates_during_catchup() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    handled: list[str] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            _trade(base + timedelta(seconds=3), "4"),
        ]),
        snapshot_fn=lambda *, request=None: [
            _trade(base + timedelta(seconds=0), "1"),
            _trade(base + timedelta(seconds=1), "2"),
            _trade(base + timedelta(seconds=1), "2"),
            _trade(base + timedelta(seconds=2), "3"),
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.trade_id or ""), stop_on_complete=True)

    assert handled == ["1", "2", "3", "4"]
    assert runner.metrics.recovery_exactness_violation_total == 0
    assert runner.metrics.snapshot_duplicates_skipped >= 2


def test_exact_verified_trade_recovery_rejects_partial_snapshot_window() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            _trade(base + timedelta(seconds=3), "4"),
        ]),
        snapshot_fn=lambda *, request=None: [
            _trade(base + timedelta(seconds=1), "2"),
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.recovery_exactness_violation_total == 1
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert stream_metrics["gap_irreparable"] is True


def test_exact_verified_trade_recovery_falls_back_when_cursor_state_is_mismatched() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    current = _trade(base + timedelta(seconds=3), "4")
    request = build_recovery_request(
        current,
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="trade"),
        previous_ts=base,
        previous_cursor_kind="trade_id",
        previous_cursor_value="not-a-cursor",
        gap_observation=GapObservation(
            detected=True,
            mode="sequence_gap_detection",
            gap_seconds=3.0,
            missing_count=2,
            strong=True,
        ),
    )

    assert request is None

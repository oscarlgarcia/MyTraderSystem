from __future__ import annotations

from datetime import datetime, timedelta, timezone

import io
import pytest

from app.marketdata.gaps import GapObservation
from app.ingestion.resilience import ResilientRunner
from app.marketdata.recovery import build_recovery_request, supports_live_recovery, verify_recovery_window
from app.marketdata.models import BarEvent, TradeEvent
from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX
from app.marketdata.temporal_state import TemporalPartitionKey
from app.observability.logger import get_logger


def _bar(symbol: str, ts: datetime) -> BarEvent:
    return BarEvent(
        symbol=symbol,
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


def _trade(symbol: str, ts: datetime, trade_id: str) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=trade_id,
        price=100.0,
        size=1.0,
        trade_id=trade_id,
    )


def test_live_recovery_scope_is_explicitly_trade_and_kline():
    assert supports_live_recovery("kline") is True
    assert supports_live_recovery("trade") is True


def test_exact_bar_recovery_fills_gap_without_edge_duplicates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(minutes=2)),
    ]
    snapshot_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(minutes=1)),
        _bar("BTCUSDT", base + timedelta(minutes=2)),
    ]
    handled: list[datetime] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [
        base,
        base + timedelta(minutes=1),
        base + timedelta(minutes=2),
    ]
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.snapshot_duplicates_skipped == 2
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is False


def test_exact_trade_recovery_fills_gap_without_duplicate_delivery():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _trade("BTCUSDT", base, "1"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "3"),
    ]
    snapshot_events = [
        _trade("BTCUSDT", base + timedelta(milliseconds=500), "2"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "3"),
    ]
    handled: list[str] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(getattr(event, "trade_id", "")), stop_on_complete=True)

    assert runner.metrics.gaps_total == 1
    assert runner.metrics.gap_irreparable_total == 0
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.recovery_exactness_violation_total == 0
    assert handled == ["1", "2", "3"]
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert stream_metrics["gap_detected"] is True
    assert stream_metrics["gap_irreparable"] is False
    assert stream_metrics["last_gap_detection_mode"] == "sequence_gap_detection"


@pytest.mark.parametrize(
    ("gap_minutes", "expected_limit"),
    [
        (1, 2),
        (5, 6),
        (12, 13),
    ],
)
def test_build_recovery_request_scales_bar_snapshot_window_with_gap(gap_minutes: int, expected_limit: int):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    current = _bar("BTCUSDT", base + timedelta(minutes=gap_minutes))
    request = build_recovery_request(
        current,
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        previous_ts=base,
        gap_observation=GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=float(gap_minutes * 60),
        ),
        previous_cursor_kind="source_id",
        previous_cursor_value=str(int((base - timedelta(minutes=1)).timestamp() * 1000)),
    )

    assert request is not None
    assert request.start_ts == base - timedelta(minutes=1)
    assert request.end_ts == base + timedelta(minutes=gap_minutes - 1)
    assert request.interval == "1m"
    assert request.limit == expected_limit
    assert request.reason == "weak_gap_detection"


def test_runner_passes_recovery_request_window_to_snapshot_fn():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(minutes=5)),
    ]
    captured: dict[str, object] = {}

    def snapshot_fn(*, request=None):
        captured["request"] = request
        return [_bar("BTCUSDT", base + timedelta(minutes=1))]

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=snapshot_fn,
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    request = captured["request"]
    assert request is not None
    assert request.partition == TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline")
    assert request.start_ts == base - timedelta(minutes=1)
    assert request.end_ts == base + timedelta(minutes=4)
    assert request.interval == "1m"
    assert request.limit == 6


def test_runner_passes_trade_recovery_request_cursor_window_to_snapshot_fn():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _trade("BTCUSDT", base, "1"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "4"),
    ]
    captured: dict[str, object] = {}

    def snapshot_fn(*, request=None):
        captured["request"] = request
        return [_trade("BTCUSDT", base + timedelta(milliseconds=500), "2")]

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=snapshot_fn,
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    request = captured["request"]
    assert request is not None
    assert request.partition == TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="trade")
    assert request.cursor_kind == "trade_id"
    assert request.cursor_value == "2"
    assert request.limit == 3


def test_exact_kline_recovery_uses_open_time_window_without_gap_or_double_count():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(minutes=5)),
    ]
    handled: list[datetime] = []

    def snapshot_fn(*, request=None):
        assert request is not None
        assert request.limit == 6
        return [
            _bar("BTCUSDT", base + timedelta(minutes=offset))
            for offset in range(0, 6)
        ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=snapshot_fn,
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [
        base,
        base + timedelta(minutes=1),
        base + timedelta(minutes=2),
        base + timedelta(minutes=3),
        base + timedelta(minutes=4),
        base + timedelta(minutes=5),
    ]
    assert runner.metrics.recovery_exactness_violation_total == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is False
    assert FEED_SUPPORT_MATRIX["kline"].supports_exact_recovery is True


def test_exact_kline_recovery_marks_gap_irreparable_when_snapshot_window_is_incomplete():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(minutes=12)),
    ]
    handled: list[datetime] = []
    buffer = io.StringIO()
    get_logger(name="ingest.resilience", level="INFO", stream=buffer)

    def snapshot_fn(*, request=None):
        assert request is not None
        assert request.limit == 13
        return [
            _bar("BTCUSDT", base + timedelta(minutes=1)),
            _bar("BTCUSDT", base + timedelta(minutes=12)),
        ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=snapshot_fn,
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [
        base,
        base + timedelta(minutes=1),
        base + timedelta(minutes=12),
    ]
    assert runner.metrics.recovery_window_rows_requested == 13
    assert runner.metrics.recovery_window_rows_received == 2
    assert runner.metrics.recovery_exactness_violation_total == 1
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is True
    assert stream_metrics["recovery_window_rows_requested"] == 13
    assert stream_metrics["recovery_window_rows_received"] == 2
    assert stream_metrics["recovery_exactness_violation_total"] == 1
    alerts = [line for line in buffer.getvalue().splitlines() if "recovery_exactness_violation" in line]
    assert alerts
    assert FEED_SUPPORT_MATRIX["kline"].supports_exact_recovery is True


def test_verify_recovery_window_rejects_missing_and_unexpected_kline_rows():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    request = build_recovery_request(
        _bar("BTCUSDT", base + timedelta(minutes=5)),
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        previous_ts=base,
        previous_cursor_kind="source_id",
        previous_cursor_value=str(int((base - timedelta(minutes=1)).timestamp() * 1000)),
        gap_observation=GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=300.0,
        ),
    )

    verification = verify_recovery_window(
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        request=request,
        recovered_events=[
            _bar("BTCUSDT", base),
            _bar("BTCUSDT", base + timedelta(minutes=1)),
            _bar("BTCUSDT", base + timedelta(minutes=3)),
            _bar("BTCUSDT", base + timedelta(minutes=5)),
            _bar("BTCUSDT", base + timedelta(minutes=6)),
        ],
    )

    assert verification.exact is False
    assert verification.expected_rows == 6
    assert verification.received_rows == 5
    assert verification.missing_timestamps == (
        base + timedelta(minutes=1),
        base + timedelta(minutes=3),
    )
    assert verification.unexpected_timestamps == (base + timedelta(minutes=5),)


def test_verify_recovery_window_rejects_missing_and_unexpected_trade_rows():
    request = build_recovery_request(
        _trade("BTCUSDT", datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc), "4"),
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="trade"),
        previous_ts=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        previous_cursor_kind="trade_id",
        previous_cursor_value="1",
        gap_observation=GapObservation(
            detected=True,
            mode="sequence_gap_detection",
            missing_count=2,
            strong=True,
        ),
    )

    verification = verify_recovery_window(
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="trade"),
        request=request,
        recovered_events=[
            _trade("BTCUSDT", datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc), "2"),
            _trade("BTCUSDT", datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc), "5"),
        ],
    )

    assert request is not None
    assert verification.exact is False
    assert verification.expected_rows == 3
    assert verification.received_rows == 2
    assert verification.missing_timestamps == ("3", "4")
    assert verification.unexpected_timestamps == ("5",)

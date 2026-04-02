from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.marketdata.gaps import GapObservation
from app.ingestion.resilience import ResilientRunner
from app.marketdata.recovery import build_recovery_request, supports_live_recovery
from app.marketdata.models import BarEvent, TradeEvent
from app.marketdata.temporal_state import TemporalPartitionKey


def _bar(symbol: str, ts: datetime) -> BarEvent:
    return BarEvent(
        symbol=symbol,
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
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


def test_live_recovery_scope_is_explicitly_bars_only():
    assert supports_live_recovery("kline") is True
    assert supports_live_recovery("trade") is False


def test_exact_bar_recovery_fills_gap_without_edge_duplicates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _bar("BTCUSDT", base),
        _bar("BTCUSDT", base + timedelta(seconds=12)),
    ]
    snapshot_events = [
        _bar("BTCUSDT", base + timedelta(seconds=5)),
        _bar("BTCUSDT", base + timedelta(seconds=12)),
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
        base + timedelta(seconds=5),
        base + timedelta(seconds=12),
    ]
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.snapshot_duplicates_skipped == 1
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gap_irreparable"] is False


def test_trade_gap_without_exact_recovery_is_marked_irreparable():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        _trade("BTCUSDT", base, "1"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "3"),
    ]
    snapshot_events = [
        _bar("BTCUSDT", base + timedelta(seconds=1)),
    ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(stream_events),
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.gaps_total == 1
    assert runner.metrics.gap_irreparable_total == 1
    assert runner.metrics.snapshot_runs == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert stream_metrics["gap_detected"] is True
    assert stream_metrics["gap_irreparable"] is True
    assert stream_metrics["last_gap_detection_mode"] == "sequence_gap_detection"


def test_build_recovery_request_scales_bar_snapshot_window_with_gap():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    current = _bar("BTCUSDT", base + timedelta(minutes=5))
    request = build_recovery_request(
        current,
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        previous_ts=base,
        gap_observation=GapObservation(
            detected=True,
            mode="weak_gap_detection",
            gap_seconds=300.0,
        ),
    )

    assert request is not None
    assert request.start_ts == base
    assert request.end_ts == base + timedelta(minutes=5)
    assert request.interval == "1m"
    assert request.limit == 6
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
    assert request.start_ts == base
    assert request.end_ts == base + timedelta(minutes=5)
    assert request.interval == "1m"
    assert request.limit == 6

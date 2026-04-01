from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.ingestion.resilience import ResilientRunner
from app.marketdata.models import BarEvent, TradeEvent


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

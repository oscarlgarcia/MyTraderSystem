from datetime import datetime, timedelta, timezone

from app.marketdata.models import TradeEvent
from app.ingestion.resilience import ResilientRunner


def _trade(symbol: str, ts: datetime, trade_id: str) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        price=100.0,
        size=1.0,
        trade_id=trade_id,
    )


def test_interleaved_multi_symbol_stream_does_not_create_false_temporal_incidents():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        _trade("BTCUSDT", base, "1"),
        _trade("ETHUSDT", base, "1"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "2"),
        _trade("ETHUSDT", base + timedelta(seconds=1), "2"),
    ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        lag_threshold_seconds=5.0,
        sleeper=lambda _seconds: None,
    )
    handled: list[TradeEvent] = []
    runner.run(lambda event: handled.append(event), stop_on_complete=True)

    assert len(handled) == 4
    btc_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    eth_metrics = runner.metrics.temporal_streams["BINANCE:ETHUSDT:trade"]
    assert btc_metrics["gaps_total"] == 0
    assert eth_metrics["gaps_total"] == 0
    assert btc_metrics["late_events"] == 0
    assert eth_metrics["late_events"] == 0


def test_broken_sequence_is_reported_as_strong_gap_for_affected_stream():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        _trade("BTCUSDT", base, "1"),
        _trade("BTCUSDT", base + timedelta(seconds=1), "3"),
    ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert metrics["gaps_total"] == 1
    assert metrics["gap_irreparable_total"] == 1
    assert metrics["last_gap_detection_mode"] == "sequence_gap_detection"
    assert metrics["last_gap_missing_count"] == 1

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock

from app.ingestion import pipeline
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sources import StaticSource
from app.marketdata.models import BarEvent, TradeEvent


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


def _snapshot_trade(ts: datetime, trade_id: str) -> TradeEvent:
    event = _trade(ts, trade_id)
    event.metadata["recovery_source"] = "snapshot"
    event.metadata["historical_trade_kind"] = "aggregate_trade"
    return event


class DummySink:
    def __init__(self) -> None:
        self.items: list[object] = []

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
            return
        self.items.append(batch)

    def close(self):
        return None


def test_reconnect_old_resend_is_deduplicated_after_exact_recovery() -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    handled: list[datetime] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar(base),
            _bar(base + timedelta(minutes=3)),
            _bar(base + timedelta(minutes=2)),
        ]),
        snapshot_fn=lambda *, request=None: [
            _bar(base),
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
    assert runner.metrics.dedup_skipped >= 1
    assert runner.metrics.recovery_exactness_violation_total == 0


def test_production_mode_accepts_kline_after_exact_verified_claim() -> None:
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=StaticSource(events=[_bar(base)]),
        sink=DummySink(),
        stream_types=("kline",),
        production_mode=True,
    )

    assert len(out) == 1
    assert out[0].source == "kline"


def test_minute_kline_cadence_does_not_trigger_gap_recovery() -> None:
    base = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    handled: list[datetime] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _bar(base),
            _bar(base + timedelta(minutes=1)),
        ]),
        snapshot_fn=lambda *, request=None: [_bar(base + timedelta(minutes=2))],
        lag_threshold_seconds=5,
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda event: handled.append(event.event_ts), stop_on_complete=True)

    assert handled == [base, base + timedelta(minutes=1)]
    assert runner.metrics.gaps_total == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:kline"]
    assert stream_metrics["gaps_total"] == 0


def test_processing_latency_tracks_receive_clock_for_kline() -> None:
    now = datetime.now(timezone.utc)
    stale_exchange_ts = now - timedelta(minutes=2)
    event = BarEvent(
        symbol="BTCUSDT",
        exchange_ts=stale_exchange_ts,
        receive_ts=now,
        process_ts=now,
        venue="BINANCE",
        source_id="1",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=5.0,
        interval="1m",
        open_ts=stale_exchange_ts - timedelta(minutes=1),
        close_ts=stale_exchange_ts,
    )
    runner = ResilientRunner(
        stream_fn=lambda: iter([event]),
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.max_latency_seconds < 5.0


def test_trade_reconnect_old_resend_is_deduplicated_after_exact_recovery() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    handled: list[str] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            _trade(base + timedelta(seconds=3), "4"),
            _trade(base + timedelta(seconds=2), "3"),
        ]),
        snapshot_fn=lambda *, request=None: [
            _trade(base + timedelta(seconds=1), "2"),
            _trade(base + timedelta(seconds=2), "3"),
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event.trade_id or ""), stop_on_complete=True)

    assert handled == ["1", "2", "3", "4"]
    assert runner.metrics.dedup_skipped >= 1
    assert runner.metrics.recovery_exactness_violation_total == 0


def test_trade_exact_recovery_clears_gap_totals_after_successful_resync() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    handled: list[str] = []

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            _trade(base + timedelta(seconds=3), "4"),
        ]),
        snapshot_fn=lambda *, request=None: [
            _snapshot_trade(base + timedelta(seconds=1), "2"),
            _snapshot_trade(base + timedelta(seconds=2), "3"),
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda event: handled.append(event.trade_id or ""), stop_on_complete=True)

    assert handled == ["1", "2", "3", "4"]
    assert runner.metrics.gaps_total == 0
    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert stream_metrics["gaps_total"] == 0
    assert stream_metrics["gap_irreparable_total"] == 0
    assert stream_metrics["gap_detected"] is False


def test_trade_recovery_snapshot_events_do_not_count_toward_live_skew_metrics() -> None:
    base = datetime.now(timezone.utc)
    now = base + timedelta(seconds=30)
    event = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=base,
        receive_ts=now,
        process_ts=now + timedelta(seconds=10),
        venue="BINANCE",
        source_id="2",
        price=100.0,
        size=1.0,
        trade_id="2",
        metadata={
            "aggregate_trade_id": "2",
            "recovery_source": "snapshot",
            "historical_trade_kind": "aggregate_trade",
        },
    )

    runner = ResilientRunner(
        stream_fn=lambda: iter([event]),
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.exchange_receive_skew_seconds == 0.0
    assert runner.metrics.receive_process_skew_seconds == 0.0
    assert runner.metrics.max_latency_seconds == 0.0


def test_trade_gap_boundary_event_does_not_count_as_live_exchange_receive_skew_when_recovered() -> None:
    base = datetime.now(timezone.utc)
    late_receive_ts = base + timedelta(seconds=15)
    boundary = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=base + timedelta(seconds=3),
        receive_ts=late_receive_ts,
        process_ts=late_receive_ts,
        venue="BINANCE",
        source_id="4",
        price=100.0,
        size=1.0,
        trade_id="4",
        metadata={"aggregate_trade_id": "4"},
    )

    runner = ResilientRunner(
        stream_fn=lambda: iter([
            _trade(base, "1"),
            boundary,
        ]),
        snapshot_fn=lambda *, request=None: [
            _snapshot_trade(base + timedelta(seconds=1), "2"),
            _snapshot_trade(base + timedelta(seconds=2), "3"),
        ],
        lag_threshold_seconds=0.5,
        sleeper=lambda _seconds: None,
    )

    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.gaps_total == 0
    assert runner.metrics.exchange_receive_skew_seconds == 0.0
    assert runner.metrics.max_latency_seconds < 2.0

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock

from app.ingestion import pipeline
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sources import StaticSource
from app.marketdata.models import BarEvent


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

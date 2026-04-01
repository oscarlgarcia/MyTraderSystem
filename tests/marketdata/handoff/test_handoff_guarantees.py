from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.ingestion.errors import IngestionError
from app.ingestion.pipeline import collect_events
from app.ingestion.sources import StaticSource
from app.marketdata.handoff import HandoffSource, HistoricalWindow
from app.marketdata.models import TradeEvent
from app.observability.logger import get_logger


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        env="test",
        data_dir=tmp_path.resolve(),
        log_level="INFO",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
    )


def _trade(ts: datetime, trade_id: int) -> TradeEvent:
    trade_id_str = str(trade_id)
    return TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=trade_id_str,
        price=100.0 + trade_id,
        size=1.0,
        trade_id=trade_id_str,
    )


class RecordingSink:
    def __init__(self) -> None:
        self.items = []
        self.persisted_count = 0
        self.write_latency_seconds = 0.0

    def add(self, batch):
        rows = batch if isinstance(batch, list) else [batch]
        self.items.extend(rows)
        self.persisted_count += len(rows)

    def close(self):
        return None


def test_historical_to_live_handoff_is_clean_without_gaps_or_duplicates(tmp_path: Path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade(base, 1), _trade(base + timedelta(seconds=1), 2)]
    live = [_trade(base + timedelta(seconds=2), 3), _trade(base + timedelta(seconds=3), 4)]
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        window=HistoricalWindow(start_ts=base, end_ts=base + timedelta(seconds=2)),
    )

    out = collect_events(
        mode="live",
        cfg=cfg,
        logger=get_logger(name="test.handoff.guarantees.clean", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
    )

    assert [event.trade_id for event in out] == ["1", "2", "3", "4"]
    assert source.stats.handoff_bootstrap_rows == 2
    assert source.stats.handoff_overlap_dropped == 0
    assert source.stats.handoff_inconsistent == 0


def test_handoff_duplicate_edge_is_removed_by_strong_identity(tmp_path: Path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade(base, 1), _trade(base + timedelta(seconds=1), 2)]
    live = [_trade(base + timedelta(seconds=1), 2), _trade(base + timedelta(seconds=2), 3)]
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
    )

    out = collect_events(
        mode="live",
        cfg=cfg,
        logger=get_logger(name="test.handoff.guarantees.dup", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
    )

    assert [event.trade_id for event in out] == ["1", "2", "3"]
    assert source.stats.handoff_overlap_dropped == 1
    assert source.stats.handoff_inconsistent == 0


def test_handoff_gap_is_visible_and_respects_strict_or_degraded_policy(tmp_path: Path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade(base, 1), _trade(base + timedelta(seconds=1), 2)]
    live = [_trade(base + timedelta(seconds=3), 4)]

    strict_source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        strict=True,
    )
    with pytest.raises(IngestionError):
        collect_events(
            mode="live",
            cfg=cfg,
            logger=get_logger(name="test.handoff.guarantees.fail", level="INFO", stream=io.StringIO()),
            source=strict_source,
            sink=RecordingSink(),
            snapshot_enabled=False,
            summary_logging=True,
            duration_s=0,
            error_policy="fail_fast",
        )
    assert strict_source.stats.handoff_inconsistent == 1

    degraded_source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        strict=True,
    )
    out = collect_events(
        mode="live",
        cfg=cfg,
        logger=get_logger(name="test.handoff.guarantees.degraded", level="INFO", stream=io.StringIO()),
        source=degraded_source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
        error_policy="degraded",
    )

    assert out == []
    assert degraded_source.stats.handoff_inconsistent == 1

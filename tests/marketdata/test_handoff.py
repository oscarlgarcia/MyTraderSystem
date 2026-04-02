from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.ingestion.checkpoints import CheckpointState, CheckpointStore
from app.ingestion.dedup import DedupStateEntry
from app.ingestion.errors import IngestionError
from app.ingestion.pipeline import collect_events
from app.ingestion.sources import StaticSource
from app.marketdata.handoff import HandoffSource, HistoricalWindow
from app.marketdata.models import BarEvent, TradeEvent
from app.marketdata.temporal_state import CursorState, TemporalPartitionKey
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


def _trade_with_id(ts: datetime, trade_id: str) -> TradeEvent:
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


def _bar(ts: datetime, open_ts: datetime, bar_id: str, *, interval: str = "1m") -> BarEvent:
    return BarEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id=bar_id,
        metadata={"source_id": bar_id},
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
        interval=interval,
        open_ts=open_ts,
        close_ts=ts,
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


def test_handoff_clean_bootstrap_to_live_without_gaps_or_duplicates(tmp_path):
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
        logger=get_logger(name="test.handoff.clean", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
    )

    assert [event.trade_id for event in out] == ["1", "2", "3", "4"]
    assert source.stats.handoff_inconsistent == 0
    assert source.stats.handoff_overlap_dropped == 0
    assert source.stats.handoff_bootstrap_rows == 2


def test_handoff_duplicate_edge_is_deduplicated(tmp_path):
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
        logger=get_logger(name="test.handoff.dup", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
    )

    assert [event.trade_id for event in out] == ["1", "2", "3"]
    assert source.stats.handoff_overlap_dropped == 1
    assert source.stats.handoff_inconsistent == 0


def test_handoff_identity_overlap_with_mismatched_timestamp_is_inconsistent(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade_with_id(base, "10"), _trade_with_id(base + timedelta(seconds=1), "11")]
    live = [_trade_with_id(base + timedelta(seconds=2), "11")]
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        strict=True,
    )

    with pytest.raises(IngestionError):
        collect_events(
            mode="live",
            cfg=cfg,
            logger=get_logger(name="test.handoff.parity.mismatch", level="INFO", stream=io.StringIO()),
            source=source,
            sink=RecordingSink(),
            snapshot_enabled=False,
            summary_logging=True,
            duration_s=0,
            error_policy="fail_fast",
        )

    assert source.stats.handoff_inconsistent == 1


def test_handoff_gap_is_visible_and_respects_policy(tmp_path):
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
            logger=get_logger(name="test.handoff.fail", level="INFO", stream=io.StringIO()),
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
        logger=get_logger(name="test.handoff.degraded", level="INFO", stream=io.StringIO()),
        source=degraded_source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
        error_policy="degraded",
    )
    assert out == []
    assert degraded_source.stats.handoff_inconsistent == 1


def test_handoff_uses_checkpoint_to_skip_bootstrap_overlap(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade(base, 1), _trade(base + timedelta(seconds=1), 2), _trade(base + timedelta(seconds=2), 3)]
    live = [_trade(base + timedelta(seconds=3), 4)]
    checkpoint_path = tmp_path / "handoff-checkpoint.json"
    store = CheckpointStore(checkpoint_path)
    prior_event = _trade(base + timedelta(seconds=1), 2)
    store.save(
        CheckpointState(
            last_event_ts=prior_event.event_ts,
            seen_entries=(DedupStateEntry(key=("native", "BINANCE", "BTCUSDT", "trade", "trade_id", "2"), seen_at=1.0),),
            stream_cursors={
                "BINANCE:BTCUSDT:trade": CursorState(
                    partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="trade"),
                    last_event_ts=prior_event.event_ts,
                    cursor_kind="trade_id",
                    cursor_value="2",
                    seen_entries=(DedupStateEntry(key=("native", "BINANCE", "BTCUSDT", "trade", "trade_id", "2"), seen_at=1.0),),
                )
            },
            metadata={},
        )
    )
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
    )

    out = collect_events(
        mode="live",
        cfg=cfg,
        logger=get_logger(name="test.handoff.checkpoint", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
        checkpoint_store=store,
    )

    assert [event.trade_id for event in out] == ["3", "4"]


def test_handoff_post_transition_window_detects_cursor_gap_after_clean_edge(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [_trade(base, 1), _trade(base + timedelta(seconds=1), 2)]
    live = [
        _trade(base + timedelta(seconds=1), 2),
        _trade(base + timedelta(seconds=2), 3),
        _trade(base + timedelta(seconds=4), 5),
    ]
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        strict=True,
        validation_rows=2,
        post_validation_rows=3,
    )

    with pytest.raises(IngestionError, match="post-transition window detected cursor gap"):
        collect_events(
            mode="live",
            cfg=cfg,
            logger=get_logger(name="test.handoff.post.cursor_gap", level="INFO", stream=io.StringIO()),
            source=source,
            sink=RecordingSink(),
            snapshot_enabled=False,
            summary_logging=True,
            duration_s=0,
            error_policy="fail_fast",
        )

    assert source.stats.handoff_inconsistent == 1
    assert source.stats.handoff_post_inconsistent == 1
    assert source.stats.handoff_post_validation_rows >= 2


def test_handoff_post_transition_window_degrades_on_bar_gap_after_clean_edge(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bootstrap = [
        _bar(base + timedelta(minutes=1), base, "1"),
        _bar(base + timedelta(minutes=2), base + timedelta(minutes=1), "2"),
    ]
    live = [
        _bar(base + timedelta(minutes=2), base + timedelta(minutes=1), "2"),
        _bar(base + timedelta(minutes=3), base + timedelta(minutes=2), "3"),
        _bar(base + timedelta(minutes=5), base + timedelta(minutes=4), "5"),
    ]
    source = HandoffSource(
        live_source=StaticSource(events=live),
        bootstrap_fn=lambda: bootstrap,
        strict=True,
        validation_rows=2,
        post_validation_rows=3,
    )

    out = collect_events(
        mode="live",
        cfg=cfg,
        logger=get_logger(name="test.handoff.post.bar_gap", level="INFO", stream=io.StringIO()),
        source=source,
        sink=RecordingSink(),
        snapshot_enabled=False,
        summary_logging=True,
        duration_s=0,
        error_policy="degraded",
    )

    assert out == []
    assert source.stats.handoff_inconsistent == 1
    assert source.stats.handoff_post_inconsistent == 1

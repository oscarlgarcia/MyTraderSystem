from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.storage import ParquetWriter, list_normalized_partition_paths
from app.marketdata.dataset_quality import build_dataset_quality_registry
from app.marketdata.gap_fill import build_gap_fill_plan
from app.marketdata.models import TradeEvent
from app.marketdata.dataset_contracts import parse_normalized_partition_path


def _write_trade_partition(tmp_path, *, day: str, trade_id: str) -> None:
    ts = datetime.fromisoformat(f"{day}T00:00:00+00:00")
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id=trade_id,
            trade_id=trade_id,
            side="buy",
            price=100.0,
            size=1.0,
            metadata={
                "raw_run_id": f"run-{trade_id}",
                "raw_ingestion_seq": trade_id,
                "historical_feed_kind": "aggregate_trade",
                "metadata_snapshot_mode": "runtime",
                "instrument_catalog_snapshot_json": "[]",
            },
        )
    )
    writer.flush()


def test_gap_fill_plan_detects_missing_partition_day(tmp_path):
    _write_trade_partition(tmp_path, day="2024-01-01", trade_id="1")
    _write_trade_partition(tmp_path, day="2024-01-03", trade_id="3")
    refs = [parse_normalized_partition_path(path) for path in list_normalized_partition_paths(tmp_path, "test")]
    quality = build_dataset_quality_registry(list_normalized_partition_paths(tmp_path, "test"))

    plan = build_gap_fill_plan(env="test", refs=refs, quality_registry=quality)

    assert len(plan.candidates) == 1
    assert plan.candidates[0].missing_dates == ("2024-01-02",)
    assert plan.candidates[0].reason == "missing_partitions"

from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.storage import ParquetWriter
from app.marketdata.models import TradeEvent
from app.marketdata.storage_lifecycle import apply_storage_lifecycle


def test_storage_lifecycle_writes_downsampled_copy_for_cold_partitions(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    for index in range(5):
        writer.add(
            TradeEvent(
                symbol="BTCUSDT",
                exchange_ts=ts.replace(minute=index),
                provider_ts=ts.replace(minute=index),
                receive_ts=ts.replace(minute=index),
                process_ts=ts.replace(minute=index),
                venue="BINANCE",
                source_id=str(index),
                trade_id=str(index),
                side="buy",
                price=100.0 + index,
                size=1.0,
                metadata={
                    "raw_run_id": "run-1",
                    "raw_ingestion_seq": str(index),
                    "historical_feed_kind": "aggregate_trade",
                    "metadata_snapshot_mode": "runtime",
                    "instrument_catalog_snapshot_json": "[]",
                },
            )
        )
    writer.flush()

    report = apply_storage_lifecycle(tmp_path, "test", sample_every=2)

    assert len(report.actions) == 1
    assert report.actions[0].action in {"downsampled_copy", "retain_hot"}

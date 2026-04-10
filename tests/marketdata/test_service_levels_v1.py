from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.storage import ParquetWriter, list_normalized_partition_paths
from app.marketdata.dataset_catalog import refresh_dataset_catalog
from app.marketdata.dataset_quality import build_dataset_quality_registry
from app.marketdata.delivery import build_delivery_contract_registry
from app.marketdata.models import TradeEvent
from app.marketdata.service_levels import build_dataset_service_levels


def test_service_levels_follow_quality_and_delivery_contracts(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="1",
            trade_id="1",
            side="buy",
            price=100.0,
            size=1.0,
            metadata={
                "raw_run_id": "run-1",
                "raw_ingestion_seq": "1",
                "historical_feed_kind": "aggregate_trade",
                "metadata_snapshot_mode": "runtime",
                "instrument_catalog_snapshot_json": "[]",
            },
        )
    )
    writer.flush()

    catalog = refresh_dataset_catalog(tmp_path, "test")
    quality = build_dataset_quality_registry(list_normalized_partition_paths(tmp_path, "test"))
    delivery = build_delivery_contract_registry(env="test")
    registry = build_dataset_service_levels(env="test", catalog=catalog, quality_registry=quality, delivery_registry=delivery)

    assert len(registry.records) == 1
    assert registry.records[0].service_status == "within_slo"
    assert registry.records[0].target_freshness_seconds == 30.0

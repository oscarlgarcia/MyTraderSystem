from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.storage import ParquetWriter
from app.marketdata.dataset_catalog import dataset_catalog_path, refresh_dataset_catalog
from app.marketdata.dataset_contracts import dataset_contract_registry_path, read_dataset_contract_registry
from app.marketdata.dataset_quality import dataset_quality_registry_path, read_dataset_quality_registry
from app.marketdata.delivery import build_delivery_contract_registry
from app.marketdata.models import TradeEvent
from app.marketdata.storage_lifecycle import build_storage_lifecycle_report


def _write_trade_dataset(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
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
                "normalizer_version": "v1",
            },
        )
    )
    writer.flush()


def test_refresh_dataset_catalog_writes_contracts_quality_and_catalog(tmp_path):
    _write_trade_dataset(tmp_path)

    catalog = refresh_dataset_catalog(tmp_path, "test")
    contracts = read_dataset_contract_registry(dataset_contract_registry_path(tmp_path, "test"))
    quality = read_dataset_quality_registry(dataset_quality_registry_path(tmp_path, "test"))

    assert len(catalog.entries) == 1
    assert catalog.entries[0].contract_pass_ok is True
    assert catalog.entries[0].quality_status == "healthy"
    assert dataset_catalog_path(tmp_path, "test").exists()
    assert len(contracts.records) == 1
    assert contracts.records[0].contract.pass_ok is True
    assert len(quality.reports) == 1
    assert quality.reports[0].score == 100.0


def test_delivery_and_storage_lifecycle_reports_are_buildable(tmp_path):
    _write_trade_dataset(tmp_path)
    refresh_dataset_catalog(tmp_path, "test")

    delivery = build_delivery_contract_registry(env="test")
    lifecycle = build_storage_lifecycle_report(tmp_path, "test")

    assert any(item.stream_type == "trade" for item in delivery.contracts)
    assert len(lifecycle.entries) == 1
    assert lifecycle.entries[0].tier == "cold"

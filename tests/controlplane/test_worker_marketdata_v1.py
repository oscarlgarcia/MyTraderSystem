from __future__ import annotations

from datetime import datetime, timezone

from app.config import AppConfig
from app.controlplane.models import CommandRequestRecord
from app.controlplane.sqlite_store import SQLiteControlPlaneStore
from app.controlplane.worker import process_next_command
from app.ingestion.storage import ParquetWriter
from app.marketdata.dataset_catalog import dataset_catalog_path
from app.marketdata.service_levels import dataset_service_levels_path
from app.marketdata.storage_lifecycle import storage_lifecycle_execution_path
from app.marketdata.models import TradeEvent
from app.marketdata.publication import publication_path
from app.marketdata.serving import refresh_curated_store
from app.marketdata.subscriptions import read_subscription_config


def _cfg(tmp_path) -> AppConfig:
    return AppConfig(
        env="test",
        data_dir=tmp_path,
        log_level="INFO",
        ws_base="wss://example.test",
        rest_base="https://example.test",
        symbols=["BTCUSDT"],
        control_plane_backend="sqlite",
        control_plane_db_path=tmp_path / "control-plane.sqlite",
        control_plane_db_url=None,
        control_plane_telemetry_dir=tmp_path / "telemetry",
        control_plane_poll_interval_seconds=5.0,
        control_plane_command_poll_interval_seconds=1.0,
    )


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
            },
        )
    )
    writer.flush()


def test_worker_executes_catalog_refresh_and_subscription_update(tmp_path):
    cfg = _cfg(tmp_path)
    _write_trade_dataset(tmp_path)
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-1",
            command_type="refresh_dataset_catalog",
            scope="env:test:catalog",
            payload={},
            requested_by="tester",
            requested_at="2026-04-10T10:00:00+00:00",
        )
    )
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-2",
            command_type="update_subscriptions",
            scope="env:test:subscriptions",
            payload={"symbols": ["BTCUSDT", "ETHUSDT"], "stream_types": ["trade", "kline"], "updated_by": "tester"},
            requested_by="tester",
            requested_at="2026-04-10T10:01:00+00:00",
        )
    )

    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    assert dataset_catalog_path(tmp_path, "test").exists()
    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    subscriptions = read_subscription_config(tmp_path, "test", default_symbols=cfg.symbols, default_stream_types=("trade", "kline"))
    assert subscriptions.symbols == ("BTCUSDT", "ETHUSDT")


def test_worker_can_publish_snapshot_and_benchmark_serving(tmp_path):
    cfg = _cfg(tmp_path)
    _write_trade_dataset(tmp_path)
    refresh_curated_store(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT")
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-publish",
            command_type="publish_snapshot",
            scope="env:test:trade:BTCUSDT:publication",
            payload={"symbol": "BTCUSDT", "stream_type": "trade"},
            requested_by="tester",
            requested_at="2026-04-10T10:00:00+00:00",
        )
    )
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-benchmark",
            command_type="benchmark_serving",
            scope="env:test:trade:BTCUSDT",
            payload={"symbol": "BTCUSDT", "stream_type": "trade"},
            requested_by="tester",
            requested_at="2026-04-10T10:01:00+00:00",
        )
    )

    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    assert publication_path(tmp_path, "test", stream_type="trade").exists()
    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    assert store.get_command("cmd-benchmark").status == "succeeded"


def test_worker_executes_service_level_refresh_and_storage_lifecycle(tmp_path):
    cfg = _cfg(tmp_path)
    _write_trade_dataset(tmp_path)
    refresh_curated_store(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT")
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-service-levels",
            command_type="refresh_service_levels",
            scope="env:test:service-levels",
            payload={},
            requested_by="tester",
            requested_at="2026-04-10T10:00:00+00:00",
        )
    )
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-storage",
            command_type="apply_storage_lifecycle",
            scope="env:test:storage-lifecycle",
            payload={"sample_every": 2},
            requested_by="tester",
            requested_at="2026-04-10T10:01:00+00:00",
        )
    )

    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    assert dataset_service_levels_path(tmp_path, "test").exists()
    assert process_next_command(store=store, cfg=cfg, worker_id="worker-1") is True
    assert storage_lifecycle_execution_path(tmp_path, "test").exists()

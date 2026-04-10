from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.config import AppConfig
from app.controlplane.api import build_app
from app.controlplane.sqlite_store import SQLiteControlPlaneStore
from app.ingestion.storage import ParquetWriter
from app.marketdata.dataset_catalog import refresh_dataset_catalog
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord, write_raw_manifest
from app.marketdata.serving import refresh_curated_store


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
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    raw_sink = JsonlRawSink(tmp_path / "raw", env="test")
    raw_path = raw_sink.write(
        RawRecord(
            payload={"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "E": int(ts.timestamp() * 1000), "p": "100.0", "q": "1.0", "a": 1}},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            source_id="1",
        )
    )
    write_raw_manifest(raw_path)
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


def test_api_exposes_catalog_query_snapshot_and_subscriptions(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.control_plane_telemetry_dir.mkdir(parents=True, exist_ok=True)
    with (cfg.control_plane_telemetry_dir / "ingestion_summary.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"recorded_at": "2026-04-10T10:00:00+00:00", "trace_id": "trace-1", "env": "test", "mode": "paper", "result": "ok", "events_persisted": 1, "stream_metrics": []}) + "\n")
    _write_trade_dataset(tmp_path)
    refresh_dataset_catalog(tmp_path, "test")
    refresh_curated_store(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT")
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.get("/api/datasets/catalog")
    assert response.status_code == 200
    assert response.json()["entries"][0]["dataset_id"].startswith("test:BINANCE:BTCUSDT:trade")

    response = client.get("/api/datasets/query", params={"stream_type": "trade", "symbol": "BTCUSDT"})
    assert response.status_code == 200
    assert response.json()["count"] == 1

    response = client.get("/api/datasets/snapshot", params={"stream_type": "trade", "symbol": "BTCUSDT"})
    assert response.status_code == 200
    assert response.json()["trade_id"] == "1"

    response = client.get("/api/datasets/replay-report", params={"stream_type": "trade", "symbol": "BTCUSDT"})
    assert response.status_code == 200
    assert response.json()["replayed_events"] == 1

    response = client.get("/api/subscriptions")
    assert response.status_code == 200
    assert response.json()["symbols"] == ["BTCUSDT"]


def test_api_exposes_service_levels_gap_fill_and_incidents(tmp_path):
    cfg = _cfg(tmp_path)
    _write_trade_dataset(tmp_path)
    refresh_dataset_catalog(tmp_path, "test")
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.get("/api/datasets/service-levels")
    assert response.status_code == 200

    response = client.get("/api/datasets/gap-fill-plan")
    assert response.status_code == 200

    response = client.get("/api/datasets/incidents")
    assert response.status_code == 200


def test_api_enqueues_catalog_and_subscription_commands(tmp_path):
    cfg = _cfg(tmp_path)
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.post("/api/commands/catalog-refresh", data={"requested_by": "tester"})
    assert response.status_code == 202
    assert store.get_command(response.json()["command_id"]) is not None

    response = client.post(
        "/api/commands/subscriptions",
        data={"symbols": "BTCUSDT,ETHUSDT", "stream_types": "trade,kline", "requested_by": "tester"},
    )
    assert response.status_code == 202
    payload = response.json()
    command = store.get_command(payload["command_id"])
    assert command is not None
    assert command.payload["symbols"] == ["BTCUSDT", "ETHUSDT"]

    response = client.post("/api/commands/service-levels-refresh", data={"requested_by": "tester"})
    assert response.status_code == 202

    response = client.post("/api/commands/storage-lifecycle-apply", data={"requested_by": "tester", "sample_every": 5})
    assert response.status_code == 202

    response = client.post("/api/commands/gap-fill", data={"requested_by": "tester", "stream_type": "trade", "symbol": "BTCUSDT"})
    assert response.status_code == 202

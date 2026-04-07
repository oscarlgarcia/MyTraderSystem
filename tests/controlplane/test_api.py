from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.config import AppConfig
from app.controlplane.api import build_app
from app.controlplane.sqlite_store import SQLiteControlPlaneStore


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


def test_api_renders_overview_and_enqueues_command(tmp_path):
    cfg = _cfg(tmp_path)
    telemetry_dir = cfg.control_plane_telemetry_dir
    telemetry_dir.mkdir(parents=True, exist_ok=True)
    with (telemetry_dir / "ingestion_summary.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "recorded_at": "2026-04-07T10:00:00+00:00",
                    "trace_id": "trace-1",
                    "env": "test",
                    "mode": "paper",
                    "result": "ok",
                    "events_persisted": 3,
                    "stream_metrics": [],
                }
            )
            + "\n"
        )
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.get("/ui/overview")
    assert response.status_code == 200
    assert "Control Plane" in response.text
    assert "Requires attention" in response.text

    response = client.post(
        "/api/commands/ack-alert",
        data={"alert_id": "alert-123", "requested_by": "tester"},
    )
    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "pending"
    assert store.get_command(payload["command_id"]) is not None


def test_api_stream_filters_and_recovery_prefill(tmp_path):
    cfg = _cfg(tmp_path)
    telemetry_dir = cfg.control_plane_telemetry_dir
    telemetry_dir.mkdir(parents=True, exist_ok=True)
    with (telemetry_dir / "ingestion_summary.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "recorded_at": "2026-04-07T10:00:00+00:00",
                    "trace_id": "trace-1",
                    "env": "test",
                    "mode": "paper",
                    "result": "degraded",
                    "events_persisted": 3,
                    "stream_metrics": [
                        {
                            "venue": "BINANCE",
                            "symbol": "BTCUSDT",
                            "stream_type": "kline",
                            "gaps_total": 2,
                        },
                        {
                            "venue": "BINANCE",
                            "symbol": "ETHUSDT",
                            "stream_type": "trade",
                            "duplicates_total": 1,
                        },
                    ],
                }
            )
            + "\n"
        )
    with (telemetry_dir / "checkpoint_audit_ref.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "recorded_at": "2026-04-07T10:00:03+00:00",
                    "stream_key": "BINANCE:BTCUSDT:kline",
                    "checkpoint_path": str(tmp_path / "checkpoint.json"),
                    "cursor_kind": "source_id",
                    "cursor_value": "123",
                    "cursor_seen_entry_count": 5,
                    "checkpoint_metadata": {"mode": "paper"},
                }
            )
            + "\n"
        )
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.get("/ui/streams?status=degraded&symbol=BTCUSDT&stream_type=kline&q=BTC")
    assert response.status_code == 200
    assert "BINANCE:BTCUSDT:kline" in response.text
    assert "BINANCE:ETHUSDT:trade" not in response.text
    assert "Apply filters" in response.text

    response = client.get("/ui/streams/BINANCE:BTCUSDT:kline")
    assert response.status_code == 200
    assert "Checkpoint" in response.text
    assert "Raw metrics JSON" in response.text

    response = client.get("/ui/recovery?symbol=BTCUSDT&stream_type=kline&interval=1m&trace_id=trace-1")
    assert response.status_code == 200
    assert 'value="BTCUSDT"' in response.text
    assert 'value="trace-1"' in response.text
    assert "Recent commands" in response.text


def test_hx_command_feedback_is_richer(tmp_path):
    cfg = _cfg(tmp_path)
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    client = TestClient(build_app(cfg, store=store))

    response = client.post(
        "/api/commands/ack-alert",
        data={"alert_id": "alert-123", "requested_by": "tester"},
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "Command queued" in response.text
    assert "status endpoint" in response.text

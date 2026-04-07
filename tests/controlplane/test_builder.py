from __future__ import annotations

import json

from app.controlplane.builder import ReadModelBuilder
from app.controlplane.sqlite_store import SQLiteControlPlaneStore


def _append(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def test_builder_materializes_overview_from_telemetry(tmp_path):
    telemetry_dir = tmp_path / "telemetry"
    store = SQLiteControlPlaneStore(tmp_path / "control-plane.sqlite")
    builder = ReadModelBuilder(telemetry_dir, store)

    _append(
        telemetry_dir / "ingestion_summary.jsonl",
        {
            "recorded_at": "2026-04-07T10:00:00+00:00",
            "trace_id": "trace-1",
            "env": "test",
            "mode": "paper",
            "result": "ok",
            "events_persisted": 12,
            "stream_metrics": [
                {
                    "venue": "BINANCE",
                    "symbol": "BTCUSDT",
                    "stream_type": "kline",
                    "gaps_total": 1,
                }
            ],
        },
    )
    _append(
        telemetry_dir / "ingestion_health.jsonl",
        {
            "recorded_at": "2026-04-07T10:00:01+00:00",
            "trace_id": "trace-1",
            "env": "test",
            "mode": "paper",
            "result": "degraded",
            "streams_degraded": ["BINANCE:BTCUSDT:kline"],
        },
    )
    _append(
        telemetry_dir / "operational_alert.jsonl",
        {
            "recorded_at": "2026-04-07T10:00:02+00:00",
            "trace_id": "trace-1",
            "env": "test",
            "mode": "paper",
            "alert_type": "gap_detected",
            "alert_severity": "warning",
            "observed": 1,
            "threshold": 1,
            "recommended_action": "inspect",
        },
    )
    _append(
        telemetry_dir / "checkpoint_audit_ref.jsonl",
        {
            "recorded_at": "2026-04-07T10:00:03+00:00",
            "stream_key": "BINANCE:BTCUSDT:kline",
            "checkpoint_path": str(tmp_path / "checkpoint.json"),
            "cursor_kind": "source_id",
            "cursor_value": "123",
            "cursor_seen_entry_count": 5,
            "checkpoint_metadata": {"mode": "paper"},
        },
    )

    builder.sync_once()
    overview = store.overview()
    assert overview.total_runs == 1
    assert overview.streams_total == 1
    assert len(overview.streams_degraded) == 1
    assert len(overview.alerts_open) == 1
    assert overview.checkpoints_total == 1

    # Re-running should not duplicate by offset.
    builder.sync_once()
    assert len(store.list_alerts()) == 1

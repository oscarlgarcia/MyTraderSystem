from __future__ import annotations

from app.config import AppConfig
from app.controlplane.models import CommandRequestRecord
from app.controlplane.sqlite_store import SQLiteControlPlaneStore
from app.controlplane.worker import process_next_command


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


def test_worker_processes_pending_command(tmp_path):
    cfg = _cfg(tmp_path)
    store = SQLiteControlPlaneStore(cfg.control_plane_db_path)
    store.enqueue_command(
        CommandRequestRecord(
            command_id="cmd-1",
            command_type="replay_range",
            scope="BINANCE:BTCUSDT:trade",
            payload={"symbol": "BTCUSDT", "stream_type": "trade"},
            requested_by="tester",
            requested_at="2026-04-07T10:00:00+00:00",
        )
    )

    processed = process_next_command(
        store=store,
        cfg=cfg,
        worker_id="worker-1",
        executors={"replay_range": lambda **_: {"replayed_events": 2}},
    )

    assert processed is True
    command = store.get_command("cmd-1")
    assert command is not None
    assert command.status == "succeeded"
    assert "replayed_events" in (command.result_summary or "")
    assert len(store.list_command_audit()) >= 2

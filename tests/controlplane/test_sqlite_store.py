from __future__ import annotations

from app.controlplane.models import (
    AlertRecord,
    CheckpointSummaryRecord,
    CommandAuditRecord,
    CommandRequestRecord,
    RunRecord,
    StreamStatusRecord,
)
from app.controlplane.sqlite_store import SQLiteControlPlaneStore


def test_sqlite_store_supports_control_plane_contract(tmp_path):
    store = SQLiteControlPlaneStore(tmp_path / "control-plane.sqlite")
    run = RunRecord(
        run_id="run-1",
        trace_id="trace-1",
        env="test",
        mode="paper",
        result="ok",
        updated_at="2026-04-07T10:00:00+00:00",
        summary={"events_persisted": 10},
        health={"result": "ok"},
    )
    store.upsert_run(run)
    store.upsert_stream_status(
        StreamStatusRecord(
            scope="BINANCE:BTCUSDT:kline",
            venue="BINANCE",
            symbol="BTCUSDT",
            stream_type="kline",
            status="degraded",
            run_id="run-1",
            updated_at="2026-04-07T10:00:01+00:00",
            metrics={"gaps_total": 1},
        )
    )
    store.insert_alert(
        AlertRecord(
            alert_id="alert-1",
            trace_id="trace-1",
            env="test",
            mode="paper",
            alert_type="gap_detected",
            severity="warning",
            observed=1.0,
            threshold=1,
            recommended_action="inspect",
            created_at="2026-04-07T10:00:02+00:00",
            payload={"stream": "BINANCE:BTCUSDT:kline"},
        )
    )
    store.upsert_checkpoint_summary(
        CheckpointSummaryRecord(
            stream_key="BINANCE:BTCUSDT:kline",
            checkpoint_path=str(tmp_path / "checkpoint.json"),
            recorded_at="2026-04-07T10:00:03+00:00",
            cursor_kind="source_id",
            cursor_value="1",
            cursor_seen_entry_count=3,
            metadata={"mode": "paper"},
        )
    )
    command = CommandRequestRecord(
        command_id="cmd-1",
        command_type="ack_alert",
        scope="alert:alert-1",
        payload={"alert_id": "alert-1"},
        requested_by="tester",
        requested_at="2026-04-07T10:00:04+00:00",
    )
    store.enqueue_command(command)
    claimed = store.claim_next_command(worker_id="worker-1", started_at="2026-04-07T10:00:05+00:00")
    assert claimed is not None
    assert claimed.status == "running"
    store.complete_command(
        "cmd-1",
        status="succeeded",
        finished_at="2026-04-07T10:00:06+00:00",
        result_summary='{"ok":true}',
        error_summary=None,
    )
    store.append_command_audit(
        CommandAuditRecord(
            command_id="cmd-1",
            event_ts="2026-04-07T10:00:07+00:00",
            event_type="succeeded",
            payload={"ok": True},
        )
    )

    assert store.get_run("run-1") is not None
    assert store.get_stream("BINANCE:BTCUSDT:kline") is not None
    assert len(store.list_alerts()) == 1
    assert store.ack_alert("alert-1", acked_by="tester", acked_at="2026-04-07T10:00:08+00:00") is True
    assert len(store.list_checkpoints()) == 1
    assert store.get_command("cmd-1").status == "succeeded"
    assert len(store.list_command_audit()) == 1
    assert store.overview().streams_total == 1

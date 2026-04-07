from __future__ import annotations

from app.controlplane.models import CheckpointSummaryRecord, OverviewSnapshot, RunRecord, StreamStatusRecord
from app.controlplane.presenters import build_overview_view, build_stream_detail, filter_streams


def test_stream_detail_presenter_exposes_operational_summary():
    stream = StreamStatusRecord(
        scope="BINANCE:BTCUSDT:kline",
        venue="BINANCE",
        symbol="BTCUSDT",
        stream_type="kline",
        status="degraded",
        run_id="run-1",
        updated_at="2026-04-08T09:00:00+00:00",
        metrics={"gaps_total": 2, "duplicates_total": 1},
    )
    checkpoint = CheckpointSummaryRecord(
        stream_key="BINANCE:BTCUSDT:kline",
        checkpoint_path="checkpoint.json",
        recorded_at="2026-04-08T09:00:01+00:00",
        cursor_kind="source_id",
        cursor_value="123",
        cursor_last_event_ts="2026-04-08T08:59:59+00:00",
        metadata={"mode": "paper"},
    )
    run = RunRecord(
        run_id="run-1",
        trace_id="trace-1",
        env="test",
        mode="paper",
        result="degraded",
        updated_at="2026-04-08T09:00:02+00:00",
        summary={"events_persisted": 12, "reconnects": 1},
        health={"streams_degraded": ["BINANCE:BTCUSDT:kline"]},
    )

    detail = build_stream_detail(stream, checkpoint=checkpoint, run=run)

    assert detail.stream.issue_count == 3
    assert detail.stream.suggested_action == "resync"
    assert detail.stream.checkpoint_cursor == "source_id / 123"
    assert "queue resync" in detail.recommended_actions


def test_overview_presenter_keeps_attention_lists():
    run = RunRecord(
        run_id="run-1",
        trace_id="trace-1",
        env="test",
        mode="paper",
        result="ok",
        updated_at="2026-04-08T09:00:00+00:00",
        summary={"events_persisted": 22},
        health={},
    )
    stream = StreamStatusRecord(
        scope="BINANCE:BTCUSDT:trade",
        venue="BINANCE",
        symbol="BTCUSDT",
        stream_type="trade",
        status="degraded",
        run_id="run-1",
        updated_at="2026-04-08T09:00:01+00:00",
        metrics={"heartbeat_missed_total": 1},
    )
    overview = OverviewSnapshot(
        total_runs=1,
        recent_runs=(run,),
        streams_total=1,
        streams_degraded=(stream,),
        alerts_open=(),
        checkpoints_total=0,
        commands_pending=1,
    )

    view = build_overview_view(overview, checkpoints_by_scope={}, runs_by_id={"run-1": run})

    assert view.degraded_streams_total == 1
    assert view.recent_runs[0].summary_line == "22 persisted, 0 reconnects"
    assert view.attention_streams[0].suggested_action == "replay"


def test_filter_streams_applies_server_side_filters():
    streams = [
        StreamStatusRecord(
            scope="BINANCE:BTCUSDT:kline",
            venue="BINANCE",
            symbol="BTCUSDT",
            stream_type="kline",
            status="ok",
            run_id=None,
            updated_at="2026-04-08T09:00:00+00:00",
            metrics={},
        ),
        StreamStatusRecord(
            scope="BINANCE:ETHUSDT:trade",
            venue="BINANCE",
            symbol="ETHUSDT",
            stream_type="trade",
            status="degraded",
            run_id=None,
            updated_at="2026-04-08T09:00:00+00:00",
            metrics={},
        ),
    ]

    filtered = filter_streams(streams, status="degraded", symbol="ETHUSDT", stream_type="trade", query="ETH")

    assert [item.scope for item in filtered] == ["BINANCE:ETHUSDT:trade"]

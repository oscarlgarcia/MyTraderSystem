from __future__ import annotations

import json
from pathlib import Path

from app.ingestion.storage import normalized_partition_path, read_parquet
from app.ops.quarantine_cli import (
    default_quarantine_paths,
    list_quarantine_records,
    main,
    replay_quarantine_records,
)


def _write_record(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def test_list_quarantine_records_filters_by_trace_symbol_and_stream(tmp_path: Path) -> None:
    dlq_path, schema_path, _anomaly_path = default_quarantine_paths(tmp_path)
    _write_record(
        dlq_path,
        {
            "ts": "2026-04-02T12:00:00+00:00",
            "error_category": "validation",
            "error_severity": "permanent",
            "error_type": "IngestionError",
            "error_message": "bad close",
            "raw_message": '{"stream":"btcusdt@kline_1m","data":{"s":"BTCUSDT","E":1710000000000,"k":{"t":1709999940000,"T":1709999999999,"o":"100","h":"101","l":"99","c":"100","q":"1","i":"1m"}}}',
            "context": {"trace_id": "trace-a", "stage": "stream"},
        },
    )
    _write_record(
        schema_path,
        {
            "ts": "2026-04-02T12:01:00+00:00",
            "error_category": "parse",
            "error_severity": "permanent",
            "error_type": "SchemaDriftError",
            "error_message": "schema drift",
            "raw_message": '{"stream":"ethusdt@trade","data":{"s":"ETHUSDT","E":1710000000000,"p":"100","q":"1","unexpected":{"x":1}}}',
            "context": {"trace_id": "trace-b", "stage": "stream", "quarantine_reason": "schema_drift"},
        },
    )

    rows = list_quarantine_records(base_dir=tmp_path, trace_id="trace-a", symbol="BTCUSDT", stream_type="kline")

    assert len(rows) == 1
    assert rows[0]["trace_id"] == "trace-a"
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["stream_type"] == "kline"


def test_replay_quarantine_records_dry_run_reports_no_normalized_change(tmp_path: Path) -> None:
    dlq_path, _schema_path, _anomaly_path = default_quarantine_paths(tmp_path)
    _write_record(
        dlq_path,
        {
            "ts": "2026-04-02T12:00:00+00:00",
            "error_category": "validation",
            "error_severity": "permanent",
            "error_type": "IngestionError",
            "error_message": "fixed payload",
            "raw_message": '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1710000000000,"p":"100","q":"1","t":7}}',
            "context": {"trace_id": "trace-replay", "stage": "stream"},
        },
    )

    report = replay_quarantine_records(base_dir=tmp_path, env="dev", trace_id="trace-replay", write_normalized=False)

    assert report.inspected_records == 1
    assert report.replayed_records == 1
    assert report.failed_records == 0
    assert report.normalized_modified is False
    assert report.persisted_events == 0
    assert report.results[0].status == "replayed"


def test_replay_quarantine_records_can_write_normalized_and_persist_report(tmp_path: Path) -> None:
    dlq_path, _schema_path, _anomaly_path = default_quarantine_paths(tmp_path)
    report_path = tmp_path / "docs" / "validation" / "quarantine-replay-report.json"
    _write_record(
        dlq_path,
        {
            "ts": "2026-04-02T12:00:00+00:00",
            "error_category": "validation",
            "error_severity": "permanent",
            "error_type": "IngestionError",
            "error_message": "fixed payload",
            "raw_message": '{"stream":"btcusdt@kline_1m","data":{"s":"BTCUSDT","E":1710000000000,"k":{"t":1709999940000,"T":1709999999999,"o":"100","h":"101","l":"99","c":"100","q":"1","i":"1m","x":true}}}',
            "context": {"trace_id": "trace-write", "stage": "stream"},
        },
    )

    report = replay_quarantine_records(
        base_dir=tmp_path,
        env="dev",
        trace_id="trace-write",
        write_normalized=True,
        report_path=report_path,
    )

    partition_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="kline",
        symbol="BTCUSDT",
        day="2024-03-09",
        venue="BINANCE",
    )

    assert report.replayed_records == 1
    assert report.failed_records == 0
    assert report.normalized_modified is True
    assert report.persisted_events == 1
    assert report.touched_partitions == (str(partition_path),)
    assert report_path.exists()
    table = read_parquet(partition_path)
    assert table.num_rows == 1


def test_quarantine_cli_main_help_and_replay_exit_codes(tmp_path: Path, capsys) -> None:
    dlq_path, _schema_path, _anomaly_path = default_quarantine_paths(tmp_path)
    _write_record(
        dlq_path,
        {
            "ts": "2026-04-02T12:00:00+00:00",
            "error_category": "validation",
            "error_severity": "permanent",
            "error_type": "IngestionError",
            "error_message": "fixed payload",
            "raw_message": '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1710000000000,"p":"100","q":"1","t":7}}',
            "context": {"trace_id": "trace-cli", "stage": "stream"},
        },
    )

    assert main(["--base-dir", str(tmp_path), "list", "--trace-id", "trace-cli"]) == 0
    list_output = capsys.readouterr().out
    assert "trace-cli" in list_output

    assert main(["--base-dir", str(tmp_path), "replay", "--env", "dev", "--trace-id", "trace-cli"]) == 0
    replay_output = capsys.readouterr().out
    assert '"normalized_modified": false' in replay_output


def test_list_quarantine_records_reads_marketdata_anomaly_quarantine_file(tmp_path: Path) -> None:
    _dlq_path, _schema_path, anomaly_path = default_quarantine_paths(tmp_path)
    _write_record(
        anomaly_path,
        {
            "ts": "2026-04-02T12:00:00+00:00",
            "error_category": "validation",
            "error_severity": "permanent",
            "error_type": "MarketdataAnomalyError",
            "error_message": "marketdata anomaly",
            "raw_message": '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1710000000000,"p":"170","q":"1","t":7}}',
            "context": {"trace_id": "trace-anomaly", "stage": "stream", "stream_type": "trade", "symbol": "BTCUSDT"},
            "incident": {"anomaly_action": "fail"},
        },
    )

    rows = list_quarantine_records(base_dir=tmp_path, trace_id="trace-anomaly")

    assert len(rows) == 1
    assert rows[0]["error_type"] == "MarketdataAnomalyError"

import json
from datetime import datetime, timezone
from pathlib import Path

from app.marketdata.raw_sink import JsonlRawSink, RawRecord, build_raw_manifest, write_raw_manifest



def test_raw_manifest_reports_checksum_and_line_count(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="dev")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    path = sink.write(
        RawRecord(
            payload={"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "E": int(ts.timestamp() * 1000), "p": "100", "q": "1", "t": 1}},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=ts,
            receive_ts=ts,
            source_id="1",
        )
    )

    manifest = build_raw_manifest(path)
    manifest_path = write_raw_manifest(path)

    assert manifest["line_count"] == 1
    assert manifest["sha256"]
    assert manifest["run_ids"] == [sink.run_id]
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["line_count"] == 1

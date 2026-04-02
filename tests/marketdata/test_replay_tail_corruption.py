import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.marketdata.replay import ReplaySource, read_raw_entries


def _trade_envelope(symbol: str, event_ms: int, trade_id: int, price: str) -> dict:
    return {
        "stream": f"{symbol.lower()}@trade",
        "data": {
            "s": symbol,
            "E": event_ms,
            "p": price,
            "q": "1",
            "t": trade_id,
        },
    }



def test_replay_skips_truncated_tail_and_quarantines_it(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sink.write(
        RawRecord(
            payload=_trade_envelope("BTCUSDT", int(base.timestamp() * 1000), 1, "100"),
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base + timedelta(seconds=1),
            source_id="1",
        )
    )
    path = next((tmp_path / "raw").glob("env=test/venue=BINANCE/stream_type=trade/symbol=BTCUSDT/date=*/events.jsonl"))
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"payload": ')  # truncated tail

    out = list(ReplaySource(base_dir=tmp_path / "raw", env="test", symbol="BTCUSDT", stream_types=("trade",)).stream())

    quarantine = tmp_path / "errors" / "replay-corruption-dlq.jsonl"
    assert len(out) == 1
    assert out[0].trade_id == "1"
    assert quarantine.exists()
    rows = [json.loads(line) for line in quarantine.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["error_type"] == "ReplayRawCorruptionError"



def test_replay_entries_include_runtime_order_metadata_when_present(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sink.write(
        RawRecord(
            payload=_trade_envelope("BTCUSDT", int(base.timestamp() * 1000), 11, "100"),
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base,
            source_id="11",
        )
    )

    entries = read_raw_entries(tmp_path / "raw", "test", symbol="BTCUSDT", stream_types=("trade",))
    replayed = list(ReplaySource(base_dir=tmp_path / "raw", env="test", symbol="BTCUSDT", stream_types=("trade",)).stream())

    assert entries[0].record.run_id is not None
    assert entries[0].record.ingestion_seq == 1
    assert replayed[0].metadata["raw_run_id"] == entries[0].record.run_id
    assert replayed[0].metadata["raw_ingestion_seq"] == "1"

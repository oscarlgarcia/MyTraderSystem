from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.ops.replay_parity import build_replay_parity_report


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



def test_replay_parity_report_passes_for_single_trade(tmp_path: Path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    raw_sink = JsonlRawSink(tmp_path / "raw", env="dev")
    record = RawRecord(
        payload=_trade_envelope("BTCUSDT", int(ts.timestamp() * 1000), 1, "100"),
        venue="BINANCE",
        stream_type="trade",
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        source_id="1",
    )
    raw_sink.write(record)

    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="1",
            trade_id="1",
            side="buy",
            price=100.0,
            size=1.0,
            metadata={
                "raw_run_id": record.run_id,
                "raw_ingestion_seq": str(record.ingestion_seq),
            },
        )
    )
    writer.flush()

    report = build_replay_parity_report(
        raw_base_dir=tmp_path / "raw",
        normalized_path=normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01"),
        env="dev",
        symbol="BTCUSDT",
        stream_type="trade",
    )

    assert report.pass_ok is True
    assert report.order_match is True



def test_contract_and_parity_scripts_help_run():
    for script_name in ("scripts/check_normalized_contract.py", "scripts/check_replay_parity.py"):
        result = subprocess.run([sys.executable, script_name, "--help"], capture_output=True, text=True, check=False)
        assert result.returncode == 0
        assert "--help" in result.stdout or "usage:" in result.stdout.lower()

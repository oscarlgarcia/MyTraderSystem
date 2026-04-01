from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.ingestion.storage import list_normalized_parquet_files, read_parquet, ParquetWriter
from app.marketdata import NORMALIZER_VERSION
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.marketdata.replay import ReplaySource


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


def _write_raw_trade(
    sink: JsonlRawSink,
    *,
    symbol: str,
    event_ts: datetime,
    receive_ts: datetime,
    trade_id: int,
    price: str,
) -> Path:
    return sink.write(
        RawRecord(
            payload=_trade_envelope(symbol, int(event_ts.timestamp() * 1000), trade_id, price),
            venue="BINANCE",
            stream_type="trade",
            symbol=symbol,
            exchange_ts=event_ts,
            receive_ts=receive_ts,
            trace_id="trace-ing-017",
            source_id=str(trade_id),
        )
    )


def test_raw_replay_preserves_exact_append_only_order(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base,
        receive_ts=base + timedelta(seconds=2),
        trade_id=10,
        price="100",
    )
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base + timedelta(seconds=1),
        receive_ts=base + timedelta(seconds=3),
        trade_id=11,
        price="101",
    )
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base + timedelta(seconds=2),
        receive_ts=base + timedelta(seconds=4),
        trade_id=12,
        price="102",
    )

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="test",
            symbol="BTCUSDT",
            stream_types=("trade",),
            normalizer_version=NORMALIZER_VERSION,
        ).stream()
    )

    assert [event.trade_id for event in replayed] == ["10", "11", "12"]
    assert [event.receive_ts for event in replayed] == [
        base + timedelta(seconds=2),
        base + timedelta(seconds=3),
        base + timedelta(seconds=4),
    ]
    assert all(isinstance(event, TradeEvent) for event in replayed)


def test_raw_and_normalized_parity_holds_for_replayed_trade_dataset(tmp_path: Path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base,
        receive_ts=base + timedelta(seconds=1),
        trade_id=101,
        price="100",
    )
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base + timedelta(seconds=1),
        receive_ts=base + timedelta(seconds=2),
        trade_id=102,
        price="101",
    )

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="test",
            symbol="BTCUSDT",
            stream_types=("trade",),
            normalizer_version=NORMALIZER_VERSION,
        ).stream()
    )

    writer = ParquetWriter(base_dir=tmp_path / "data", env="test", flush_size=10, dedup=False)
    writer.add(replayed)
    writer.flush()

    normalized_files = list_normalized_parquet_files(tmp_path / "data", "test", include_legacy=False)
    assert len(normalized_files) == 1

    table = read_parquet(normalized_files[0])
    rows = table.to_pylist()

    assert [row["symbol"] for row in rows] == [event.symbol for event in replayed]
    assert [row["event_ts"] for row in rows] == [event.exchange_ts for event in replayed]
    assert [dict(row["metadata"])["trade_id"] for row in rows] == [event.trade_id for event in replayed]
    assert all(row["normalizer_version"] == NORMALIZER_VERSION for row in rows)

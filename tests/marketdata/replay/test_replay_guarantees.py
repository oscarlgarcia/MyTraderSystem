from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from app.ingestion import backfill
from app.ingestion.storage import list_normalized_parquet_files, read_parquet, ParquetWriter
from app.marketdata import NORMALIZER_VERSION
from app.marketdata.models import BarEvent, TradeEvent
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


def _trade_signature(event: TradeEvent) -> tuple:
    def _ts_ms(value):
        return value.replace(microsecond=(value.microsecond // 1000) * 1000) if value is not None else None

    return (
        event.symbol,
        event.trade_id,
        event.exchange_ts,
        _ts_ms(event.receive_ts),
        _ts_ms(event.process_ts),
        event.price,
        event.size,
        event.side,
        event.source_id,
    )


def _trade_row_signature(row: dict) -> tuple:
    return (
        row["symbol"],
        row["trade_id"],
        row["exchange_ts"],
        row["receive_ts"],
        row["process_ts"],
        row["price"],
        row["size"],
        row["side"],
        row["source_id"],
    )


def _bar_signature(event: BarEvent) -> tuple:
    def _ts_ms(value):
        return value.replace(microsecond=(value.microsecond // 1000) * 1000) if value is not None else None

    return (
        event.symbol,
        event.exchange_ts,
        _ts_ms(event.receive_ts),
        _ts_ms(event.process_ts),
        event.open,
        event.high,
        event.low,
        event.close,
        event.volume,
        event.interval,
        _ts_ms(event.open_ts),
        _ts_ms(event.close_ts),
        event.source_id,
    )


def _bar_row_signature(row: dict) -> tuple:
    return (
        row["symbol"],
        row["exchange_ts"],
        row["receive_ts"],
        row["process_ts"],
        row["open"],
        row["high"],
        row["low"],
        row["close"],
        row["volume"],
        row["interval"],
        row["open_ts"],
        row["close_ts"],
        row["source_id"],
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

    assert [_trade_row_signature(row) for row in rows] == [_trade_signature(event) for event in replayed]
    assert all(row["normalizer_version"] == NORMALIZER_VERSION for row in rows)


def test_backfill_raw_replay_and_normalized_parity_hold_for_bar_dataset(tmp_path: Path, monkeypatch):
    cfg = SimpleNamespace(env="test", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "99", "101", "98", "100", "10", 1704067260000],
        [1704067260001, "100", "102", "99", "101", "11", 1704067320000],
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(
        [
            "--env",
            "test",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:10:00+00:00",
            "--interval",
            "1m",
            "--dedup",
        ]
    )

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="test",
            symbol="BTCUSDT",
            stream_types=("kline",),
            normalizer_version=NORMALIZER_VERSION,
        ).stream()
    )

    assert all(isinstance(event, BarEvent) for event in replayed)

    normalized_files = list_normalized_parquet_files(tmp_path, "test", include_legacy=False)
    assert len(normalized_files) == 1
    table = read_parquet(normalized_files[0])
    rows_out = table.to_pylist()

    assert [_bar_row_signature(row) for row in rows_out] == [_bar_signature(event) for event in replayed[: len(rows_out)]]
    assert all(row["normalizer_version"] == NORMALIZER_VERSION for row in rows_out)

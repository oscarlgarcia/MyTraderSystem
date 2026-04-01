from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from app.ingestion import pipeline
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import BinanceSource
from app.ingestion.storage import ParquetWriter, list_normalized_parquet_files, read_parquet
from app.marketdata import NORMALIZER_VERSION
from app.marketdata.models import TradeEvent
from app.marketdata.raw_sink import JsonlRawSink
from app.marketdata.replay import ReplaySource, read_raw_entries


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


def test_live_raw_replay_and_normalized_trade_dataset_have_same_parity(tmp_path: Path):
    cfg = mock.Mock(
        env="test",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )

    def fake_ws_stream(_url: str, end_time=None):
        del end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":1001,"m":false}}'
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067201000,"p":"101","q":"2","t":1002,"m":true}}'

    source = BinanceSource(
        cfg,
        ws_stream=fake_ws_stream,
        raw_sink=JsonlRawSink(tmp_path / "raw", env="test"),
    )
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path / "data", env="test", flush_size=10, dedup=True))

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=source,
        sink=sink,
    )

    assert all(isinstance(event, TradeEvent) for event in out)

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="test",
            symbol="BTCUSDT",
            stream_types=("trade",),
            normalizer_version=NORMALIZER_VERSION,
        ).stream()
    )
    normalized_files = list_normalized_parquet_files(tmp_path / "data", "test", include_legacy=False)
    assert len(normalized_files) == 1
    rows = read_parquet(normalized_files[0]).to_pylist()

    assert [_trade_signature(event) for event in replayed] == [_trade_signature(event) for event in out]
    assert [_trade_row_signature(row) for row in rows] == [_trade_signature(event) for event in replayed]
    assert [row["normalizer_version"] for row in rows] == [NORMALIZER_VERSION, NORMALIZER_VERSION]


def test_live_raw_ingestion_seq_is_monotonic_across_symbol_partitions(tmp_path: Path):
    cfg = mock.Mock(
        env="test",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT", "ETHUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )

    def fake_ws_stream(_url: str, end_time=None):
        del end_time
        yield '{"stream":"ethusdt@trade","data":{"s":"ETHUSDT","E":1704067200000,"p":"200","q":"1","t":2001,"m":false}}'
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067201000,"p":"100","q":"1","t":1001,"m":false}}'
        yield '{"stream":"ethusdt@trade","data":{"s":"ETHUSDT","E":1704067202000,"p":"201","q":"1","t":2002,"m":true}}'

    source = BinanceSource(
        cfg,
        ws_stream=fake_ws_stream,
        raw_sink=JsonlRawSink(tmp_path / "raw", env="test"),
    )
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path / "data", env="test", flush_size=10, dedup=True))

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=source,
        sink=sink,
    )

    entries = read_raw_entries(tmp_path / "raw", "test", stream_types=("trade",))

    assert [event.trade_id for event in out] == ["2001", "1001", "2002"]
    assert [entry.record.ingestion_seq for entry in entries] == [1, 2, 3]
    assert len({entry.record.run_id for entry in entries}) == 1
    assert [entry.record.symbol for entry in entries] == ["ETHUSDT", "BTCUSDT", "ETHUSDT"]

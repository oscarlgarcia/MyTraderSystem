from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.common.dto import MarketEvent
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.compaction import compact_partition
from app.marketdata import NORMALIZER_VERSION
from app.marketdata.instruments import instrument_catalog_version
from app.marketdata.models import BarEvent, BookEvent, TradeEvent
from app.ingestion.storage import (
    ParquetWriter,
    legacy_partition_path,
    normalized_partition_path,
    normalized_partition_data_path,
    partition_segments_dir,
    read_parquet,
)


def make_event(symbol: str, ts: datetime, price: float = 1.0, size: float = 1.0, source: str = "trade"):
    return MarketEvent(symbol=symbol, event_ts=ts, price=price, size=size, source=source)


def _out_path(tmp_path: Path, *, symbol: str, day: str, source: str = "trade") -> Path:
    return normalized_partition_path(tmp_path, "dev", source=source, symbol=symbol, day=day)


def _data_path(tmp_path: Path, *, symbol: str, day: str, source: str = "trade") -> Path:
    return normalized_partition_data_path(tmp_path, "dev", source=source, symbol=symbol, day=day)


def test_flush_writes_partition_and_preserves_order(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=3)
    ts = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    events = [
        make_event("BTCUSDT", ts),
        make_event("BTCUSDT", ts.replace(hour=1)),
        make_event("BTCUSDT", ts.replace(hour=2)),
    ]
    for ev in events:
        writer.add(ev)
    out = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    assert out.exists()
    table = read_parquet(out)
    assert table.num_rows == 3
    assert table.column("feed_type").to_pylist() == ["trades", "trades", "trades"]
    assert table.column("normalizer_version").to_pylist() == [NORMALIZER_VERSION] * 3
    assert table.schema.metadata[b"normalizer_version"] == NORMALIZER_VERSION.encode("utf-8")


def test_partition_by_date(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    writer.add(make_event("ETHUSDT", ts1))
    writer.add(make_event("ETHUSDT", ts2))
    writer.flush()
    p1 = _out_path(tmp_path, symbol="ETHUSDT", day="2024-01-01")
    p2 = _out_path(tmp_path, symbol="ETHUSDT", day="2024-01-02")
    assert p1.exists() and p2.exists()


def test_flush_threshold(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=2)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    assert not (tmp_path / "normalized").exists()
    writer.add(make_event("BTCUSDT", ts))
    assert _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01").exists()


def test_partition_by_symbol(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    writer.add(make_event("ETHUSDT", ts))
    writer.flush()
    assert _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01").exists()
    assert _out_path(tmp_path, symbol="ETHUSDT", day="2024-01-01").exists()


def test_flush_manual_without_threshold(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()
    assert _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01").exists()


def test_schema_stable_across_writer_instances(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer1 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer1.add(make_event("BTCUSDT", ts))
    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer2.add(make_event("BTCUSDT", ts.replace(hour=1)))
    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 2


def test_online_v2_writer_appends_segments_without_compacting_existing_partition(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer1 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer1.add(make_event("BTCUSDT", ts))

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer2.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))

    partition_path = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    segments = sorted(partition_segments_dir(partition_path).glob("*.parquet"))

    assert len(segments) == 2
    assert not _data_path(tmp_path, symbol="BTCUSDT", day="2024-01-01").exists()
    assert read_parquet(partition_path).num_rows == 2


def test_offline_compaction_merges_segments_into_single_partition_snapshot(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer1 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer1.add(make_event("BTCUSDT", ts))
    writer1.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer2.add(make_event("BTCUSDT", ts))
    writer2.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))
    writer2.flush()

    partition_path = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    assert len(sorted(partition_segments_dir(partition_path).glob("*.parquet"))) == 2

    compacted = compact_partition(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")

    assert compacted == _data_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    assert compacted.exists()
    assert not partition_segments_dir(partition_path).exists()
    table = read_parquet(partition_path)
    assert table.num_rows == 2


def test_dedup_sorts_after_merge(tmp_path):
    ts = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(make_event("BTCUSDT", ts.replace(hour=1)))
    writer.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer2.add(make_event("BTCUSDT", ts))
    writer2.add(make_event("BTCUSDT", ts.replace(hour=1)))
    writer2.flush()

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)
    assert table.num_rows == 2


def test_atomic_write_never_exposes_partial_parquet(monkeypatch, tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()

    out = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    before = read_parquet(out).to_pylist()

    def broken_write(table, path, use_dictionary=False):
        Path(path).write_bytes(b"partial")
        raise RuntimeError("disk full")

    monkeypatch.setattr("app.ingestion.storage.pq.write_table", broken_write)

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer2.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))
    with pytest.raises(RuntimeError):
        writer2.flush()

    after = read_parquet(out).to_pylist()
    assert after == before
    assert not list(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01").rglob("*.tmp"))


def test_sink_failure_preserves_previous_valid_file(monkeypatch, tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()

    out = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01")
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10))
    sink.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))

    monkeypatch.setattr("app.ingestion.storage.pq.write_table", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sink failure")))

    with pytest.raises(RuntimeError):
        sink.close()

    table = read_parquet(out)
    assert table.num_rows == 1
    assert sink.writer.persisted_events == 0
    assert sink.writer.buffered_events == 1


def test_existing_partition_merge_remains_ordered(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts + timedelta(hours=2), price=102.0))
    writer.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer2.add(make_event("BTCUSDT", ts, price=100.0))
    writer2.add(make_event("BTCUSDT", ts + timedelta(hours=1), price=101.0))
    writer2.flush()

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)


def test_sink_exposes_accepted_vs_persisted_counts(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10))

    sink.add(make_event("BTCUSDT", ts))

    assert sink.accepted_count == 1
    assert sink.persisted_count == 0
    assert sink.buffered_count == 1

    sink.close()

    assert sink.accepted_count == 1
    assert sink.persisted_count == 1
    assert sink.buffered_count == 0
    assert sink.write_latency_seconds >= 0.0
    assert sink.last_write_latency_seconds >= 0.0


def test_dedup_threshold_appends_without_oom(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100, dedup=True, max_dedup_rows=2)
    writer.add(make_event("BTCUSDT", ts))
    writer.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))
    writer.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100, dedup=True, max_dedup_rows=2)
    writer2.add(make_event("BTCUSDT", ts))
    writer2.add(make_event("BTCUSDT", ts + timedelta(minutes=2)))
    writer2.flush()

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 3
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)


def test_v2_storage_never_mixes_trades_and_bars_in_same_partition(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts, source="trade"))
    writer.add(make_event("BTCUSDT", ts, source="kline"))
    writer.flush()

    trades_path = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01", source="trade")
    bars_path = _out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01", source="kline")

    assert trades_path.exists()
    assert bars_path.exists()
    assert read_parquet(trades_path).column("source").to_pylist() == ["trade"]
    bars_table = read_parquet(bars_path)
    assert bars_table.column("source").to_pylist() == ["kline"]
    assert "open" in bars_table.column_names
    assert "close" in bars_table.column_names


def test_read_parquet_keeps_legacy_v1_compatibility(tmp_path):
    path = legacy_partition_path(tmp_path, "dev", "BTCUSDT", "2024-01-01")
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    table = pa.Table.from_pydict(
        {
            "symbol": ["BTCUSDT"],
            "event_ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "price": [100.0],
            "size": [1.0],
            "source": ["trade"],
            "metadata": [{"venue": "BINANCE"}],
        },
        schema=schema,
    ).replace_schema_metadata({b"schema_version": b"v1"})
    pq.write_table(table, path, use_dictionary=False)

    loaded = read_parquet(path)
    assert loaded.num_rows == 1


def test_v2_dataset_includes_normalizer_version_metadata_and_column(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    writer.add(make_event("BTCUSDT", ts))

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))

    assert "normalizer_version" in table.column_names
    assert table.column("normalizer_version").to_pylist() == [NORMALIZER_VERSION]
    assert table.schema.metadata[b"normalizer_version"] == NORMALIZER_VERSION.encode("utf-8")
    assert table.schema.metadata[b"instrument_catalog_version"] == instrument_catalog_version().encode("utf-8")
    assert b"instrument_snapshot" in table.schema.metadata


def test_trade_dataset_persists_first_class_typed_columns(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    exchange_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    receive_ts = exchange_ts + timedelta(milliseconds=1)
    process_ts = exchange_ts + timedelta(milliseconds=2)

    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=exchange_ts,
            receive_ts=receive_ts,
            process_ts=process_ts,
            venue="BINANCE",
            source_id="7001",
            price=100.0,
            size=1.0,
            trade_id="7001",
            side="buy",
        )
    )

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))

    assert "trade_id" in table.column_names
    assert "side" in table.column_names
    assert "exchange_ts" in table.column_names
    assert "receive_ts" in table.column_names
    assert "process_ts" in table.column_names
    assert "source_id" in table.column_names
    assert table.column("trade_id").to_pylist() == ["7001"]
    assert table.column("side").to_pylist() == ["buy"]
    assert table.column("exchange_ts").to_pylist() == [exchange_ts]
    assert table.column("receive_ts").to_pylist() == [receive_ts]
    assert table.column("process_ts").to_pylist() == [process_ts]
    assert table.schema.metadata[b"instrument_catalog_version"] == instrument_catalog_version().encode("utf-8")
    assert b"instrument_snapshot" in table.schema.metadata
    assert dict(table.column("metadata").to_pylist()[0])["instrument_catalog_version"] == instrument_catalog_version()


def test_trade_dataset_merges_old_v2_rows_with_first_class_columns(tmp_path):
    path = normalized_partition_data_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("venue", pa.string()),
            ("feed_type", pa.string()),
            ("normalizer_version", pa.string()),
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    old_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    old_receive_ts = old_ts + timedelta(milliseconds=1)
    old_process_ts = old_ts + timedelta(milliseconds=2)
    table = pa.Table.from_pydict(
        {
            "venue": ["BINANCE"],
            "feed_type": ["trades"],
            "normalizer_version": [NORMALIZER_VERSION],
            "symbol": ["BTCUSDT"],
            "event_ts": [old_ts],
            "price": [100.0],
            "size": [1.0],
            "source": ["trade"],
            "metadata": [
                {
                    "trade_id": "8001",
                    "source_id": "8001",
                    "side": "sell",
                    "receive_ts": old_receive_ts.isoformat(),
                    "process_ts": old_process_ts.isoformat(),
                    "venue": "BINANCE",
                }
            ],
        },
        schema=schema,
    ).replace_schema_metadata(
        {
            b"schema_version": b"v2",
            b"normalizer_version": NORMALIZER_VERSION.encode("utf-8"),
        }
    )
    pq.write_table(table, path, use_dictionary=False)

    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=old_ts + timedelta(seconds=1),
            receive_ts=old_ts + timedelta(seconds=1, milliseconds=1),
            process_ts=old_ts + timedelta(seconds=1, milliseconds=2),
            venue="BINANCE",
            source_id="8002",
            price=101.0,
            size=1.0,
            trade_id="8002",
            side="buy",
        )
    )
    writer.flush()

    loaded = read_parquet(path)

    assert loaded.num_rows == 2
    assert loaded.column("trade_id").to_pylist() == ["8001", "8002"]
    assert loaded.column("side").to_pylist() == ["sell", "buy"]
    assert loaded.column("receive_ts").to_pylist()[0] == old_receive_ts
    assert loaded.column("process_ts").to_pylist()[0] == old_process_ts


def test_bar_dataset_persists_first_class_typed_columns(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    exchange_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    open_ts = exchange_ts - timedelta(minutes=1)
    close_ts = exchange_ts
    receive_ts = exchange_ts + timedelta(milliseconds=1)
    process_ts = exchange_ts + timedelta(milliseconds=2)

    writer.add(
        BarEvent(
            symbol="BTCUSDT",
            exchange_ts=exchange_ts,
            receive_ts=receive_ts,
            process_ts=process_ts,
            venue="BINANCE",
            source_id="bar-1",
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=5.0,
            interval="1m",
            open_ts=open_ts,
            close_ts=close_ts,
        )
    )

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01", source="kline"))

    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "interval",
        "open_ts",
        "close_ts",
        "exchange_ts",
        "receive_ts",
        "process_ts",
        "source_id",
    ):
        assert column in table.column_names

    assert table.column("open").to_pylist() == [100.0]
    assert table.column("high").to_pylist() == [102.0]
    assert table.column("low").to_pylist() == [99.0]
    assert table.column("close").to_pylist() == [101.0]
    assert table.column("volume").to_pylist() == [5.0]
    assert table.column("interval").to_pylist() == ["1m"]
    assert table.column("open_ts").to_pylist() == [open_ts]
    assert table.column("close_ts").to_pylist() == [close_ts]
    assert table.column("exchange_ts").to_pylist() == [exchange_ts]
    assert table.column("receive_ts").to_pylist() == [receive_ts]
    assert table.column("process_ts").to_pylist() == [process_ts]
    assert table.column("source_id").to_pylist() == ["bar-1"]
    assert table.schema.metadata[b"instrument_catalog_version"] == instrument_catalog_version().encode("utf-8")
    assert b"instrument_snapshot" in table.schema.metadata
    assert dict(table.column("metadata").to_pylist()[0])["instrument_catalog_version"] == instrument_catalog_version()


def test_bar_dataset_merges_old_v2_rows_with_first_class_columns(tmp_path):
    path = normalized_partition_data_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01")
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("venue", pa.string()),
            ("feed_type", pa.string()),
            ("normalizer_version", pa.string()),
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    old_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    old_open_ts = old_ts - timedelta(minutes=1)
    old_close_ts = old_ts
    old_receive_ts = old_ts + timedelta(milliseconds=1)
    old_process_ts = old_ts + timedelta(milliseconds=2)
    table = pa.Table.from_pydict(
        {
            "venue": ["BINANCE"],
            "feed_type": ["bars"],
            "normalizer_version": [NORMALIZER_VERSION],
            "symbol": ["BTCUSDT"],
            "event_ts": [old_ts],
            "price": [101.0],
            "size": [5.0],
            "source": ["kline"],
            "metadata": [
                {
                    "open": "100.0",
                    "high": "102.0",
                    "low": "99.0",
                    "interval": "1m",
                    "open_ts": old_open_ts.isoformat(),
                    "close_ts": old_close_ts.isoformat(),
                    "receive_ts": old_receive_ts.isoformat(),
                    "process_ts": old_process_ts.isoformat(),
                    "source_id": "bar-legacy",
                    "venue": "BINANCE",
                }
            ],
        },
        schema=schema,
    ).replace_schema_metadata(
        {
            b"schema_version": b"v2",
            b"normalizer_version": NORMALIZER_VERSION.encode("utf-8"),
        }
    )
    pq.write_table(table, path, use_dictionary=False)

    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        BarEvent(
            symbol="BTCUSDT",
            exchange_ts=old_ts + timedelta(minutes=1),
            receive_ts=old_ts + timedelta(minutes=1, milliseconds=1),
            process_ts=old_ts + timedelta(minutes=1, milliseconds=2),
            venue="BINANCE",
            source_id="bar-new",
            open=101.0,
            high=103.0,
            low=100.0,
            close=102.0,
            volume=6.0,
            interval="1m",
            open_ts=old_ts,
            close_ts=old_ts + timedelta(minutes=1),
        )
    )
    writer.flush()

    loaded = read_parquet(path)

    assert loaded.num_rows == 2
    assert loaded.column("open").to_pylist() == [100.0, 101.0]
    assert loaded.column("high").to_pylist() == [102.0, 103.0]
    assert loaded.column("low").to_pylist() == [99.0, 100.0]
    assert loaded.column("close").to_pylist() == [101.0, 102.0]
    assert loaded.column("volume").to_pylist() == [5.0, 6.0]
    assert loaded.column("interval").to_pylist() == ["1m", "1m"]
    assert loaded.column("open_ts").to_pylist()[0] == old_open_ts
    assert loaded.column("close_ts").to_pylist()[0] == old_close_ts
    assert loaded.column("receive_ts").to_pylist()[0] == old_receive_ts
    assert loaded.column("process_ts").to_pylist()[0] == old_process_ts
    assert loaded.column("source_id").to_pylist() == ["bar-legacy", "bar-new"]


def test_normalized_storage_rejects_book_feed_as_out_of_scope(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)

    with pytest.raises(ValueError, match="book feed is out of scope for normalized storage"):
        writer.add(
            BookEvent(
                symbol="BTCUSDT",
                exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
                venue="BINANCE",
                bid_price=100.0,
                bid_size=1.0,
                ask_price=101.0,
                ask_size=1.0,
                sequence_id="42",
            )
        )


def test_read_parquet_backfills_missing_normalizer_version_for_old_v2_files(tmp_path):
    path = normalized_partition_data_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("venue", pa.string()),
            ("feed_type", pa.string()),
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    table = pa.Table.from_pydict(
        {
            "venue": ["BINANCE"],
            "feed_type": ["trades"],
            "symbol": ["BTCUSDT"],
            "event_ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "price": [100.0],
            "size": [1.0],
            "source": ["trade"],
            "metadata": [{}],
        },
        schema=schema,
    ).replace_schema_metadata({b"schema_version": b"v2"})
    pq.write_table(table, path, use_dictionary=False)

    loaded = read_parquet(path)

    assert loaded.column("normalizer_version").to_pylist() == [NORMALIZER_VERSION]
    assert loaded.schema.metadata[b"normalizer_version"] == NORMALIZER_VERSION.encode("utf-8")


def test_storage_dedup_keeps_distinct_rows_when_native_trade_ids_differ(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=ts,
            price=100.0,
            size=1.0,
            source="trade",
            metadata={"trade_id": "3001", "source_id": "3001", "venue": "BINANCE"},
        )
    )
    writer.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer2.add(
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=ts,
            price=100.0,
            size=1.0,
            source="trade",
            metadata={"trade_id": "3002", "source_id": "3002", "venue": "BINANCE"},
        )
    )
    writer2.flush()

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 2


def test_storage_dedup_collapses_rows_when_native_trade_id_matches(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=ts,
            price=100.0,
            size=1.0,
            source="trade",
            metadata={"trade_id": "4001", "source_id": "4001", "venue": "BINANCE"},
        )
    )
    writer.flush()

    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer2.add(
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=ts,
            price=100.0,
            size=1.0,
            source="trade",
            metadata={"trade_id": "4001", "source_id": "4001", "venue": "BINANCE"},
        )
    )
    writer2.flush()

    table = read_parquet(_out_path(tmp_path, symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 1


def test_read_parquet_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_parquet(tmp_path / "missing.parquet")

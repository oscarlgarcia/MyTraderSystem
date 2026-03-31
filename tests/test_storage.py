from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app.common.dto import MarketEvent
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.storage import ParquetWriter, read_parquet


def make_event(symbol: str, ts: datetime, price: float = 1.0, size: float = 1.0, source: str = "trade"):
    return MarketEvent(symbol=symbol, event_ts=ts, price=price, size=size, source=source)


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
    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    assert out.exists()
    table = read_parquet(out)
    assert table.num_rows == 3
    # order preserved
    prices = table.column("price").to_pylist()
    assert prices == [1.0, 1.0, 1.0]


def test_partition_by_date(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    writer.add(make_event("ETHUSDT", ts1))
    writer.add(make_event("ETHUSDT", ts2))
    writer.flush()
    p1 = tmp_path / "dev" / "symbol=ETHUSDT" / "date=2024-01-01" / "data.parquet"
    p2 = tmp_path / "dev" / "symbol=ETHUSDT" / "date=2024-01-02" / "data.parquet"
    assert p1.exists() and p2.exists()


def test_flush_threshold(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=2)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    assert not (tmp_path / "dev").exists()
    writer.add(make_event("BTCUSDT", ts))
    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    assert out.exists()


def test_partition_by_symbol(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    writer.add(make_event("ETHUSDT", ts))
    writer.flush()
    p1 = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    p2 = tmp_path / "dev" / "symbol=ETHUSDT" / "date=2024-01-01" / "data.parquet"
    assert p1.exists() and p2.exists()


def test_flush_manual_without_threshold(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()
    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    assert out.exists()


def test_schema_stable_across_writer_instances(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer1 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer1.add(make_event("BTCUSDT", ts))
    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer2.add(make_event("BTCUSDT", ts.replace(hour=1)))
    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    table = read_parquet(out)
    assert table.num_rows == 2


def test_dedup_sorts_after_merge(tmp_path):
    ts = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer.add(make_event("BTCUSDT", ts.replace(hour=1)))  # later
    writer.flush()

    # new event earlier + duplicate
    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    writer2.add(make_event("BTCUSDT", ts))
    writer2.add(make_event("BTCUSDT", ts.replace(hour=1)))  # duplicate
    writer2.flush()

    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    table = read_parquet(out)
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)
    assert table.num_rows == 2


def test_atomic_write_never_exposes_partial_parquet(monkeypatch, tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()

    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
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
    assert not out.with_name("data.parquet.tmp").exists()


def test_sink_failure_preserves_previous_valid_file(monkeypatch, tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    writer.add(make_event("BTCUSDT", ts))
    writer.flush()

    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10))
    sink.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))

    def broken_write(*args, **kwargs):
        raise RuntimeError("sink failure")

    monkeypatch.setattr("app.ingestion.storage.pq.write_table", broken_write)

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

    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    table = read_parquet(out)
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


def test_dedup_threshold_appends_without_oom(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100, dedup=True, max_dedup_rows=2)
    # existing file with 2 rows
    writer.add(make_event("BTCUSDT", ts))
    writer.add(make_event("BTCUSDT", ts + timedelta(minutes=1)))
    writer.flush()

    # new writer triggers threshold path
    writer2 = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=100, dedup=True, max_dedup_rows=2)
    writer2.add(make_event("BTCUSDT", ts))  # duplicate
    writer2.add(make_event("BTCUSDT", ts + timedelta(minutes=2)))
    writer2.flush()

    out = tmp_path / "dev" / "symbol=BTCUSDT" / "date=2024-01-01" / "data.parquet"
    table = read_parquet(out)
    assert table.num_rows == 3
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)


def test_read_parquet_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_parquet(tmp_path / "missing.parquet")

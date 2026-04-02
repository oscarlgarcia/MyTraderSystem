from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.storage import ParquetWriter, normalized_partition_path, read_parquet
from app.marketdata.models import TradeEvent
from app.ops.normalized_contract import validate_normalized_contract


def test_trade_normalized_schema_preserves_raw_lineage_and_provider_ts(tmp_path: Path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    event = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        provider_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id="101",
        trade_id="101",
        side="buy",
        price=100.0,
        size=1.0,
        metadata={
            "raw_run_id": "run-1",
            "raw_ingestion_seq": "1",
            "historical_feed_kind": "aggregate_trade",
        },
    )
    writer.add(event)
    writer.flush()

    path = normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    rows = read_parquet(path).to_pylist()

    assert rows[0]["provider_ts"] == ts
    assert rows[0]["raw_run_id"] == "run-1"
    assert rows[0]["raw_ingestion_seq"] == 1
    assert rows[0]["historical_feed_kind"] == "aggregate_trade"
    assert dict(rows[0]["metadata"])["raw_run_id"] == "run-1"



def test_trade_normalized_order_uses_raw_lineage_for_same_exchange_ts(tmp_path: Path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=False)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    second = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id="202",
        trade_id="202",
        side="buy",
        price=101.0,
        size=1.0,
        metadata={"raw_run_id": "run-1", "raw_ingestion_seq": "2"},
    )
    first = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        source_id="201",
        trade_id="201",
        side="buy",
        price=100.0,
        size=1.0,
        metadata={"raw_run_id": "run-1", "raw_ingestion_seq": "1"},
    )
    writer.add([second, first])
    writer.flush()

    path = normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    rows = read_parquet(path).to_pylist()
    assert [row["trade_id"] for row in rows] == ["201", "202"]



def test_normalized_contract_cli_helper_passes_for_trade_partition(tmp_path: Path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10, dedup=True)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            provider_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="301",
            trade_id="301",
            side="sell",
            price=99.0,
            size=2.0,
            metadata={
                "raw_run_id": "run-2",
                "raw_ingestion_seq": "1",
                "historical_feed_kind": "aggregate_trade",
            },
        )
    )
    writer.flush()

    path = normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    report = validate_normalized_contract(path)

    assert report.pass_ok is True
    assert report.feed_type == "trade"
    assert not report.missing_columns
    assert not report.missing_metadata_keys

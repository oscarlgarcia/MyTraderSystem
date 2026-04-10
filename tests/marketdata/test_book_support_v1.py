from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.client import build_ws_url, parse_typed_message
from app.ingestion.storage import ParquetWriter, normalized_partition_path, read_parquet
from app.marketdata.models import BookEvent


def test_bookticker_message_parses_to_book_event():
    message = (
        '{"stream":"btcusdt@bookTicker","data":{"u":42,"s":"BTCUSDT","b":"100.0","B":"1.2","a":"100.5","A":"1.5"}}'
    )

    event = parse_typed_message(
        message,
        receive_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        process_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    assert isinstance(event, BookEvent)
    assert event.source == "book"
    assert event.bid_price == 100.0
    assert event.ask_price == 100.5


def test_book_event_can_be_persisted_in_normalized_quotes_partition(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(
        BookEvent(
            symbol="BTCUSDT",
            exchange_ts=ts,
            receive_ts=ts,
            process_ts=ts,
            venue="BINANCE",
            source_id="42",
            sequence_id="42",
            bid_price=100.0,
            bid_size=1.0,
            ask_price=100.5,
            ask_size=1.5,
            metadata={"raw_run_id": "run-1", "raw_ingestion_seq": "1"},
        )
    )
    writer.flush()

    path = normalized_partition_path(tmp_path, "test", source="book", symbol="BTCUSDT", day="2024-01-01")
    rows = read_parquet(path).to_pylist()

    assert len(rows) == 1
    assert rows[0]["feed_type"] == "quotes"
    assert rows[0]["sequence_id"] == "42"


def test_ws_url_builder_accepts_book_stream():
    url = build_ws_url("wss://stream.binance.test", ["BTCUSDT"], stream_types=("trade", "book"))
    assert "btcusdt@bookTicker" in url

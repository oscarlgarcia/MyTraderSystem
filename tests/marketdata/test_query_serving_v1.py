from __future__ import annotations

from datetime import datetime, timezone

from app.ingestion.storage import ParquetWriter
from app.marketdata.models import TradeEvent
from app.marketdata.query import HistoricalQueryRequest, query_latest_row, query_rows
from app.marketdata.serving import CuratedServingStore, refresh_curated_store, serving_db_path
from app.marketdata.snapshot_service import SnapshotRequest, load_snapshot


def _write_trade_series(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True)
    ts1 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    for idx, ts in enumerate((ts1, ts2), start=1):
        writer.add(
            TradeEvent(
                symbol="BTCUSDT",
                exchange_ts=ts,
                provider_ts=ts,
                receive_ts=ts,
                process_ts=ts,
                venue="BINANCE",
                source_id=str(idx),
                trade_id=str(idx),
                side="buy",
                price=100.0 + idx,
                size=1.0,
                metadata={
                    "raw_run_id": "run-1",
                    "raw_ingestion_seq": str(idx),
                    "historical_feed_kind": "aggregate_trade",
                    "metadata_snapshot_mode": "runtime",
                    "instrument_catalog_snapshot_json": "[]",
                },
            )
        )
    writer.flush()


def test_query_rows_and_latest_return_expected_events(tmp_path):
    _write_trade_series(tmp_path)

    request = HistoricalQueryRequest(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT")
    rows = query_rows(request)
    latest = query_latest_row(request)

    assert len(rows) == 2
    assert latest is not None
    assert latest["trade_id"] == "2"


def test_refresh_curated_store_and_snapshot_service_use_latest_row(tmp_path):
    _write_trade_series(tmp_path)

    report = refresh_curated_store(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT")
    store = CuratedServingStore(serving_db_path(tmp_path, "test"))
    latest = store.latest(env="test", venue="BINANCE", stream_type="trade", symbol="BTCUSDT")
    snapshot = load_snapshot(SnapshotRequest(base_dir=tmp_path, env="test", stream_type="trade", symbol="BTCUSDT"))

    assert report.refreshed_rows == 2
    assert latest is not None
    assert latest["trade_id"] == "2"
    assert snapshot is not None
    assert snapshot["trade_id"] == "2"

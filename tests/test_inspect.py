from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.common.dto import MarketEvent
from app.ingestion.inspect import collect_events


def test_collect_events_filters_and_limits(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer.add(MarketEvent(symbol="BTCUSDT", event_ts=ts, price=1.0, size=1.0, source="trade"))
    writer.add(MarketEvent(symbol="ETHUSDT", event_ts=ts, price=2.0, size=1.0, source="trade"))
    writer.flush()

    rows = collect_events(tmp_path, "dev", symbol="BTCUSDT", limit=5)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"


def test_collect_events_filter_date_and_limit(tmp_path):
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10)
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    writer.add(MarketEvent(symbol="BTCUSDT", event_ts=ts1, price=1.0, size=1.0, source="trade"))
    writer.add(MarketEvent(symbol="BTCUSDT", event_ts=ts2, price=2.0, size=1.0, source="trade"))
    writer.flush()

    # Limitar a fecha 2024-01-01
    files = [normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")]
    assert files[0].exists()
    dataset_rows = collect_events(tmp_path, "dev", symbol="BTCUSDT", date="2024-01-01", limit=1)
    assert len(dataset_rows) == 1
    assert str(dataset_rows[0]["event_ts"]).startswith("2024-01-01")

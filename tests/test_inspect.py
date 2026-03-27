from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.storage import ParquetWriter
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

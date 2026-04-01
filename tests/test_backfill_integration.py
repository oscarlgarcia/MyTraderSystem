import datetime as dt
import os
from types import SimpleNamespace
from pathlib import Path

import pytest

from app.ingestion import backfill
from app.marketdata.models import BarEvent
from app.marketdata.replay import ReplaySource
from app.ingestion.storage import normalized_partition_path, read_parquet


@pytest.mark.slow
def test_backfill_integration_local_mock(tmp_path, monkeypatch):
    """End-to-end con datos simulados (5 minutos) escribiendo Parquet."""
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    start = dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
    rows = []
    for i in range(5):
        open_ms = int((start + dt.timedelta(minutes=i)).timestamp() * 1000)
        close_ms = open_ms + 60_000
        rows.append([open_ms, str(99 + i), str(101 + i), str(98 + i), str(100 + i), str(10 + i), close_ms])

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(
        [
            "--env",
            "dev",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:05:00+00:00",
            "--interval",
            "1m",
            "--batch",
            "5",
        ]
    )

    path = normalized_partition_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01")
    assert path.exists()
    table = read_parquet(path)
    assert table.num_rows == 5
    assert "open" in table.column_names
    assert "close" in table.column_names

    raw_files = list((tmp_path / "raw").glob("env=dev/venue=BINANCE/stream_type=kline/symbol=BTCUSDT/date=*/events.jsonl"))
    assert len(raw_files) == 1

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="dev",
            symbol="BTCUSDT",
            stream_types=("kline",),
        ).stream()
    )
    assert len(replayed) == 5
    assert all(isinstance(event, BarEvent) for event in replayed)

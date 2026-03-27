import datetime as dt
import os
from types import SimpleNamespace
from pathlib import Path

import pytest

from app.ingestion import backfill
from app.ingestion.storage import read_parquet


@pytest.mark.slow
def test_backfill_integration_local_mock(tmp_path, monkeypatch):
    """End-to-end con datos simulados (5 minutos) escribiendo Parquet."""
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    start = dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
    rows = []
    for i in range(5):
        open_ms = int((start + dt.timedelta(minutes=i)).timestamp() * 1000)
        close_ms = open_ms + 60_000
        rows.append([open_ms, "", "", "", str(100 + i), str(10 + i), close_ms])

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

    files = list(tmp_path.glob("dev/symbol=BTCUSDT/date=2024-01-01/data.parquet"))
    assert files
    table = read_parquet(files[0])
    assert table.num_rows == 5

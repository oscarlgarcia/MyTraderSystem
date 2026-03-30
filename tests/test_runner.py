from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.ingestion import runner
from app.common.dto import MarketEvent
from app.ingestion.storage import read_parquet
from app.observability.logger import get_logger as base_logger


class DummyConfig(SimpleNamespace):
    pass


def make_cfg(tmp_path: Path):
    return DummyConfig(
        env="dev",
        data_dir=tmp_path,
        log_level="INFO",
        ws_base="wss://example/stream",
        rest_base="https://example",
        symbols=["BTCUSDT"],
    )


def test_dry_run_writes_parquet(monkeypatch, tmp_path):
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(runner, "load_config", lambda env=None: cfg)
    # Avoid real logging IO
    monkeypatch.setattr(runner, "get_logger", lambda name=None, level=None: base_logger(stream=None))

    rc = runner.run(["--env", "dev", "--duration", "0.1", "--dry-run"])
    assert rc == 0
    files = list(tmp_path.glob("dev/symbol=*/date=*/data.parquet"))
    assert files, "expected parquet output"
    table = read_parquet(files[0])
    assert table.num_rows > 0


def test_ws_endpoint_invalid_raises(monkeypatch, tmp_path):
    cfg = make_cfg(tmp_path)
    cfg.ws_base = "ftp://bad"  # invalid scheme, will still build URL but connect will fail; mock to force raise
    original_source = runner.BinanceSource

    def fail_stream(url, end_time=None):
        raise ConnectionError("invalid endpoint")

    monkeypatch.setattr(runner, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(runner, "BinanceSource", lambda cfg: original_source(cfg, ws_stream=fail_stream))
    with pytest.raises(ConnectionError):
        runner.run(["--env", "dev", "--duration", "0.1"])


def test_timer_stops_infinite_stream(monkeypatch, tmp_path):
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(runner, "load_config", lambda env=None: cfg)
    original_source = runner.BinanceSource

    class DummyResponse:
        status_code = 200

        def json(self):
            return []

        def raise_for_status(self):
            return None

    def infinite_stream(url, end_time=None):
        while True:
            yield MarketEvent(symbol="BTCUSDT", event_ts=datetime.now(timezone.utc), price=1.0, size=1.0, source="trade")

    monkeypatch.setattr(
        runner,
        "BinanceSource",
        lambda cfg: original_source(cfg, ws_stream=infinite_stream, http_get=lambda *args, **kwargs: DummyResponse()),
    )
    rc = runner.run(["--env", "dev", "--duration", "0.01"])
    assert rc == 0

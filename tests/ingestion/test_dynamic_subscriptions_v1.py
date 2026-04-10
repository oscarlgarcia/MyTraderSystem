from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.config import AppConfig
from app.ingestion.errors import IngestionError
from app.ingestion.sources import BinanceSource
from app.marketdata.subscriptions import update_subscription_config


@dataclass
class _CatalogState:
    instrument_catalog_version: str = "v1"
    path: Path | None = None
    metadata_snapshot_mode: str = "runtime"
    venue_snapshot_path: Path | None = None
    fallback_reason: str | None = None
    drift: object | None = None

    @property
    def catalog(self):
        return {}

    def instrument_metadata(self, symbol: str, *, venue: str) -> dict[str, str]:
        return {"instrument_catalog_version": "v1", "venue": venue, "symbol": symbol}


def _cfg(tmp_path) -> AppConfig:
    return AppConfig(
        env="test",
        data_dir=tmp_path,
        log_level="INFO",
        ws_base="wss://example.test",
        rest_base="https://example.test",
        symbols=["BTCUSDT"],
        control_plane_backend="sqlite",
        control_plane_db_path=tmp_path / "control-plane.sqlite",
        control_plane_db_url=None,
        control_plane_telemetry_dir=tmp_path / "telemetry",
        control_plane_poll_interval_seconds=5.0,
        control_plane_command_poll_interval_seconds=1.0,
    )


def test_binance_source_detects_runtime_subscription_revision_change(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.ingestion.sources.persist_runtime_instrument_catalog_snapshot",
        lambda **_: _CatalogState(path=tmp_path / "catalog.json"),
    )
    cfg = _cfg(tmp_path)

    def fake_stream(url, end_time=None):
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100.0","q":"1.0","a":1}}'
        update_subscription_config(
            base_dir=tmp_path,
            env="test",
            symbols=("BTCUSDT", "ETHUSDT"),
            stream_types=("trade", "kline"),
            updated_by="tester",
        )
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067201000,"p":"101.0","q":"1.0","a":2}}'

    source = BinanceSource(cfg, ws_stream=fake_stream, subscription_reload_interval_seconds=0.0)

    iterator = source.stream()
    first = next(iterator)
    assert first.symbol == "BTCUSDT"
    with pytest.raises(IngestionError):
        next(iterator)

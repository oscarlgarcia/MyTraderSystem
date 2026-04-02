from __future__ import annotations

from pathlib import Path

import pytest

from app.marketdata.instrument_loader import fetch_binance_exchange_info_snapshot
from app.ops.ingestion_validation import _default_fetch_rows


pytestmark = [pytest.mark.network]


def test_binance_exchange_info_contract_live() -> None:
    payload = fetch_binance_exchange_info_snapshot(base_url="https://api.binance.com")
    assert isinstance(payload.get("symbols"), list)
    btc = next(item for item in payload["symbols"] if item.get("symbol") == "BTCUSDT")
    assert btc["baseAsset"] == "BTC"
    assert btc["quoteAsset"] == "USDT"


def test_binance_kline_rest_contract_live() -> None:
    rows = _default_fetch_rows(
        rest_base="https://api.binance.com",
        symbol="BTCUSDT",
        interval="1m",
        bars=3,
        end_time=None,
    )
    assert len(rows) == 3
    assert all(len(row) >= 7 for row in rows)


def test_bundled_exchange_info_snapshot_exists() -> None:
    bundled_path = Path(__file__).resolve().parents[2] / "app" / "marketdata" / "data" / "binance_exchange_info.json"
    assert bundled_path.exists()

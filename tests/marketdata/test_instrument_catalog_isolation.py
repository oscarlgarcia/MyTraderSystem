from pathlib import Path

from app.marketdata.instrument_loader import load_binance_exchange_info_snapshot
from app.marketdata.instruments import (
    active_instrument_catalog,
    persist_runtime_instrument_catalog_snapshot,
    resolve_instrument,
    use_instrument_catalog,
)


def test_runtime_catalog_context_isolated_from_default_catalog(monkeypatch, tmp_path: Path):
    snapshot = load_binance_exchange_info_snapshot()
    modified_snapshot = dict(snapshot)
    modified_symbols = []
    for item in snapshot["symbols"]:
        row = dict(item)
        if row.get("symbol") == "BTCUSDT":
            row["filters"] = [
                dict(filter_item, tickSize="0.10000000") if filter_item.get("filterType") == "PRICE_FILTER" else dict(filter_item)
                for filter_item in row.get("filters", [])
            ]
        modified_symbols.append(row)
    modified_snapshot["symbols"] = modified_symbols

    monkeypatch.setattr(
        "app.marketdata.instruments.fetch_binance_exchange_info_snapshot",
        lambda *, base_url: modified_snapshot,
    )

    default_tick_size = resolve_instrument("BTCUSDT", venue="BINANCE").tick_size
    persisted = persist_runtime_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="isolated-run",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
    )

    assert resolve_instrument("BTCUSDT", venue="BINANCE").tick_size == default_tick_size
    with use_instrument_catalog(persisted.catalog):
        assert active_instrument_catalog().resolve("BINANCE", "BTCUSDT").tick_size == "0.10000000"
    assert resolve_instrument("BTCUSDT", venue="BINANCE").tick_size == default_tick_size

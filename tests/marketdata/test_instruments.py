from app.marketdata.instruments import (
    InstrumentCatalog,
    ensure_default_instruments,
    infer_spot_assets,
    instrument_catalog_version,
    instrument_metadata,
    instrument_snapshot,
    resolve_instrument,
)


def test_infer_spot_assets_splits_binance_symbol():
    assert infer_spot_assets("BTCUSDT") == ("BTC", "USDT")


def test_default_catalog_resolves_static_binance_symbol():
    instrument = resolve_instrument("BTCUSDT", venue="BINANCE")

    assert instrument.base_asset == "BTC"
    assert instrument.quote_asset == "USDT"
    assert instrument.contract_type == "spot"
    assert instrument.price_precision == 2


def test_ensure_default_instruments_registers_config_symbols():
    ensure_default_instruments(["SOLUSDT"], venue="BINANCE")

    instrument = resolve_instrument("SOLUSDT", venue="BINANCE")

    assert instrument.base_asset == "SOL"
    assert instrument.quote_asset == "USDT"


def test_catalog_raises_for_unknown_symbol_without_quote_match():
    catalog = InstrumentCatalog()

    try:
        catalog.resolve("BINANCE", "BTCUNKNOWN")
    except KeyError as exc:
        assert "unsupported instrument" in str(exc)
    else:
        raise AssertionError("expected KeyError for unsupported instrument")


def test_catalog_version_changes_when_catalog_changes():
    catalog = InstrumentCatalog()
    catalog.register_static_spot_symbol("BTCUSDT", venue="BINANCE")
    version_before = catalog.version()

    catalog.register_static_spot_symbol("ETHUSDT", venue="BINANCE")

    assert catalog.version() != version_before


def test_instrument_metadata_includes_catalog_version_and_snapshot():
    metadata = instrument_metadata("BTCUSDT", venue="BINANCE")

    assert metadata["instrument_catalog_version"] == instrument_catalog_version()
    assert "\"symbol\":\"BTCUSDT\"" in metadata["instrument_snapshot"]
    assert instrument_snapshot("BTCUSDT", venue="BINANCE")["quote_asset"] == "USDT"

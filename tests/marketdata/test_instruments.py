import json
from pathlib import Path

from app.marketdata.instrument_loader import load_binance_exchange_info_snapshot, load_binance_instrument_records
from app.marketdata.instruments import (
    DEFAULT_INSTRUMENTS,
    Instrument,
    InstrumentCatalog,
    detect_instrument_catalog_drift,
    ensure_default_instruments,
    infer_spot_assets,
    instrument_catalog_snapshot_json,
    instrument_catalog_version,
    instrument_metadata,
    instrument_snapshot,
    persist_instrument_catalog_snapshot,
    persist_runtime_instrument_catalog_snapshot,
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
    assert instrument.metadata_source in {"venue_snapshot", "venue_runtime_snapshot"}
    assert instrument.venue_snapshot_version is not None


def test_ensure_default_instruments_registers_config_symbols():
    ensure_default_instruments(["SOLUSDT"], venue="BINANCE")

    instrument = resolve_instrument("SOLUSDT", venue="BINANCE")

    assert instrument.base_asset == "SOL"
    assert instrument.quote_asset == "USDT"
    assert float(instrument.tick_size) > 0.0
    assert instrument.metadata_source in {"venue_snapshot", "venue_runtime_snapshot"}


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
    assert metadata["metadata_source"] in {"venue_snapshot", "venue_runtime_snapshot"}
    assert "venue_snapshot_version" in metadata


def test_binance_loader_reads_authoritative_snapshot_records():
    snapshot = load_binance_exchange_info_snapshot()
    records = load_binance_instrument_records(symbols=("BTCUSDT", "SOLUSDT"))

    assert snapshot["symbols"]
    assert [record.symbol for record in records] == ["BTCUSDT", "SOLUSDT"]
    assert all(record.metadata_source == "venue_snapshot" for record in records)
    assert len({record.venue_snapshot_version for record in records}) == 1


def test_detect_instrument_catalog_drift_marks_material_field_changes():
    before = [entry for entry in json.loads(instrument_catalog_snapshot_json())]
    after = [dict(entry) for entry in before]
    btc_entry = next(entry for entry in after if entry["symbol"] == "BTCUSDT")
    btc_entry["tick_size"] = "0.10000000"
    btc_entry["price_precision"] = 1

    drift = detect_instrument_catalog_drift(before, after)

    assert drift.has_drift is True
    assert drift.material is True
    assert "BTCUSDT" in drift.changed_symbols
    assert "tick_size" in drift.changed_fields_by_symbol["BTCUSDT"]
    assert "price_precision" in drift.changed_fields_by_symbol["BTCUSDT"]


def test_persist_instrument_catalog_snapshot_writes_run_and_detects_previous_drift(tmp_path: Path):
    previous_btc = resolve_instrument("BTCUSDT", venue="BINANCE")
    previous_catalog = InstrumentCatalog(
        [
            Instrument(
                venue=previous_btc.venue,
                symbol=previous_btc.symbol,
                base_asset=previous_btc.base_asset,
                quote_asset=previous_btc.quote_asset,
                contract_type=previous_btc.contract_type,
                tick_size="0.10000000",
                step_size=previous_btc.step_size,
                price_precision=1,
                size_precision=previous_btc.size_precision,
                metadata_source=previous_btc.metadata_source,
                venue_snapshot_version="older-snapshot",
            ),
            *[instrument for instrument in DEFAULT_INSTRUMENTS if instrument.symbol != "BTCUSDT"],
        ]
    )
    persist_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="previous-run",
        catalog=previous_catalog,
    )

    current = persist_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="current-run",
    )

    latest_path = tmp_path / "metadata" / "instruments" / "env=dev" / "venue=BINANCE" / "latest.json"
    assert current.path.exists()
    assert latest_path.exists()
    assert current.drift is not None
    assert current.drift.material is True
    assert "BTCUSDT" in current.drift.changed_symbols


def test_persist_runtime_instrument_catalog_snapshot_uses_runtime_snapshot(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "app.marketdata.instruments.fetch_binance_exchange_info_snapshot",
        lambda *, base_url: load_binance_exchange_info_snapshot(),
    )

    current = persist_runtime_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="runtime-run",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
    )

    assert current.metadata_snapshot_mode == "runtime"
    assert current.venue_snapshot_path is not None
    assert current.venue_snapshot_path.exists()
    assert current.fallback_reason is None


def test_persist_runtime_instrument_catalog_snapshot_falls_back_when_vendor_fetch_fails(monkeypatch, tmp_path: Path):
    def _raise(*, base_url):
        raise RuntimeError(f"failed against {base_url}")

    monkeypatch.setattr("app.marketdata.instruments.fetch_binance_exchange_info_snapshot", _raise)

    current = persist_runtime_instrument_catalog_snapshot(
        base_dir=tmp_path,
        env="dev",
        venue="BINANCE",
        run_label="fallback-run",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
    )

    assert current.metadata_snapshot_mode == "fallback"
    assert current.fallback_reason is not None

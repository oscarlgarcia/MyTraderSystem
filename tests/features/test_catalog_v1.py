from app.features.catalog import CatalogPhase, CatalogStatus, get_default_feature_catalog


def test_feature_catalog_has_expected_phase_counts():
    catalog = get_default_feature_catalog()

    assert len(catalog.list_features(phase=CatalogPhase.PHASE_1.value)) == 90
    assert len(catalog.list_features(phase=CatalogPhase.PHASE_2.value)) == 10
    assert len(catalog.list_features(phase=CatalogPhase.PHASE_3.value)) == 12


def test_feature_catalog_exposes_filters_and_runtime_aliases():
    catalog = get_default_feature_catalog()

    trend_entries = catalog.list_features(family="trend")
    assert any(item.feature_name == "trend.sma.3" for item in trend_entries)

    implemented = catalog.list_features(status=CatalogStatus.IMPLEMENTED.value)
    price = next(item for item in implemented if item.feature_name == "price.last")
    assert price.runtime_aliases == ("price",)

    execution_sensitive = catalog.list_features(strategy_family="execution_sensitive")
    assert any(item.feature_name == "flow.vwap.1m" for item in execution_sensitive)


def test_feature_catalog_lists_bundles_and_scopes():
    catalog = get_default_feature_catalog()

    bundles = catalog.list_bundles()
    assert "core_market_bundle" in bundles
    assert "trend_bundle" in bundles
    assert "phase3_extension_bundle" in bundles

    scopes = catalog.list_source_scopes()
    assert "trade" in scopes
    assert "kline" in scopes
    assert "book" in scopes

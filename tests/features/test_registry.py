import pytest

from app.features.definition_registry import DefinitionRegistry, DefinitionRegistryError
from app.features.definitions import build_legacy_feature_set_definition


def _feature_set(version: str, windows, aggregators, transformers):
    return build_legacy_feature_set_definition(
        name="default",
        version=version,
        description="baseline features",
        windows=windows,
        aggregators=aggregators,
        transformers=transformers,
    )


def test_register_and_get(tmp_path):
    reg = DefinitionRegistry(storage_dir=tmp_path)
    fs = reg.register(_feature_set("1.0.0", [3, 5], ["sma", "ema"], ["clip_non_finite"]))
    assert reg.get("default", "1.0.0") == fs


def test_duplicate_raises(tmp_path):
    reg = DefinitionRegistry(storage_dir=tmp_path)
    reg.register(_feature_set("1.0.0", [3], ["sma"], []))
    with pytest.raises(DefinitionRegistryError):
        reg.register(_feature_set("1.0.0", [5], ["ema"], []))


def test_multiple_versions_coexist(tmp_path):
    reg = DefinitionRegistry(storage_dir=tmp_path)
    reg.register(_feature_set("1.0.0", [3], ["sma"], []))
    reg.register(_feature_set("1.1.0", [3, 5], ["sma", "ema"], ["clip_non_finite"]))
    versions = reg.list_versions("default")
    assert set(versions.keys()) == {"1.0.0", "1.1.0"}

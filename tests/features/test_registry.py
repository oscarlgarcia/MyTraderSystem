import pytest

from app.features.registry import FeatureRegistry


def test_register_and_get():
    reg = FeatureRegistry()
    fs = reg.register_feature_set(
        name="default",
        version="1.0.0",
        description="baseline features",
        windows=[3, 5],
        aggregators=["sma", "ema"],
        transformers=["clip_non_finite"],
    )
    assert reg.get("default", "1.0.0") == fs


def test_duplicate_raises():
    reg = FeatureRegistry()
    reg.register_feature_set(
        name="default",
        version="1.0.0",
        description="baseline features",
        windows=[3],
        aggregators=["sma"],
        transformers=[],
    )
    with pytest.raises(ValueError):
        reg.register_feature_set(
            name="default",
            version="1.0.0",
            description="baseline features copy",
            windows=[5],
            aggregators=["ema"],
            transformers=[],
        )


def test_multiple_versions_coexist():
    reg = FeatureRegistry()
    reg.register_feature_set("default", "1.0.0", "baseline", [3], ["sma"], [])
    reg.register_feature_set("default", "1.1.0", "tuned", [3, 5], ["sma", "ema"], ["clip_non_finite"])
    versions = reg.list_versions("default")
    assert set(versions.keys()) == {"1.0.0", "1.1.0"}

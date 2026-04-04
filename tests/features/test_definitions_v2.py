from app.features.definitions import FeatureDefinition, build_legacy_feature_set_definition
from app.features.definition_registry import DefinitionRegistry, DefinitionRegistryError


def test_feature_definition_hash_changes_with_metadata():
    a = FeatureDefinition(name="price", version="1.0.0", description="p", owner="me")
    b = FeatureDefinition(name="price", version="1.0.1", description="p", owner="me")
    assert a.definition_hash != b.definition_hash


def test_definition_registry_is_immutable(tmp_path):
    reg = DefinitionRegistry(storage_dir=tmp_path)
    fs1 = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[3], aggregators=["sma"], transformers=[])
    reg.register(fs1)
    fs2 = build_legacy_feature_set_definition(name="default", version="1.0.0", description="changed", windows=[5], aggregators=["ema"], transformers=[])
    try:
        reg.register(fs2)
        assert False, "Expected immutable conflict"
    except DefinitionRegistryError:
        pass

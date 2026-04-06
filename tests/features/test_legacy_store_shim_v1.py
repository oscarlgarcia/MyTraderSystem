from app.features import store
from app.features import legacy_store_v1


def test_legacy_store_shim_points_to_isolated_module():
    assert store.AGGREGATORS is legacy_store_v1.AGGREGATORS
    assert store.TRANSFORMERS is legacy_store_v1.TRANSFORMERS

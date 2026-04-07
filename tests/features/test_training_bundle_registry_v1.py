from app.features.training_bundle_registry import (
    TrainingBundleRecord,
    TrainingBundleRegistry,
    TrainingBundleRegistryError,
)


def test_training_bundle_registry_roundtrips_record(tmp_path):
    registry = TrainingBundleRegistry(tmp_path)
    record = TrainingBundleRecord(
        bundle_id="train-bundle-1",
        dataset_id="dataset-2024-01",
        feature_schema_hash="schema-v2",
        feature_set_name="default",
        feature_set_version="1.1.0",
        feature_bundle_id="feature-bundle-1",
    )
    registry.register(record)

    loaded = TrainingBundleRegistry(tmp_path).get("train-bundle-1")
    assert loaded == record


def test_training_bundle_registry_rejects_mutation_for_same_id(tmp_path):
    registry = TrainingBundleRegistry(tmp_path)
    registry.register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v2",
            feature_set_name="default",
            feature_set_version="1.1.0",
        )
    )
    try:
        registry.register(
            TrainingBundleRecord(
                bundle_id="train-bundle-1",
                dataset_id="dataset-legacy",
                feature_schema_hash="schema-v2",
                feature_set_name="default",
                feature_set_version="1.1.0",
            )
        )
    except TrainingBundleRegistryError:
        pass
    else:
        raise AssertionError("expected immutable training bundle conflict")


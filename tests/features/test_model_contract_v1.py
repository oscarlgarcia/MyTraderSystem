from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.model_contract import FeatureConsumerContract, validate_feature_contract
from app.features.training_bundle_registry import TrainingBundleRecord, TrainingBundleRegistry


def test_model_contract_validates_feature_set_version_and_required_metadata():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0, "ret_1": 0.1},
        feature_set_name="default",
        feature_set_version="1.1.0",
        lineage_id="bundle-1",
    )
    contract = FeatureConsumerContract(
        consumer_name="paper-strategy",
        consumer_kind="strategy",
        feature_set_name="default",
        feature_set_version="1.1.0",
        required_features=("price", "ret_1"),
        required_metadata_keys=("feature_bundle_id",),
    )
    ok = validate_feature_contract(
        contract=contract,
        feature_vector=vector,
        consumer_metadata={"feature_bundle_id": "bundle-1"},
    )
    assert ok.pass_ok is True

    bad = validate_feature_contract(contract=contract, feature_vector=vector, consumer_metadata={})
    assert bad.pass_ok is False
    assert "missing_metadata:feature_bundle_id" in bad.reasons


def test_model_contract_validates_training_serving_alignment():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0, "ret_1": 0.1},
        feature_set_name="default",
        feature_set_version="1.1.0",
        lineage_id="bundle-1",
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
    )
    contract = FeatureConsumerContract(
        consumer_name="paper-strategy",
        consumer_kind="strategy",
        feature_set_name="default",
        feature_set_version="1.1.0",
        required_features=("price", "ret_1"),
        required_metadata_keys=("feature_bundle_id", "dataset_id", "feature_schema_hash", "training_bundle_id"),
        required_entity_values=(("account", "paper"),),
        required_dataset_id="dataset-2024-01",
        required_schema_hash="schema-v2",
        required_training_bundle_id="train-bundle-1",
        require_feature_bundle_match=True,
    )
    ok = validate_feature_contract(
        contract=contract,
        feature_vector=vector,
        consumer_metadata={
            "feature_bundle_id": "bundle-1",
            "dataset_id": "dataset-2024-01",
            "feature_schema_hash": "schema-v2",
            "training_bundle_id": "train-bundle-1",
        },
    )
    assert ok.pass_ok is True

    bad = validate_feature_contract(
        contract=contract,
        feature_vector=vector,
        consumer_metadata={
            "feature_bundle_id": "bundle-x",
            "dataset_id": "dataset-legacy",
            "feature_schema_hash": "schema-v1",
            "training_bundle_id": "train-bundle-x",
        },
    )
    assert bad.pass_ok is False
    assert "feature_bundle_id_mismatch" in bad.reasons
    assert "dataset_id_mismatch" in bad.reasons
    assert "schema_hash_mismatch" in bad.reasons
    assert "training_bundle_id_mismatch" in bad.reasons


def test_model_contract_validates_registered_training_bundle():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0},
        feature_set_name="default",
        feature_set_version="1.1.0",
        lineage_id="bundle-1",
    )
    contract = FeatureConsumerContract(
        consumer_name="paper-strategy",
        consumer_kind="strategy",
        feature_set_name="default",
        feature_set_version="1.1.0",
        required_features=("price",),
        required_metadata_keys=("dataset_id", "feature_schema_hash", "training_bundle_id"),
        required_dataset_id="dataset-2024-01",
        required_schema_hash="schema-v2",
        required_training_bundle_id="train-bundle-1",
    )
    registry = TrainingBundleRegistry()
    registry.register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v2",
            feature_set_name="default",
            feature_set_version="1.1.0",
        ),
        persist=False,
    )
    ok = validate_feature_contract(
        contract=contract,
        feature_vector=vector,
        consumer_metadata={
            "dataset_id": "dataset-2024-01",
            "feature_schema_hash": "schema-v2",
            "training_bundle_id": "train-bundle-1",
        },
        training_bundle_registry=registry,
    )
    assert ok.pass_ok is True

    bad = validate_feature_contract(
        contract=contract,
        feature_vector=vector,
        consumer_metadata={
            "dataset_id": "dataset-legacy",
            "feature_schema_hash": "schema-v1",
            "training_bundle_id": "train-bundle-1",
        },
        training_bundle_registry=registry,
    )
    assert bad.pass_ok is False
    assert "dataset_id_mismatch" in bad.reasons
    assert "schema_hash_mismatch" in bad.reasons
    assert "training_bundle_dataset_mismatch" in bad.reasons
    assert "training_bundle_schema_mismatch" in bad.reasons

from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.model_contract import FeatureConsumerContract, validate_feature_contract


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

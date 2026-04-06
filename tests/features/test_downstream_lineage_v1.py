from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.execution.paper import paper_execute
from app.risk.rules import apply_risk
from app.strategy.basic import generate_signals


def test_lineage_flows_from_feature_to_intent_and_execution():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    feature = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 101.0, "ret_1": 0.1, "sma_3": 100.0},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id="bundle-1",
    )
    signal = generate_signals([feature])[0]
    intent = apply_risk([signal], price_by_symbol={"BTCUSDT": 101.0})[0]
    report = paper_execute([intent], {"BTCUSDT": 101.0})[0]
    assert intent.metadata["feature_bundle_id"] == "bundle-1"
    assert report.metadata["feature_bundle_id"] == "bundle-1"
    assert report.metadata["feature_set_version"] == "1.0.0"

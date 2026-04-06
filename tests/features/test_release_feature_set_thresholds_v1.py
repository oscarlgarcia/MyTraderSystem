from app.features.definitions import build_legacy_feature_set_definition
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import run_feature_release_gate


def test_release_gate_can_use_feature_set_specific_slos():
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    feature_set.metadata["release_slos"] = {
        "paper": {
            "max_compute_latency_seconds": 10.0,
        }
    }
    metrics = FeatureMetrics(serving_requests=1, serving_latency_max=1.0)
    report = run_feature_release_gate(
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=metrics,
        target="paper",
        feature_set=feature_set,
    )
    assert report.pass_ok is True

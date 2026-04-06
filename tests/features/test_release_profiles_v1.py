from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import run_feature_release_gate


def test_release_gate_blocks_invalid_ratio_breach():
    metrics = FeatureMetrics(serving_requests=10, invalid_serves=2)
    report = run_feature_release_gate(parity_report=ParityReport(pass_ok=True, mismatches=()), metrics=metrics, target="live")
    assert report.pass_ok is False
    assert "invalid_ratio_breached" in report.reasons

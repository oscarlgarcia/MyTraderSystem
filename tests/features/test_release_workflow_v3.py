from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import run_feature_release_gate
from app.features.release_workflow import publish_feature_release, rollback_feature_release


def test_release_gate_uses_metrics_and_parity(tmp_path):
    report = run_feature_release_gate(parity_report=ParityReport(pass_ok=True, mismatches=()), metrics=FeatureMetrics(), target="paper")
    assert report.pass_ok


def test_publish_and_rollback_feature_release(tmp_path):
    registry_path = tmp_path / "releases.json"
    gate = run_feature_release_gate(parity_report=ParityReport(pass_ok=True, mismatches=()), metrics=FeatureMetrics(), target="live")
    published = publish_feature_release(registry_path=registry_path, feature_set_name="default", version="1.0.0", gate_report=gate)
    assert published.released.active_version == "1.0.0"
    published = publish_feature_release(registry_path=registry_path, feature_set_name="default", version="1.1.0", gate_report=gate)
    rolled = rollback_feature_release(registry_path=registry_path, feature_set_name="default")
    assert rolled.released.active_version == "1.0.0"

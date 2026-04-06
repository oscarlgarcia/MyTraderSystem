import json

from app.features.metrics import FeatureMetrics
from app.features.observability import export_feature_observability_bundle


def test_observability_bundle_exports_metrics_and_alerts(tmp_path):
    path = export_feature_observability_bundle(
        metrics=FeatureMetrics(stale_serves=1, invalid_serves=1, parity_mismatches=2),
        target="paper",
        output_path=tmp_path / "bundle.json",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["metrics"]["stale_serves"] == 1
    assert "stale_features_detected" in payload["alerts"]

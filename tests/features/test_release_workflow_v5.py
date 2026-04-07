import json
from pathlib import Path

from app.features.live_readiness import FeatureLiveReadinessInputs, evaluate_feature_live_readiness
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_workflow import gate_and_publish_feature_release, rollback_feature_release


def test_release_workflow_persists_audit_events(tmp_path: Path):
    registry_path = tmp_path / "releases.json"
    live_readiness = evaluate_feature_live_readiness(
        inputs=FeatureLiveReadinessInputs(
            online_backend="http",
            observability_sink="http",
            serving_soak_pass_ok=True,
            rollout_audit_enabled=True,
            contract_validation_pass_ok=True,
            benchmark_pass_ok=True,
        )
    )
    result = gate_and_publish_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        version="1.0.0",
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=FeatureMetrics(),
        target="live",
        actor="pytest",
        live_readiness=live_readiness,
    )
    result = gate_and_publish_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        version="1.1.0",
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=FeatureMetrics(),
        target="live",
        actor="pytest",
        live_readiness=live_readiness,
    )
    assert result.audit_path is not None and result.audit_path.exists()

    rollback = rollback_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        target="paper",
        actor="pytest",
    )
    assert rollback.audit_path is not None and rollback.audit_path.exists()

    lines = [json.loads(line) for line in result.audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [entry["action"] for entry in lines] == ["publish", "publish", "rollback"]
    assert lines[1]["gate_report"]["pass_ok"] is True
    assert lines[1]["live_readiness"]["pass_ok"] is True

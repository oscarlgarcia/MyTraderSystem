from pathlib import Path
from datetime import datetime, timezone

import pytest

from app.features.live_readiness import FeatureLiveReadinessInputs, evaluate_feature_live_readiness
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityMismatch, ParityReport
from app.features.release_workflow import gate_and_publish_feature_release, rollback_feature_release
from app.features.releases import FeatureReleaseRegistry


def test_gate_and_publish_blocks_when_gate_fails(tmp_path: Path):
    registry_path = tmp_path / "releases.json"
    with pytest.raises(ValueError, match="release gate failed"):
        gate_and_publish_feature_release(
            registry_path=registry_path,
            feature_set_name="default",
            version="1.0.0",
            parity_report=ParityReport(
                pass_ok=False,
                mismatches=(
                    ParityMismatch(
                        symbol="BTCUSDT",
                        ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
                        feature_name="price",
                        offline_value=100.0,
                        online_value=101.0,
                        reason="persisted_batch_diverged",
                    ),
                ),
            ),
            metrics=FeatureMetrics(),
            target="paper",
        )


def test_gate_and_publish_and_rollback_govern_active_version(tmp_path: Path):
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
    published = gate_and_publish_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        version="1.0.0",
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=FeatureMetrics(),
        target="live",
        live_readiness=live_readiness,
    )
    assert published.released.active_version == "1.0.0"

    gate_and_publish_feature_release(
        registry_path=registry_path,
        feature_set_name="default",
        version="1.1.0",
        parity_report=ParityReport(pass_ok=True, mismatches=()),
        metrics=FeatureMetrics(),
        target="live",
        live_readiness=live_readiness,
    )

    rolled = rollback_feature_release(registry_path=registry_path, feature_set_name="default")
    assert rolled.released.active_version == "1.0.0"

    state = FeatureReleaseRegistry(registry_path).get("default")
    assert state is not None
    assert state.active_version == "1.0.0"


def test_publish_live_release_requires_live_readiness(tmp_path: Path):
    registry_path = tmp_path / "releases.json"
    with pytest.raises(ValueError, match="live release requires live readiness decision"):
        gate_and_publish_feature_release(
            registry_path=registry_path,
            feature_set_name="default",
            version="1.0.0",
            parity_report=ParityReport(pass_ok=True, mismatches=()),
            metrics=FeatureMetrics(),
            target="live",
        )

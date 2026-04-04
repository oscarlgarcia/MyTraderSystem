from datetime import datetime, timedelta, timezone

from app.common.dto import FeatureVector
from app.features.definitions import FeatureDefinition, FeatureSetDefinition
from app.features.validators import FeatureValidator


def _feature_set(policy):
    return FeatureSetDefinition(
        name="default",
        version="1.0.0",
        description="baseline",
        feature_definitions=(
            FeatureDefinition(
                name="price",
                version="1.0.0",
                description="price",
                owner="test",
                validation_policy=policy,
            ),
        ),
    )


def _vector(value: float, offset: int = 0) -> FeatureVector:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=offset)
    return FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": value},
        feature_set_name="default",
        feature_set_version="1.0.0",
    )


def test_validator_flags_sparse_runs():
    validator = FeatureValidator(_feature_set({"sparsity_window": 4, "sparsity_epsilon": 0.01, "max_sparse_ratio": 0.74}))
    validator.validate(_vector(0.0, 0))
    validator.validate(_vector(0.0, 1))
    validator.validate(_vector(0.005, 2))
    result = validator.validate(_vector(1.0, 3))
    assert "price:sparse_run" in result.flags


def test_validator_flags_drift_shift():
    validator = FeatureValidator(_feature_set({"drift_baseline_window": 3, "drift_recent_window": 2, "max_mean_shift": 5.0}))
    validator.validate(_vector(100.0, 0))
    validator.validate(_vector(101.0, 1))
    validator.validate(_vector(99.0, 2))
    validator.validate(_vector(115.0, 3))
    result = validator.validate(_vector(118.0, 4))
    assert "price:drift_shift" in result.flags

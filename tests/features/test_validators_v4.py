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


def test_validator_flags_constant_runs():
    validator = FeatureValidator(_feature_set({"constant_window": 3}))
    validator.validate(_vector(100.0, 0))
    validator.validate(_vector(100.0, 1))
    result = validator.validate(_vector(100.0, 2))
    assert "price:constant_run" in result.flags


def test_validator_flags_low_variance():
    validator = FeatureValidator(_feature_set({"variance_window": 3, "min_variance": 0.5}))
    validator.validate(_vector(100.0, 0))
    validator.validate(_vector(100.1, 1))
    result = validator.validate(_vector(100.0, 2))
    assert "price:low_variance" in result.flags


def test_validator_flags_large_step_changes():
    validator = FeatureValidator(_feature_set({"max_abs_delta": 5.0}))
    validator.validate(_vector(100.0, 0))
    result = validator.validate(_vector(120.0, 1))
    assert "price:delta_exceeded" in result.flags

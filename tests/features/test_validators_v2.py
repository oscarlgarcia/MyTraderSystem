from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.definitions import FeatureDefinition, FeatureSetDefinition
from app.features.validators import FeatureValidator


def test_validator_flags_out_of_range_values():
    feature_set = FeatureSetDefinition(
        name="default",
        version="1.0.0",
        description="baseline",
        feature_definitions=(FeatureDefinition(name="price", version="1.0.0", description="p", owner="me", validation_policy={"min": 0, "max": 10}),),
    )
    fv = FeatureVector(symbol="BTCUSDT", ts=datetime.fromtimestamp(1700000000, tz=timezone.utc), values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0")
    result = FeatureValidator(feature_set).validate(fv)
    assert not result.is_valid
    assert "price:above_max" in result.flags

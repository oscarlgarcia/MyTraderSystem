from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.views import build_basic_strategy_view


def test_basic_strategy_view_maps_expected_fields():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 101.0, "ret_1": 0.1, "sma_3": 100.0})
    view = build_basic_strategy_view(vector)
    assert view.price == 101.0
    assert view.ret_1 == 0.1
    assert view.sma_3 == 100.0

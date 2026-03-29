from datetime import datetime, timezone, timedelta

from app.features.cache import FeatureCache
from app.common.dto import FeatureVector


def _fv(ts_offset: int, symbol="BTCUSDT"):
    return FeatureVector(
        symbol=symbol,
        ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        values={"price": 100 + ts_offset},
    )


def test_eviction_by_capacity():
    cache = FeatureCache(capacity_per_symbol=2)
    cache.put(_fv(0))
    cache.put(_fv(1))
    cache.put(_fv(2))  # debería expulsar ts=0
    latest = cache.get_latest("BTCUSDT")
    assert latest.ts.timestamp() == 1700000002
    assert len(cache.data["BTCUSDT"]) == 2
    assert 1700000000 not in cache.data["BTCUSDT"]


def test_get_at_with_tolerance():
    cache = FeatureCache(capacity_per_symbol=3)
    cache.put(_fv(0))
    cache.put(_fv(60))
    ts_query = datetime.fromtimestamp(1700000000 + 70, tz=timezone.utc)
    fv = cache.get_at("BTCUSDT", ts_query, tolerance=15)
    assert fv is not None
    assert fv.ts.timestamp() == 1700000000 + 60
    # sin tolerancia debería fallar
    assert cache.get_at("BTCUSDT", ts_query, tolerance=5) is None

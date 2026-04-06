from datetime import datetime, timedelta, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore


def _ev(offset: int, *, available_delay_seconds: int) -> MarketEvent:
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=ts,
        price=100.0 + offset,
        size=1.0,
        source="trade",
        available_ts=ts + timedelta(seconds=available_delay_seconds),
    )


def test_materialization_uses_target_aware_validation_profile(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    feature_set = build_legacy_feature_set_definition(
        name="default",
        version="1.0.0",
        description="baseline",
        windows=[2],
        aggregators=["sma"],
        transformers=[],
    )
    materializer = FeatureMaterializer()
    research_vectors = materializer.materialize(
        [_ev(0, available_delay_seconds=60), _ev(60, available_delay_seconds=60)],
        feature_set=feature_set,
        store=store,
        run_id="research-run",
        target="research",
    )
    live_vectors = materializer.materialize(
        [_ev(0, available_delay_seconds=60), _ev(60, available_delay_seconds=60)],
        feature_set=feature_set,
        store=store,
        run_id="live-run",
        target="live",
    )
    assert research_vectors
    assert live_vectors
    assert all("feature_staleness_profile_breach" not in vector.quality_flags for vector in research_vectors)
    assert any("feature_staleness_profile_breach" in vector.quality_flags for vector in live_vectors)

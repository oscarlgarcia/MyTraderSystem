from datetime import datetime, timezone

from app.common.dto import FeatureVector, MarketEvent
from app.features.definitions import FeatureDefinition, FeatureNodeDefinition, FeatureSetDefinition
from app.features.parity import run_parity_check


def _ev(offset: int, price: float, *, account: str) -> MarketEvent:
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=ts,
        price=price,
        size=1.0,
        source="trade",
        available_ts=ts,
        metadata={"account": account},
    )


def _feature_set() -> FeatureSetDefinition:
    return FeatureSetDefinition(
        name="multi-parity",
        version="1.0.0",
        description="multi entity parity",
        entity_keys=("symbol", "account"),
        feature_definitions=(
            FeatureDefinition(name="price", version="1.0.0", description="price", owner="test", entity_keys=("symbol", "account")),
        ),
        node_definitions=(FeatureNodeDefinition(name="price", kind="price", outputs=("price",)),),
    )


def test_parity_matches_by_entity_scope_for_composite_entities(tmp_path):
    report = run_parity_check(
        [
            _ev(0, 100.0, account="paper"),
            _ev(0, 200.0, account="live"),
        ],
        feature_set=_feature_set(),
        offline_store_path=tmp_path / "offline.sqlite",
        online_store_path=tmp_path / "online.sqlite",
    )
    assert report.pass_ok


def test_parity_reports_batch_mismatch_for_specific_entity_scope(monkeypatch, tmp_path):
    feature_set = _feature_set()

    def fake_execute(self, events, *, feature_set):
        ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
        return [
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 999.0},
                feature_set_name=feature_set.name,
                feature_set_version=feature_set.version,
                entity_keys={"symbol": "BTCUSDT", "account": "paper"},
            ),
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 200.0},
                feature_set_name=feature_set.name,
                feature_set_version=feature_set.version,
                entity_keys={"symbol": "BTCUSDT", "account": "live"},
            ),
        ]

    monkeypatch.setattr("app.features.parity.BatchFeatureExecutor.execute", fake_execute)
    report = run_parity_check(
        [
            _ev(0, 100.0, account="paper"),
            _ev(0, 200.0, account="live"),
        ],
        feature_set=feature_set,
        offline_store_path=tmp_path / "offline.sqlite",
        online_store_path=tmp_path / "online.sqlite",
    )
    assert report.pass_ok is False
    assert len(report.mismatches) == 1
    mismatch = report.mismatches[0]
    assert mismatch.feature_name == "price"
    assert '"account":"paper"' in mismatch.entity_scope


import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.common.dto import FeatureVector
from app.features.offline_store import MaterializationRunRecord, OfflineFeatureStore
from app.features.query import FeatureQueryService


def _fv(i: int) -> FeatureVector:
    return FeatureVector(
        symbol="BTCUSDT",
        ts=datetime.fromtimestamp(1700000000 + i, tz=timezone.utc),
        values={"price": 100 + i},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id=f"bundle-{i}",
    )


def test_offline_store_round_trip_and_run_query(tmp_path: Path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    features_in = [_fv(i) for i in range(5)]
    record = MaterializationRunRecord(
        run_id="run-1",
        feature_set_name="default",
        feature_set_version="1.0.0",
        definition_hash="abc",
        input_fingerprint="fingerprint",
        bundle_id="bundle-1",
        row_count=len(features_in),
        status="completed",
        created_at=datetime.now(timezone.utc),
        min_event_ts=features_in[0].ts,
        max_event_ts=features_in[-1].ts,
    )
    store.register_materialization_run(record)
    store.put_many(features_in, run_id="run-1")

    query = FeatureQueryService(offline_store=store)
    loaded = query.reconstruct_run(run_id="run-1")
    assert len(loaded) == len(features_in)
    assert query.get_run("run-1") is not None
    assert loaded[0].values["price"] == 100


def test_get_materialization_run_missing_returns_none(tmp_path: Path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    assert store.get_materialization_run("missing") is None


def test_invalid_feature_payload_raises(tmp_path: Path):
    path = tmp_path / "offline.sqlite"
    store = OfflineFeatureStore(path)
    with store._connect() as conn:  # intentional low-level corruption check
        conn.execute(
            "INSERT OR REPLACE INTO feature_vectors (symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version, lineage_id, run_id, quality_flags, values_json, entity_keys_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BTCUSDT", "not-a-ts", "not-a-ts", "not-a-ts", "default", "1.0.0", "bad", "bad-run", json.dumps([]), json.dumps({"price": 1}), json.dumps({"symbol": "BTCUSDT"})),
        )
        conn.commit()
    with pytest.raises(ValueError):
        store.reconstruct_run(run_id="bad-run")

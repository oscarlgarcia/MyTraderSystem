import json

import pytest

from app.features.state import RuntimeStateStore
from app.features.state_snapshot import StateSnapshotStore


def test_snapshot_store_rejects_unknown_schema(tmp_path):
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"schema_version": "v99", "effective_window": 2}), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported runtime snapshot schema_version"):
        StateSnapshotStore(path).load()


def test_snapshot_store_roundtrip_v2(tmp_path):
    path = tmp_path / "snapshot.json"
    store = StateSnapshotStore(path)
    state = RuntimeStateStore(effective_window=2)
    store.save(state)
    loaded = store.load()
    assert loaded is not None

from datetime import datetime, timezone
import json
import tempfile
from pathlib import Path

import pytest

from app.features.storage import save, load, StorageError, STORAGE_VERSION
from app.common.dto import FeatureVector


def _fv(i: int) -> FeatureVector:
    return FeatureVector(
        symbol="BTCUSDT",
        ts=datetime.fromtimestamp(1700000000 + i, tz=timezone.utc),
        values={"price": 100 + i},
    )


def test_round_trip_json(tmp_path: Path):
    path = tmp_path / "features.json"
    features_in = [_fv(i) for i in range(5)]
    save(features_in, path, feature_set=("default", "1.0.0"))
    features_out, feature_set = load(path)
    assert len(features_out) == len(features_in)
    assert feature_set == ("default", "1.0.0")
    assert features_out[0].values["price"] == 100


def test_missing_version_raises(tmp_path: Path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"features": []}), encoding="utf-8")
    with pytest.raises(StorageError):
        load(path)


def test_version_mismatch(tmp_path: Path):
    path = tmp_path / "bad_ver.json"
    path.write_text(json.dumps({"storage_version": "0.9", "features": []}), encoding="utf-8")
    with pytest.raises(StorageError):
        load(path)


def test_unsupported_extension():
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with pytest.raises(StorageError):
            save([], tmp.name)

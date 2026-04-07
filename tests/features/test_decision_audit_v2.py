from datetime import datetime, timezone

from app.common.dto import FeatureVector, Signal
from app.features.audit import build_decision_audit_record, persist_decision_audits


def test_decision_audit_record_and_persist(tmp_path):
    ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    fv = FeatureVector(symbol="BTCUSDT", ts=ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-1")
    sig = Signal(symbol="BTCUSDT", ts=ts, side="buy", size=1.0)
    rec = build_decision_audit_record(
        fv,
        sig,
        consumer_metadata={
            "dataset_id": "dataset-2024-01",
            "feature_schema_hash": "schema-v2",
            "training_bundle_id": "train-bundle-1",
            "consumer_name": "paper-strategy",
            "consumer_kind": "strategy",
        },
    )
    path = tmp_path / "audit.jsonl"
    persist_decision_audits([rec], path)
    text = path.read_text(encoding="utf-8")
    assert "bundle-1" in text
    assert "train-bundle-1" in text
    assert "schema-v2" in text

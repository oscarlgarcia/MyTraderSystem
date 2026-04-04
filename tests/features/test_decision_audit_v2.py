from datetime import datetime, timezone

from app.common.dto import FeatureVector, Signal
from app.features.audit import build_decision_audit_record, persist_decision_audits


def test_decision_audit_record_and_persist(tmp_path):
    ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    fv = FeatureVector(symbol="BTCUSDT", ts=ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-1")
    sig = Signal(symbol="BTCUSDT", ts=ts, side="buy", size=1.0)
    rec = build_decision_audit_record(fv, sig)
    path = tmp_path / "audit.jsonl"
    persist_decision_audits([rec], path)
    assert "bundle-1" in path.read_text(encoding="utf-8")

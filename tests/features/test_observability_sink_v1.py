import json

from app.features.metrics import FeatureMetrics
from app.features.observability import HttpObservabilitySink, JsonlObservabilitySink, MemoryObservabilitySink, emit_feature_observability_bundle
from tests.features.http_test_support import feature_http_server


def test_observability_can_emit_to_memory_sink():
    sink = MemoryObservabilitySink()
    emitted = emit_feature_observability_bundle(metrics=FeatureMetrics(stale_serves=1), target="paper", sink=sink)
    assert emitted is sink.bundles[0]
    assert sink.bundles[0].alerts == ("stale_features_detected",)


def test_observability_can_emit_to_jsonl_sink(tmp_path):
    sink = JsonlObservabilitySink(tmp_path / "observability.jsonl")
    path = emit_feature_observability_bundle(metrics=FeatureMetrics(invalid_serves=1), target="paper", sink=sink)
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["alerts"] == ["invalid_features_detected"]


def test_observability_can_emit_to_http_sink():
    with feature_http_server() as (server, handler):
        sink = HttpObservabilitySink(f"http://127.0.0.1:{server.server_port}/observability")
        payload = emit_feature_observability_bundle(metrics=FeatureMetrics(parity_mismatches=1), target="paper", sink=sink)
        assert payload["alerts"] == ["feature_parity_mismatch_detected"]
        assert handler.observability_payloads[-1]["alerts"] == ["feature_parity_mismatch_detected"]

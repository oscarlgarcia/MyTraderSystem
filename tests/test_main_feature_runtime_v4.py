from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.common.dto import MarketEvent
from app.config import load_config
from app.features.training_bundle_registry import TrainingBundleRecord, TrainingBundleRegistry
from app.main import run, run_cycle
from app.observability.logger import get_logger


def _event():
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        available_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        price=100.0,
        size=1.0,
        source="trade",
    )


def test_run_cycle_wires_feature_audit_path(monkeypatch, tmp_path):
    cfg = load_config("dev")
    called = {}

    monkeypatch.setattr("app.main.run_ingestion_service", lambda **kwargs: [_event()])

    def fake_trading(events, **kwargs):
        called["feature_audit_path"] = kwargs["feature_audit_path"]
        called["mode"] = kwargs["mode"]
        return {"events": len(events), "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0}

    monkeypatch.setattr("app.main.run_trading_cycle", fake_trading)
    audit_path = tmp_path / "features-audit.jsonl"
    run_cycle(cfg=cfg, logger=get_logger(level="INFO"), mode="live", max_events=1, feature_audit_path=str(audit_path))

    assert called["feature_audit_path"] == str(audit_path)
    assert called["mode"] == "live"


def test_run_wires_feature_audit_path_from_args(monkeypatch, tmp_path):
    cfg = load_config("dev")
    registry_dir = tmp_path / "training-bundles"
    TrainingBundleRegistry(registry_dir).register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v1",
            feature_set_name="legacy",
            feature_set_version="legacy",
        )
    )
    args = SimpleNamespace(
        env="dev",
        release_gates=False,
        feature_release_action=None,
        feature_release_gates=False,
        mode="live",
        max_events=1,
        duration=None,
        feature_audit_path=str(tmp_path / "features-audit.jsonl"),
        feature_dataset_id="dataset-2024-01",
        feature_schema_hash="schema-v1",
        feature_training_bundle_id="train-bundle-1",
        feature_consumer_name="paper-strategy",
        feature_consumer_kind="strategy",
        feature_training_bundle_registry=str(registry_dir),
        feature_observability_output=None,
        fast_path=False,
        production_mode=False,
        trace_steps=False,
        ingest_max_buffer=10_000,
        ingest_dedup=True,
        ingest_batch_size=1,
        ingest_lag_warn=None,
        ingest_buffer_warn=None,
        ingest_backpressure_policy="pause",
        ingest_temporal_policy="accept",
        ingest_pipeline_version="v2",
        ingest_shadow_mode=False,
        ingest_shadow_block_on_diff=False,
        ingest_stream_types=("kline",),
        allow_live_fallback=False,
        error_policy=None,
    )
    called = {}

    monkeypatch.setattr("app.main.parse_args", lambda: args)
    monkeypatch.setattr("app.main.load_config", lambda env=None: cfg)
    monkeypatch.setattr("app.main._validate_operational_security", lambda *args, **kwargs: None)

    def fake_run_cycle(**kwargs):
        called["feature_audit_path"] = kwargs["feature_audit_path"]
        return {"events": 1, "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0}

    monkeypatch.setattr("app.main.run_cycle", fake_run_cycle)

    assert run() == 0
    assert called["feature_audit_path"] == args.feature_audit_path


def test_run_cycle_live_without_audit_path_fails(monkeypatch):
    cfg = load_config("dev")
    monkeypatch.setattr("app.main.run_ingestion_service", lambda **kwargs: [_event()])

    with pytest.raises(ValueError, match="feature_audit_path"):
        run_cycle(cfg=cfg, logger=get_logger(level="INFO"), mode="live", max_events=1)

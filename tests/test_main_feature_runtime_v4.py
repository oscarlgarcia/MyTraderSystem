from datetime import datetime, timezone
from dataclasses import replace
from types import SimpleNamespace

import pytest

from app import main as main_module
from app.common.dto import FeatureVector, MarketEvent
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


def test_operational_feature_serving_roundtrips_vectors(tmp_path):
    cfg = replace(
        load_config("dev"),
        feature_online_store_path=tmp_path / "online.sqlite",
        feature_offline_store_path=tmp_path / "offline.sqlite",
        feature_training_bundle_registry_dir=tmp_path / "training-bundles",
    )
    fv = FeatureVector(
        symbol="BTCUSDT",
        ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        available_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        values={"price": 100.0, "ret_1": 0.01, "sma_3": 99.0},
        feature_set_name="legacy",
        feature_set_version="legacy",
        lineage_id="bundle-1",
    )
    served, metrics = main_module._serve_operational_feature_vectors(
        cfg=cfg,
        feature_vectors=[fv],
        feature_consumer_metadata={
            "dataset_id": "dataset-2024-01",
            "feature_schema_hash": "schema-v1",
            "training_bundle_id": "train-bundle-1",
            "consumer_name": "paper-strategy",
            "consumer_kind": "strategy",
            "target": "paper",
        },
        training_bundle_registry_path=str(cfg.feature_training_bundle_registry_dir),
        mode="paper",
    )
    assert len(served) == 1
    assert served[0].values["price"] == 100.0
    assert metrics.serving_requests == 1


def test_run_trading_cycle_paper_uses_operational_serving_and_writes_artifacts(monkeypatch, tmp_path):
    cfg = replace(
        load_config("dev"),
        feature_online_store_path=tmp_path / "online.sqlite",
        feature_offline_store_path=tmp_path / "offline.sqlite",
        feature_training_bundle_registry_dir=tmp_path / "training-bundles",
    )
    event = _event()
    fv = FeatureVector(
        symbol="BTCUSDT",
        ts=event.event_ts,
        available_ts=event.available_ts,
        values={"price": 100.0, "ret_1": 0.01, "sma_3": 99.0},
        feature_set_name="legacy",
        feature_set_version="legacy",
        lineage_id="bundle-1",
    )
    monkeypatch.setattr("app.main.run_feature_pipeline", lambda *args, **kwargs: [fv])
    result = main_module.run_trading_cycle(
        [event],
        cfg=cfg,
        logger=get_logger(level="INFO"),
        mode="paper",
        feature_audit_path=str(tmp_path / "feature-audit.jsonl"),
        feature_observability_output=str(tmp_path / "feature-observability.json"),
        feature_training_bundle_registry_path=str(cfg.feature_training_bundle_registry_dir),
        feature_consumer_metadata={
            "dataset_id": "dataset-2024-01",
            "feature_schema_hash": "schema-v1",
            "training_bundle_id": "train-bundle-1",
            "consumer_name": "paper-strategy",
            "consumer_kind": "strategy",
            "target": "paper",
        },
    )
    assert result["features"] == 1
    assert (tmp_path / "feature-audit.jsonl").exists()
    assert (tmp_path / "feature-observability.json").exists()

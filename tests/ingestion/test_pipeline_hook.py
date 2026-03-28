from unittest import mock
from app.ingestion import pipeline
from app.common.dto import MarketEvent
from datetime import datetime, timezone


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_compute_features_after_flag_off(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100)]
    monkeypatch.setattr(pipeline, "_synthetic_events", lambda n: events)
    feats_mock = mock.Mock(return_value=[])
    monkeypatch.setattr(pipeline, "run_feature_pipeline", feats_mock)

    out = pipeline.collect_events(mode="dry", cfg=cfg, max_events=1, logger=mock.Mock(), compute_features_after=False)
    assert out == events
    feats_mock.assert_not_called()


def test_compute_features_after_flag_on(monkeypatch, caplog):
    caplog.set_level("INFO")
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]
    monkeypatch.setattr(pipeline, "_synthetic_events", lambda n: events)
    feats_mock = mock.Mock(return_value=[1, 2])
    monkeypatch.setattr(pipeline, "run_feature_pipeline", feats_mock)

    out = pipeline.collect_events(
        mode="dry",
        cfg=cfg,
        max_events=2,
        logger=mock.Mock(),
        compute_features_after=True,
        max_buffer=5,
        dedup_enabled=True,
    )
    assert out == events
    feats_mock.assert_called_once()

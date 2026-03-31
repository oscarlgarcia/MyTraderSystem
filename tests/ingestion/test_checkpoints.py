import io
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from app.common.dto import MarketEvent
from app.ingestion.checkpoints import CheckpointState, CheckpointStore
from app.ingestion.pipeline import collect_events
from app.ingestion.sources import StaticSource
from app.observability.logger import get_logger


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


class DummySink:
    def __init__(self):
        self.items = []

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
            return
        self.items.append(batch)

    def close(self):
        return None


def _cfg(tmp_path: Path):
    return mock.Mock(
        env="dev",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )


def test_checkpoint_roundtrip_restores_last_state(tmp_path):
    store = CheckpointStore(tmp_path / "state" / "checkpoint.json")
    state = CheckpointState(
        last_event_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        seen_keys=(("BTCUSDT", datetime(2024, 1, 1, tzinfo=timezone.utc), 100.0, 1.0, "trade"),),
        metadata={"mode": "live", "events_out": 1},
    )

    store.save(state)
    loaded = store.load()

    assert loaded == state


def test_restart_does_not_reprocess_recent_duplicates(tmp_path):
    cfg = _cfg(tmp_path)
    store = CheckpointStore(tmp_path / "state" / "checkpoint.json")
    events = [_ev(0, 100.0), _ev(60, 101.0)]

    first = collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        dedup_enabled=True,
        snapshot_enabled=False,
        source=StaticSource(events=events),
        sink=DummySink(),
        checkpoint_store=store,
    )
    second = collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        dedup_enabled=True,
        snapshot_enabled=False,
        source=StaticSource(events=events),
        sink=DummySink(),
        checkpoint_store=store,
    )

    assert first == events
    assert second == []


def test_corrupt_checkpoint_triggers_safe_recovery(tmp_path):
    cfg = _cfg(tmp_path)
    store = CheckpointStore(tmp_path / "state" / "checkpoint.json")
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text("{not-json", encoding="utf-8")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.checkpoint.recovery", level="INFO", stream=buffer)
    events = [_ev(0, 100.0)]

    out = collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=logger,
        dedup_enabled=True,
        snapshot_enabled=False,
        source=StaticSource(events=events),
        sink=DummySink(),
        checkpoint_store=store,
    )

    assert out == events
    records = _json_lines(buffer)
    warning = next(record for record in records if record["message"] == "checkpoint recovery using empty state")
    assert str(store.path) in warning["checkpoint_path"]
    assert "Corrupt checkpoint file" in warning["error"]
    reloaded = store.load()
    assert reloaded is not None
    assert reloaded.last_event_ts == events[0].event_ts

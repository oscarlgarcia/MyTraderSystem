from __future__ import annotations

import io
from dataclasses import replace

from app.config import load_config
from app.ingestion.pipeline import collect_events
from app.observability.logger import get_logger


def test_control_plane_telemetry_failure_does_not_break_ingestion(tmp_path):
    cfg = replace(
        load_config("dev"),
        data_dir=tmp_path,
        control_plane_telemetry_dir=tmp_path / "occupied-path",
    )
    cfg.control_plane_telemetry_dir.write_text("not-a-directory", encoding="utf-8")
    logger = get_logger(name="test.ingestion.controlplane", level="INFO", stream=io.StringIO())

    events = collect_events(
        mode="dry",
        cfg=cfg,
        max_events=5,
        duration_s=0,
        logger=logger,
        summary_logging=True,
        snapshot_enabled=False,
    )

    assert len(events) == 5

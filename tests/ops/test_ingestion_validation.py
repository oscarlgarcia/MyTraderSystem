from datetime import datetime, timezone
import subprocess
from pathlib import Path
import sys

from app.ingestion.backfill import normalize_kline_row
from app.ops.ingestion_validation import run_canary_validation, run_soak_validation, run_ws_live_canary


def _kline_rows(count: int) -> list[list[object]]:
    base_open = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    rows: list[list[object]] = []
    for index in range(count):
        open_ms = base_open + index * 60_000
        close_ms = open_ms + 59_999
        rows.append(
            [
                open_ms,
                f"{100 + index:.2f}",
                f"{101 + index:.2f}",
                f"{99 + index:.2f}",
                f"{100.5 + index:.2f}",
                "1.0",
                close_ms,
                f"{1000 + index:.2f}",
            ]
        )
    return rows


def _bar_events(count: int):
    receive_ts = datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc)
    return [
        normalize_kline_row(
            "BTCUSDT",
            row,
            interval="1m",
            receive_ts=receive_ts,
            process_ts=receive_ts,
            venue="BINANCE",
        )
        for row in _kline_rows(count)
    ]


def test_soak_validation_writes_evidence_and_passes(tmp_path: Path):
    output = tmp_path / "soak.json"
    evidence = run_soak_validation(output, iterations=2, events_per_iteration=20, pipeline_version="v2")

    assert evidence.pass_ok is True
    assert evidence.total_events_persisted == 40
    assert output.exists()


def test_canary_validation_refreshes_vendor_baseline_and_writes_report(tmp_path: Path):
    output = tmp_path / "canary.json"
    baseline_path = tmp_path / "baseline.json"

    def fake_fetch_rows(**kwargs):
        assert kwargs["symbol"] == "BTCUSDT"
        assert kwargs["interval"] == "1m"
        assert kwargs["bars"] == 5
        return _kline_rows(5)

    evidence = run_canary_validation(
        output,
        baseline_version="v1",
        candidate_version="v2",
        bars=5,
        baseline_path=baseline_path,
        refresh_baseline=True,
        fetch_rows=fake_fetch_rows,
    )

    assert evidence.pass_ok is True
    assert evidence.baseline_source == "vendor_refresh"
    assert evidence.diffs["row_count"] == 0
    assert evidence.diffs["baseline_matches_vendor"] is True
    assert evidence.diffs["candidate_matches_vendor"] is True
    assert evidence.diffs["projection_checksum_match"] is True
    assert output.exists()
    assert baseline_path.exists()


def test_canary_validation_reuses_persisted_baseline_without_network(tmp_path: Path):
    output = tmp_path / "canary.json"
    baseline_path = tmp_path / "baseline.json"

    run_canary_validation(
        output,
        baseline_version="v1",
        candidate_version="v2",
        bars=4,
        baseline_path=baseline_path,
        refresh_baseline=True,
        fetch_rows=lambda **_: _kline_rows(4),
    )

    def fail_fetch_rows(**kwargs):
        raise AssertionError(f"network fetch should not be used: {kwargs}")

    evidence = run_canary_validation(
        output,
        baseline_version="v1",
        candidate_version="v2",
        bars=4,
        baseline_path=baseline_path,
        refresh_baseline=False,
        fetch_rows=fail_fetch_rows,
    )

    assert evidence.pass_ok is True
    assert evidence.baseline_source == "persisted"
    assert evidence.bars == 4
    assert evidence.diffs["row_count"] == 0
    assert evidence.diffs["projection_checksum_match"] is True


def test_ws_live_canary_writes_report_and_records_reconnects(tmp_path: Path):
    output = tmp_path / "ws-canary.json"
    events = _bar_events(3)

    class FakeWSCanarySource:
        def __init__(self):
            self.calls = 0

        def stream(self, end_time=None):
            del end_time
            if self.calls == 0:
                self.calls += 1
                yield events[0]
                raise TimeoutError("induced reconnect")
            self.calls += 1
            for event in events[1:]:
                yield event

        def snapshot(self, request=None):
            del request
            return [events[0], events[1]]

    evidence = run_ws_live_canary(
        output,
        symbol="BTCUSDT",
        stream_type="kline",
        max_events=2,
        duration_seconds=5.0,
        reconnect_after_events=1,
        induced_reconnects=1,
        source_builder=lambda cfg: FakeWSCanarySource(),
    )

    assert evidence.pass_ok is True
    assert evidence.reconnects_observed >= 1
    assert evidence.continuity["events_persisted"] == 2
    assert evidence.continuity["gaps"] >= 1
    assert evidence.continuity["gap_irreparable"] == 0
    assert "reconnects" in evidence.continuity
    assert "duplicates" in evidence.continuity
    assert output.exists()


def test_ingestion_canary_script_help_exposes_ws_mode():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_canary.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--mode" in result.stdout
    assert "--stream-type" in result.stdout

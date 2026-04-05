from datetime import datetime, timezone
import subprocess
from pathlib import Path
import sys

import app.ops.ingestion_validation as ingestion_validation
from app.ingestion.backfill import normalize_kline_row
from types import SimpleNamespace

from app.ops.ingestion_validation import (
    CRITICAL_FAILURE_INJECTION_TEST_IDS,
    run_canary_validation,
    run_failure_injection_validation,
    run_soak_validation,
    run_vendor_contract_validation,
    run_ws_live_canary,
)


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
    evidence = run_soak_validation(output, target_profile="paper", iterations=2, events_per_iteration=20, pipeline_version="v2")

    assert evidence.pass_ok is True
    assert evidence.target_profile == "paper"
    assert evidence.total_events_persisted == 40
    assert evidence.generated_at
    assert evidence.max_allowed_gaps == 0
    assert evidence.max_gap_irreparable == 0
    assert evidence.max_gaps == 0
    assert evidence.max_allowed_duplicates == 0
    assert evidence.max_duplicates == 0
    assert evidence.max_allowed_heartbeat_missed_total == 0
    assert evidence.max_heartbeat_missed_total == 0
    assert evidence.max_allowed_gap_irreparable == 0
    assert evidence.max_allowed_compaction_failures == 0
    assert evidence.compaction_failures_total == 0
    assert evidence.reconnects_observed == 0
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


def test_ws_live_canary_writes_report_with_clean_runtime(tmp_path: Path, monkeypatch):
    output = tmp_path / "ws-canary.json"
    monkeypatch.setattr(ingestion_validation, "collect_events", lambda **_: None)
    monkeypatch.setattr(
        ingestion_validation,
        "_json_lines",
        lambda _buffer: [
            {
                "message": "ingestion summary",
                "mode": "live",
                "events_in": 2,
                "events_persisted": 2,
                "events_dedup_skipped": 0,
                "gaps_total": 0,
                "gap_irreparable_total": 0,
                "reconnects": 0,
                "processing_latency_seconds": 0.1,
                "write_latency_seconds": 0.1,
                "exchange_receive_skew_seconds": 0.1,
                "receive_process_skew_seconds": 0.1,
                "stream_metrics": [
                    {
                        "heartbeat_missed_total": 0,
                        "exchange_receive_skew_seconds": 0.1,
                        "receive_process_skew_seconds": 0.1,
                    }
                ],
            },
            {
                "message": "ingestion health",
                "streams_degraded": [],
                "result": "ok",
            },
        ],
    )
    monkeypatch.setattr(ingestion_validation, "_count_jsonl_records", lambda _path: 0)

    evidence = run_ws_live_canary(
        output,
        target_profile="paper",
        symbol="BTCUSDT",
        stream_type="kline",
        max_events=2,
        duration_seconds=5.0,
        reconnect_after_events=1,
        induced_reconnects=0,
        source_builder=lambda cfg: SimpleNamespace(stream=lambda end_time=None: iter(()), snapshot=lambda request=None: []),
    )

    assert evidence.pass_ok is True
    assert evidence.target_profile == "paper"
    assert evidence.reconnects_observed == 0
    assert evidence.continuity["events_persisted"] == 2
    assert evidence.continuity["gaps"] == 0
    assert evidence.continuity["duplicates"] == 0
    assert evidence.continuity["gap_irreparable"] == 0
    assert evidence.continuity["heartbeat_missed_total"] == 0
    assert "reconnects" in evidence.continuity
    assert "duplicates" in evidence.continuity
    assert output.exists()


def test_ws_live_canary_fails_when_runtime_is_degraded(tmp_path: Path):
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
        target_profile="live",
        symbol="BTCUSDT",
        stream_type="kline",
        max_events=2,
        duration_seconds=5.0,
        reconnect_after_events=1,
        induced_reconnects=1,
        source_builder=lambda cfg: FakeWSCanarySource(),
    )

    assert evidence.pass_ok is False
    assert "gaps_detected" in evidence.comparison_reason


def test_ws_live_soak_writes_report_with_clean_runtime(tmp_path: Path, monkeypatch):
    output = tmp_path / "soak.json"
    monkeypatch.setattr(ingestion_validation, "collect_events", lambda **_: None)
    monkeypatch.setattr(
        ingestion_validation,
        "_json_lines",
        lambda _buffer: [
            {
                "message": "ingestion summary",
                "mode": "live",
                "events_in": 2,
                "events_persisted": 2,
                "events_dedup_skipped": 0,
                "gaps_total": 0,
                "gap_irreparable_total": 0,
                "reconnects": 0,
                "processing_latency_seconds": 0.1,
                "write_latency_seconds": 0.1,
                "exchange_receive_skew_seconds": 0.1,
                "receive_process_skew_seconds": 0.1,
                "stream_metrics": [
                    {
                        "heartbeat_missed_total": 0,
                        "exchange_receive_skew_seconds": 0.1,
                        "receive_process_skew_seconds": 0.1,
                    }
                ],
            },
            {
                "message": "ingestion health",
                "streams_degraded": [],
                "result": "ok",
            },
        ],
    )

    evidence = run_soak_validation(
        output,
        target_profile="paper",
        mode="ws-live",
        iterations=1,
        events_per_iteration=2,
        duration_seconds=5.0,
        pipeline_version="v2",
        symbol="BTCUSDT",
        stream_type="kline",
        interval="1m",
        reconnect_after_events=1,
        induced_reconnects=0,
        source_builder=lambda cfg: SimpleNamespace(stream=lambda end_time=None: iter(()), snapshot=lambda request=None: []),
    )

    assert evidence.pass_ok is True
    assert evidence.reconnects_observed == 0
    assert evidence.reconnects_target == 0
    assert evidence.max_allowed_gaps == 0
    assert evidence.max_duplicates == 0
    assert evidence.compaction_failures_total == 0
    assert evidence.max_gap_irreparable == 0
    assert output.exists()


def test_ws_live_soak_fails_when_runtime_is_degraded(tmp_path: Path):
    output = tmp_path / "soak.json"
    events = _bar_events(3)

    class FakeWSSoakSource:
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

    evidence = run_soak_validation(
        output,
        target_profile="live",
        mode="ws-live",
        iterations=1,
        events_per_iteration=2,
        duration_seconds=5.0,
        pipeline_version="v2",
        symbol="BTCUSDT",
        stream_type="kline",
        interval="1m",
        reconnect_after_events=1,
        induced_reconnects=1,
        source_builder=lambda cfg: FakeWSSoakSource(),
    )

    assert evidence.pass_ok is False
    assert evidence.max_gaps >= 1


def test_vendor_contract_validation_writes_artifact(tmp_path: Path):
    output = tmp_path / "vendor-contracts.json"

    def fake_runner(command, **kwargs):
        assert command[-3:] == ["-q", "-m", "network"]
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert kwargs["check"] is False
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    evidence = run_vendor_contract_validation(
        output,
        pytest_target="tests/network/test_binance_contracts.py",
        runner=fake_runner,
    )

    assert evidence.pass_ok is True
    assert evidence.returncode == 0
    assert evidence.pytest_target == "tests/network/test_binance_contracts.py"
    assert output.exists()


def test_failure_injection_validation_writes_artifact_for_critical_subset(tmp_path: Path):
    output = tmp_path / "failure-injection.json"

    def fake_runner(command, **kwargs):
        assert command[:3] == [sys.executable, "-m", "pytest"]
        assert command[3] == "-q"
        assert command[4:] == list(CRITICAL_FAILURE_INJECTION_TEST_IDS)
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert kwargs["check"] is False
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    evidence = run_failure_injection_validation(output, runner=fake_runner)

    assert evidence.pass_ok is True
    assert evidence.returncode == 0
    assert evidence.critical_test_ids == CRITICAL_FAILURE_INJECTION_TEST_IDS
    assert evidence.pytest_target == "tests/ops/test_failure_injection.py"
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


def test_ingestion_ws_canary_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_ws_canary.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--stream-type" in result.stdout
    assert "--induced-reconnects" in result.stdout


def test_ingestion_failure_injection_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_failure_injection.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--critical-test-ids" in result.stdout


def test_ingestion_soak_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_soak.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--mode" in result.stdout
    assert "--duration-seconds" in result.stdout

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from app.ops.ingestion_validation import run_storage_benchmark


def test_storage_benchmark_writes_artifact_and_measures_all_cases(tmp_path: Path):
    output = tmp_path / "storage-benchmark.json"

    evidence = run_storage_benchmark(
        output,
        symbol_count=4,
        bursts=2,
        events_per_symbol_per_burst=4,
        min_rows_per_second=0.001,
        max_compaction_elapsed_slo=60.0,
        max_shadow_elapsed_slo=60.0,
    )

    assert evidence.pass_ok is True
    assert evidence.synthetic_case.rows_in > 0
    assert evidence.replay_case.rows_in > 0
    assert evidence.concurrent_compaction_case.compaction_elapsed_seconds >= 0.0
    assert evidence.shadow_scoped_case.shadow_elapsed_seconds >= 0.0
    assert output.exists()

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["synthetic_case"]["dataset_kind"] == "synthetic"
    assert payload["replay_case"]["dataset_kind"] == "replay_raw"
    assert "min_rows_per_second" in payload["slo"]


def test_storage_benchmark_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_storage_benchmark.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--symbol-count" in result.stdout
    assert "--min-rows-per-second" in result.stdout

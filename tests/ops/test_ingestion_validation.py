from pathlib import Path

from app.ops.ingestion_validation import run_canary_validation, run_soak_validation


def test_soak_validation_writes_evidence_and_passes(tmp_path: Path):
    output = tmp_path / "soak.json"
    evidence = run_soak_validation(output, iterations=2, events_per_iteration=20, pipeline_version="v2")

    assert evidence.pass_ok is True
    assert evidence.total_events_persisted == 40
    assert output.exists()


def test_canary_validation_writes_report_and_matches_counts(tmp_path: Path):
    output = tmp_path / "canary.json"
    evidence = run_canary_validation(output, baseline_version="v1", candidate_version="v2", event_count=20)

    assert evidence.pass_ok is True
    assert evidence.diffs["events_persisted"] == 0.0
    assert evidence.diffs["duplicates"] == 0.0
    assert evidence.diffs["gaps"] == 0.0
    assert output.exists()

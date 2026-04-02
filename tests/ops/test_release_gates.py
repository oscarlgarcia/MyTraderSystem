import json
from pathlib import Path

from app.ops.release_gates import render_release_gate_summary, run_release_gates


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_shadow_comparison(path: Path, *, significant: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": "2026-04-02T12:00:00+00:00",
        "diffs": {"row_count": 0, "identity_count": 0},
        "significant": significant,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def test_release_gates_paper_passes_with_clean_canary_artifacts(tmp_path: Path):
    rest_path = tmp_path / "rest.json"
    ws_path = tmp_path / "ws.json"
    output_path = tmp_path / "release-gates.json"
    _write_json(
        rest_path,
        {
            "pass_ok": True,
            "diffs": {"row_count": 0},
            "comparison_reason": "semantic_match",
        },
    )
    _write_json(
        ws_path,
        {
            "pass_ok": True,
            "continuity": {
                "reconnects": 1,
                "duplicates": 0,
                "gaps": 0,
                "gap_irreparable": 0,
            },
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "continuity_ok",
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        output_path=output_path,
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    assert output_path.exists()
    exact_block = next(block for block in report.blocks if block.name == "exact_recovery")
    shadow_block = next(block for block in report.blocks if block.name == "shadow_diffs")
    assert exact_block.status == "pass"
    assert shadow_block.status == "warn"
    assert "Release gates: PASS (paper)" in render_release_gate_summary(report)


def test_release_gates_live_passes_with_exact_verified_and_clean_artifacts(tmp_path: Path):
    rest_path = tmp_path / "rest.json"
    ws_path = tmp_path / "ws.json"
    shadow_path = tmp_path / "shadow" / "env=dev" / "comparisons.jsonl"
    _write_json(
        rest_path,
        {
            "pass_ok": True,
            "diffs": {"row_count": 0},
            "comparison_reason": "semantic_match",
        },
    )
    _write_json(
        ws_path,
        {
            "pass_ok": True,
            "continuity": {
                "reconnects": 1,
                "duplicates": 0,
                "gaps": 0,
                "gap_irreparable": 0,
            },
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "continuity_ok",
        },
    )
    _write_shadow_comparison(shadow_path, significant=False)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    exact_block = next(block for block in report.blocks if block.name == "exact_recovery")
    shadow_block = next(block for block in report.blocks if block.name == "shadow_diffs")
    assert exact_block.status == "pass"
    assert shadow_block.status == "pass"

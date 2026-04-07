import json
from datetime import datetime, timezone

from app.features.shadow_summary import summarize_shadow_reports, write_shadow_summary


def test_shadow_summary_counts_failures_and_writes_json(tmp_path):
    path = tmp_path / "shadow.jsonl"
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
    path.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": ts, "pass_ok": True, "severity": "info"}),
                json.dumps({"timestamp": ts, "pass_ok": False, "severity": "medium"}),
                json.dumps({"timestamp": ts, "pass_ok": False, "severity": "critical"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    summary = summarize_shadow_reports(path, max_failed_reports=2, max_critical_failures=1)
    assert summary.total_reports == 3
    assert summary.failed_reports == 2
    assert summary.critical_failures == 1
    assert summary.pass_ok is True

    output = tmp_path / "shadow-summary.json"
    write_shadow_summary(output, summary)
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["failed_reports"] == 2


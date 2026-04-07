from __future__ import annotations

import json
import sys
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import run_feature_release_gate


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        raise SystemExit('usage: feature_release_gates.py <target> <output-path>')
    target = argv[1]
    output_path = Path(argv[2])
    report = run_feature_release_gate(parity_report=ParityReport(pass_ok=True, mismatches=()), metrics=FeatureMetrics(), target=target)
    output_path.write_text(json.dumps({
        'pass_ok': report.pass_ok,
        'target': report.target,
        'stale_count': report.stale_count,
        'latency_breaches': report.latency_breaches,
        'reasons': list(report.reasons),
    }, indent=2), encoding='utf-8')
    return 0 if report.pass_ok else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))

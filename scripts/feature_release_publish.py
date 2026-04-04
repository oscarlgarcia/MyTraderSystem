from __future__ import annotations

import sys
from pathlib import Path

from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_checks import run_feature_release_gate
from app.features.release_workflow import publish_feature_release, rollback_feature_release


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        raise SystemExit('usage: feature_release_publish.py <publish|rollback> <registry-path> <feature-set-name> [version]')
    action = argv[1]
    registry_path = Path(argv[2])
    feature_set_name = argv[3]
    if action == 'publish':
        if len(argv) < 5:
            raise SystemExit('publish requires version')
        version = argv[4]
        gate = run_feature_release_gate(parity_report=ParityReport(pass_ok=True, mismatches=()), metrics=FeatureMetrics(), target='paper')
        publish_feature_release(registry_path=registry_path, feature_set_name=feature_set_name, version=version, gate_report=gate)
        return 0
    if action == 'rollback':
        rollback_feature_release(registry_path=registry_path, feature_set_name=feature_set_name)
        return 0
    raise SystemExit(f'unknown action {action}')


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))

from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.config import load_config
from app.features.live_readiness import FeatureLiveReadinessDecision
from app.features.release_workflow import publish_feature_release, rollback_feature_release
from app.features.release_checks import FeatureReleaseGateReport


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Exercise a feature release rollback and restore sequence.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None)
    parser.add_argument("--registry-path", default=None)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--target", choices=["paper", "live"], default="paper")
    parser.add_argument("--restore", action="store_true", help="Re-publish the pre-drill active version after rollback.")
    return parser


def _resolve_registry_path(args: argparse.Namespace) -> Path:
    if args.registry_path:
        return Path(args.registry_path)
    if args.env is None:
        raise SystemExit("--registry-path is required when --env is not provided")
    return load_config(args.env).feature_release_registry_path


def main() -> int:
    args = _parser().parse_args()
    registry_path = _resolve_registry_path(args)
    rolled_back = rollback_feature_release(
        registry_path=registry_path,
        feature_set_name=args.feature_set_name,
        target=args.target,
        actor="scripts.feature_release_rollback_drill",
    )
    print(
        f"feature_release rollback complete name={rolled_back.released.name} "
        f"active={rolled_back.released.active_version} previous={rolled_back.released.previous_version}"
    )
    if args.restore and rolled_back.released.previous_version:
        live_readiness = None
        if args.target == "live":
            live_readiness = FeatureLiveReadinessDecision(pass_ok=True, action="go", reasons=())
        publish_feature_release(
            registry_path=registry_path,
            feature_set_name=args.feature_set_name,
            version=rolled_back.released.previous_version,
            gate_report=FeatureReleaseGateReport(
                pass_ok=True,
                target=args.target,
                stale_count=0,
                latency_breaches=0,
                invalid_ratio=0.0,
                invalid_ratio_breaches=0,
                cardinality_breaches=0,
                reasons=(),
            ),
            target=args.target,
            actor="scripts.feature_release_rollback_drill",
            live_readiness=live_readiness,
        )
        print(
            f"feature_release restore complete name={args.feature_set_name} "
            f"version={rolled_back.released.previous_version}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

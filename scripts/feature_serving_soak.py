from __future__ import annotations

import argparse
from datetime import datetime, timezone

from app.features.online_store_http import RemoteHttpOnlineFeatureStore
from app.features.operational_probes import run_serving_soak_probe, write_probe_report
from app.features.serving import FeatureServingService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a serving soak probe against the feature HTTP service")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--feature-set-version", required=True)
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--max-latency-seconds", type=float, default=1.0)
    parser.add_argument("--target", choices=["research", "paper", "live"], default="research")
    parser.add_argument("--output", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = RemoteHttpOnlineFeatureStore(args.base_url)
    service = FeatureServingService(online_store=store, target=args.target)
    decision_ts = datetime.now(timezone.utc)
    report = run_serving_soak_probe(
        request_fn=lambda: service.get_latest_servable(
            decision_ts=decision_ts,
            symbol=args.symbol,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
        ),
        iterations=args.iterations,
        max_latency_seconds=args.max_latency_seconds,
    )
    write_probe_report(args.output, report)
    print(f"feature_soak pass_ok={report.pass_ok} max_latency={report.max_latency_seconds:.6f}s")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

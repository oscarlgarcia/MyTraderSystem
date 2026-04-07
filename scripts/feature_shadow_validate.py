from __future__ import annotations

import argparse
from datetime import datetime, timezone

from app.features.online_store_http import RemoteHttpOnlineFeatureStore
from app.features.shadow import ShadowServingService
from app.features.shadow_report_store import ShadowReportStore
from app.features.shadow_summary import summarize_shadow_reports, write_shadow_summary
from app.features.serving import FeatureServingService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run feature shadow validation between two feature serving endpoints")
    parser.add_argument("--primary-url", required=True)
    parser.add_argument("--shadow-url", required=True)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--feature-set-version", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--requests", type=int, default=25)
    parser.add_argument("--report-path", required=True)
    parser.add_argument("--summary-path", required=True)
    parser.add_argument("--max-failed-reports", type=int, default=0)
    parser.add_argument("--max-critical-failures", type=int, default=0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    primary_store = RemoteHttpOnlineFeatureStore(args.primary_url)
    shadow_store = RemoteHttpOnlineFeatureStore(args.shadow_url)
    service = ShadowServingService(
        primary=FeatureServingService(online_store=primary_store, target="research"),
        shadow=FeatureServingService(online_store=shadow_store, target="research"),
        report_store=ShadowReportStore(args.report_path),
    )
    decision_ts = datetime.now(timezone.utc)
    for _ in range(args.requests):
        service.get_latest_servable(
            decision_ts=decision_ts,
            symbol=args.symbol,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
        )
    summary = summarize_shadow_reports(
        args.report_path,
        max_failed_reports=args.max_failed_reports,
        max_critical_failures=args.max_critical_failures,
    )
    write_shadow_summary(args.summary_path, summary)
    print(f"feature_shadow pass_ok={summary.pass_ok} failed={summary.failed_reports} critical={summary.critical_failures}")
    return 0 if summary.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

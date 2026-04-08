from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.features.online_store_factory import PRODUCTION_CANONICAL_ONLINE_BACKEND
from app.features.online_store_http import RemoteHttpOnlineFeatureStore
from app.features.operational_probes import run_serving_concurrency_probe, run_serving_soak_probe, write_probe_report
from app.features.serving import FeatureServingService
from app.features.shadow import ShadowServingService
from app.features.shadow_report_store import ShadowReportStore
from app.features.shadow_summary import summarize_shadow_reports, write_shadow_summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate feature release evidence artifacts.")
    parser.add_argument("--primary-url", required=True)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--feature-set-version", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target", choices=["paper", "live"], default="paper")
    parser.add_argument("--shadow-url", default=None)
    parser.add_argument("--shadow-requests", type=int, default=25)
    parser.add_argument("--soak-iterations", type=int, default=100)
    parser.add_argument("--concurrency-rounds", type=int, default=10)
    parser.add_argument("--concurrency-readers-per-round", type=int, default=12)
    parser.add_argument("--max-latency-seconds", type=float, default=1.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.target == "live" and not args.shadow_url:
        raise SystemExit("live feature evidence requires --shadow-url to generate fresh shadow validation")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    decision_ts = datetime.now(timezone.utc)

    primary_store = RemoteHttpOnlineFeatureStore(args.primary_url)
    primary_service = FeatureServingService(online_store=primary_store, target="research")

    soak_report = run_serving_soak_probe(
        request_fn=lambda: primary_service.get_latest_servable(
            decision_ts=decision_ts,
            symbol=args.symbol,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
        ),
        iterations=args.soak_iterations,
        max_latency_seconds=args.max_latency_seconds,
    )
    soak_path = write_probe_report(output_dir / "feature_serving_soak.json", soak_report)

    def _writer(round_id: int) -> None:
        # Reader-only concurrency is enough when the serving backend is fronted by a remote store.
        # Keep a no-op writer hook to preserve the probe interface and future extensibility.
        return None

    concurrency_report = run_serving_concurrency_probe(
        request_fn=lambda: primary_service.get_latest_servable(
            decision_ts=decision_ts,
            symbol=args.symbol,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
        ),
        writer_fn=_writer,
        rounds=args.concurrency_rounds,
        readers_per_round=args.concurrency_readers_per_round,
        max_latency_seconds=args.max_latency_seconds,
    )
    concurrency_path = write_probe_report(output_dir / "feature_serving_concurrency.json", concurrency_report)

    shadow_summary_path = None
    shadow_report_path = None
    if args.shadow_url:
        shadow_store = RemoteHttpOnlineFeatureStore(args.shadow_url)
        shadow_service = FeatureServingService(online_store=shadow_store, target="research")
        report_store = ShadowReportStore(output_dir / "feature_shadow_reports.jsonl")
        service = ShadowServingService(primary=primary_service, shadow=shadow_service, report_store=report_store)
        for _ in range(args.shadow_requests):
            service.get_latest_servable(
                decision_ts=decision_ts,
                symbol=args.symbol,
                feature_set_name=args.feature_set_name,
                feature_set_version=args.feature_set_version,
            )
        shadow_report_path = report_store.path
        shadow_summary = summarize_shadow_reports(shadow_report_path)
        shadow_summary_path = write_shadow_summary(output_dir / "feature_shadow_summary.json", shadow_summary)
        close_shadow = getattr(shadow_store, "close", None)
        if callable(close_shadow):
            close_shadow()

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": args.target,
        "feature_set_name": args.feature_set_name,
        "feature_set_version": args.feature_set_version,
        "symbol": args.symbol,
        "primary_backend": PRODUCTION_CANONICAL_ONLINE_BACKEND,
        "primary_url": args.primary_url,
        "shadow_backend": PRODUCTION_CANONICAL_ONLINE_BACKEND if args.shadow_url else None,
        "shadow_url": args.shadow_url,
        "artifacts": {
            "soak_path": str(soak_path),
            "concurrency_path": str(concurrency_path),
            "shadow_report_path": str(shadow_report_path) if shadow_report_path else None,
            "shadow_summary_path": str(shadow_summary_path) if shadow_summary_path else None,
        },
        "artifact_pass": {
            "soak": bool(soak_report.pass_ok),
            "concurrency": bool(concurrency_report.pass_ok),
            "shadow": (
                None
                if shadow_report_path is None
                else bool(summarize_shadow_reports(shadow_report_path).pass_ok)
            ),
        },
        "pass_ok": bool(
            soak_report.pass_ok
            and concurrency_report.pass_ok
            and (shadow_summary_path is None or summarize_shadow_reports(shadow_report_path).pass_ok)
        ),
    }
    manifest_path = output_dir / "feature_release_evidence_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    close_primary = getattr(primary_store, "close", None)
    if callable(close_primary):
        close_primary()

    print(f"feature_release_evidence pass_ok={manifest['pass_ok']} manifest={manifest_path}")
    return 0 if manifest["pass_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

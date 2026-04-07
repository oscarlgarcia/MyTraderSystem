from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from app.common.dto import FeatureVector
from app.features.online_store_http import RemoteHttpOnlineFeatureStore
from app.features.operational_probes import run_serving_concurrency_probe, write_probe_report
from app.features.serving import FeatureServingService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a serving concurrency probe against the feature HTTP service")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--feature-set-name", required=True)
    parser.add_argument("--feature-set-version", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--readers-per-round", type=int, default=12)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--max-latency-seconds", type=float, default=1.0)
    parser.add_argument("--target", choices=["research", "paper", "live"], default="research")
    parser.add_argument("--output", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = RemoteHttpOnlineFeatureStore(args.base_url)
    service = FeatureServingService(online_store=store, target=args.target)
    decision_ts = datetime.now(timezone.utc)

    def _writer(round_id: int) -> None:
        vector_ts = decision_ts + timedelta(seconds=round_id + 1)
        store.upsert(
            FeatureVector(
                symbol=args.symbol,
                ts=vector_ts,
                available_ts=vector_ts,
                values={"price": 100.0 + round_id},
                feature_set_name=args.feature_set_name,
                feature_set_version=args.feature_set_version,
                lineage_id=f"concurrency-{round_id}",
            )
        )

    report = run_serving_concurrency_probe(
        request_fn=lambda: service.get_latest_servable(
            decision_ts=decision_ts,
            symbol=args.symbol,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
        ),
        writer_fn=_writer,
        rounds=args.rounds,
        readers_per_round=args.readers_per_round,
        max_workers=args.max_workers,
        max_latency_seconds=args.max_latency_seconds,
    )
    write_probe_report(args.output, report)
    print(f"feature_concurrency pass_ok={report.pass_ok} max_latency={report.max_latency_seconds:.6f}s")
    return 0 if report.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

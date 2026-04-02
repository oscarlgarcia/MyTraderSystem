from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import load_config
from app.ingestion.compaction import CompactionJobPolicy, run_compaction_job


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compact normalized segmented storage partitions with explicit batch, retry and retention policy.",
    )
    parser.add_argument("--env", default="dev", help="Application environment to load.")
    parser.add_argument("--data-dir", default=None, help="Override data dir. Defaults to config data_dir.")
    parser.add_argument("--batch-limit", type=int, default=25, help="Maximum number of partitions to compact in one run.")
    parser.add_argument("--retry-attempts", type=int, default=2, help="Retries per partition after the first failure.")
    parser.add_argument("--min-segments-pending", type=int, default=2, help="Minimum pending segments required to select a partition.")
    parser.add_argument("--min-compaction-lag-seconds", type=float, default=300.0, help="Minimum lag required to select a partition.")
    parser.add_argument("--retain-compacted-segments", type=int, default=0, help="How many retained-segment runs to keep outside the active read path.")
    parser.add_argument("--dry-run", action="store_true", help="Report candidate partitions without compacting them.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cfg = load_config(args.env)
    base_dir = Path(args.data_dir) if args.data_dir else Path(cfg.data_dir)
    report = run_compaction_job(
        base_dir,
        args.env,
        policy=CompactionJobPolicy(
            batch_limit=args.batch_limit,
            retry_attempts=args.retry_attempts,
            min_segments_pending=args.min_segments_pending,
            min_compaction_lag_seconds=args.min_compaction_lag_seconds,
            retain_compacted_segments=args.retain_compacted_segments,
            dry_run=args.dry_run,
        ),
    )
    payload = {
        "env": report.env,
        "policy": {
            "batch_limit": report.policy.batch_limit,
            "retry_attempts": report.policy.retry_attempts,
            "min_segments_pending": report.policy.min_segments_pending,
            "min_compaction_lag_seconds": report.policy.min_compaction_lag_seconds,
            "retain_compacted_segments": report.policy.retain_compacted_segments,
            "dry_run": report.policy.dry_run,
        },
        "planned_partitions": report.planned_partitions,
        "compacted_partitions": report.compacted_partitions,
        "failed_partitions": report.failed_partitions,
        "results": [
            {
                "partition_path": item.partition_path,
                "symbol": item.symbol,
                "day": item.day,
                "status": item.status,
                "attempt_count": item.attempt_count,
                "retained_segments": item.retained_segments,
                "output_path": item.output_path,
                "error": item.error,
            }
            for item in report.results
        ],
    }
    print(json.dumps(payload, indent=2))
    return 1 if report.failed_partitions > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

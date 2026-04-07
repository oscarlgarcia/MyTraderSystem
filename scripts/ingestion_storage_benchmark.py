from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.ingestion_validation import run_storage_benchmark


def main() -> int:
    def _parse_counts(value: str) -> tuple[int, ...]:
        if str(value).strip() == "":
            return ()
        return tuple(int(part.strip()) for part in str(value).split(",") if part.strip())

    parser = argparse.ArgumentParser(description="Deterministic storage throughput benchmark for segmented normalized storage")
    parser.add_argument("--target-profile", choices=["paper", "live", "robustness"], default="paper")
    parser.add_argument("--symbol-count", type=int, default=12)
    parser.add_argument("--high-cardinality-symbol-counts", type=_parse_counts, default=())
    parser.add_argument("--bursts", type=int, default=4)
    parser.add_argument("--events-per-symbol-per-burst", type=int, default=12)
    parser.add_argument("--min-rows-per-second", type=float, default=None)
    parser.add_argument("--max-write-latency-seconds", type=float, default=None)
    parser.add_argument("--max-compaction-elapsed-seconds", type=float, default=None)
    parser.add_argument("--max-shadow-elapsed-seconds", type=float, default=None)
    parser.add_argument("--workspace-dir", default=None)
    parser.add_argument("--keep-workdir", action="store_true")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_storage_benchmark.json",
        help="JSON evidence output path",
    )
    args = parser.parse_args()

    evidence = run_storage_benchmark(
        Path(args.output),
        target_profile=args.target_profile,
        symbol_count=args.symbol_count,
        high_cardinality_symbol_counts=args.high_cardinality_symbol_counts,
        bursts=args.bursts,
        events_per_symbol_per_burst=args.events_per_symbol_per_burst,
        min_rows_per_second=args.min_rows_per_second,
        max_write_latency_slo=args.max_write_latency_seconds,
        max_compaction_elapsed_slo=args.max_compaction_elapsed_seconds,
        max_shadow_elapsed_slo=args.max_shadow_elapsed_seconds,
        cleanup=not args.keep_workdir,
        workspace_dir=Path(args.workspace_dir) if args.workspace_dir else None,
    )
    print(f"storage benchmark: {'PASS' if evidence.pass_ok else 'FAIL'}")
    print(f"- generated_at: {evidence.generated_at}")
    print(f"- target_profile: {evidence.target_profile}")
    print(f"- synthetic_rows_per_second: {evidence.synthetic_case.rows_per_second:.3f}")
    print(f"- replay_rows_per_second: {evidence.replay_case.rows_per_second:.3f}")
    print(f"- high_cardinality_cases: {len(evidence.high_cardinality_cases)}")
    print(f"- report_path: {args.output}")
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

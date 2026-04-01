from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.ingestion_validation import run_canary_validation


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic ingestion canary validation")
    parser.add_argument("--baseline-version", choices=["v1", "v2"], default="v1")
    parser.add_argument("--candidate-version", choices=["v1", "v2"], default="v2")
    parser.add_argument("--event-count", type=int, default=200)
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_canary_report.json",
        help="JSON report output path",
    )
    args = parser.parse_args()

    evidence = run_canary_validation(
        Path(args.output),
        baseline_version=args.baseline_version,
        candidate_version=args.candidate_version,
        event_count=args.event_count,
    )
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

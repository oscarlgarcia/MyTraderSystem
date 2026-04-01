from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.ingestion_validation import run_soak_validation


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic ingestion soak validation")
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--events-per-iteration", type=int, default=500)
    parser.add_argument("--pipeline-version", choices=["v1", "v2"], default="v2")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_soak_evidence.json",
        help="JSON evidence output path",
    )
    args = parser.parse_args()

    evidence = run_soak_validation(
        Path(args.output),
        iterations=args.iterations,
        events_per_iteration=args.events_per_iteration,
        pipeline_version=args.pipeline_version,
    )
    return 0 if evidence.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

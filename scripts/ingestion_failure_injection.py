from __future__ import annotations

import argparse
from pathlib import Path

from app.ops.ingestion_validation import (
    CRITICAL_FAILURE_INJECTION_TEST_IDS,
    run_failure_injection_validation,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the critical ingestion failure-injection subset and persist a JSON artifact")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_failure_injection.json",
        help="JSON artifact output path",
    )
    parser.add_argument(
        "--pytest-target",
        default="tests/ops/test_failure_injection.py",
        help="Reference pytest target that owns the critical failure-injection subset",
    )
    parser.add_argument(
        "--critical-test-ids",
        default=",".join(CRITICAL_FAILURE_INJECTION_TEST_IDS),
        help="Comma-separated pytest node ids for the critical live-blocking subset",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    critical_test_ids = tuple(part.strip() for part in str(args.critical_test_ids).split(",") if part.strip())
    evidence = run_failure_injection_validation(
        Path(args.output),
        pytest_target=args.pytest_target,
        critical_test_ids=critical_test_ids,
    )
    print(f"failure injection: {'PASS' if evidence.pass_ok else 'FAIL'}")
    print(f"- generated_at: {evidence.generated_at}")
    print(f"- pytest_target: {evidence.pytest_target}")
    print(f"- critical_test_ids: {len(evidence.critical_test_ids)}")
    print(f"- duration_seconds: {evidence.duration_seconds:.3f}")
    print(f"- returncode: {evidence.returncode}")
    print(f"- report_path: {args.output}")
    return 0 if evidence.pass_ok else evidence.returncode


if __name__ == "__main__":
    raise SystemExit(main())

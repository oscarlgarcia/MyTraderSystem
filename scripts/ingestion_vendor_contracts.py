from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.ingestion_validation import run_vendor_contract_validation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run vendor network contract tests and persist a JSON artifact")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_vendor_contracts.json",
        help="JSON artifact output path",
    )
    parser.add_argument(
        "--pytest-target",
        default="tests/network/test_binance_contracts.py",
        help="Pytest target executed with marker=network",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence = run_vendor_contract_validation(
        Path(args.output),
        pytest_target=args.pytest_target,
    )
    print(f"vendor contracts: {'PASS' if evidence.pass_ok else 'FAIL'}")
    print(f"- generated_at: {evidence.generated_at}")
    print(f"- pytest_target: {evidence.pytest_target}")
    print(f"- duration_seconds: {evidence.duration_seconds:.3f}")
    print(f"- returncode: {evidence.returncode}")
    print(f"- report_path: {args.output}")
    return 0 if evidence.pass_ok else evidence.returncode


if __name__ == "__main__":
    raise SystemExit(main())

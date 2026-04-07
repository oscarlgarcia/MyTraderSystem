from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.config import load_config
from app.ops.live_cutover import render_live_cutover_summary, run_live_cutover_drill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute the ingestion live cutover drill and persist evidence."
    )
    parser.add_argument("--env", default="dev")
    parser.add_argument("--base-dir", default=None)
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_live_drill_report.json",
    )
    parser.add_argument(
        "--release-gates-path",
        default="docs/validation/ingestion_release_gates.json",
    )
    parser.add_argument(
        "--rest-canary-path",
        default="docs/validation/ingestion_canary_report.json",
    )
    parser.add_argument(
        "--ws-canary-path",
        default="docs/validation/ingestion_ws_canary_report.json",
    )
    parser.add_argument(
        "--benchmark-path",
        default="docs/validation/ingestion_storage_benchmark.json",
    )
    parser.add_argument(
        "--failure-injection-path",
        default="docs/validation/ingestion_failure_injection.json",
    )
    parser.add_argument(
        "--rollback-checklist-path",
        default="docs/operations/ingestion_rollback_checklist.md",
    )
    parser.add_argument(
        "--live-cutover-doc-path",
        default="docs/ops/live_cutover.md",
    )
    parser.add_argument(
        "--promotion-runbook-path",
        default="docs/operations/ingestion_promotion_runbook.md",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cfg = load_config(args.env)
    base_dir = Path(args.base_dir) if args.base_dir else Path(cfg.data_dir)
    report = run_live_cutover_drill(
        base_dir=base_dir,
        env=args.env,
        output_path=Path(args.output),
        release_gate_path=Path(args.release_gates_path),
        rest_canary_path=Path(args.rest_canary_path),
        ws_canary_path=Path(args.ws_canary_path),
        benchmark_path=Path(args.benchmark_path),
        failure_injection_path=Path(args.failure_injection_path),
        rollback_checklist_path=Path(args.rollback_checklist_path),
        live_cutover_doc_path=Path(args.live_cutover_doc_path),
        promotion_runbook_path=Path(args.promotion_runbook_path),
    )
    print(render_live_cutover_summary(report))
    print(f"- report_path: {args.output}")
    return 0 if report.promote_ready and report.rollback_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())

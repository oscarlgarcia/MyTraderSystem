from __future__ import annotations

import argparse

from app.features.shadow_summary import summarize_shadow_reports, write_shadow_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize feature shadow validation reports")
    parser.add_argument("--input", required=True, help="Ruta al JSONL de shadow reports.")
    parser.add_argument("--output", required=True, help="Ruta del resumen JSON.")
    parser.add_argument("--max-failed-reports", type=int, default=0)
    parser.add_argument("--max-critical-failures", type=int, default=0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = summarize_shadow_reports(
        args.input,
        max_failed_reports=args.max_failed_reports,
        max_critical_failures=args.max_critical_failures,
    )
    write_shadow_summary(args.output, summary)
    print(f"shadow_summary pass_ok={summary.pass_ok} failed={summary.failed_reports} critical={summary.critical_failures}")
    return 0 if summary.pass_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

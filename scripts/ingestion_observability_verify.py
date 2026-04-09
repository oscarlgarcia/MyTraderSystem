from __future__ import annotations

import argparse
from pathlib import Path

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.ops.operational_evidence import build_observability_verification_report, write_observability_verification_report


SURFACE_ALIASES = ("runtime", "alerts", "logs", "promotion", "cutover")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persist external observability verification for ingestion closure")
    parser.add_argument("--target", choices=["paper", "live"], required=True)
    parser.add_argument("--output", default="docs/validation/ingestion_observability_verification.json")
    parser.add_argument("--verification-source", default="manual_surface_check")
    for alias in SURFACE_ALIASES:
        parser.add_argument(f"--{alias}-owner", default=None)
        parser.add_argument(f"--{alias}-surface-ref", default=None)
        parser.add_argument(f"--{alias}-verification-ref", default=None)
    return parser


def _build_surface_overrides(args: argparse.Namespace) -> dict[str, dict[str, object]]:
    target = str(args.target)
    mapping = {
        "runtime": f"ingestion.{target}.runtime",
        "alerts": f"ingestion.{target}.alerts",
        "logs": f"ingestion.{target}.logs",
        "promotion": f"ingestion.{target}.promotion",
        "cutover": "ingestion.live.cutover",
    }
    overrides: dict[str, dict[str, object]] = {}
    for alias, surface_id in mapping.items():
        owner = getattr(args, f"{alias}_owner")
        surface_ref = getattr(args, f"{alias}_surface_ref")
        verification_ref = getattr(args, f"{alias}_verification_ref")
        if target != "live" and alias == "cutover":
            continue
        if owner is None and surface_ref is None and verification_ref is None:
            continue
        overrides[surface_id] = {}
        if owner is not None:
            overrides[surface_id]["owner"] = owner
        if surface_ref is not None:
            overrides[surface_id]["surface_ref"] = surface_ref
        if verification_ref is not None:
            overrides[surface_id]["verification_ref"] = verification_ref
    return overrides


def main() -> int:
    args = build_parser().parse_args()
    report = build_observability_verification_report(
        target=args.target,
        verification_source=str(args.verification_source),
        surface_overrides=_build_surface_overrides(args),
    )
    output_path = Path(args.output)
    write_observability_verification_report(output_path, report)
    print(f"ingestion observability verification: {'PASS' if report['pass_ok'] else 'FAIL'} ({args.target})")
    print(f"- report_path: {output_path}")
    return 0 if report["pass_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from _script_bootstrap import bootstrap_repo_path

bootstrap_repo_path()

from app.common.dto import MarketEvent
from app.config import load_config
from app.features.benchmarks import FeatureBenchmarkReport, FeatureBenchmarkThresholds, run_feature_benchmarks
from app.features.model_contract import FeatureConsumerContract, validate_feature_contract
from app.features.definitions import build_legacy_feature_set_definition
from app.features.observability import export_feature_observability_bundle
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.parity import ParityReport, run_parity_check
from app.features.rollout_audit import CanaryAuditStore
from app.features.serving import FeatureServingService
from app.features.training_bundle_registry import TrainingBundleRecord, TrainingBundleRegistry


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap operational feature artifacts and seed the online store.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default="dev")
    parser.add_argument("--target", choices=["paper", "live"], default="paper")
    parser.add_argument("--feature-set-name", default="legacy")
    parser.add_argument("--feature-set-version", default="legacy")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--event-count", type=int, default=32)
    return parser


def _build_events(*, symbol: str, count: int) -> list[MarketEvent]:
    base_ts = datetime.now(timezone.utc) - timedelta(minutes=count + 1)
    return [
        MarketEvent(
            symbol=symbol,
            event_ts=base_ts + timedelta(minutes=index),
            available_ts=base_ts + timedelta(minutes=index),
            price=100.0 + float(index),
            size=1.0,
            source="trade",
        )
        for index in range(count)
    ]


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _serialize_parity(report: ParityReport) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pass_ok": report.pass_ok,
        "parity_mismatches": len(report.mismatches),
        "mismatches": [
            {
                "symbol": mismatch.symbol,
                "entity_scope": mismatch.entity_scope,
                "ts": mismatch.ts.isoformat(),
                "feature_name": mismatch.feature_name,
                "offline_value": mismatch.offline_value,
                "online_value": mismatch.online_value,
                "reason": mismatch.reason,
            }
            for mismatch in report.mismatches[:25]
        ],
    }


def _serialize_benchmark(report: FeatureBenchmarkReport) -> dict[str, object]:
    payload = asdict(report)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    payload["pass_ok"] = bool(report.threshold_pass_ok)
    return payload


def _bootstrap_benchmark_thresholds(target: str) -> FeatureBenchmarkThresholds | None:
    if target != "live":
        return None
    return FeatureBenchmarkThresholds(
        min_materialization_rows_per_second=20.0,
        min_online_updates_per_second=10.0,
        min_serving_requests_per_second=25.0,
    )


def main() -> int:
    args = _parser().parse_args()
    cfg = load_config(args.env)
    output_dir = Path(args.output_dir) if args.output_dir else cfg.feature_validation_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_set = build_legacy_feature_set_definition(
        name=args.feature_set_name,
        version=args.feature_set_version,
        description=f"{args.target} bootstrap feature set",
        windows=[2, 3],
        aggregators=["sma", "ema"],
        transformers=[],
    )
    events = _build_events(symbol=args.symbol, count=max(args.event_count, 8))

    parity_report = run_parity_check(
        events,
        feature_set=feature_set,
        offline_store_path=cfg.feature_offline_store_path,
        online_store_path=cfg.feature_store_server_path,
        runtime_mode=args.target,
    )
    _write_json(output_dir / "feature_parity_report.json", _serialize_parity(parity_report))

    benchmark_report = run_feature_benchmarks(
        events,
        feature_set=feature_set,
        offline_store_path=cfg.feature_offline_store_path,
        online_store_path=cfg.feature_store_server_path,
        thresholds=_bootstrap_benchmark_thresholds(args.target),
        target=args.target,
    )
    _write_json(output_dir / "feature_benchmark_report.json", _serialize_benchmark(benchmark_report))

    training_registry = TrainingBundleRegistry(cfg.feature_training_bundle_registry_dir)
    dataset_id = f"{args.target}-runtime-{args.symbol.lower()}"
    schema_hash = feature_set.definition_hash
    online_store = OnlineFeatureStore(cfg.feature_store_server_path)
    offline_store = OfflineFeatureStore(cfg.feature_offline_store_path)
    latest = online_store.get_latest(
        symbol=args.symbol,
        feature_set_name=args.feature_set_name,
        feature_set_version=args.feature_set_version,
    )
    if latest is None:
        raise SystemExit("feature bootstrap failed to seed the online store")
    if not latest.lineage_id:
        latest.lineage_id = f"{args.feature_set_name}-{args.feature_set_version}-{args.target}-{uuid4().hex[:12]}"
        online_store.upsert(latest)
        offline_store.put_many([latest], run_id=f"bootstrap-{args.target}-{args.symbol.lower()}")
    training_bundle_id = f"{args.feature_set_name}-{args.feature_set_version}-{args.target}-{uuid4().hex[:12]}"
    training_registry.register(
        TrainingBundleRecord(
            bundle_id=training_bundle_id,
            dataset_id=dataset_id,
            feature_schema_hash=schema_hash,
            feature_set_name=args.feature_set_name,
            feature_set_version=args.feature_set_version,
            feature_bundle_id=latest.lineage_id,
            metadata={"target": args.target, "symbol": args.symbol},
        )
    )

    contract = FeatureConsumerContract(
        consumer_name=f"{args.target}-strategy",
        consumer_kind="strategy",
        feature_set_name=args.feature_set_name,
        feature_set_version=args.feature_set_version,
        required_features=tuple(sorted(latest.values.keys())),
        required_metadata_keys=("feature_bundle_id", "dataset_id", "feature_schema_hash", "training_bundle_id"),
        required_dataset_id=dataset_id,
        required_schema_hash=schema_hash,
        required_training_bundle_id=training_bundle_id,
        require_feature_bundle_match=True,
        target=args.target,
    )
    consumer_metadata = {
        "feature_bundle_id": latest.lineage_id,
        "dataset_id": dataset_id,
        "feature_schema_hash": schema_hash,
        "training_bundle_id": training_bundle_id,
    }
    contract_result = validate_feature_contract(
        contract=contract,
        feature_vector=latest,
        consumer_metadata=consumer_metadata,
        training_bundle_registry=training_registry,
    )
    _write_json(
        output_dir / "feature_contract_validation.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pass_ok": contract_result.pass_ok,
            "reasons": list(contract_result.reasons),
            "dataset_id": dataset_id,
            "feature_schema_hash": schema_hash,
            "training_bundle_id": training_bundle_id,
            "feature_bundle_id": latest.lineage_id,
        },
    )

    service = FeatureServingService(
        online_store=online_store,
        offline_store=offline_store,
        training_bundle_registry=training_registry,
        target=args.target,
    )
    decision_ts = latest.available_ts + timedelta(seconds=1)
    serving_result = service.get_latest_servable(
        symbol=args.symbol,
        decision_ts=decision_ts,
        feature_set_name=args.feature_set_name,
        feature_set_version=args.feature_set_version,
        contract=contract,
        consumer_metadata=consumer_metadata,
    )
    if serving_result.status == "fail":
        raise SystemExit(f"feature bootstrap serving failed: {serving_result.reason}")
    export_feature_observability_bundle(
        metrics=service.metrics,
        target=args.target,
        output_path=output_dir / "feature_observability.json",
    )

    audit_store = CanaryAuditStore(output_dir / "feature_rollout_audit.jsonl")
    audit_store.append(
        feature_set_name=args.feature_set_name,
        route="primary",
        version=args.feature_set_version,
        symbol=args.symbol,
        entity_scope=f"symbol={args.symbol}",
        decision_ts=decision_ts,
        status=serving_result.status,
    )
    _write_json(
        output_dir / "feature_rollout_audit.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pass_ok": True,
            "decision_count": 1,
            "audit_path": str(audit_store.path),
            "route": "primary",
            "feature_set_name": args.feature_set_name,
            "feature_set_version": args.feature_set_version,
        },
    )

    print(
        "feature_bootstrap_release_inputs"
        f" target={args.target}"
        f" feature_set={args.feature_set_name}:{args.feature_set_version}"
        f" symbol={args.symbol}"
        f" output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

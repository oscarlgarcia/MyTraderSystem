"""
Entry point for the trading system.

Implements a three-mode pipeline:
- dry (default): determinista, sin IO externo, apto para tests/CI.
- paper: usa market data live con ejecucion paper y auditoria obligatoria.
- live: usa el runtime operativo mas estricto disponible.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Dict
from uuid import uuid4

from app import common, execution, features, ingestion, observability, ops, portfolio, risk, strategy  # noqa: F401
from app.common.dto import TraceContext
from app.config import AppConfig, DEFAULT_INGEST_STREAM_TYPES, load_config, parse_args
from app.controlplane.telemetry import configure_control_plane_telemetry
from app.ingestion.service import run_ingestion_service
from app.marketdata.support_matrix import validate_live_feed_support
from app.marketdata.models import IngestionEvent
from app.observability.logger import get_logger, set_trace_id
from app.ingestion.storage import validate_output_path
from app.ingestion.storage_health import assert_storage_health_for_runtime
from app.features.pipeline import run_feature_pipeline
from app.features.engine import FeatureEngine
from app.features.audit import build_decision_audit_record, persist_decision_audits
from app.features.live_readiness import FeatureLiveReadinessDecision
from app.features.metrics import FeatureMetrics
from app.features.parity import ParityReport
from app.features.release_workflow import gate_and_publish_feature_release, rollback_feature_release
from app.ops.release_gates import render_release_gate_summary, run_release_gates
from app.strategy.basic import generate_signals
from app.risk.rules import apply_risk
from app.execution.paper import paper_execute
from app.portfolio.state import update_portfolio

FAST_PATH_BATCH_SIZE = 256


def _load_feature_release_gate_inputs(path: str | None) -> tuple[ParityReport, FeatureMetrics, FeatureLiveReadinessDecision | None]:
    if not path:
        raise ValueError("feature release publish requires --feature-release-gate-input")
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    gate_payload = payload.get("gate_report", payload)
    mismatches = int(gate_payload.get("parity_mismatches", payload.get("parity_mismatches", 0)))
    if mismatches == 0 and gate_payload.get("pass_ok") is False:
        mismatches = 1
    stale_serves = int(gate_payload.get("stale_count", payload.get("stale_serves", 0)))
    serving_latency_max = float(gate_payload.get("latency_breaches", payload.get("serving_latency_max", 0.0)))
    invalid_ratio_breaches = int(gate_payload.get("invalid_ratio_breaches", 0))
    parity_report = ParityReport(pass_ok=mismatches == 0, mismatches=tuple(object() for _ in range(mismatches)))
    metrics = FeatureMetrics(
        stale_serves=stale_serves,
        serving_latency_max=serving_latency_max,
        invalid_serves=invalid_ratio_breaches,
        serving_requests=max(invalid_ratio_breaches, 1),
    )
    live_readiness_payload = payload.get("live_readiness")
    live_readiness = None
    if isinstance(live_readiness_payload, dict):
        live_readiness = FeatureLiveReadinessDecision(
            pass_ok=bool(live_readiness_payload.get("pass_ok", False)),
            action=str(live_readiness_payload.get("action", "hold")),
            reasons=tuple(str(item) for item in live_readiness_payload.get("reasons", [])),
        )
    return parity_report, metrics, live_readiness


def _trace(logger, enabled: bool, phase: str, status: str, extra: Optional[Dict[str, object]] = None) -> None:
    if not enabled:
        return
    payload = {"phase": phase, "status": status}
    if extra:
        payload.update(extra)
    logger.info("pipeline step", extra=payload)


def _mark(recorder: Optional[List[str]], step: str) -> None:
    if recorder is not None:
        recorder.append(step)


def _price_map_from_events(events: List[IngestionEvent]) -> Dict[str, float]:
    price_by_symbol: Dict[str, float] = {}
    for ev in events:
        price_by_symbol[ev.symbol] = ev.price
    return price_by_symbol


def run_trading_cycle(
    events: List[IngestionEvent],
    *,
    logger,
    recorder: Optional[List[str]] = None,
    trace_steps: bool = False,
    feature_audit_path: str | None = None,
    mode: str = "dry",
):
    if mode != "dry" and not feature_audit_path:
        raise ValueError("feature_audit_path is required outside dry mode")

    _trace(logger, trace_steps, "features", "start")
    runtime_mode = "live" if mode == "live" else ("paper" if mode == "paper" else "research")
    feature_engine = FeatureEngine(strict_temporal_semantics=mode != "dry", runtime_mode=runtime_mode)
    fvs = run_feature_pipeline(events, engine=feature_engine)
    _trace(logger, trace_steps, "features", "done", {"count": len(fvs)})
    _mark(recorder, "features")

    _trace(logger, trace_steps, "strategy", "start")
    signals = generate_signals(fvs)
    if feature_audit_path:
        audits = [build_decision_audit_record(fv, sig) for fv, sig in zip(fvs, signals)]
        persist_decision_audits(audits, feature_audit_path)
    _trace(logger, trace_steps, "strategy", "done", {"count": len(signals)})
    _mark(recorder, "strategy")

    price_by_symbol = _price_map_from_events(events)
    _trace(logger, trace_steps, "risk", "start")
    order_intents = apply_risk(signals, price_by_symbol=price_by_symbol)
    _trace(logger, trace_steps, "risk", "done", {"count": len(order_intents)})
    _mark(recorder, "risk")

    _trace(logger, trace_steps, "execution", "start")
    reports = paper_execute(order_intents, price_by_symbol=price_by_symbol)
    _trace(logger, trace_steps, "execution", "done", {"count": len(reports)})
    _mark(recorder, "execution")

    _trace(logger, trace_steps, "portfolio", "start")
    portfolio_state = update_portfolio(reports)
    _trace(logger, trace_steps, "portfolio", "done", {"positions": portfolio_state.positions})
    _mark(recorder, "portfolio")

    return {
        "events": len(events),
        "features": len(fvs),
        "signals": len(signals),
        "orders": len(order_intents),
        "fills": len([r for r in reports if r.status == "filled"]),
        "positions": portfolio_state.positions,
        "cash": portfolio_state.cash,
    }


def _resolve_runtime_options(args) -> Dict[str, object]:
    fast_path = bool(getattr(args, "fast_path", False))
    ingest_batch_size = int(getattr(args, "ingest_batch_size", 1))
    return {
        "fast_path": fast_path,
        "production_mode": bool(getattr(args, "production_mode", False)),
        "trace_steps": False if fast_path else bool(getattr(args, "trace_steps", False)),
        "ingest_max_buffer": int(getattr(args, "ingest_max_buffer", 10_000)),
        "ingest_dedup": False if fast_path else bool(getattr(args, "ingest_dedup", True)),
        "ingest_batch_size": max(FAST_PATH_BATCH_SIZE, ingest_batch_size) if fast_path else ingest_batch_size,
        "ingest_lag_warn": getattr(args, "ingest_lag_warn", None),
        "ingest_buffer_warn": getattr(args, "ingest_buffer_warn", None),
        "ingest_backpressure_policy": getattr(args, "ingest_backpressure_policy", "pause"),
        "ingest_temporal_policy": getattr(args, "ingest_temporal_policy", "accept"),
        "ingest_pipeline_version": getattr(args, "ingest_pipeline_version", "v2"),
        "ingest_shadow_mode": bool(getattr(args, "ingest_shadow_mode", False)),
        "ingest_shadow_block_on_diff": bool(getattr(args, "ingest_shadow_block_on_diff", False)),
        "ingest_stream_types": tuple(getattr(args, "ingest_stream_types", DEFAULT_INGEST_STREAM_TYPES)),
        "allow_live_fallback": bool(getattr(args, "allow_live_fallback", False)),
        "error_policy": getattr(args, "error_policy", None),
        "snapshot_enabled": not fast_path,
        "summary_logging": not fast_path,
    }


def _validate_operational_security(
    cfg: AppConfig,
    *,
    mode: str,
    runtime: Dict[str, object],
) -> None:
    production_mode = bool(runtime.get("production_mode", False))
    ingest_stream_types = tuple(runtime.get("ingest_stream_types", DEFAULT_INGEST_STREAM_TYPES))
    validate_output_path(cfg.data_dir, require_absolute=production_mode)
    if mode in {"live", "paper"} and not production_mode:
        try:
            validate_live_feed_support(
                ingest_stream_types,
                require_exact_recovery=production_mode,
                require_exact_verified=production_mode,
                require_handoff=production_mode,
            )
        except ValueError as exc:
            prefix = (
                "Unsafe production configuration: "
                if production_mode
                else "Unsupported live feed configuration: "
            )
            raise ValueError(prefix + str(exc)) from exc
    if not production_mode:
        return

    errors: list[str] = []
    if cfg.env != "prod":
        errors.append("production mode requires --env prod")
    if mode != "live":
        errors.append("production mode requires --mode live")
    if bool(runtime.get("fast_path", False)):
        errors.append("production mode rejects --fast-path")
    if bool(runtime.get("allow_live_fallback", False)):
        errors.append("production mode rejects --allow-live-fallback")
    if runtime.get("error_policy") not in (None, "fail_fast"):
        errors.append("production mode requires error_policy=fail_fast")
    if not bool(runtime.get("ingest_dedup", True)):
        errors.append("production mode requires ingest dedup enabled")
    if not bool(runtime.get("summary_logging", True)):
        errors.append("production mode requires ingestion summary logging")
    if runtime.get("ingest_backpressure_policy") in {"drop_oldest", "drop_newest"}:
        errors.append("production mode rejects lossy backpressure policies")
    if str(cfg.log_level).upper() == "DEBUG":
        errors.append("production mode rejects DEBUG logging")
    if errors:
        raise ValueError("Unsafe production configuration: " + "; ".join(errors))
    if mode == "live":
        try:
            validate_live_feed_support(
                ingest_stream_types,
                require_exact_recovery=True,
                require_exact_verified=True,
                require_handoff=True,
            )
        except ValueError as exc:
            raise ValueError("Unsafe production configuration: " + str(exc)) from exc
        metadata_path = Path(cfg.data_dir) / "metadata" / "instruments" / f"env={cfg.env}" / "venue=BINANCE" / "latest.json"
        if not metadata_path.exists():
            raise ValueError(f"Unsafe production configuration: missing instrument metadata snapshot {metadata_path}")
        metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata_payload.get("metadata_snapshot_mode") != "runtime":
            raise ValueError("Unsafe production configuration: production mode requires runtime instrument metadata snapshot")
        if bool((metadata_payload.get("drift") or {}).get("material")):
            raise ValueError("Unsafe production configuration: material provider metadata drift detected")
        assert_storage_health_for_runtime(cfg.data_dir, cfg.env)


def run_cycle(
    cfg: Optional[AppConfig] = None,
    logger=None,
    *,
    mode: str = "dry",
    max_events: int = 50,
    duration_s: Optional[float] = None,
    recorder: Optional[List[str]] = None,
    trace_steps: bool = False,
    ingest_max_buffer: int = 10_000,
    ingest_dedup: bool = True,
    ingest_batch_size: int = 1,
    snapshot_enabled: bool = True,
    live_summary_logging: bool = True,
    ingest_lag_warn: float | None = None,
    ingest_buffer_warn: int | None = None,
    ingest_backpressure_policy: str = "pause",
    ingest_temporal_policy: str = "accept",
    ingest_pipeline_version: str = "v2",
    ingest_shadow_mode: bool = False,
    ingest_shadow_block_on_diff: bool = False,
    ingest_stream_types: tuple[str, ...] = DEFAULT_INGEST_STREAM_TYPES,
    production_mode: bool = False,
    allow_live_fallback: bool = False,
    error_policy: str | None = None,
    feature_audit_path: str | None = None,
):
    """
    Ejecuta el pipeline completo (determinista por defecto).

    Steps: ingestion -> features -> strategy -> risk -> execution -> portfolio.
    """
    cfg = cfg or load_config()
    logger = logger or get_logger(level=cfg.log_level)

    _trace(logger, trace_steps, "ingestion", "start")
    events = run_ingestion_service(
        cfg=cfg,
        logger=logger,
        mode=mode,
        max_events=max_events,
        duration_s=duration_s,
        ingest_max_buffer=ingest_max_buffer,
        ingest_dedup=ingest_dedup,
        ingest_batch_size=ingest_batch_size,
        snapshot_enabled=snapshot_enabled,
        live_summary_logging=live_summary_logging,
        ingest_lag_warn=ingest_lag_warn,
        ingest_buffer_warn=ingest_buffer_warn,
        ingest_backpressure_policy=ingest_backpressure_policy,
        ingest_temporal_policy=ingest_temporal_policy,
        ingest_pipeline_version=ingest_pipeline_version,
        ingest_shadow_mode=ingest_shadow_mode,
        ingest_shadow_block_on_diff=ingest_shadow_block_on_diff,
        ingest_stream_types=ingest_stream_types,
        production_mode=production_mode,
        allow_live_fallback=allow_live_fallback,
        error_policy=error_policy,
    )
    _trace(logger, trace_steps, "ingestion", "done", {"count": len(events)})
    _mark(recorder, "ingestion")

    return run_trading_cycle(
        events,
        logger=logger,
        recorder=recorder,
        trace_steps=trace_steps,
        feature_audit_path=feature_audit_path,
        mode=mode,
    )


def run() -> int:

    """Bootstrap principal; devuelve 0 en éxito."""
    args = parse_args()
    config = load_config(args.env)
    configure_control_plane_telemetry(config.control_plane_telemetry_dir)
    if getattr(args, "feature_release_action", None):
        if not getattr(args, "feature_release_name", None):
            raise ValueError("feature release action requires --feature-release-name")
        registry_path = Path(args.feature_release_registry)
        if args.feature_release_action == "publish":
            if not getattr(args, "feature_release_version", None):
                raise ValueError("feature release publish requires --feature-release-version")
            parity_report, metrics, live_readiness = _load_feature_release_gate_inputs(getattr(args, "feature_release_gate_input", None))
            gate_and_publish_feature_release(
                registry_path=registry_path,
                feature_set_name=args.feature_release_name,
                version=args.feature_release_version,
                parity_report=parity_report,
                metrics=metrics,
                target=args.feature_release_target,
                actor="app.main.run",
                live_readiness=live_readiness,
            )
            return 0
        rollback_feature_release(
            registry_path=registry_path,
            feature_set_name=args.feature_release_name,
            target=args.feature_release_target,
            actor="app.main.run",
        )
        return 0
    if getattr(args, "release_gates", False):
        report = run_release_gates(
            base_dir=config.data_dir,
            env=config.env,
            target=args.release_gates_target,
            stream_types=getattr(args, "ingest_stream_types", DEFAULT_INGEST_STREAM_TYPES),
            output_path=args.release_gates_output,
            rest_canary_path=args.release_gates_rest_canary_path,
            ws_canary_path=args.release_gates_ws_canary_path,
            replay_parity_path=args.release_gates_replay_parity_path,
            benchmark_path=args.release_gates_benchmark_path,
            soak_path=args.release_gates_soak_path,
            network_contracts_path=args.release_gates_vendor_contracts_path,
            live_drill_path=args.release_gates_live_drill_path,
        )
        print(render_release_gate_summary(report))
        return 0 if report.pass_ok else 1
    runtime = _resolve_runtime_options(args)
    _validate_operational_security(config, mode=args.mode, runtime=runtime)
    trace_id = str(uuid4())
    set_trace_id(trace_id)
    logger = get_logger(level=config.log_level)
    _ = TraceContext(trace_id=trace_id)

    metrics = run_cycle(
        cfg=config,
        logger=logger,
        mode=args.mode,
        max_events=args.max_events,
        duration_s=args.duration,
        recorder=[],
        trace_steps=runtime["trace_steps"],
        ingest_max_buffer=runtime["ingest_max_buffer"],
        ingest_dedup=runtime["ingest_dedup"],
        ingest_batch_size=runtime["ingest_batch_size"],
        snapshot_enabled=runtime["snapshot_enabled"],
        live_summary_logging=runtime["summary_logging"],
        ingest_lag_warn=runtime["ingest_lag_warn"],
        ingest_buffer_warn=runtime["ingest_buffer_warn"],
        ingest_backpressure_policy=runtime["ingest_backpressure_policy"],
        ingest_temporal_policy=runtime["ingest_temporal_policy"],
        ingest_pipeline_version=runtime["ingest_pipeline_version"],
        ingest_shadow_mode=runtime["ingest_shadow_mode"],
        ingest_shadow_block_on_diff=runtime["ingest_shadow_block_on_diff"],
        ingest_stream_types=runtime["ingest_stream_types"],
        production_mode=runtime["production_mode"],
        allow_live_fallback=runtime["allow_live_fallback"],
        error_policy=runtime["error_policy"],
        feature_audit_path=args.feature_audit_path,
    )

    logger.info(
        "pipeline ok",
        extra={
            "env": config.env,
            "data_dir": str(config.data_dir),
            "trace_id": trace_id,
            "mode": args.mode,
            "ingest_stream_types": list(runtime["ingest_stream_types"]),
            "fast_path": runtime["fast_path"],
            "error_policy": runtime["error_policy"] or ("allow_fallback" if runtime["allow_live_fallback"] else "fail_fast"),
            "metrics": metrics,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

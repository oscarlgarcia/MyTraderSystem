"""
Entry point for the trading system.

Implements a dual-mode pipeline:
- dry (default): determinista, sin IO externo, apto para tests/CI.
- live: usa WS/REST existentes con ResilientRunner y escribe Parquet acotado.
"""

from __future__ import annotations

from typing import List, Optional, Dict
from uuid import uuid4

from app import common, execution, features, ingestion, observability, ops, portfolio, risk, strategy  # noqa: F401
from app.common.dto import MarketEvent, TraceContext
from app.config import AppConfig, load_config, parse_args
from app.observability.logger import get_logger, set_trace_id
from app.ingestion.pipeline import collect_events
from app.features.pipeline import run_feature_pipeline
from app.features.engine import FeatureEngine
from app.strategy.basic import generate_signals
from app.risk.rules import apply_risk
from app.execution.paper import paper_execute
from app.portfolio.state import update_portfolio

FAST_PATH_BATCH_SIZE = 256


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


def _price_map_from_events(events: List[MarketEvent]) -> Dict[str, float]:
    price_by_symbol: Dict[str, float] = {}
    for ev in events:
        price_by_symbol[ev.symbol] = ev.price
    return price_by_symbol


def _resolve_runtime_options(args) -> Dict[str, object]:
    fast_path = bool(getattr(args, "fast_path", False))
    ingest_batch_size = int(getattr(args, "ingest_batch_size", 1))
    return {
        "fast_path": fast_path,
        "trace_steps": False if fast_path else bool(getattr(args, "trace_steps", False)),
        "compute_features_after_ingest": bool(getattr(args, "features_after_ingest", False)),
        "ingest_max_buffer": int(getattr(args, "ingest_max_buffer", 10_000)),
        "ingest_dedup": False if fast_path else bool(getattr(args, "ingest_dedup", True)),
        "ingest_batch_size": max(FAST_PATH_BATCH_SIZE, ingest_batch_size) if fast_path else ingest_batch_size,
        "snapshot_enabled": not fast_path,
        "summary_logging": not fast_path,
    }


def run_cycle(
    cfg: Optional[AppConfig] = None,
    logger=None,
    *,
    mode: str = "dry",
    max_events: int = 50,
    duration_s: Optional[float] = None,
    recorder: Optional[List[str]] = None,
    trace_steps: bool = False,
    compute_features_after_ingest: bool = False,
    ingest_max_buffer: int = 10_000,
    ingest_dedup: bool = True,
    ingest_batch_size: int = 1,
    snapshot_enabled: bool = True,
    live_summary_logging: bool = True,
):
    """
    Ejecuta el pipeline completo (determinista por defecto).

    Steps: ingestion -> features -> strategy -> risk -> execution -> portfolio.
    """
    cfg = cfg or load_config()
    logger = logger or get_logger(level=cfg.log_level)

    # Ingestión
    _trace(logger, trace_steps, "ingestion", "start")
    events = collect_events(
        mode=mode,
        cfg=cfg,
        max_events=max_events,
        duration_s=duration_s,
        logger=logger,
        compute_features_after=compute_features_after_ingest,
        max_buffer=ingest_max_buffer,
        dedup_enabled=ingest_dedup,
        batch_size=ingest_batch_size,
        snapshot_enabled=snapshot_enabled,
        summary_logging=live_summary_logging,
    )
    _trace(logger, trace_steps, "ingestion", "done", {"count": len(events)})
    _mark(recorder, "ingestion")

    # Features
    _trace(logger, trace_steps, "features", "start")
    feature_engine = FeatureEngine()
    fvs = run_feature_pipeline(events, engine=feature_engine)
    _trace(logger, trace_steps, "features", "done", {"count": len(fvs)})
    _mark(recorder, "features")

    # Estrategia
    _trace(logger, trace_steps, "strategy", "start")
    signals = generate_signals(fvs)
    _trace(logger, trace_steps, "strategy", "done", {"count": len(signals)})
    _mark(recorder, "strategy")

    # Riesgo
    price_by_symbol = _price_map_from_events(events)
    _trace(logger, trace_steps, "risk", "start")
    order_intents = apply_risk(signals, price_by_symbol=price_by_symbol)
    _trace(logger, trace_steps, "risk", "done", {"count": len(order_intents)})
    _mark(recorder, "risk")

    # Ejecución (paper)
    _trace(logger, trace_steps, "execution", "start")
    reports = paper_execute(order_intents, price_by_symbol=price_by_symbol)
    _trace(logger, trace_steps, "execution", "done", {"count": len(reports)})
    _mark(recorder, "execution")

    # Portfolio
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


def run() -> int:
    """Bootstrap principal; devuelve 0 en éxito."""
    args = parse_args()
    config = load_config(args.env)
    runtime = _resolve_runtime_options(args)
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
        compute_features_after_ingest=runtime["compute_features_after_ingest"],
        ingest_max_buffer=runtime["ingest_max_buffer"],
        ingest_dedup=runtime["ingest_dedup"],
        ingest_batch_size=runtime["ingest_batch_size"],
        snapshot_enabled=runtime["snapshot_enabled"],
        live_summary_logging=runtime["summary_logging"],
    )

    logger.info(
        "pipeline ok",
        extra={
            "env": config.env,
            "data_dir": str(config.data_dir),
            "trace_id": trace_id,
            "mode": args.mode,
            "fast_path": runtime["fast_path"],
            "metrics": metrics,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

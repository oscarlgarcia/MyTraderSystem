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
from app.features.store import compute_features
from app.strategy.basic import generate_signals
from app.risk.rules import apply_risk
from app.execution.paper import paper_execute
from app.portfolio.state import update_portfolio


def _mark(recorder: Optional[List[str]], step: str) -> None:
    if recorder is not None:
        recorder.append(step)


def _price_map_from_events(events: List[MarketEvent]) -> Dict[str, float]:
    price_by_symbol: Dict[str, float] = {}
    for ev in events:
        price_by_symbol[ev.symbol] = ev.price
    return price_by_symbol


def run_cycle(
    cfg: Optional[AppConfig] = None,
    logger=None,
    *,
    mode: str = "dry",
    max_events: int = 50,
    duration_s: Optional[float] = None,
    recorder: Optional[List[str]] = None,
):
    """
    Ejecuta el pipeline completo (determinista por defecto).

    Steps: ingestion -> features -> strategy -> risk -> execution -> portfolio.
    """
    cfg = cfg or load_config()
    logger = logger or get_logger(level=cfg.log_level)

    # Ingestión
    events = collect_events(mode=mode, cfg=cfg, max_events=max_events, duration_s=duration_s, logger=logger)
    _mark(recorder, "ingestion")

    # Features
    fvs = compute_features(events)
    _mark(recorder, "features")

    # Estrategia
    signals = generate_signals(fvs)
    _mark(recorder, "strategy")

    # Riesgo
    price_by_symbol = _price_map_from_events(events)
    order_intents = apply_risk(signals, price_by_symbol=price_by_symbol)
    _mark(recorder, "risk")

    # Ejecución (paper)
    reports = paper_execute(order_intents, price_by_symbol=price_by_symbol)
    _mark(recorder, "execution")

    # Portfolio
    portfolio_state = update_portfolio(reports)
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
    )

    logger.info(
        "pipeline ok",
        extra={
            "env": config.env,
            "data_dir": str(config.data_dir),
            "trace_id": trace_id,
            "mode": args.mode,
            "metrics": metrics,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

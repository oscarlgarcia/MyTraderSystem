"""
Actualización mínima de estado de portafolio para paper trading.
"""

from __future__ import annotations

from typing import Dict, List, Optional
from datetime import datetime, timezone

from app.common.dto import ExecutionReport, PortfolioState


def update_portfolio(
    reports: List[ExecutionReport],
    prev_state: Optional[PortfolioState] = None,
    cash_start: float = 10_000.0,
) -> PortfolioState:
    positions: Dict[str, float] = dict(prev_state.positions) if prev_state else {}
    cash = prev_state.cash if prev_state else cash_start
    ts = datetime.now(tz=timezone.utc)

    for rep in reports:
        if rep.status not in {"filled", "partial"}:
            continue
        pos = positions.get(rep.symbol, 0.0)
        if rep.client_order_id.startswith("paper") and rep.avg_price > 0:
            notional = rep.avg_price * rep.filled_qty
            if rep.status in {"filled", "partial"}:
                if rep.client_order_id.endswith("sell") or "sell" in rep.client_order_id:
                    cash += notional
                    pos -= rep.filled_qty
                else:
                    cash -= notional
                    pos += rep.filled_qty
        positions[rep.symbol] = pos

    if cash < 0:
        cash = 0.0

    return PortfolioState(
        ts=ts,
        positions=positions,
        cash=cash,
    )

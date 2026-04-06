"""
Ejecución en modo paper: fills instantáneos sin IO externo.
"""

from __future__ import annotations

from typing import Dict, List

from app.common.dto import ExecutionReport, OrderIntent


def paper_execute(order_intents: List[OrderIntent], price_by_symbol: Dict[str, float]) -> List[ExecutionReport]:
    reports: List[ExecutionReport] = []
    for intent in order_intents:
        price = price_by_symbol.get(intent.symbol)
        if price is None or price <= 0:
            reports.append(
                ExecutionReport(
                    symbol=intent.symbol,
                    ts=intent.ts,
                    status="rejected",
                    filled_qty=0.0,
                    avg_price=0.0,
                    client_order_id=intent.intent_id or "paper",
                    reason="missing_price",
                    metadata=dict(intent.metadata),
                )
            )
            continue

        reports.append(
            ExecutionReport(
                symbol=intent.symbol,
                ts=intent.ts,
                status="filled",
                filled_qty=intent.quantity,
                avg_price=price,
                client_order_id=intent.intent_id or f"paper-{intent.side}",
                metadata=dict(intent.metadata),
            )
        )
    return reports

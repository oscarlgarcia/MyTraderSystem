"""
Reglas simples de riesgo para filtrar y dimensionar órdenes.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from app.common.dto import OrderIntent, Signal


def apply_risk(
    signals: List[Signal],
    *,
    price_by_symbol: Dict[str, float],
    max_notional: float = 1000.0,
    max_symbols: int = 3,
) -> List[OrderIntent]:
    # Ordenar por confianza descendente y limitar número de símbolos
    sorted_signals = sorted(signals, key=lambda s: s.confidence, reverse=True)
    intents: List[OrderIntent] = []
    seen = set()
    for sig in sorted_signals:
        if sig.side == "flat":
            continue
        if sig.symbol in seen:
            continue
        price = price_by_symbol.get(sig.symbol)
        if price is None or price <= 0:
            continue
        size_cap = max_notional / price
        adj_size = min(sig.size, size_cap)
        if adj_size <= 0:
            continue
        intents.append(
            OrderIntent(
                symbol=sig.symbol,
                ts=sig.ts,
                side="buy" if sig.side == "buy" else "sell",
                quantity=adj_size,
                price_limit=price,
                strategy_id=sig.strategy_id,
            )
        )
        seen.add(sig.symbol)
        if len(intents) >= max_symbols:
            break
    return intents

"""
Feature Store en memoria con ventana deslizante.

API pura: compute_features(events, window) -> FeatureVector por evento.
Sin IO ni dependencias externas.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from datetime import datetime
from typing import Deque, Dict, Iterable, List, Sequence

from app.common.dto import FeatureVector, MarketEvent


def _is_finite_price(price: float) -> bool:
    return isinstance(price, (int, float)) and math.isfinite(price)


def compute_features(
    events: Sequence[MarketEvent],
    window: int = 5,
    windows: Iterable[int] | None = None,
) -> List[FeatureVector]:
    """
    Calcula features simples (price, ret_1, sma_window) para cada evento.

    - Agrupa por símbolo y ordena por event_ts.
    - Usa una ventana deslizante acotada (window) para limitar memoria.
    - Descarta eventos con precios no finitos.
    """
    if not events:
        return []

    window_set = set(windows) if windows is not None else set()
    window_set.add(window)
    effective_window = max(window_set)

    by_symbol: Dict[str, List[MarketEvent]] = defaultdict(list)
    for ev in events:
        by_symbol[ev.symbol].append(ev)

    results: List[FeatureVector] = []
    for sym, evs in by_symbol.items():
        evs.sort(key=lambda e: e.event_ts)
        prices: Deque[float] = deque(maxlen=effective_window)
        prev_price: float | None = None

        for ev in evs:
            if not _is_finite_price(ev.price):
                continue

            prices.append(ev.price)
            values: Dict[str, float] = {"price": ev.price}

            if prev_price is not None and _is_finite_price(prev_price):
                values["ret_1"] = math.log(ev.price / prev_price) if prev_price > 0 else 0.0

            for w in window_set:
                if len(prices) >= w:
                    values[f"sma_{w}"] = sum(list(prices)[-w:]) / w

            results.append(
                FeatureVector(
                    symbol=sym,
                    ts=ev.event_ts if isinstance(ev.event_ts, datetime) else datetime.fromisoformat(str(ev.event_ts)),
                    values=values,
                )
            )

            prev_price = ev.price

    return results

"""
Feature Store en memoria con ventana deslizante.

API pura: compute_features(events, window) -> FeatureVector por evento.
Sin IO ni dependencias externas.
"""

from __future__ import annotations

import math
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Deque, Dict, Iterable, List, Sequence

from app.common.dto import FeatureVector, MarketEvent

logger = logging.getLogger("features.store")
REQUIRED_KEYS = {"price"}


def _is_finite_price(price: float) -> bool:
    return isinstance(price, (int, float)) and math.isfinite(price)


def _sma(prices: Sequence[float], window: int) -> float | None:
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / window


def _log_return(prev_price: float | None, current_price: float) -> float | None:
    if prev_price is None:
        return None
    if prev_price <= 0 or current_price <= 0:
        return None
    return math.log(current_price / prev_price)


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
    dropped_invalid = 0
    for sym, evs in by_symbol.items():
        evs.sort(key=lambda e: e.event_ts)
        prices: Deque[float] = deque(maxlen=effective_window)
        prev_valid_price: float | None = None

        for ev in evs:
            if not _is_finite_price(ev.price):
                dropped_invalid += 1
                continue

            prices.append(ev.price)
            values: Dict[str, float] = {"price": ev.price}

            ret = _log_return(prev_valid_price, ev.price)
            if ret is not None:
                values["ret_1"] = ret

            for w in window_set:
                sma_val = _sma(list(prices), w)
                if sma_val is not None:
                    values[f"sma_{w}"] = sma_val

            missing = REQUIRED_KEYS - set(values.keys())
            if missing:
                dropped_invalid += 1
                continue
            if not all(_is_finite_price(v) for v in values.values()):
                dropped_invalid += 1
                continue

            values["window_max"] = float(effective_window)

            results.append(
                FeatureVector(
                    symbol=sym,
                    ts=ev.event_ts if isinstance(ev.event_ts, datetime) else datetime.fromisoformat(str(ev.event_ts)),
                    values=values,
                )
            )

            if ev.price > 0:
                prev_valid_price = ev.price

    if dropped_invalid:
        logger.info("features discarded", extra={"dropped": dropped_invalid})

    return results

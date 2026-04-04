"""Estrategia m?nima basada en retorno y SMA."""

from __future__ import annotations

from typing import Iterable, List

from app.common.dto import FeatureVector, Signal
from app.features.views import build_basic_strategy_view


def generate_signals(features: Iterable[FeatureVector]) -> List[Signal]:
    signals: List[Signal] = []
    for fv in features:
        view = build_basic_strategy_view(fv)
        price = view.price
        ret_1 = view.ret_1
        sma3 = view.sma_3
        if price is None or sma3 is None or ret_1 is None:
            side = "flat"
        elif ret_1 > 0 and price > sma3:
            side = "buy"
        elif ret_1 < 0 and price < sma3:
            side = "sell"
        else:
            side = "flat"

        size = 0.0 if side == "flat" else max(0.0, min(abs(ret_1), 1.0))
        signals.append(
            Signal(
                symbol=fv.symbol,
                ts=fv.ts,
                side=side,
                size=size,
                confidence=max(0.0, min(abs(ret_1) if ret_1 is not None else 0.0, 1.0)),
                metadata={
                    "feature_bundle_id": fv.lineage_id,
                    "feature_set_name": fv.feature_set_name,
                    "feature_set_version": fv.feature_set_version,
                },
            )
        )
    return signals

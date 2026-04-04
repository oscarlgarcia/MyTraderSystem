from __future__ import annotations

from dataclasses import dataclass

from app.common.dto import FeatureVector


@dataclass(frozen=True)
class BasicStrategyFeatureView:
    price: float | None
    ret_1: float | None
    sma_3: float | None


def build_basic_strategy_view(feature_vector: FeatureVector) -> BasicStrategyFeatureView:
    values = feature_vector.values
    return BasicStrategyFeatureView(
        price=values.get("price"),
        ret_1=values.get("ret_1"),
        sma_3=values.get("sma_3"),
    )

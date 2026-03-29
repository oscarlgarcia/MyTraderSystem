"""
Wrapper simple para calcular features sobre una lista de MarketEvent.
Se mantiene sin efectos colaterales y sin dependencias externas.
"""

from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.engine import FeatureEngine

logger = logging.getLogger("features.pipeline")


def run_feature_pipeline(
    events: Iterable[MarketEvent],
    window: int = 5,
    engine: Optional[FeatureEngine] = None,
) -> List[FeatureVector]:
    events_list = sorted(list(events), key=lambda e: (e.symbol, e.event_ts))
    eng = engine or FeatureEngine(window=window)
    features = eng.update_batch(events_list)
    logger.info(
        "feature pipeline done",
        extra={
            "events_in": len(events_list),
            "features_out": len(features),
            "window": window,
            "dropped_non_finite": eng.metrics["dropped_non_finite"],
            "latency_max": eng.metrics["compute_latency_max"],
            "latency_avg": eng.avg_latency(),
        },
    )
    return features

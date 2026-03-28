"""
Wrapper simple para calcular features sobre una lista de MarketEvent.
Se mantiene sin efectos colaterales y sin dependencias externas.
"""

from __future__ import annotations

import logging
from typing import Iterable, List

from app.common.dto import FeatureVector, MarketEvent
from app.features.store import compute_features

logger = logging.getLogger("features.pipeline")


def run_feature_pipeline(events: Iterable[MarketEvent], window: int = 5) -> List[FeatureVector]:
    events_list = list(events)
    features = compute_features(events_list, window=window)
    logger.info(
        "feature pipeline done",
        extra={"events_in": len(events_list), "features_out": len(features), "window": window},
    )
    return features

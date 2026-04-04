from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.features.serving import FeatureServingService, ServingResult


@dataclass(frozen=True)
class ShadowServingReport:
    pass_ok: bool
    primary: ServingResult
    shadow: ServingResult
    reason: str = ""


class ShadowServingService:
    def __init__(self, *, primary: FeatureServingService, shadow: FeatureServingService, tolerance: float = 1e-9) -> None:
        self.primary = primary
        self.shadow = shadow
        self.tolerance = tolerance

    def get_latest_servable(self, **kwargs) -> ShadowServingReport:
        primary = self.primary.get_latest_servable(**kwargs)
        shadow = self.shadow.get_latest_servable(**kwargs)
        if primary.status != shadow.status:
            return ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason="status_mismatch")
        if primary.feature_vector and shadow.feature_vector:
            names = sorted(set(primary.feature_vector.values) | set(shadow.feature_vector.values))
            for name in names:
                pv = primary.feature_vector.values.get(name)
                sv = shadow.feature_vector.values.get(name)
                if pv is None or sv is None:
                    return ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason=f"missing:{name}")
                if abs(float(pv) - float(sv)) > self.tolerance:
                    return ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason=f"value:{name}")
        return ShadowServingReport(pass_ok=True, primary=primary, shadow=shadow)

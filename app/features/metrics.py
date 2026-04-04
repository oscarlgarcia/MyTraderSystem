from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class FeatureMetrics:
    events_in: int = 0
    features_out: int = 0
    dropped_non_finite: int = 0
    transform_errors: int = 0
    compute_latency_total: float = 0.0
    compute_latency_max: float = 0.0
    serving_requests: int = 0
    serving_failures: int = 0
    parity_mismatches: int = 0
    stale_serves: int = 0

    def as_dict(self) -> Dict[str, float | int]:
        return {
            "events_in": self.events_in,
            "features_out": self.features_out,
            "dropped_non_finite": self.dropped_non_finite,
            "transform_errors": self.transform_errors,
            "compute_latency_total": self.compute_latency_total,
            "compute_latency_max": self.compute_latency_max,
            "serving_requests": self.serving_requests,
            "serving_failures": self.serving_failures,
            "parity_mismatches": self.parity_mismatches,
            "stale_serves": self.stale_serves,
        }

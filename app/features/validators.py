from __future__ import annotations

from collections import defaultdict, deque
import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

from app.common.dto import FeatureVector
from app.features.definitions import FeatureDefinition, FeatureSetDefinition


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    flags: Tuple[str, ...]


class FeatureValidator:
    def __init__(self, feature_set: FeatureSetDefinition) -> None:
        self.feature_set = feature_set
        self.by_name: Dict[str, FeatureDefinition] = {fd.name: fd for fd in feature_set.feature_definitions}
        self.history = defaultdict(lambda: deque(maxlen=128))

    def validate(self, fv: FeatureVector) -> ValidationResult:
        flags: List[str] = []
        for name, value in fv.values.items():
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                flags.append(f"{name}:non_finite")
                continue
            fd = self.by_name.get(name)
            if fd is None:
                continue
            policy = fd.validation_policy or {}
            if "min" in policy and value < policy["min"]:
                flags.append(f"{name}:below_min")
            if "max" in policy and value > policy["max"]:
                flags.append(f"{name}:above_max")
            if policy.get("non_zero") and value == 0:
                flags.append(f"{name}:zero_not_allowed")
            history = self.history[name]
            previous = history[-1] if history else None
            if previous is not None and "max_abs_delta" in policy:
                if abs(value - previous) > float(policy["max_abs_delta"]):
                    flags.append(f"{name}:delta_exceeded")
            history.append(float(value))
            constant_window = int(policy.get("constant_window", 0) or 0)
            if constant_window > 1 and len(history) >= constant_window:
                recent = list(history)[-constant_window:]
                if max(recent) == min(recent):
                    flags.append(f"{name}:constant_run")
            variance_window = int(policy.get("variance_window", 0) or 0)
            min_variance = policy.get("min_variance")
            if variance_window > 1 and min_variance is not None and len(history) >= variance_window:
                recent = list(history)[-variance_window:]
                mean = sum(recent) / len(recent)
                variance = sum((item - mean) ** 2 for item in recent) / len(recent)
                if variance < float(min_variance):
                    flags.append(f"{name}:low_variance")
        if fv.available_ts > fv.ts and "available_after_ts" not in flags:
            flags.append("available_after_ts")
        return ValidationResult(is_valid=not flags, flags=tuple(flags))

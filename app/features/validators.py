from __future__ import annotations

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
        if fv.available_ts > fv.ts and "available_after_ts" not in flags:
            flags.append("available_after_ts")
        return ValidationResult(is_valid=not flags, flags=tuple(flags))

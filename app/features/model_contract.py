from __future__ import annotations

from dataclasses import dataclass

from app.common.dto import FeatureVector


@dataclass(frozen=True)
class FeatureConsumerContract:
    consumer_name: str
    consumer_kind: str
    feature_set_name: str
    feature_set_version: str
    required_features: tuple[str, ...]
    required_metadata_keys: tuple[str, ...] = ()
    target: str = "paper"
    notes: str = ""


@dataclass(frozen=True)
class FeatureContractValidationResult:
    pass_ok: bool
    reasons: tuple[str, ...] = ()


def validate_feature_contract(
    *,
    contract: FeatureConsumerContract,
    feature_vector: FeatureVector,
    consumer_metadata: dict[str, str] | None = None,
) -> FeatureContractValidationResult:
    reasons: list[str] = []
    if feature_vector.feature_set_name != contract.feature_set_name:
        reasons.append("feature_set_name_mismatch")
    if feature_vector.feature_set_version != contract.feature_set_version:
        reasons.append("feature_set_version_mismatch")
    missing_features = [name for name in contract.required_features if name not in feature_vector.values]
    if missing_features:
        reasons.append(f"missing_features:{','.join(missing_features)}")
    metadata = consumer_metadata or {}
    missing_metadata = [key for key in contract.required_metadata_keys if key not in metadata]
    if missing_metadata:
        reasons.append(f"missing_metadata:{','.join(missing_metadata)}")
    if not feature_vector.lineage_id:
        reasons.append("missing_lineage_id")
    return FeatureContractValidationResult(pass_ok=not reasons, reasons=tuple(reasons))

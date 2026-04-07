from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
from app.features.training_bundle_registry import TrainingBundleRegistry

from app.common.dto import FeatureVector


@dataclass(frozen=True)
class FeatureConsumerContract:
    consumer_name: str
    consumer_kind: str
    feature_set_name: str
    feature_set_version: str
    required_features: tuple[str, ...]
    required_metadata_keys: tuple[str, ...] = ()
    required_entity_values: tuple[tuple[str, str], ...] = ()
    required_dataset_id: str = ""
    required_schema_hash: str = ""
    required_training_bundle_id: str = ""
    require_feature_bundle_match: bool = False
    target: str = "paper"
    notes: str = ""


@dataclass(frozen=True)
class FeatureContractValidationResult:
    pass_ok: bool
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class FeatureConsumerMetadata:
    feature_bundle_id: str = ""
    dataset_id: str = ""
    feature_schema_hash: str = ""
    training_bundle_id: str = ""
    consumer_name: str = ""
    consumer_kind: str = ""
    target: str = ""
    extra: dict[str, str] | None = None

    def as_dict(self) -> dict[str, str]:
        payload = {
            "feature_bundle_id": self.feature_bundle_id,
            "dataset_id": self.dataset_id,
            "feature_schema_hash": self.feature_schema_hash,
            "training_bundle_id": self.training_bundle_id,
            "consumer_name": self.consumer_name,
            "consumer_kind": self.consumer_kind,
            "target": self.target,
        }
        if self.extra:
            payload.update(self.extra)
        return {key: value for key, value in payload.items() if value}


def normalize_consumer_metadata(
    consumer_metadata: FeatureConsumerMetadata | Mapping[str, str] | None,
) -> dict[str, str]:
    if consumer_metadata is None:
        return {}
    if isinstance(consumer_metadata, FeatureConsumerMetadata):
        return consumer_metadata.as_dict()
    return {str(key): str(value) for key, value in consumer_metadata.items()}


def validate_feature_contract(
    *,
    contract: FeatureConsumerContract,
    feature_vector: FeatureVector,
    consumer_metadata: FeatureConsumerMetadata | Mapping[str, str] | None = None,
    training_bundle_registry: TrainingBundleRegistry | None = None,
) -> FeatureContractValidationResult:
    reasons: list[str] = []
    if feature_vector.feature_set_name != contract.feature_set_name:
        reasons.append("feature_set_name_mismatch")
    if feature_vector.feature_set_version != contract.feature_set_version:
        reasons.append("feature_set_version_mismatch")
    missing_features = [name for name in contract.required_features if name not in feature_vector.values]
    if missing_features:
        reasons.append(f"missing_features:{','.join(missing_features)}")
    metadata = normalize_consumer_metadata(consumer_metadata)
    missing_metadata = [key for key in contract.required_metadata_keys if key not in metadata]
    if missing_metadata:
        reasons.append(f"missing_metadata:{','.join(missing_metadata)}")
    if contract.require_feature_bundle_match:
        feature_bundle_id = metadata.get("feature_bundle_id")
        if not feature_bundle_id:
            reasons.append("missing_metadata:feature_bundle_id")
        elif feature_bundle_id != feature_vector.lineage_id:
            reasons.append("feature_bundle_id_mismatch")
    if contract.required_dataset_id:
        if metadata.get("dataset_id") != contract.required_dataset_id:
            reasons.append("dataset_id_mismatch")
    if contract.required_schema_hash:
        if metadata.get("feature_schema_hash") != contract.required_schema_hash:
            reasons.append("schema_hash_mismatch")
    if contract.required_training_bundle_id:
        training_bundle_id = metadata.get("training_bundle_id")
        if training_bundle_id != contract.required_training_bundle_id:
            reasons.append("training_bundle_id_mismatch")
        elif training_bundle_registry is not None:
            bundle = training_bundle_registry.get(training_bundle_id)
            if bundle is None:
                reasons.append("training_bundle_missing")
            else:
                if bundle.dataset_id != metadata.get("dataset_id"):
                    reasons.append("training_bundle_dataset_mismatch")
                if bundle.feature_schema_hash != metadata.get("feature_schema_hash"):
                    reasons.append("training_bundle_schema_mismatch")
                if bundle.feature_set_name != feature_vector.feature_set_name:
                    reasons.append("training_bundle_feature_set_mismatch")
                if bundle.feature_set_version != feature_vector.feature_set_version:
                    reasons.append("training_bundle_feature_version_mismatch")
    entity_mismatches = [
        key
        for key, expected_value in contract.required_entity_values
        if feature_vector.entity_keys.get(key) != expected_value
    ]
    if entity_mismatches:
        reasons.append(f"entity_key_mismatch:{','.join(entity_mismatches)}")
    if not feature_vector.lineage_id:
        reasons.append("missing_lineage_id")
    return FeatureContractValidationResult(pass_ok=not reasons, reasons=tuple(reasons))

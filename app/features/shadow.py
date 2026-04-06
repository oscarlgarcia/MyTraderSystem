from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.features.serving import FeatureServingService, ServingResult
from app.features.shadow_report_store import ShadowReportStore


@dataclass(frozen=True)
class ShadowServingReport:
    pass_ok: bool
    primary: ServingResult
    shadow: ServingResult
    reason: str = ""


class ShadowServingService:
    def __init__(
        self,
        *,
        primary: FeatureServingService,
        shadow: FeatureServingService,
        tolerance: float = 1e-9,
        report_store: ShadowReportStore | None = None,
    ) -> None:
        self.primary = primary
        self.shadow = shadow
        self.tolerance = tolerance
        self.report_store = report_store

    def get_latest_servable(self, **kwargs) -> ShadowServingReport:
        primary = self.primary.get_latest_servable(**kwargs)
        shadow = self.shadow.get_latest_servable(**kwargs)
        self.primary.metrics.shadow_requests += 1
        if primary.status != shadow.status:
            report = ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason="status_mismatch")
            self._persist_report(kwargs=kwargs, report=report)
            self.primary.metrics.shadow_failures += 1
            return report
        if primary.feature_vector and shadow.feature_vector:
            names = sorted(set(primary.feature_vector.values) | set(shadow.feature_vector.values))
            for name in names:
                pv = primary.feature_vector.values.get(name)
                sv = shadow.feature_vector.values.get(name)
                if pv is None or sv is None:
                    report = ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason=f"missing:{name}")
                    self._persist_report(kwargs=kwargs, report=report)
                    self.primary.metrics.shadow_failures += 1
                    return report
                if abs(float(pv) - float(sv)) > self.tolerance:
                    report = ShadowServingReport(pass_ok=False, primary=primary, shadow=shadow, reason=f"value:{name}")
                    self._persist_report(kwargs=kwargs, report=report)
                    self.primary.metrics.shadow_failures += 1
                    return report
        report = ShadowServingReport(pass_ok=True, primary=primary, shadow=shadow)
        self._persist_report(kwargs=kwargs, report=report)
        return report

    def _persist_report(self, *, kwargs, report: ShadowServingReport) -> None:
        if self.report_store is None:
            return
        decision_ts = kwargs["decision_ts"]
        self.report_store.append(
            symbol=kwargs.get("symbol") or (kwargs.get("entity_keys") or {}).get("symbol", ""),
            decision_ts=decision_ts,
            feature_set_name=kwargs["feature_set_name"],
            feature_set_version=kwargs.get("feature_set_version") or (
                report.primary.feature_vector.feature_set_version if report.primary.feature_vector else ""
            ),
            pass_ok=report.pass_ok,
            reason=report.reason,
        )

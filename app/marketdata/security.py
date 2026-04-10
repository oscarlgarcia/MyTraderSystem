from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.config import SECRET_ENV_PREFIX
from app.marketdata.serving import serving_db_path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class SecurityBaselineReport:
    env: str
    secret_env_prefix: str
    data_root: str
    catalog_root: str
    control_plane_root: str
    serving_db: str
    env_scoped_layout: bool
    generated_at: str


def security_baseline_report_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "security-baseline.json"


def build_security_baseline_report(base_dir: Path, env: str) -> SecurityBaselineReport:
    env_root = Path(base_dir) / env
    return SecurityBaselineReport(
        env=env,
        secret_env_prefix=SECRET_ENV_PREFIX,
        data_root=str(Path(base_dir)),
        catalog_root=str(env_root / "catalog"),
        control_plane_root=str(env_root / "control-plane"),
        serving_db=str(serving_db_path(base_dir, env)),
        env_scoped_layout=True,
        generated_at=_utc_now(),
    )


def write_security_baseline_report(path: Path, report: SecurityBaselineReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    return path

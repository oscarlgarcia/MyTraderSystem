from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from app.common.dto import FeatureVector
from app.features.pit import feature_vector_is_servable_at


class OfflineFeatureStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_vectors (
                    symbol TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(symbol, ts, feature_set_name, feature_set_version, lineage_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feature_vectors_pit ON feature_vectors(symbol, feature_set_name, feature_set_version, available_ts, ts)"
            )

    def put_many(self, vectors: Iterable[FeatureVector]) -> None:
        payload = [
            (
                fv.symbol,
                fv.ts.isoformat(),
                fv.available_ts.isoformat(),
                fv.feature_set_name,
                fv.feature_set_version,
                fv.lineage_id,
                json.dumps(list(fv.quality_flags), ensure_ascii=False),
                json.dumps(fv.values, ensure_ascii=False, sort_keys=True),
                json.dumps(fv.entity_keys, ensure_ascii=False, sort_keys=True),
            )
            for fv in vectors
        ]
        if not payload:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO feature_vectors (
                    symbol, ts, available_ts, feature_set_name, feature_set_version,
                    lineage_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()

    def get_point_in_time(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> Optional[FeatureVector]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, ts, available_ts, feature_set_name, feature_set_version, lineage_id, quality_flags, values_json, entity_keys_json
                FROM feature_vectors
                WHERE symbol = ?
                  AND feature_set_name = ?
                  AND feature_set_version = ?
                  AND available_ts <= ?
                  AND ts <= ?
                ORDER BY ts DESC, available_ts DESC
                LIMIT 1
                """,
                (symbol, feature_set_name, feature_set_version, decision_ts.isoformat(), decision_ts.isoformat()),
            ).fetchone()
        return self._row_to_vector(row) if row else None

    def get_range(self, *, symbol: str, start_ts: datetime, end_ts: datetime, feature_set_name: str, feature_set_version: str) -> List[FeatureVector]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, ts, available_ts, feature_set_name, feature_set_version, lineage_id, quality_flags, values_json, entity_keys_json
                FROM feature_vectors
                WHERE symbol = ? AND feature_set_name = ? AND feature_set_version = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
                """,
                (symbol, feature_set_name, feature_set_version, start_ts.isoformat(), end_ts.isoformat()),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def _row_to_vector(self, row) -> FeatureVector:
        symbol, ts, available_ts, fs_name, fs_version, lineage_id, quality_flags, values_json, entity_keys_json = row
        fv = FeatureVector(
            symbol=symbol,
            ts=datetime.fromisoformat(ts),
            available_ts=datetime.fromisoformat(available_ts),
            values=json.loads(values_json),
            feature_set_name=fs_name,
            feature_set_version=fs_version,
            lineage_id=lineage_id,
            quality_flags=tuple(json.loads(quality_flags)),
            entity_keys=json.loads(entity_keys_json),
        )
        return fv

    def assert_point_in_time_safe(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> None:
        fv = self.get_point_in_time(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if fv and not feature_vector_is_servable_at(fv, decision_ts):
            raise ValueError("offline store returned non-point-in-time-safe vector")

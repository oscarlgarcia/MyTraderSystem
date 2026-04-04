from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.common.dto import FeatureVector
from app.features.pit import feature_vector_is_servable_at


class OnlineFeatureStore:
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
                CREATE TABLE IF NOT EXISTS latest_vectors (
                    symbol TEXT NOT NULL,
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(symbol, feature_set_name, feature_set_version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_vectors (
                    symbol TEXT NOT NULL,
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(symbol, feature_set_name, feature_set_version, ts, lineage_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_history_vectors_lookup
                ON history_vectors(symbol, feature_set_name, feature_set_version, available_ts, ts)
                """
            )

    def upsert(self, fv: FeatureVector) -> None:
        if tuple(sorted(fv.entity_keys.keys())) != ("symbol",):
            raise ValueError("OnlineFeatureStore only supports symbol-scoped entity keys")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO latest_vectors (
                    symbol, feature_set_name, feature_set_version, ts, available_ts,
                    lineage_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fv.symbol,
                    fv.feature_set_name,
                    fv.feature_set_version,
                    fv.ts.isoformat(),
                    fv.available_ts.isoformat(),
                    fv.lineage_id,
                    json.dumps(list(fv.quality_flags), ensure_ascii=False),
                    json.dumps(fv.values, ensure_ascii=False, sort_keys=True),
                    json.dumps(fv.entity_keys, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO history_vectors (
                    symbol, feature_set_name, feature_set_version, ts, available_ts,
                    lineage_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fv.symbol,
                    fv.feature_set_name,
                    fv.feature_set_version,
                    fv.ts.isoformat(),
                    fv.available_ts.isoformat(),
                    fv.lineage_id,
                    json.dumps(list(fv.quality_flags), ensure_ascii=False),
                    json.dumps(fv.values, ensure_ascii=False, sort_keys=True),
                    json.dumps(fv.entity_keys, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def get_latest(self, *, symbol: str, feature_set_name: str, feature_set_version: str) -> Optional[FeatureVector]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM latest_vectors WHERE symbol=? AND feature_set_name=? AND feature_set_version=?
                """,
                (symbol, feature_set_name, feature_set_version),
            ).fetchone()
        if not row:
            return None
        symbol, fs_name, fs_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json = row
        return FeatureVector(
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

    def get_latest_servable(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> Optional[FeatureVector]:
        fv = self.get_latest(symbol=symbol, feature_set_name=feature_set_name, feature_set_version=feature_set_version)
        if fv is None:
            return None
        return fv if feature_vector_is_servable_at(fv, decision_ts) else None

    def get_recent_history(
        self,
        *,
        symbol: str,
        feature_set_name: str,
        feature_set_version: str,
        limit: int = 10,
    ) -> list[FeatureVector]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM history_vectors
                WHERE symbol=? AND feature_set_name=? AND feature_set_version=?
                ORDER BY ts DESC, available_ts DESC
                LIMIT ?
                """,
                (symbol, feature_set_name, feature_set_version, limit),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def get_history_range(
        self,
        *,
        symbol: str,
        feature_set_name: str,
        feature_set_version: str,
        start_ts: datetime,
        end_ts: datetime,
    ) -> list[FeatureVector]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM history_vectors
                WHERE symbol=? AND feature_set_name=? AND feature_set_version=? AND ts>=? AND ts<=?
                ORDER BY ts ASC, available_ts ASC
                """,
                (symbol, feature_set_name, feature_set_version, start_ts.isoformat(), end_ts.isoformat()),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def _row_to_vector(self, row) -> FeatureVector:
        symbol, fs_name, fs_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json = row
        return FeatureVector(
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

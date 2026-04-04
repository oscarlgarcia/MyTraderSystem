from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from app.common.dto import FeatureVector
from app.features.pit import feature_vector_is_servable_at


@dataclass(frozen=True)
class MaterializationRunRecord:
    run_id: str
    feature_set_name: str
    feature_set_version: str
    definition_hash: str
    input_fingerprint: str
    bundle_id: str
    row_count: int
    status: str
    created_at: datetime
    min_event_ts: datetime | None = None
    max_event_ts: datetime | None = None


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
                    source_cutoff_ts TEXT NOT NULL,
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    run_id TEXT NOT NULL DEFAULT '',
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(symbol, ts, feature_set_name, feature_set_version, lineage_id)
                )
                """
            )
            self._ensure_column(conn, "feature_vectors", "source_cutoff_ts", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "feature_vectors", "run_id", "TEXT NOT NULL DEFAULT ''")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feature_vectors_pit ON feature_vectors(symbol, feature_set_name, feature_set_version, available_ts, ts)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feature_vectors_run ON feature_vectors(run_id, symbol, ts)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS materialization_runs (
                    run_id TEXT PRIMARY KEY,
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    definition_hash TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    bundle_id TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    min_event_ts TEXT,
                    max_event_ts TEXT
                )
                """
            )

    def _ensure_column(self, conn, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def put_many(self, vectors: Iterable[FeatureVector], *, run_id: str = "") -> None:
        vectors = list(vectors)
        for fv in vectors:
            if tuple(sorted(fv.entity_keys.keys())) != ("symbol",):
                raise ValueError("OfflineFeatureStore only supports symbol-scoped entity keys")
        payload = [
            (
                fv.symbol,
                fv.ts.isoformat(),
                fv.available_ts.isoformat(),
                fv.source_cutoff_ts.isoformat(),
                fv.feature_set_name,
                fv.feature_set_version,
                fv.lineage_id,
                run_id,
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
                    symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version,
                    lineage_id, run_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()

    def register_materialization_run(self, record: MaterializationRunRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO materialization_runs (
                    run_id, feature_set_name, feature_set_version, definition_hash,
                    input_fingerprint, bundle_id, row_count, status, created_at,
                    min_event_ts, max_event_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.run_id,
                    record.feature_set_name,
                    record.feature_set_version,
                    record.definition_hash,
                    record.input_fingerprint,
                    record.bundle_id,
                    record.row_count,
                    record.status,
                    record.created_at.isoformat(),
                    record.min_event_ts.isoformat() if record.min_event_ts else None,
                    record.max_event_ts.isoformat() if record.max_event_ts else None,
                ),
            )
            conn.commit()

    def get_materialization_run(self, run_id: str) -> Optional[MaterializationRunRecord]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run_id, feature_set_name, feature_set_version, definition_hash,
                       input_fingerprint, bundle_id, row_count, status, created_at,
                       min_event_ts, max_event_ts
                FROM materialization_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return MaterializationRunRecord(
            run_id=row[0],
            feature_set_name=row[1],
            feature_set_version=row[2],
            definition_hash=row[3],
            input_fingerprint=row[4],
            bundle_id=row[5],
            row_count=int(row[6]),
            status=row[7],
            created_at=datetime.fromisoformat(row[8]),
            min_event_ts=datetime.fromisoformat(row[9]) if row[9] else None,
            max_event_ts=datetime.fromisoformat(row[10]) if row[10] else None,
        )

    def get_run_vectors(self, run_id: str) -> List[FeatureVector]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version,
                       lineage_id, quality_flags, values_json, entity_keys_json
                FROM feature_vectors
                WHERE run_id = ?
                ORDER BY symbol ASC, ts ASC, available_ts ASC
                """,
                (run_id,),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def get_point_in_time(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> Optional[FeatureVector]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version,
                       lineage_id, quality_flags, values_json, entity_keys_json
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
                SELECT symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version,
                       lineage_id, quality_flags, values_json, entity_keys_json
                FROM feature_vectors
                WHERE symbol = ? AND feature_set_name = ? AND feature_set_version = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
                """,
                (symbol, feature_set_name, feature_set_version, start_ts.isoformat(), end_ts.isoformat()),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def reconstruct_run(self, *, run_id: str, symbol: str | None = None, start_ts: datetime | None = None, end_ts: datetime | None = None) -> List[FeatureVector]:
        query = [
            """
            SELECT symbol, ts, available_ts, source_cutoff_ts, feature_set_name, feature_set_version,
                   lineage_id, quality_flags, values_json, entity_keys_json
            FROM feature_vectors
            WHERE run_id = ?
            """
        ]
        params: list[str] = [run_id]
        if symbol is not None:
            query.append("AND symbol = ?")
            params.append(symbol)
        if start_ts is not None:
            query.append("AND ts >= ?")
            params.append(start_ts.isoformat())
        if end_ts is not None:
            query.append("AND ts <= ?")
            params.append(end_ts.isoformat())
        query.append("ORDER BY symbol ASC, ts ASC, available_ts ASC")
        with self._connect() as conn:
            rows = conn.execute("\n".join(query), tuple(params)).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def _row_to_vector(self, row) -> FeatureVector:
        symbol, ts, available_ts, source_cutoff_ts, fs_name, fs_version, lineage_id, quality_flags, values_json, entity_keys_json = row
        fv = FeatureVector(
            symbol=symbol,
            ts=datetime.fromisoformat(ts),
            available_ts=datetime.fromisoformat(available_ts),
            source_cutoff_ts=datetime.fromisoformat(source_cutoff_ts) if source_cutoff_ts else datetime.fromisoformat(available_ts),
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

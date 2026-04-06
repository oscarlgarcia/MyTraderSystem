from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from app.common.dto import FeatureVector
from app.features.entity_codec import entity_scope, normalize_entity_keys, primary_symbol
from app.features.online_store_base import FeatureOnlineStore
from app.features.pit import feature_vector_is_servable_at


class OnlineFeatureStore(FeatureOnlineStore):
    def __init__(
        self,
        path: str | Path,
        *,
        history_max_rows_per_scope: int | None = None,
        history_retention_seconds: float | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.history_max_rows_per_scope = history_max_rows_per_scope
        self.history_retention_seconds = history_retention_seconds
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS latest_vectors (
                    symbol TEXT NOT NULL,
                    entity_scope TEXT NOT NULL DEFAULT '',
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(entity_scope, feature_set_name, feature_set_version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_vectors (
                    symbol TEXT NOT NULL,
                    entity_scope TEXT NOT NULL DEFAULT '',
                    feature_set_name TEXT NOT NULL,
                    feature_set_version TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    quality_flags TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    entity_keys_json TEXT NOT NULL,
                    PRIMARY KEY(entity_scope, feature_set_name, feature_set_version, ts, lineage_id)
                )
                """
            )
            self._ensure_column(conn, "latest_vectors", "entity_scope", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "history_vectors", "entity_scope", "TEXT NOT NULL DEFAULT ''")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_history_vectors_lookup
                ON history_vectors(symbol, feature_set_name, feature_set_version, available_ts, ts)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_history_vectors_scope_lookup
                ON history_vectors(entity_scope, feature_set_name, feature_set_version, available_ts, ts)
                """
            )

    def _ensure_column(self, conn, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def upsert(self, fv: FeatureVector) -> None:
        normalized_keys = normalize_entity_keys(fv.entity_keys, symbol=fv.symbol)
        scope = entity_scope(normalized_keys)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO latest_vectors (
                    symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts,
                    lineage_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    primary_symbol(normalized_keys, fallback_symbol=fv.symbol),
                    scope,
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
                    symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts,
                    lineage_id, quality_flags, values_json, entity_keys_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    primary_symbol(normalized_keys, fallback_symbol=fv.symbol),
                    scope,
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
        self.prune_history(
            symbol=fv.symbol,
            entity_keys=normalized_keys,
            feature_set_name=fv.feature_set_name,
            feature_set_version=fv.feature_set_version,
        )

    def _lookup_filter(
        self,
        *,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> tuple[str, tuple[str, ...]]:
        if entity_keys is not None:
            return ("entity_scope = ?", (entity_scope(entity_keys, symbol=symbol),))
        if symbol is None:
            raise ValueError("symbol or entity_keys is required")
        return ("symbol = ?", (primary_symbol({"symbol": symbol}),))

    def get_latest(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        lookup_sql, lookup_params = self._lookup_filter(symbol=symbol, entity_keys=entity_keys)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM latest_vectors WHERE """
                + lookup_sql
                + """ AND feature_set_name=? AND feature_set_version=?
                """,
                lookup_params + (feature_set_name, feature_set_version),
            ).fetchone()
        return self._row_to_vector(row) if row else None

    def get_latest_servable(
        self,
        *,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        fv = self.get_latest(
            symbol=symbol,
            entity_keys=entity_keys,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if fv is None:
            return None
        return fv if feature_vector_is_servable_at(fv, decision_ts) else None

    def get_recent_history(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        limit: int = 10,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        lookup_sql, lookup_params = self._lookup_filter(symbol=symbol, entity_keys=entity_keys)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM history_vectors
                WHERE """
                + lookup_sql
                + """ AND feature_set_name=? AND feature_set_version=?
                ORDER BY ts DESC, available_ts DESC
                LIMIT ?
                """,
                lookup_params + (feature_set_name, feature_set_version, limit),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def get_history_range(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        start_ts: datetime,
        end_ts: datetime,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        lookup_sql, lookup_params = self._lookup_filter(symbol=symbol, entity_keys=entity_keys)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM history_vectors
                WHERE """
                + lookup_sql
                + """ AND feature_set_name=? AND feature_set_version=? AND ts>=? AND ts<=?
                ORDER BY ts ASC, available_ts ASC
                """,
                lookup_params + (feature_set_name, feature_set_version, start_ts.isoformat(), end_ts.isoformat()),
            ).fetchall()
        return [self._row_to_vector(row) for row in rows]

    def get_snapshot_before(
        self,
        *,
        cutoff_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        lookup_sql, lookup_params = self._lookup_filter(symbol=symbol, entity_keys=entity_keys)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, entity_scope, feature_set_name, feature_set_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json
                FROM history_vectors
                WHERE """
                + lookup_sql
                + """ AND feature_set_name=? AND feature_set_version=? AND available_ts<=? AND ts<=?
                ORDER BY ts DESC, available_ts DESC
                LIMIT 1
                """,
                lookup_params + (feature_set_name, feature_set_version, cutoff_ts.isoformat(), cutoff_ts.isoformat()),
            ).fetchone()
        return self._row_to_vector(row) if row else None

    def prune_history(
        self,
        *,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
        feature_set_name: str | None = None,
        feature_set_version: str | None = None,
        now: datetime | None = None,
    ) -> int:
        total_deleted = 0
        now = now or datetime.utcnow().astimezone()
        filters = []
        params: list[str] = []
        if entity_keys is not None:
            filters.append("entity_scope = ?")
            params.append(entity_scope(entity_keys, symbol=symbol))
        elif symbol is not None:
            filters.append("symbol = ?")
            params.append(primary_symbol({"symbol": symbol}))
        if feature_set_name is not None:
            filters.append("feature_set_name = ?")
            params.append(feature_set_name)
        if feature_set_version is not None:
            filters.append("feature_set_version = ?")
            params.append(feature_set_version)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        with self._connect() as conn:
            if self.history_retention_seconds is not None:
                cutoff = now - timedelta(seconds=float(self.history_retention_seconds))
                retention_where = f"{where} {'AND' if where else 'WHERE'} available_ts < ?"
                cursor = conn.execute(
                    f"DELETE FROM history_vectors {retention_where}",
                    tuple(params + [cutoff.isoformat()]),
                )
                total_deleted += cursor.rowcount or 0
            if self.history_max_rows_per_scope is not None and self.history_max_rows_per_scope > 0:
                rows = conn.execute(
                    f"""
                    SELECT symbol, entity_scope, feature_set_name, feature_set_version, ts, lineage_id
                    FROM history_vectors
                    {where}
                    ORDER BY entity_scope ASC, feature_set_name ASC, feature_set_version ASC, ts DESC, available_ts DESC
                    """,
                    tuple(params),
                ).fetchall()
                grouped: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
                for _, row_scope, row_name, row_version, row_ts, row_lineage in rows:
                    grouped.setdefault((row_scope, row_name, row_version), []).append((row_ts, row_lineage))
                delete_payload = []
                for (row_scope, row_name, row_version), entries in grouped.items():
                    for row_ts, row_lineage in entries[self.history_max_rows_per_scope :]:
                        delete_payload.append((row_scope, row_name, row_version, row_ts, row_lineage))
                if delete_payload:
                    cursor = conn.executemany(
                        """
                        DELETE FROM history_vectors
                        WHERE entity_scope=? AND feature_set_name=? AND feature_set_version=? AND ts=? AND lineage_id=?
                        """,
                        delete_payload,
                    )
                    total_deleted += cursor.rowcount or 0
            conn.commit()
        return total_deleted

    def _row_to_vector(self, row) -> FeatureVector:
        symbol, _, fs_name, fs_version, ts, available_ts, lineage_id, quality_flags, values_json, entity_keys_json = row
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

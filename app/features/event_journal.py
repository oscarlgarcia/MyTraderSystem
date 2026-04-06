from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from app.common.dto import MarketEvent
from app.features.entity_codec import entity_scope, normalize_entity_keys


def _event_hash(event: MarketEvent, *, scope: str) -> str:
    payload = {
        "scope": scope,
        "symbol": event.symbol,
        "event_ts": event.event_ts.isoformat(),
        "available_ts": event.available_ts.isoformat() if event.available_ts else "",
        "published_ts": event.published_ts.isoformat() if event.published_ts else "",
        "price": event.price,
        "size": event.size,
        "source": event.source,
        "metadata": dict(sorted(event.metadata.items())),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class JournaledEvent:
    scope: str
    event: MarketEvent


class FeatureEventJournal:
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
                CREATE TABLE IF NOT EXISTS feature_event_journal (
                    event_hash TEXT PRIMARY KEY,
                    entity_scope TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_ts TEXT NOT NULL,
                    available_ts TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_feature_event_journal_scope
                ON feature_event_journal(entity_scope, available_ts, event_ts)
                """
            )

    def append(self, event: MarketEvent, *, entity_keys: dict[str, str] | None = None) -> str:
        normalized = normalize_entity_keys(entity_keys, symbol=event.symbol)
        scope = entity_scope(normalized)
        payload_json = self._serialize_event(event)
        event_hash = _event_hash(event, scope=scope)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO feature_event_journal (
                    event_hash, entity_scope, symbol, event_ts, available_ts, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_hash,
                    scope,
                    normalized["symbol"],
                    event.event_ts.isoformat(),
                    event.available_ts.isoformat() if event.available_ts else event.event_ts.isoformat(),
                    payload_json,
                ),
            )
            conn.commit()
        return scope

    def append_many(self, events: Iterable[tuple[MarketEvent, dict[str, str] | None]]) -> None:
        with self._connect() as conn:
            for event, entity_keys in events:
                normalized = normalize_entity_keys(entity_keys, symbol=event.symbol)
                scope = entity_scope(normalized)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO feature_event_journal (
                        event_hash, entity_scope, symbol, event_ts, available_ts, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _event_hash(event, scope=scope),
                        scope,
                        normalized["symbol"],
                        event.event_ts.isoformat(),
                        event.available_ts.isoformat() if event.available_ts else event.event_ts.isoformat(),
                        self._serialize_event(event),
                    ),
                )
            conn.commit()

    def load_scope_events(self, scope: str) -> list[MarketEvent]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM feature_event_journal
                WHERE entity_scope = ?
                ORDER BY available_ts ASC, event_ts ASC
                """,
                (scope,),
            ).fetchall()
        return [self._deserialize_event(row[0]) for row in rows]

    def _serialize_event(self, event: MarketEvent) -> str:
        return json.dumps(
            {
                "symbol": event.symbol,
                "event_ts": event.event_ts.isoformat(),
                "price": event.price,
                "size": event.size,
                "source": event.source,
                "metadata": event.metadata,
                "published_ts": event.published_ts.isoformat() if event.published_ts else None,
                "available_ts": event.available_ts.isoformat() if event.available_ts else None,
                "processed_ts": event.processed_ts.isoformat() if event.processed_ts else None,
                "observation_ts": event.observation_ts.isoformat() if event.observation_ts else None,
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def _deserialize_event(self, payload_json: str) -> MarketEvent:
        payload = json.loads(payload_json)
        return MarketEvent(
            symbol=payload["symbol"],
            event_ts=datetime.fromisoformat(payload["event_ts"]),
            price=payload["price"],
            size=payload["size"],
            source=payload["source"],
            metadata=payload.get("metadata", {}),
            published_ts=datetime.fromisoformat(payload["published_ts"]) if payload.get("published_ts") else None,
            available_ts=datetime.fromisoformat(payload["available_ts"]) if payload.get("available_ts") else None,
            processed_ts=datetime.fromisoformat(payload["processed_ts"]) if payload.get("processed_ts") else None,
            observation_ts=datetime.fromisoformat(payload["observation_ts"]) if payload.get("observation_ts") else None,
        )

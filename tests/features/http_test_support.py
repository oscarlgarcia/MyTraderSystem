from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from app.features.online_store_codec import serialize_feature_vector
from app.features.online_store_memory import MemoryOnlineFeatureStore


class _FeatureHttpHandler(BaseHTTPRequestHandler):
    store = MemoryOnlineFeatureStore()
    observability_payloads: list[dict[str, object]] = []

    def log_message(self, format, *args):  # noqa: A003
        return

    def _read_json(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8"))

    def _query(self) -> dict[str, str]:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        return {key: values[-1] for key, values in query.items()}

    def _entity_keys(self, query: dict[str, str]) -> dict[str, str] | None:
        payload = query.get("entity_keys")
        return json.loads(payload) if payload else None

    def _write_json(self, status: int, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/vectors":
            from app.features.online_store_codec import deserialize_feature_vector

            payload = self._read_json()
            vector = deserialize_feature_vector(dict(payload["vector"]))
            self.store.upsert(vector)
            self._write_json(200, {"status": "ok"})
            return
        if parsed.path == "/observability":
            payload = self._read_json()
            self.observability_payloads.append(payload)
            self._write_json(200, {"status": "ok"})
            return
        self._write_json(404, {"error": "not_found"})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = self._query()
        kwargs = {
            "symbol": query.get("symbol"),
            "entity_keys": self._entity_keys(query),
            "feature_set_name": query["feature_set_name"],
            "feature_set_version": query["feature_set_version"],
        }
        if parsed.path == "/vectors/latest":
            vector = self.store.get_latest(**kwargs)
            if vector is None:
                self._write_json(404, {"vector": None})
                return
            self._write_json(200, {"vector": serialize_feature_vector(vector)})
            return
        if parsed.path == "/vectors/latest_servable":
            vector = self.store.get_latest_servable(decision_ts=self._parse_ts(query["decision_ts"]), **kwargs)
            if vector is None:
                self._write_json(404, {"vector": None})
                return
            self._write_json(200, {"vector": serialize_feature_vector(vector)})
            return
        if parsed.path == "/vectors/history/recent":
            vectors = self.store.get_recent_history(limit=int(query.get("limit", "10")), **kwargs)
            self._write_json(200, {"vectors": [serialize_feature_vector(item) for item in vectors]})
            return
        if parsed.path == "/vectors/history/range":
            vectors = self.store.get_history_range(
                start_ts=self._parse_ts(query["start_ts"]),
                end_ts=self._parse_ts(query["end_ts"]),
                **kwargs,
            )
            self._write_json(200, {"vectors": [serialize_feature_vector(item) for item in vectors]})
            return
        if parsed.path == "/vectors/snapshot_before":
            vector = self.store.get_snapshot_before(cutoff_ts=self._parse_ts(query["cutoff_ts"]), **kwargs)
            if vector is None:
                self._write_json(404, {"vector": None})
                return
            self._write_json(200, {"vector": serialize_feature_vector(vector)})
            return
        self._write_json(404, {"error": "not_found"})

    @staticmethod
    def _parse_ts(value: str):
        from datetime import datetime

        return datetime.fromisoformat(value)


@contextmanager
def feature_http_server():
    _FeatureHttpHandler.store = MemoryOnlineFeatureStore()
    _FeatureHttpHandler.observability_payloads = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FeatureHttpHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, _FeatureHttpHandler
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

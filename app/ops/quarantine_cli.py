from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.ingestion.client import parse_message_parts, parse_typed_message
from app.ingestion.storage import ParquetWriter, normalized_partition_path, validate_output_path
from app.marketdata.connectors import normalize_binance_event
from app.marketdata.models import IngestionEvent
from app.marketdata.validators import validate_ingestion_event


DEFAULT_DQL_FILENAME = "ingestion-dlq.jsonl"
DEFAULT_SCHEMA_DRIFT_FILENAME = "schema-drift-quarantine.jsonl"
DEFAULT_MARKETDATA_ANOMALY_FILENAME = "marketdata-anomaly-quarantine.jsonl"


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    source_file: Path
    line_no: int
    ts: str | None
    error_category: str | None
    error_severity: str | None
    error_type: str | None
    error_message: str | None
    raw_message: object
    context: dict[str, object]
    symbol: str | None
    stream_type: str | None
    trace_id: str | None

    @property
    def record_id(self) -> str:
        return f"{self.source_file.name}:{self.line_no}"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    record_id: str
    source_file: str
    line_no: int
    stream_type: str | None
    symbol: str | None
    trace_id: str | None
    status: str
    error_type: str | None = None
    error_message: str | None = None
    normalized_partition_path: str | None = None


@dataclass(frozen=True, slots=True)
class ReplayReport:
    ts: str
    inspected_records: int
    replayed_records: int
    failed_records: int
    normalized_modified: bool
    persisted_events: int
    touched_partitions: tuple[str, ...]
    results: tuple[ReplayResult, ...]


def default_quarantine_paths(base_dir: Path) -> tuple[Path, ...]:
    root = validate_output_path(base_dir)
    return (
        root / "errors" / DEFAULT_DQL_FILENAME,
        root / "errors" / DEFAULT_SCHEMA_DRIFT_FILENAME,
        root / "errors" / DEFAULT_MARKETDATA_ANOMALY_FILENAME,
    )


def _safe_load_json_lines(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    if not path.exists():
        return ()
    rows: list[tuple[int, dict[str, Any]]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if not isinstance(payload, dict):
            continue
        rows.append((line_no, payload))
    return tuple(rows)


def _infer_symbol_stream(raw_message: object, context: dict[str, object]) -> tuple[str | None, str | None]:
    symbol = context.get("symbol")
    stream_type = context.get("stream_type")
    if isinstance(symbol, str) and isinstance(stream_type, str):
        return symbol.upper(), stream_type
    if isinstance(raw_message, str):
        try:
            _payload, data, _stream, event_type = parse_message_parts(raw_message)
        except Exception:
            return _coerce_opt_str(symbol), _coerce_opt_str(stream_type)
        return str(data.get("s", symbol)).upper(), str(event_type)
    if isinstance(raw_message, dict):
        data = raw_message.get("data", raw_message)
        if isinstance(data, dict):
            event_type = data.get("e")
            if not event_type:
                event_type = "kline" if "k" in data else "trade"
            return str(data.get("s", symbol)).upper(), str(event_type)
    return _coerce_opt_str(symbol), _coerce_opt_str(stream_type)


def _coerce_opt_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def load_quarantine_records(
    *,
    paths: Iterable[Path],
    trace_id: str | None = None,
    symbol: str | None = None,
    stream_type: str | None = None,
    record_id: str | None = None,
    limit: int | None = None,
) -> list[QuarantineRecord]:
    normalized_symbol = symbol.upper() if symbol else None
    normalized_stream = stream_type.lower() if stream_type else None
    records: list[QuarantineRecord] = []
    for path in paths:
        for line_no, payload in _safe_load_json_lines(path):
            context = payload.get("context", {})
            if not isinstance(context, dict):
                context = {}
            inferred_symbol, inferred_stream = _infer_symbol_stream(payload.get("raw_message"), context)
            row = QuarantineRecord(
                source_file=path,
                line_no=line_no,
                ts=_coerce_opt_str(payload.get("ts")),
                error_category=_coerce_opt_str(payload.get("error_category")),
                error_severity=_coerce_opt_str(payload.get("error_severity")),
                error_type=_coerce_opt_str(payload.get("error_type")),
                error_message=_coerce_opt_str(payload.get("error_message")),
                raw_message=payload.get("raw_message"),
                context=context,
                symbol=inferred_symbol,
                stream_type=inferred_stream,
                trace_id=_coerce_opt_str(context.get("trace_id")),
            )
            if trace_id and row.trace_id != trace_id:
                continue
            if normalized_symbol and row.symbol != normalized_symbol:
                continue
            if normalized_stream and (row.stream_type or "").lower() != normalized_stream:
                continue
            if record_id and row.record_id != record_id:
                continue
            records.append(row)
            if limit is not None and len(records) >= limit:
                return records
    return records


def list_quarantine_records(
    *,
    base_dir: Path,
    trace_id: str | None = None,
    symbol: str | None = None,
    stream_type: str | None = None,
    record_id: str | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    records = load_quarantine_records(
        paths=default_quarantine_paths(base_dir),
        trace_id=trace_id,
        symbol=symbol,
        stream_type=stream_type,
        record_id=record_id,
        limit=limit,
    )
    return [
        {
            "record_id": row.record_id,
            "source_file": str(row.source_file),
            "line_no": row.line_no,
            "ts": row.ts,
            "trace_id": row.trace_id,
            "symbol": row.symbol,
            "stream_type": row.stream_type,
            "error_type": row.error_type,
            "error_category": row.error_category,
            "error_severity": row.error_severity,
            "error_message": row.error_message,
        }
        for row in records
    ]


def _replay_event_from_record(record: QuarantineRecord) -> IngestionEvent:
    raw_message = record.raw_message
    receive_ts = datetime.now(timezone.utc)
    if isinstance(raw_message, str):
        return parse_typed_message(raw_message, receive_ts=receive_ts, process_ts=receive_ts)
    if isinstance(raw_message, dict):
        if "data" in raw_message or "stream" in raw_message:
            return parse_typed_message(json.dumps(raw_message), receive_ts=receive_ts, process_ts=receive_ts)
        if not record.stream_type:
            raise ValueError(f"cannot infer stream type for {record.record_id}")
        event = normalize_binance_event(
            record.stream_type,
            raw_message,
            receive_ts=receive_ts,
            process_ts=receive_ts,
        )
        validate_ingestion_event(event)
        return event
    raise TypeError(f"unsupported quarantine raw_message type: {type(raw_message)!r}")


def replay_quarantine_records(
    *,
    base_dir: Path,
    env: str,
    trace_id: str | None = None,
    symbol: str | None = None,
    stream_type: str | None = None,
    record_id: str | None = None,
    limit: int | None = None,
    write_normalized: bool = False,
    report_path: Path | None = None,
) -> ReplayReport:
    data_root = validate_output_path(base_dir)
    records = load_quarantine_records(
        paths=default_quarantine_paths(data_root),
        trace_id=trace_id,
        symbol=symbol,
        stream_type=stream_type,
        record_id=record_id,
        limit=limit,
    )
    writer = ParquetWriter(data_root, env=env, dedup=True) if write_normalized else None
    touched_partitions: set[str] = set()
    results: list[ReplayResult] = []
    replayed_records = 0
    failed_records = 0
    for record in records:
        try:
            event = _replay_event_from_record(record)
            partition_path_value: str | None = None
            if writer is not None:
                partition_path = normalized_partition_path(
                    data_root,
                    env,
                    source=event.source,
                    symbol=event.symbol,
                    day=event.event_ts.date().isoformat(),
                    venue=getattr(event, "venue", "BINANCE"),
                )
                partition_path_value = str(partition_path)
                writer.add(event)
                touched_partitions.add(str(partition_path))
            replayed_records += 1
            results.append(
                ReplayResult(
                    record_id=record.record_id,
                    source_file=str(record.source_file),
                    line_no=record.line_no,
                    stream_type=record.stream_type,
                    symbol=record.symbol,
                    trace_id=record.trace_id,
                    status="replayed",
                    normalized_partition_path=partition_path_value,
                )
            )
        except Exception as exc:
            failed_records += 1
            results.append(
                ReplayResult(
                    record_id=record.record_id,
                    source_file=str(record.source_file),
                    line_no=record.line_no,
                    stream_type=record.stream_type,
                    symbol=record.symbol,
                    trace_id=record.trace_id,
                    status="failed",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )
    persisted_events = 0
    if writer is not None:
        writer.flush()
        persisted_events = writer.persisted_events
    report = ReplayReport(
        ts=datetime.now(timezone.utc).isoformat(),
        inspected_records=len(records),
        replayed_records=replayed_records,
        failed_records=failed_records,
        normalized_modified=bool(write_normalized and persisted_events > 0),
        persisted_events=persisted_events,
        touched_partitions=tuple(sorted(touched_partitions)),
        results=tuple(results),
    )
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    **asdict(report),
                    "results": [asdict(result) for result in report.results],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return report


def _render_list(records: list[dict[str, object]]) -> str:
    return json.dumps({"records": records, "count": len(records)}, ensure_ascii=False, indent=2)


def _render_replay(report: ReplayReport) -> str:
    payload = {
        **asdict(report),
        "results": [asdict(result) for result in report.results],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and replay ingestion quarantine/DLQ payloads.")
    parser.add_argument("--base-dir", default=".", help="Base data directory that contains errors/ and normalized/.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List quarantined records with optional filters.")
    for subparser in (list_parser,):
        subparser.add_argument("--trace-id", default=None)
        subparser.add_argument("--symbol", default=None)
        subparser.add_argument("--stream-type", default=None)
        subparser.add_argument("--record-id", default=None)
        subparser.add_argument("--limit", type=int, default=None)

    replay_parser = subparsers.add_parser("replay", help="Replay quarantined records after payload fixes.")
    replay_parser.add_argument("--env", default="dev")
    replay_parser.add_argument("--trace-id", default=None)
    replay_parser.add_argument("--symbol", default=None)
    replay_parser.add_argument("--stream-type", default=None)
    replay_parser.add_argument("--record-id", default=None)
    replay_parser.add_argument("--limit", type=int, default=None)
    replay_parser.add_argument("--write-normalized", action="store_true")
    replay_parser.add_argument("--report-path", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    base_dir = Path(args.base_dir)
    if args.command == "list":
        records = list_quarantine_records(
            base_dir=base_dir,
            trace_id=args.trace_id,
            symbol=args.symbol,
            stream_type=args.stream_type,
            record_id=args.record_id,
            limit=args.limit,
        )
        print(_render_list(records))
        return 0

    report = replay_quarantine_records(
        base_dir=base_dir,
        env=args.env,
        trace_id=args.trace_id,
        symbol=args.symbol,
        stream_type=args.stream_type,
        record_id=args.record_id,
        limit=args.limit,
        write_normalized=bool(args.write_normalized),
        report_path=Path(args.report_path) if args.report_path else None,
    )
    print(_render_replay(report))
    return 0 if report.failed_records == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

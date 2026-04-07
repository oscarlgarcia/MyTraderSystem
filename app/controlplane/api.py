from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
import uvicorn

from app.config import AppConfig, load_config
from app.controlplane.builder import ReadModelBuilder
from app.controlplane.models import CommandAuditRecord, CommandRequestRecord
from app.controlplane.store import ControlPlaneStore
from app.controlplane.store_factory import create_control_plane_store
from app.controlplane.telemetry import configure_control_plane_telemetry, emit_control_plane_event


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class AppServices:
    cfg: AppConfig
    store: ControlPlaneStore
    builder: ReadModelBuilder
    templates: Jinja2Templates


def build_app(
    cfg: AppConfig | None = None,
    *,
    store: ControlPlaneStore | None = None,
    builder: ReadModelBuilder | None = None,
) -> FastAPI:
    cfg = cfg or load_config()
    configure_control_plane_telemetry(cfg.control_plane_telemetry_dir)
    store = store or create_control_plane_store(cfg)
    builder = builder or ReadModelBuilder(cfg.control_plane_telemetry_dir, store)
    templates = Jinja2Templates(directory=str(Path(__file__).with_name("templates")))
    services = AppServices(cfg=cfg, store=store, builder=builder, templates=templates)
    app = FastAPI(title="MyTraderSystem Control Plane", version="0.1.0")
    app.state.services = services

    def sync() -> AppServices:
        services.builder.sync_once()
        return services

    @app.get("/", response_class=HTMLResponse)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/ui/overview", status_code=302)

    @app.get("/ui/overview", response_class=HTMLResponse)
    def ui_overview(request: Request) -> HTMLResponse:
        current = sync()
        overview = current.store.overview()
        return current.templates.TemplateResponse(
            request,
            "overview.html",
            {
                "cfg": current.cfg,
                "overview": overview,
                "refresh_seconds": current.cfg.control_plane_poll_interval_seconds,
            },
        )

    @app.get("/ui/runs", response_class=HTMLResponse)
    def ui_runs(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "runs.html",
            {
                "cfg": current.cfg,
                "runs": current.store.list_runs(limit=100),
            },
        )

    @app.get("/ui/runs/{run_id}", response_class=HTMLResponse)
    def ui_run_detail(run_id: str, request: Request) -> HTMLResponse:
        current = sync()
        run = current.store.get_run(run_id)
        return current.templates.TemplateResponse(
            request,
            "run_detail.html",
            {
                "cfg": current.cfg,
                "run": run,
            },
        )

    @app.get("/ui/streams", response_class=HTMLResponse)
    def ui_streams(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "streams.html",
            {
                "cfg": current.cfg,
                "streams": current.store.list_streams(),
            },
        )

    @app.get("/ui/streams/{scope}", response_class=HTMLResponse)
    def ui_stream_detail(scope: str, request: Request) -> HTMLResponse:
        current = sync()
        stream = current.store.get_stream(scope)
        return current.templates.TemplateResponse(
            request,
            "stream_detail.html",
            {
                "cfg": current.cfg,
                "stream": stream,
            },
        )

    @app.get("/ui/alerts", response_class=HTMLResponse)
    def ui_alerts(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "alerts.html",
            {
                "cfg": current.cfg,
                "alerts": current.store.list_alerts(),
            },
        )

    @app.get("/ui/checkpoints", response_class=HTMLResponse)
    def ui_checkpoints(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "checkpoints.html",
            {
                "cfg": current.cfg,
                "checkpoints": current.store.list_checkpoints(),
            },
        )

    @app.get("/ui/audit", response_class=HTMLResponse)
    def ui_audit(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "audit.html",
            {
                "cfg": current.cfg,
                "commands": current.store.list_command_audit(limit=200),
            },
        )

    @app.get("/ui/recovery", response_class=HTMLResponse)
    def ui_recovery(request: Request) -> HTMLResponse:
        current = sync()
        return current.templates.TemplateResponse(
            request,
            "recovery.html",
            {
                "cfg": current.cfg,
            },
        )

    @app.post("/api/commands/ack-alert")
    async def api_ack_alert(
        request: Request,
        alert_id: str = Form(...),
        requested_by: str = Form("ui-operator"),
    ) -> Response:
        current = sync()
        record = _enqueue_command(
            current.store,
            command_type="ack_alert",
            scope=f"alert:{alert_id}",
            payload={"alert_id": alert_id},
            requested_by=requested_by,
        )
        return _command_response(request, record)

    @app.post("/api/commands/resync")
    async def api_resync_stream(
        request: Request,
        symbol: str = Form(...),
        stream_type: str = Form("kline"),
        interval: str = Form("1m"),
        start_ts: str = Form(""),
        end_ts: str = Form(""),
        limit: str = Form(""),
        write_normalized: str = Form("true"),
        requested_by: str = Form("ui-operator"),
    ) -> Response:
        current = sync()
        record = _enqueue_command(
            current.store,
            command_type="resync_stream",
            scope=f"BINANCE:{symbol.upper()}:{stream_type}",
            payload={
                "symbol": symbol.upper(),
                "stream_type": stream_type,
                "interval": interval,
                "start_ts": start_ts or None,
                "end_ts": end_ts or None,
                "limit": limit or None,
                "write_normalized": str(write_normalized).lower() in {"1", "true", "yes", "on"},
            },
            requested_by=requested_by,
        )
        return _command_response(request, record)

    @app.post("/api/commands/replay")
    async def api_replay_range(
        request: Request,
        symbol: str = Form(""),
        stream_type: str = Form("trade"),
        start_ts: str = Form(""),
        end_ts: str = Form(""),
        mode: str = Form("range"),
        trace_id: str = Form(""),
        record_id: str = Form(""),
        limit: str = Form(""),
        write_normalized: str = Form("true"),
        requested_by: str = Form("ui-operator"),
    ) -> Response:
        current = sync()
        scope = f"BINANCE:{symbol.upper()}:{stream_type}" if symbol else f"trace:{trace_id or record_id or 'quarantine'}"
        record = _enqueue_command(
            current.store,
            command_type="replay_range",
            scope=scope,
            payload={
                "mode": mode,
                "symbol": symbol.upper() if symbol else None,
                "stream_type": stream_type or None,
                "start_ts": start_ts or None,
                "end_ts": end_ts or None,
                "trace_id": trace_id or None,
                "record_id": record_id or None,
                "limit": limit or None,
                "write_normalized": str(write_normalized).lower() in {"1", "true", "yes", "on"},
            },
            requested_by=requested_by,
        )
        return _command_response(request, record)

    @app.get("/api/commands/{command_id}")
    def api_command_status(command_id: str) -> JSONResponse:
        current = sync()
        command = current.store.get_command(command_id)
        if command is None:
            return JSONResponse({"error": "command_not_found", "command_id": command_id}, status_code=404)
        return JSONResponse(
            {
                "command_id": command.command_id,
                "command_type": command.command_type,
                "scope": command.scope,
                "requested_by": command.requested_by,
                "requested_at": command.requested_at,
                "status": command.status,
                "started_at": command.started_at,
                "finished_at": command.finished_at,
                "result_summary": command.result_summary,
                "error_summary": command.error_summary,
                "payload": command.payload,
            }
        )

    return app


def _enqueue_command(
    store: ControlPlaneStore,
    *,
    command_type: str,
    scope: str,
    payload: dict,
    requested_by: str,
) -> CommandRequestRecord:
    command_id = str(uuid4())
    record = CommandRequestRecord(
        command_id=command_id,
        command_type=command_type,
        scope=scope,
        payload=dict(payload),
        requested_by=requested_by,
        requested_at=_utc_now(),
    )
    store.enqueue_command(record)
    store.append_command_audit(
        CommandAuditRecord(
            command_id=command_id,
            event_ts=record.requested_at,
            event_type="queued",
            payload={"command_type": command_type, "scope": scope, "requested_by": requested_by},
        )
    )
    emit_control_plane_event(
        "recovery_command_audit",
        {
            "command_id": command_id,
            "command_type": command_type,
            "scope": scope,
            "requested_by": requested_by,
            "status": "pending",
            "requested_at": record.requested_at,
        },
    )
    return record


def _command_response(request: Request, record: CommandRequestRecord) -> Response:
    if request.headers.get("HX-Request") == "true":
        body = (
            f"<div class='command-feedback success'>"
            f"Command queued: <code>{record.command_id}</code> ({record.command_type})"
            f"</div>"
        )
        return HTMLResponse(body)
    return JSONResponse(
        {
            "command_id": record.command_id,
            "command_type": record.command_type,
            "scope": record.scope,
            "status": record.status,
            "requested_at": record.requested_at,
        },
        status_code=202,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Control-plane web API for ingestion UI.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    return parser


app = build_app()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    uvicorn.run("app.controlplane.api:app", host=args.host, port=args.port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

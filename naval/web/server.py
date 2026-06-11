"""Naval Battle Web GUI — FastAPI server.

`FleetService`（naval/service.py）を HTTP API として公開し、
`naval/web/static/` のブラウザ GUI を配信する。

起動:
    python -m naval gui              # サーバ起動 + ブラウザを開く
    uvicorn naval.web.server:app    # 直接起動
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from naval.service import (
    FleetService,
    FleetServiceError,
    MissionNotFound,
    TaskAlreadyFinished,
    TaskNotFound,
)
from naval.web import ships_config

STATIC_DIR = Path(__file__).resolve().parent / "static"
REPO_ROOT = Path(__file__).resolve().parents[2]

EVENTS_POLL_INTERVAL = float(os.getenv("NAVAL_GUI_POLL_SEC", "3"))


# ------------------------------------------------------------------ #
# request models                                                       #
# ------------------------------------------------------------------ #


class NewMissionRequest(BaseModel):
    objective: str = ""
    ticket: str = ""
    task_id: str = ""
    repo_url: str = ""
    doctrine: str = "standard_full"
    budget_month: str = ""


class ApproveRequest(BaseModel):
    approve: bool
    note: str = ""


class InputRequest(BaseModel):
    message: str = Field(min_length=1)


class NoteRequest(BaseModel):
    note: str = ""


class CaDirectiveRequest(BaseModel):
    directive: str = Field(min_length=1)
    repo_url: str = ""
    advance: bool = True


class ShipUpdateRequest(BaseModel):
    role: Optional[str] = None
    voice: Optional[str] = None
    objective: Optional[str] = None
    thinking: Optional[str] = None
    model_env: Optional[str] = None
    model_tag: Optional[str] = None
    constraints: Optional[List[str]] = None
    checklist: Optional[List[str]] = None
    focus: Optional[List[str]] = None
    armament: Optional[Dict[str, Any]] = None


class SetupRequest(BaseModel):
    values: Dict[str, str]


SETUP_KEYS = [
    "FLEET_REGION",
    "FLEET_SQS_NAME",
    "FLEET_STATE_TABLE",
    "FLEET_TASK_STATE_TABLE",
    "FLEET_BUDGET_TABLE",
    "FLEET_S3_BUCKET",
    "REPO_URL",
    "BEDROCK_SONNET_MODEL_ID",
    "BEDROCK_OPUS_MODEL_ID",
    "BEDROCK_MICRO_MODEL_ID",
    "BEDROCK_LITE_MODEL_ID",
]


# ------------------------------------------------------------------ #
# app factory                                                          #
# ------------------------------------------------------------------ #


def create_app(service_factory: Optional[Callable[[], FleetService]] = None) -> FastAPI:
    app = FastAPI(title="Naval Battle GUI", docs_url="/api/docs", openapi_url="/api/openapi.json")

    _service_lock = threading.Lock()
    _service_holder: Dict[str, FleetService] = {}

    def get_service() -> FleetService:
        if service_factory is not None:
            return service_factory()
        with _service_lock:
            if "svc" not in _service_holder:
                _service_holder["svc"] = FleetService(repo_root=REPO_ROOT)
            return _service_holder["svc"]

    @app.exception_handler(FleetServiceError)
    async def _service_error(_req: Request, exc: FleetServiceError) -> Response:
        status = 404 if isinstance(exc, (MissionNotFound, TaskNotFound)) else 400
        if isinstance(exc, TaskAlreadyFinished):
            status = 409
        return Response(
            content=json.dumps({"detail": str(exc)}, ensure_ascii=False),
            status_code=status,
            media_type="application/json",
        )

    @app.exception_handler(ships_config.ShipsConfigError)
    async def _ships_error(_req: Request, exc: ships_config.ShipsConfigError) -> Response:
        return Response(
            content=json.dumps({"detail": str(exc)}, ensure_ascii=False),
            status_code=400,
            media_type="application/json",
        )

    # -------------------------------------------------------------- #
    # missions                                                         #
    # -------------------------------------------------------------- #

    @app.get("/api/missions")
    def list_missions() -> Dict[str, Any]:
        rows = get_service().list_missions()
        return {"missions": [m.to_dict() for m in rows]}

    @app.get("/api/pending")
    def list_pending() -> Dict[str, Any]:
        rows = get_service().list_pending()
        return {"missions": [m.to_dict() for m in rows]}

    @app.get("/api/missions/{mission_id}")
    def get_mission(mission_id: str) -> Dict[str, Any]:
        return get_service().get_mission(mission_id).to_dict()

    @app.post("/api/missions", status_code=201)
    def create_mission(req: NewMissionRequest) -> Dict[str, Any]:
        svc = get_service()
        task_id = svc.start_task(
            objective=req.objective,
            ticket_file="",
            task_id=req.task_id,
            repo_url=req.repo_url,
            doctrine=req.doctrine,
            budget_month=req.budget_month,
        )
        return {"task_id": task_id}

    @app.post("/api/missions/{mission_id}/approve")
    def approve_mission(mission_id: str, req: ApproveRequest) -> Dict[str, Any]:
        new_status = get_service().approve_mission(
            mission_id, yes=req.approve, note=req.note
        )
        return {"mission_id": mission_id, "status": new_status}

    @app.post("/api/missions/{mission_id}/input")
    def input_mission(mission_id: str, req: InputRequest) -> Dict[str, Any]:
        get_service().send_input(mission_id, req.message)
        return {"mission_id": mission_id, "status": "RUNNING"}

    @app.get("/api/missions/{mission_id}/artifacts")
    def list_artifacts(mission_id: str) -> Dict[str, Any]:
        files = get_service().list_artifacts(mission_id)
        return {"files": [f.to_dict() for f in files]}

    @app.get("/api/missions/{mission_id}/artifacts/{path:path}")
    def download_artifact(mission_id: str, path: str) -> Response:
        body = get_service().get_artifact_bytes(mission_id, path)
        filename = path.rsplit("/", 1)[-1] or "artifact"
        return Response(
            content=body,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # -------------------------------------------------------------- #
    # tasks                                                            #
    # -------------------------------------------------------------- #

    @app.get("/api/tasks")
    def list_tasks() -> Dict[str, Any]:
        rows = get_service().list_tasks()
        return {"tasks": [t.to_dict() for t in rows]}

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str) -> Dict[str, Any]:
        return get_service().get_task(task_id).to_dict()

    @app.post("/api/tasks/{task_id}/abort")
    def abort_task(task_id: str, req: NoteRequest) -> Dict[str, Any]:
        cancelled = get_service().abort_task(task_id, req.note)
        return {"task_id": task_id, "missions_cancelled": cancelled}

    @app.post("/api/tasks/{task_id}/retry")
    def retry_task(task_id: str, req: NoteRequest) -> Dict[str, Any]:
        retried, errors = get_service().retry_task(task_id, req.note)
        return {"task_id": task_id, "missions_retried": retried, "errors": errors}

    @app.post("/api/tasks/{task_id}/ca")
    def ca_directive(task_id: str, req: CaDirectiveRequest) -> Dict[str, Any]:
        svc = get_service()
        mission_id = svc.save_ca_directive(task_id, req.directive)
        if req.advance:
            svc.advance_task(task_id, req.repo_url)
        return {"task_id": task_id, "mission_id": mission_id}

    @app.post("/api/tasks/{task_id}/approve")
    def approve_task(task_id: str, req: NoteRequest) -> Dict[str, Any]:
        svc = get_service()
        n = svc.approve_task_missions(task_id, req.note or "Approved via GUI")
        if n:
            try:
                svc.advance_task(task_id)
            except FleetServiceError:
                pass
        return {"task_id": task_id, "missions_approved": n}

    @app.post("/api/tasks/{task_id}/input")
    def input_task(task_id: str, req: InputRequest) -> Dict[str, Any]:
        svc = get_service()
        n = svc.input_task_missions(task_id, req.message)
        if n:
            try:
                svc.advance_task(task_id)
            except FleetServiceError:
                pass
        return {"task_id": task_id, "missions_signalled": n}

    # -------------------------------------------------------------- #
    # budget / doctor / config                                         #
    # -------------------------------------------------------------- #

    @app.get("/api/budget")
    def get_budget(month: str = "") -> Dict[str, Any]:
        m = month or datetime.now(timezone.utc).strftime("%Y-%m")
        return get_service().get_budget(m).to_dict()

    @app.get("/api/doctor")
    def doctor() -> Dict[str, Any]:
        from naval.cli import (
            _check_aws_identity,
            _check_bedrock_profile,
            _check_ddb_table,
            _check_s3_bucket,
            _check_sqs,
            _check_temporal,
        )

        svc = get_service()
        cfg = svc.cfg
        region = cfg["region"]
        results = [
            _check_temporal(os.getenv("TEMPORAL_ADDRESS", "localhost:7233")),
            _check_aws_identity(region),
            _check_sqs(region, cfg["sqs_name"]),
            _check_ddb_table(region, cfg["state_table"], "aws.ddb.state_table"),
            _check_ddb_table(region, cfg["budget_table"], "aws.ddb.budget_table"),
            _check_ddb_table(region, cfg["task_table"], "aws.ddb.task_table"),
            _check_s3_bucket(region, cfg["bucket"]),
            _check_bedrock_profile(
                region, "aws.bedrock.sonnet_profile", os.getenv("BEDROCK_SONNET_MODEL_ID", "")
            ),
            _check_bedrock_profile(
                region, "aws.bedrock.opus_profile", os.getenv("BEDROCK_OPUS_MODEL_ID", "")
            ),
            _check_bedrock_profile(
                region, "aws.bedrock.micro_profile", os.getenv("BEDROCK_MICRO_MODEL_ID", "")
            ),
            _check_bedrock_profile(
                region, "aws.bedrock.lite_profile", os.getenv("BEDROCK_LITE_MODEL_ID", "")
            ),
        ]
        return {
            "region": region,
            "results": [r.__dict__ for r in results],
        }

    @app.get("/api/config")
    def get_config() -> Dict[str, Any]:
        cfg = get_service().cfg
        return {
            "region": cfg["region"],
            "state_table": cfg["state_table"],
            "task_table": cfg["task_table"],
            "budget_table": cfg["budget_table"],
            "sqs_name": cfg["sqs_name"],
            "bucket": cfg["bucket"],
        }

    # -------------------------------------------------------------- #
    # fleet (艦種ごとの AI エージェント設定)                            #
    # -------------------------------------------------------------- #

    @app.get("/api/fleet/ships")
    def fleet_ships() -> Dict[str, Any]:
        ships = ships_config.list_ships()
        return {"ships": ships, "base_classes": ships_config.BASE_SHIP_CLASSES}

    @app.get("/api/fleet/ships/{ship_class}")
    def fleet_ship(ship_class: str) -> Dict[str, Any]:
        return {"ship_class": ship_class, "profile": ships_config.get_ship(ship_class)}

    @app.put("/api/fleet/ships/{ship_class}")
    def update_fleet_ship(ship_class: str, req: ShipUpdateRequest) -> Dict[str, Any]:
        update = {k: v for k, v in req.model_dump().items() if v is not None}
        if not update:
            raise HTTPException(status_code=400, detail="更新項目がありません")
        profile = ships_config.update_ship(ship_class, update)
        worker_restart = "model_env" in update or "armament" in update
        return {
            "ship_class": ship_class,
            "profile": profile,
            "worker_restart_required": worker_restart,
        }

    @app.get("/api/fleet/models")
    def fleet_models() -> Dict[str, Any]:
        return {"models": ships_config.list_models()}

    @app.get("/api/fleet/formations")
    def fleet_formations() -> Dict[str, Any]:
        return {"formations": ships_config.list_formations()}

    # -------------------------------------------------------------- #
    # setup wizard                                                     #
    # -------------------------------------------------------------- #

    @app.get("/api/setup")
    def get_setup() -> Dict[str, Any]:
        values = {k: os.getenv(k, "") for k in SETUP_KEYS}
        return {"keys": SETUP_KEYS, "values": values}

    @app.post("/api/setup")
    def post_setup(req: SetupRequest) -> Dict[str, Any]:
        unknown = [k for k in req.values if k not in SETUP_KEYS]
        if unknown:
            raise HTTPException(
                status_code=400, detail=f"未対応のキーです: {', '.join(unknown)}"
            )
        env_path = REPO_ROOT / ".env"
        lines: List[str] = []
        existing: Dict[str, str] = {}
        if env_path.exists():
            import shutil

            shutil.copy2(env_path, env_path.with_suffix(".env.bak"))
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    k, _, v = line.partition("=")
                    existing[k.strip()] = v
        existing.update({k: v for k, v in req.values.items()})
        for k, v in existing.items():
            lines.append(f"{k}={v}")
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        # 現在のプロセスにも反映（doctor 等で即時チェック可能にする）
        for k, v in req.values.items():
            os.environ[k] = v
        return {"written": str(env_path), "keys": sorted(req.values.keys())}

    # -------------------------------------------------------------- #
    # events (SSE)                                                     #
    # -------------------------------------------------------------- #

    @app.get("/api/events")
    async def events() -> StreamingResponse:
        async def stream():
            last_payload = ""
            while True:
                try:
                    rows = await asyncio.to_thread(
                        lambda: [m.to_dict() for m in get_service().list_missions()]
                    )
                    payload = json.dumps({"missions": rows}, ensure_ascii=False)
                    if payload != last_payload:
                        last_payload = payload
                        yield f"event: missions\ndata: {payload}\n\n"
                    else:
                        yield ": heartbeat\n\n"
                except FleetServiceError as exc:
                    err = json.dumps({"detail": str(exc)}, ensure_ascii=False)
                    yield f"event: error\ndata: {err}\n\n"
                await asyncio.sleep(EVENTS_POLL_INTERVAL)

        return StreamingResponse(stream(), media_type="text/event-stream")

    # -------------------------------------------------------------- #
    # static GUI                                                       #
    # -------------------------------------------------------------- #

    if STATIC_DIR.exists():
        @app.get("/")
        def index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


app = create_app()


def serve(host: str = "127.0.0.1", port: int = 8800, open_browser: bool = True) -> None:
    """`naval gui` コマンドのエントリポイント。"""
    import uvicorn

    if open_browser:
        import webbrowser

        threading.Timer(
            1.0, lambda: webbrowser.open(f"http://{host}:{port}/")
        ).start()
    uvicorn.run(app, host=host, port=port, log_level="info")

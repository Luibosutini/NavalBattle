from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import boto3
import typer
from botocore.exceptions import BotoCoreError, ClientError

from .base import RuntimeBase


class TemporalRuntime(RuntimeBase):
    @staticmethod
    def _resolve_ticket(ticket: str) -> str:
        p = Path(ticket).expanduser()
        if p.exists() and p.is_file():
            try:
                return p.read_text(encoding="utf-8")
            except OSError as exc:
                typer.echo(f"[ERROR] failed to read ticket file: {p} ({exc})", err=True)
                raise typer.Exit(2)
        return ticket

    @staticmethod
    def _normalize_workflow_id(mission: str) -> str:
        mission_clean = mission.strip()
        if mission_clean.startswith("mission-"):
            return mission_clean
        return f"mission-{mission_clean}"

    @staticmethod
    def _derive_next_action(mission_id: str, user_state: str, next_action: str) -> str:
        if next_action.strip():
            return next_action
        if user_state == "NEED_APPROVAL":
            return f"naval approve --mission {mission_id} --yes"
        if user_state == "NEED_INPUT":
            return f"naval input --mission {mission_id} \"<message>\""
        return ""

    @staticmethod
    def _normalize_mission_id(mission: str) -> str:
        m = mission.strip()
        if m.startswith("mission-"):
            return m[len("mission-") :]
        return m

    def _resolve_region_bucket(self) -> tuple[str, str]:
        env = self.ctx.env()
        region = env.get("FLEET_REGION", os.getenv("FLEET_REGION", "ap-northeast-1"))
        bucket = env.get("FLEET_S3_BUCKET", os.getenv("FLEET_S3_BUCKET", ""))
        try:
            from fleet_config import resolve_fleet_config

            cfg = resolve_fleet_config()
            region = cfg.get("region") or region
            bucket = cfg.get("bucket") or bucket
        except Exception:
            pass
        return region, bucket

    @staticmethod
    def _list_keys(s3: Any, bucket: str, prefix: str) -> list[str]:
        keys: list[str] = []
        token: Optional[str] = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = str(obj.get("Key", ""))
                if key and not key.endswith("/"):
                    keys.append(key)
            if not resp.get("IsTruncated"):
                break
            token = str(resp.get("NextContinuationToken", ""))
            if not token:
                break
        return keys

    @staticmethod
    def _load_json_object(s3: Any, bucket: str, key: str) -> dict[str, Any]:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body.decode("utf-8"))

    def _read_mission_logs(self, mission_id: str, limit: int) -> list[dict]:
        region, bucket = self._resolve_region_bucket()
        if not bucket:
            typer.echo("[ERROR] FLEET_S3_BUCKET is not set", err=True)
            raise typer.Exit(2)

        s3 = boto3.client("s3", region_name=region)
        events_prefix = f"missions/{mission_id}/events/"
        comms_prefix = f"missions/{mission_id}/comms/"

        rows: list[dict] = []
        for key in self._list_keys(s3, bucket, events_prefix):
            try:
                payload = self._load_json_object(s3, bucket, key)
            except Exception:
                continue
            rows.append(
                {
                    "ts": int(payload.get("ts", 0)),
                    "kind": "event",
                    "etype": str(payload.get("event_type", "")),
                    "message": str(payload.get("detail", "")),
                }
            )
        for key in self._list_keys(s3, bucket, comms_prefix):
            try:
                payload = self._load_json_object(s3, bucket, key)
            except Exception:
                continue
            rows.append(
                {
                    "ts": int(payload.get("ts", 0)),
                    "kind": "comm",
                    "etype": str(payload.get("type", "")),
                    "message": str(payload.get("content", "")),
                }
            )
        rows.sort(key=lambda x: int(x.get("ts", 0)))
        return rows[-limit:]

    @staticmethod
    def _state_rank(state: str) -> int:
        order = {
            "NEED_APPROVAL": 0,
            "NEED_INPUT": 1,
            "RUNNING": 2,
            "ENQUEUED": 3,
            "FAILED": 4,
            "BUDGET_DENIED": 5,
            "CANCELLED": 6,
            "DONE": 7,
        }
        return order.get(state, 99)

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        value = str(text)
        if len(value) <= limit:
            return value
        if limit <= 3:
            return value[:limit]
        return value[: limit - 3] + "..."

    async def _fetch_temporal_missions_async(
        self, *, address: str, namespace: str, limit: int = 200
    ) -> list[dict[str, str]]:
        from temporalio.client import Client

        client = await Client.connect(address, namespace=namespace)
        rows: list[dict[str, str]] = []
        async for wf in client.list_workflows(
            query='WorkflowType = "MissionWorkflow"',
            limit=limit,
        ):
            mission_id = wf.id[len("mission-") :] if wf.id.startswith("mission-") else wf.id
            row: dict[str, str] = {
                "mission_id": mission_id,
                "workflow_id": wf.id,
                "workflow_status": str(wf.status).split(".")[-1],
                "user_state": "",
                "needs_reason": "",
                "next_action": "",
            }
            try:
                handle = client.get_workflow_handle(wf.id, run_id=wf.run_id)
                query_data = await handle.query("get_status")
                row["user_state"] = str(query_data.get("user_state", "")).strip()
                row["needs_reason"] = str(query_data.get("needs_reason", "")).strip()
                row["next_action"] = str(query_data.get("next_action", "")).strip()
            except Exception:
                pass
            row["next_action"] = self._derive_next_action(
                row["mission_id"],
                row["user_state"],
                row["next_action"],
            )
            rows.append(row)

        rows.sort(
            key=lambda r: (
                self._state_rank(r.get("user_state", "") or r.get("workflow_status", "")),
                r.get("mission_id", ""),
            )
        )
        return rows

    def _list_temporal_missions(self, *, limit: int = 200) -> list[dict[str, str]]:
        address = self.ctx.env().get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = self.ctx.env().get("TEMPORAL_NAMESPACE", "default")
        return asyncio.run(
            self._fetch_temporal_missions_async(
                address=address,
                namespace=namespace,
                limit=limit,
            )
        )

    def _build_watch_layout(
        self,
        *,
        rows: list[dict[str, str]],
        selected_mission: str,
        events: list[dict[str, Any]],
        comms: list[dict[str, Any]],
        mission_filter: str,
        page: int,
        total_pages: int,
    ) -> Any:
        from rich.layout import Layout
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text

        layout = Layout(name="root")
        layout.split_row(Layout(name="left", ratio=2), Layout(name="right", ratio=3))
        layout["right"].split_column(Layout(name="events"), Layout(name="comms"))

        missions = Table(expand=True)
        missions.add_column("Mission", overflow="fold")
        missions.add_column("State", width=14)
        missions.add_column("Reason", overflow="fold")
        if not rows:
            missions.add_row("-", "-", "no missions")
        else:
            for row in rows:
                mid = row.get("mission_id", "")
                state = row.get("user_state", "") or row.get("workflow_status", "")
                reason = self._truncate(row.get("needs_reason", ""), 60)
                state_style = "bold red" if state in {"NEED_APPROVAL", "NEED_INPUT"} else ""
                marker = ">" if mid == selected_mission else " "
                missions.add_row(f"{marker} {mid}", Text(state, style=state_style), reason)

        filter_text = mission_filter if mission_filter else "-"
        missions_title = f"Missions page={page}/{total_pages} filter={filter_text}"
        layout["left"].update(Panel(missions, title=missions_title))

        events_text = Text()
        if not events:
            events_text.append("No events\n")
        else:
            for row in events[-20:]:
                etype = str(row.get("etype", ""))
                msg = self._truncate(str(row.get("message", "")), 120)
                style = "bold red" if etype in {"STATUS_NEED_APPROVAL", "STATUS_NEED_INPUT"} else ""
                events_text.append(f"[{row.get('ts', 0)}] {etype} {msg}\n", style=style)
        layout["events"].update(Panel(events_text, title=f"Events ({selected_mission or '-'})"))

        comms_text = Text()
        if not comms:
            comms_text.append("No comms\n")
        else:
            for row in comms[-20:]:
                ctype = str(row.get("etype", ""))
                msg = self._truncate(str(row.get("message", "")), 120)
                style = "bold red" if ctype in {"need_approval", "need_input"} else ""
                comms_text.append(f"[{row.get('ts', 0)}] {ctype} {msg}\n", style=style)
        layout["comms"].update(Panel(comms_text, title=f"Comms ({selected_mission or '-'})"))
        return layout

    def enqueue(
        self,
        *,
        doctrine: str,
        ticket: str,
        budget: Optional[str],
        repo_url: str,
        task_id: str,
        watch: bool,
        hitl_mode: str,
        hitl_timeout_hours: int,
        hitl_timeout_action: str,
    ) -> None:
        try:
            from temporalio.client import Client
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: temporalio", err=True)
            typer.echo("Install dependencies: pip install -r requirements.txt", err=True)
            raise typer.Exit(1)

        try:
            from naval.temporal.workflows import MissionWorkflow
        except Exception as exc:
            typer.echo(f"[ERROR] cannot load MissionWorkflow: {exc}", err=True)
            raise typer.Exit(1)

        mission_id = f"MS-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        task_value = task_id or f"T-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        workflow_id = f"mission-{mission_id}"
        ticket_text = self._resolve_ticket(ticket)
        address = self.ctx.env().get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = self.ctx.env().get("TEMPORAL_NAMESPACE", "default")
        task_queue = self.ctx.env().get("TEMPORAL_TASK_QUEUE", "naval-mission")

        payload = {
            "mission_id": mission_id,
            "task_id": task_value,
            "ship": "CVL",
            "doctrine": doctrine,
            "ticket": ticket_text,
            "budget_month": budget or self.ctx.now_month(),
            "repo_url": repo_url,
            "trace_id": mission_id,
            "hitl_mode": hitl_mode,
            "hitl_timeout_hours": hitl_timeout_hours,
            "hitl_timeout_action": hitl_timeout_action.upper(),
        }

        async def _start() -> tuple[str, str]:
            client = await Client.connect(address, namespace=namespace)
            handle = await client.start_workflow(
                MissionWorkflow.run,
                payload,
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(hours=1),
            )
            return handle.id, handle.first_execution_run_id

        try:
            wid, run_id = asyncio.run(_start())
        except Exception as exc:
            typer.echo(f"[ERROR] failed to start workflow: {exc}", err=True)
            raise typer.Exit(1)

        typer.echo("Temporal workflow started.")
        typer.echo(f"workflow_id={wid}")
        typer.echo(f"run_id={run_id}")
        typer.echo(f"mission_id={mission_id}")
        if hitl_mode:
            typer.echo(f"hitl_mode={hitl_mode} timeout_hours={hitl_timeout_hours} on_timeout={hitl_timeout_action.upper()}")
        if watch:
            typer.echo("Starting watch UI (--filter mission_id). Press Ctrl+C to stop.")
            self.watch(month=None, interval=2, mission_filter=mission_id, page_size=20, page=1)

    def status(self, *, mission: str, month: Optional[str]) -> None:
        try:
            from temporalio.client import Client
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: temporalio", err=True)
            typer.echo("Install dependencies: pip install -r requirements.txt", err=True)
            raise typer.Exit(1)

        address = self.ctx.env().get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = self.ctx.env().get("TEMPORAL_NAMESPACE", "default")
        if not mission:
            if month:
                typer.echo(f"[INFO] --month={month} is ignored in temporal runtime.")

            try:
                rows = self._list_temporal_missions(limit=50)
            except Exception as exc:
                typer.echo(f"[ERROR] failed to list workflows: {exc}", err=True)
                raise typer.Exit(1)

            if not rows:
                typer.echo("No missions found in temporal namespace.")
                return

            typer.echo("Missions:")
            for row in rows:
                state = row["user_state"] or row["workflow_status"]
                line = f"- {row['mission_id']} state={state}"
                if row["needs_reason"]:
                    line += f" reason={row['needs_reason']}"
                typer.echo(line)
                if row["next_action"]:
                    typer.echo(f"  Next: {row['next_action']}")
            return

        workflow_id = self._normalize_workflow_id(mission)

        async def _describe() -> tuple[str, Optional[dict]]:
            client = await Client.connect(address, namespace=namespace)
            handle = client.get_workflow_handle(workflow_id)
            desc = await handle.describe()
            status_name = str(desc.status).split(".")[-1]
            query_data: Optional[dict] = None
            try:
                query_data = await handle.query("get_status")
            except Exception:
                query_data = None
            return status_name, query_data

        try:
            status_name, query_data = asyncio.run(_describe())
        except Exception as exc:
            typer.echo(f"[ERROR] failed to query workflow: {exc}", err=True)
            raise typer.Exit(1)

        typer.echo(f"workflow_id={workflow_id}")
        typer.echo(f"status={status_name}")
        if isinstance(query_data, dict):
            mission_id = str(query_data.get("mission_id", mission)).strip() or mission
            user_state = str(query_data.get("user_state", ""))
            needs_reason = str(query_data.get("needs_reason", ""))
            next_action = str(query_data.get("next_action", ""))
            last_error = str(query_data.get("last_error", ""))
            last_error_code = str(query_data.get("last_error_code", ""))
            next_action = self._derive_next_action(mission_id, user_state, next_action)
            if user_state:
                typer.echo(f"user_state={user_state}")
            if needs_reason:
                typer.echo(f"needs_reason={needs_reason}")
            if last_error_code:
                typer.echo(f"error_code={last_error_code}")
            if last_error:
                typer.echo(f"last_error={last_error}")
            if next_action:
                typer.echo(f"Next: {next_action}")

    def tail(self, *, mission: str, limit: int, follow: bool) -> None:
        if not mission.strip():
            typer.echo("[ERROR] --mission is required for temporal tail", err=True)
            raise typer.Exit(2)
        mission_id = self._normalize_mission_id(mission)
        seen: set[tuple[int, str, str]] = set()
        try:
            while True:
                rows = self._read_mission_logs(mission_id, limit)
                for row in rows:
                    key = (
                        int(row.get("ts", 0)),
                        str(row.get("kind", "")),
                        str(row.get("etype", "")),
                    )
                    if follow and key in seen:
                        continue
                    etype = str(row.get("etype", ""))
                    text = str(row.get("message", ""))
                    is_need = etype in {
                        "STATUS_NEED_INPUT",
                        "STATUS_NEED_APPROVAL",
                        "need_input",
                        "need_approval",
                    }
                    marker = "!!" if is_need else "  "
                    typer.echo(f"{marker} [{row['kind']}] ts={row['ts']} type={etype} {text}")
                    seen.add(key)
                if not follow:
                    break
                time.sleep(0.5)
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to read mission logs: {exc}", err=True)
            raise typer.Exit(1)
        except KeyboardInterrupt:
            typer.echo("tail stopped.")

    def approve(self, *, mission: str, yes: bool, no: bool, note: str) -> None:
        if yes == no:
            typer.echo("[ERROR] choose exactly one of --yes or --no", err=True)
            raise typer.Exit(2)
        decision = bool(yes)
        workflow_id = mission if mission.startswith("mission-") else f"mission-{mission}"
        address = self.ctx.env().get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = self.ctx.env().get("TEMPORAL_NAMESPACE", "default")
        signal_id = f"approve-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

        async def _signal() -> None:
            from temporalio.client import Client

            client = await Client.connect(address, namespace=namespace)
            handle = client.get_workflow_handle(workflow_id)
            await handle.signal(
                "approve",
                {
                    "mission_id": mission,
                    "decision": decision,
                    "note": note,
                    "signal_id": signal_id,
                },
            )

        try:
            asyncio.run(_signal())
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: temporalio", err=True)
            typer.echo("Install dependencies: pip install -r requirements.txt", err=True)
            raise typer.Exit(1)
        except Exception as exc:
            typer.echo(f"[ERROR] failed to send approve signal: {exc}", err=True)
            raise typer.Exit(1)

        typer.echo(f"approve signal sent: workflow_id={workflow_id} signal_id={signal_id}")

    def input(self, *, mission: str, message: str) -> None:
        workflow_id = mission if mission.startswith("mission-") else f"mission-{mission}"
        address = self.ctx.env().get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = self.ctx.env().get("TEMPORAL_NAMESPACE", "default")
        signal_id = f"input-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

        async def _signal() -> None:
            from temporalio.client import Client

            client = await Client.connect(address, namespace=namespace)
            handle = client.get_workflow_handle(workflow_id)
            await handle.signal(
                "provide_input",
                {
                    "mission_id": mission,
                    "message": message,
                    "signal_id": signal_id,
                },
            )

        try:
            asyncio.run(_signal())
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: temporalio", err=True)
            typer.echo("Install dependencies: pip install -r requirements.txt", err=True)
            raise typer.Exit(1)
        except Exception as exc:
            typer.echo(f"[ERROR] failed to send input signal: {exc}", err=True)
            raise typer.Exit(1)

        typer.echo(f"input signal sent: workflow_id={workflow_id} signal_id={signal_id}")

    def pull(self, *, mission: str, out: str) -> None:
        mission_id = self._normalize_mission_id(mission)
        region, bucket = self._resolve_region_bucket()
        if not bucket:
            typer.echo("[ERROR] FLEET_S3_BUCKET is not set", err=True)
            raise typer.Exit(2)

        out_dir = Path(out).expanduser() if out else (self.ctx.repo_root / "missions" / mission_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"missions/{mission_id}/"

        s3 = boto3.client("s3", region_name=region)
        try:
            keys = self._list_keys(s3, bucket, prefix)
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to list mission objects: {exc}", err=True)
            raise typer.Exit(1)

        if not keys:
            typer.echo(f"No objects found for mission={mission_id}")
            return

        result_summaries: list[str] = []
        for key in keys:
            rel = key[len(prefix) :] if key.startswith(prefix) else key
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            dest.write_bytes(body)

            if key.endswith("/result.json"):
                try:
                    obj = json.loads(body.decode("utf-8"))
                    result_summaries.append(
                        f"{obj.get('task_id', '?')}/{obj.get('ship', '?')} "
                        f"status={obj.get('status', '?')} confidence={obj.get('confidence', '?')}"
                    )
                except Exception:
                    result_summaries.append(f"{rel} (parse_error)")

        typer.echo(f"pulled mission={mission_id} files={len(keys)} out={out_dir}")
        if result_summaries:
            typer.echo("result.json summary:")
            for line in result_summaries:
                typer.echo(f"- {line}")

    def pending(self) -> None:
        """Temporal では approve/input コマンドで直接シグナルを送る。"""
        typer.echo(
            "[INFO] temporal runtime では pending コマンドは使えません。\n"
            "  naval status                      でミッション一覧を確認\n"
            "  naval approve --mission M-XXX --yes  で承認\n"
            "  naval input   --mission M-XXX \"...\"  で入力送信"
        )

    def ca(self, *, task_id: str, directive: str, repo_url: str, auto_advance: bool) -> None:
        typer.echo(
            "[INFO] ca command is not supported in temporal runtime. "
            "Use workflow signals (approve/input) instead.",
            err=True,
        )
        raise typer.Exit(2)

    def watch(
        self,
        *,
        month: Optional[str],
        interval: int,
        mission_filter: str,
        page_size: int,
        page: int,
    ) -> None:
        if month:
            typer.echo(f"[INFO] --month={month} is ignored in temporal runtime.")
        try:
            from rich.console import Console
            from rich.live import Live
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            typer.echo("Install dependencies: pip install -r requirements.txt", err=True)
            raise typer.Exit(1)

        refresh_sec = max(1, interval)
        page_size = max(1, min(page_size, 200))
        page = max(1, page)
        filter_norm = mission_filter.strip().lower()
        console = Console()
        selected_mission = ""

        try:
            with Live(console=console, screen=True, auto_refresh=False) as live:
                while True:
                    rows_all = self._list_temporal_missions(limit=500)
                    if filter_norm:
                        rows_all = [
                            r
                            for r in rows_all
                            if filter_norm in r.get("mission_id", "").lower()
                            or filter_norm in r.get("user_state", "").lower()
                        ]

                    total_pages = max(1, (len(rows_all) + page_size - 1) // page_size)
                    page_use = min(page, total_pages)
                    start = (page_use - 1) * page_size
                    end = start + page_size
                    rows = rows_all[start:end]

                    need_rows = [
                        r
                        for r in rows
                        if (r.get("user_state", "") or r.get("workflow_status", ""))
                        in {"NEED_APPROVAL", "NEED_INPUT"}
                    ]
                    if need_rows:
                        selected_mission = need_rows[0].get("mission_id", "")
                    else:
                        if selected_mission and all(r.get("mission_id", "") != selected_mission for r in rows):
                            selected_mission = ""
                        if not selected_mission and rows:
                            selected_mission = rows[0].get("mission_id", "")

                    events: list[dict[str, Any]] = []
                    comms: list[dict[str, Any]] = []
                    if selected_mission:
                        logs = self._read_mission_logs(selected_mission, limit=200)
                        events = [r for r in logs if r.get("kind") == "event"]
                        comms = [r for r in logs if r.get("kind") == "comm"]

                    layout = self._build_watch_layout(
                        rows=rows,
                        selected_mission=selected_mission,
                        events=events,
                        comms=comms,
                        mission_filter=mission_filter,
                        page=page_use,
                        total_pages=total_pages,
                    )
                    live.update(layout, refresh=True)
                    time.sleep(refresh_sec)
        except KeyboardInterrupt:
            typer.echo("watch stopped.")
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] watch failed: {exc}", err=True)
            raise typer.Exit(1)
        except Exception as exc:
            typer.echo(f"[ERROR] watch failed: {exc}", err=True)
            raise typer.Exit(1)

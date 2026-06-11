from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import typer
from botocore.exceptions import BotoCoreError, ClientError

from .base import RuntimeBase


class AwsRuntime(RuntimeBase):
    def _resolve_aws_config(self) -> Dict[str, str]:
        env = self.ctx.env()
        region = env.get("FLEET_REGION", os.getenv("FLEET_REGION", "ap-northeast-1"))
        state_table = env.get("FLEET_STATE_TABLE", os.getenv("FLEET_STATE_TABLE", "fleet-mission-state"))
        task_table = env.get("FLEET_TASK_STATE_TABLE", os.getenv("FLEET_TASK_STATE_TABLE", "fleet-task-state"))
        sqs_name = env.get("FLEET_SQS_NAME", os.getenv("FLEET_SQS_NAME", "fleet-missions"))
        bucket = env.get("FLEET_S3_BUCKET", os.getenv("FLEET_S3_BUCKET", ""))
        try:
            from fleet_config import resolve_fleet_config
            cfg = resolve_fleet_config()
            region = cfg.get("region") or region
            state_table = cfg.get("state_table") or state_table
            sqs_name = cfg.get("sqs_name") or sqs_name
            bucket = cfg.get("bucket") or bucket
        except Exception:
            pass
        return {
            "region": region,
            "state_table": state_table,
            "task_table": task_table,
            "sqs_name": sqs_name,
            "bucket": bucket,
        }

    def _get_mission_payload(self, s3: Any, bucket: str, mission_id: str) -> Dict[str, Any]:
        try:
            key = f"missions/{mission_id}/orders/payload.json"
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            return {}

    def _list_s3_keys(self, s3: Any, bucket: str, prefix: str) -> List[str]:
        keys: List[str] = []
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
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
    def _resolve_ca_mission_id(task_item: Dict[str, Any]) -> Optional[str]:
        stages = task_item.get("stages", {}).get("M", {})
        for stage_name, stage_item in stages.items():
            base = stage_name.split("_", 1)[0] if "_" in stage_name else stage_name
            if base != "CA":
                continue
            mission = stage_item.get("M", {}).get("mission_id", {})
            if "S" in mission and mission["S"]:
                return mission["S"]
        return None

    def _resume_mission(
        self,
        ddb: Any,
        sqs_client: Any,
        s3: Any,
        mission_id: str,
        expected_status: str,
        new_status: str,
        response_text: str,
        state_table: str,
        sqs_name: str,
        bucket: str,
    ) -> None:
        """共通の DynamoDB 更新 + SQS 再投入ロジック。"""
        now = int(time.time())
        expr_names = {"#s": "status"}
        expr_values: Dict[str, Any] = {
            ":new_status": {"S": new_status},
            ":now": {"N": str(now)},
            ":resp": {"S": response_text},
            ":from": {"S": expected_status},
        }
        update_expr = "SET #s=:new_status, updated_at=:now, human_response=:resp"
        if new_status == "FAILED":
            expr_values[":fr"] = {"S": f"Approval denied: {response_text}"}
            update_expr += ", fail_reason=:fr"
        update_expr += " REMOVE needs_input, needs_approval, owner, lock_until"

        try:
            resp = ddb.get_item(TableName=state_table, Key={"mission_id": {"S": mission_id}})
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get mission: {exc}", err=True)
            raise typer.Exit(1)
        if "Item" not in resp:
            typer.echo(f"[ERROR] mission not found: {mission_id}", err=True)
            raise typer.Exit(1)
        item = resp["Item"]
        current_status = item.get("status", {}).get("S", "")
        if current_status != expected_status:
            typer.echo(
                f"[ERROR] mission status is {current_status!r}, expected {expected_status!r}", err=True
            )
            raise typer.Exit(1)

        try:
            ddb.update_item(
                TableName=state_table,
                Key={"mission_id": {"S": mission_id}},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression="#s=:from",
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to update mission: {exc}", err=True)
            raise typer.Exit(1)

        if new_status == "RUNNING":
            try:
                queue_url = sqs_client.get_queue_url(QueueName=sqs_name)["QueueUrl"]
                payload: Dict[str, Any] = {}
                if s3 and bucket:
                    payload = self._get_mission_payload(s3, bucket, mission_id)
                if not payload:
                    payload = {
                        "mission_id": mission_id,
                        "ship_class": item.get("ship_class", {}).get("S", ""),
                        "task_id": item.get("task_id", {}).get("S", ""),
                        "ship_id": item.get("ship_id", {}).get("S", ""),
                    }
                payload["resume"] = True
                sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(payload, ensure_ascii=False),
                )
            except (ClientError, BotoCoreError) as exc:
                typer.echo(f"[ERROR] failed to re-enqueue mission: {exc}", err=True)
                raise typer.Exit(1)

    # ------------------------------------------------------------------ #
    # RuntimeBase interface                                                #
    # ------------------------------------------------------------------ #

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
        if hitl_mode:
            typer.echo(
                "[WARN] hitl options are ignored in aws runtime (applies to temporal runtime only)."
            )
        _ = hitl_timeout_hours
        _ = hitl_timeout_action
        budget_month = budget or self.ctx.now_month()
        ticket_path = Path(ticket).expanduser()
        args: list[str] = ["run", "--formation", doctrine, "--budget-month", budget_month]
        if task_id:
            args.extend(["--task-id", task_id])
        if repo_url:
            args.extend(["--repo-url", repo_url])
        if ticket_path.exists():
            args.extend(["--ticket-file", str(ticket_path)])
        else:
            args.extend(["--objective", ticket])
        if not watch:
            args.append("--no-watch")
        self.run_script("fleet_interact.py", args)

    def status(self, *, mission: str, month: Optional[str]) -> None:
        if mission:
            self.run_script("task_orchestrator.py", ["status", mission])
            return
        budget_month = month or self.ctx.now_month()
        self.run_script("mission_monitor.py", [budget_month])

    def tail(self, *, mission: str, limit: int, follow: bool) -> None:
        args = ["search", "--limit", str(limit)]
        if mission:
            args.extend(["--mission-id", mission])
        if follow:
            typer.echo("[WARN] --follow is not supported in aws runtime; showing single snapshot.")
        self.run_script("navalctl.py", args)

    def approve(self, *, mission: str, yes: bool, no: bool, note: str) -> None:
        if yes == no:
            typer.echo("[ERROR] choose exactly one of --yes or --no", err=True)
            raise typer.Exit(2)
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        ddb = boto3.client("dynamodb", region_name=region)
        sqs_client = boto3.client("sqs", region_name=region)
        s3 = boto3.client("s3", region_name=region) if cfg["bucket"] else None

        new_status = "RUNNING" if yes else "FAILED"
        response_text = note or ("approved" if yes else "rejected")
        self._resume_mission(
            ddb=ddb,
            sqs_client=sqs_client,
            s3=s3,
            mission_id=mission,
            expected_status="NEED_APPROVAL",
            new_status=new_status,
            response_text=response_text,
            state_table=cfg["state_table"],
            sqs_name=cfg["sqs_name"],
            bucket=cfg["bucket"],
        )
        if new_status == "RUNNING":
            typer.echo(f"mission approved and re-enqueued: mission_id={mission}")
        else:
            typer.echo(f"mission rejected: mission_id={mission}")

    def input(self, *, mission: str, message: str) -> None:
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        ddb = boto3.client("dynamodb", region_name=region)
        sqs_client = boto3.client("sqs", region_name=region)
        s3 = boto3.client("s3", region_name=region) if cfg["bucket"] else None

        self._resume_mission(
            ddb=ddb,
            sqs_client=sqs_client,
            s3=s3,
            mission_id=mission,
            expected_status="NEED_INPUT",
            new_status="RUNNING",
            response_text=message,
            state_table=cfg["state_table"],
            sqs_name=cfg["sqs_name"],
            bucket=cfg["bucket"],
        )
        typer.echo(f"input sent and mission re-enqueued: mission_id={mission}")

    def pull(self, *, mission: str, out: str) -> None:
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        bucket = cfg["bucket"]
        if not bucket:
            typer.echo("[ERROR] FLEET_S3_BUCKET is not set", err=True)
            raise typer.Exit(2)

        out_dir = Path(out).expanduser() if out else (self.ctx.repo_root / "missions" / mission)
        out_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"missions/{mission}/"

        s3 = boto3.client("s3", region_name=region)
        try:
            keys = self._list_s3_keys(s3, bucket, prefix)
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to list mission objects: {exc}", err=True)
            raise typer.Exit(1)

        if not keys:
            typer.echo(f"No objects found for mission={mission}")
            return

        result_summaries: List[str] = []
        for key in keys:
            rel = key[len(prefix):] if key.startswith(prefix) else key
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            dest.write_bytes(body)
            if key.endswith("/result.json"):
                try:
                    obj_data = json.loads(body.decode("utf-8"))
                    result_summaries.append(
                        f"{obj_data.get('task_id', '?')}/{obj_data.get('ship', '?')} "
                        f"status={obj_data.get('status', '?')} confidence={obj_data.get('confidence', '?')}"
                    )
                except Exception:
                    result_summaries.append(f"{rel} (parse_error)")

        typer.echo(f"pulled mission={mission} files={len(keys)} out={out_dir}")
        if result_summaries:
            typer.echo("result.json summary:")
            for line in result_summaries:
                typer.echo(f"- {line}")

    def ca(self, *, task_id: str, directive: str, repo_url: str, auto_advance: bool) -> None:
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        bucket = cfg["bucket"]
        if not bucket:
            typer.echo("[ERROR] FLEET_S3_BUCKET is not set", err=True)
            raise typer.Exit(2)

        ddb = boto3.client("dynamodb", region_name=region)
        s3 = boto3.client("s3", region_name=region)

        # タスク状態から CA ミッションIDを取得
        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task state: {exc}", err=True)
            raise typer.Exit(1)

        task_item = resp.get("Item")
        if not task_item:
            typer.echo(f"[ERROR] task not found: {task_id}", err=True)
            raise typer.Exit(1)

        mission_id = self._resolve_ca_mission_id(task_item)
        if not mission_id:
            typer.echo(f"[ERROR] no CA mission found for task: {task_id}", err=True)
            raise typer.Exit(1)

        # S3 に指令書を書き込む
        now_ts = int(time.time())
        now_iso = datetime.fromtimestamp(now_ts, timezone.utc).isoformat().replace("+00:00", "Z")
        content = (
            f"# Human CA Directive\n"
            f"task_id: {task_id}\n"
            f"updated_at: {now_iso}\n\n"
            f"{directive.strip()}\n"
        )
        key = f"missions/{mission_id}/artifacts/ca_human_directive.md"
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown; charset=utf-8",
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to put CA directive: {exc}", err=True)
            raise typer.Exit(1)

        # 通信ログを記録
        comm_id = uuid.uuid4().hex[:8]
        comm = {
            "ts": now_ts,
            "comm_id": comm_id,
            "from": "user",
            "to": "CA",
            "type": "ca_directive",
            "content": directive.strip(),
        }
        comm_key = f"missions/{mission_id}/comms/{now_ts}-{comm_id}.json"
        try:
            s3.put_object(
                Bucket=bucket,
                Key=comm_key,
                Body=json.dumps(comm, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception:
            typer.echo("[WARN] failed to record comm (non-fatal)")

        typer.echo(f"CA directive saved: mission_id={mission_id}")

        if not auto_advance:
            return

        args = ["advance", task_id]
        if repo_url:
            args.append(repo_url)
        self.run_script("task_orchestrator.py", args)

    def _list_comms(self, s3_client: Any, bucket: str, mission_id: str) -> List[Dict[str, Any]]:
        """S3 から通信ログを取得して時系列順に返す。"""
        try:
            prefix = f"missions/{mission_id}/comms/"
            keys = self._list_s3_keys(s3_client, bucket, prefix)
            comms: List[Dict[str, Any]] = []
            for key in keys:
                try:
                    obj = s3_client.get_object(Bucket=bucket, Key=key)
                    comms.append(json.loads(obj["Body"].read().decode("utf-8")))
                except Exception:
                    continue
            comms.sort(key=lambda x: int(x.get("ts", 0)))
            return comms
        except Exception:
            return []

    def _scan_missions(self, ddb: Any, state_table: str) -> List[Dict[str, Any]]:
        """DynamoDB からミッション一覧を全件スキャンする。"""
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {"TableName": state_table}
        while True:
            resp = ddb.scan(**kwargs)
            items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return items

    @staticmethod
    def _item_to_row(item: Dict[str, Any]) -> Dict[str, str]:
        def _s(field: str) -> str:
            v = item.get(field, {})
            return v.get("S") or v.get("N") or ""

        updated_raw = item.get("updated_at", {})
        updated_n = updated_raw.get("N") or updated_raw.get("S", "0")
        try:
            ts = int(float(updated_n))
            updated = datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%MZ")
        except (ValueError, OSError):
            updated = updated_n

        return {
            "mission_id": _s("mission_id"),
            "user_state": _s("status"),
            "ship_class": _s("ship_class"),
            "task_id": _s("task_id"),
            "updated": updated,
        }

    @staticmethod
    def _state_rank(state: str) -> int:
        return {
            "NEED_APPROVAL": 0,
            "NEED_INPUT": 1,
            "RUNNING": 2,
            "ENQUEUED": 3,
            "FAILED": 4,
            "BUDGET_DENIED": 5,
            "CANCELLED": 6,
            "DONE": 7,
        }.get(state, 99)

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        return text[: max(limit - 3, 0)] + "..."

    def _build_aws_watch_layout(
        self,
        rows: List[Dict[str, str]],
        selected_mission: str,
        comms: List[Dict[str, Any]],
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

        tbl = Table(expand=True)
        tbl.add_column("Mission", overflow="fold")
        tbl.add_column("State", width=16)
        tbl.add_column("Ship", width=5)
        tbl.add_column("Updated", width=13)

        if not rows:
            tbl.add_row("-", "no missions", "-", "-")
        else:
            for row in rows:
                mid = row["mission_id"]
                state = row["user_state"]
                style = "bold red" if state in {"NEED_APPROVAL", "NEED_INPUT"} else ""
                marker = ">" if mid == selected_mission else " "
                tbl.add_row(
                    f"{marker} {mid}",
                    Text(state, style=style),
                    row.get("ship_class", ""),
                    row.get("updated", ""),
                )

        filter_label = f" filter={mission_filter}" if mission_filter else ""
        layout["left"].update(Panel(tbl, title=f"Missions page={page}/{total_pages}{filter_label}"))

        comms_text = Text()
        if not comms:
            comms_text.append("No comms\n")
        else:
            for c in comms[-25:]:
                ctype = str(c.get("type", ""))
                sender = str(c.get("from", "?"))
                msg = self._truncate(str(c.get("content", "")), 120)
                is_need = ctype in {"need_approval", "need_input"}
                style = "bold red" if is_need else ""
                comms_text.append(f"[{sender}] {ctype}\n{msg}\n\n", style=style)
        layout["right"].update(Panel(comms_text, title=f"Comms ({selected_mission or '-'})"))

        return layout

    # ------------------------------------------------------------------ #
    # run() helpers                                                        #
    # ------------------------------------------------------------------ #

    def _put_ticket(
        self,
        task_id: str,
        *,
        objective: str,
        ticket_file: str,
        cfg: Dict[str, str],
    ) -> str:
        """Upload ticket to S3 and return the s3:// URI."""
        bucket = cfg["bucket"]
        region = cfg["region"]
        if not bucket:
            typer.echo("[ERROR] FLEET_S3_BUCKET is not set", err=True)
            raise typer.Exit(2)

        if ticket_file:
            content = Path(ticket_file).expanduser().read_text(encoding="utf-8")
        else:
            content = f"# Mission Ticket\n\ntask_id: {task_id}\n\n{objective.strip()}\n"

        key = f"tickets/{task_id}.md"
        s3 = boto3.client("s3", region_name=region)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )
        return f"s3://{bucket}/{key}"

    def _get_task_status_ddb(
        self, task_id: str, ddb: Any, cfg: Dict[str, str]
    ) -> tuple[str, str]:
        """Return (status, current_stage) from DynamoDB fleet-task-state."""
        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task status: {exc}", err=True)
            raise typer.Exit(1)
        item = resp.get("Item")
        if not item:
            return ("UNKNOWN", "")
        status = item.get("status", {}).get("S", "UNKNOWN")
        current_stage = item.get("current_stage", {}).get("S", "")
        return status, current_stage

    def _get_ca_confidence(self, task_id: str, ddb: Any, cfg: Dict[str, str]) -> int:
        """Return CA mission confidence_level from DynamoDB, or 0 on failure."""
        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError):
            return 0
        item = resp.get("Item")
        if not item:
            return 0
        mission_id = self._resolve_ca_mission_id(item)
        if not mission_id:
            return 0
        try:
            resp2 = ddb.get_item(
                TableName=cfg["state_table"],
                Key={"mission_id": {"S": mission_id}},
            )
        except (ClientError, BotoCoreError):
            return 0
        m_item = resp2.get("Item")
        if not m_item:
            return 0
        cl = m_item.get("confidence_level", {})
        raw = cl.get("N") or cl.get("S", "0")
        try:
            return int(float(raw))
        except (ValueError, TypeError):
            return 0

    def _auto_approve_task_missions(
        self,
        task_id: str,
        ddb: Any,
        sqs_client: Any,
        s3_client: Any,
        cfg: Dict[str, str],
        note: str,
    ) -> int:
        """Approve all NEED_APPROVAL missions in the task. Returns count approved."""
        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task item: {exc}", err=True)
            return 0
        item = resp.get("Item")
        if not item:
            return 0

        stages = item.get("stages", {}).get("M", {})
        approved = 0
        for _stage_name, stage_val in stages.items():
            stage_m = stage_val.get("M", {})
            stage_status = stage_m.get("status", {}).get("S", "")
            if stage_status != "NEED_APPROVAL":
                continue
            mission_id_val = stage_m.get("mission_id", {}).get("S", "")
            if not mission_id_val:
                continue
            try:
                self._resume_mission(
                    ddb=ddb,
                    sqs_client=sqs_client,
                    s3=s3_client,
                    mission_id=mission_id_val,
                    expected_status="NEED_APPROVAL",
                    new_status="RUNNING",
                    response_text=note,
                    state_table=cfg["state_table"],
                    sqs_name=cfg["sqs_name"],
                    bucket=cfg["bucket"],
                )
                approved += 1
            except (typer.Exit, Exception):
                pass
        return approved

    def _input_task_missions(
        self,
        task_id: str,
        ddb: Any,
        sqs_client: Any,
        s3_client: Any,
        cfg: Dict[str, str],
        message: str,
    ) -> int:
        """Send input to all NEED_INPUT missions in the task. Returns count sent."""
        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task item: {exc}", err=True)
            return 0
        item = resp.get("Item")
        if not item:
            return 0

        stages = item.get("stages", {}).get("M", {})
        sent = 0
        for _stage_name, stage_val in stages.items():
            stage_m = stage_val.get("M", {})
            stage_status = stage_m.get("status", {}).get("S", "")
            if stage_status != "NEED_INPUT":
                continue
            mission_id_val = stage_m.get("mission_id", {}).get("S", "")
            if not mission_id_val:
                continue
            try:
                self._resume_mission(
                    ddb=ddb,
                    sqs_client=sqs_client,
                    s3=s3_client,
                    mission_id=mission_id_val,
                    expected_status="NEED_INPUT",
                    new_status="RUNNING",
                    response_text=message,
                    state_table=cfg["state_table"],
                    sqs_name=cfg["sqs_name"],
                    bucket=cfg["bucket"],
                )
                sent += 1
            except (typer.Exit, Exception):
                pass
        return sent

    @staticmethod
    def _timed_input(prompt: str, timeout: int) -> Optional[str]:
        """Show prompt and wait up to timeout seconds for input. Returns None on timeout."""
        import threading

        result: List[Optional[str]] = [None]
        ready = threading.Event()

        def _reader() -> None:
            try:
                result[0] = input(prompt)
            except (EOFError, KeyboardInterrupt):
                result[0] = None
            finally:
                ready.set()

        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        ready.wait(timeout)
        return result[0]

    def run(
        self,
        *,
        objective: str,
        ticket_file: str,
        task_id: str,
        repo_url: str,
        doctrine: str,
        budget: Optional[str],
        interval: int,
        auto_approve: bool,
        auto_approve_threshold: int,
    ) -> None:
        """1コマンドで投入からHITL対応まで完結するメインループ。"""
        from naval.notify import notify
        from rich.console import Console

        console = Console()
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        budget_month = budget or self.ctx.now_month()

        if not task_id:
            task_id = f"T-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

        ddb = boto3.client("dynamodb", region_name=region)
        sqs_client = boto3.client("sqs", region_name=region)
        s3_client = boto3.client("s3", region_name=region) if cfg["bucket"] else None

        # 1. チケットS3アップロード
        ticket_s3 = self._put_ticket(
            task_id,
            objective=objective,
            ticket_file=ticket_file,
            cfg=cfg,
        )

        # 2. task_orchestrator init / start
        self.run_script(
            "task_orchestrator.py",
            ["init", task_id, ticket_s3, budget_month, "--formation", doctrine],
        )
        start_args = ["start", task_id]
        if repo_url:
            start_args.append(repo_url)
        self.run_script("task_orchestrator.py", start_args)

        console.print(f"[bold green]Task started:[/bold green] {task_id}")
        if auto_approve:
            console.print(
                f"[cyan]Auto-approve enabled (threshold={auto_approve_threshold})[/cyan]"
            )
        console.print(
            "[dim]Ctrl+C to stop watching  |  RUNNING 中は Enter で CA 指令、q で終了[/dim]"
        )

        # 3. 監視ループ
        terminal = {"DONE", "FAILED", "BUDGET_DENIED"}
        last_state: tuple[Optional[str], Optional[str]] = (None, None)

        try:
            while True:
                status, stage = self._get_task_status_ddb(task_id, ddb, cfg)
                state = (status, stage)
                if state != last_state:
                    console.print(f"  status=[bold]{status}[/bold]  stage={stage}")
                    last_state = state

                if status in terminal:
                    color = "green" if status == "DONE" else "red"
                    console.print(f"[bold {color}]Finished: {status}[/bold {color}]")
                    notify("Naval Battle", f"{task_id} {status}")
                    break

                advance_args = ["advance", task_id]
                if repo_url:
                    advance_args.append(repo_url)

                if status == "NEED_APPROVAL":
                    confidence = self._get_ca_confidence(task_id, ddb, cfg)
                    if auto_approve and confidence >= auto_approve_threshold:
                        n = self._auto_approve_task_missions(
                            task_id,
                            ddb,
                            sqs_client,
                            s3_client,
                            cfg,
                            f"Auto-approved (confidence={confidence})",
                        )
                        console.print(
                            f"[green]Auto-approved {n} mission(s) (confidence={confidence})[/green]"
                        )
                        self.run_script("task_orchestrator.py", advance_args)
                    else:
                        notify(
                            "Naval Battle",
                            f"承認待ち: {task_id}  確信度={confidence}",
                        )
                        console.print(
                            f"[bold yellow]Approval required[/bold yellow]"
                            f"  confidence=[bold]{confidence}[/bold]/{auto_approve_threshold}"
                        )
                        ans = input("Approve? [y/N] (note after space, e.g. 'y looks good'): ").strip()
                        if ans.lower().startswith("y"):
                            note_text = ans[1:].strip() or f"Approved (confidence={confidence})"
                            n = self._auto_approve_task_missions(
                                task_id, ddb, sqs_client, s3_client, cfg, note_text
                            )
                            console.print(f"[green]Approved {n} mission(s).[/green]")
                            self.run_script("task_orchestrator.py", advance_args)
                        else:
                            console.print("[dim]Skipped. Will poll again.[/dim]")
                            time.sleep(interval)

                elif status == "NEED_INPUT":
                    notify("Naval Battle", f"入力待ち: {task_id}")
                    console.print("[bold yellow]Input required[/bold yellow]")
                    console.print("Enter response (Ctrl+D to finish, empty = skip):")
                    lines_in: List[str] = []
                    try:
                        while True:
                            lines_in.append(input())
                    except EOFError:
                        pass
                    message = "\n".join(lines_in).strip()
                    if message:
                        n = self._input_task_missions(
                            task_id, ddb, sqs_client, s3_client, cfg, message
                        )
                        console.print(f"[green]Input sent to {n} mission(s).[/green]")
                        self.run_script("task_orchestrator.py", advance_args)
                    else:
                        console.print("[dim]No input. Will poll again.[/dim]")
                        time.sleep(interval)

                elif status in ("RUNNING", "ENQUEUED"):
                    self.run_script("task_orchestrator.py", advance_args)
                    # タイムアウト付き対話待機
                    user_in = self._timed_input(
                        f"  [{interval}s] CA指令を入力 (Enter=スキップ, q=終了): ",
                        interval,
                    )
                    if user_in is None:
                        pass  # timeout — continue
                    elif user_in.strip().lower() == "q":
                        console.print("[yellow]Watch stopped.[/yellow]")
                        break
                    elif user_in.strip():
                        directive = user_in.strip()
                        # S3 に CA 指令書を保存
                        now_ts = int(time.time())
                        now_iso = (
                            datetime.fromtimestamp(now_ts, timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                        content = (
                            f"# Human CA Directive\n"
                            f"task_id: {task_id}\n"
                            f"updated_at: {now_iso}\n\n"
                            f"{directive}\n"
                        )
                        try:
                            resp = ddb.get_item(
                                TableName=cfg["task_table"],
                                Key={"task_id": {"S": task_id}},
                            )
                            mission_id = self._resolve_ca_mission_id(resp.get("Item", {}))
                            if mission_id and s3_client and cfg["bucket"]:
                                key = f"missions/{mission_id}/artifacts/ca_human_directive.md"
                                s3_client.put_object(
                                    Bucket=cfg["bucket"],
                                    Key=key,
                                    Body=content.encode("utf-8"),
                                    ContentType="text/markdown; charset=utf-8",
                                )
                                console.print(
                                    f"[green]CA directive saved → mission {mission_id}[/green]"
                                )
                                self.run_script("task_orchestrator.py", advance_args)
                            else:
                                console.print("[yellow]CA mission not found; directive not saved.[/yellow]")
                        except Exception as exc:
                            console.print(f"[red]Failed to save CA directive: {exc}[/red]")

                else:
                    console.print(f"[yellow]Unknown status: {status}[/yellow]")
                    time.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]Watch stopped (Ctrl+C).[/yellow]")

    # ------------------------------------------------------------------ #
    # abort / show / retry                                                 #
    # ------------------------------------------------------------------ #

    def _cancel_mission_force(self, ddb: Any, mission_id: str, state_table: str) -> bool:
        """ステータスを問わず CANCELLED に更新する。失敗時は False を返す。"""
        now = int(time.time())
        try:
            ddb.update_item(
                TableName=state_table,
                Key={"mission_id": {"S": mission_id}},
                UpdateExpression=(
                    "SET #s=:cancelled, updated_at=:now"
                    " REMOVE needs_input, needs_approval, owner, lock_until"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":cancelled": {"S": "CANCELLED"},
                    ":now": {"N": str(now)},
                },
            )
            return True
        except (ClientError, BotoCoreError):
            return False

    def abort(self, *, task_id: str, note: str) -> None:
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        ddb = boto3.client("dynamodb", region_name=region)

        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task: {exc}", err=True)
            raise typer.Exit(1)

        item = resp.get("Item")
        if not item:
            typer.echo(f"[ERROR] task not found: {task_id}", err=True)
            raise typer.Exit(1)

        current_status = item.get("status", {}).get("S", "")
        if current_status in ("DONE", "CANCELLED"):
            typer.echo(f"[WARN] task is already {current_status}, nothing to abort.")
            return

        # アクティブなミッションを全て CANCELLED に
        active = {"RUNNING", "ENQUEUED", "NEED_APPROVAL", "NEED_INPUT"}
        stages = item.get("stages", {}).get("M", {})
        cancelled_count = 0
        for stage_val in stages.values():
            stage_m = stage_val.get("M", {})
            if stage_m.get("status", {}).get("S", "") not in active:
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if mid and self._cancel_mission_force(ddb, mid, cfg["state_table"]):
                cancelled_count += 1

        # タスク自体を CANCELLED に
        now = int(time.time())
        abort_reason = note or "Aborted by user"
        try:
            ddb.update_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
                UpdateExpression=(
                    "SET #s=:cancelled, updated_at=:now, abort_reason=:reason"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":cancelled": {"S": "CANCELLED"},
                    ":now": {"N": str(now)},
                    ":reason": {"S": abort_reason},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to cancel task: {exc}", err=True)
            raise typer.Exit(1)

        typer.echo(
            f"aborted: task_id={task_id}"
            f"  missions_cancelled={cancelled_count}"
            f"  reason={abort_reason!r}"
        )

    def show(self, *, task_id: str) -> None:
        try:
            from rich.console import Console
            from rich.panel import Panel
            from rich.table import Table
            from rich.text import Text
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        cfg = self._resolve_aws_config()
        region = cfg["region"]
        ddb = boto3.client("dynamodb", region_name=region)
        console = Console()

        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task: {exc}", err=True)
            raise typer.Exit(1)

        item = resp.get("Item")
        if not item:
            typer.echo(f"[ERROR] task not found: {task_id}", err=True)
            raise typer.Exit(1)

        def _s(field: str) -> str:
            v = item.get(field, {})
            return v.get("S") or v.get("N") or ""

        task_status = _s("status")
        current_stage = _s("current_stage")
        budget_month = _s("budget_month")

        updated_raw = item.get("updated_at", {})
        updated_n = updated_raw.get("N") or updated_raw.get("S", "0")
        try:
            updated_str = datetime.fromtimestamp(int(float(updated_n)), timezone.utc).strftime(
                "%Y-%m-%d %H:%MZ"
            )
        except (ValueError, OSError):
            updated_str = updated_n

        status_color = {
            "DONE": "green",
            "FAILED": "red",
            "CANCELLED": "yellow",
            "RUNNING": "cyan",
            "NEED_APPROVAL": "bold red",
            "NEED_INPUT": "bold red",
        }.get(task_status, "white")

        console.print(
            Panel(
                f"task_id=[bold]{task_id}[/bold]  "
                f"status=[{status_color}]{task_status}[/{status_color}]  "
                f"stage={current_stage or '-'}  "
                f"budget={budget_month or '-'}  "
                f"updated={updated_str}",
                title="Task",
            )
        )

        stages = item.get("stages", {}).get("M", {})
        if not stages:
            console.print("[dim]No stage info.[/dim]")
            return

        # ミッションの詳細を一括取得
        mission_items: Dict[str, Any] = {}
        mid_list = [
            v.get("M", {}).get("mission_id", {}).get("S", "")
            for v in stages.values()
            if v.get("M", {}).get("mission_id", {}).get("S", "")
        ]
        for mid in mid_list:
            try:
                r = ddb.get_item(
                    TableName=cfg["state_table"],
                    Key={"mission_id": {"S": mid}},
                )
                if "Item" in r:
                    mission_items[mid] = r["Item"]
            except (ClientError, BotoCoreError):
                pass

        tbl = Table(show_lines=True)
        tbl.add_column("Stage", style="bold cyan", no_wrap=True)
        tbl.add_column("Status", width=16)
        tbl.add_column("Mission ID", overflow="fold")
        tbl.add_column("Ship", width=5)
        tbl.add_column("Conf", width=5, justify="right")
        tbl.add_column("Updated", width=13)

        stage_status_rank = {
            "NEED_APPROVAL": 0, "NEED_INPUT": 1, "RUNNING": 2,
            "ENQUEUED": 3, "FAILED": 4, "DONE": 5, "CANCELLED": 6,
        }
        sorted_stages = sorted(
            stages.items(),
            key=lambda kv: stage_status_rank.get(
                kv[1].get("M", {}).get("status", {}).get("S", ""), 99
            ),
        )

        for stage_name, stage_val in sorted_stages:
            stage_m = stage_val.get("M", {})
            st = stage_m.get("status", {}).get("S", "-")
            mid = stage_m.get("mission_id", {}).get("S", "-")
            ship = stage_m.get("ship_class", {}).get("S", "")

            # ミッション詳細から補完
            m_item = mission_items.get(mid, {})
            if not ship:
                ship = m_item.get("ship_class", {}).get("S", "")
            conf_raw = m_item.get("confidence_level", {})
            conf = conf_raw.get("N") or conf_raw.get("S", "")
            m_updated = m_item.get("updated_at", {})
            m_upd_n = m_updated.get("N") or m_updated.get("S", "0")
            try:
                m_upd_str = datetime.fromtimestamp(int(float(m_upd_n)), timezone.utc).strftime(
                    "%m-%d %H:%MZ"
                )
            except (ValueError, OSError):
                m_upd_str = ""

            st_color = {
                "DONE": "green", "FAILED": "red", "CANCELLED": "dim",
                "RUNNING": "cyan", "NEED_APPROVAL": "bold red", "NEED_INPUT": "bold red",
            }.get(st, "")
            tbl.add_row(
                stage_name,
                Text(st, style=st_color),
                mid if mid != "-" else "",
                ship,
                conf,
                m_upd_str,
            )

        console.print(tbl)

        abort_reason = item.get("abort_reason", {}).get("S", "")
        if abort_reason:
            console.print(f"[dim]abort_reason: {abort_reason}[/dim]")

    def retry(self, *, task_id: str, note: str) -> None:
        cfg = self._resolve_aws_config()
        region = cfg["region"]
        ddb = boto3.client("dynamodb", region_name=region)
        sqs_client = boto3.client("sqs", region_name=region)
        s3_client = boto3.client("s3", region_name=region) if cfg["bucket"] else None

        try:
            resp = ddb.get_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to get task: {exc}", err=True)
            raise typer.Exit(1)

        item = resp.get("Item")
        if not item:
            typer.echo(f"[ERROR] task not found: {task_id}", err=True)
            raise typer.Exit(1)

        stages = item.get("stages", {}).get("M", {})
        response_text = note or "Retried by user"
        retried = 0
        errors = 0

        for stage_val in stages.values():
            stage_m = stage_val.get("M", {})
            if stage_m.get("status", {}).get("S", "") != "FAILED":
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if not mid:
                continue
            try:
                self._resume_mission(
                    ddb=ddb,
                    sqs_client=sqs_client,
                    s3=s3_client,
                    mission_id=mid,
                    expected_status="FAILED",
                    new_status="RUNNING",
                    response_text=response_text,
                    state_table=cfg["state_table"],
                    sqs_name=cfg["sqs_name"],
                    bucket=cfg["bucket"],
                )
                retried += 1
            except (typer.Exit, Exception) as exc:
                typer.echo(f"[WARN] could not retry mission {mid}: {exc}")
                errors += 1

        if retried == 0:
            typer.echo(f"No FAILED missions found for task {task_id}.")
            return

        # タスクステータスを RUNNING に戻す
        now = int(time.time())
        try:
            ddb.update_item(
                TableName=cfg["task_table"],
                Key={"task_id": {"S": task_id}},
                UpdateExpression="SET #s=:running, updated_at=:now",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":running": {"S": "RUNNING"},
                    ":now": {"N": str(now)},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[WARN] failed to reset task status: {exc}")

        typer.echo(
            f"retried: task_id={task_id}"
            f"  missions_retried={retried}"
            + (f"  errors={errors}" if errors else "")
        )

    def pending(self) -> None:
        """NEED_INPUT / NEED_APPROVAL なミッションを一覧表示して対話応答する。"""
        try:
            from rich.console import Console
            from rich.panel import Panel
            from rich.table import Table
            from rich.text import Text
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        cfg = self._resolve_aws_config()
        region = cfg["region"]
        state_table = cfg["state_table"]
        bucket = cfg["bucket"]
        sqs_name = cfg["sqs_name"]

        ddb = boto3.client("dynamodb", region_name=region)
        s3_client = boto3.client("s3", region_name=region) if bucket else None
        sqs_client = boto3.client("sqs", region_name=region)
        console = Console()

        # NEED_INPUT / NEED_APPROVAL のミッションをスキャン
        try:
            resp = ddb.scan(
                TableName=state_table,
                FilterExpression="#s IN (:input, :approval)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":input": {"S": "NEED_INPUT"},
                    ":approval": {"S": "NEED_APPROVAL"},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            typer.echo(f"[ERROR] failed to scan missions: {exc}", err=True)
            raise typer.Exit(1)

        items = resp.get("Items", [])
        if not items:
            console.print("[green]No missions pending human input/approval.[/green]")
            return

        rows = [self._item_to_row(i) for i in items]
        rows.sort(key=lambda r: (self._state_rank(r["user_state"]), r["mission_id"]))

        # ミッション一覧テーブル
        tbl = Table(title="Pending Missions", show_lines=True)
        tbl.add_column("#", style="bold cyan", width=3)
        tbl.add_column("Status", style="bold red", width=16)
        tbl.add_column("Mission ID")
        tbl.add_column("Ship", width=5)
        tbl.add_column("Task")
        tbl.add_column("Updated", width=13)

        for i, row in enumerate(rows, 1):
            tbl.add_row(
                str(i),
                row["user_state"],
                row["mission_id"],
                row["ship_class"],
                row["task_id"],
                row["updated"],
            )
        console.print(tbl)

        # ミッション選択
        try:
            choice = input("\nSelect mission number (q=quit): ").strip()
        except (EOFError, KeyboardInterrupt):
            return
        if choice.lower() == "q":
            return
        try:
            idx = int(choice) - 1
            if not (0 <= idx < len(rows)):
                raise ValueError
        except ValueError:
            typer.echo("[ERROR] invalid selection", err=True)
            raise typer.Exit(1)

        selected = rows[idx]
        mission_id = selected["mission_id"]
        status = selected["user_state"]

        console.rule(f"[bold]{mission_id}[/bold]")
        console.print(f"Status : [bold red]{status}[/bold red]")
        console.print(f"Ship   : {selected['ship_class']}  |  Task : {selected['task_id']}")

        # 最近の通信ログを表示
        if s3_client and bucket:
            comms = self._list_comms(s3_client, bucket, mission_id)
            if comms:
                lines = "\n".join(
                    f"[{c.get('from', '?')}] {self._truncate(str(c.get('content', '')), 100)}"
                    for c in comms[-5:]
                )
                console.print(Panel(lines, title="Recent Communications"))

        # 応答入力
        if status == "NEED_APPROVAL":
            try:
                answer = input("\nApprove? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return
            yes = answer in ("y", "yes")
            new_status = "RUNNING" if yes else "FAILED"
            response_text = "approved" if yes else "rejected"

        elif status == "NEED_INPUT":
            console.print("\nEnter response (Ctrl+D / Ctrl+Z to finish):")
            lines_in: List[str] = []
            try:
                while True:
                    lines_in.append(input())
            except EOFError:
                pass
            except KeyboardInterrupt:
                return
            response_text = "\n".join(lines_in).strip()
            if not response_text:
                typer.echo("No response provided, aborting.")
                return
            new_status = "RUNNING"

        else:
            typer.echo(f"[ERROR] unexpected status: {status}", err=True)
            raise typer.Exit(1)

        self._resume_mission(
            ddb=ddb,
            sqs_client=sqs_client,
            s3=s3_client,
            mission_id=mission_id,
            expected_status=status,
            new_status=new_status,
            response_text=response_text,
            state_table=state_table,
            sqs_name=sqs_name,
            bucket=bucket,
        )

        if new_status == "RUNNING":
            console.print(f"[green]Mission {mission_id} resumed.[/green]")
        else:
            console.print(f"[yellow]Mission {mission_id} rejected.[/yellow]")

    def watch(
        self,
        *,
        month: Optional[str],
        interval: int,
        mission_filter: str,
        page_size: int,
        page: int,
    ) -> None:
        try:
            from rich.console import Console
            from rich.live import Live
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        cfg = self._resolve_aws_config()
        region = cfg["region"]
        state_table = cfg["state_table"]
        bucket = cfg["bucket"]

        ddb = boto3.client("dynamodb", region_name=region)
        s3_client = boto3.client("s3", region_name=region) if bucket else None

        refresh_sec = max(1, interval)
        page_size = max(1, min(page_size, 200))
        page = max(1, page)
        filter_norm = mission_filter.strip().lower()
        console = Console()
        selected_mission = ""

        try:
            with Live(console=console, screen=True, auto_refresh=False) as live:
                while True:
                    try:
                        items = self._scan_missions(ddb, state_table)
                    except (ClientError, BotoCoreError) as exc:
                        from rich.panel import Panel
                        live.update(Panel(f"[red]Error: {exc}[/red]", title="watch"), refresh=True)
                        time.sleep(refresh_sec)
                        continue

                    rows = [self._item_to_row(i) for i in items]
                    rows.sort(key=lambda r: (self._state_rank(r["user_state"]), r["mission_id"]))

                    # オプションのフィルタ（月 or 文字列）
                    budget_month = month or self.ctx.now_month()
                    if filter_norm:
                        rows = [
                            r for r in rows
                            if filter_norm in r["mission_id"].lower()
                            or filter_norm in r["user_state"].lower()
                        ]
                    elif month:
                        # updated フィールドは MM-DD HH:MMZ 形式なので month フィルタは task_id で代用
                        rows = [r for r in rows if budget_month in r.get("task_id", "")]

                    total_pages = max(1, (len(rows) + page_size - 1) // page_size)
                    page_use = min(page, total_pages)
                    start = (page_use - 1) * page_size
                    rows_page = rows[start: start + page_size]

                    # フォーカスミッションを決定
                    need_rows = [r for r in rows_page if r["user_state"] in {"NEED_APPROVAL", "NEED_INPUT"}]
                    if need_rows:
                        selected_mission = need_rows[0]["mission_id"]
                    elif not selected_mission and rows_page:
                        selected_mission = rows_page[0]["mission_id"]
                    elif selected_mission and all(r["mission_id"] != selected_mission for r in rows_page):
                        selected_mission = rows_page[0]["mission_id"] if rows_page else ""

                    comms: List[Dict[str, Any]] = []
                    if selected_mission and s3_client and bucket:
                        comms = self._list_comms(s3_client, bucket, selected_mission)

                    layout = self._build_aws_watch_layout(
                        rows=rows_page,
                        selected_mission=selected_mission,
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

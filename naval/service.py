"""Fleet service layer: pure data operations shared by CLI and Web GUI.

This module contains the AWS-runtime business logic extracted from
``naval.runtime.aws_runtime`` so that results are returned as structured
dataclasses instead of being printed.  Presentation (typer/rich output)
stays in the CLI layer; the FastAPI web layer consumes the same service.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class FleetServiceError(Exception):
    """Operation failed; message is safe to show to the user."""


class MissionNotFound(FleetServiceError):
    pass


class TaskNotFound(FleetServiceError):
    pass


class TaskAlreadyFinished(FleetServiceError):
    pass


# ------------------------------------------------------------------ #
# Structured results                                                   #
# ------------------------------------------------------------------ #


@dataclass
class MissionSummary:
    mission_id: str
    status: str
    ship_class: str
    task_id: str
    updated_at: int
    confidence: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CommEntry:
    ts: int
    sender: str
    to: str
    type: str
    content: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MissionDetail:
    summary: MissionSummary
    human_response: str = ""
    fail_reason: str = ""
    comms: List[CommEntry] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.summary.to_dict(),
            "human_response": self.human_response,
            "fail_reason": self.fail_reason,
            "comms": [c.to_dict() for c in self.comms],
        }


@dataclass
class StageStatus:
    stage: str
    status: str
    mission_id: str
    ship_class: str
    confidence: Optional[int]
    updated_at: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TaskDetail:
    task_id: str
    status: str
    current_stage: str
    budget_month: str
    updated_at: int
    abort_reason: str
    stages: List[StageStatus] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "current_stage": self.current_stage,
            "budget_month": self.budget_month,
            "updated_at": self.updated_at,
            "abort_reason": self.abort_reason,
            "stages": [s.to_dict() for s in self.stages],
        }


@dataclass
class BudgetStatus:
    month: str
    fuel_cap: float
    fuel_remaining: float
    fuel_burned: float
    sorties_total: int
    ammo_used: Dict[str, int]
    ammo_caps: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ArtifactFile:
    key: str
    path: str
    size: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


STATE_RANK = {
    "NEED_APPROVAL": 0,
    "NEED_INPUT": 1,
    "RUNNING": 2,
    "ENQUEUED": 3,
    "FAILED": 4,
    "BUDGET_DENIED": 5,
    "CANCELLED": 6,
    "DONE": 7,
}


def state_rank(state: str) -> int:
    return STATE_RANK.get(state, 99)


def resolve_aws_config(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    e = env if env is not None else dict(os.environ)
    region = e.get("FLEET_REGION", "ap-northeast-1")
    state_table = e.get("FLEET_STATE_TABLE", "fleet-mission-state")
    task_table = e.get("FLEET_TASK_STATE_TABLE", "fleet-task-state")
    budget_table = e.get("FLEET_BUDGET_TABLE", "fleet-budget")
    sqs_name = e.get("FLEET_SQS_NAME", "fleet-missions")
    bucket = e.get("FLEET_S3_BUCKET", "")
    try:
        from fleet_config import resolve_fleet_config

        cfg = resolve_fleet_config()
        region = cfg.get("region") or region
        state_table = cfg.get("state_table") or state_table
        sqs_name = cfg.get("sqs_name") or sqs_name
        bucket = cfg.get("bucket") or bucket
        budget_table = cfg.get("budget_table") or budget_table
    except Exception:
        pass
    return {
        "region": region,
        "state_table": state_table,
        "task_table": task_table,
        "budget_table": budget_table,
        "sqs_name": sqs_name,
        "bucket": bucket,
    }


def _attr(item: Dict[str, Any], field_name: str) -> str:
    v = item.get(field_name, {})
    return v.get("S") or v.get("N") or ""


def _attr_int(item: Dict[str, Any], field_name: str) -> int:
    raw = _attr(item, field_name)
    try:
        return int(float(raw))
    except (ValueError, TypeError):
        return 0


def _attr_opt_int(item: Dict[str, Any], field_name: str) -> Optional[int]:
    if field_name not in item:
        return None
    raw = _attr(item, field_name)
    try:
        return int(float(raw))
    except (ValueError, TypeError):
        return None


def mission_item_to_summary(item: Dict[str, Any]) -> MissionSummary:
    return MissionSummary(
        mission_id=_attr(item, "mission_id"),
        status=_attr(item, "status"),
        ship_class=_attr(item, "ship_class"),
        task_id=_attr(item, "task_id"),
        updated_at=_attr_int(item, "updated_at"),
        confidence=_attr_opt_int(item, "confidence_level"),
    )


class FleetService:
    """Structured-data operations against the AWS fleet backend.

    boto3 clients can be injected for testing via the ``clients`` mapping
    (keys: ``ddb``, ``sqs``, ``s3``).
    """

    def __init__(
        self,
        *,
        env: Optional[Dict[str, str]] = None,
        repo_root: Optional[Path] = None,
        clients: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.cfg = resolve_aws_config(env)
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        self._env = env
        self._clients: Dict[str, Any] = dict(clients or {})

    # -------------------------------------------------------------- #
    # clients                                                          #
    # -------------------------------------------------------------- #

    def _client(self, name: str) -> Any:
        if name not in self._clients:
            service = {"ddb": "dynamodb", "sqs": "sqs", "s3": "s3"}[name]
            self._clients[name] = boto3.client(service, region_name=self.cfg["region"])
        return self._clients[name]

    @property
    def ddb(self) -> Any:
        return self._client("ddb")

    @property
    def sqs(self) -> Any:
        return self._client("sqs")

    @property
    def s3(self) -> Any:
        return self._client("s3")

    # -------------------------------------------------------------- #
    # missions                                                         #
    # -------------------------------------------------------------- #

    def list_missions(self) -> List[MissionSummary]:
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {"TableName": self.cfg["state_table"]}
        try:
            while True:
                resp = self.ddb.scan(**kwargs)
                items.extend(resp.get("Items", []))
                if "LastEvaluatedKey" not in resp:
                    break
                kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to scan missions: {exc}") from exc
        rows = [mission_item_to_summary(i) for i in items]
        rows.sort(key=lambda r: (state_rank(r.status), r.mission_id))
        return rows

    def list_pending(self) -> List[MissionSummary]:
        try:
            resp = self.ddb.scan(
                TableName=self.cfg["state_table"],
                FilterExpression="#s IN (:input, :approval)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":input": {"S": "NEED_INPUT"},
                    ":approval": {"S": "NEED_APPROVAL"},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to scan missions: {exc}") from exc
        rows = [mission_item_to_summary(i) for i in resp.get("Items", [])]
        rows.sort(key=lambda r: (state_rank(r.status), r.mission_id))
        return rows

    def _get_mission_item(self, mission_id: str) -> Dict[str, Any]:
        try:
            resp = self.ddb.get_item(
                TableName=self.cfg["state_table"],
                Key={"mission_id": {"S": mission_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to get mission: {exc}") from exc
        item = resp.get("Item")
        if not item:
            raise MissionNotFound(f"mission not found: {mission_id}")
        return item

    def get_mission(self, mission_id: str, *, with_comms: bool = True) -> MissionDetail:
        item = self._get_mission_item(mission_id)
        detail = MissionDetail(
            summary=mission_item_to_summary(item),
            human_response=_attr(item, "human_response"),
            fail_reason=_attr(item, "fail_reason"),
        )
        if with_comms and self.cfg["bucket"]:
            detail.comms = self.list_comms(mission_id)
        return detail

    def list_comms(self, mission_id: str) -> List[CommEntry]:
        bucket = self.cfg["bucket"]
        if not bucket:
            return []
        try:
            prefix = f"missions/{mission_id}/comms/"
            comms: List[CommEntry] = []
            for key in self._list_s3_keys(bucket, prefix):
                try:
                    obj = self.s3.get_object(Bucket=bucket, Key=key)
                    raw = json.loads(obj["Body"].read().decode("utf-8"))
                    comms.append(
                        CommEntry(
                            ts=int(raw.get("ts", 0)),
                            sender=str(raw.get("from", "?")),
                            to=str(raw.get("to", "")),
                            type=str(raw.get("type", "")),
                            content=str(raw.get("content", "")),
                        )
                    )
                except Exception:
                    continue
            comms.sort(key=lambda c: c.ts)
            return comms
        except Exception:
            return []

    def _list_s3_keys(self, bucket: str, prefix: str) -> List[str]:
        keys: List[str] = []
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
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

    def _get_mission_payload(self, mission_id: str) -> Dict[str, Any]:
        bucket = self.cfg["bucket"]
        if not bucket:
            return {}
        try:
            key = f"missions/{mission_id}/orders/payload.json"
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            return {}

    # -------------------------------------------------------------- #
    # HITL signals                                                     #
    # -------------------------------------------------------------- #

    def resume_mission(
        self,
        mission_id: str,
        *,
        expected_status: str,
        new_status: str,
        response_text: str,
    ) -> None:
        """DynamoDB の状態遷移 + （RUNNING時）SQS 再投入。"""
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

        item = self._get_mission_item(mission_id)
        current_status = _attr(item, "status")
        if current_status != expected_status:
            raise FleetServiceError(
                f"mission status is {current_status!r}, expected {expected_status!r}"
            )

        try:
            self.ddb.update_item(
                TableName=self.cfg["state_table"],
                Key={"mission_id": {"S": mission_id}},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression="#s=:from",
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to update mission: {exc}") from exc

        if new_status == "RUNNING":
            try:
                queue_url = self.sqs.get_queue_url(QueueName=self.cfg["sqs_name"])["QueueUrl"]
                payload = self._get_mission_payload(mission_id)
                if not payload:
                    payload = {
                        "mission_id": mission_id,
                        "ship_class": _attr(item, "ship_class"),
                        "task_id": _attr(item, "task_id"),
                        "ship_id": _attr(item, "ship_id"),
                    }
                payload["resume"] = True
                self.sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(payload, ensure_ascii=False),
                )
            except (ClientError, BotoCoreError) as exc:
                raise FleetServiceError(f"failed to re-enqueue mission: {exc}") from exc

    def approve_mission(self, mission_id: str, *, yes: bool, note: str = "") -> str:
        new_status = "RUNNING" if yes else "FAILED"
        response_text = note or ("approved" if yes else "rejected")
        self.resume_mission(
            mission_id,
            expected_status="NEED_APPROVAL",
            new_status=new_status,
            response_text=response_text,
        )
        return new_status

    def send_input(self, mission_id: str, message: str) -> None:
        self.resume_mission(
            mission_id,
            expected_status="NEED_INPUT",
            new_status="RUNNING",
            response_text=message,
        )

    # -------------------------------------------------------------- #
    # tasks                                                            #
    # -------------------------------------------------------------- #

    def _get_task_item(self, task_id: str) -> Dict[str, Any]:
        try:
            resp = self.ddb.get_item(
                TableName=self.cfg["task_table"],
                Key={"task_id": {"S": task_id}},
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to get task: {exc}") from exc
        item = resp.get("Item")
        if not item:
            raise TaskNotFound(f"task not found: {task_id}")
        return item

    @staticmethod
    def _iter_stages(task_item: Dict[str, Any]) -> List[tuple[str, Dict[str, Any]]]:
        stages = task_item.get("stages", {}).get("M", {})
        return [(name, val.get("M", {})) for name, val in stages.items()]

    @staticmethod
    def resolve_ca_mission_id(task_item: Dict[str, Any]) -> Optional[str]:
        for stage_name, stage_m in FleetService._iter_stages(task_item):
            base = stage_name.split("_", 1)[0] if "_" in stage_name else stage_name
            if base != "CA":
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if mid:
                return mid
        return None

    def get_task(self, task_id: str) -> TaskDetail:
        item = self._get_task_item(task_id)
        detail = TaskDetail(
            task_id=task_id,
            status=_attr(item, "status"),
            current_stage=_attr(item, "current_stage"),
            budget_month=_attr(item, "budget_month"),
            updated_at=_attr_int(item, "updated_at"),
            abort_reason=_attr(item, "abort_reason"),
        )
        for stage_name, stage_m in self._iter_stages(item):
            mid = stage_m.get("mission_id", {}).get("S", "")
            ship = stage_m.get("ship_class", {}).get("S", "")
            confidence: Optional[int] = None
            m_updated = 0
            if mid:
                try:
                    m_item = self._get_mission_item(mid)
                    ship = ship or _attr(m_item, "ship_class")
                    confidence = _attr_opt_int(m_item, "confidence_level")
                    m_updated = _attr_int(m_item, "updated_at")
                except FleetServiceError:
                    pass
            detail.stages.append(
                StageStatus(
                    stage=stage_name,
                    status=stage_m.get("status", {}).get("S", ""),
                    mission_id=mid,
                    ship_class=ship,
                    confidence=confidence,
                    updated_at=m_updated,
                )
            )
        stage_rank = {
            "NEED_APPROVAL": 0, "NEED_INPUT": 1, "RUNNING": 2,
            "ENQUEUED": 3, "FAILED": 4, "DONE": 5, "CANCELLED": 6,
        }
        detail.stages.sort(key=lambda s: stage_rank.get(s.status, 99))
        return detail

    def get_task_status(self, task_id: str) -> tuple[str, str]:
        """Return (status, current_stage); ("UNKNOWN", "") when missing."""
        try:
            item = self._get_task_item(task_id)
        except TaskNotFound:
            return ("UNKNOWN", "")
        return (_attr(item, "status") or "UNKNOWN", _attr(item, "current_stage"))

    def get_ca_confidence(self, task_id: str) -> int:
        try:
            item = self._get_task_item(task_id)
        except FleetServiceError:
            return 0
        mission_id = self.resolve_ca_mission_id(item)
        if not mission_id:
            return 0
        try:
            m_item = self._get_mission_item(mission_id)
        except FleetServiceError:
            return 0
        return _attr_opt_int(m_item, "confidence_level") or 0

    def _signal_task_missions(
        self, task_id: str, *, expected_status: str, response_text: str
    ) -> int:
        """expected_status のミッション全てに resume を送る。成功数を返す。"""
        try:
            item = self._get_task_item(task_id)
        except FleetServiceError:
            return 0
        count = 0
        for _name, stage_m in self._iter_stages(item):
            if stage_m.get("status", {}).get("S", "") != expected_status:
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if not mid:
                continue
            try:
                self.resume_mission(
                    mid,
                    expected_status=expected_status,
                    new_status="RUNNING",
                    response_text=response_text,
                )
                count += 1
            except FleetServiceError:
                pass
        return count

    def approve_task_missions(self, task_id: str, note: str) -> int:
        return self._signal_task_missions(
            task_id, expected_status="NEED_APPROVAL", response_text=note
        )

    def input_task_missions(self, task_id: str, message: str) -> int:
        return self._signal_task_missions(
            task_id, expected_status="NEED_INPUT", response_text=message
        )

    def _cancel_mission_force(self, mission_id: str) -> bool:
        now = int(time.time())
        try:
            self.ddb.update_item(
                TableName=self.cfg["state_table"],
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

    def abort_task(self, task_id: str, note: str = "") -> int:
        """タスクと配下のアクティブミッションを CANCELLED にする。

        Returns the number of missions cancelled.  Raises FleetServiceError
        when the task is already終了状態 (message starts with "task is already").
        """
        item = self._get_task_item(task_id)
        current_status = _attr(item, "status")
        if current_status in ("DONE", "CANCELLED"):
            raise TaskAlreadyFinished(f"task is already {current_status}, nothing to abort.")

        active = {"RUNNING", "ENQUEUED", "NEED_APPROVAL", "NEED_INPUT"}
        cancelled = 0
        for _name, stage_m in self._iter_stages(item):
            if stage_m.get("status", {}).get("S", "") not in active:
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if mid and self._cancel_mission_force(mid):
                cancelled += 1

        now = int(time.time())
        abort_reason = note or "Aborted by user"
        try:
            self.ddb.update_item(
                TableName=self.cfg["task_table"],
                Key={"task_id": {"S": task_id}},
                UpdateExpression="SET #s=:cancelled, updated_at=:now, abort_reason=:reason",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":cancelled": {"S": "CANCELLED"},
                    ":now": {"N": str(now)},
                    ":reason": {"S": abort_reason},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to cancel task: {exc}") from exc
        return cancelled

    def retry_task(self, task_id: str, note: str = "") -> tuple[int, int]:
        """FAILED ミッションを再キューする。(retried, errors) を返す。"""
        item = self._get_task_item(task_id)
        response_text = note or "Retried by user"
        retried = 0
        errors = 0
        for _name, stage_m in self._iter_stages(item):
            if stage_m.get("status", {}).get("S", "") != "FAILED":
                continue
            mid = stage_m.get("mission_id", {}).get("S", "")
            if not mid:
                continue
            try:
                self.resume_mission(
                    mid,
                    expected_status="FAILED",
                    new_status="RUNNING",
                    response_text=response_text,
                )
                retried += 1
            except FleetServiceError:
                errors += 1

        if retried:
            now = int(time.time())
            try:
                self.ddb.update_item(
                    TableName=self.cfg["task_table"],
                    Key={"task_id": {"S": task_id}},
                    UpdateExpression="SET #s=:running, updated_at=:now",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":running": {"S": "RUNNING"},
                        ":now": {"N": str(now)},
                    },
                )
            except (ClientError, BotoCoreError):
                pass
        return retried, errors

    def list_tasks(self) -> List[TaskDetail]:
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {"TableName": self.cfg["task_table"]}
        try:
            while True:
                resp = self.ddb.scan(**kwargs)
                items.extend(resp.get("Items", []))
                if "LastEvaluatedKey" not in resp:
                    break
                kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to scan tasks: {exc}") from exc
        tasks = [
            TaskDetail(
                task_id=_attr(i, "task_id"),
                status=_attr(i, "status"),
                current_stage=_attr(i, "current_stage"),
                budget_month=_attr(i, "budget_month"),
                updated_at=_attr_int(i, "updated_at"),
                abort_reason=_attr(i, "abort_reason"),
            )
            for i in items
        ]
        tasks.sort(key=lambda t: (state_rank(t.status), -t.updated_at))
        return tasks

    # -------------------------------------------------------------- #
    # CA directive                                                     #
    # -------------------------------------------------------------- #

    def save_ca_directive(self, task_id: str, directive: str) -> str:
        """CA 指令書を S3 に保存し、対象 mission_id を返す。"""
        bucket = self.cfg["bucket"]
        if not bucket:
            raise FleetServiceError("FLEET_S3_BUCKET is not set")

        task_item = self._get_task_item(task_id)
        mission_id = self.resolve_ca_mission_id(task_item)
        if not mission_id:
            raise FleetServiceError(f"no CA mission found for task: {task_id}")

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
            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown; charset=utf-8",
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to put CA directive: {exc}") from exc

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
            self.s3.put_object(
                Bucket=bucket,
                Key=comm_key,
                Body=json.dumps(comm, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception:
            pass  # comm log is best-effort
        return mission_id

    # -------------------------------------------------------------- #
    # artifacts                                                        #
    # -------------------------------------------------------------- #

    def list_artifacts(self, mission_id: str) -> List[ArtifactFile]:
        bucket = self.cfg["bucket"]
        if not bucket:
            raise FleetServiceError("FLEET_S3_BUCKET is not set")
        prefix = f"missions/{mission_id}/"
        try:
            files: List[ArtifactFile] = []
            token: Optional[str] = None
            while True:
                kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self.s3.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []):
                    key = str(obj.get("Key", ""))
                    if not key or key.endswith("/"):
                        continue
                    files.append(
                        ArtifactFile(
                            key=key,
                            path=key[len(prefix):],
                            size=int(obj.get("Size", 0)),
                        )
                    )
                if not resp.get("IsTruncated"):
                    break
                token = str(resp.get("NextContinuationToken", ""))
                if not token:
                    break
            return files
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to list mission objects: {exc}") from exc

    def get_artifact_bytes(self, mission_id: str, rel_path: str) -> bytes:
        bucket = self.cfg["bucket"]
        if not bucket:
            raise FleetServiceError("FLEET_S3_BUCKET is not set")
        if ".." in rel_path or rel_path.startswith("/"):
            raise FleetServiceError(f"invalid artifact path: {rel_path}")
        key = f"missions/{mission_id}/{rel_path}"
        try:
            return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to get artifact: {exc}") from exc

    # -------------------------------------------------------------- #
    # budget                                                           #
    # -------------------------------------------------------------- #

    def get_budget(self, month: str) -> BudgetStatus:
        try:
            resp = self.ddb.get_item(
                TableName=self.cfg["budget_table"],
                Key={"month": {"S": month}},
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to get budget: {exc}") from exc
        item = resp.get("Item")
        if not item:
            raise FleetServiceError(f"budget not found for month: {month}")

        def _n(field_name: str) -> float:
            try:
                return float(item.get(field_name, {}).get("N", 0))
            except (TypeError, ValueError):
                return 0.0

        def _ammo(group: str, key: str) -> int:
            try:
                return int(item.get(group, {}).get("M", {}).get(key, {}).get("N", 0))
            except (TypeError, ValueError):
                return 0

        return BudgetStatus(
            month=month,
            fuel_cap=_n("fuel_cap_usd"),
            fuel_remaining=_n("fuel_remaining_usd"),
            fuel_burned=_n("fuel_burned_usd"),
            sorties_total=int(_n("sorties_total")),
            ammo_used={
                "BB": _ammo("ammo_used", "BB_main_gun"),
                "CA": _ammo("ammo_used", "CA_salvo"),
                "CVB": _ammo("ammo_used", "CVB_air_wing"),
            },
            ammo_caps={
                "BB": _ammo("ammo_caps", "BB_main_gun"),
                "CA": _ammo("ammo_caps", "CA_salvo"),
                "CVB": _ammo("ammo_caps", "CVB_air_wing"),
            },
        )

    # -------------------------------------------------------------- #
    # task start (enqueue)                                             #
    # -------------------------------------------------------------- #

    def put_ticket(self, task_id: str, *, objective: str = "", ticket_file: str = "") -> str:
        bucket = self.cfg["bucket"]
        if not bucket:
            raise FleetServiceError("FLEET_S3_BUCKET is not set")
        if ticket_file:
            content = Path(ticket_file).expanduser().read_text(encoding="utf-8")
        else:
            content = f"# Mission Ticket\n\ntask_id: {task_id}\n\n{objective.strip()}\n"
        key = f"tickets/{task_id}.md"
        try:
            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown; charset=utf-8",
            )
        except (ClientError, BotoCoreError) as exc:
            raise FleetServiceError(f"failed to upload ticket: {exc}") from exc
        return f"s3://{bucket}/{key}"

    def _run_orchestrator(self, args: List[str]) -> None:
        script = self.repo_root / "task_orchestrator.py"
        if not script.exists():
            raise FleetServiceError("script not found: task_orchestrator.py")
        env = dict(os.environ)
        if self._env:
            env.update(self._env)
        proc = subprocess.run(
            [sys.executable, str(script), *args],
            cwd=str(self.repo_root),
            env=env,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise FleetServiceError(
                f"task_orchestrator {args[0]} failed (exit={proc.returncode}): {detail[-500:]}"
            )

    def start_task(
        self,
        *,
        objective: str = "",
        ticket_file: str = "",
        task_id: str = "",
        repo_url: str = "",
        doctrine: str = "standard_full",
        budget_month: str = "",
    ) -> str:
        """チケットをS3へ置き、task_orchestrator init/start を実行して task_id を返す。"""
        if not objective.strip() and not ticket_file:
            raise FleetServiceError("objective or ticket_file is required")
        if not task_id:
            task_id = f"T-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        if not budget_month:
            budget_month = datetime.now(timezone.utc).strftime("%Y-%m")
        ticket_s3 = self.put_ticket(task_id, objective=objective, ticket_file=ticket_file)
        self._run_orchestrator(
            ["init", task_id, ticket_s3, budget_month, "--formation", doctrine]
        )
        start_args = ["start", task_id]
        if repo_url:
            start_args.append(repo_url)
        self._run_orchestrator(start_args)
        return task_id

    def advance_task(self, task_id: str, repo_url: str = "") -> None:
        args = ["advance", task_id]
        if repo_url:
            args.append(repo_url)
        self._run_orchestrator(args)

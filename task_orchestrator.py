#!/usr/bin/env python3
"""
Task Orchestrator - Manages task-level workflow (CVL→DD→CL→CA→BB)
Minimal implementation for Phase 3
"""
import os
import sys
import json
import time
import uuid
import re
import boto3
from typing import Dict, Any, List, Optional

try:
    from fleet_local import load_local_config
except Exception:
    load_local_config = None

REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
TASK_STATE_TABLE = os.getenv("FLEET_TASK_STATE_TABLE", "fleet-task-state")
MISSION_STATE_TABLE = os.getenv("FLEET_STATE_TABLE", "fleet-mission-state")
BUCKET = os.getenv("FLEET_S3_BUCKET", "")
SQS_NAME = os.getenv("FLEET_SQS_NAME", "fleet-missions")

dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)

# Default stage sequence
DEFAULT_STAGES = ["CVL", "DD", "CL", "CA"]

# Task status
TASK_STATUS_ENQUEUED = "ENQUEUED"
TASK_STATUS_RUNNING = "RUNNING"
TASK_STATUS_NEED_INPUT = "NEED_INPUT"
TASK_STATUS_NEED_APPROVAL = "NEED_APPROVAL"
TASK_STATUS_DONE = "DONE"
TASK_STATUS_FAILED = "FAILED"

# Stage status
STAGE_STATUS_PENDING = "PENDING"
STAGE_STATUS_RUNNING = "RUNNING"
STAGE_STATUS_DONE = "DONE"
STAGE_STATUS_FAILED = "FAILED"
STAGE_STATUS_NEED_INPUT = "NEED_INPUT"
STAGE_STATUS_NEED_APPROVAL = "NEED_APPROVAL"


def now() -> int:
    return int(time.time())


def _load_formations() -> Dict[str, Any]:
    if not load_local_config:
        return {}
    try:
        cfg = load_local_config()
    except Exception:
        return {}
    formations = cfg.get("formations", {})
    return formations if isinstance(formations, dict) else {}


def _default_formation_name() -> Optional[str]:
    if not load_local_config:
        return None
    try:
        cfg = load_local_config()
    except Exception:
        return None
    name = cfg.get("formation_default")
    return name if isinstance(name, str) and name else None


def _base_stage(stage: str) -> str:
    if not stage:
        return stage
    if "_" in stage:
        base = stage.split("_", 1)[0]
        if base in {"CVL", "DD", "CL", "CVB", "CA", "BB"}:
            return base
    return stage


def _get_ship_profile(stage: str) -> Dict[str, Any]:
    if not load_local_config:
        return {}
    try:
        cfg = load_local_config()
    except Exception:
        return {}
    ships = cfg.get("ships", {})
    if not isinstance(ships, dict):
        return {}
    profile = ships.get(stage)
    if profile is None:
        profile = ships.get(_base_stage(stage), {})
    return profile or {}

def _normalize_stage_name(stage: str) -> Optional[str]:
    if not stage:
        return None
    s = stage.strip().replace('-', '_').upper()
    return s if s else None


def _extract_extra_stages(text: str) -> List[str]:
    if not text:
        return []
    patterns = [
        r"(?im)^\s*(?:extra|add|additional)\s*stages?\s*[:=]\s*(.+)$",
        r"(?im)^\s*(?:extra|add|additional)\s*[:=]\s*(.+)$",
        r"(?im)^\s*追加(?:実施)?\s*[:=]\s*(.+)$",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            raw = m.group(1)
            tokens = re.split(r"[\s,]+", raw)
            stages = []
            for t in tokens:
                s = _normalize_stage_name(t)
                if s:
                    stages.append(s)
            return stages
    return []
def _apply_extra_stages(
    task_id: str,
    current_stage: str,
    stage_list: List[str],
    stages_map: Dict[str, Any],
    extra_stages: List[str],
) -> Optional[tuple[list[str], Dict[str, Any], str]]:
    if not extra_stages:
        return None
    valid: List[str] = []
    seen = set()
    for s in extra_stages:
        if s in seen:
            continue
        seen.add(s)
        if s == current_stage:
            continue
        s = _normalize_stage_name(s) or s
        profile = _get_ship_profile(s)
        if not profile:
            print(f"Warning: unknown extra stage '{s}', skipped")
            continue
        valid.append(s)
    if not valid:
        return None

    extras_set = set(valid)
    prefix: List[str] = []
    suffix: List[str] = []
    seen_current = False
    for s in stage_list:
        if s in extras_set:
            continue
        if s == current_stage:
            seen_current = True
            continue
        if not seen_current:
            prefix.append(s)
        else:
            suffix.append(s)

    new_order = prefix + valid + [current_stage] + suffix

    new_stages = dict(stages_map)
    def _reset_stage(stage_name: str) -> None:
        entry = new_stages.get(stage_name, {}).get('M', {})
        entry['status'] = {'S': STAGE_STATUS_PENDING}
        entry['mission_id'] = {'NULL': True}
        new_stages[stage_name] = {'M': entry}

    for s in valid:
        _reset_stage(s)
    _reset_stage(current_stage)

    dynamodb.update_item(
        TableName=TASK_STATE_TABLE,
        Key={'task_id': {'S': task_id}},
        UpdateExpression="SET stage_order=:order, stages=:stages, updated_at=:u",
        ExpressionAttributeValues={
            ':order': {'L': [{'S': s} for s in new_order]},
            ':stages': {'M': new_stages},
            ':u': {'N': str(now())},
        },
    )
    print(f"Applied extra stages: {', '.join(valid)} (CA will re-run)")
    return new_order, new_stages, valid[0]


def _parse_stage_list(raw: str) -> List[str]:
    tokens = re.split(r"[\s,]+", raw.strip())
    stages: List[str] = []
    for t in tokens:
        if not t:
            continue
        s = _normalize_stage_name(t)
        if s:
            stages.append(s)
    return stages


def _parse_bool(value: str) -> Optional[bool]:
    v = value.strip().lower()
    if v in {"yes", "true", "1", "on", "y"}:
        return True
    if v in {"no", "false", "0", "off", "n"}:
        return False
    return None


def _parse_patch_map(raw: str) -> Dict[str, str]:
    patch_map: Dict[str, str] = {}
    tokens = re.split(r"[\s,]+", raw.strip())
    for token in tokens:
        if not token:
            continue
        if "=" in token:
            key, val = token.split("=", 1)
        elif ":" in token:
            key, val = token.split(":", 1)
        else:
            continue
        stage = _normalize_stage_name(key)
        val = val.strip()
        if stage and val.startswith("s3://"):
            patch_map[stage] = val
    return patch_map


def _parse_ca_directives(text: str) -> Dict[str, Any]:
    result = {"execute": [], "skip": [], "patch_map": {}, "done": False}
    if not text:
        return result
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(("-", "*")):
            line = line[1:].strip()
        lower = line.lower()
        if lower.startswith(("formation:", "陣形:")):
            continue
        if lower.startswith(("execute:", "run:", "実行:")):
            raw = line.split(":", 1)[1].strip()
            result["execute"] = _parse_stage_list(raw)
            continue
        if lower.startswith(("skip:", "omit:", "除外:")):
            raw = line.split(":", 1)[1].strip()
            result["skip"] = _parse_stage_list(raw)
            continue
        if lower.startswith(("patch:", "patches:", "パッチ:")):
            raw = line.split(":", 1)[1].strip()
            result["patch_map"] = _parse_patch_map(raw)
            continue
        if lower.startswith(("done:", "complete:", "終了:")):
            raw = line.split(":", 1)[1].strip()
            parsed = _parse_bool(raw)
            if parsed is not None:
                result["done"] = parsed
            continue
    if result["skip"] and result["execute"]:
        skip_set = set(result["skip"])
        result["execute"] = [s for s in result["execute"] if s not in skip_set]
    return result
def _apply_skip_stages(
    task_id: str,
    current_stage: str,
    stage_list: List[str],
    stages_map: Dict[str, Any],
    skip_stages: List[str],
) -> Optional[tuple[list[str], Dict[str, Any]]]:
    if not skip_stages:
        return None
    valid = []
    for s in skip_stages:
        s = _normalize_stage_name(s) or s
        if s == current_stage:
            continue
        if s in stage_list:
            valid.append(s)
    if not valid:
        return None
    new_order = [s for s in stage_list if s not in valid]
    new_stages = dict(stages_map)
    for s in valid:
        entry = new_stages.get(s, {}).get("M", {})
        entry["status"] = {"S": STAGE_STATUS_DONE}
        entry["mission_id"] = {"NULL": True}
        new_stages[s] = {"M": entry}
    dynamodb.update_item(
        TableName=TASK_STATE_TABLE,
        Key={"task_id": {"S": task_id}},
        UpdateExpression="SET stage_order=:order, stages=:stages, updated_at=:u",
        ExpressionAttributeValues={
            ":order": {"L": [{"S": s} for s in new_order]},
            ":stages": {"M": new_stages},
            ":u": {"N": str(now())},
        },
    )
    print(f"Applied skip stages: {', '.join(valid)}")
    return new_order, new_stages


def resolve_stages(stages: Optional[List[str]], formation_name: Optional[str]) -> List[str]:
    if stages:
        return stages
    formations = _load_formations()
    chosen = formation_name or os.getenv("FLEET_FORMATION") or _default_formation_name()
    if chosen and chosen in formations:
        entry = formations[chosen]
        if isinstance(entry, dict):
            entry = entry.get("stages", [])
        if isinstance(entry, list) and entry:
            return [s for s in entry if isinstance(s, str)]
    return DEFAULT_STAGES


def _get_formation_stages(formation_name: str) -> List[str]:
    formations = _load_formations()
    if formation_name not in formations:
        return []
    entry = formations[formation_name]
    if isinstance(entry, dict):
        entry = entry.get("stages", [])
    if not isinstance(entry, list):
        return []
    return [s for s in entry if isinstance(s, str) and s]


def _read_ca_report(mission_id: str) -> str:
    if not mission_id:
        return ""
    try:
        key = f"missions/{mission_id}/artifacts/report.CA-01.md"
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _read_ca_human_directive(mission_id: str) -> str:
    if not mission_id:
        return ""
    try:
        key = f"missions/{mission_id}/artifacts/ca_human_directive.md"
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        return ""
def _extract_formation_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r"(?im)^\s*formation\s*[:=]\s*([A-Za-z0-9_-]+)\s*$",
        r"(?im)^\s*陣形\s*[:=]\s*([A-Za-z0-9_-]+)\s*$",
        r"(?i)formation\s*[:=]\s*([A-Za-z0-9_-]+)",
        r"陣形\s*[:=]\s*([A-Za-z0-9_-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None
def _apply_formation_override(
    task_id: str,
    formation_name: str,
    current_stage: str,
    stages_map: Dict[str, Any],
) -> Optional[tuple[list[str], Dict[str, Any]]]:
    stage_list = _get_formation_stages(formation_name)
    if not stage_list:
        print(f"Warning: unknown formation '{formation_name}'")
        return None
    if current_stage not in stage_list:
        print(f"Warning: formation '{formation_name}' does not include current stage {current_stage}")
        return None

    new_stages_map = dict(stages_map)
    for stage in stage_list:
        if stage not in new_stages_map:
            new_stages_map[stage] = {
                "M": {
                    "status": {"S": STAGE_STATUS_PENDING},
                    "mission_id": {"NULL": True},
                }
            }

    dynamodb.update_item(
        TableName=TASK_STATE_TABLE,
        Key={"task_id": {"S": task_id}},
        UpdateExpression="SET stage_order=:order, stages=:stages, formation=:f, updated_at=:u",
        ExpressionAttributeValues={
            ":order": {"L": [{"S": s} for s in stage_list]},
            ":stages": {"M": new_stages_map},
            ":f": {"S": formation_name},
            ":u": {"N": str(now())},
        },
    )
    print(f"Applied formation override: {formation_name}")
    return stage_list, new_stages_map


def get_queue_url() -> str:
    resp = sqs.get_queue_url(QueueName=SQS_NAME)
    return resp["QueueUrl"]


def init_task(
    task_id: str,
    ticket_s3: str,
    budget_month: str,
    stages: Optional[List[str]] = None,
    formation_name: Optional[str] = None,
) -> None:
    """Initialize task state in DynamoDB"""
    stages = resolve_stages(stages, formation_name)
    
    n = now()
    stages_map = {}
    for stage in stages:
        stages_map[stage] = {
            "M": {
                "status": {"S": STAGE_STATUS_PENDING},
                "mission_id": {"NULL": True}
            }
        }
    
    try:
        dynamodb.put_item(
            TableName=TASK_STATE_TABLE,
            Item={
                "task_id": {"S": task_id},
                "status": {"S": TASK_STATUS_ENQUEUED},
                "current_stage": {"S": stages[0]},
                "stage_order": {"L": [{"S": s} for s in stages]},
                "stages": {"M": stages_map},
                **({"formation": {"S": formation_name}} if formation_name else {}),
                "ticket_s3": {"S": ticket_s3},
                "budget_month": {"S": budget_month},
                "created_at": {"N": str(n)},
                "updated_at": {"N": str(n)}
            },
            ConditionExpression="attribute_not_exists(task_id)"
        )
        msg = f"Task {task_id} initialized"
        if formation_name:
            msg += f" (formation={formation_name})"
        print(msg)
    except Exception as e:
        if "ConditionalCheckFailedException" in str(e):
            print(f"Task {task_id} already exists")
        else:
            raise


def get_task_state(task_id: str) -> Optional[Dict[str, Any]]:
    """Get task state from DynamoDB"""
    try:
        resp = dynamodb.get_item(
            TableName=TASK_STATE_TABLE,
            Key={"task_id": {"S": task_id}}
        )
        return resp.get("Item")
    except Exception as e:
        print(f"Error getting task state: {e}")
        return None


def update_task_status(task_id: str, status: str) -> None:
    """Update task status"""
    dynamodb.update_item(
        TableName=TASK_STATE_TABLE,
        Key={"task_id": {"S": task_id}},
        UpdateExpression="SET #s=:status, updated_at=:u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":status": {"S": status},
            ":u": {"N": str(now())}
        }
    )


def update_stage_status(task_id: str, stage: str, status: str, mission_id: Optional[str] = None) -> None:
    """Update stage status"""
    update_expr = "SET stages.#stage.#status=:status, updated_at=:u"
    expr_names = {"#stage": stage, "#status": "status"}
    expr_values = {
        ":status": {"S": status},
        ":u": {"N": str(now())}
    }
    
    if mission_id:
        update_expr += ", stages.#stage.mission_id=:mid"
        expr_values[":mid"] = {"S": mission_id}
    
    dynamodb.update_item(
        TableName=TASK_STATE_TABLE,
        Key={"task_id": {"S": task_id}},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values
    )


def enqueue_mission(
    task_id: str,
    stage: str,
    ticket_s3: str,
    budget_month: str,
    repo_url: Optional[str] = None,
    patch_map: Optional[Dict[str, str]] = None,
) -> str:
    """Enqueue a mission to SQS"""
    mission_id = f"M-{task_id}-{stage}-01-{uuid.uuid4().hex[:8]}"
    ship_id = f"{stage}-01"
    profile = _get_ship_profile(stage)
    
    payload = {
        "mission_id": mission_id,
        "task_id": task_id,
        "ship_id": ship_id,
        "ship_class": stage,
        "inputs": {
            "ticket_s3": ticket_s3
        },
        "budget": {
            "month": budget_month
        }
    }
    if profile.get("role"):
        payload["role"] = profile["role"]
    if profile.get("model_tag"):
        payload["model"] = profile["model_tag"]
    if patch_map and stage in patch_map:
        payload["inputs"]["patch_s3"] = patch_map[stage]
    
    # Add repo for DD/CL/CVB
    if _base_stage(stage) in ["DD", "CL", "CVB"] and repo_url:
        payload["inputs"]["repo"] = {
            "url": repo_url,
            "branch": f"agent/{task_id}".lower()
        }
    
    queue_url = get_queue_url()
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload, ensure_ascii=False)
    )
    
    print(f"✓ Mission {mission_id} enqueued for stage {stage}")
    return mission_id


def start_task(task_id: str, repo_url: Optional[str] = None) -> None:
    """Start task by enqueuing first stage"""
    task = get_task_state(task_id)
    if not task:
        print(f"Task {task_id} not found")
        return
    
    status = task.get("status", {}).get("S", "")
    if status != TASK_STATUS_ENQUEUED:
        print(f"Task {task_id} is not ENQUEUED (current: {status})")
        return
    
    current_stage = task.get("current_stage", {}).get("S", "")
    ticket_s3 = task.get("ticket_s3", {}).get("S", "")
    budget_month = task.get("budget_month", {}).get("S", "")
    
    # Enqueue first mission
    mission_id = enqueue_mission(task_id, current_stage, ticket_s3, budget_month, repo_url)
    
    # Update task and stage status
    update_task_status(task_id, TASK_STATUS_RUNNING)
    update_stage_status(task_id, current_stage, STAGE_STATUS_RUNNING, mission_id)
    
    print(f"✓ Task {task_id} started with stage {current_stage}")


def check_mission_status(mission_id: str) -> Optional[str]:
    """Check mission status from DynamoDB"""
    try:
        resp = dynamodb.get_item(
            TableName=MISSION_STATE_TABLE,
            Key={"mission_id": {"S": mission_id}}
        )
        item = resp.get("Item")
        if item:
            return item.get("status", {}).get("S")
        return None
    except Exception as e:
        print(f"Error checking mission status: {e}")
        return None


def advance_task(task_id: str, repo_url: Optional[str] = None) -> None:
    """Check current stage and advance to next if completed"""
    task = get_task_state(task_id)
    if not task:
        print(f"Task {task_id} not found")
        return
    
    task_status = task.get("status", {}).get("S", "")
    if task_status != TASK_STATUS_RUNNING:
        print(f"Task {task_id} is not RUNNING (current: {task_status})")
        return
    
    current_stage = task.get("current_stage", {}).get("S", "")
    stages_map = task.get("stages", {}).get("M", {})
    
    # Get current stage info
    stage_info = stages_map.get(current_stage, {}).get("M", {})
    stage_status = stage_info.get("status", {}).get("S", "")
    mission_id = stage_info.get("mission_id", {}).get("S")
    
    # Check mission status
    if mission_id:
        mission_status = check_mission_status(mission_id)
        
        # Update stage status based on mission status
        if mission_status == "DONE" and stage_status != STAGE_STATUS_DONE:
            update_stage_status(task_id, current_stage, STAGE_STATUS_DONE)
            stage_status = STAGE_STATUS_DONE
            print(f"✓ Stage {current_stage} completed")
        elif mission_status == "FAILED":
            update_stage_status(task_id, current_stage, STAGE_STATUS_FAILED)
            update_task_status(task_id, TASK_STATUS_FAILED)
            print(f"✗ Stage {current_stage} failed")
            return
        elif mission_status in ["NEED_INPUT", "NEED_APPROVAL"]:
            stage_status_val = STAGE_STATUS_NEED_INPUT if mission_status == "NEED_INPUT" else STAGE_STATUS_NEED_APPROVAL
            update_stage_status(task_id, current_stage, stage_status_val)
            update_task_status(task_id, mission_status)
            print(f"⚠ Stage {current_stage} needs human intervention: {mission_status}")
            return
    
    # If current stage is done, decide next action
    if stage_status == STAGE_STATUS_DONE:
        stage_order_attr = task.get("stage_order", {}).get("L", [])
        stage_list = [s.get("S") for s in stage_order_attr if isinstance(s, dict) and s.get("S")]
        if not stage_list:
            stage_list = [s for s in DEFAULT_STAGES if s in stages_map]
            extras = [s for s in stages_map.keys() if s not in stage_list]
            stage_list.extend(sorted(extras))
        
        if current_stage not in stage_list:
            print(f"Warning: current_stage {current_stage} not in stage list; cannot advance")
            return
        # Special handling for CA stage
        ca_patch_map = {}
        if current_stage == "CA":
            report_text = _read_ca_report(mission_id)
            human_text = _read_ca_human_directive(mission_id)
            parse_text = "\n".join([t for t in [report_text, human_text] if t]).strip()
            formation_name = _extract_formation_from_text(parse_text) if parse_text else None
            directives = _parse_ca_directives(parse_text)
            ca_patch_map = directives.get("patch_map", {})
            if formation_name:
                override = _apply_formation_override(task_id, formation_name, current_stage, stages_map)
                if override:
                    stage_list, stages_map = override
                    if current_stage not in stage_list:
                        print(f"Warning: current_stage {current_stage} not in overridden formation")
                        return
                else:
                    formation_name = None

            if not formation_name:
                if directives.get("skip"):
                    skipped = _apply_skip_stages(task_id, current_stage, stage_list, stages_map, directives["skip"])
                    if skipped:
                        stage_list, stages_map = skipped
                        if current_stage not in stage_list:
                            print(f"Warning: current_stage {current_stage} not in stage list after skip")
                            return
                if directives.get("done") is True:
                    update_task_status(task_id, TASK_STATUS_DONE)
                    print(f"Task {task_id} completed (CA done directive)")
                    return

                if directives.get("execute"):
                    applied = _apply_extra_stages(
                        task_id,
                        current_stage,
                        stage_list,
                        stages_map,
                        directives["execute"],
                    )
                    if applied:
                        stage_list, stages_map, next_stage = applied
                        ticket_s3 = task.get("ticket_s3", {}).get("S", "")
                        budget_month = task.get("budget_month", {}).get("S", "")

                        mission_id = enqueue_mission(
                            task_id,
                            next_stage,
                            ticket_s3,
                            budget_month,
                            repo_url,
                            patch_map=ca_patch_map,
                        )
                        dynamodb.update_item(
                            TableName=TASK_STATE_TABLE,
                            Key={"task_id": {"S": task_id}},
                            UpdateExpression="SET current_stage=:stage, updated_at=:u",
                            ExpressionAttributeValues={
                                ":stage": {"S": next_stage},
                                ":u": {"N": str(now())}
                            }
                        )
                        update_stage_status(task_id, next_stage, STAGE_STATUS_RUNNING, mission_id)
                        print(f"Advanced to extra stage {next_stage} (CA requested additional execution)")
                        return

                extra_stages = _extract_extra_stages(parse_text)
                if extra_stages:
                    applied = _apply_extra_stages(task_id, current_stage, stage_list, stages_map, extra_stages)
                    if applied:
                        stage_list, stages_map, next_stage = applied
                        ticket_s3 = task.get("ticket_s3", {}).get("S", "")
                        budget_month = task.get("budget_month", {}).get("S", "")

                        mission_id = enqueue_mission(
                            task_id,
                            next_stage,
                            ticket_s3,
                            budget_month,
                            repo_url,
                            patch_map=ca_patch_map,
                        )
                        dynamodb.update_item(
                            TableName=TASK_STATE_TABLE,
                            Key={"task_id": {"S": task_id}},
                            UpdateExpression="SET current_stage=:stage, updated_at=:u",
                            ExpressionAttributeValues={
                                ":stage": {"S": next_stage},
                                ":u": {"N": str(now())}
                            }
                        )
                        update_stage_status(task_id, next_stage, STAGE_STATUS_RUNNING, mission_id)
                        print(f"Advanced to extra stage {next_stage} (CA requested additional execution)")
                        return

                # Check if BB is needed by reading CA report
                should_escalate_to_bb = check_ca_escalation(mission_id)
                if not should_escalate_to_bb:
                    # CA decided task is complete
                    update_task_status(task_id, TASK_STATUS_DONE)
                    print(f"Task {task_id} completed (CA decided no BB needed)")
                    return
            # If BB needed, continue to next stage

        current_idx = stage_list.index(current_stage)

        if current_idx + 1 < len(stage_list):
            next_stage = stage_list[current_idx + 1]
            ticket_s3 = task.get("ticket_s3", {}).get("S", "")
            budget_month = task.get("budget_month", {}).get("S", "")
            
            # Enqueue next mission
            mission_id = enqueue_mission(
                task_id,
                next_stage,
                ticket_s3,
                budget_month,
                repo_url,
                patch_map=ca_patch_map if current_stage == "CA" else None,
            )
            
            # Update task
            dynamodb.update_item(
                TableName=TASK_STATE_TABLE,
                Key={"task_id": {"S": task_id}},
                UpdateExpression="SET current_stage=:stage, updated_at=:u",
                ExpressionAttributeValues={
                    ":stage": {"S": next_stage},
                    ":u": {"N": str(now())}
                }
            )
            update_stage_status(task_id, next_stage, STAGE_STATUS_RUNNING, mission_id)
            
            print(f"✓ Advanced to stage {next_stage}")
        else:
            # All stages complete
            update_task_status(task_id, TASK_STATUS_DONE)
            print(f"✓ Task {task_id} completed")


def check_ca_escalation(mission_id: str) -> bool:
    """Check if CA report indicates BB escalation is needed"""
    try:
        report_text = _read_ca_report(mission_id)
        if not report_text:
            return False

        report_lower = report_text.lower()

        # Completion signals have priority over escalation signals.
        completion_keywords = [
            "complete",
            "done",
            "no further",
            "no bb needed",
            "bb not needed",
            "bb不要",
            "完了",
        ]
        for keyword in completion_keywords:
            if keyword in report_lower:
                return False

        escalation_keywords = [
            "bb",
            "battleship",
            "escalation",
            "decisive",
            "戦艦",
            "エスカレーション",
        ]
        for keyword in escalation_keywords:
            if keyword in report_lower:
                return True

        return False
    except Exception as e:
        print(f"Warning: Could not read CA report: {e}")
        return False


def watch_tasks(repo_url: Optional[str] = None, interval: int = 60) -> None:
    """Watch all running tasks and advance them automatically"""
    print(f"Watching tasks (interval: {interval}s)...")
    try:
        while True:
            # Scan for running tasks
            resp = dynamodb.scan(
                TableName=TASK_STATE_TABLE,
                FilterExpression="#s = :running",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":running": {"S": TASK_STATUS_RUNNING}}
            )
            
            tasks = resp.get("Items", [])
            if tasks:
                print(f"\nFound {len(tasks)} running task(s)")
                for task in tasks:
                    task_id = task.get("task_id", {}).get("S", "")
                    print(f"\nChecking task {task_id}...")
                    advance_task(task_id, repo_url)
            else:
                print(".", end="", flush=True)
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped watching")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python task_orchestrator.py init <task_id> <ticket_s3> <budget_month> [stages...] [--formation NAME]")
        print("  python task_orchestrator.py start <task_id> [repo_url]")
        print("  python task_orchestrator.py advance <task_id> [repo_url]")
        print("  python task_orchestrator.py watch [repo_url] [interval_sec]")
        print("  python task_orchestrator.py status <task_id>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "init":
        if len(sys.argv) < 5:
            print("Usage: python task_orchestrator.py init <task_id> <ticket_s3> <budget_month> [stages...] [--formation NAME]")
            sys.exit(1)
        task_id = sys.argv[2]
        ticket_s3 = sys.argv[3]
        budget_month = sys.argv[4]
        extra = sys.argv[5:] if len(sys.argv) > 5 else []
        formation_name = None
        if "--formation" in extra:
            idx = extra.index("--formation")
            if idx + 1 < len(extra):
                formation_name = extra[idx + 1]
            del extra[idx:idx + 2]
        else:
            for i, arg in enumerate(list(extra)):
                if arg.startswith("--formation="):
                    formation_name = arg.split("=", 1)[1]
                    extra.pop(i)
                    break
        stages = extra if extra else None
        init_task(task_id, ticket_s3, budget_month, stages, formation_name)
    
    elif command == "start":
        if len(sys.argv) < 3:
            print("Usage: python task_orchestrator.py start <task_id> [repo_url]")
            sys.exit(1)
        task_id = sys.argv[2]
        repo_url = sys.argv[3] if len(sys.argv) > 3 else None
        start_task(task_id, repo_url)
    
    elif command == "advance":
        if len(sys.argv) < 3:
            print("Usage: python task_orchestrator.py advance <task_id> [repo_url]")
            sys.exit(1)
        task_id = sys.argv[2]
        repo_url = sys.argv[3] if len(sys.argv) > 3 else None
        advance_task(task_id, repo_url)
    
    elif command == "watch":
        repo_url = sys.argv[2] if len(sys.argv) > 2 else None
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 60
        watch_tasks(repo_url, interval)
    
    elif command == "status":
        if len(sys.argv) < 3:
            print("Usage: python task_orchestrator.py status <task_id>")
            sys.exit(1)
        task_id = sys.argv[2]
        task = get_task_state(task_id)
        if task:
            print(json.dumps(task, indent=2, ensure_ascii=False))
        else:
            print(f"Task {task_id} not found")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()


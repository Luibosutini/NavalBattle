from __future__ import annotations

import json
import re
import shlex
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import boto3
from botocore.exceptions import ClientError

from fleet_config import resolve_fleet_config
try:
    from fleet_local import (
        ensure_local_workspace,
        get_ship_profile,
        log_comm_local,
        format_mental_model,
        load_local_config,
    )
except Exception:
    ensure_local_workspace = None
    get_ship_profile = None
    log_comm_local = None
    format_mental_model = None
    load_local_config = None


# =========================
# Config
# =========================
_cfg = resolve_fleet_config()
REGION = _cfg["region"]
SQS_NAME = _cfg["sqs_name"]
STATE_TABLE = _cfg["state_table"]
BUDGET_TABLE = _cfg["budget_table"]
BUCKET = _cfg["bucket"]

if not SQS_NAME:
    raise ValueError("FLEET_SQS_NAME environment variable is required (or set in FleetNodePolicy.json)")
if not STATE_TABLE:
    raise ValueError("FLEET_STATE_TABLE environment variable is required (or set in FleetNodePolicy.json)")
if not BUDGET_TABLE:
    raise ValueError("FLEET_BUDGET_TABLE environment variable is required (or set in FleetNodePolicy.json)")
if not BUCKET:
    raise ValueError("FLEET_S3_BUCKET environment variable is required (or set in FleetNodePolicy.json)")

NODE_ID = os.getenv("FLEET_NODE_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

try:
    SQS_VISIBILITY_SEC = int(os.getenv("FLEET_SQS_VISIBILITY_SEC", "60"))
    LOCK_SEC = int(os.getenv("FLEET_LOCK_SEC", "900"))
except ValueError:
    raise ValueError("FLEET_SQS_VISIBILITY_SEC and FLEET_LOCK_SEC must be integers")

WORK_ROOT = Path(os.getenv("FLEET_WORK_ROOT", str(Path.home() / "fleet_work"))).resolve()
WORK_ROOT.mkdir(parents=True, exist_ok=True)

DEFAULT_TEST_CMD = os.getenv("FLEET_DEFAULT_TEST_CMD", "")  # e.g. "cargo test" or "pytest"
WRITE_LEGACY_RESULTS = os.getenv("FLEET_WRITE_LEGACY_RESULTS", "1").lower() not in ("0", "false", "no")

#==========================
# model ID
#=========================
BEDROCK_SONNET_MODEL_ID = os.getenv("BEDROCK_SONNET_MODEL_ID", "")
BEDROCK_MICRO_MODEL_ID  = os.getenv("BEDROCK_MICRO_MODEL_ID", "")
BEDROCK_LITE_MODEL_ID   = os.getenv("BEDROCK_LITE_MODEL_ID", "")
BEDROCK_OPUS_MODEL_ID   = os.getenv("BEDROCK_OPUS_MODEL_ID", "")

def log_bedrock_error(service: str, operation: str, model_id: str, exc: Exception) -> None:
    print(
        f"[{NODE_ID}] BedrockError region={REGION} service={service} "
        f"operation={operation} modelId={model_id} error={exc}"
    )


def is_inference_profile_arn(model_id: str) -> bool:
    return bool(model_id) and model_id.startswith("arn:aws:bedrock:") and ":inference-profile/" in model_id


def canonical_ship_class(ship_class: str) -> str:
    if not ship_class:
        return ship_class
    if "_" in ship_class:
        base = ship_class.split("_", 1)[0]
        if base in {"CVL", "DD", "CL", "CVB", "CA", "BB"}:
            return base
    return ship_class


def clamp_confidence(value: Any, default: int = 50) -> int:
    try:
        v = int(round(float(value)))
    except Exception:
        return default
    return max(0, min(100, v))


def extract_confidence(text: str) -> Optional[int]:
    if not text:
        return None
    patterns = [
        r"(?im)^\s*confidence(?:_level)?\s*[:=]\s*(\d{1,3})\s*$",
        r"(?im)^\s*confidence(?:_level)?\s*[:=]\s*(\d{1,3})",
        r"(?im)^\s*確信度\s*[:=]\s*(\d{1,3})\s*$",
        r"(?im)^\s*確信度\s*[:=]\s*(\d{1,3})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return clamp_confidence(m.group(1))
    return None


def derive_confidence(ship_base: str, result: Dict[str, Any]) -> int:
    if ship_base == "CA":
        parsed = extract_confidence(result.get("ca_report", ""))
        return parsed if parsed is not None else 70
    if ship_base == "BB":
        parsed = extract_confidence(result.get("bb_strike", ""))
        return parsed if parsed is not None else 65
    if ship_base == "CVL":
        return 60
    if ship_base == "DD":
        exit_code = result.get("test_exit_code")
        diff = (result.get("diff") or "").strip()
        log = (result.get("test_log") or "").lower()
        c = 80 if exit_code == 0 else 35
        if not diff:
            c -= 10
        if "error" in log or "failed" in log:
            c -= 10
        return clamp_confidence(c, 60)
    if ship_base == "CL":
        exit_code = result.get("test_exit_code")
        log = (result.get("test_log") or "").lower()
        c = 75 if exit_code == 0 else 35
        if "error" in log or "failed" in log:
            c -= 10
        return clamp_confidence(c, 55)
    if ship_base == "CVB":
        results = result.get("campaign_results") or []
        if not results:
            return 30
        total = len(results)
        passed = sum(1 for r in results if r.get("exit_code") == 0)
        ratio = passed / total if total else 0.0
        return clamp_confidence(40 + int(60 * ratio), 50)
    return 50


def get_armament(ship_class: str, default_max_tokens: int, default_temperature: float) -> tuple[int, float]:
    if not get_ship_profile:
        return default_max_tokens, default_temperature
    try:
        profile = get_ship_profile(ship_class) or {}
    except Exception:
        return default_max_tokens, default_temperature
    arm = profile.get("armament") or {}
    max_tokens = arm.get("max_tokens", default_max_tokens)
    temperature = arm.get("temperature", default_temperature)
    try:
        max_tokens = int(max_tokens)
    except Exception:
        max_tokens = default_max_tokens
    try:
        temperature = float(temperature)
    except Exception:
        temperature = default_temperature
    return max_tokens, temperature



def now() -> int:
    return int(time.time())


def sh(cmd: str, cwd: Optional[Path] = None, timeout: Optional[int] = None) -> Tuple[int, str]:
    p = subprocess.run(
        shlex.split(cmd),
        cwd=str(cwd) if cwd else None,
        shell=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    return p.returncode, p.stdout


# =========================
# AWS clients
# =========================
sqs = boto3.client("sqs", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
bedrock_control = boto3.client("bedrock", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)



def get_queue_url() -> str:
    try:
        return sqs.get_queue_url(QueueName=SQS_NAME)["QueueUrl"]
    except (ClientError, KeyError) as e:
        raise RuntimeError(f"Failed to get SQS queue URL: {e}")


QUEUE_URL = get_queue_url()


# =========================
# DynamoDB helpers
# =========================

# Mission status constants
STATUS_READY = "READY"  # legacy compatibility
STATUS_ENQUEUED = "ENQUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_NEED_INPUT = "NEED_INPUT"
STATUS_NEED_APPROVAL = "NEED_APPROVAL"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_BUDGET_DENIED = "BUDGET_DENIED"

# Valid state transitions
VALID_TRANSITIONS = {
    STATUS_READY: [STATUS_RUNNING, STATUS_BUDGET_DENIED],
    STATUS_ENQUEUED: [STATUS_RUNNING, STATUS_BUDGET_DENIED],
    STATUS_RUNNING: [STATUS_NEED_INPUT, STATUS_NEED_APPROVAL, STATUS_DONE, STATUS_FAILED, STATUS_BUDGET_DENIED],
    STATUS_NEED_INPUT: [STATUS_RUNNING, STATUS_FAILED],
    STATUS_NEED_APPROVAL: [STATUS_RUNNING, STATUS_FAILED],
}

def ddb_get_item(table: str, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = ddb.get_item(TableName=table, Key=key)
    return r.get("Item")

def init_mission_state(mission_id: str, task_id: str, ship_id: str, ship_class: str) -> None:
    """Initialize mission state in DynamoDB"""
    n = now()
    try:
        ddb.put_item(
            TableName=STATE_TABLE,
            Item={
                "mission_id": {"S": mission_id},
                "task_id": {"S": task_id},
                "ship_id": {"S": ship_id},
                "ship_class": {"S": ship_class},
                "status": {"S": STATUS_ENQUEUED},
                "s3_prefix": {"S": mission_s3_prefix(mission_id)},
                "created_at": {"N": str(n)},
                "updated_at": {"N": str(n)},
                "attempt": {"N": "0"},
            },
            ConditionExpression="attribute_not_exists(mission_id)",
        )
        # Best-effort mission.json + init event
        try:
            put_mission_json(mission_id, {
                "mission_id": mission_id,
                "task_id": task_id,
                "ship_id": ship_id,
                "ship_class": ship_class,
                "status": STATUS_ENQUEUED,
                "s3_prefix": mission_s3_prefix(mission_id),
                "created_at": n,
            })
            record_event(mission_id, "MISSION_INIT", {"status": STATUS_ENQUEUED})
        except Exception as e:
            print(f"[{NODE_ID}] WARN: failed to init mission artifacts for {mission_id}: {e}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

def transition_status(mission_id: str, from_status: str, to_status: str, **kwargs) -> bool:
    """Transition mission status with validation"""
    # Validate transition
    if from_status not in VALID_TRANSITIONS:
        raise ValueError(f"Invalid from_status: {from_status}")
    if to_status not in VALID_TRANSITIONS.get(from_status, []):
        raise ValueError(f"Invalid transition: {from_status} -> {to_status}")
    
    n = now()
    set_exprs = ["#s=:to", "updated_at=:u"]
    remove_exprs: List[str] = []
    add_exprs: List[str] = []
    expr_names = {"#s": "status"}
    expr_values = {
        ":to": {"S": to_status},
        ":from": {"S": from_status},
        ":u": {"N": str(n)},
    }
    
    # Add optional fields
    if "owner" in kwargs:
        set_exprs.append("#owner=:owner")
        expr_names["#owner"] = "owner"
        expr_values[":owner"] = {"S": kwargs["owner"]}
    
    if "lock_until" in kwargs:
        set_exprs.append("lock_until=:lock")
        expr_values[":lock"] = {"N": str(kwargs["lock_until"])}
    
    if "needs_input" in kwargs:
        set_exprs.append("needs_input=:ni")
        expr_values[":ni"] = {"M": {
            "question": {"S": kwargs["needs_input"].get("question", "")},
            "context": {"S": json.dumps(kwargs["needs_input"].get("context", {}))},
        }}
    
    if "needs_approval" in kwargs:
        set_exprs.append("needs_approval=:na")
        expr_values[":na"] = {"M": {
            "reason": {"S": kwargs["needs_approval"].get("reason", "")},
            "cost": {"N": str(kwargs["needs_approval"].get("cost", 0))},
        }}
    
    if "result_s3" in kwargs:
        set_exprs.append("result_s3=:r")
        expr_values[":r"] = {"S": kwargs["result_s3"]}
    
    if "fail_reason" in kwargs:
        safe_reason = kwargs["fail_reason"].encode('utf-8')[:3500].decode('utf-8', errors='ignore')
        set_exprs.append("fail_reason=:fr")
        expr_values[":fr"] = {"S": safe_reason}
    
    if kwargs.get("increment_attempt"):
        add_exprs.append("attempt :one")
        expr_values[":one"] = {"N": "1"}
    
    if kwargs.get("clear_lock"):
        remove_exprs.extend(["#owner", "lock_until"])
        expr_names["#owner"] = "owner"
    
    if kwargs.get("clear_needs") or (from_status in (STATUS_NEED_INPUT, STATUS_NEED_APPROVAL) and to_status == STATUS_RUNNING):
        remove_exprs.extend(["needs_input", "needs_approval"])

    update_expr = ""
    if set_exprs:
        update_expr += "SET " + ", ".join(set_exprs)
    if remove_exprs:
        update_expr += " REMOVE " + ", ".join(remove_exprs)
    if add_exprs:
        update_expr += " ADD " + ", ".join(add_exprs)
    
    try:
        ddb.update_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ConditionExpression="#s=:from",
        )
        # Record event (best-effort)
        try:
            record_event(mission_id, f"STATUS_{to_status}", {"from": from_status, "to": to_status})
        except Exception as e:
            print(f"[{NODE_ID}] WARN: failed to record event for {mission_id}: {e}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def try_lock_mission(mission_id: str, lock_sec: int = LOCK_SEC) -> bool:
    """Try to lock mission for execution (ENQUEUED/READY -> RUNNING)"""
    n = now()
    lock_until = n + lock_sec
    # First, try ENQUEUED -> RUNNING
    if transition_status(
        mission_id,
        STATUS_ENQUEUED,
        STATUS_RUNNING,
        owner=NODE_ID,
        lock_until=lock_until,
        increment_attempt=True
    ):
        return True
    # Legacy READY -> RUNNING
    if transition_status(
        mission_id,
        STATUS_READY,
        STATUS_RUNNING,
        owner=NODE_ID,
        lock_until=lock_until,
        increment_attempt=True
    ):
        return True

    # If already RUNNING but lock expired, take over lock
    try:
        ddb.update_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}},
            UpdateExpression="SET #owner=:o, #lock_until=:lu, updated_at=:u ADD attempt :one",
            ExpressionAttributeNames={"#owner": "owner", "#lock_until": "lock_until", "#s": "status"},
            ExpressionAttributeValues={
                ":o": {"S": NODE_ID},
                ":lu": {"N": str(lock_until)},
                ":u": {"N": str(n)},
                ":one": {"N": "1"},
                ":running": {"S": STATUS_RUNNING},
                ":n": {"N": str(n)},
            },
            ConditionExpression="#s=:running AND (attribute_not_exists(#lock_until) OR #lock_until < :n)",
        )
        try:
            record_event(mission_id, "LOCK_TAKEOVER", {"status": STATUS_RUNNING, "owner": NODE_ID})
        except Exception as e:
            print(f"[{NODE_ID}] WARN: failed to record lock takeover for {mission_id}: {e}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def mark_done(mission_id: str, result_s3: str) -> None:
    """Mark mission as done (RUNNING -> DONE)"""
    transition_status(mission_id, STATUS_RUNNING, STATUS_DONE, result_s3=result_s3, clear_lock=True, clear_needs=True)


def mark_failed(mission_id: str, reason: str) -> None:
    """Mark mission as failed (RUNNING -> FAILED)"""
    transition_status(mission_id, STATUS_RUNNING, STATUS_FAILED, fail_reason=reason, clear_lock=True, clear_needs=True)

def mark_budget_denied(mission_id: str, reason: str) -> None:
    """Mark mission as budget denied (RUNNING/ENQUEUED -> BUDGET_DENIED)"""
    if transition_status(mission_id, STATUS_RUNNING, STATUS_BUDGET_DENIED, fail_reason=reason, clear_lock=True):
        return
    transition_status(mission_id, STATUS_ENQUEUED, STATUS_BUDGET_DENIED, fail_reason=reason, clear_lock=True)

def update_confidence(mission_id: str, confidence_level: int) -> None:
    try:
        dynamodb.update_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}},
            UpdateExpression="SET confidence_level=:c, updated_at=:u",
            ExpressionAttributeValues={
                ":c": {"N": str(clamp_confidence(confidence_level))},
                ":u": {"N": str(now())},
            },
        )
    except Exception as e:
        print(f"[{NODE_ID}] WARN: failed to update confidence for {mission_id}: {e}")

def mark_need_input(mission_id: str, question: str, context: Dict[str, Any]) -> None:
    """Mark mission as needing input (RUNNING -> NEED_INPUT)"""
    transition_status(
        mission_id,
        STATUS_RUNNING,
        STATUS_NEED_INPUT,
        needs_input={"question": question, "context": context},
        clear_lock=True
    )
    try:
        record_comm(mission_id, "system", "user", "question", question)
    except Exception as e:
        print(f"[{NODE_ID}] WARN: failed to record comm for {mission_id}: {e}")

def mark_need_approval(mission_id: str, reason: str, cost: float) -> None:
    """Mark mission as needing approval (RUNNING -> NEED_APPROVAL)"""
    transition_status(
        mission_id,
        STATUS_RUNNING,
        STATUS_NEED_APPROVAL,
        needs_approval={"reason": reason, "cost": cost},
        clear_lock=True
    )
    try:
        record_comm(mission_id, "system", "user", "approval_request", reason)
    except Exception as e:
        print(f"[{NODE_ID}] WARN: failed to record comm for {mission_id}: {e}")

def resume_from_input(mission_id: str, answer: str) -> bool:
    """Resume mission from NEED_INPUT (NEED_INPUT -> RUNNING)"""
    try:
        record_comm(mission_id, "user", "system", "answer", answer)
    except Exception as e:
        print(f"[{NODE_ID}] WARN: failed to record comm for {mission_id}: {e}")
    return transition_status(mission_id, STATUS_NEED_INPUT, STATUS_RUNNING)

def resume_from_approval(mission_id: str, approved: bool, note: str = "") -> bool:
    """Resume mission from NEED_APPROVAL (NEED_APPROVAL -> RUNNING or FAILED)"""
    try:
        record_comm(mission_id, "user", "system", "approval_response", f"Approved: {approved}. {note}")
    except Exception as e:
        print(f"[{NODE_ID}] WARN: failed to record comm for {mission_id}: {e}")
    if approved:
        return transition_status(mission_id, STATUS_NEED_APPROVAL, STATUS_RUNNING)
    else:
        return transition_status(
            mission_id,
            STATUS_NEED_APPROVAL,
            STATUS_FAILED,
            fail_reason=f"Approval denied: {note}",
            clear_lock=True,
            clear_needs=True
        )

# =========================
# S3 helpers
# =========================
def list_s3_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        r = s3.list_objects_v2(**kwargs)
        for obj in r.get("Contents", []):
            keys.append(obj["Key"])
        if not r.get("IsTruncated"):
            break
        token = r.get("NextContinuationToken")
    return keys

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    assert s3_uri.startswith("s3://")
    rest = s3_uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


# =========================
# Mission S3 Structure Helpers
# =========================
def mission_s3_prefix(mission_id: str) -> str:
    """Return S3 prefix for mission: missions/{mission_id}/"""
    return f"missions/{mission_id}/"

def put_mission_json(mission_id: str, data: Dict[str, Any]) -> str:
    """Save mission.json to S3"""
    key = f"{mission_s3_prefix(mission_id)}mission.json"
    return put_s3_text(key, json.dumps(data, ensure_ascii=False, indent=2), "application/json")

def get_mission_json(mission_id: str) -> Dict[str, Any]:
    """Load mission.json from S3"""
    key = f"{mission_s3_prefix(mission_id)}mission.json"
    text = get_s3_text(f"s3://{BUCKET}/{key}")
    return json.loads(text)

def record_event(mission_id: str, event_type: str, data: Dict[str, Any]) -> str:
    """Record event as {ts}-{uuid}.json in events/"""
    ts = now()
    event_id = uuid.uuid4().hex[:8]
    event = {
        "ts": ts,
        "event_id": event_id,
        "event_type": event_type,
        "mission_id": mission_id,
        "data": data
    }
    key = f"{mission_s3_prefix(mission_id)}events/{ts}-{event_id}.json"
    return put_s3_text(key, json.dumps(event, ensure_ascii=False), "application/json")

def put_order_json(mission_id: str, data: Dict[str, Any]) -> str:
    """Save original mission payload under orders/ for resume/replay."""
    key = f"{mission_s3_prefix(mission_id)}orders/payload.json"
    return put_s3_text(key, json.dumps(data, ensure_ascii=False, indent=2), "application/json")

def list_events(mission_id: str) -> List[Dict[str, Any]]:
    """List all events for mission, sorted by timestamp"""
    prefix = f"{mission_s3_prefix(mission_id)}events/"
    keys = [k for k in list_s3_keys(BUCKET, prefix) if k.endswith(".json")]
    events = []
    for key in sorted(keys):
        text = get_s3_text(f"s3://{BUCKET}/{key}")
        events.append(json.loads(text))
    return events

def put_artifact(mission_id: str, filename: str, content: str, content_type: str = "text/plain") -> str:
    """Save artifact to artifacts/ folder"""
    key = f"{mission_s3_prefix(mission_id)}artifacts/{filename}"
    return put_s3_text(key, content, content_type)

def record_comm(
    mission_id: str,
    from_: str,
    to: str,
    comm_type: str,
    content: str,
    task_id: Optional[str] = None,
    ship_class: Optional[str] = None,
    ship_id: Optional[str] = None,
) -> str:
    """Record communication as {ts}-{uuid}.json in comms/"""
    ts = now()
    comm_id = uuid.uuid4().hex[:8]
    comm = {
        "ts": ts,
        "comm_id": comm_id,
        "from": from_,
        "to": to,
        "type": comm_type,
        "content": content
    }
    key = f"{mission_s3_prefix(mission_id)}comms/{ts}-{comm_id}.json"
    s3_uri = put_s3_text(key, json.dumps(comm, ensure_ascii=False), "application/json")

    # Local log (best-effort)
    if log_comm_local:
        if not (task_id and ship_class and ship_id):
            try:
                item = ddb_get_item(STATE_TABLE, {"mission_id": {"S": mission_id}})
                if item:
                    task_id = task_id or item.get("task_id", {}).get("S")
                    ship_class = ship_class or item.get("ship_class", {}).get("S")
                    ship_id = ship_id or item.get("ship_id", {}).get("S")
            except Exception:
                pass
        try:
            log_comm_local(
                mission_id=mission_id,
                task_id=task_id,
                ship_class=ship_class,
                ship_id=ship_id,
                from_role=from_,
                to_role=to,
                comm_type=comm_type,
                content=content,
                source="fleet_node",
                model_id="",
            )
        except Exception:
            pass

    return s3_uri

def list_comms(mission_id: str) -> List[Dict[str, Any]]:
    """List all communications for mission, sorted by timestamp"""
    prefix = f"{mission_s3_prefix(mission_id)}comms/"
    keys = [k for k in list_s3_keys(BUCKET, prefix) if k.endswith(".json")]
    comms = []
    for key in sorted(keys):
        text = get_s3_text(f"s3://{BUCKET}/{key}")
        comms.append(json.loads(text))
    return comms

def put_aar(mission_id: str, aar_content: str) -> str:
    """Save AAR (After Action Report) as aar.md"""
    key = f"{mission_s3_prefix(mission_id)}aar.md"
    return put_s3_text(key, aar_content, "text/markdown; charset=utf-8")


def generate_and_save_aar(mission_id: str) -> None:
    """Generate AAR via aar_generator (best-effort)."""
    try:
        from aar_generator import generate_aar
    except Exception as e:
        print(f"[{NODE_ID}] WARN: aar_generator import failed: {e}")
        return
    try:
        content = generate_aar(mission_id)
        put_aar(mission_id, content)
    except Exception as e:
        print(f"[{NODE_ID}] WARN: AAR generation failed for {mission_id}: {e}")

# =========================
# Fuel / Ammo (Budget)
# =========================
DEFAULT_FUEL_CAP = float(os.getenv("FLEET_FUEL_CAP_USD", "30.0"))
DEFAULT_BB_CAP = int(os.getenv("FLEET_BB_MAIN_GUN_CAP", "4"))
DEFAULT_CA_CAP = int(os.getenv("FLEET_CA_SALVO_CAP", "40"))
DEFAULT_CVB_CAP = int(os.getenv("FLEET_CVB_AIRWING_CAP", "20"))

# Estimated burn per mission (USD, rough)
BURN_BY_SHIPCLASS = {
    "CVL": 0.003,  # micro-like
    "CL":  0.005,  # lite-like
    "DD":  0.050,  # implement
    "CA":  0.300,  # commander
    "BB":  0.500,  # battleship
    "CVB": 0.200,  # armored carrier (campaign)
}


def ensure_budget(month: str) -> None:
    item = ddb_get_item(BUDGET_TABLE, {"month": {"S": month}})
    if item is not None:
        return
    try:
        ddb.put_item(
            TableName=BUDGET_TABLE,
            Item={
                "month": {"S": month},
                "fuel_cap_usd": {"N": str(DEFAULT_FUEL_CAP)},
                "fuel_remaining_usd": {"N": str(DEFAULT_FUEL_CAP)},
                "fuel_burned_usd": {"N": "0"},
                "sorties_total": {"N": "0"},
                "ammo_caps": {"M": {
                    "BB_main_gun": {"N": str(DEFAULT_BB_CAP)},
                    "CA_salvo": {"N": str(DEFAULT_CA_CAP)},
                    "CVB_air_wing": {"N": str(DEFAULT_CVB_CAP)},
                }},
                "ammo_used": {"M": {
                    "BB_main_gun": {"N": "0"},
                    "CA_salvo": {"N": "0"},
                    "CVB_air_wing": {"N": "0"},
                }},
                "updated_at": {"N": str(now())},
            },
            ConditionExpression="attribute_not_exists(#m)",
            ExpressionAttributeNames={"#m": "month"},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def reserve_fuel_and_ammo(month: str, ship_class: str) -> bool:
    ensure_budget(month)
    burn = float(BURN_BY_SHIPCLASS.get(ship_class, 0.01))
    if ship_class not in BURN_BY_SHIPCLASS:
        print(f"Warning: Unknown ship_class '{ship_class}', using default burn rate")

    is_bb = ship_class == "BB"
    is_ca = ship_class == "CA"
    is_cvb = ship_class == "CVB"

    conds = ["fuel_remaining_usd >= :burn"]
    if is_bb:
        conds.append("ammo_used.BB_main_gun < ammo_caps.BB_main_gun")
    if is_ca:
        conds.append("ammo_used.CA_salvo < ammo_caps.CA_salvo")
    if is_cvb:
        conds.append("ammo_used.CVB_air_wing < ammo_caps.CVB_air_wing")

    expr_values = {
        ":burn": {"N": str(burn)},
        ":zero": {"N": "0"},
        ":one": {"N": "1"},
        ":u": {"N": str(now())},
    }

    updates = [
        "fuel_remaining_usd = fuel_remaining_usd - :burn",
        "fuel_burned_usd = if_not_exists(fuel_burned_usd, :zero) + :burn",
        "updated_at = :u",
    ]
    if is_bb:
        updates.append("ammo_used.BB_main_gun = if_not_exists(ammo_used.BB_main_gun, :zero) + :one")
    if is_ca:
        updates.append("ammo_used.CA_salvo = if_not_exists(ammo_used.CA_salvo, :zero) + :one")
    if is_cvb:
        updates.append("ammo_used.CVB_air_wing = if_not_exists(ammo_used.CVB_air_wing, :zero) + :one")

    try:
        ddb.update_item(
            TableName=BUDGET_TABLE,
            Key={"month": {"S": month}},
            UpdateExpression="SET " + ", ".join(updates) + " ADD sorties_total :one",
            ConditionExpression=" AND ".join(conds),
            ExpressionAttributeValues=expr_values,
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


# =========================
# S3 helpers
# =========================
def put_s3_text(key: str, text: str, content_type: str) -> str:
    s3.put_object(Bucket=BUCKET, Key=key, Body=text.encode("utf-8"), ContentType=content_type)
    return f"s3://{BUCKET}/{key}"


def get_s3_text(s3_uri: str) -> str:
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI format: must start with 's3://'")
    _, _, rest = s3_uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="replace")



# =========================
# Bedrock helpers
# =========================

def validate_inference_profile(label: str, model_id: str) -> None:
    if not model_id:
        log_bedrock_error("bedrock", "GetInferenceProfile", "MISSING", RuntimeError(f"{label} not set"))
        raise RuntimeError(f"{label} must be set to an inference profile ARN")
    if not is_inference_profile_arn(model_id):
        log_bedrock_error("bedrock", "GetInferenceProfile", model_id, RuntimeError("model_id not ARN"))
        raise RuntimeError(f"{label} must be an inference profile ARN (got: {model_id})")
    try:
        bedrock_control.get_inference_profile(inferenceProfileIdentifier=model_id)
    except Exception as e:
        log_bedrock_error("bedrock", "GetInferenceProfile", model_id, e)
        raise


def validate_bedrock_profiles() -> None:
    validate_inference_profile("BEDROCK_SONNET_MODEL_ID", BEDROCK_SONNET_MODEL_ID)
    validate_inference_profile("BEDROCK_OPUS_MODEL_ID", BEDROCK_OPUS_MODEL_ID)
    validate_inference_profile("BEDROCK_MICRO_MODEL_ID", BEDROCK_MICRO_MODEL_ID)
    validate_inference_profile("BEDROCK_LITE_MODEL_ID", BEDROCK_LITE_MODEL_ID)


def bedrock_invoke_claude(
    model_id: str,
    system: str,
    user: str,
    max_tokens: int = 1200,
    temperature: float = 0.2,
) -> str:
    if not model_id:
        return "(Bedrock model_id not set)\n"

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [
            {"role": "user", "content": user}
        ],
        "temperature": float(temperature),
    }
    try:
        r = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body).encode("utf-8"),
        )
        data = json.loads(r["body"].read())
    except Exception as e:
        log_bedrock_error("bedrock-runtime", "InvokeModel", model_id, e)
        raise
    # Claude on Bedrock: content is list with {type:"text", text:"..."}
    parts = data.get("content", [])
    texts = [p.get("text","") for p in parts if p.get("type") == "text"]
    return "".join(texts).strip()


# =========================
# Mission model
# =========================
@dataclass
class Mission:
    mission_id: str
    task_id: str
    ship_id: str
    ship_class: str
    role: str
    model: str
    inputs: Dict[str, Any]
    budget: Dict[str, Any]
    constraints: Dict[str, Any]
    signals: Dict[str, Any]


def parse_mission(body: Dict[str, Any]) -> Mission:
    body = normalize_mission_body(body)

    mission_id = body.get("mission_id")
    ship_id = body.get("ship_id")
    task_id = body.get("task_id") or body.get("taskId") or body.get("task")
    if not task_id and mission_id and ship_id:
        task_id = extract_task_id_from_mission(mission_id, ship_id)

    required = {
        "mission_id": mission_id,
        "task_id": task_id,
        "ship_id": ship_id,
    }
    for key, value in required.items():
        if not value:
            raise ValueError(f"Missing required field: {key}")
    return Mission(
        mission_id=mission_id,
        task_id=task_id,
        ship_id=ship_id,
        ship_class=body.get("ship_class", "DD"),
        role=body.get("role", ""),
        model=body.get("model", ""),
        inputs=body.get("inputs", {}),
        budget=body.get("budget", {}),
        constraints=body.get("constraints", {}),
        signals=body.get("signals", {}),
    )


def _maybe_load_json(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(data, dict):
            return data
    return None


def normalize_mission_body(body: Dict[str, Any]) -> Dict[str, Any]:
    # Common wrappers: SNS -> {"Message": "{...}"} or EventBridge -> {"detail": {...}}
    if not isinstance(body, dict):
        raise ValueError("Message body must be a JSON object")

    for key in ("Message", "message", "body", "payload"):
        if key in body:
            nested = _maybe_load_json(body.get(key))
            if nested is not None:
                body = nested
                break

    if isinstance(body, dict) and "detail" in body:
        nested = _maybe_load_json(body.get("detail"))
        if nested is not None:
            body = nested

    return body


def extract_task_id_from_mission(mission_id: str, ship_id: str) -> Optional[str]:
    if not mission_id or not ship_id:
        return None
    prefix = "M-"
    marker = f"-{ship_id}-"
    if mission_id.startswith(prefix):
        idx = mission_id.find(marker)
        if idx != -1:
            return mission_id[len(prefix):idx]
    return None


def repo_workdir(mission_id: str, task_id: str, ship_id: str, ship_class: str) -> Path:
    if ensure_local_workspace:
        try:
            paths = ensure_local_workspace(mission_id, task_id, ship_class, ship_id)
            return paths["work_dir"]
        except Exception as e:
            print(f"[{NODE_ID}] WARN: local workspace init failed: {e}")
    d = WORK_ROOT / f"{task_id}_{ship_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_repo(repo_url: str, workdir: Path) -> None:
    if not repo_url or not isinstance(repo_url, str):
        raise ValueError("Invalid repo_url")
    
    # Support file:// protocol for local directories
    if repo_url.startswith("file://"):
        local_path = Path(repo_url[7:])  # Remove "file://"
        if not local_path.exists():
            raise ValueError(f"Local path does not exist: {local_path}")
        if not local_path.is_dir():
            raise ValueError(f"Local path is not a directory: {local_path}")
        # Copy local directory to workdir
        if not (workdir / ".git").exists():
            subprocess.run(["git", "clone", str(local_path), "."], cwd=str(workdir), check=True)
        else:
            subprocess.run(["git", "fetch", str(local_path), "--all", "--prune"], cwd=str(workdir), check=True)
        return
    
    # Standard git clone/fetch for remote repos
    if (workdir / ".git").exists():
        subprocess.run(["git", "fetch", "--all", "--prune"], cwd=str(workdir), check=True)
        return
    subprocess.run(["git", "clone", repo_url, "."], cwd=str(workdir), check=True)


def checkout_branch(workdir: Path, branch: str) -> None:
    if not branch or not isinstance(branch, str):
        raise ValueError("Invalid branch name")
    code = subprocess.run(["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], cwd=str(workdir)).returncode
    if code == 0:
        subprocess.run(["git", "checkout", branch], cwd=str(workdir), check=True)
    else:
        subprocess.run(["git", "checkout", "-B", branch], cwd=str(workdir), check=True)


def git_diff(workdir: Path) -> str:
    _, out = sh("git diff", cwd=workdir)
    return out


def run_one_test(workdir: Path, cmd: str, timeout_sec: int) -> Tuple[int, str]:
    if not cmd:
        return 0, "(no test cmd)\n"
    return sh(cmd, cwd=workdir, timeout=timeout_sec)


def summarize_ticket_text(ticket: str, max_lines: int = 80) -> str:
    lines = ticket.splitlines()
    head = lines[:max_lines]
    return "\n".join(head) + ("\n\n...(truncated)\n" if len(lines) > max_lines else "\n")


# =========================
# Handlers
# =========================
def handle_dd(m: Mission) -> Dict[str, Any]:
    repo = m.inputs.get("repo", {})
    repo_url = repo.get("url", "")
    branch = repo.get("branch", f"agent/{m.task_id}-{m.ship_id}".lower())
    if not repo_url:
        raise RuntimeError("inputs.repo.url is required")

    wd = repo_workdir(m.mission_id, m.task_id, m.ship_id, m.ship_class)
    ensure_repo(repo_url, wd)
    checkout_branch(wd, branch)

    applied_patch = False
    patch_s3 = m.inputs.get("patch_s3")
    if patch_s3:
        patch_text = get_s3_text(patch_s3)
        (wd / "agent.patch").write_text(patch_text, encoding="utf-8")
        code, out = sh("git apply agent.patch", cwd=wd)
        if code != 0:
            raise RuntimeError("git apply failed:\n" + out)
        applied_patch = True

    test_cmd = m.inputs.get("test_cmd") or DEFAULT_TEST_CMD
    timeout_sec = int(m.constraints.get("time_limit_sec", 1800))
    tcode, tlog = run_one_test(wd, test_cmd, timeout_sec)

    return {
        "repo_url": repo_url,
        "branch": branch,
        "workdir": str(wd),
        "applied_patch": applied_patch,
        "test_cmd": test_cmd,
        "test_exit_code": tcode,
        "test_log": tlog,
        "diff": git_diff(wd),
        "ticket_s3": m.inputs.get("ticket_s3", ""),
    }


def handle_cl(m: Mission) -> Dict[str, Any]:
    # CL: tests only (no patch apply)
    repo = m.inputs.get("repo", {})
    repo_url = repo.get("url", "")
    branch = repo.get("branch", f"agent/{m.task_id}-{m.ship_id}".lower())
    if not repo_url:
        raise RuntimeError("inputs.repo.url is required")

    wd = repo_workdir(m.mission_id, m.task_id, m.ship_id, m.ship_class)
    ensure_repo(repo_url, wd)
    checkout_branch(wd, branch)

    test_cmd = m.inputs.get("test_cmd") or DEFAULT_TEST_CMD
    timeout_sec = int(m.constraints.get("time_limit_sec", 1800))
    tcode, tlog = run_one_test(wd, test_cmd, timeout_sec)

    return {
        "repo_url": repo_url,
        "branch": branch,
        "workdir": str(wd),
        "test_cmd": test_cmd,
        "test_exit_code": tcode,
        "test_log": tlog,
        "diff": git_diff(wd),
        "ticket_s3": m.inputs.get("ticket_s3", ""),
    }


def handle_cvb(m: Mission) -> Dict[str, Any]:
    """
    CVB: test/verification campaign
    - run multiple test commands (test_cmds list) to emulate "air wing"
    """
    repo = m.inputs.get("repo", {})
    repo_url = repo.get("url", "")
    branch = repo.get("branch", f"agent/{m.task_id}".lower())
    if not repo_url:
        raise RuntimeError("inputs.repo.url is required")

    wd = repo_workdir(m.mission_id, m.task_id, m.ship_id, m.ship_class)
    ensure_repo(repo_url, wd)
    checkout_branch(wd, branch)

    timeout_sec = int(m.constraints.get("time_limit_sec", 1800))
    test_cmds: List[str] = m.inputs.get("test_cmds") or []
    if not test_cmds:
        # fallback: single test_cmd
        cmd = m.inputs.get("test_cmd") or DEFAULT_TEST_CMD
        test_cmds = [cmd] if cmd else []

    results = []
    for cmd in test_cmds:
        code, log = run_one_test(wd, cmd, timeout_sec)
        results.append({"cmd": cmd, "exit_code": code, "log": log})

    return {
        "repo_url": repo_url,
        "branch": branch,
        "workdir": str(wd),
        "campaign_results": results,
        "diff": git_diff(wd),
        "ticket_s3": m.inputs.get("ticket_s3", ""),
        "notes": "CVB campaign executed (micro/lite would later generate/add tests; for now runs provided cmds).",
    }


def handle_cvl(m: Mission) -> Dict[str, Any]:
    """
    CVL: recon / compression
    - summarize ticket or logs to reduce context (placeholder)
    """
    ticket_s3 = m.inputs.get("ticket_s3", "")
    ticket_text = ""
    if ticket_s3 and ticket_s3.strip():
        ticket_text = get_s3_text(ticket_s3)
    summary = summarize_ticket_text(ticket_text) if ticket_text else "(no ticket)\n"
    return {
        "ticket_s3": ticket_s3,
        "summary": summary,
        "notes": "CVL recon placeholder (later replace with Micro on Bedrock).",
    }

def handle_ca(m: Mission) -> Dict[str, Any]:
    ticket_s3 = m.inputs.get("ticket_s3", "")
    results_prefix_uri = m.inputs.get("results_prefix", "")
    ticket = get_s3_text(ticket_s3) if ticket_s3 else ""

    # Collect recent result reports (limit to avoid huge context)
    reports_text = ""
    if results_prefix_uri:
        try:
            b, prefix_key = parse_s3_uri(results_prefix_uri)
            keys = [k for k in list_s3_keys(b, prefix_key) if k.endswith(".md")]
            # newest last; take tail
            keys = sorted(keys)[-12:]
            chunks = []
            for k in keys:
                md = s3.get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8", errors="replace")
                chunks.append(f"\n\n---\n# {k}\n{md[:6000]}")
            reports_text = "".join(chunks)
        except Exception as e:
            reports_text = f"\n\n---\n# (report collection failed)\n{e}\n"

    formation_hint = ""
    if load_local_config:
        try:
            cfg = load_local_config()
            formations = cfg.get("formations", {})
            names = sorted([k for k in formations.keys() if isinstance(k, str) and k])
            if names:
                formation_hint = "Available formations: " + ", ".join(names) + ".\n"
        except Exception:
            formation_hint = ""

    system = (
        "You are CA-01, the fleet commander. "
        "You receive mission reports from DD/CL/CVB/CVL and must produce a concise command decision.\n"
        "Output MUST be markdown with sections:\n"
        "1) Situation Summary\n"
        "2) Findings (bullets)\n"
        "3) Next Orders (concrete actions)\n"
        "4) If Escalation Needed: choose BB or more CVB and why\n"
        "If you want to change workflow formation, add a standalone line:\n"
        "Formation: <name>  (or 陣形: <name>)\n"
        "If no change, omit the formation line.\n"
        "If you want additional execution before final decision, add a line:\n"
        "Extra Stages: <stage1>, <stage2>  (or 追加: <stage1>, <stage2>)\n"
        "Extra stages will run and CA will be scheduled again. Do not combine with Formation.\n"
        "If you want explicit control, you can also add lines:\n"
        "Execute: <stage1>, <stage2>\n"
        "Skip: <stage1>, <stage2>\n"
        "Patch: <stage>=s3://bucket/key.patch\n"
        "Done: yes/no\n"
        "Also include a line: Confidence: <0-100>\n"
        f"{formation_hint}"
    )

    user = (
        f"## Ticket\n{ticket}\n\n"
        f"## Reports\n{reports_text}\n\n"
        "Decide next orders. If tests failed or evidence unclear, propose CVB campaign settings. "
        "If architecture or deep fix needed, propose BB strike."
    )

    def fallback_ca_report(reason: str) -> str:
        return (
            "1) Situation Summary\n"
            f"- Ticket present: {'yes' if bool(ticket.strip()) else 'no'}\n"
            f"- Reports collected: {'yes' if bool(reports_text.strip()) else 'no'}\n"
            "\n"
            "2) Findings (bullets)\n"
            f"- CA fallback used: {reason}\n"
            "- Evidence is limited; recommend additional verification if needed.\n"
            "\n"
            "3) Next Orders (concrete actions)\n"
            "- If tests or evidence are insufficient, dispatch CVB with targeted test commands.\n"
            "- If scope/architecture issues are suspected, consider BB escalation.\n"
            "\n"
            "4) If Escalation Needed: choose BB or more CVB and why\n"
            "- Default: CVB first for evidence gathering; BB only if stuck or architectural change is required.\n"
            "\n"
            "Confidence: 50\n"
        )

    if not BEDROCK_SONNET_MODEL_ID:
        ca_text = fallback_ca_report("BEDROCK_SONNET_MODEL_ID not set")
    else:
        try:
            max_tokens, temperature = get_armament("CA", 1400, 0.2)
            ca_text = bedrock_invoke_claude(
                BEDROCK_SONNET_MODEL_ID,
                system,
                user,
                max_tokens=max_tokens,
                temperature=temperature,
            )
        except Exception as e:
            ca_text = fallback_ca_report(f"Bedrock invoke failed: {e}")
    return {
        "ticket_s3": ticket_s3,
        "results_prefix": results_prefix_uri,
        "ca_report": ca_text,
        "notes": "CA generated by Bedrock Sonnet." if BEDROCK_SONNET_MODEL_ID else "CA generated by fallback (no Bedrock model_id)."
    }

def handle_bb(m: Mission) -> Dict[str, Any]:
    ticket_s3 = m.inputs.get("ticket_s3", "")
    results_prefix_uri = m.inputs.get("results_prefix", "")
    ticket = get_s3_text(ticket_s3) if ticket_s3 else ""

    # できればCVB/CLの結果を少し拾う（CAと同じヘルパでOK）
    reports_text = ""
    if results_prefix_uri:
        b, prefix_key = parse_s3_uri(results_prefix_uri)
        keys = [k for k in list_s3_keys(b, prefix_key) if k.endswith(".md")]
        keys = sorted(keys)[-12:]
        chunks = []
        for k in keys:
            md = s3.get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8", errors="replace")
            chunks.append(f"\n\n---\n# {k}\n{md[:7000]}")
        reports_text = "".join(chunks)

    system = (
        "You are BB-01, the fleet's battleship (decisive strike). "
        "Your job is to break deadlocks and propose the most likely root cause and fix.\n"
        "Output MUST be markdown with:\n"
        "1) Root cause hypothesis\n"
        "2) Proposed fix (step-by-step)\n"
        "3) Risks / tradeoffs\n"
        "4) Minimal verification plan\n"
        "Also include a line: Confidence: <0-100>\n"
    )

    user = (
        f"## Ticket\n{ticket}\n\n"
        f"## Reports\n{reports_text}\n\n"
        "Provide a decisive fix plan. If code changes are needed, describe them precisely."
    )

    # Opus用の model_id を環境変数で
    max_tokens, temperature = get_armament("BB", 1600, 0.2)
    bb_text = bedrock_invoke_claude(
        BEDROCK_OPUS_MODEL_ID,
        system,
        user,
        max_tokens=max_tokens,
        temperature=temperature,
    )

    return {
        "ticket_s3": ticket_s3,
        "results_prefix": results_prefix_uri,
        "bb_strike": bb_text,
        "notes": "BB generated by Bedrock Opus."
    }



def make_report_md(
    m: Mission,
    result: Dict[str, Any],
    budget_ok: bool,
    confidence_level: Optional[int] = None,
) -> str:
    lines = []
    lines.append(f"# {m.task_id} / {m.mission_id}")
    lines.append("")
    lines.append("## Ship")
    lines.append(f"- ship_id: `{m.ship_id}`")
    lines.append(f"- class: `{m.ship_class}`")
    ship_base = canonical_ship_class(m.ship_class)
    if ship_base != m.ship_class:
        lines.append(f"- class_base: `{ship_base}`")
    lines.append(f"- role: `{m.role}`")
    lines.append(f"- model(tag): `{m.model}`")
    lines.append("")
    if confidence_level is not None:
        lines.append("## Confidence")
        lines.append(f"- confidence_level: `{confidence_level}`")
        lines.append("")
    if os.getenv("FLEET_INCLUDE_MENTAL_MODEL", "0").lower() not in ("0", "false", "no"):
        if get_ship_profile and format_mental_model:
            profile = get_ship_profile(m.ship_class)
            if profile:
                lines.append("## Mental Model")
                lines.append("```")
                lines.append(format_mental_model(m.ship_class, profile).rstrip())
                lines.append("```")
                lines.append("")
    lines.append("## Budget")
    lines.append(f"- reserve_ok: `{budget_ok}`")
    lines.append("")

    if ship_base in ("DD", "CL", "CVB"):
        lines.append("## Repo")
        lines.append(f"- url: `{result.get('repo_url','')}`")
        lines.append(f"- branch: `{result.get('branch','')}`")
        lines.append(f"- workdir: `{result.get('workdir','')}`")
        lines.append("")

    if ship_base == "CVL":
        lines.append("## Recon Summary")
        lines.append("```")
        lines.append((result.get("summary","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if ship_base in ("DD", "CL"):
        lines.append("## Tests")
        lines.append(f"- cmd: `{result.get('test_cmd','')}`")
        lines.append(f"- exit_code: `{result.get('test_exit_code','')}`")
        lines.append("")
        lines.append("### Test Log")
        lines.append("```")
        lines.append((result.get("test_log","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if ship_base == "CVB":
        lines.append("## CVB Campaign Results")
        campaign_results = result.get("campaign_results", [])
        if not campaign_results:
            lines.append("**WARNING: No test commands executed**")
            lines.append("")
        for i, r in enumerate(campaign_results, start=1):
            lines.append(f"### Sortie {i}")
            lines.append(f"- cmd: `{r.get('cmd','')}`")
            lines.append(f"- exit_code: `{r.get('exit_code','')}`")
            lines.append("```")
            lines.append((r.get("log","") or "").rstrip())
            lines.append("```")
            lines.append("")

    if ship_base == "CA":
        lines.append("## CA Decision")
        lines.append("```")
        lines.append((result.get("ca_report","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if ship_base == "BB":
        lines.append("## BB Strike Plan")
        lines.append("```")
        lines.append((result.get("bb_strike","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if "diff" in result:
        lines.append("## Diff")
        lines.append("```diff")
        lines.append((result.get("diff","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if result.get("ticket_s3"):
        lines.append("## Ticket")
        lines.append(f"- {result['ticket_s3']}")
        lines.append("")

    if result.get("notes"):
        lines.append("## Notes")
        lines.append(result["notes"])
        lines.append("")

    return "\n".join(lines)


# =========================
# Main loop
# =========================
def main() -> None:
    validate_bedrock_profiles()
    print(f"[{NODE_ID}] fleet node started (region={REGION}, queue={SQS_NAME})")
    while True:
        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=SQS_VISIBILITY_SEC,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        msg = msgs[0]
        receipt = msg["ReceiptHandle"]
        try:
            body = json.loads(msg["Body"])
        except json.JSONDecodeError as e:
            print(f"[{NODE_ID}] INVALID JSON body: {e}")
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            continue
        try:
            m = parse_mission(body)
        except ValueError as e:
            print(f"[{NODE_ID}] INVALID MESSAGE: {e}")
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            continue

        # Ensure state exists
        init_mission_state(m.mission_id, m.task_id, m.ship_id, m.ship_class)
        # Ensure local workspace + mental model (best-effort)
        if ensure_local_workspace:
            try:
                ensure_local_workspace(m.mission_id, m.task_id, m.ship_class, m.ship_id)
            except Exception as e:
                print(f"[{NODE_ID}] WARN: local workspace init failed: {e}")
        # Save mission payload for resume/replay (best-effort)
        try:
            normalized_body = normalize_mission_body(body)
            put_order_json(m.mission_id, normalized_body)
        except Exception as e:
            print(f"[{NODE_ID}] WARN: failed to store mission payload for {m.mission_id}: {e}")

        if not try_lock_mission(m.mission_id):
            continue

        month = m.budget.get("month", "")
        budget_ok = True
        if month:
            ship_base = canonical_ship_class(m.ship_class)
            budget_ok = reserve_fuel_and_ammo(month, ship_base)
            if not budget_ok:
                mark_budget_denied(m.mission_id, f"BudgetDenied month={month} class={m.ship_class}")
                # Best-effort budget denied report
                try:
                    denied_report = f"""# BUDGET_DENIED: {m.task_id} / {m.mission_id}

## Ship
- ship_id: `{m.ship_id}`
- class: `{m.ship_class}`
- role: `{m.role}`

## Budget
- month: `{month}`
- reserve_ok: `False`
"""
                    put_artifact(m.mission_id, f"budget_denied.{m.ship_id}.md", denied_report, "text/markdown; charset=utf-8")
                except Exception as e:
                    print(f"[{NODE_ID}] WARN: failed to write budget denied report: {e}")
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
                print(f"[{NODE_ID}] BudgetDenied {m.mission_id} ({m.ship_class})")
                continue

        try:
            ship_base = canonical_ship_class(m.ship_class)
            if ship_base == "DD":
                result = handle_dd(m)
            elif ship_base == "CL":
                result = handle_cl(m)
            elif ship_base == "CVB":
                result = handle_cvb(m)
            elif ship_base == "CVL":
                result = handle_cvl(m)
            elif ship_base == "BB":
                result = handle_bb(m)
            elif ship_base == "CA":
                result = handle_ca(m)
            else:
                result = {"notes": f"unknown ship_class={m.ship_class}"}

            ship_base = canonical_ship_class(m.ship_class)
            confidence_level = derive_confidence(ship_base, result)
            result["confidence_level"] = confidence_level
            report = make_report_md(m, result, budget_ok, confidence_level=confidence_level)
            artifact_name = f"report.{m.ship_id}.md"
            result_s3 = put_artifact(m.mission_id, artifact_name, report, "text/markdown; charset=utf-8")
            update_confidence(m.mission_id, confidence_level)
            if WRITE_LEGACY_RESULTS:
                legacy_key = f"results/{m.task_id}/{m.mission_id}.{m.ship_id}.md"
                try:
                    put_s3_text(legacy_key, report, "text/markdown; charset=utf-8")
                except Exception as e:
                    print(f"[{NODE_ID}] WARN: failed to write legacy result for {m.mission_id}: {e}")

            mark_done(m.mission_id, result_s3)
            try:
                generate_and_save_aar(m.mission_id)
            except Exception as e:
                print(f"[{NODE_ID}] WARN: failed to generate AAR for {m.mission_id}: {e}")
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

            print(f"[{NODE_ID}] DONE {m.mission_id} ({m.ship_id}/{m.ship_class}) -> {result_s3}")

        except Exception as e:
            error_msg = str(e)
            mark_failed(m.mission_id, error_msg)
            
            # Save failure report to S3
            failure_report = f"""# FAILED: {m.task_id} / {m.mission_id}

## Ship
- ship_id: `{m.ship_id}`
- class: `{m.ship_class}`
- role: `{m.role}`

## Error
```
{error_msg}
```

## Budget
- reserve_ok: `{budget_ok}`
"""
            try:
                put_artifact(m.mission_id, f"failure.{m.ship_id}.md", failure_report, "text/markdown; charset=utf-8")
                if WRITE_LEGACY_RESULTS:
                    key = f"results/{m.task_id}/{m.mission_id}.{m.ship_id}.FAILED.md"
                    put_s3_text(key, failure_report, "text/markdown; charset=utf-8")
            except Exception:
                pass
            
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            print(f"[{NODE_ID}] FAILED {m.mission_id}: {e}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
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

#==========================
# model ID
#=========================
BEDROCK_SONNET_MODEL_ID = os.getenv("BEDROCK_SONNET_MODEL_ID", "")
BEDROCK_MICRO_MODEL_ID  = os.getenv("BEDROCK_MICRO_MODEL_ID", "")
BEDROCK_LITE_MODEL_ID   = os.getenv("BEDROCK_LITE_MODEL_ID", "")
BEDROCK_OPUS_MODEL_ID   = os.getenv("BEDROCK_OPUS_MODEL_ID", "")



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
def ddb_get_item(table: str, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = ddb.get_item(TableName=table, Key=key)
    return r.get("Item")


def try_lock_mission(mission_id: str, lock_sec: int = LOCK_SEC) -> bool:
    n = now()
    lock_until = n + lock_sec
    try:
        ddb.update_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}},
            UpdateExpression="SET #s=:inflight, #owner=:o, #lock_until=:lu, updated_at=:u ADD attempt :one",
            ExpressionAttributeNames={"#s": "status", "#owner": "owner", "#lock_until": "lock_until"},
            ExpressionAttributeValues={
                ":inflight": {"S": "INFLIGHT"},
                ":o": {"S": NODE_ID},
                ":lu": {"N": str(lock_until)},
                ":u": {"N": str(n)},
                ":one": {"N": "1"},
                ":ready": {"S": "READY"},
                ":n": {"N": str(n)},
            },
            ConditionExpression="attribute_not_exists(#s) OR #s=:ready OR #lock_until < :n",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def mark_done(mission_id: str, result_s3: str) -> None:
    ddb.update_item(
        TableName=STATE_TABLE,
        Key={"mission_id": {"S": mission_id}},
        UpdateExpression="SET #s=:done, result_s3=:r, updated_at=:u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":done": {"S": "DONE"}, ":r": {"S": result_s3}, ":u": {"N": str(now())}},
    )


def mark_failed(mission_id: str, reason: str) -> None:
    safe_reason = reason.encode('utf-8')[:3500].decode('utf-8', errors='ignore')
    ddb.update_item(
        TableName=STATE_TABLE,
        Key={"mission_id": {"S": mission_id}},
        UpdateExpression="SET #s=:failed, fail_reason=:fr, updated_at=:u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":failed": {"S": "FAILED"}, ":fr": {"S": safe_reason}, ":u": {"N": str(now())}},
    )

# ========================
# S3 helpers
# ========================
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

def bedrock_invoke_claude(model_id: str, system: str, user: str, max_tokens: int = 1200) -> str:
    if not model_id:
        return "(Bedrock model_id not set)\n"

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [
            {"role": "user", "content": user}
        ],
        "temperature": 0.2,
    }
    r = bedrock.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body).encode("utf-8"),
    )
    data = json.loads(r["body"].read())
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


def repo_workdir(task_id: str, ship_id: str) -> Path:
    d = WORK_ROOT / f"{task_id}_{ship_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_repo(repo_url: str, workdir: Path) -> None:
    if not repo_url or not isinstance(repo_url, str):
        raise ValueError("Invalid repo_url")
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

    wd = repo_workdir(m.task_id, m.ship_id)
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

    wd = repo_workdir(m.task_id, m.ship_id)
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

    wd = repo_workdir(m.task_id, m.ship_id)
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
        b, prefix_key = parse_s3_uri(results_prefix_uri)
        keys = [k for k in list_s3_keys(b, prefix_key) if k.endswith(".md")]
        # newest last; take tail
        keys = sorted(keys)[-12:]
        chunks = []
        for k in keys:
            md = s3.get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8", errors="replace")
            chunks.append(f"\n\n---\n# {k}\n{md[:6000]}")
        reports_text = "".join(chunks)

    system = (
        "You are CA-01, the fleet commander. "
        "You receive mission reports from DD/CL/CVB/CVL and must produce a concise command decision.\n"
        "Output MUST be markdown with sections:\n"
        "1) Situation Summary\n"
        "2) Findings (bullets)\n"
        "3) Next Orders (concrete actions)\n"
        "4) If Escalation Needed: choose BB or more CVB and why\n"
    )

    user = (
        f"## Ticket\n{ticket}\n\n"
        f"## Reports\n{reports_text}\n\n"
        "Decide next orders. If tests failed or evidence unclear, propose CVB campaign settings. "
        "If architecture or deep fix needed, propose BB strike."
    )

    ca_text = bedrock_invoke_claude(BEDROCK_SONNET_MODEL_ID, system, user, max_tokens=1400)
    return {
        "ticket_s3": ticket_s3,
        "results_prefix": results_prefix_uri,
        "ca_report": ca_text,
        "notes": "CA generated by Bedrock Sonnet."
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
    )

    user = (
        f"## Ticket\n{ticket}\n\n"
        f"## Reports\n{reports_text}\n\n"
        "Provide a decisive fix plan. If code changes are needed, describe them precisely."
    )

    # Opus用の model_id を環境変数で
    bb_text = bedrock_invoke_claude(BEDROCK_OPUS_MODEL_ID, system, user, max_tokens=1600)

    return {
        "ticket_s3": ticket_s3,
        "results_prefix": results_prefix_uri,
        "bb_strike": bb_text,
        "notes": "BB generated by Bedrock Opus."
    }



def make_report_md(m: Mission, result: Dict[str, Any], budget_ok: bool) -> str:
    lines = []
    lines.append(f"# {m.task_id} / {m.mission_id}")
    lines.append("")
    lines.append("## Ship")
    lines.append(f"- ship_id: `{m.ship_id}`")
    lines.append(f"- class: `{m.ship_class}`")
    lines.append(f"- role: `{m.role}`")
    lines.append(f"- model(tag): `{m.model}`")
    lines.append("")
    lines.append("## Budget")
    lines.append(f"- reserve_ok: `{budget_ok}`")
    lines.append("")

    if m.ship_class in ("DD", "CL", "CVB"):
        lines.append("## Repo")
        lines.append(f"- url: `{result.get('repo_url','')}`")
        lines.append(f"- branch: `{result.get('branch','')}`")
        lines.append(f"- workdir: `{result.get('workdir','')}`")
        lines.append("")

    if m.ship_class == "CVL":
        lines.append("## Recon Summary")
        lines.append("```")
        lines.append((result.get("summary","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if m.ship_class in ("DD", "CL"):
        lines.append("## Tests")
        lines.append(f"- cmd: `{result.get('test_cmd','')}`")
        lines.append(f"- exit_code: `{result.get('test_exit_code','')}`")
        lines.append("")
        lines.append("### Test Log")
        lines.append("```")
        lines.append((result.get("test_log","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if m.ship_class == "CVB":
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

    if m.ship_class == "CA":
        lines.append("## CA Decision")
        lines.append("```")
        lines.append((result.get("ca_report","") or "").rstrip())
        lines.append("```")
        lines.append("")

    if m.ship_class == "BB":
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
        try:
            ddb.update_item(
                TableName=STATE_TABLE,
                Key={"mission_id": {"S": m.mission_id}},
                UpdateExpression="SET #s=if_not_exists(#s,:ready), updated_at=:u",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":ready": {"S": "READY"}, ":u": {"N": str(now())}},
            )
        except Exception:
            pass

        if not try_lock_mission(m.mission_id):
            continue

        month = m.budget.get("month", "")
        budget_ok = True
        if month:
            budget_ok = reserve_fuel_and_ammo(month, m.ship_class)
            if not budget_ok:
                mark_failed(m.mission_id, f"BudgetDenied month={month} class={m.ship_class}")
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
                print(f"[{NODE_ID}] BudgetDenied {m.mission_id} ({m.ship_class})")
                continue

        try:
            if m.ship_class == "DD":
                result = handle_dd(m)
            elif m.ship_class == "CL":
                result = handle_cl(m)
            elif m.ship_class == "CVB":
                result = handle_cvb(m)
            elif m.ship_class == "CVL":
                result = handle_cvl(m)
            elif m.ship_class in "BB":
                result = handle_bb(m)
            elif m.ship_class == "CA":
                result = handle_ca(m)
            else:
                result = {"notes": f"unknown ship_class={m.ship_class}"}

            report = make_report_md(m, result, budget_ok)
            key = f"results/{m.task_id}/{m.mission_id}.{m.ship_id}.md"
            result_s3 = put_s3_text(key, report, "text/markdown; charset=utf-8")

            mark_done(m.mission_id, result_s3)
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
                key = f"results/{m.task_id}/{m.mission_id}.{m.ship_id}.FAILED.md"
                put_s3_text(key, failure_report, "text/markdown; charset=utf-8")
            except Exception:
                pass
            
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            print(f"[{NODE_ID}] FAILED {m.mission_id}: {e}")


if __name__ == "__main__":
    main()

import json
import os
import time
import uuid
from pathlib import Path
from typing import Dict, Any, List

import boto3

from fleet_config import resolve_fleet_config


_cfg = resolve_fleet_config()
REGION = _cfg["region"]
SQS_NAME = _cfg["sqs_name"]
BUCKET = _cfg["bucket"]

if not SQS_NAME:
    raise ValueError("FLEET_SQS_NAME environment variable is required (or set in FleetNodePolicy.json)")
if not BUCKET:
    raise ValueError("FLEET_S3_BUCKET environment variable is required (or set in FleetNodePolicy.json)")

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

QUEUE_URL = sqs.get_queue_url(QueueName=SQS_NAME)["QueueUrl"]


def send(body: Dict[str, Any]) -> None:
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body, ensure_ascii=False))


def put_ticket(task_id: str, ticket_path: Path) -> str:
    key = f"tickets/{task_id}.md"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=ticket_path.read_bytes(),
        ContentType="text/markdown; charset=utf-8",
    )
    return f"s3://{BUCKET}/{key}"


def auto_capital_choice(signals: Dict[str, Any]) -> str:
    """Return 'CVB' or 'BB' based on signals."""
    if signals.get("needs_many_tests") or signals.get("flaky"):
        return "CVB"
    if signals.get("blocked") or signals.get("needs_architecture"):
        return "BB"
    # default: CVB first (cheaper + evidence gathering), then CA can decide to call BB
    return "CVB"


def main():
    task_id = os.getenv("TASK_ID", "T-0001")
    repo_url = os.getenv("REPO_URL", "")
    month = os.getenv("BUDGET_MONTH", time.strftime("%Y-%m"))

    # Ticket
    ticket_file_env = os.getenv("TICKET_FILE", f"./{task_id}.md")
    ticket_file = Path(ticket_file_env).resolve()
    if not str(ticket_file).startswith(str(Path.cwd())):
        raise ValueError(f"TICKET_FILE must be within current directory: {ticket_file}")
    if not ticket_file.exists():
        ticket_file.write_text(f"# {task_id}\n\n- Describe objective here.\n", encoding="utf-8")
    ticket_s3 = put_ticket(task_id, ticket_file)

    # Signals (JSON string env or defaults)
    try:
        signals = json.loads(os.getenv("SIGNALS_JSON", "{}") or "{}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in SIGNALS_JSON: {e}")
    # e.g. {"flaky":true,"needs_many_tests":true,"blocked":false,"needs_architecture":false}

    # Common constraints
    try:
        time_limit = int(os.getenv("TIME_LIMIT_SEC", "1800"))
    except ValueError:
        raise ValueError("TIME_LIMIT_SEC must be an integer")

    # 1) CVL Recon (placeholder)
    recon_id = f"M-{task_id}-CVL-{uuid.uuid4().hex[:6]}"
    send({
        "mission_id": recon_id,
        "task_id": task_id,
        "ship_id": "CVL-01",
        "ship_class": "CVL",
        "role": "recon",
        "model": "nova.micro",  # tag only for now
        "inputs": {"ticket_s3": ticket_s3},
        "constraints": {"time_limit_sec": 600},
        "budget": {"month": month, "max_cost_usd": 30.0},
        "signals": signals
    })

    # 2) DD Implement swarm (A/B/C)
    dd_ships = os.getenv("DD_SHIPS", "DD-01,DD-02,DD-03").split(",")
    test_cmd = os.getenv("TEST_CMD", "")  # optional single test cmd for DD
    if not repo_url:
        dd_ships = []  # Skip DD if no repo
    for ship in [s.strip() for s in dd_ships if s.strip()]:
        mid = f"M-{task_id}-{ship}-{uuid.uuid4().hex[:6]}"
        branch = f"agent/{task_id}-{ship}".lower()
        send({
            "mission_id": mid,
            "task_id": task_id,
            "ship_id": ship,
            "ship_class": "DD",
            "role": "implement",
            "model": "mistral",  # tag only
            "inputs": {
                "ticket_s3": ticket_s3,
                "repo": {"url": repo_url, "branch": branch},
                **({"test_cmd": test_cmd} if test_cmd else {}),
            },
            "constraints": {"time_limit_sec": time_limit, "max_attempts": 3},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals
        })

    # 3) CL Test pass (single)
    cl_branch = os.getenv("CL_BRANCH", f"agent/{task_id}".lower())
    cl_test_cmd = os.getenv("CL_TEST_CMD", test_cmd)
    if repo_url:
        cl_id = f"M-{task_id}-CL-01-{uuid.uuid4().hex[:6]}"
        send({
            "mission_id": cl_id,
            "task_id": task_id,
            "ship_id": "CL-01",
            "ship_class": "CL",
            "role": "test",
            "model": "nova.lite",  # tag only
            "inputs": {
                "ticket_s3": ticket_s3,
                "repo": {"url": repo_url, "branch": cl_branch},
                **({"test_cmd": cl_test_cmd} if cl_test_cmd else {}),
            },
            "constraints": {"time_limit_sec": time_limit, "max_attempts": 3},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals
        })

    # 4) Capital ship selection (BB or CVB)
    capital = auto_capital_choice(signals)

    # New structure: missions are under missions/{mission_id}/... and mission_id starts with M-{task_id}-*
    # Use a prefix that matches all missions for this task.
    results_prefix = f"s3://{BUCKET}/missions/M-{task_id}-"

    if capital == "CVB" and repo_url:
        # CVB campaign: micro/lite "air wing" emulated by running multiple test_cmds
        # Provide multiple commands via env TEST_CMDS="pytest;pytest -q;cargo test"
        test_cmds_env = os.getenv("TEST_CMDS", "")
        test_cmds: List[str] = [c.strip() for c in test_cmds_env.split(";") if c.strip()]
        # fallback: if none, try single command
        if not test_cmds and cl_test_cmd:
            test_cmds = [cl_test_cmd]

        try:
            cvb_sorties = int(os.getenv("CVB_SORTIES", "3"))
        except ValueError:
            raise ValueError("CVB_SORTIES must be an integer")
        for i in range(1, cvb_sorties + 1):
            ship_id = f"CVB-{i:02d}"
            mid = f"M-{task_id}-{ship_id}-{uuid.uuid4().hex[:6]}"
            send({
                "mission_id": mid,
                "task_id": task_id,
                "ship_id": ship_id,
                "ship_class": "CVB",
                "role": "test_swarm",
                "model": "micro/lite",  # tag only (later use Micro/Lite on Bedrock)
                "inputs": {
                    "ticket_s3": ticket_s3,
                    "repo": {"url": repo_url, "branch": cl_branch},
                    **({"test_cmds": test_cmds} if test_cmds else {}),
                },
                "constraints": {"time_limit_sec": time_limit, "max_attempts": 2},
                "budget": {"month": month, "max_cost_usd": 30.0},
                "signals": signals
            })

    else:
        # BB single decisive strike (placeholder for Opus later)
        bb_id = f"M-{task_id}-BB-01-{uuid.uuid4().hex[:6]}"
        send({
            "mission_id": bb_id,
            "task_id": task_id,
            "ship_id": "BB-01",
            "ship_class": "BB",
            "role": "breakthrough",
            "model": "opus",  # tag only
            "inputs": {
                "ticket_s3": ticket_s3,
                "results_prefix": results_prefix,
            },
            "constraints": {"time_limit_sec": time_limit, "max_attempts": 1},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals
        })

    # 5) CA commander integration (placeholder for Sonnet later)
    ca_id = f"M-{task_id}-CA-01-{uuid.uuid4().hex[:6]}"
    send({
        "mission_id": ca_id,
        "task_id": task_id,
        "ship_id": "CA-01",
        "ship_class": "CA",
        "role": "command",
        "model": "sonnet",  # tag only
        "inputs": {
            "ticket_s3": ticket_s3,
            "results_prefix": results_prefix,
        },
        "constraints": {"time_limit_sec": 1200, "max_attempts": 1},
        "budget": {"month": month, "max_cost_usd": 30.0},
        "signals": signals
    })

    print("Enqueued.")
    print("task_id:", task_id)
    print("ticket:", ticket_s3)
    print("queue:", QUEUE_URL)
    print("capital:", capital)


if __name__ == "__main__":
    main()

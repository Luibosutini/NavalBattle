import argparse
import json
import os
import time
import uuid
from pathlib import Path
from typing import Dict, Any

import boto3

from fleet_config import resolve_fleet_config


def send(sqs: boto3.client, queue_url: str, body: Dict[str, Any], dry_run: bool) -> None:
    payload = json.dumps(body, ensure_ascii=False)
    if dry_run:
        print("[dry-run] send:", payload)
        return
    sqs.send_message(QueueUrl=queue_url, MessageBody=payload)


def put_ticket(s3: boto3.client, bucket: str, task_id: str, ticket_path: Path, dry_run: bool) -> str:
    key = f"tickets/{task_id}.md"
    if dry_run:
        print(f"[dry-run] put_ticket: s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=ticket_path.read_bytes(),
        ContentType="text/markdown; charset=utf-8",
    )
    return f"s3://{bucket}/{key}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue Phase1 missions.")
    parser.add_argument("--dry-run", action="store_true", help="Print messages without sending to AWS.")
    return parser.parse_args()

def main():
    args = parse_args()
    dry_run = args.dry_run or os.getenv("FLEET_DRY_RUN", "0") == "1"

    cfg = resolve_fleet_config()
    region = cfg["region"]
    sqs_name = cfg["sqs_name"]
    bucket = cfg["bucket"]

    if not dry_run and not sqs_name:
        raise ValueError("FLEET_SQS_NAME environment variable is required (or set in FleetNodePolicy.json)")
    if not dry_run and not bucket:
        raise ValueError("FLEET_S3_BUCKET environment variable is required (or set in FleetNodePolicy.json)")

    if dry_run:
        if not sqs_name:
            sqs_name = "dry-run-queue"
        if not bucket:
            bucket = "dry-run-bucket"
        sqs = None
        s3 = None
        queue_url = "(dry-run)"
    else:
        sqs = boto3.client("sqs", region_name=region)
        s3 = boto3.client("s3", region_name=region)
        queue_url = sqs.get_queue_url(QueueName=sqs_name)["QueueUrl"]

    task_id = os.getenv("TASK_ID", "T-0001")
    repo_url = os.getenv("REPO_URL")
    if not repo_url:
        raise ValueError("REPO_URL environment variable is required")
    month = os.getenv("BUDGET_MONTH", time.strftime("%Y-%m"))
    try:
        time_limit = int(os.getenv("TIME_LIMIT_SEC", "1800"))
    except ValueError:
        raise ValueError("TIME_LIMIT_SEC must be an integer")

    ticket_file = Path(os.getenv("TICKET_FILE", f"./{task_id}.md")).resolve()
    if not str(ticket_file).startswith(str(Path.cwd())):
        raise ValueError(f"TICKET_FILE must be within current directory: {ticket_file}")
    if not ticket_file.exists():
        ticket_file.write_text(f"# {task_id}\n\n- Describe objective here.\n", encoding="utf-8")
    ticket_s3 = put_ticket(s3, bucket, task_id, ticket_file, dry_run)

    try:
        signals = json.loads(os.getenv("SIGNALS_JSON", "{}") or "{}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in SIGNALS_JSON: {e}")

    # CVL recon (optional but cheap)
    if os.getenv("ENABLE_CVL", "1") == "1":
        mid = f"M-{task_id}-CVL-01-{uuid.uuid4().hex[:6]}"
        send(sqs, queue_url, {
            "mission_id": mid,
            "task_id": task_id,
            "ship_id": "CVL-01",
            "ship_class": "CVL",
            "role": "recon",
            "model": "nova.micro",
            "inputs": {"ticket_s3": ticket_s3},
            "constraints": {"time_limit_sec": 600},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals,
        }, dry_run)

    # DD implement swarm
    dd_ships = [s.strip() for s in os.getenv("DD_SHIPS", "DD-01,DD-02,DD-03").split(",") if s.strip()]
    test_cmd = os.getenv("TEST_CMD", "")  # optional
    for ship in dd_ships:
        mid = f"M-{task_id}-{ship}-{uuid.uuid4().hex[:6]}"
        branch = f"agent/{task_id}-{ship}".lower()
        send(sqs, queue_url, {
            "mission_id": mid,
            "task_id": task_id,
            "ship_id": ship,
            "ship_class": "DD",
            "role": "implement",
            "model": "mistral",
            "inputs": {
                "ticket_s3": ticket_s3,
                "repo": {"url": repo_url, "branch": branch},
                **({"test_cmd": test_cmd} if test_cmd else {}),
            },
            "constraints": {"time_limit_sec": time_limit, "max_attempts": 3},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals,
        }, dry_run)

    # CL test
    cl_mid = f"M-{task_id}-CL-01-{uuid.uuid4().hex[:6]}"
    cl_branch = os.getenv("CL_BRANCH", f"agent/{task_id}".lower())
    cl_test_cmd = os.getenv("CL_TEST_CMD", test_cmd)
    send(sqs, queue_url, {
        "mission_id": cl_mid,
        "task_id": task_id,
        "ship_id": "CL-01",
        "ship_class": "CL",
        "role": "test",
        "model": "nova.lite",
        "inputs": {
            "ticket_s3": ticket_s3,
            "repo": {"url": repo_url, "branch": cl_branch},
            **({"test_cmd": cl_test_cmd} if cl_test_cmd else {}),
        },
        "constraints": {"time_limit_sec": time_limit, "max_attempts": 2},
        "budget": {"month": month, "max_cost_usd": 30.0},
        "signals": signals,
    }, dry_run)

    print("Phase1 enqueued.")
    print("task_id:", task_id)
    print("ticket:", ticket_s3)
    print("queue:", queue_url)

if __name__ == "__main__":
    main()

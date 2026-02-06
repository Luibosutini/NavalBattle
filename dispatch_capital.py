import argparse
import json
import os
import time
import uuid
from typing import Dict, Any, List

import boto3

from fleet_config import resolve_fleet_config


def send(sqs: boto3.client, queue_url: str, body: Dict[str, Any], dry_run: bool) -> None:
    payload = json.dumps(body, ensure_ascii=False)
    if dry_run:
        print("[dry-run] send:", payload)
        return
    sqs.send_message(QueueUrl=queue_url, MessageBody=payload)


def list_result_keys(s3: boto3.client, bucket: str, task_id: str) -> List[str]:
    prefix = f"results/{task_id}/"
    keys = []
    token = None
    while True:
        args = {"Bucket": bucket, "Prefix": prefix}
        if token:
            args["ContinuationToken"] = token
        r = s3.list_objects_v2(**args)
        for obj in r.get("Contents", []):
            keys.append(obj["Key"])
        if not r.get("IsTruncated"):
            break
        token = r.get("NextContinuationToken")
    return keys


def read_text(s3: boto3.client, bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="replace")

def parse_exit_code_from_report(md: str) -> int:
    # report contains: "- exit_code: `X`"
    marker = "- exit_code: `"
    i = md.find(marker)
    if i < 0:
        return 0
    j = md.find("`", i + len(marker))
    if j < 0:
        return 0
    try:
        return int(md[i + len(marker): j])
    except:
        return 0

def auto_choose_capital(signals: Dict[str, Any]) -> str:
    if signals.get("needs_many_tests") or signals.get("flaky"):
        return "CVB"
    if signals.get("blocked") or signals.get("needs_architecture"):
        return "BB"
    return "CVB"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch capital missions based on Phase1 results.")
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
        if dry_run:
            repo_url = "dry-run://repo"
        else:
            raise ValueError("REPO_URL environment variable is required")
    month = os.getenv("BUDGET_MONTH", time.strftime("%Y-%m"))
    try:
        time_limit = int(os.getenv("TIME_LIMIT_SEC", "1800"))
    except ValueError:
        raise ValueError("TIME_LIMIT_SEC must be an integer")

    ticket_s3 = f"s3://{bucket}/tickets/{task_id}.md"
    results_prefix = f"s3://{bucket}/results/{task_id}/"

    # 1) gather reports
    if dry_run:
        reports = []
    else:
        keys = list_result_keys(s3, bucket, task_id)
        reports = [k for k in keys if k.endswith(".md")]

    # 2) derive signals from CL report (heuristic)
    signals: Dict[str, Any] = {"blocked": False, "flaky": False, "needs_many_tests": False, "needs_architecture": False}

    cl_reports = [k for k in reports if ".CL-01." in k or k.endswith(".CL-01.md")]
    if cl_reports:
        md = read_text(s3, bucket, cl_reports[-1])
        exit_code = parse_exit_code_from_report(md)
        if exit_code != 0:
            signals["blocked"] = True
            # if tests fail, usually we want CVB first (campaign / reproduce / more tests)
            signals["needs_many_tests"] = True

    # Optional manual overrides via env (JSON)
    try:
        override = json.loads(os.getenv("SIGNALS_OVERRIDE_JSON", "{}") or "{}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in SIGNALS_OVERRIDE_JSON: {e}")
    signals.update(override)

    capital = auto_choose_capital(signals)

    # 3) Dispatch capital
    if capital == "CVB":
        test_cmds_env = os.getenv("TEST_CMDS", "")
        test_cmds = [c.strip() for c in test_cmds_env.split(";") if c.strip()]
        # If none given, re-run basic tests as campaign
        if not test_cmds:
            fallback = os.getenv("CL_TEST_CMD", os.getenv("TEST_CMD", ""))
            if fallback:
                test_cmds = [fallback]

        sorties = int(os.getenv("CVB_SORTIES", "3"))
        branch = os.getenv("CL_BRANCH", f"agent/{task_id}".lower())
        for i in range(1, sorties + 1):
            ship_id = f"CVB-{i:02d}"
            mid = f"M-{task_id}-{ship_id}-{uuid.uuid4().hex[:6]}"
            send(sqs, queue_url, {
                "mission_id": mid,
                "task_id": task_id,
                "ship_id": ship_id,
                "ship_class": "CVB",
                "role": "test_swarm",
                "model": "micro/lite",
                "inputs": {
                    "ticket_s3": ticket_s3,
                    "repo": {"url": repo_url, "branch": branch},
                    "test_cmds": test_cmds,
                },
                "constraints": {"time_limit_sec": time_limit, "max_attempts": 2},
                "budget": {"month": month, "max_cost_usd": 30.0},
                "signals": signals
            }, dry_run)

    else:
        bb_id = f"M-{task_id}-BB-01-{uuid.uuid4().hex[:6]}"
        send(sqs, queue_url, {
            "mission_id": bb_id,
            "task_id": task_id,
            "ship_id": "BB-01",
            "ship_class": "BB",
            "role": "breakthrough",
            "model": "opus",
            "inputs": {"ticket_s3": ticket_s3, "results_prefix": results_prefix},
            "constraints": {"time_limit_sec": time_limit, "max_attempts": 1},
            "budget": {"month": month, "max_cost_usd": 30.0},
            "signals": signals
        }, dry_run)

    # 4) CA commander (always, after capital dispatch)
    ca_id = f"M-{task_id}-CA-01-{uuid.uuid4().hex[:6]}"
    send(sqs, queue_url, {
        "mission_id": ca_id,
        "task_id": task_id,
        "ship_id": "CA-01",
        "ship_class": "CA",
        "role": "command",
        "model": "sonnet",
        "inputs": {"ticket_s3": ticket_s3, "results_prefix": results_prefix},
        "constraints": {"time_limit_sec": 1200, "max_attempts": 1},
        "budget": {"month": month, "max_cost_usd": 30.0},
        "signals": signals
    }, dry_run)

    print("Dispatched capital:", capital)
    print("signals:", signals)
    print("results_prefix:", results_prefix)

if __name__ == "__main__":
    main()

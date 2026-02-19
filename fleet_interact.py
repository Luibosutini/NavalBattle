#!/usr/bin/env python3
"""
Fleet Interact - CLI tool for human-in-the-loop dialogue
Handles NEED_INPUT and NEED_APPROVAL states
"""
import os
import sys
import json
import time
import uuid
import boto3
from datetime import datetime
try:
    from fleet_local import log_comm_local
except Exception:
    log_comm_local = None

REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
STATE_TABLE = os.getenv("FLEET_STATE_TABLE", "fleet-mission-state")
BUCKET = os.getenv("FLEET_S3_BUCKET", "")
SQS_NAME = os.getenv("FLEET_SQS_NAME", "fleet-missions")

dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)


def _format_updated_at(value: str) -> str:
    if not value:
        return ""
    try:
        ts = int(float(value))
        return datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except ValueError:
        return value


def scan_pending_missions():
    """Scan for missions needing human input/approval"""
    try:
        resp = dynamodb.scan(
            TableName=STATE_TABLE,
            FilterExpression="#s IN (:input, :approval)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":input": {"S": "NEED_INPUT"},
                ":approval": {"S": "NEED_APPROVAL"}
            }
        )
        items = resp.get("Items", [])
        missions = []
        for item in items:
            updated_at = item.get("updated_at", {}).get("N") or item.get("updated_at", {}).get("S", "")
            missions.append({
                "mission_id": item.get("mission_id", {}).get("S", ""),
                "status": item.get("status", {}).get("S", ""),
                "ship_class": item.get("ship_class", {}).get("S", ""),
                "task_id": item.get("task_id", {}).get("S", ""),
                "updated_at": _format_updated_at(updated_at)
            })
        return missions
    except Exception as e:
        print(f"Error scanning missions: {e}")
        return []


def get_mission_payload(mission_id):
    """Retrieve original mission payload from S3 (orders/payload.json)."""
    try:
        key = f"missions/{mission_id}/orders/payload.json"
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Error reading mission payload: {e}")
        return {}


def list_comms(mission_id):
    """List communication events"""
    try:
        prefix = f"missions/{mission_id}/comms/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        comms = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            data = s3.get_object(Bucket=BUCKET, Key=key)
            comms.append(json.loads(data["Body"].read().decode("utf-8")))
        comms.sort(key=lambda x: x.get("ts", 0))
        return comms
    except Exception as e:
        print(f"Error listing comms: {e}")
        return []


def record_comm(mission_id, from_, to, comm_type, content):
    ts = int(time.time())
    comm_id = uuid.uuid4().hex[:8]
    comm = {
        "ts": ts,
        "comm_id": comm_id,
        "from": from_,
        "to": to,
        "type": comm_type,
        "content": content
    }
    key = f"missions/{mission_id}/comms/{ts}-{comm_id}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(comm, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    if log_comm_local:
        try:
            meta = dynamodb.get_item(
                TableName=STATE_TABLE,
                Key={"mission_id": {"S": mission_id}}
            ).get("Item", {})
            task_id = meta.get("task_id", {}).get("S")
            ship_class = meta.get("ship_class", {}).get("S")
            ship_id = meta.get("ship_id", {}).get("S")
            log_comm_local(
                mission_id=mission_id,
                task_id=task_id,
                ship_class=ship_class,
                ship_id=ship_id,
                from_role=from_,
                to_role=to,
                comm_type=comm_type,
                content=content,
                source="fleet_interact",
                model_id="",
            )
        except Exception:
            pass


def resume_mission(mission_id, status, response_text):
    """Resume mission by updating DynamoDB and re-enqueuing"""
    try:
        # Get current mission state
        resp = dynamodb.get_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}}
        )
        if "Item" not in resp:
            print(f"Mission {mission_id} not found")
            return False

        item = resp["Item"]
        current_status = item.get("status", {}).get("S", "")

        if current_status != status:
            print(f"Status mismatch: expected {status}, got {current_status}")
            return False

        # Determine new status
        if status == "NEED_INPUT":
            new_status = "RUNNING"
        elif status == "NEED_APPROVAL":
            if response_text.strip().lower() in ["approve", "approved", "yes", "y"]:
                new_status = "RUNNING"
            else:
                new_status = "FAILED"
        else:
            print(f"Invalid status for resume: {status}")
            return False

        # Record comms
        try:
            if status == "NEED_INPUT":
                record_comm(mission_id, "user", "system", "answer", response_text)
            elif status == "NEED_APPROVAL":
                record_comm(mission_id, "user", "system", "approval_response", response_text)
        except Exception as e:
            print(f"Warning: failed to record comm: {e}")

        # Update DynamoDB
        now = int(time.time())
        expr_names = {"#s": "status"}
        expr_values = {
            ":new_status": {"S": new_status},
            ":now": {"N": str(now)},
            ":resp": {"S": response_text},
            ":from": {"S": status},
        }
        update_expr = "SET #s=:new_status, updated_at=:now, human_response=:resp"
        if new_status == "FAILED":
            expr_values[":fr"] = {"S": f"Approval denied: {response_text}"}
            update_expr += ", fail_reason=:fr"
        update_expr += " REMOVE needs_input, needs_approval, owner, lock_until"

        dynamodb.update_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ConditionExpression="#s=:from",
        )

        # Re-enqueue if RUNNING
        if new_status == "RUNNING":
            queue_url_resp = sqs.get_queue_url(QueueName=SQS_NAME)
            queue_url = queue_url_resp["QueueUrl"]

            payload = get_mission_payload(mission_id)
            if not payload:
                payload = {
                    "mission_id": mission_id,
                    "ship_class": item.get("ship_class", {}).get("S", ""),
                    "task_id": item.get("task_id", {}).get("S", ""),
                    "ship_id": item.get("ship_id", {}).get("S", ""),
                }
            payload["resume"] = True

            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(payload, ensure_ascii=False)
            )
            print(f"Mission {mission_id} resumed and re-enqueued")
        else:
            print(f"Mission {mission_id} marked as {new_status}")

        return True

    except Exception as e:
        print(f"Error resuming mission: {e}")
        return False


def main():
    if not BUCKET:
        print("Error: FLEET_S3_BUCKET not set")
        sys.exit(1)

    print("=== Fleet Interact - Pending Missions ===\n")

    missions = scan_pending_missions()

    if not missions:
        print("No missions pending human input/approval")
        return

    print(f"Found {len(missions)} pending mission(s):\n")

    for i, m in enumerate(missions, 1):
        print(f"{i}. [{m['status']}] {m['mission_id']}")
        print(f"   Ship: {m['ship_class']} | Task: {m['task_id']}")
        print(f"   Updated: {m['updated_at']}\n")

    # Select mission
    try:
        choice = input("Select mission number (or 'q' to quit): ").strip()
        if choice.lower() == 'q':
            return

        idx = int(choice) - 1
        if idx < 0 or idx >= len(missions):
            print("Invalid selection")
            return

        selected = missions[idx]
        mission_id = selected["mission_id"]
        status = selected["status"]

        print(f"\n=== Mission: {mission_id} ===")
        print(f"Status: {status}\n")

        # Show mission details
        mission_json = get_mission_payload(mission_id)
        if mission_json:
            print("Orders:")
            inputs = mission_json.get("inputs", {})
            ticket = inputs.get("ticket_s3") or mission_json.get("ticket_s3") or "N/A"
            print(ticket)
            print()

        # Show recent comms
        comms = list_comms(mission_id)
        if comms:
            print("Recent Communications:")
            for comm in comms[-3:]:  # Last 3 comms
                print(f"  [{comm.get('from', 'UNKNOWN')}] {comm.get('content', '')[:80]}")
            print()

        # Get human response
        if status == "NEED_INPUT":
            print("Enter your response (multi-line, Ctrl+D/Ctrl+Z to finish):")
            lines = []
            try:
                while True:
                    line = input()
                    lines.append(line)
            except EOFError:
                pass
            response = "\n".join(lines).strip()
        elif status == "NEED_APPROVAL":
            response = input("Approve? (yes/no): ").strip()
        else:
            print(f"Unknown status: {status}")
            return

        if not response:
            print("No response provided, aborting")
            return

        # Resume mission
        print(f"\nResuming mission with response...")
        resume_mission(mission_id, status, response)

    except (ValueError, KeyboardInterrupt):
        print("\nAborted")
        return


if __name__ == "__main__":
    main()

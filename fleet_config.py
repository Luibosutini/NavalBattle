from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_REGION = "ap-northeast-1"


def _iter_resources(stmt: Dict[str, object]) -> List[str]:
    res = stmt.get("Resource", [])
    if isinstance(res, str):
        return [res]
    if isinstance(res, list):
        return [r for r in res if isinstance(r, str)]
    return []


def _parse_arn(arn: str) -> Optional[Dict[str, str]]:
    if not arn.startswith("arn:"):
        return None
    parts = arn.split(":", 5)
    if len(parts) < 6:
        return None
    return {"service": parts[2], "region": parts[3], "resource": parts[5]}


def _load_policy_defaults(policy_path: Path) -> Dict[str, Optional[str]]:
    if not policy_path.exists():
        return {}
    try:
        data = json.loads(policy_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    statements = data.get("Statement", [])
    if not isinstance(statements, list):
        return {}

    sqs_name: Optional[str] = None
    bucket: Optional[str] = None
    region: Optional[str] = None
    ddb_tables: List[str] = []

    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        for res in _iter_resources(stmt):
            parsed = _parse_arn(res)
            if not parsed:
                continue
            service = parsed["service"]
            resource = parsed["resource"]
            if parsed["region"] and not region:
                region = parsed["region"]

            if service == "sqs" and not sqs_name:
                sqs_name = resource.split("/")[-1]
            elif service == "dynamodb" and resource.startswith("table/"):
                ddb_tables.append(resource.split("/", 1)[1])
            elif service == "s3" and not bucket:
                bucket = resource.split("/", 1)[0]

    state_table: Optional[str] = None
    budget_table: Optional[str] = None
    for name in ddb_tables:
        lname = name.lower()
        if not budget_table and "budget" in lname:
            budget_table = name
        if not state_table and ("state" in lname or "mission" in lname):
            state_table = name

    return {
        "region": region,
        "sqs_name": sqs_name,
        "state_table": state_table,
        "budget_table": budget_table,
        "bucket": bucket,
    }


def resolve_fleet_config() -> Dict[str, Optional[str]]:
    default_policy = Path(__file__).with_name("policies").joinpath("FleetNodePolicy.json")
    if not default_policy.exists():
        default_policy = Path(__file__).with_name("FleetNodePolicy.json")
    policy_path = Path(os.getenv("FLEET_POLICY_PATH", str(default_policy)))
    defaults = _load_policy_defaults(policy_path)

    return {
        "region": os.getenv("FLEET_REGION") or defaults.get("region") or DEFAULT_REGION,
        "sqs_name": os.getenv("FLEET_SQS_NAME") or defaults.get("sqs_name"),
        "state_table": os.getenv("FLEET_STATE_TABLE") or defaults.get("state_table"),
        "budget_table": os.getenv("FLEET_BUDGET_TABLE") or defaults.get("budget_table"),
        "bucket": os.getenv("FLEET_S3_BUCKET") or defaults.get("bucket"),
    }

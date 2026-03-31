from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from temporalio import activity
from temporalio.exceptions import ApplicationError

from naval.artifacts import validate_result_artifact, validate_summary_markdown


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _required_meta(payload: dict[str, Any]) -> tuple[str, str, str, str]:
    mission_id = str(payload.get("mission_id", "")).strip()
    task_id = str(payload.get("task_id", "")).strip()
    ship = str(payload.get("ship", "CVL")).strip() or "CVL"
    trace_id = str(payload.get("trace_id", mission_id)).strip() or mission_id
    if not mission_id:
        raise ValueError("mission_id is required")
    if not task_id:
        task_id = f"T-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    return mission_id, task_id, ship, trace_id


def _s3_client() -> tuple[str, Any]:
    """Get S3 client and bucket name. Returns empty bucket string if not configured."""
    bucket = os.getenv("FLEET_S3_BUCKET", "").strip()
    region = os.getenv("FLEET_REGION", "ap-northeast-1")
    s3 = boto3.client("s3", region_name=region)
    return bucket, s3


def _ddb_client() -> Any:
    region = os.getenv("FLEET_REGION", "ap-northeast-1")
    return boto3.client("dynamodb", region_name=region)


RETRYABLE_BEDROCK_CODES = {
    "ThrottlingException",
    "TooManyRequestsException",
    "ServiceUnavailableException",
    "InternalServerException",
    "ModelNotReadyException",
}
PERMISSION_BEDROCK_CODES = {
    "AccessDeniedException",
    "UnauthorizedException",
    "UnrecognizedClientException",
}
VALIDATION_BEDROCK_CODES = {
    "ValidationException",
    "ResourceNotFoundException",
}


def _classify_bedrock_error(exc: Exception) -> tuple[str, str, bool]:
    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {})
        code = str(err.get("Code", "ClientError"))
        if code in RETRYABLE_BEDROCK_CODES:
            return "throttle", code, False
        if code in PERMISSION_BEDROCK_CODES:
            return "permission", code, True
        if code in VALIDATION_BEDROCK_CODES:
            return "validation", code, True
        return "client", code, False

    if isinstance(exc, (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError)):
        return "network", exc.__class__.__name__, False

    if isinstance(exc, BotoCoreError):
        return "network", exc.__class__.__name__, False

    return "unknown", exc.__class__.__name__, False


def _default_ship_payload(ship: str, payload: dict[str, Any], now_iso: str) -> dict[str, Any]:
    ticket = str(payload.get("ticket", "")).strip()
    if ship == "CVL":
        return {
            "requirements": [ticket] if ticket else [],
            "constraints": list(payload.get("constraints", [])),
            "questions": list(payload.get("questions", [])),
            "risks": list(payload.get("risks", [])),
            "message": str(payload.get("message", "Hello from MissionWorkflow")),
            "generated_at": now_iso,
            "doctrine": payload.get("doctrine", "standard_full"),
            "human_input": payload.get("human_input", ""),
            "approval_note": payload.get("approval_note", ""),
            "approval_decision": payload.get("approval_decision"),
        }
    if ship == "DD":
        return {
            "plan": list(payload.get("plan", [])),
            "files_changed": list(payload.get("files_changed", [])),
            "commands": list(payload.get("commands", [])),
            "diff_summary": str(payload.get("diff_summary", "")),
            "generated_at": now_iso,
        }
    if ship == "CL":
        return {
            "test_plan": list(payload.get("test_plan", [])),
            "test_results": list(payload.get("test_results", [])),
            "repro_steps": list(payload.get("repro_steps", [])),
            "generated_at": now_iso,
        }
    return {
        "message": str(payload.get("message", "Hello from MissionWorkflow")),
        "generated_at": now_iso,
    }


def _attr_to_float(item: dict[str, Any], key: str, default: float = 0.0) -> float:
    raw = item.get(key, {})
    if isinstance(raw, dict):
        if "N" in raw:
            try:
                return float(raw["N"])
            except Exception:
                return default
        if "S" in raw:
            try:
                return float(raw["S"])
            except Exception:
                return default
    return default


def _attr_to_bool(item: dict[str, Any], key: str, default: bool = False) -> bool:
    raw = item.get(key, {})
    if isinstance(raw, dict):
        if "BOOL" in raw:
            return bool(raw["BOOL"])
        if "N" in raw:
            try:
                return float(raw["N"]) != 0
            except Exception:
                return default
        if "S" in raw:
            return str(raw["S"]).strip().lower() in {"1", "true", "yes"}
    return default


def _list_s3_keys(s3: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = str(obj.get("Key", ""))
            if key and not key.endswith("/"):
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        token = str(resp.get("NextContinuationToken", "")).strip() or None
    return keys


def _merge_context_values(base: Any, patch: Any) -> Any:
    if isinstance(base, dict) and isinstance(patch, dict):
        merged = dict(base)
        for k, v in patch.items():
            if k in merged:
                merged[k] = _merge_context_values(merged[k], v)
            else:
                merged[k] = v
        return merged

    if isinstance(base, list) and isinstance(patch, list):
        out = list(base)
        seen = {json.dumps(v, ensure_ascii=False, sort_keys=True) for v in out}
        for v in patch:
            sig = json.dumps(v, ensure_ascii=False, sort_keys=True)
            if sig not in seen:
                out.append(v)
                seen.add(sig)
        return out

    return patch


@activity.defn
async def write_event_log_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    event_type = str(payload.get("event_type", "WORKFLOW_EVENT")).strip()
    detail = payload.get("detail", {})
    ts = _now_ts()
    entry = {
        "ts": ts,
        "iso_time": _now_iso(),
        "event_id": uuid.uuid4().hex[:12],
        "event_type": event_type,
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "detail": detail,
    }

    bucket, s3 = _s3_client()
    if not bucket:
        return {"status": "SKIPPED", "reason": "FLEET_S3_BUCKET is not set", "entry": entry}

    key = f"missions/{mission_id}/events/{ts}-{entry['event_id']}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return {"status": "OK", "s3_uri": f"s3://{bucket}/{key}"}


@activity.defn
async def write_comms_log_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    ts = _now_ts()
    comm_id = uuid.uuid4().hex[:12]
    entry = {
        "ts": ts,
        "iso_time": _now_iso(),
        "comm_id": comm_id,
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "from": str(payload.get("from_role", "system")),
        "to": str(payload.get("to_role", "user")),
        "type": str(payload.get("comm_type", "notice")),
        "content": str(payload.get("content", "")),
    }

    bucket, s3 = _s3_client()
    if not bucket:
        return {"status": "SKIPPED", "reason": "FLEET_S3_BUCKET is not set", "entry": entry}

    key = f"missions/{mission_id}/comms/{ts}-{comm_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return {"status": "OK", "s3_uri": f"s3://{bucket}/{key}"}


@activity.defn
async def budget_check_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    limit = payload.get("budget_limit_usd")
    estimated = payload.get("estimated_cost_usd", 0.0)
    try:
        estimated_val = float(estimated)
    except Exception:
        estimated_val = 0.0

    if limit is None:
        return {
            "allowed": True,
            "reason": "no_budget_limit",
            "mission_id": mission_id,
            "task_id": task_id,
            "ship": ship,
            "trace_id": trace_id,
            "estimated_cost_usd": estimated_val,
        }

    try:
        limit_val = float(limit)
    except Exception:
        limit_val = 0.0

    allowed = estimated_val <= limit_val
    return {
        "allowed": allowed,
        "reason": "within_limit" if allowed else "budget_exceeded",
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "estimated_cost_usd": estimated_val,
        "budget_limit_usd": limit_val,
    }


@activity.defn
async def budget_guard_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    phase = str(payload.get("phase", "pre")).strip().lower() or "pre"
    if phase not in {"pre", "post"}:
        phase = "pre"

    limit = payload.get("budget_limit_usd")
    if limit is None:
        return {
            "status": "SKIPPED",
            "allowed": True,
            "reason": "no_budget_limit",
            "phase": phase,
            "mission_id": mission_id,
            "task_id": task_id,
            "ship": ship,
            "trace_id": trace_id,
            "total_spent_usd": float(payload.get("spent_usd", 0.0) or 0.0),
            "call_cost_usd": float(payload.get("call_cost_usd", 0.0) or 0.0),
        }

    try:
        limit_val = float(limit)
    except Exception:
        limit_val = 0.0
    try:
        call_cost = float(payload.get("call_cost_usd", 0.0) or 0.0)
    except Exception:
        call_cost = 0.0
    try:
        spent = float(payload.get("spent_usd", 0.0) or 0.0)
    except Exception:
        spent = 0.0

    source = "memory"
    circuit_open = False
    table = os.getenv("NAVAL_BUDGET_GUARD_TABLE", "").strip() or os.getenv("FLEET_BUDGET_TABLE", "").strip()
    if table:
        try:
            ddb = _ddb_client()
            got = ddb.get_item(TableName=table, Key={"mission_id": {"S": mission_id}})
            item = got.get("Item", {})
            if isinstance(item, dict):
                spent = _attr_to_float(item, "spent_usd", spent)
                circuit_open = _attr_to_bool(item, "circuit_open", False)
            source = "ddb"
        except Exception:
            source = "memory"

    if phase == "pre":
        allowed = (not circuit_open) and (spent < limit_val)
        reason = "within_limit" if allowed else ("circuit_open" if circuit_open else "budget_exceeded")
        return {
            "status": "OK",
            "allowed": allowed,
            "reason": reason,
            "phase": phase,
            "source": source,
            "mission_id": mission_id,
            "task_id": task_id,
            "ship": ship,
            "trace_id": trace_id,
            "total_spent_usd": spent,
            "call_cost_usd": call_cost,
            "budget_limit_usd": limit_val,
            "circuit_open": circuit_open,
        }

    total = spent + call_cost
    circuit_open = total > limit_val
    allowed = not circuit_open
    if table and source == "ddb":
        try:
            ddb = _ddb_client()
            ddb.put_item(
                TableName=table,
                Item={
                    "mission_id": {"S": mission_id},
                    "spent_usd": {"N": f"{total:.10f}"},
                    "budget_limit_usd": {"N": f"{limit_val:.10f}"},
                    "circuit_open": {"BOOL": circuit_open},
                    "updated_at": {"N": str(_now_ts())},
                    "trace_id": {"S": trace_id},
                    "task_id": {"S": task_id},
                    "ship_class": {"S": ship},
                },
            )
        except Exception:
            source = "memory"

    return {
        "status": "OK",
        "allowed": allowed,
        "reason": "within_limit" if allowed else "budget_exceeded",
        "phase": phase,
        "source": source,
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "total_spent_usd": total,
        "call_cost_usd": call_cost,
        "budget_limit_usd": limit_val,
        "circuit_open": circuit_open,
    }


@activity.defn
async def artifact_write_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    now_iso = _now_iso()
    ship_payload = payload.get("ship_payload")
    if not isinstance(ship_payload, dict):
        ship_payload = _default_ship_payload(ship, payload, now_iso)

    result = {
        "schema_version": "1.0",
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "status": str(payload.get("result_status", "OK")),
        "confidence": float(payload.get("confidence", 1.0)),
        "trace_id": trace_id,
        "inputs": payload.get("inputs", [{"type": "ticket", "value": payload.get("ticket", "")}]),
        "outputs": payload.get("outputs", [{"type": "message", "value": "hello temporal"}]),
        "next_actions": payload.get(
            "next_actions",
            [{"action": "advance", "reason": "activity pipeline complete"}],
        ),
        "payload": ship_payload,
    }

    summary = payload.get("summary_md")
    if not summary:
        summary = (
            "# Summary\n\n"
            "## 決定事項\n"
            "- Temporal Workflow が artifact を生成しました。\n\n"
            "## 未決事項\n"
            "- shipごとの詳細ロジックは段階的に拡張します。\n\n"
            "## 次アクション\n"
            "- 次フェーズで艦種別の出力を追加します。\n"
        )

    try:
        result = validate_result_artifact(result)
        summary = validate_summary_markdown(str(summary))
    except Exception as exc:
        raise ApplicationError(
            f"artifact schema validation failed: {exc}",
            type="ARTIFACT_SCHEMA_INVALID",
            non_retryable=True,
        ) from exc

    bucket, s3 = _s3_client()
    if not bucket:
        return {
            "status": "SKIPPED",
            "reason": "FLEET_S3_BUCKET is not set",
            "result": result,
            "summary": summary,
        }

    base = f"missions/{mission_id}/artifacts/{task_id}/{ship}"
    result_key = f"{base}/result.json"
    summary_key = f"{base}/summary.md"

    s3.put_object(
        Bucket=bucket,
        Key=result_key,
        Body=json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    s3.put_object(
        Bucket=bucket,
        Key=summary_key,
        Body=str(summary).encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
    )
    return {
        "status": "OK",
        "result_s3": f"s3://{bucket}/{result_key}",
        "summary_s3": f"s3://{bucket}/{summary_key}",
    }


@activity.defn
async def index_update_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    table = os.getenv("NAVAL_INDEX_TABLE", "").strip() or os.getenv("FLEET_STATE_TABLE", "").strip()
    if not table:
        return {"status": "SKIPPED", "reason": "NAVAL_INDEX_TABLE/FLEET_STATE_TABLE is not set"}

    state = str(payload.get("user_state", "RUNNING"))
    reason = str(payload.get("needs_reason", ""))
    next_action = str(payload.get("next_action", ""))
    ts = _now_ts()
    ddb = _ddb_client()
    ddb.put_item(
        TableName=table,
        Item={
            "mission_id": {"S": mission_id},
            "task_id": {"S": task_id},
            "ship_class": {"S": ship},
            "status": {"S": state},
            "trace_id": {"S": trace_id},
            "updated_at": {"N": str(ts)},
            "needs_reason": {"S": reason},
            "next_action": {"S": next_action},
        },
    )
    return {"status": "OK", "table": table, "mission_id": mission_id}


@activity.defn
async def context_append_patch_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    patch_payload = payload.get("context_patch")
    if not isinstance(patch_payload, dict):
        patch_payload = {}

    ts = _now_ts()
    patch_id = uuid.uuid4().hex[:12]
    entry = {
        "schema_version": "1.0",
        "ts": ts,
        "iso_time": _now_iso(),
        "patch_id": patch_id,
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "base_context_ref": str(payload.get("context_ref", "")),
        "context_patch": patch_payload,
    }

    bucket, s3 = _s3_client()
    if not bucket:
        return {"status": "SKIPPED", "reason": "FLEET_S3_BUCKET is not set", "patch": entry}

    key = f"missions/{mission_id}/context/patches/{ts}-{patch_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return {
        "status": "OK",
        "context_patch_ref": f"s3://{bucket}/{key}",
        "patch_id": patch_id,
    }


@activity.defn
async def context_build_latest_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    bucket, s3 = _s3_client()
    if not bucket:
        return {"status": "SKIPPED", "reason": "FLEET_S3_BUCKET is not set", "context": {}}

    prefix = f"missions/{mission_id}/context/patches/"
    keys = _list_s3_keys(s3, bucket, prefix)
    patch_docs: list[tuple[int, str, str, dict[str, Any]]] = []
    for key in keys:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        try:
            patch_doc = json.loads(raw.decode("utf-8"))
        except Exception:
            continue
        ts_val = int(patch_doc.get("ts", 0))
        iso_val = str(patch_doc.get("iso_time", ""))
        patch_docs.append((ts_val, iso_val, key, patch_doc))
    patch_docs.sort(key=lambda x: (x[0], x[1], x[2]))

    latest: dict[str, Any] = {}
    lineage: list[dict[str, Any]] = []
    for _, _, key, patch_doc in patch_docs:
        patch_payload = patch_doc.get("context_patch", {})
        if not isinstance(patch_payload, dict):
            patch_payload = {}
        latest = _merge_context_values(latest, patch_payload)
        lineage.append(
            {
                "patch_id": str(patch_doc.get("patch_id", "")),
                "ts": int(patch_doc.get("ts", 0)),
                "s3_key": key,
                "ship": str(patch_doc.get("ship", "")),
            }
        )

    doc = {
        "schema_version": "1.0",
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "updated_at": _now_iso(),
        "patch_count": len(lineage),
        "lineage": lineage,
        "context": latest,
    }

    key_latest = f"missions/{mission_id}/context/latest.json"
    s3.put_object(
        Bucket=bucket,
        Key=key_latest,
        Body=json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return {
        "status": "OK",
        "context_ref": f"s3://{bucket}/{key_latest}",
        "patch_count": len(lineage),
        "context": latest,
    }


@activity.defn
async def bedrock_invoke_activity(payload: dict[str, Any]) -> dict[str, Any]:
    mission_id, task_id, ship, trace_id = _required_meta(payload)
    region = os.getenv("FLEET_REGION", "ap-northeast-1")
    model_id = str(payload.get("model_id") or os.getenv("BEDROCK_SONNET_MODEL_ID", "")).strip()
    prompt = str(payload.get("prompt", "")).strip()
    system_prompt = str(payload.get("system_prompt", "You are a helpful assistant.")).strip()
    max_tokens = int(payload.get("max_tokens", 256))
    temperature = float(payload.get("temperature", 0.2))

    if not model_id:
        return {
            "status": "SKIPPED",
            "reason": "model_id_not_set",
            "mission_id": mission_id,
            "task_id": task_id,
            "ship": ship,
            "trace_id": trace_id,
            "output_text": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "estimated_cost_usd": 0.0,
        }

    bedrock = boto3.client("bedrock-runtime", region_name=region)
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system_prompt,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        resp = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body).encode("utf-8"),
        )
        data = json.loads(resp["body"].read())
    except Exception as exc:
        kind, code, non_retryable = _classify_bedrock_error(exc)
        raise ApplicationError(
            f"bedrock invoke failed kind={kind} code={code} mission_id={mission_id} task_id={task_id}",
            type=f"BEDROCK_{kind.upper()}",
            non_retryable=non_retryable,
        ) from exc

    parts = data.get("content", []) if isinstance(data, dict) else []
    output_text = "".join(
        [str(p.get("text", "")) for p in parts if isinstance(p, dict) and p.get("type") == "text"]
    ).strip()

    usage = data.get("usage", {}) if isinstance(data, dict) else {}
    in_tokens = int(usage.get("input_tokens", max(1, len(prompt) // 4)))
    out_tokens = int(usage.get("output_tokens", max(1, len(output_text) // 4)))

    in_rate = float(os.getenv("BEDROCK_INPUT_USD_PER_1K", "0"))
    out_rate = float(os.getenv("BEDROCK_OUTPUT_USD_PER_1K", "0"))
    estimated_cost = (in_tokens / 1000.0) * in_rate + (out_tokens / 1000.0) * out_rate

    return {
        "status": "OK",
        "mission_id": mission_id,
        "task_id": task_id,
        "ship": ship,
        "trace_id": trace_id,
        "model_id": model_id,
        "output_text": output_text,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "estimated_cost_usd": estimated_cost,
    }


# Backward-compatible alias for T-VNEXT-004 code path.
@activity.defn(name="write_hello_artifact")
async def write_hello_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    return await artifact_write_activity(payload)

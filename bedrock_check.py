#!/usr/bin/env python3
"""
Bedrock integration check.
 - Verifies required env vars
 - Optionally invokes Sonnet/Opus with a tiny request to confirm permissions
"""
import json
import os
import sys
from typing import Dict, Tuple

import boto3
from botocore.exceptions import ClientError


REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
SONNET_ID = os.getenv("BEDROCK_SONNET_MODEL_ID", "")
OPUS_ID = os.getenv("BEDROCK_OPUS_MODEL_ID", "")
MICRO_ID = os.getenv("BEDROCK_MICRO_MODEL_ID", "")
LITE_ID = os.getenv("BEDROCK_LITE_MODEL_ID", "")


def log_bedrock_error(service: str, operation: str, model_id: str, exc: Exception) -> None:
    print(
        f"ERROR region={REGION} service={service} operation={operation} modelId={model_id} error={exc}"
    )


def is_inference_profile_arn(model_id: str) -> bool:
    return bool(model_id) and model_id.startswith("arn:aws:bedrock:") and ":inference-profile/" in model_id


def print_env() -> None:
    print("Bedrock env check:")
    print(f"  FLEET_REGION: {REGION}")
    print(f"  BEDROCK_SONNET_MODEL_ID: {'SET' if SONNET_ID else 'MISSING'}")
    print(f"  BEDROCK_OPUS_MODEL_ID:   {'SET' if OPUS_ID else 'MISSING'}")
    print(f"  BEDROCK_MICRO_MODEL_ID:  {'SET' if MICRO_ID else 'MISSING'}")
    print(f"  BEDROCK_LITE_MODEL_ID:   {'SET' if LITE_ID else 'MISSING'}")
    print("")


def _anthropic_payload(system: str, user: str, max_tokens: int = 32) -> Dict:
    return {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
        "temperature": 0.0,
    }


def _validate_profile(model_id: str) -> Tuple[bool, str]:
    if not model_id:
        return False, "MISSING"
    if not is_inference_profile_arn(model_id):
        return False, f"INVALID (not inference-profile ARN): {model_id}"
    bedrock = boto3.client("bedrock", region_name=REGION)
    try:
        bedrock.get_inference_profile(inferenceProfileIdentifier=model_id)
        return True, "OK"
    except ClientError as e:
        log_bedrock_error("bedrock", "GetInferenceProfile", model_id, e)
        return False, f"{e.response.get('Error', {}).get('Code')}: {e}"
    except Exception as e:
        log_bedrock_error("bedrock", "GetInferenceProfile", model_id, e)
        return False, str(e)


def _invoke(model_id: str) -> Tuple[bool, str]:
    bedrock = boto3.client("bedrock-runtime", region_name=REGION)
    body = _anthropic_payload("You are a ping responder.", "Return the word OK.")
    try:
        resp = bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body).encode("utf-8"),
        )
        data = json.loads(resp["body"].read())
        parts = data.get("content", [])
        text = "".join([p.get("text", "") for p in parts if p.get("type") == "text"]).strip()
        return True, text or "(no text)"
    except ClientError as e:
        log_bedrock_error("bedrock-runtime", "InvokeModel", model_id, e)
        return False, f"{e.response.get('Error', {}).get('Code')}: {e}"
    except Exception as e:
        log_bedrock_error("bedrock-runtime", "InvokeModel", model_id, e)
        return False, str(e)


def main() -> None:
    invoke = "--invoke" in sys.argv
    print_env()

    if not invoke:
        print("Dry-run only. Use --invoke to test Bedrock permissions.")
        return

    for name, model_id in [
        ("SONNET", SONNET_ID),
        ("OPUS", OPUS_ID),
        ("MICRO", MICRO_ID),
        ("LITE", LITE_ID),
    ]:
        ok, msg = _validate_profile(model_id)
        status = "OK" if ok else "FAIL"
        print(f"{name} PROFILE: {status} - {msg}")

    for name, model_id in [("SONNET", SONNET_ID), ("OPUS", OPUS_ID)]:
        if not model_id:
            print(f"{name}: SKIP (model_id not set)")
            continue
        if not is_inference_profile_arn(model_id):
            print(f"{name}: SKIP (model_id not inference-profile ARN)")
            continue
        ok, msg = _invoke(model_id)
        status = "OK" if ok else "FAIL"
        print(f"{name}: {status} - {msg}")


if __name__ == "__main__":
    main()

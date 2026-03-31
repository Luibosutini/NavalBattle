from __future__ import annotations

import asyncio
import io
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

from botocore.exceptions import ClientError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# Ensure repository root is importable when running this file directly.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.temporal.activities import (
    artifact_write_activity,
    bedrock_invoke_activity,
    budget_check_activity,
    budget_guard_activity,
    context_append_patch_activity,
    context_build_latest_activity,
    index_update_activity,
    write_comms_log_activity,
    write_event_log_activity,
    write_hello_artifact,
)
from naval.temporal.workflows import MissionWorkflow


async def _wait_state(handle: Any, expected: str, tries: int = 120) -> dict[str, Any]:
    for _ in range(tries):
        q = await handle.query("get_status")
        if q.get("user_state") == expected:
            return q
        await asyncio.sleep(0.05)
    raise RuntimeError(f"timeout waiting for state={expected}")


async def _run_with_worker(client: Any, task_queue: str, coro: Any) -> None:
    # max_cached_workflows=0 avoids sticky-cache behavior in restart tests.
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MissionWorkflow],
        activities=[
            write_hello_artifact,
            write_event_log_activity,
            write_comms_log_activity,
            budget_check_activity,
            budget_guard_activity,
            bedrock_invoke_activity,
            artifact_write_activity,
            index_update_activity,
            context_append_patch_activity,
            context_build_latest_activity,
        ],
        max_cached_workflows=0,
    ):
        await coro


async def test_approval_yes(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q1"
    wid = "mission-MS-FUNC-APPROVE-YES"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-APPROVE-YES",
                "task_id": "T-FUNC-1",
                "ticket": "test",
                "hitl_mode": "approval",
                "hitl_timeout_hours": 72,
                "hitl_timeout_action": "FAILED",
            },
            id=wid,
            task_queue=task_queue,
        )
        q = await _wait_state(handle, "NEED_APPROVAL")
        assert str(q.get("next_action", "")).startswith("naval approve"), q
        await handle.signal(
            "approve",
            {"mission_id": "MS-FUNC-APPROVE-YES", "decision": True, "signal_id": "func-s1"},
        )
        result = await handle.result()
        assert result.get("status") == "DONE", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_approval_no(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q2"
    wid = "mission-MS-FUNC-APPROVE-NO"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-APPROVE-NO",
                "task_id": "T-FUNC-2",
                "ticket": "test",
                "hitl_mode": "approval",
                "hitl_timeout_hours": 72,
                "hitl_timeout_action": "FAILED",
            },
            id=wid,
            task_queue=task_queue,
        )
        await _wait_state(handle, "NEED_APPROVAL")
        await handle.signal(
            "approve",
            {
                "mission_id": "MS-FUNC-APPROVE-NO",
                "decision": False,
                "signal_id": "func-s2",
                "note": "deny",
            },
        )
        result = await handle.result()
        assert result.get("status") == "FAILED", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_input_mode(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q3"
    wid = "mission-MS-FUNC-INPUT"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-INPUT",
                "task_id": "T-FUNC-3",
                "ticket": "test",
                "hitl_mode": "input",
                "hitl_timeout_hours": 72,
                "hitl_timeout_action": "FAILED",
            },
            id=wid,
            task_queue=task_queue,
        )
        q = await _wait_state(handle, "NEED_INPUT")
        assert str(q.get("next_action", "")).startswith("naval input"), q
        await handle.signal(
            "provide_input",
            {"mission_id": "MS-FUNC-INPUT", "message": "ok", "signal_id": "func-s3"},
        )
        result = await handle.result()
        assert result.get("status") == "DONE", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_timeout_cancelled(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q4"
    wid = "mission-MS-FUNC-TIMEOUT"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-TIMEOUT",
                "task_id": "T-FUNC-4",
                "ticket": "test",
                "hitl_mode": "approval",
                "hitl_timeout_hours": 1,
                "hitl_timeout_action": "CANCELLED",
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "CANCELLED", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_restart_resume_approval(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q5"
    wid = "mission-MS-FUNC-RESTART"

    async def _run_before_restart() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-RESTART",
                "task_id": "T-FUNC-5",
                "ticket": "test",
                "hitl_mode": "approval",
                "hitl_timeout_hours": 72,
                "hitl_timeout_action": "FAILED",
            },
            id=wid,
            task_queue=task_queue,
        )
        await _wait_state(handle, "NEED_APPROVAL")

    await _run_with_worker(env.client, task_queue, _run_before_restart())

    async def _run_after_restart() -> None:
        handle = env.client.get_workflow_handle(wid)
        await handle.signal(
            "approve",
            {"mission_id": "MS-FUNC-RESTART", "decision": True, "signal_id": "func-s5"},
        )
        result = await handle.result()
        assert result.get("status") == "DONE", result

    await _run_with_worker(env.client, task_queue, _run_after_restart())


async def test_budget_denied(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q6"
    wid = "mission-MS-FUNC-BUDGET-DENIED"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-BUDGET-DENIED",
                "task_id": "T-FUNC-6",
                "ticket": "test",
                "estimated_cost_usd": 10.0,
                "budget_limit_usd": 1.0,
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "BUDGET_DENIED", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_bedrock_skipped_without_model(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q7"
    wid = "mission-MS-FUNC-BEDROCK-SKIP"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-BEDROCK-SKIP",
                "task_id": "T-FUNC-7",
                "ticket": "Say hello",
                "use_bedrock": True,
                "model_id": "",
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "DONE", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_bedrock_retry_then_success(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q8"
    wid = "mission-MS-FUNC-BEDROCK-RETRY"
    invoke_calls = {"n": 0}

    def _fake_boto3_client(service_name: str, *args: Any, **kwargs: Any) -> Any:
        original = _fake_boto3_client.original
        if service_name != "bedrock-runtime":
            return original(service_name, *args, **kwargs)

        class _FakeBedrockClient:
            def invoke_model(self, **kw: Any) -> dict[str, Any]:
                _ = kw
                invoke_calls["n"] += 1
                if invoke_calls["n"] == 1:
                    raise ClientError(
                        {"Error": {"Code": "ThrottlingException", "Message": "rate"}},
                        "InvokeModel",
                    )
                payload = {
                    "content": [{"type": "text", "text": "hello from fake model"}],
                    "usage": {"input_tokens": 120, "output_tokens": 80},
                }
                return {"body": io.BytesIO(json.dumps(payload).encode("utf-8"))}

        return _FakeBedrockClient()

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-BEDROCK-RETRY",
                "task_id": "T-FUNC-8",
                "ticket": "Say hello",
                "use_bedrock": True,
                "model_id": "fake-model",
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "DONE", result
        bedrock = result.get("bedrock", {})
        assert bedrock.get("status") == "OK", bedrock
        assert float(bedrock.get("estimated_cost_usd", 0.0)) > 0.0, bedrock

    from naval.temporal import activities as activities_module

    prev_in = os.environ.get("BEDROCK_INPUT_USD_PER_1K")
    prev_out = os.environ.get("BEDROCK_OUTPUT_USD_PER_1K")
    os.environ["BEDROCK_INPUT_USD_PER_1K"] = "0.003"
    os.environ["BEDROCK_OUTPUT_USD_PER_1K"] = "0.015"
    _fake_boto3_client.original = activities_module.boto3.client  # type: ignore[attr-defined]
    try:
        with patch("naval.temporal.activities.boto3.client", side_effect=_fake_boto3_client):
            await _run_with_worker(env.client, task_queue, _run_case())
    finally:
        if prev_in is None:
            os.environ.pop("BEDROCK_INPUT_USD_PER_1K", None)
        else:
            os.environ["BEDROCK_INPUT_USD_PER_1K"] = prev_in
        if prev_out is None:
            os.environ.pop("BEDROCK_OUTPUT_USD_PER_1K", None)
        else:
            os.environ["BEDROCK_OUTPUT_USD_PER_1K"] = prev_out
    assert invoke_calls["n"] == 2, invoke_calls


async def test_bedrock_permission_denied(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q9"
    wid = "mission-MS-FUNC-BEDROCK-PERM"
    invoke_calls = {"n": 0}

    def _fake_boto3_client(service_name: str, *args: Any, **kwargs: Any) -> Any:
        original = _fake_boto3_client.original
        if service_name != "bedrock-runtime":
            return original(service_name, *args, **kwargs)

        class _FakeBedrockClient:
            def invoke_model(self, **kw: Any) -> dict[str, Any]:
                _ = kw
                invoke_calls["n"] += 1
                raise ClientError(
                    {"Error": {"Code": "AccessDeniedException", "Message": "no permission"}},
                    "InvokeModel",
                )

        return _FakeBedrockClient()

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-BEDROCK-PERM",
                "task_id": "T-FUNC-9",
                "ticket": "Say hello",
                "use_bedrock": True,
                "model_id": "fake-model",
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "FAILED", result
        assert result.get("error_code") == "BEDROCK_PERMISSION", result
        assert "permission" in str(result.get("error", "")).lower(), result

    from naval.temporal import activities as activities_module

    _fake_boto3_client.original = activities_module.boto3.client  # type: ignore[attr-defined]
    with patch("naval.temporal.activities.boto3.client", side_effect=_fake_boto3_client):
        await _run_with_worker(env.client, task_queue, _run_case())
    assert invoke_calls["n"] == 1, invoke_calls


async def test_budget_guard_post_deny(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q12"
    wid = "mission-MS-FUNC-BUDGET-GUARD-DENY"

    def _fake_boto3_client(service_name: str, *args: Any, **kwargs: Any) -> Any:
        original = _fake_boto3_client.original
        if service_name != "bedrock-runtime":
            return original(service_name, *args, **kwargs)

        class _FakeBedrockClient:
            def invoke_model(self, **kw: Any) -> dict[str, Any]:
                _ = kw
                payload = {
                    "content": [{"type": "text", "text": "budget burn"}],
                    "usage": {"input_tokens": 1000, "output_tokens": 1000},
                }
                return {"body": io.BytesIO(json.dumps(payload).encode("utf-8"))}

        return _FakeBedrockClient()

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-BUDGET-GUARD-DENY",
                "task_id": "T-FUNC-12",
                "ticket": "Burn budget",
                "use_bedrock": True,
                "model_id": "fake-model",
                "budget_limit_usd": 0.0001,
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "BUDGET_DENIED", result
        assert result.get("error_code") == "BUDGET_CIRCUIT_OPEN", result

    prev_in = os.environ.get("BEDROCK_INPUT_USD_PER_1K")
    prev_out = os.environ.get("BEDROCK_OUTPUT_USD_PER_1K")
    os.environ["BEDROCK_INPUT_USD_PER_1K"] = "0.010"
    os.environ["BEDROCK_OUTPUT_USD_PER_1K"] = "0.010"
    from naval.temporal import activities as activities_module

    _fake_boto3_client.original = activities_module.boto3.client  # type: ignore[attr-defined]
    try:
        with patch("naval.temporal.activities.boto3.client", side_effect=_fake_boto3_client):
            await _run_with_worker(env.client, task_queue, _run_case())
    finally:
        if prev_in is None:
            os.environ.pop("BEDROCK_INPUT_USD_PER_1K", None)
        else:
            os.environ["BEDROCK_INPUT_USD_PER_1K"] = prev_in
        if prev_out is None:
            os.environ.pop("BEDROCK_OUTPUT_USD_PER_1K", None)
        else:
            os.environ["BEDROCK_OUTPUT_USD_PER_1K"] = prev_out


async def test_artifact_schema_invalid(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q10"
    wid = "mission-MS-FUNC-ARTIFACT-SCHEMA"

    async def _run_case() -> None:
        handle = await env.client.start_workflow(
            MissionWorkflow.run,
            {
                "mission_id": "MS-FUNC-ARTIFACT-SCHEMA",
                "task_id": "T-FUNC-10",
                "ticket": "test",
                "confidence": 1.5,
            },
            id=wid,
            task_queue=task_queue,
        )
        result = await handle.result()
        assert result.get("status") == "FAILED", result
        assert result.get("error_code") == "ARTIFACT_SCHEMA_INVALID", result

    await _run_with_worker(env.client, task_queue, _run_case())


async def test_all_ships_artifact_schema(env: WorkflowEnvironment) -> None:
    task_queue = "naval-func-q11"
    ships = ("CVL", "DD", "CL", "CVB", "CA", "BB")

    async def _run_case() -> None:
        for idx, ship in enumerate(ships, start=1):
            mission_id = f"MS-FUNC-SHIP-{ship}"
            workflow_id = f"mission-{mission_id}"
            handle = await env.client.start_workflow(
                MissionWorkflow.run,
                {
                    "mission_id": mission_id,
                    "task_id": f"T-FUNC-11-{idx}",
                    "ticket": f"ship={ship}",
                    "ship": ship,
                },
                id=workflow_id,
                task_queue=task_queue,
            )
            result = await handle.result()
            assert result.get("status") == "DONE", (ship, result)

    await _run_with_worker(env.client, task_queue, _run_case())


async def _main() -> None:
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        await test_approval_yes(env)
        print("PASS: approval_yes")
        await test_approval_no(env)
        print("PASS: approval_no")
        await test_input_mode(env)
        print("PASS: input_mode")
        await test_timeout_cancelled(env)
        print("PASS: timeout_cancelled")
        await test_restart_resume_approval(env)
        print("PASS: restart_resume_approval")
        await test_budget_denied(env)
        print("PASS: budget_denied")
        await test_bedrock_skipped_without_model(env)
        print("PASS: bedrock_skipped_without_model")
        await test_bedrock_retry_then_success(env)
        print("PASS: bedrock_retry_then_success")
        await test_bedrock_permission_denied(env)
        print("PASS: bedrock_permission_denied")
        await test_artifact_schema_invalid(env)
        print("PASS: artifact_schema_invalid")
        await test_all_ships_artifact_schema(env)
        print("PASS: all_ships_artifact_schema")
        await test_budget_guard_post_deny(env)
        print("PASS: budget_guard_post_deny")
    finally:
        await env.shutdown()


if __name__ == "__main__":
    asyncio.run(_main())

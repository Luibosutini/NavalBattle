#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Naval Temporal worker (T-VNEXT-004 scaffold)")
    p.add_argument("--address", default=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    p.add_argument("--namespace", default=os.getenv("TEMPORAL_NAMESPACE", "default"))
    p.add_argument("--task-queue", default=os.getenv("TEMPORAL_TASK_QUEUE", "naval-mission"))
    p.add_argument(
        "--max-cached-workflows",
        type=int,
        default=int(os.getenv("TEMPORAL_MAX_CACHED_WORKFLOWS", "0")),
        help="Temporal worker cache size (default: 0 for deterministic restart behavior).",
    )
    return p.parse_args()


async def _run(address: str, namespace: str, task_queue: str, max_cached_workflows: int) -> None:
    from temporalio.client import Client
    from temporalio.worker import Worker

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

    client = await Client.connect(address, namespace=namespace)
    worker = Worker(
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
        max_cached_workflows=max_cached_workflows,
    )
    print(
        "[naval_worker] connected "
        f"address={address} namespace={namespace} task_queue={task_queue} "
        f"max_cached_workflows={max_cached_workflows}"
    )
    await worker.run()


def main() -> None:
    args = parse_args()
    try:
        import temporalio  # noqa: F401
    except ModuleNotFoundError:
        print("[ERROR] Missing dependency: temporalio")
        print("Install dependencies: pip install -r requirements.txt")
        raise SystemExit(1)
    asyncio.run(_run(args.address, args.namespace, args.task_queue, args.max_cached_workflows))


if __name__ == "__main__":
    main()

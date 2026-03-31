from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.runtime.base import RuntimeContext
from naval.runtime.temporal_runtime import TemporalRuntime


def _runtime() -> TemporalRuntime:
    return TemporalRuntime(
        RuntimeContext(
            repo_root=REPO_ROOT,
            config=None,
            profile="default",
            runtime="temporal",
        )
    )


def test_approve_input_signal_send() -> None:
    runtime = _runtime()
    calls: list[tuple[str, str, dict[str, Any]]] = []

    class _Handle:
        def __init__(self, workflow_id: str) -> None:
            self.workflow_id = workflow_id

        async def signal(self, name: str, payload: dict[str, Any]) -> None:
            calls.append((self.workflow_id, name, payload))

    class _Client:
        def get_workflow_handle(self, workflow_id: str) -> _Handle:
            return _Handle(workflow_id)

    async def _fake_connect(*args: Any, **kwargs: Any) -> _Client:
        _ = args
        _ = kwargs
        return _Client()

    with patch("temporalio.client.Client.connect", side_effect=_fake_connect):
        runtime.approve(mission="MS-U-1", yes=True, no=False, note="ok")
        runtime.input(mission="MS-U-1", message="hello")

    assert len(calls) == 2, calls
    assert calls[0][0] == "mission-MS-U-1"
    assert calls[0][1] == "approve"
    assert calls[0][2]["decision"] is True
    assert calls[1][1] == "provide_input"
    assert calls[1][2]["message"] == "hello"


def test_tail_marks_need_state() -> None:
    runtime = _runtime()
    rows = [
        {"ts": 1, "kind": "event", "etype": "WORKFLOW_STARTED", "message": "{}"},
        {"ts": 2, "kind": "event", "etype": "STATUS_NEED_APPROVAL", "message": "{}"},
        {"ts": 3, "kind": "comm", "etype": "need_input", "message": "input please"},
    ]
    out: list[str] = []
    with patch.object(runtime, "_read_mission_logs", return_value=rows), patch(
        "typer.echo", side_effect=lambda msg, *a, **k: out.append(str(msg))
    ):
        runtime.tail(mission="MS-U-2", limit=50, follow=False)

    assert any("!! [event]" in line and "STATUS_NEED_APPROVAL" in line for line in out), out
    assert any("!! [comm]" in line and "need_input" in line for line in out), out


def test_pull_sync_and_summary() -> None:
    runtime = _runtime()
    mission_id = "MS-U-3"
    prefix = f"missions/{mission_id}/"
    result_obj = {
        "task_id": "T-1",
        "ship": "CVL",
        "status": "OK",
        "confidence": 0.9,
    }
    store: dict[str, bytes] = {
        f"{prefix}artifacts/T-1/CVL/result.json": json.dumps(result_obj).encode("utf-8"),
        f"{prefix}artifacts/T-1/CVL/summary.md": b"# Summary\n",
        f"{prefix}events/1-a.json": b'{"ts":1}',
    }

    class _S3:
        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            p = str(kwargs.get("Prefix", ""))
            keys = [k for k in store if k.startswith(p)]
            return {
                "Contents": [{"Key": k} for k in keys],
                "IsTruncated": False,
            }

        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            key = str(kwargs["Key"])
            return {"Body": io.BytesIO(store[key])}

    out_lines: list[str] = []
    with TemporaryDirectory() as td, patch.object(
        runtime, "_resolve_region_bucket", return_value=("ap-northeast-1", "mock-bucket")
    ), patch("naval.runtime.temporal_runtime.boto3.client", return_value=_S3()), patch(
        "typer.echo", side_effect=lambda msg, *a, **k: out_lines.append(str(msg))
    ):
        runtime.pull(mission=mission_id, out=td)
        assert (Path(td) / "artifacts" / "T-1" / "CVL" / "result.json").exists()
        assert (Path(td) / "artifacts" / "T-1" / "CVL" / "summary.md").exists()

    assert any("result.json summary" in line for line in out_lines), out_lines
    assert any("T-1/CVL status=OK confidence=0.9" in line for line in out_lines), out_lines


def test_watch_prioritizes_need_state() -> None:
    runtime = _runtime()
    snapshots = [
        [
            {
                "mission_id": "MS-A",
                "workflow_status": "RUNNING",
                "user_state": "RUNNING",
                "needs_reason": "",
                "next_action": "",
            },
            {
                "mission_id": "MS-B",
                "workflow_status": "RUNNING",
                "user_state": "RUNNING",
                "needs_reason": "",
                "next_action": "",
            },
        ],
        [
            {
                "mission_id": "MS-A",
                "workflow_status": "RUNNING",
                "user_state": "RUNNING",
                "needs_reason": "",
                "next_action": "",
            },
            {
                "mission_id": "MS-B",
                "workflow_status": "RUNNING",
                "user_state": "NEED_APPROVAL",
                "needs_reason": "approval",
                "next_action": "",
            },
        ],
    ]
    idx = {"i": 0}
    selected: list[str] = []

    def _fake_list(*, limit: int = 500) -> list[dict[str, str]]:
        _ = limit
        i = min(idx["i"], len(snapshots) - 1)
        idx["i"] += 1
        return snapshots[i]

    def _fake_layout(**kwargs: Any) -> str:
        selected.append(str(kwargs["selected_mission"]))
        return "layout"

    class _FakeLive:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = args
            _ = kwargs

        def __enter__(self) -> "_FakeLive":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            _ = exc_type
            _ = exc
            _ = tb
            return False

        def update(self, renderable: Any, refresh: bool = True) -> None:
            _ = renderable
            _ = refresh

    sleep_calls = {"n": 0}

    def _fake_sleep(sec: float) -> None:
        _ = sec
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt()

    with patch.object(runtime, "_list_temporal_missions", side_effect=_fake_list), patch.object(
        runtime, "_read_mission_logs", return_value=[]
    ), patch.object(runtime, "_build_watch_layout", side_effect=_fake_layout), patch(
        "rich.live.Live", _FakeLive
    ), patch("time.sleep", side_effect=_fake_sleep):
        runtime.watch(month=None, interval=1, mission_filter="", page_size=20, page=1)

    assert selected[:2] == ["MS-A", "MS-B"], selected


def test_status_shows_next_action() -> None:
    runtime = _runtime()
    out: list[str] = []

    class _Desc:
        def __init__(self) -> None:
            self.status = "RUNNING"

    class _Handle:
        async def describe(self) -> _Desc:
            return _Desc()

        async def query(self, name: str) -> dict[str, Any]:
            _ = name
            return {
                "mission_id": "MS-U-4",
                "user_state": "NEED_APPROVAL",
                "needs_reason": "Awaiting human approval signal",
                "next_action": "",
            }

    class _Client:
        def get_workflow_handle(self, workflow_id: str, run_id: str | None = None) -> _Handle:
            _ = workflow_id
            _ = run_id
            return _Handle()

    async def _fake_connect(*args: Any, **kwargs: Any) -> _Client:
        _ = args
        _ = kwargs
        return _Client()

    with patch("temporalio.client.Client.connect", side_effect=_fake_connect), patch(
        "typer.echo", side_effect=lambda msg, *a, **k: out.append(str(msg))
    ):
        runtime.status(mission="MS-U-4", month=None)

    assert any("Next: naval approve --mission MS-U-4 --yes" in line for line in out), out


def main() -> None:
    test_approve_input_signal_send()
    print("PASS: temporal_runtime_actions.approve_input")
    test_tail_marks_need_state()
    print("PASS: temporal_runtime_actions.tail")
    test_pull_sync_and_summary()
    print("PASS: temporal_runtime_actions.pull")
    test_watch_prioritizes_need_state()
    print("PASS: temporal_runtime_actions.watch")
    test_status_shows_next_action()
    print("PASS: temporal_runtime_actions.status")


if __name__ == "__main__":
    main()

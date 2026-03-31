from __future__ import annotations

import sys
from pathlib import Path

from rich.console import Console

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.runtime.base import RuntimeContext
from naval.runtime.temporal_runtime import TemporalRuntime


def main() -> None:
    runtime = TemporalRuntime(
        RuntimeContext(
            repo_root=REPO_ROOT,
            config=None,
            profile="default",
            runtime="temporal",
        )
    )

    assert runtime._state_rank("NEED_APPROVAL") < runtime._state_rank("RUNNING")
    assert runtime._truncate("abcdef", 4) == "a..."

    rows = [
        {
            "mission_id": "MS-1",
            "workflow_status": "RUNNING",
            "user_state": "RUNNING",
            "needs_reason": "",
            "next_action": "",
        },
        {
            "mission_id": "MS-2",
            "workflow_status": "RUNNING",
            "user_state": "NEED_APPROVAL",
            "needs_reason": "waiting",
            "next_action": "naval approve --mission MS-2 --yes",
        },
    ]
    events = [
        {"ts": 1, "kind": "event", "etype": "WORKFLOW_STARTED", "message": "{}"},
        {"ts": 2, "kind": "event", "etype": "STATUS_NEED_APPROVAL", "message": "{}"},
    ]
    comms = [
        {"ts": 3, "kind": "comm", "etype": "need_approval", "message": "please approve"},
    ]

    layout = runtime._build_watch_layout(
        rows=rows,
        selected_mission="MS-2",
        events=events,
        comms=comms,
        mission_filter="MS",
        page=1,
        total_pages=1,
    )

    console = Console(record=True, width=140)
    console.print(layout)
    rendered = console.export_text()
    assert "MS-2" in rendered
    assert "NEED_APPROVAL" in rendered
    assert "need_approval" in rendered
    print("PASS: temporal_watch_tui")


if __name__ == "__main__":
    main()

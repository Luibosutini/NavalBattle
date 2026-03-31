from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.runtime.base import RuntimeContext
from naval.runtime.temporal_runtime import TemporalRuntime


def main() -> None:
    ctx = RuntimeContext(
        repo_root=REPO_ROOT,
        config=None,
        profile="default",
        runtime="temporal",
    )
    runtime = TemporalRuntime(ctx)

    with TemporaryDirectory() as td:
        ticket_path = Path(td) / "ticket.txt"
        ticket_path.write_text("ticket from file", encoding="utf-8")
        assert runtime._resolve_ticket(str(ticket_path)) == "ticket from file"

    assert runtime._resolve_ticket("inline ticket text") == "inline ticket text"
    assert runtime._normalize_workflow_id("MS-1") == "mission-MS-1"
    assert runtime._normalize_workflow_id("mission-MS-1") == "mission-MS-1"

    assert (
        runtime._derive_next_action("MS-2", "NEED_APPROVAL", "")
        == "naval approve --mission MS-2 --yes"
    )
    assert (
        runtime._derive_next_action("MS-3", "NEED_INPUT", "")
        == "naval input --mission MS-3 \"<message>\""
    )
    assert runtime._derive_next_action("MS-4", "RUNNING", "") == ""
    assert runtime._derive_next_action("MS-5", "RUNNING", "naval custom") == "naval custom"

    print("PASS: temporal_runtime_helpers")


if __name__ == "__main__":
    main()

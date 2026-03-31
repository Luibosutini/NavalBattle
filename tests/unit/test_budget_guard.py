from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.temporal.activities import budget_guard_activity


class _FakeDDB:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        key = kwargs.get("Key", {}).get("mission_id", {}).get("S", "")
        if key in self.items:
            return {"Item": self.items[key]}
        return {}

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        item = kwargs.get("Item", {})
        key = item.get("mission_id", {}).get("S", "")
        if key:
            self.items[key] = item
        return {}


def main() -> None:
    fake_ddb = _FakeDDB()
    prev_table = os.environ.get("NAVAL_BUDGET_GUARD_TABLE")
    os.environ["NAVAL_BUDGET_GUARD_TABLE"] = "mock-budget-table"

    base = {
        "mission_id": "MS-BG-1",
        "task_id": "T-BG-1",
        "ship": "CVL",
        "trace_id": "MS-BG-1",
        "budget_limit_usd": 1.0,
    }

    try:
        with patch("naval.temporal.activities._ddb_client", return_value=fake_ddb):
            pre1 = asyncio.run(budget_guard_activity({**base, "phase": "pre"}))
            assert pre1["allowed"] is True, pre1

            post1 = asyncio.run(budget_guard_activity({**base, "phase": "post", "call_cost_usd": 0.8}))
            assert post1["allowed"] is True, post1
            assert float(post1["total_spent_usd"]) == 0.8, post1

            pre2 = asyncio.run(budget_guard_activity({**base, "phase": "pre"}))
            assert pre2["allowed"] is True, pre2

            post2 = asyncio.run(budget_guard_activity({**base, "phase": "post", "call_cost_usd": 0.4}))
            assert post2["allowed"] is False, post2
            assert post2["circuit_open"] is True, post2

            pre3 = asyncio.run(budget_guard_activity({**base, "phase": "pre"}))
            assert pre3["allowed"] is False, pre3
            assert pre3["reason"] == "circuit_open", pre3
    finally:
        if prev_table is None:
            os.environ.pop("NAVAL_BUDGET_GUARD_TABLE", None)
        else:
            os.environ["NAVAL_BUDGET_GUARD_TABLE"] = prev_table

    skipped = asyncio.run(
        budget_guard_activity(
            {
                "mission_id": "MS-BG-2",
                "task_id": "T-BG-2",
                "ship": "DD",
                "trace_id": "MS-BG-2",
                "phase": "pre",
            }
        )
    )
    assert skipped["status"] == "SKIPPED", skipped
    assert skipped["allowed"] is True, skipped

    print("PASS: budget_guard")


if __name__ == "__main__":
    main()

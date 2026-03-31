from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_step(name: str, command: list[str]) -> None:
    print(f"[STEP] {name}")
    proc = subprocess.run(command, cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        raise SystemExit(f"[FAIL] {name} (exit_code={proc.returncode})")
    print(f"[PASS] {name}")


def main() -> None:
    py = sys.executable
    run_step(
        "Temporal functional HITL+budget acceptance",
        [py, "tests/functional/test_temporal_hitl.py"],
    )
    run_step(
        "Temporal runtime command acceptance",
        [py, "tests/unit/test_temporal_runtime_actions.py"],
    )
    run_step(
        "Context protocol acceptance",
        [py, "tests/unit/test_context_protocol.py"],
    )
    run_step(
        "Budget guard acceptance",
        [py, "tests/unit/test_budget_guard.py"],
    )
    run_step(
        "Watch TUI rendering acceptance",
        [py, "tests/unit/test_temporal_watch_tui.py"],
    )
    run_step(
        "Bootstrap local/aws acceptance",
        [py, "tests/unit/test_bootstrap_local.py"],
    )
    run_step(
        "Bootstrap aws apply path acceptance",
        [py, "tests/unit/test_bootstrap_aws.py"],
    )
    print("[DONE] vNext acceptance suite")


if __name__ == "__main__":
    main()

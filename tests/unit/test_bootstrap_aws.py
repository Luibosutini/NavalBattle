from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.bootstrap import (
    AWS_ENV_FILE,
    AWS_TERRAFORM_DIR,
    bootstrap_aws,
    ensure_aws_bootstrap_files,
)


def test_aws_scaffold_files() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        tf_dir, created = ensure_aws_bootstrap_files(root)
        assert tf_dir == root / AWS_TERRAFORM_DIR
        assert (tf_dir / "main.tf").exists()
        assert (tf_dir / "variables.tf").exists()
        assert (tf_dir / "outputs.tf").exists()
        assert (tf_dir / "terraform.tfvars.example").exists()
        assert (tf_dir / "README.md").exists()
        assert len(created) == 5, created

        # idempotent
        _, created2 = ensure_aws_bootstrap_files(root)
        assert created2 == []


def test_aws_bootstrap_dry_run() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        result = bootstrap_aws(root, apply=False, region="ap-northeast-1", name_prefix="Naval Vnext")
        assert result.applied is False
        assert "terraform scaffold generated" in result.message
        assert (root / AWS_TERRAFORM_DIR / "main.tf").exists()


def test_aws_bootstrap_no_terraform() -> None:
    with TemporaryDirectory() as td, patch("naval.bootstrap.shutil.which", return_value=None):
        root = Path(td)
        result = bootstrap_aws(root, apply=True, region="ap-northeast-1", name_prefix="naval-vnext")
        assert result.applied is False
        assert "terraform command not found" in result.message


def test_aws_bootstrap_apply_success() -> None:
    outputs = {
        "region": {"value": "ap-northeast-1"},
        "sqs_name": {"value": "naval-vnext-queue"},
        "state_table": {"value": "naval-vnext-state"},
        "budget_table": {"value": "naval-vnext-budget"},
        "budget_guard_table": {"value": "naval-vnext-budget-guard"},
        "index_table": {"value": "naval-vnext-index"},
        "artifact_bucket": {"value": "naval-vnext-artifacts"},
    }

    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], *args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
        _ = args
        _ = kwargs
        calls.append(cmd)
        if "output" in cmd:
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(outputs), stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    with TemporaryDirectory() as td, patch("naval.bootstrap.shutil.which", return_value="terraform"), patch(
        "naval.bootstrap.subprocess.run", side_effect=_fake_run
    ):
        root = Path(td)
        result = bootstrap_aws(root, apply=True, region="ap-northeast-1", name_prefix="NAVAL Vnext!!")
        assert result.applied is True, result
        assert "terraform apply completed" in result.message
        assert any("init" in cmd for cmd in calls), calls
        assert any("apply" in cmd for cmd in calls), calls
        env_file = root / AWS_ENV_FILE
        assert env_file.exists(), env_file
        env_text = env_file.read_text(encoding="utf-8")
        assert "FLEET_SQS_NAME=naval-vnext-queue" in env_text, env_text


def main() -> None:
    test_aws_scaffold_files()
    print("PASS: bootstrap_aws.scaffold")
    test_aws_bootstrap_dry_run()
    print("PASS: bootstrap_aws.dry_run")
    test_aws_bootstrap_no_terraform()
    print("PASS: bootstrap_aws.no_terraform")
    test_aws_bootstrap_apply_success()
    print("PASS: bootstrap_aws.apply_success")


if __name__ == "__main__":
    main()

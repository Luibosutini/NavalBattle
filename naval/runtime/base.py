from __future__ import annotations

import os
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer


@dataclass
class RuntimeContext:
    repo_root: Path
    config: Optional[Path]
    profile: str
    runtime: str

    def env(self) -> dict[str, str]:
        out = os.environ.copy()
        if self.config:
            out["FLEET_LOCAL_CONFIG"] = str(self.config)
        out["NAVAL_PROFILE"] = self.profile
        out["NAVAL_RUNTIME"] = self.runtime
        return out

    def now_month(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")


class RuntimeBase(ABC):
    def __init__(self, ctx: RuntimeContext) -> None:
        self.ctx = ctx

    def run_script(self, script_name: str, args: list[str]) -> None:
        script = self.ctx.repo_root / script_name
        if not script.exists():
            typer.echo(f"[ERROR] script not found: {script_name}", err=True)
            raise typer.Exit(2)
        cmd = [sys.executable, str(script), *args]
        proc = subprocess.run(cmd, cwd=str(self.ctx.repo_root), env=self.ctx.env())
        if proc.returncode != 0:
            raise typer.Exit(proc.returncode)

    @abstractmethod
    def enqueue(
        self,
        *,
        doctrine: str,
        ticket: str,
        budget: Optional[str],
        repo_url: str,
        task_id: str,
        watch: bool,
        hitl_mode: str,
        hitl_timeout_hours: int,
        hitl_timeout_action: str,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def status(self, *, mission: str, month: Optional[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def tail(self, *, mission: str, limit: int, follow: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def approve(self, *, mission: str, yes: bool, no: bool, note: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def input(self, *, mission: str, message: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def pull(self, *, mission: str, out: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def watch(
        self,
        *,
        month: Optional[str],
        interval: int,
        mission_filter: str,
        page_size: int,
        page: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def ca(self, *, task_id: str, directive: str, repo_url: str, auto_advance: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def pending(self) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # ワークスペース管理（ランタイム非依存、navalctl.py に委譲）           #
    # ------------------------------------------------------------------ #

    def ws_init(self) -> None:
        self.run_script("navalctl.py", ["init"])

    def ws_mkws(self, *, mission_id: str, task_id: str, ship_class: str, ship_id: str) -> None:
        args = ["mkws", "--mission-id", mission_id, "--task-id", task_id, "--ship-class", ship_class]
        if ship_id:
            args.extend(["--ship-id", ship_id])
        self.run_script("navalctl.py", args)

    def ws_mental(self, *, ship_class: str) -> None:
        self.run_script("navalctl.py", ["mental", "--ship-class", ship_class])

    def ws_search(
        self,
        *,
        query: str,
        mission_id: str,
        task_id: str,
        ship_class: str,
        limit: int,
    ) -> None:
        args = ["search", "--limit", str(limit)]
        if query:
            args.extend(["--query", query])
        if mission_id:
            args.extend(["--mission-id", mission_id])
        if task_id:
            args.extend(["--task-id", task_id])
        if ship_class:
            args.extend(["--ship-class", ship_class])
        self.run_script("navalctl.py", args)

    def ws_tail(self, *, limit: int) -> None:
        self.run_script("navalctl.py", ["tail", "--limit", str(limit)])

    def ws_info(self) -> None:
        self.run_script("navalctl.py", ["info"])

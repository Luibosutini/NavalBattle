from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer

from naval.service import (
    FleetService,
    FleetServiceError,
    MissionSummary,
    TaskAlreadyFinished,
)

from .base import RuntimeBase


def _format_updated(ts: int, fmt: str = "%m-%d %H:%MZ") -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts, timezone.utc).strftime(fmt)
    except (ValueError, OSError):
        return str(ts)


class AwsRuntime(RuntimeBase):
    """AWS runtime: thin presentation layer over naval.service.FleetService."""

    _service: Optional[FleetService] = None

    @property
    def service(self) -> FleetService:
        if self._service is None:
            self._service = FleetService(
                env=self.ctx.env(),
                repo_root=self.ctx.repo_root,
            )
        return self._service

    @staticmethod
    def _summary_to_row(m: MissionSummary) -> Dict[str, str]:
        return {
            "mission_id": m.mission_id,
            "user_state": m.status,
            "ship_class": m.ship_class,
            "task_id": m.task_id,
            "updated": _format_updated(m.updated_at),
        }

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        return text[: max(limit - 3, 0)] + "..."

    # ------------------------------------------------------------------ #
    # RuntimeBase interface                                                #
    # ------------------------------------------------------------------ #

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
        if hitl_mode:
            typer.echo(
                "[WARN] hitl options are ignored in aws runtime (applies to temporal runtime only)."
            )
        _ = hitl_timeout_hours
        _ = hitl_timeout_action
        budget_month = budget or self.ctx.now_month()
        ticket_path = Path(ticket).expanduser()
        args: list[str] = ["run", "--formation", doctrine, "--budget-month", budget_month]
        if task_id:
            args.extend(["--task-id", task_id])
        if repo_url:
            args.extend(["--repo-url", repo_url])
        if ticket_path.exists():
            args.extend(["--ticket-file", str(ticket_path)])
        else:
            args.extend(["--objective", ticket])
        if not watch:
            args.append("--no-watch")
        self.run_script("fleet_interact.py", args)

    def status(self, *, mission: str, month: Optional[str]) -> None:
        if mission:
            self.run_script("task_orchestrator.py", ["status", mission])
            return
        budget_month = month or self.ctx.now_month()
        self.run_script("mission_monitor.py", [budget_month])

    def tail(self, *, mission: str, limit: int, follow: bool) -> None:
        args = ["search", "--limit", str(limit)]
        if mission:
            args.extend(["--mission-id", mission])
        if follow:
            typer.echo("[WARN] --follow is not supported in aws runtime; showing single snapshot.")
        self.run_script("navalctl.py", args)

    def approve(self, *, mission: str, yes: bool, no: bool, note: str) -> None:
        if yes == no:
            typer.echo("[ERROR] choose exactly one of --yes or --no", err=True)
            raise typer.Exit(2)
        try:
            new_status = self.service.approve_mission(mission, yes=yes, note=note)
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)
        if new_status == "RUNNING":
            typer.echo(f"mission approved and re-enqueued: mission_id={mission}")
        else:
            typer.echo(f"mission rejected: mission_id={mission}")

    def input(self, *, mission: str, message: str) -> None:
        try:
            self.service.send_input(mission, message)
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)
        typer.echo(f"input sent and mission re-enqueued: mission_id={mission}")

    def pull(self, *, mission: str, out: str) -> None:
        import json

        out_dir = Path(out).expanduser() if out else (self.ctx.repo_root / "missions" / mission)
        try:
            files = self.service.list_artifacts(mission)
        except FleetServiceError as exc:
            msg = str(exc)
            typer.echo(f"[ERROR] {msg}", err=True)
            raise typer.Exit(2 if "FLEET_S3_BUCKET" in msg else 1)

        if not files:
            typer.echo(f"No objects found for mission={mission}")
            return

        out_dir.mkdir(parents=True, exist_ok=True)
        result_summaries: List[str] = []
        for f in files:
            dest = out_dir / f.path
            dest.parent.mkdir(parents=True, exist_ok=True)
            body = self.service.get_artifact_bytes(mission, f.path)
            dest.write_bytes(body)
            if f.key.endswith("/result.json"):
                try:
                    obj_data = json.loads(body.decode("utf-8"))
                    result_summaries.append(
                        f"{obj_data.get('task_id', '?')}/{obj_data.get('ship', '?')} "
                        f"status={obj_data.get('status', '?')} confidence={obj_data.get('confidence', '?')}"
                    )
                except Exception:
                    result_summaries.append(f"{f.path} (parse_error)")

        typer.echo(f"pulled mission={mission} files={len(files)} out={out_dir}")
        if result_summaries:
            typer.echo("result.json summary:")
            for line in result_summaries:
                typer.echo(f"- {line}")

    def ca(self, *, task_id: str, directive: str, repo_url: str, auto_advance: bool) -> None:
        try:
            mission_id = self.service.save_ca_directive(task_id, directive)
        except FleetServiceError as exc:
            msg = str(exc)
            typer.echo(f"[ERROR] {msg}", err=True)
            raise typer.Exit(2 if "FLEET_S3_BUCKET" in msg else 1)

        typer.echo(f"CA directive saved: mission_id={mission_id}")

        if not auto_advance:
            return

        args = ["advance", task_id]
        if repo_url:
            args.append(repo_url)
        self.run_script("task_orchestrator.py", args)

    # ------------------------------------------------------------------ #
    # watch                                                                #
    # ------------------------------------------------------------------ #

    def _build_aws_watch_layout(
        self,
        rows: List[Dict[str, str]],
        selected_mission: str,
        comms: List[Dict[str, Any]],
        mission_filter: str,
        page: int,
        total_pages: int,
    ) -> Any:
        from rich.layout import Layout
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text

        layout = Layout(name="root")
        layout.split_row(Layout(name="left", ratio=2), Layout(name="right", ratio=3))

        tbl = Table(expand=True)
        tbl.add_column("Mission", overflow="fold")
        tbl.add_column("State", width=16)
        tbl.add_column("Ship", width=5)
        tbl.add_column("Updated", width=13)

        if not rows:
            tbl.add_row("-", "no missions", "-", "-")
        else:
            for row in rows:
                mid = row["mission_id"]
                state = row["user_state"]
                style = "bold red" if state in {"NEED_APPROVAL", "NEED_INPUT"} else ""
                marker = ">" if mid == selected_mission else " "
                tbl.add_row(
                    f"{marker} {mid}",
                    Text(state, style=style),
                    row.get("ship_class", ""),
                    row.get("updated", ""),
                )

        filter_label = f" filter={mission_filter}" if mission_filter else ""
        layout["left"].update(Panel(tbl, title=f"Missions page={page}/{total_pages}{filter_label}"))

        comms_text = Text()
        if not comms:
            comms_text.append("No comms\n")
        else:
            for c in comms[-25:]:
                ctype = str(c.get("type", ""))
                sender = str(c.get("from", "?"))
                msg = self._truncate(str(c.get("content", "")), 120)
                is_need = ctype in {"need_approval", "need_input"}
                style = "bold red" if is_need else ""
                comms_text.append(f"[{sender}] {ctype}\n{msg}\n\n", style=style)
        layout["right"].update(Panel(comms_text, title=f"Comms ({selected_mission or '-'})"))

        return layout

    def watch(
        self,
        *,
        month: Optional[str],
        interval: int,
        mission_filter: str,
        page_size: int,
        page: int,
    ) -> None:
        try:
            from rich.console import Console
            from rich.live import Live
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        refresh_sec = max(1, interval)
        page_size = max(1, min(page_size, 200))
        page = max(1, page)
        filter_norm = mission_filter.strip().lower()
        console = Console()
        selected_mission = ""

        try:
            with Live(console=console, screen=True, auto_refresh=False) as live:
                while True:
                    try:
                        summaries = self.service.list_missions()
                    except FleetServiceError as exc:
                        from rich.panel import Panel
                        live.update(Panel(f"[red]Error: {exc}[/red]", title="watch"), refresh=True)
                        time.sleep(refresh_sec)
                        continue

                    rows = [self._summary_to_row(m) for m in summaries]

                    # オプションのフィルタ（月 or 文字列）
                    budget_month = month or self.ctx.now_month()
                    if filter_norm:
                        rows = [
                            r for r in rows
                            if filter_norm in r["mission_id"].lower()
                            or filter_norm in r["user_state"].lower()
                        ]
                    elif month:
                        # updated フィールドは MM-DD HH:MMZ 形式なので month フィルタは task_id で代用
                        rows = [r for r in rows if budget_month in r.get("task_id", "")]

                    total_pages = max(1, (len(rows) + page_size - 1) // page_size)
                    page_use = min(page, total_pages)
                    start = (page_use - 1) * page_size
                    rows_page = rows[start: start + page_size]

                    # フォーカスミッションを決定
                    need_rows = [r for r in rows_page if r["user_state"] in {"NEED_APPROVAL", "NEED_INPUT"}]
                    if need_rows:
                        selected_mission = need_rows[0]["mission_id"]
                    elif not selected_mission and rows_page:
                        selected_mission = rows_page[0]["mission_id"]
                    elif selected_mission and all(r["mission_id"] != selected_mission for r in rows_page):
                        selected_mission = rows_page[0]["mission_id"] if rows_page else ""

                    comms: List[Dict[str, Any]] = []
                    if selected_mission:
                        comms = [
                            {"ts": c.ts, "from": c.sender, "type": c.type, "content": c.content}
                            for c in self.service.list_comms(selected_mission)
                        ]

                    layout = self._build_aws_watch_layout(
                        rows=rows_page,
                        selected_mission=selected_mission,
                        comms=comms,
                        mission_filter=mission_filter,
                        page=page_use,
                        total_pages=total_pages,
                    )
                    live.update(layout, refresh=True)
                    time.sleep(refresh_sec)

        except KeyboardInterrupt:
            typer.echo("watch stopped.")
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] watch failed: {exc}", err=True)
            raise typer.Exit(1)

    # ------------------------------------------------------------------ #
    # run                                                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _timed_input(prompt: str, timeout: int) -> Optional[str]:
        """Show prompt and wait up to timeout seconds for input. Returns None on timeout."""
        import threading

        result: List[Optional[str]] = [None]
        ready = threading.Event()

        def _reader() -> None:
            try:
                result[0] = input(prompt)
            except (EOFError, KeyboardInterrupt):
                result[0] = None
            finally:
                ready.set()

        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        ready.wait(timeout)
        return result[0]

    def run(
        self,
        *,
        objective: str,
        ticket_file: str,
        task_id: str,
        repo_url: str,
        doctrine: str,
        budget: Optional[str],
        interval: int,
        auto_approve: bool,
        auto_approve_threshold: int,
    ) -> None:
        """1コマンドで投入からHITL対応まで完結するメインループ。"""
        from naval.notify import notify
        from rich.console import Console

        console = Console()
        budget_month = budget or self.ctx.now_month()

        if not task_id:
            task_id = f"T-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

        # 1. チケットS3アップロード
        try:
            ticket_s3 = self.service.put_ticket(
                task_id, objective=objective, ticket_file=ticket_file
            )
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(2)

        # 2. task_orchestrator init / start
        self.run_script(
            "task_orchestrator.py",
            ["init", task_id, ticket_s3, budget_month, "--formation", doctrine],
        )
        start_args = ["start", task_id]
        if repo_url:
            start_args.append(repo_url)
        self.run_script("task_orchestrator.py", start_args)

        console.print(f"[bold green]Task started:[/bold green] {task_id}")
        if auto_approve:
            console.print(
                f"[cyan]Auto-approve enabled (threshold={auto_approve_threshold})[/cyan]"
            )
        console.print(
            "[dim]Ctrl+C to stop watching  |  RUNNING 中は Enter で CA 指令、q で終了[/dim]"
        )

        # 3. 監視ループ
        terminal = {"DONE", "FAILED", "BUDGET_DENIED"}
        last_state: tuple[Optional[str], Optional[str]] = (None, None)

        try:
            while True:
                status, stage = self.service.get_task_status(task_id)
                state = (status, stage)
                if state != last_state:
                    console.print(f"  status=[bold]{status}[/bold]  stage={stage}")
                    last_state = state

                if status in terminal:
                    color = "green" if status == "DONE" else "red"
                    console.print(f"[bold {color}]Finished: {status}[/bold {color}]")
                    notify("Naval Battle", f"{task_id} {status}")
                    break

                advance_args = ["advance", task_id]
                if repo_url:
                    advance_args.append(repo_url)

                if status == "NEED_APPROVAL":
                    confidence = self.service.get_ca_confidence(task_id)
                    if auto_approve and confidence >= auto_approve_threshold:
                        n = self.service.approve_task_missions(
                            task_id, f"Auto-approved (confidence={confidence})"
                        )
                        console.print(
                            f"[green]Auto-approved {n} mission(s) (confidence={confidence})[/green]"
                        )
                        self.run_script("task_orchestrator.py", advance_args)
                    else:
                        notify(
                            "Naval Battle",
                            f"承認待ち: {task_id}  確信度={confidence}",
                        )
                        console.print(
                            f"[bold yellow]Approval required[/bold yellow]"
                            f"  confidence=[bold]{confidence}[/bold]/{auto_approve_threshold}"
                        )
                        ans = input("Approve? [y/N] (note after space, e.g. 'y looks good'): ").strip()
                        if ans.lower().startswith("y"):
                            note_text = ans[1:].strip() or f"Approved (confidence={confidence})"
                            n = self.service.approve_task_missions(task_id, note_text)
                            console.print(f"[green]Approved {n} mission(s).[/green]")
                            self.run_script("task_orchestrator.py", advance_args)
                        else:
                            console.print("[dim]Skipped. Will poll again.[/dim]")
                            time.sleep(interval)

                elif status == "NEED_INPUT":
                    notify("Naval Battle", f"入力待ち: {task_id}")
                    console.print("[bold yellow]Input required[/bold yellow]")
                    console.print("Enter response (Ctrl+D to finish, empty = skip):")
                    lines_in: List[str] = []
                    try:
                        while True:
                            lines_in.append(input())
                    except EOFError:
                        pass
                    message = "\n".join(lines_in).strip()
                    if message:
                        n = self.service.input_task_missions(task_id, message)
                        console.print(f"[green]Input sent to {n} mission(s).[/green]")
                        self.run_script("task_orchestrator.py", advance_args)
                    else:
                        console.print("[dim]No input. Will poll again.[/dim]")
                        time.sleep(interval)

                elif status in ("RUNNING", "ENQUEUED"):
                    self.run_script("task_orchestrator.py", advance_args)
                    # タイムアウト付き対話待機
                    user_in = self._timed_input(
                        f"  [{interval}s] CA指令を入力 (Enter=スキップ, q=終了): ",
                        interval,
                    )
                    if user_in is None:
                        pass  # timeout — continue
                    elif user_in.strip().lower() == "q":
                        console.print("[yellow]Watch stopped.[/yellow]")
                        break
                    elif user_in.strip():
                        directive = user_in.strip()
                        try:
                            mission_id = self.service.save_ca_directive(task_id, directive)
                            console.print(
                                f"[green]CA directive saved → mission {mission_id}[/green]"
                            )
                            self.run_script("task_orchestrator.py", advance_args)
                        except FleetServiceError as exc:
                            if "no CA mission" in str(exc):
                                console.print("[yellow]CA mission not found; directive not saved.[/yellow]")
                            else:
                                console.print(f"[red]Failed to save CA directive: {exc}[/red]")

                else:
                    console.print(f"[yellow]Unknown status: {status}[/yellow]")
                    time.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]Watch stopped (Ctrl+C).[/yellow]")

    # ------------------------------------------------------------------ #
    # abort / show / retry                                                 #
    # ------------------------------------------------------------------ #

    def abort(self, *, task_id: str, note: str) -> None:
        try:
            cancelled_count = self.service.abort_task(task_id, note)
        except TaskAlreadyFinished as exc:
            typer.echo(f"[WARN] {exc}")
            return
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)

        abort_reason = note or "Aborted by user"
        typer.echo(
            f"aborted: task_id={task_id}"
            f"  missions_cancelled={cancelled_count}"
            f"  reason={abort_reason!r}"
        )

    def show(self, *, task_id: str) -> None:
        try:
            from rich.console import Console
            from rich.panel import Panel
            from rich.table import Table
            from rich.text import Text
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        try:
            detail = self.service.get_task(task_id)
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)

        console = Console()
        status_color = {
            "DONE": "green",
            "FAILED": "red",
            "CANCELLED": "yellow",
            "RUNNING": "cyan",
            "NEED_APPROVAL": "bold red",
            "NEED_INPUT": "bold red",
        }.get(detail.status, "white")

        console.print(
            Panel(
                f"task_id=[bold]{task_id}[/bold]  "
                f"status=[{status_color}]{detail.status}[/{status_color}]  "
                f"stage={detail.current_stage or '-'}  "
                f"budget={detail.budget_month or '-'}  "
                f"updated={_format_updated(detail.updated_at, '%Y-%m-%d %H:%MZ')}",
                title="Task",
            )
        )

        if not detail.stages:
            console.print("[dim]No stage info.[/dim]")
            return

        tbl = Table(show_lines=True)
        tbl.add_column("Stage", style="bold cyan", no_wrap=True)
        tbl.add_column("Status", width=16)
        tbl.add_column("Mission ID", overflow="fold")
        tbl.add_column("Ship", width=5)
        tbl.add_column("Conf", width=5, justify="right")
        tbl.add_column("Updated", width=13)

        for s in detail.stages:
            st_color = {
                "DONE": "green", "FAILED": "red", "CANCELLED": "dim",
                "RUNNING": "cyan", "NEED_APPROVAL": "bold red", "NEED_INPUT": "bold red",
            }.get(s.status, "")
            tbl.add_row(
                s.stage,
                Text(s.status or "-", style=st_color),
                s.mission_id,
                s.ship_class,
                "" if s.confidence is None else str(s.confidence),
                _format_updated(s.updated_at),
            )

        console.print(tbl)

        if detail.abort_reason:
            console.print(f"[dim]abort_reason: {detail.abort_reason}[/dim]")

    def retry(self, *, task_id: str, note: str) -> None:
        try:
            retried, errors = self.service.retry_task(task_id, note)
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)

        if retried == 0:
            typer.echo(f"No FAILED missions found for task {task_id}.")
            return

        typer.echo(
            f"retried: task_id={task_id}"
            f"  missions_retried={retried}"
            + (f"  errors={errors}" if errors else "")
        )

    # ------------------------------------------------------------------ #
    # pending                                                              #
    # ------------------------------------------------------------------ #

    def pending(self) -> None:
        """NEED_INPUT / NEED_APPROVAL なミッションを一覧表示して対話応答する。"""
        try:
            from rich.console import Console
            from rich.panel import Panel
            from rich.table import Table
        except ModuleNotFoundError:
            typer.echo("[ERROR] Missing dependency: rich", err=True)
            raise typer.Exit(1)

        console = Console()
        try:
            pending = self.service.list_pending()
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)

        if not pending:
            console.print("[green]No missions pending human input/approval.[/green]")
            return

        rows = [self._summary_to_row(m) for m in pending]

        # ミッション一覧テーブル
        tbl = Table(title="Pending Missions", show_lines=True)
        tbl.add_column("#", style="bold cyan", width=3)
        tbl.add_column("Status", style="bold red", width=16)
        tbl.add_column("Mission ID")
        tbl.add_column("Ship", width=5)
        tbl.add_column("Task")
        tbl.add_column("Updated", width=13)

        for i, row in enumerate(rows, 1):
            tbl.add_row(
                str(i),
                row["user_state"],
                row["mission_id"],
                row["ship_class"],
                row["task_id"],
                row["updated"],
            )
        console.print(tbl)

        # ミッション選択
        try:
            choice = input("\nSelect mission number (q=quit): ").strip()
        except (EOFError, KeyboardInterrupt):
            return
        if choice.lower() == "q":
            return
        try:
            idx = int(choice) - 1
            if not (0 <= idx < len(rows)):
                raise ValueError
        except ValueError:
            typer.echo("[ERROR] invalid selection", err=True)
            raise typer.Exit(1)

        selected = rows[idx]
        mission_id = selected["mission_id"]
        status = selected["user_state"]

        console.rule(f"[bold]{mission_id}[/bold]")
        console.print(f"Status : [bold red]{status}[/bold red]")
        console.print(f"Ship   : {selected['ship_class']}  |  Task : {selected['task_id']}")

        # 最近の通信ログを表示
        comms = self.service.list_comms(mission_id)
        if comms:
            lines = "\n".join(
                f"[{c.sender}] {self._truncate(c.content, 100)}"
                for c in comms[-5:]
            )
            console.print(Panel(lines, title="Recent Communications"))

        # 応答入力
        if status == "NEED_APPROVAL":
            try:
                answer = input("\nApprove? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return
            yes = answer in ("y", "yes")
            new_status = "RUNNING" if yes else "FAILED"
            response_text = "approved" if yes else "rejected"

        elif status == "NEED_INPUT":
            console.print("\nEnter response (Ctrl+D / Ctrl+Z to finish):")
            lines_in: List[str] = []
            try:
                while True:
                    lines_in.append(input())
            except EOFError:
                pass
            except KeyboardInterrupt:
                return
            response_text = "\n".join(lines_in).strip()
            if not response_text:
                typer.echo("No response provided, aborting.")
                return
            new_status = "RUNNING"

        else:
            typer.echo(f"[ERROR] unexpected status: {status}", err=True)
            raise typer.Exit(1)

        try:
            self.service.resume_mission(
                mission_id,
                expected_status=status,
                new_status=new_status,
                response_text=response_text,
            )
        except FleetServiceError as exc:
            typer.echo(f"[ERROR] {exc}", err=True)
            raise typer.Exit(1)

        if new_status == "RUNNING":
            console.print(f"[green]Mission {mission_id} resumed.[/green]")
        else:
            console.print(f"[yellow]Mission {mission_id} rejected.[/yellow]")

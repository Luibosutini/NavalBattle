from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import boto3
import typer
from botocore.exceptions import BotoCoreError, ClientError

from .bootstrap import bootstrap_aws, bootstrap_local
from .runtime import RuntimeBase, RuntimeContext, make_runtime

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Naval Battle unified CLI (vNext scaffold).",
)

CHECK_OK = "OK"
CHECK_WARN = "WARN"
CHECK_FAIL = "FAIL"


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_config(config: Optional[Path], root: Path) -> Optional[Path]:
    if config is None:
        return None
    p = config.expanduser()
    if not p.is_absolute():
        p = (root / p).resolve()
    return p


def _ctx_state(ctx: typer.Context) -> tuple[RuntimeContext, RuntimeBase]:
    obj = ctx.obj
    if not isinstance(obj, dict):
        typer.echo("[ERROR] internal state is not initialized", err=True)
        raise typer.Exit(2)
    runtime_ctx = obj.get("runtime_ctx")
    runtime_impl = obj.get("runtime_impl")
    if not isinstance(runtime_ctx, RuntimeContext) or not isinstance(runtime_impl, RuntimeBase):
        typer.echo("[ERROR] internal runtime is not initialized", err=True)
        raise typer.Exit(2)
    return runtime_ctx, runtime_impl


def _check_temporal(address: str, timeout_sec: float = 2.0) -> CheckResult:
    host, _, port_str = address.partition(":")
    if not host:
        return CheckResult("temporal.connection", CHECK_FAIL, "empty host")
    try:
        port = int(port_str) if port_str else 7233
    except ValueError:
        return CheckResult("temporal.connection", CHECK_FAIL, f"invalid port: {port_str}")
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return CheckResult("temporal.connection", CHECK_OK, f"{host}:{port}")
    except OSError as exc:
        return CheckResult("temporal.connection", CHECK_WARN, f"{host}:{port} ({exc})")


def _check_aws_identity(region: str) -> CheckResult:
    try:
        sts = boto3.client("sts", region_name=region)
        ident = sts.get_caller_identity()
        arn = ident.get("Arn", "(unknown)")
        return CheckResult("aws.identity", CHECK_OK, arn)
    except (ClientError, BotoCoreError) as exc:
        return CheckResult("aws.identity", CHECK_FAIL, str(exc))
    except Exception as exc:
        return CheckResult("aws.identity", CHECK_FAIL, str(exc))


def _check_sqs(region: str, queue_name: str) -> CheckResult:
    if not queue_name:
        return CheckResult("aws.sqs", CHECK_WARN, "FLEET_SQS_NAME is not set")
    try:
        sqs = boto3.client("sqs", region_name=region)
        url = sqs.get_queue_url(QueueName=queue_name).get("QueueUrl", "")
        return CheckResult("aws.sqs", CHECK_OK, url or queue_name)
    except (ClientError, BotoCoreError) as exc:
        return CheckResult("aws.sqs", CHECK_FAIL, str(exc))
    except Exception as exc:
        return CheckResult("aws.sqs", CHECK_FAIL, str(exc))


def _check_ddb_table(region: str, table_name: str, label: str) -> CheckResult:
    if not table_name:
        return CheckResult(label, CHECK_WARN, "table name is not set")
    try:
        ddb = boto3.client("dynamodb", region_name=region)
        ddb.describe_table(TableName=table_name)
        return CheckResult(label, CHECK_OK, table_name)
    except (ClientError, BotoCoreError) as exc:
        return CheckResult(label, CHECK_FAIL, str(exc))
    except Exception as exc:
        return CheckResult(label, CHECK_FAIL, str(exc))


def _check_s3_bucket(region: str, bucket: str) -> CheckResult:
    if not bucket:
        return CheckResult("aws.s3", CHECK_WARN, "FLEET_S3_BUCKET is not set")
    try:
        s3 = boto3.client("s3", region_name=region)
        s3.head_bucket(Bucket=bucket)
        return CheckResult("aws.s3", CHECK_OK, bucket)
    except (ClientError, BotoCoreError) as exc:
        return CheckResult("aws.s3", CHECK_FAIL, str(exc))
    except Exception as exc:
        return CheckResult("aws.s3", CHECK_FAIL, str(exc))


def _check_bedrock_profile(region: str, label: str, model_id: str) -> CheckResult:
    if not model_id:
        return CheckResult(label, CHECK_WARN, "model id is not set")
    try:
        bedrock = boto3.client("bedrock", region_name=region)
        bedrock.get_inference_profile(inferenceProfileIdentifier=model_id)
        return CheckResult(label, CHECK_OK, model_id)
    except (ClientError, BotoCoreError) as exc:
        return CheckResult(label, CHECK_FAIL, str(exc))
    except Exception as exc:
        return CheckResult(label, CHECK_FAIL, str(exc))


def _print_check_results(results: list[CheckResult]) -> None:
    if not results:
        typer.echo("No checks executed.")
        return
    max_name = max(len(r.name) for r in results)
    typer.echo("")
    for r in results:
        name = r.name.ljust(max_name)
        typer.echo(f"[{r.status}] {name}  {r.detail}")
    typer.echo("")


def _check_exit_code(results: list[CheckResult]) -> int:
    return 1 if any(r.status == CHECK_FAIL for r in results) else 0


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Path to config.yaml (sets FLEET_LOCAL_CONFIG).",
    ),
    profile: str = typer.Option(
        "default",
        "--profile",
        help="Execution profile name (for future runtime selection).",
    ),
    runtime: str = typer.Option(
        "aws",
        "--runtime",
        help="Runtime backend: aws | temporal.",
    ),
) -> None:
    """Global options for all naval commands."""
    runtime_norm = runtime.strip().lower()
    root = _repo_root()
    resolved_config = _resolve_config(config, root)
    runtime_ctx = RuntimeContext(
        repo_root=root,
        config=resolved_config,
        profile=profile.strip() or "default",
        runtime=runtime_norm,
    )
    try:
        runtime_impl = make_runtime(runtime_ctx)
    except ValueError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(2)
    ctx.obj = {"runtime_ctx": runtime_ctx, "runtime_impl": runtime_impl}


@app.command("doctor")
def doctor(
    ctx: typer.Context,
    json_out: bool = typer.Option(False, "--json", help="Output machine-readable JSON."),
    invoke: bool = typer.Option(
        False,
        "--invoke",
        help="Invoke Bedrock models in addition to dry checks.",
    ),
) -> None:
    """Run environment checks."""
    runtime_ctx, runtime_impl = _ctx_state(ctx)
    results: list[CheckResult] = []

    temporal_addr = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    results.append(_check_temporal(temporal_addr))

    region = os.getenv("FLEET_REGION", "ap-northeast-1")
    sqs_name = os.getenv("FLEET_SQS_NAME", "")
    state_table = os.getenv("FLEET_STATE_TABLE", "")
    budget_table = os.getenv("FLEET_BUDGET_TABLE", "")
    task_table = os.getenv("FLEET_TASK_STATE_TABLE", "fleet-task-state")
    bucket = os.getenv("FLEET_S3_BUCKET", "")

    try:
        from fleet_config import resolve_fleet_config

        cfg = resolve_fleet_config()
        region = cfg.get("region") or region
        sqs_name = cfg.get("sqs_name") or sqs_name
        state_table = cfg.get("state_table") or state_table
        budget_table = cfg.get("budget_table") or budget_table
        bucket = cfg.get("bucket") or bucket
    except Exception:
        pass

    results.append(CheckResult("runtime.selected", CHECK_OK, runtime_ctx.runtime))
    results.append(CheckResult("aws.region", CHECK_OK, region))
    results.append(_check_aws_identity(region))
    results.append(_check_sqs(region, sqs_name))
    results.append(_check_ddb_table(region, state_table, "aws.ddb.state_table"))
    results.append(_check_ddb_table(region, budget_table, "aws.ddb.budget_table"))
    results.append(_check_ddb_table(region, task_table, "aws.ddb.task_table"))
    results.append(_check_s3_bucket(region, bucket))

    results.append(
        _check_bedrock_profile(
            region,
            "aws.bedrock.sonnet_profile",
            os.getenv("BEDROCK_SONNET_MODEL_ID", ""),
        )
    )
    results.append(
        _check_bedrock_profile(
            region,
            "aws.bedrock.opus_profile",
            os.getenv("BEDROCK_OPUS_MODEL_ID", ""),
        )
    )
    results.append(
        _check_bedrock_profile(
            region,
            "aws.bedrock.micro_profile",
            os.getenv("BEDROCK_MICRO_MODEL_ID", ""),
        )
    )
    results.append(
        _check_bedrock_profile(
            region,
            "aws.bedrock.lite_profile",
            os.getenv("BEDROCK_LITE_MODEL_ID", ""),
        )
    )

    if invoke:
        typer.echo("Running additional Bedrock invoke probe...")
        args: list[str] = ["--invoke"]
        try:
            runtime_impl.run_script("bedrock_check.py", args)
            results.append(CheckResult("aws.bedrock.invoke_probe", CHECK_OK, "bedrock_check.py --invoke"))
        except typer.Exit as exc:
            code = int(exc.exit_code)
            if code == 0:
                results.append(CheckResult("aws.bedrock.invoke_probe", CHECK_OK, "bedrock_check.py --invoke"))
            else:
                results.append(CheckResult("aws.bedrock.invoke_probe", CHECK_FAIL, f"exit_code={code}"))

    if json_out:
        payload = {
            "runtime": runtime_ctx.runtime,
            "profile": runtime_ctx.profile,
            "results": [r.__dict__ for r in results],
        }
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_check_results(results)

    raise typer.Exit(_check_exit_code(results))


@app.command("enqueue")
def enqueue(
    ctx: typer.Context,
    doctrine: str = typer.Option(
        "standard_full",
        "--doctrine",
        help="Formation/doctrine name.",
    ),
    ticket: str = typer.Option(
        ...,
        "--ticket",
        help="Ticket text or ticket file path.",
    ),
    budget: Optional[str] = typer.Option(
        None,
        "--budget",
        help="Budget month in YYYY-MM for scaffold mode.",
    ),
    repo_url: str = typer.Option("", "--repo-url", help="Git repository URL."),
    task_id: str = typer.Option("", "--task-id", help="Optional task id."),
    watch: bool = typer.Option(
        False,
        "--watch/--no-watch",
        help="Follow task loop after start.",
    ),
    need_approval: bool = typer.Option(
        False,
        "--need-approval",
        help="Require approval signal before execution (temporal runtime).",
    ),
    need_input: bool = typer.Option(
        False,
        "--need-input",
        help="Require input signal before execution (temporal runtime).",
    ),
    hitl_timeout_hours: int = typer.Option(
        72,
        "--hitl-timeout-hours",
        min=1,
        help="Timeout for HITL wait in hours.",
    ),
    hitl_timeout_action: str = typer.Option(
        "failed",
        "--hitl-timeout-action",
        help="Action on HITL timeout: failed | cancelled.",
    ),
) -> None:
    """Enqueue a mission."""
    _, runtime_impl = _ctx_state(ctx)
    if need_approval and need_input:
        typer.echo("[ERROR] choose only one of --need-approval or --need-input", err=True)
        raise typer.Exit(2)
    mode = "approval" if need_approval else ("input" if need_input else "")
    timeout_action = hitl_timeout_action.strip().lower()
    if timeout_action not in {"failed", "cancelled"}:
        typer.echo("[ERROR] --hitl-timeout-action must be 'failed' or 'cancelled'", err=True)
        raise typer.Exit(2)
    runtime_impl.enqueue(
        doctrine=doctrine,
        ticket=ticket,
        budget=budget,
        repo_url=repo_url,
        task_id=task_id,
        watch=watch,
        hitl_mode=mode,
        hitl_timeout_hours=hitl_timeout_hours,
        hitl_timeout_action=timeout_action,
    )


@app.command("status")
def status(
    ctx: typer.Context,
    mission: str = typer.Option(
        "",
        "--mission",
        help="Mission/task id for detailed status.",
    ),
    month: Optional[str] = typer.Option(
        None,
        "--month",
        help="Budget month YYYY-MM for summary mode.",
    ),
) -> None:
    """Show mission status."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.status(mission=mission, month=month)


@app.command("tail")
def tail(
    ctx: typer.Context,
    mission: str = typer.Option("", "--mission", help="Mission id filter."),
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Number of rows."),
    follow: bool = typer.Option(False, "--follow", help="Follow new logs continuously."),
) -> None:
    """Show recent logs."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.tail(mission=mission, limit=limit, follow=follow)


@app.command("approve")
def approve(
    ctx: typer.Context,
    mission: str = typer.Option(..., "--mission", help="Mission id."),
    yes: bool = typer.Option(False, "--yes", help="Approve mission."),
    no: bool = typer.Option(False, "--no", help="Reject mission."),
    note: str = typer.Option("", "--note", help="Optional note."),
) -> None:
    """Send approval signal."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.approve(mission=mission, yes=yes, no=no, note=note)


@app.command("input")
def input_(
    ctx: typer.Context,
    mission: str = typer.Option(..., "--mission", help="Mission id."),
    message: str = typer.Argument(..., help="Message for blocked mission."),
) -> None:
    """Send input signal."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.input(mission=mission, message=message)


@app.command("pull")
def pull(
    ctx: typer.Context,
    mission: str = typer.Option(..., "--mission", help="Mission id."),
    out: str = typer.Option("", "--out", help="Output directory."),
) -> None:
    """Pull mission artifacts."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.pull(mission=mission, out=out)


@app.command("watch")
def watch(
    ctx: typer.Context,
    month: Optional[str] = typer.Option(
        None,
        "--month",
        help="Budget month YYYY-MM for monitor mode.",
    ),
    interval: int = typer.Option(2, "--interval", min=1, help="Refresh interval (seconds)."),
    mission_filter: str = typer.Option("", "--filter", help="Mission id/state filter for temporal watch."),
    page_size: int = typer.Option(20, "--page-size", min=1, max=200, help="Missions per page."),
    page: int = typer.Option(1, "--page", min=1, help="Page number (1-based)."),
) -> None:
    """Watch fleet status."""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.watch(
        month=month,
        interval=interval,
        mission_filter=mission_filter,
        page_size=page_size,
        page=page,
    )


@app.command("up")
def up(
    ctx: typer.Context,
    profile: str = typer.Option("local", "--profile", help="local|aws"),
    region: str = typer.Option("ap-northeast-1", "--region", help="AWS region for --profile aws."),
    name_prefix: str = typer.Option("naval-vnext", "--name-prefix", help="Resource name prefix for --profile aws."),
    apply: bool = typer.Option(True, "--apply/--dry-run", help="Apply bootstrap changes."),
) -> None:
    """Bootstrap environment."""
    runtime_ctx, _ = _ctx_state(ctx)
    profile_norm = profile.strip().lower()
    if profile_norm == "local":
        result = bootstrap_local(runtime_ctx.repo_root, apply=apply)
        if result.created_files:
            typer.echo("Created:")
            for p in result.created_files:
                typer.echo(f"- {p}")
        else:
            typer.echo("Bootstrap files already exist.")
        typer.echo(result.message)
        if result.started:
            temporal_addr = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
            chk = _check_temporal(temporal_addr)
            typer.echo(f"temporal.connection={chk.status} {chk.detail}")
        return

    if profile_norm == "aws":
        result = bootstrap_aws(
            runtime_ctx.repo_root,
            apply=apply,
            region=region,
            name_prefix=name_prefix,
        )
        if result.created_files:
            typer.echo("Created:")
            for p in result.created_files:
                typer.echo(f"- {p}")
        else:
            typer.echo("Bootstrap files already exist.")
        typer.echo(result.message)
        if result.applied:
            chk = _check_aws_identity(region)
            typer.echo(f"aws.identity={chk.status} {chk.detail}")
            if result.env_file.exists():
                typer.echo(f"env_file={result.env_file}")
        return

    typer.echo("[ERROR] --profile must be local or aws", err=True)
    raise typer.Exit(2)


@app.command("pending")
def pending(ctx: typer.Context) -> None:
    """NEED_INPUT / NEED_APPROVAL なミッションを一覧表示して対話応答する。"""
    _, runtime_impl = _ctx_state(ctx)
    runtime_impl.pending()


@app.command("ca")
def ca(
    ctx: typer.Context,
    task_id: str = typer.Argument(..., help="Task id (e.g. T-0001)."),
    directive: str = typer.Option(
        "",
        "--directive",
        help="CA directive text. Omit to enter interactively.",
    ),
    repo_url: str = typer.Option("", "--repo-url", help="Git repository URL."),
    no_advance: bool = typer.Option(
        False,
        "--no-advance",
        help="Skip auto-advance after saving directive.",
    ),
) -> None:
    """Send a CA directive to an in-progress task."""
    _, runtime_impl = _ctx_state(ctx)

    if not directive.strip():
        typer.echo(
            "Enter CA directive (Ctrl+D / Ctrl+Z to finish).\n"
            "Example:\n"
            "  Execute: DD_SONNET, DD_MISTRAL\n"
            "  Skip: BB\n"
            "  Done: no\n"
            "  Confidence: 75"
        )
        lines: list[str] = []
        try:
            while True:
                lines.append(input())
        except EOFError:
            pass
        directive = "\n".join(lines).strip()

    if not directive:
        typer.echo("[ERROR] no directive provided", err=True)
        raise typer.Exit(2)

    runtime_impl.ca(
        task_id=task_id,
        directive=directive,
        repo_url=repo_url,
        auto_advance=not no_advance,
    )


# ------------------------------------------------------------------ #
# ws サブコマンド群                                                    #
# ------------------------------------------------------------------ #

ws_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Local workspace management (navalctl wrapper).",
)
app.add_typer(ws_app, name="ws")


def _ws_runtime(ctx: typer.Context) -> "RuntimeBase":
    _, runtime_impl = _ctx_state(ctx)
    return runtime_impl


@ws_app.command("init")
def ws_init(ctx: typer.Context) -> None:
    """Initialize local workspace and SQLite DB."""
    _ws_runtime(ctx).ws_init()


@ws_app.command("mkws")
def ws_mkws(
    ctx: typer.Context,
    mission_id: str = typer.Option(..., "--mission-id", help="Mission id."),
    task_id: str = typer.Option(..., "--task-id", help="Task id."),
    ship_class: str = typer.Option(..., "--ship-class", help="Ship class (CVL/DD/CL/CA/BB)."),
    ship_id: str = typer.Option("", "--ship-id", help="Optional ship instance id."),
) -> None:
    """Create local workspace directory for a mission."""
    _ws_runtime(ctx).ws_mkws(
        mission_id=mission_id,
        task_id=task_id,
        ship_class=ship_class,
        ship_id=ship_id,
    )


@ws_app.command("mental")
def ws_mental(
    ctx: typer.Context,
    ship_class: str = typer.Option(..., "--ship-class", help="Ship class."),
) -> None:
    """Show mental model for a ship class."""
    _ws_runtime(ctx).ws_mental(ship_class=ship_class)


@ws_app.command("search")
def ws_search(
    ctx: typer.Context,
    query: str = typer.Option("", "--query", help="Free-text search query."),
    mission_id: str = typer.Option("", "--mission-id", help="Filter by mission id."),
    task_id: str = typer.Option("", "--task-id", help="Filter by task id."),
    ship_class: str = typer.Option("", "--ship-class", help="Filter by ship class."),
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Max results."),
) -> None:
    """Search local comm logs."""
    _ws_runtime(ctx).ws_search(
        query=query,
        mission_id=mission_id,
        task_id=task_id,
        ship_class=ship_class,
        limit=limit,
    )


@ws_app.command("tail")
def ws_tail(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", min=1, max=1000, help="Number of recent rows."),
) -> None:
    """Show recent local comm logs."""
    _ws_runtime(ctx).ws_tail(limit=limit)


@ws_app.command("info")
def ws_info(ctx: typer.Context) -> None:
    """Show local config and DB path."""
    _ws_runtime(ctx).ws_info()


def run() -> None:
    app()

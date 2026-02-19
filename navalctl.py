#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from fleet_local import (
    ensure_local_workspace,
    format_mental_model,
    get_local_db_path,
    get_ship_profile,
    init_local_db,
    load_local_config,
    search_comms,
)


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def cmd_init(args: argparse.Namespace) -> None:
    cfg = load_local_config()
    db_path = init_local_db()
    print("Local config loaded.")
    print(f"  root: {cfg['local']['root']}")
    print(f"  db: {db_path}")


def cmd_mkws(args: argparse.Namespace) -> None:
    paths = ensure_local_workspace(args.mission_id, args.task_id, args.ship_class, args.ship_id)
    print("Workspace created:")
    for k, v in paths.items():
        print(f"  {k}: {v}")


def cmd_mental(args: argparse.Namespace) -> None:
    profile = get_ship_profile(args.ship_class)
    if not profile:
        print(f"No profile found for ship_class={args.ship_class}")
        return
    print(format_mental_model(args.ship_class, profile))


def cmd_search(args: argparse.Namespace) -> None:
    results = search_comms(
        query=args.query or "",
        mission_id=args.mission_id or "",
        task_id=args.task_id or "",
        ship_class=args.ship_class or "",
        limit=args.limit,
    )
    if not results:
        print("No results.")
        return
    for r in results:
        print(
            f"[{_fmt_ts(r['ts'])}] {r['mission_id']} {r['ship_class']} "
            f"{r['from_role']}->{r['to_role']} ({r['comm_type']})"
        )
        print(r["content"])
        print("-" * 60)


def cmd_tail(args: argparse.Namespace) -> None:
    results = search_comms(limit=args.limit)
    if not results:
        print("No logs.")
        return
    for r in results:
        print(
            f"[{_fmt_ts(r['ts'])}] {r['mission_id']} {r['ship_class']} "
            f"{r['from_role']}->{r['to_role']} ({r['comm_type']})"
        )
        print(r["content"])
        print("-" * 60)


def cmd_info(args: argparse.Namespace) -> None:
    cfg = load_local_config()
    info = {
        "config_version": cfg.get("version"),
        "db_path": str(get_local_db_path()),
        "ships": list(cfg.get("ships", {}).keys()),
    }
    print(json.dumps(info, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="navalctl", description="Local fleet helper CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Initialize local workspace and DB")
    p_init.set_defaults(func=cmd_init)

    p_mkws = sub.add_parser("mkws", help="Create local workspace for a mission")
    p_mkws.add_argument("--mission-id", required=True)
    p_mkws.add_argument("--task-id", required=True)
    p_mkws.add_argument("--ship-class", required=True)
    p_mkws.add_argument("--ship-id", default="")
    p_mkws.set_defaults(func=cmd_mkws)

    p_mental = sub.add_parser("mental", help="Show mental model for ship class")
    p_mental.add_argument("--ship-class", required=True)
    p_mental.set_defaults(func=cmd_mental)

    p_search = sub.add_parser("search", help="Search local comm logs")
    p_search.add_argument("--query", default="")
    p_search.add_argument("--mission-id", default="")
    p_search.add_argument("--task-id", default="")
    p_search.add_argument("--ship-class", default="")
    p_search.add_argument("--limit", type=int, default=50)
    p_search.set_defaults(func=cmd_search)

    p_tail = sub.add_parser("tail", help="Show recent logs")
    p_tail.add_argument("--limit", type=int, default=20)
    p_tail.set_defaults(func=cmd_tail)

    p_info = sub.add_parser("info", help="Show local config info")
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

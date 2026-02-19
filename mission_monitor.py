#!/usr/bin/env python3
"""
Mission Monitor - Real-time monitoring dashboard for fleet operations
Shows mission status, stuck missions, and budget consumption
"""
import os
import sys
import time
import boto3
from datetime import datetime, timezone
from collections import defaultdict

REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
MISSION_STATE_TABLE = os.getenv("FLEET_STATE_TABLE", "fleet-mission-state")
TASK_STATE_TABLE = os.getenv("FLEET_TASK_STATE_TABLE", "fleet-task-state")
BUDGET_TABLE = os.getenv("FLEET_BUDGET_TABLE", "fleet-budget")
try:
    CONF_WARN_THRESHOLD = int(os.getenv("FLEET_CONF_WARN", "40"))
    CONF_CRIT_THRESHOLD = int(os.getenv("FLEET_CONF_CRIT", "25"))
except ValueError:
    CONF_WARN_THRESHOLD = 40
    CONF_CRIT_THRESHOLD = 25

dynamodb = boto3.client("dynamodb", region_name=REGION)


def now() -> int:
    return int(time.time())


def format_timestamp(ts: int) -> str:
    """Format unix timestamp to human readable"""
    try:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ts)


def get_budget_status(month: str) -> dict:
    """Get budget status for given month"""
    try:
        resp = dynamodb.get_item(
            TableName=BUDGET_TABLE,
            Key={"month": {"S": month}}
        )
        item = resp.get("Item", {})
        if not item:
            return {"error": "Budget not found"}
        
        return {
            "fuel_cap": float(item.get("fuel_cap_usd", {}).get("N", 0)),
            "fuel_remaining": float(item.get("fuel_remaining_usd", {}).get("N", 0)),
            "fuel_burned": float(item.get("fuel_burned_usd", {}).get("N", 0)),
            "sorties_total": int(item.get("sorties_total", {}).get("N", 0)),
            "ammo_used": {
                "BB": int(item.get("ammo_used", {}).get("M", {}).get("BB_main_gun", {}).get("N", 0)),
                "CA": int(item.get("ammo_used", {}).get("M", {}).get("CA_salvo", {}).get("N", 0)),
                "CVB": int(item.get("ammo_used", {}).get("M", {}).get("CVB_air_wing", {}).get("N", 0)),
            },
            "ammo_caps": {
                "BB": int(item.get("ammo_caps", {}).get("M", {}).get("BB_main_gun", {}).get("N", 0)),
                "CA": int(item.get("ammo_caps", {}).get("M", {}).get("CA_salvo", {}).get("N", 0)),
                "CVB": int(item.get("ammo_caps", {}).get("M", {}).get("CVB_air_wing", {}).get("N", 0)),
            }
        }
    except Exception as e:
        return {"error": str(e)}


def scan_missions_by_status(status: str) -> list:
    """Scan missions by status"""
    try:
        resp = dynamodb.scan(
            TableName=MISSION_STATE_TABLE,
            FilterExpression="#s = :status",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":status": {"S": status}}
        )
        items = resp.get("Items", [])
        missions = []
        for item in items:
            missions.append({
                "mission_id": item.get("mission_id", {}).get("S", ""),
                "task_id": item.get("task_id", {}).get("S", ""),
                "ship_class": item.get("ship_class", {}).get("S", ""),
                "status": item.get("status", {}).get("S", ""),
                "updated_at": int(item.get("updated_at", {}).get("N", 0)),
                "lock_until": int(item.get("lock_until", {}).get("N", 0)) if item.get("lock_until") else None,
                "owner": item.get("owner", {}).get("S", ""),
                "confidence_level": int(float(item.get("confidence_level", {}).get("N", 0))) if item.get("confidence_level") else None,
            })
        return missions
    except Exception as e:
        print(f"Error scanning missions: {e}")
        return []


def scan_all_tasks() -> list:
    """Scan all tasks"""
    try:
        resp = dynamodb.scan(TableName=TASK_STATE_TABLE)
        items = resp.get("Items", [])
        tasks = []
        for item in items:
            tasks.append({
                "task_id": item.get("task_id", {}).get("S", ""),
                "status": item.get("status", {}).get("S", ""),
                "current_stage": item.get("current_stage", {}).get("S", ""),
                "updated_at": int(item.get("updated_at", {}).get("N", 0))
            })
        return tasks
    except Exception as e:
        print(f"Error scanning tasks: {e}")
        return []


def detect_stuck_missions() -> list:
    """Detect missions with expired locks"""
    running = scan_missions_by_status("RUNNING")
    current_time = now()
    stuck = []
    
    for mission in running:
        lock_until = mission.get("lock_until")
        if lock_until and lock_until < current_time:
            mission["stuck_duration"] = current_time - lock_until
            stuck.append(mission)
    
    return stuck


def print_budget_status(month: str) -> None:
    """Print budget status"""
    print(f"=== Budget Status ({month}) ===")
    budget = get_budget_status(month)
    
    if "error" in budget:
        print(f"Error: {budget['error']}")
        return
    
    fuel_pct = (budget["fuel_burned"] / budget["fuel_cap"] * 100) if budget["fuel_cap"] > 0 else 0
    print(f"Fuel: ${budget['fuel_burned']:.2f} / ${budget['fuel_cap']:.2f} ({fuel_pct:.1f}%)")
    print(f"Remaining: ${budget['fuel_remaining']:.2f}")
    print(f"Total Sorties: {budget['sorties_total']}")
    print()
    print("Ammo:")
    for ship, used in budget["ammo_used"].items():
        cap = budget["ammo_caps"][ship]
        pct = (used / cap * 100) if cap > 0 else 0
        print(f"  {ship}: {used}/{cap} ({pct:.0f}%)")
    print()


def print_mission_summary() -> None:
    """Print mission summary by status"""
    print("=== Mission Summary ===")
    statuses = ["RUNNING", "NEED_INPUT", "NEED_APPROVAL", "ENQUEUED", "DONE", "FAILED", "BUDGET_DENIED"]
    
    summary = {}
    for status in statuses:
        missions = scan_missions_by_status(status)
        summary[status] = len(missions)
        
        if status in ["RUNNING", "NEED_INPUT", "NEED_APPROVAL", "ENQUEUED"] and missions:
            print(f"\n{status}: {len(missions)}")
            for m in missions[:5]:  # Show first 5
                age = now() - m["updated_at"]
                age_str = f"{age//60}m" if age < 3600 else f"{age//3600}h"
                conf = m.get("confidence_level")
                conf_str = f", conf={conf}" if conf is not None else ""
                print(f"  [{m['ship_class']}] {m['mission_id'][:30]}... (updated {age_str} ago{conf_str})")
            if len(missions) > 5:
                print(f"  ... and {len(missions)-5} more")
    
    print(f"\nTotal: {sum(summary.values())} missions")
    print()


def collect_low_confidence(warn: int, crit: int) -> list:
    statuses = ["RUNNING", "NEED_INPUT", "NEED_APPROVAL", "ENQUEUED", "DONE", "FAILED", "BUDGET_DENIED"]
    lows = []
    for status in statuses:
        missions = scan_missions_by_status(status)
        for m in missions:
            conf = m.get("confidence_level")
            if conf is None:
                continue
            if conf <= warn:
                m["severity"] = "CRIT" if conf <= crit else "WARN"
                lows.append(m)
    lows.sort(key=lambda x: (x.get("confidence_level", 101), x.get("updated_at", 0)))
    return lows


def print_low_confidence(warn: int, crit: int, limit: int = 10) -> None:
    print(f"=== Low Confidence (<= {warn}, crit <= {crit}) ===")
    lows = collect_low_confidence(warn, crit)
    if not lows:
        print("No low-confidence missions detected")
        print()
        return
    for m in lows[:limit]:
        conf = m.get("confidence_level")
        sev = m.get("severity", "WARN")
        age = now() - m["updated_at"]
        age_str = f"{age//60}m" if age < 3600 else f"{age//3600}h"
        print(f"[{m['ship_class']}] {m['mission_id']} status={m['status']} conf={conf} {sev} (updated {age_str} ago)")
    if len(lows) > limit:
        print(f"... and {len(lows)-limit} more")
    print()


def print_stuck_missions() -> None:
    """Print stuck missions"""
    stuck = detect_stuck_missions()
    
    if not stuck:
        print("=== Stuck Missions ===")
        print("No stuck missions detected")
        print()
        return
    
    print(f"=== Stuck Missions ({len(stuck)}) ===")
    for m in stuck:
        duration = m["stuck_duration"]
        duration_str = f"{duration//60}m" if duration < 3600 else f"{duration//3600}h"
        print(f"[{m['ship_class']}] {m['mission_id']}")
        print(f"  Owner: {m['owner']}")
        print(f"  Stuck for: {duration_str}")
        print(f"  Updated: {format_timestamp(m['updated_at'])}")
    print()


def print_task_summary() -> None:
    """Print task summary"""
    print("=== Task Summary ===")
    tasks = scan_all_tasks()
    
    if not tasks:
        print("No tasks found")
        print()
        return
    
    by_status = defaultdict(list)
    for task in tasks:
        by_status[task["status"]].append(task)
    
    for status in ["RUNNING", "NEED_INPUT", "NEED_APPROVAL", "ENQUEUED", "DONE", "FAILED"]:
        if status in by_status:
            print(f"\n{status}: {len(by_status[status])}")
            for t in by_status[status][:5]:
                age = now() - t["updated_at"]
                age_str = f"{age//60}m" if age < 3600 else f"{age//3600}h"
                stage = t.get("current_stage", "?")
                print(f"  {t['task_id']} @ {stage} (updated {age_str} ago)")
    
    print(f"\nTotal: {len(tasks)} tasks")
    print()


def monitor_once(month: str) -> None:
    """Run monitoring once"""
    print("\n" + "="*60)
    print(f"Fleet Monitor - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*60 + "\n")
    
    print_budget_status(month)
    print_task_summary()
    print_mission_summary()
    print_low_confidence(CONF_WARN_THRESHOLD, CONF_CRIT_THRESHOLD)
    print_stuck_missions()


def monitor_loop(month: str, interval: int = 30) -> None:
    """Run monitoring in loop"""
    try:
        while True:
            monitor_once(month)
            print(f"Refreshing in {interval}s... (Ctrl+C to stop)")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python mission_monitor.py <budget_month>")
        print("  python mission_monitor.py <budget_month> watch [interval_sec]")
        print()
        print("Examples:")
        print("  python mission_monitor.py 2026-02")
        print("  python mission_monitor.py 2026-02 watch 30")
        sys.exit(1)
    
    month = sys.argv[1]
    
    if len(sys.argv) > 2 and sys.argv[2] == "watch":
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        monitor_loop(month, interval)
    else:
        monitor_once(month)


if __name__ == "__main__":
    main()

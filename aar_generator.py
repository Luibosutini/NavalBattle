#!/usr/bin/env python3
"""
AAR Generator - After Action Report generation
Separates Facts (events/costs/artifacts) from Style (ship-class narrative)
"""
import os
import sys
import json
import boto3
from datetime import datetime

REGION = os.getenv("FLEET_REGION", "ap-northeast-1")
STATE_TABLE = os.getenv("FLEET_STATE_TABLE", "fleet-mission-state")
BUCKET = os.getenv("FLEET_S3_BUCKET", "")

dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# Ship-class narrative styles
SHIP_STYLES = {
    "CVL": {
        "title": "偵察報告",
        "tone": "簡潔・客観的",
        "template": """## 偵察任務報告 - {task_id}

**艦種**: 軽空母 (CVL)  
**任務ID**: {mission_id}  
**状態**: {status}  
**燃料消費**: ${fuel_cost:.2f}

### 偵察結果
{summary}

### 収集情報
{artifacts}

### 作戦経過
{events}

---
*偵察完了 - {timestamp}*
"""
    },
    "DD": {
        "title": "実装報告",
        "tone": "実務的・簡潔",
        "template": """## 実装任務報告 - {task_id}

**艦種**: 駆逐艦 (DD)  
**任務ID**: {mission_id}  
**状態**: {status}  
**燃料消費**: ${fuel_cost:.2f}

### 実装内容
{summary}

### 成果物
{artifacts}

### 作業ログ
{events}

---
*実装完了 - {timestamp}*
"""
    },
    "CL": {
        "title": "試験報告",
        "tone": "検証重視",
        "template": """## 試験任務報告 - {task_id}

**艦種**: 軽巡洋艦 (CL)  
**任務ID**: {mission_id}  
**状態**: {status}  
**燃料消費**: ${fuel_cost:.2f}

### 試験結果
{summary}

### 検証項目
{artifacts}

### 試験経過
{events}

---
*試験完了 - {timestamp}*
"""
    },
    "CVB": {
        "title": "検証作戦報告",
        "tone": "航空作戦・組織的",
        "template": """## 検証作戦報告 - {task_id}

**艦種**: 装甲空母 (CVB)  
**任務ID**: {mission_id}  
**状態**: {status}  
**弾薬消費**: {ammo_cost} sorties  
**燃料消費**: ${fuel_cost:.2f}

### 作戦概要
{summary}

### 航空隊戦果
{artifacts}

### 作戦経過
{events}

---
*作戦終了 - {timestamp}*
"""
    },
    "CA": {
        "title": "統合作戦報告",
        "tone": "戦術的・指揮官視点",
        "template": """## 統合作戦報告 - {task_id}

**艦種**: 重巡洋艦 (CA)  
**任務ID**: {mission_id}  
**状態**: {status}  
**弾薬消費**: {ammo_cost} salvos  
**燃料消費**: ${fuel_cost:.2f}

### 作戦評価
{summary}

### 戦果
{artifacts}

### 作戦経過
{events}

---
*作戦終了 - {timestamp}*
"""
    },
    "BB": {
        "title": "決戦報告",
        "tone": "戦略的・重厚",
        "template": """## 決戦報告 - {task_id}

**艦種**: 戦艦 (BB)  
**任務ID**: {mission_id}  
**状態**: {status}  
**主砲弾薬**: {ammo_cost} rounds  
**燃料消費**: ${fuel_cost:.2f}

### 戦略的成果
{summary}

### 決戦戦果
{artifacts}

### 戦闘経過
{events}

---
*決戦終了 - {timestamp}*
"""
    }
}

def collect_facts(mission_id):
    """Collect all facts from DynamoDB and S3"""
    facts = {
        "mission_id": mission_id,
        "status": "UNKNOWN",
        "ship_class": "UNKNOWN",
        "task_id": "UNKNOWN",
        "fuel_cost": 0.0,
        "ammo_cost": 0,
        "orders": {},
        "events": [],
        "artifacts": [],
        "comms": []
    }
    
    # Get DynamoDB state
    try:
        resp = dynamodb.get_item(
            TableName=STATE_TABLE,
            Key={"mission_id": {"S": mission_id}}
        )
        if "Item" in resp:
            item = resp["Item"]
            facts["status"] = item.get("status", {}).get("S", "UNKNOWN")
            facts["ship_class"] = item.get("ship_class", {}).get("S", "UNKNOWN")
            facts["task_id"] = item.get("task_id", {}).get("S", "UNKNOWN")
            facts["fuel_cost"] = float(item.get("total_cost", {}).get("N", "0"))
            facts["ammo_cost"] = int(item.get("ammo_used", {}).get("N", "0"))
    except Exception as e:
        print(f"Warning: Could not read DynamoDB: {e}")
    
    # Get mission.json
    try:
        key = f"missions/{mission_id}/mission.json"
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        mission_json = json.loads(obj["Body"].read().decode("utf-8"))
        facts["orders"] = mission_json.get("orders", {})
    except Exception as e:
        print(f"Warning: Could not read mission.json: {e}")

    # Get original payload (preferred for orders)
    try:
        key = f"missions/{mission_id}/orders/payload.json"
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        if isinstance(payload, dict):
            facts["orders"] = payload
    except Exception as e:
        print(f"Warning: Could not read orders/payload.json: {e}")
    
    # Get events
    try:
        prefix = f"missions/{mission_id}/events/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            event = json.loads(data["Body"].read().decode("utf-8"))
            facts["events"].append(event)
        facts["events"].sort(key=lambda x: x.get("ts", 0))
    except Exception as e:
        print(f"Warning: Could not read events: {e}")
    
    # Get artifacts list
    try:
        prefix = f"missions/{mission_id}/artifacts/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            artifact_name = obj["Key"].split("/")[-1]
            if artifact_name:
                facts["artifacts"].append(artifact_name)
    except Exception as e:
        print(f"Warning: Could not list artifacts: {e}")
    
    # Get comms
    try:
        prefix = f"missions/{mission_id}/comms/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            comm = json.loads(data["Body"].read().decode("utf-8"))
            facts["comms"].append(comm)
        facts["comms"].sort(key=lambda x: x.get("ts", 0))
    except Exception as e:
        print(f"Warning: Could not read comms: {e}")
    
    return facts

def format_summary(facts):
    """Generate summary from orders and final status"""
    orders = facts.get("orders") or {}
    ticket = orders.get("ticket") or orders.get("ticket_s3")
    if not ticket:
        inputs = orders.get("inputs", {})
        ticket = inputs.get("ticket_s3")
    if not ticket:
        ticket = "No orders"
    status = facts["status"]
    
    if status == "DONE":
        return f"任務完了: {ticket}"
    elif status == "FAILED":
        return f"任務失敗: {ticket}"
    elif status in ["NEED_INPUT", "NEED_APPROVAL"]:
        return f"任務保留中: {ticket}"
    else:
        return f"任務進行中: {ticket}"

def format_artifacts(facts):
    """Format artifacts list"""
    if not facts["artifacts"]:
        return "成果物なし"
    
    lines = []
    for artifact in facts["artifacts"]:
        lines.append(f"- {artifact}")
    return "\n".join(lines)

def format_events(facts):
    """Format events timeline"""
    if not facts["events"]:
        return "イベント記録なし"
    
    lines = []
    for event in facts["events"][-10:]:  # Last 10 events
        ts_value = event.get("ts", "")
        if isinstance(ts_value, (int, float)) or (isinstance(ts_value, str) and ts_value.isdigit()):
            try:
                ts = datetime.utcfromtimestamp(int(ts_value)).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                ts = str(ts_value)[:19]
        else:
            ts = str(ts_value)[:19]
        event_type = event.get("event_type", "UNKNOWN")
        detail = event.get("detail") or event.get("data", "")
        if isinstance(detail, dict):
            detail = json.dumps(detail, ensure_ascii=False)
        lines.append(f"- `{ts}` [{event_type}] {detail}")
    
    if len(facts["events"]) > 10:
        lines.insert(0, f"*(最新10件を表示、全{len(facts['events'])}件)*\n")
    
    return "\n".join(lines)

def generate_aar(mission_id):
    """Generate AAR markdown from facts and style"""
    print(f"Collecting facts for {mission_id}...")
    facts = collect_facts(mission_id)
    
    ship_class = facts["ship_class"]
    style = SHIP_STYLES.get(ship_class)
    
    if not style:
        print(f"Warning: Unknown ship class {ship_class}, using default")
        style = SHIP_STYLES["DD"]
    
    print(f"Applying {ship_class} style...")
    
    # Format sections
    summary = format_summary(facts)
    artifacts = format_artifacts(facts)
    events = format_events(facts)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Apply template
    aar_content = style["template"].format(
        task_id=facts["task_id"],
        mission_id=facts["mission_id"],
        status=facts["status"],
        fuel_cost=facts["fuel_cost"],
        ammo_cost=facts["ammo_cost"],
        summary=summary,
        artifacts=artifacts,
        events=events,
        timestamp=timestamp
    )
    
    return aar_content

def save_aar(mission_id, content):
    """Save AAR to S3"""
    try:
        key = f"missions/{mission_id}/aar.md"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/markdown"
        )
        print(f"✓ AAR saved to s3://{BUCKET}/{key}")
        return True
    except Exception as e:
        print(f"Error saving AAR: {e}")
        return False

def main():
    if not BUCKET:
        print("Error: FLEET_S3_BUCKET not set")
        sys.exit(1)
    
    if len(sys.argv) < 2:
        print("Usage: python aar_generator.py <mission_id>")
        sys.exit(1)
    
    mission_id = sys.argv[1]
    
    print(f"=== AAR Generator ===")
    print(f"Mission: {mission_id}\n")
    
    aar_content = generate_aar(mission_id)
    
    print("\n" + "="*60)
    print(aar_content)
    print("="*60 + "\n")
    
    save_aar(mission_id, aar_content)

if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.service import (
    FleetService,
    FleetServiceError,
    MissionNotFound,
    TaskAlreadyFinished,
    mission_item_to_summary,
    state_rank,
)


class FakeDdb:
    """最小限の DynamoDB クライアントスタブ。"""

    def __init__(self, missions=None, tasks=None, budgets=None):
        self.missions = missions or {}
        self.tasks = tasks or {}
        self.budgets = budgets or {}
        self.updates = []

    def get_item(self, TableName, Key):
        if "mission_id" in Key:
            item = self.missions.get(Key["mission_id"]["S"])
        elif "task_id" in Key:
            item = self.tasks.get(Key["task_id"]["S"])
        else:
            item = self.budgets.get(Key["month"]["S"])
        return {"Item": item} if item else {}

    def scan(self, TableName, **kwargs):
        items = list(self.missions.values())
        filt = kwargs.get("FilterExpression", "")
        if "IN (:input, :approval)" in filt:
            items = [
                i for i in items
                if i.get("status", {}).get("S") in ("NEED_INPUT", "NEED_APPROVAL")
            ]
        return {"Items": items}

    def update_item(self, TableName, Key, **kwargs):
        self.updates.append((TableName, Key, kwargs))
        key_name = "mission_id" if "mission_id" in Key else "task_id"
        store = self.missions if key_name == "mission_id" else self.tasks
        item = store.get(Key[key_name]["S"])
        if item is not None:
            values = kwargs.get("ExpressionAttributeValues", {})
            for ref in (":new_status", ":cancelled", ":running"):
                if ref in values:
                    item["status"] = values[ref]
        return {}


class FakeSqs:
    def __init__(self):
        self.sent = []

    def get_queue_url(self, QueueName):
        return {"QueueUrl": f"https://sqs/{QueueName}"}

    def send_message(self, QueueUrl, MessageBody):
        self.sent.append((QueueUrl, MessageBody))
        return {}


class FakeS3:
    def __init__(self, objects=None):
        self.objects = objects or {}
        self.put = []

    def list_objects_v2(self, Bucket, Prefix, **kwargs):
        contents = [
            {"Key": k, "Size": len(v)}
            for k, v in sorted(self.objects.items())
            if k.startswith(Prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, Bucket, Key):
        import io
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.put.append(Key)
        self.objects[Key] = Body
        return {}


def _mission(mid, status, ship="DD", task="T-1", updated=100, conf=None):
    item = {
        "mission_id": {"S": mid},
        "status": {"S": status},
        "ship_class": {"S": ship},
        "task_id": {"S": task},
        "updated_at": {"N": str(updated)},
    }
    if conf is not None:
        item["confidence_level"] = {"N": str(conf)}
    return item


def make_service(ddb=None, sqs=None, s3=None, bucket="test-bucket"):
    env = {
        "FLEET_REGION": "ap-northeast-1",
        "FLEET_STATE_TABLE": "ms",
        "FLEET_TASK_STATE_TABLE": "ts",
        "FLEET_BUDGET_TABLE": "bg",
        "FLEET_SQS_NAME": "q",
        "FLEET_S3_BUCKET": bucket,
    }
    svc = FleetService(
        env=env,
        repo_root=REPO_ROOT,
        clients={"ddb": ddb or FakeDdb(), "sqs": sqs or FakeSqs(), "s3": s3 or FakeS3()},
    )
    # fleet_config の policy ファイル由来の値で上書きされないよう固定する
    svc.cfg = {
        "region": "ap-northeast-1",
        "state_table": "ms",
        "task_table": "ts",
        "budget_table": "bg",
        "sqs_name": "q",
        "bucket": bucket,
    }
    return svc


def test_summary_and_sort():
    items = {
        "M-1": _mission("M-1", "DONE"),
        "M-2": _mission("M-2", "NEED_APPROVAL", conf=80),
        "M-3": _mission("M-3", "RUNNING"),
    }
    svc = make_service(ddb=FakeDdb(missions=items))
    rows = svc.list_missions()
    assert [r.mission_id for r in rows] == ["M-2", "M-3", "M-1"]
    assert rows[0].confidence == 80
    assert rows[0].to_dict()["status"] == "NEED_APPROVAL"
    assert state_rank("NEED_APPROVAL") < state_rank("DONE")


def test_list_pending_filters():
    items = {
        "M-1": _mission("M-1", "DONE"),
        "M-2": _mission("M-2", "NEED_INPUT"),
        "M-3": _mission("M-3", "NEED_APPROVAL"),
    }
    svc = make_service(ddb=FakeDdb(missions=items))
    pending = svc.list_pending()
    assert [r.mission_id for r in pending] == ["M-3", "M-2"]


def test_approve_and_reenqueue():
    ddb = FakeDdb(missions={"M-1": _mission("M-1", "NEED_APPROVAL")})
    sqs = FakeSqs()
    svc = make_service(ddb=ddb, sqs=sqs)
    new_status = svc.approve_mission("M-1", yes=True, note="ok")
    assert new_status == "RUNNING"
    assert len(sqs.sent) == 1
    payload = json.loads(sqs.sent[0][1])
    assert payload["resume"] is True

    # 状態不一致はエラー
    try:
        svc.approve_mission("M-1", yes=True)
        assert False, "should raise"
    except FleetServiceError as exc:
        assert "expected" in str(exc)


def test_reject_no_reenqueue():
    ddb = FakeDdb(missions={"M-1": _mission("M-1", "NEED_APPROVAL")})
    sqs = FakeSqs()
    svc = make_service(ddb=ddb, sqs=sqs)
    assert svc.approve_mission("M-1", yes=False) == "FAILED"
    assert sqs.sent == []


def test_mission_not_found():
    svc = make_service()
    try:
        svc.get_mission("M-404", with_comms=False)
        assert False, "should raise"
    except MissionNotFound:
        pass


def test_task_detail_and_signals():
    task = {
        "task_id": {"S": "T-1"},
        "status": {"S": "NEED_APPROVAL"},
        "current_stage": {"S": "CA"},
        "budget_month": {"S": "2026-06"},
        "updated_at": {"N": "200"},
        "stages": {"M": {
            "CA": {"M": {
                "status": {"S": "NEED_APPROVAL"},
                "mission_id": {"S": "M-CA"},
                "ship_class": {"S": "CA"},
            }},
            "DD_SONNET": {"M": {
                "status": {"S": "DONE"},
                "mission_id": {"S": "M-DD"},
                "ship_class": {"S": "DD"},
            }},
        }},
    }
    missions = {
        "M-CA": _mission("M-CA", "NEED_APPROVAL", ship="CA", conf=85),
        "M-DD": _mission("M-DD", "DONE", ship="DD"),
    }
    ddb = FakeDdb(missions=missions, tasks={"T-1": task})
    svc = make_service(ddb=ddb)

    detail = svc.get_task("T-1")
    assert detail.status == "NEED_APPROVAL"
    assert [s.stage for s in detail.stages] == ["CA", "DD_SONNET"]
    assert detail.stages[0].confidence == 85

    assert svc.get_ca_confidence("T-1") == 85
    assert svc.get_task_status("T-1") == ("NEED_APPROVAL", "CA")
    assert svc.get_task_status("T-404") == ("UNKNOWN", "")

    approved = svc.approve_task_missions("T-1", "auto")
    assert approved == 1


def test_abort_task():
    task = {
        "task_id": {"S": "T-1"},
        "status": {"S": "RUNNING"},
        "stages": {"M": {
            "DD": {"M": {"status": {"S": "RUNNING"}, "mission_id": {"S": "M-1"}}},
            "CL": {"M": {"status": {"S": "DONE"}, "mission_id": {"S": "M-2"}}},
        }},
    }
    ddb = FakeDdb(
        missions={"M-1": _mission("M-1", "RUNNING"), "M-2": _mission("M-2", "DONE")},
        tasks={"T-1": task},
    )
    svc = make_service(ddb=ddb)
    assert svc.abort_task("T-1", "test") == 1

    task["status"] = {"S": "CANCELLED"}
    try:
        svc.abort_task("T-1")
        assert False, "should raise"
    except TaskAlreadyFinished:
        pass


def test_retry_task():
    task = {
        "task_id": {"S": "T-1"},
        "status": {"S": "FAILED"},
        "stages": {"M": {
            "DD": {"M": {"status": {"S": "FAILED"}, "mission_id": {"S": "M-1"}}},
        }},
    }
    ddb = FakeDdb(missions={"M-1": _mission("M-1", "FAILED")}, tasks={"T-1": task})
    svc = make_service(ddb=ddb, sqs=FakeSqs())
    retried, errors = svc.retry_task("T-1")
    assert (retried, errors) == (1, 0)


def test_ca_directive_and_comms():
    task = {
        "task_id": {"S": "T-1"},
        "status": {"S": "RUNNING"},
        "stages": {"M": {
            "CA": {"M": {"status": {"S": "RUNNING"}, "mission_id": {"S": "M-CA"}}},
        }},
    }
    s3 = FakeS3()
    ddb = FakeDdb(missions={"M-CA": _mission("M-CA", "RUNNING")}, tasks={"T-1": task})
    svc = make_service(ddb=ddb, s3=s3)
    mission_id = svc.save_ca_directive("T-1", "Execute: DD_SONNET")
    assert mission_id == "M-CA"
    assert any("ca_human_directive.md" in k for k in s3.put)
    comms = svc.list_comms("M-CA")
    assert len(comms) == 1
    assert comms[0].content == "Execute: DD_SONNET"


def test_artifacts():
    s3 = FakeS3(objects={
        "missions/M-1/results/result.json": b'{"status": "DONE"}',
        "missions/M-1/artifacts/diff.patch": b"diff",
    })
    svc = make_service(s3=s3)
    files = svc.list_artifacts("M-1")
    assert {f.path for f in files} == {"results/result.json", "artifacts/diff.patch"}
    body = svc.get_artifact_bytes("M-1", "artifacts/diff.patch")
    assert body == b"diff"
    try:
        svc.get_artifact_bytes("M-1", "../../etc/passwd")
        assert False, "should raise"
    except FleetServiceError:
        pass


def test_budget():
    budget_item = {
        "month": {"S": "2026-06"},
        "fuel_cap_usd": {"N": "30"},
        "fuel_remaining_usd": {"N": "12.5"},
        "fuel_burned_usd": {"N": "17.5"},
        "sorties_total": {"N": "42"},
        "ammo_used": {"M": {
            "BB_main_gun": {"N": "1"},
            "CA_salvo": {"N": "10"},
            "CVB_air_wing": {"N": "5"},
        }},
        "ammo_caps": {"M": {
            "BB_main_gun": {"N": "4"},
            "CA_salvo": {"N": "40"},
            "CVB_air_wing": {"N": "20"},
        }},
    }
    svc = make_service(ddb=FakeDdb(budgets={"2026-06": budget_item}))
    b = svc.get_budget("2026-06")
    assert b.fuel_cap == 30.0
    assert b.ammo_used["CA"] == 10
    assert b.to_dict()["ammo_caps"]["BB"] == 4


def test_item_to_summary_handles_missing():
    s = mission_item_to_summary({})
    assert s.mission_id == ""
    assert s.confidence is None


def main() -> None:
    test_summary_and_sort()
    test_list_pending_filters()
    test_approve_and_reenqueue()
    test_reject_no_reenqueue()
    test_mission_not_found()
    test_task_detail_and_signals()
    test_abort_task()
    test_retry_task()
    test_ca_directive_and_comms()
    test_artifacts()
    test_budget()
    test_item_to_summary_handles_missing()
    print("PASS: fleet_service")


if __name__ == "__main__":
    main()

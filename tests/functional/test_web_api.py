from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient

from naval.service import FleetService
from naval.web.server import create_app

sys.path.insert(0, str(REPO_ROOT / "tests" / "unit"))
from test_fleet_service import FakeDdb, FakeS3, FakeSqs, _mission, make_service  # noqa: E402


def make_client(svc: FleetService) -> TestClient:
    app = create_app(service_factory=lambda: svc)
    return TestClient(app)


def test_missions_endpoints():
    ddb = FakeDdb(missions={
        "M-1": _mission("M-1", "NEED_APPROVAL", conf=80),
        "M-2": _mission("M-2", "DONE"),
    })
    client = make_client(make_service(ddb=ddb))

    resp = client.get("/api/missions")
    assert resp.status_code == 200
    missions = resp.json()["missions"]
    assert [m["mission_id"] for m in missions] == ["M-1", "M-2"]

    resp = client.get("/api/pending")
    assert [m["mission_id"] for m in resp.json()["missions"]] == ["M-1"]

    resp = client.get("/api/missions/M-1")
    assert resp.json()["status"] == "NEED_APPROVAL"
    assert resp.json()["confidence"] == 80

    resp = client.get("/api/missions/M-404")
    assert resp.status_code == 404


def test_approve_and_input():
    ddb = FakeDdb(missions={
        "M-1": _mission("M-1", "NEED_APPROVAL"),
        "M-2": _mission("M-2", "NEED_INPUT"),
    })
    client = make_client(make_service(ddb=ddb, sqs=FakeSqs()))

    resp = client.post("/api/missions/M-1/approve", json={"approve": True, "note": "ok"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "RUNNING"

    # 二重承認は 400
    resp = client.post("/api/missions/M-1/approve", json={"approve": True})
    assert resp.status_code == 400

    resp = client.post("/api/missions/M-2/input", json={"message": "回答です"})
    assert resp.status_code == 200

    # 空メッセージはバリデーションエラー
    resp = client.post("/api/missions/M-2/input", json={"message": ""})
    assert resp.status_code == 422


def test_tasks_endpoints():
    task = {
        "task_id": {"S": "T-1"},
        "status": {"S": "RUNNING"},
        "current_stage": {"S": "DD"},
        "budget_month": {"S": "2026-06"},
        "updated_at": {"N": "100"},
        "stages": {"M": {
            "DD": {"M": {"status": {"S": "RUNNING"}, "mission_id": {"S": "M-1"}}},
        }},
    }
    ddb = FakeDdb(missions={"M-1": _mission("M-1", "RUNNING")}, tasks={"T-1": task})
    client = make_client(make_service(ddb=ddb))

    resp = client.get("/api/tasks/T-1")
    assert resp.status_code == 200
    assert resp.json()["current_stage"] == "DD"
    assert len(resp.json()["stages"]) == 1

    resp = client.post("/api/tasks/T-1/abort", json={"note": "stop"})
    assert resp.status_code == 200
    assert resp.json()["missions_cancelled"] == 1

    # 二重 abort は 409
    resp = client.post("/api/tasks/T-1/abort", json={})
    assert resp.status_code == 409

    resp = client.get("/api/tasks/T-404")
    assert resp.status_code == 404


def test_artifacts_and_budget():
    s3 = FakeS3(objects={"missions/M-1/results/result.json": b'{"status":"DONE"}'})
    budget_item = {
        "month": {"S": "2026-06"},
        "fuel_cap_usd": {"N": "30"},
        "fuel_remaining_usd": {"N": "20"},
        "fuel_burned_usd": {"N": "10"},
        "sorties_total": {"N": "5"},
        "ammo_used": {"M": {"BB_main_gun": {"N": "0"}, "CA_salvo": {"N": "2"}, "CVB_air_wing": {"N": "1"}}},
        "ammo_caps": {"M": {"BB_main_gun": {"N": "4"}, "CA_salvo": {"N": "40"}, "CVB_air_wing": {"N": "20"}}},
    }
    ddb = FakeDdb(budgets={"2026-06": budget_item})
    client = make_client(make_service(ddb=ddb, s3=s3))

    resp = client.get("/api/missions/M-1/artifacts")
    assert resp.json()["files"][0]["path"] == "results/result.json"

    resp = client.get("/api/missions/M-1/artifacts/results/result.json")
    assert resp.status_code == 200
    assert resp.content == b'{"status":"DONE"}'

    resp = client.get("/api/budget?month=2026-06")
    assert resp.status_code == 200
    assert resp.json()["fuel_cap"] == 30.0

    resp = client.get("/api/budget?month=1999-01")
    assert resp.status_code == 400


def test_fleet_ships_endpoints(tmp_path=None):
    import os
    from tempfile import TemporaryDirectory

    sample = """\
ships:
  CVL:
    role: "索敵"
    model_env: "BEDROCK_MICRO_MODEL_ID"
    armament:
      max_tokens: 500
      temperature: 0.1
"""
    with TemporaryDirectory() as td:
        cfg = Path(td) / "config.yaml"
        cfg.write_text(sample, encoding="utf-8")
        os.environ["FLEET_LOCAL_CONFIG"] = str(cfg)
        try:
            client = make_client(make_service())

            resp = client.get("/api/fleet/ships")
            assert resp.status_code == 200
            assert "CVL" in resp.json()["ships"]

            resp = client.put(
                "/api/fleet/ships/CVL",
                json={"model_env": "BEDROCK_LITE_MODEL_ID", "role": "更新済み"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["profile"]["role"] == "更新済み"
            assert body["worker_restart_required"] is True

            # 不正な model_env は 400
            resp = client.put("/api/fleet/ships/CVL", json={"model_env": "BAD"})
            assert resp.status_code == 400

            # 存在しない艦種は 400
            resp = client.put("/api/fleet/ships/ZZ", json={"role": "x"})
            assert resp.status_code == 400

            resp = client.get("/api/fleet/models")
            assert len(resp.json()["models"]) == 4
        finally:
            del os.environ["FLEET_LOCAL_CONFIG"]


def test_index_served():
    client = make_client(make_service())
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Naval Battle" in resp.text


def main() -> None:
    test_missions_endpoints()
    test_approve_and_input()
    test_tasks_endpoints()
    test_artifacts_and_budget()
    test_fleet_ships_endpoints()
    test_index_served()
    print("PASS: web_api")


if __name__ == "__main__":
    main()

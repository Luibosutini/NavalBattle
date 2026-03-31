from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.temporal.activities import (
    _merge_context_values,
    context_append_patch_activity,
    context_build_latest_activity,
)


def test_merge_rule() -> None:
    base = {
        "facts": ["a"],
        "nested": {"n": 1, "arr": [{"k": 1}]},
        "value": "old",
    }
    patch = {
        "facts": ["a", "b"],
        "nested": {"m": 2, "arr": [{"k": 1}, {"k": 2}]},
        "value": "new",
    }
    merged = _merge_context_values(base, patch)
    assert merged["facts"] == ["a", "b"], merged
    assert merged["nested"]["n"] == 1, merged
    assert merged["nested"]["m"] == 2, merged
    assert merged["nested"]["arr"] == [{"k": 1}, {"k": 2}], merged
    assert merged["value"] == "new", merged


def test_context_patch_and_latest() -> None:
    store: dict[str, bytes] = {}

    class _S3:
        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            store[str(kwargs["Key"])] = bytes(kwargs["Body"])
            return {}

        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            prefix = str(kwargs.get("Prefix", ""))
            keys = sorted(k for k in store if k.startswith(prefix))
            return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            return {"Body": io.BytesIO(store[str(kwargs["Key"])])}

    payload1 = {
        "mission_id": "MS-CTX-1",
        "task_id": "T-CTX-1",
        "ship": "CVL",
        "trace_id": "MS-CTX-1",
        "context_patch": {"facts": ["a"], "nested": {"n": 1}},
    }
    payload2 = {
        "mission_id": "MS-CTX-1",
        "task_id": "T-CTX-1",
        "ship": "DD",
        "trace_id": "MS-CTX-1",
        "context_patch": {"facts": ["b"], "nested": {"m": 2}},
    }
    payload_latest = {
        "mission_id": "MS-CTX-1",
        "task_id": "T-CTX-1",
        "ship": "CA",
        "trace_id": "MS-CTX-1",
    }

    with patch("naval.temporal.activities._s3_client", return_value=("mock-bucket", _S3())):
        r1 = asyncio.run(context_append_patch_activity(payload1))
        r2 = asyncio.run(context_append_patch_activity(payload2))
        latest = asyncio.run(context_build_latest_activity(payload_latest))

    assert r1["status"] == "OK", r1
    assert r2["status"] == "OK", r2
    assert latest["status"] == "OK", latest
    assert int(latest["patch_count"]) == 2, latest
    assert latest["context"]["facts"] == ["a", "b"], latest
    assert latest["context"]["nested"]["n"] == 1, latest
    assert latest["context"]["nested"]["m"] == 2, latest

    latest_key = "missions/MS-CTX-1/context/latest.json"
    assert latest_key in store, store.keys()
    latest_doc = json.loads(store[latest_key].decode("utf-8"))
    assert latest_doc["patch_count"] == 2, latest_doc


def main() -> None:
    test_merge_rule()
    print("PASS: context_protocol.merge_rule")
    test_context_patch_and_latest()
    print("PASS: context_protocol.patch_and_latest")


if __name__ == "__main__":
    main()

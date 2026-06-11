from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import yaml

from naval.web import ships_config
from naval.web.ships_config import ShipsConfigError

SAMPLE = """\
version: 1
local:
  root: ./local
ships:
  CVL:
    role: "索敵・圧縮"
    model_env: "BEDROCK_MICRO_MODEL_ID"
    model_tag: "nova.micro"
    armament:
      max_tokens: 500
      temperature: 0.1
    custom_key: "must survive"
  DD:
    role: "実装"
    model_env: "BEDROCK_SONNET_MODEL_ID"
"""


def _write_sample(td: Path) -> Path:
    p = td / "config.yaml"
    p.write_text(SAMPLE, encoding="utf-8")
    return p


def test_list_and_get():
    with TemporaryDirectory() as td:
        p = _write_sample(Path(td))
        ships = ships_config.list_ships(p)
        assert set(ships.keys()) == {"CVL", "DD"}
        cvl = ships_config.get_ship("CVL", p)
        assert cvl["model_tag"] == "nova.micro"
        try:
            ships_config.get_ship("BB", p)
            assert False, "should raise"
        except ShipsConfigError:
            pass


def test_update_valid():
    with TemporaryDirectory() as td:
        p = _write_sample(Path(td))
        profile = ships_config.update_ship(
            "CVL",
            {
                "role": "新しい役割",
                "model_env": "BEDROCK_LITE_MODEL_ID",
                "armament": {"max_tokens": 800, "temperature": 0.3},
                "constraints": ["a", "b"],
            },
            p,
        )
        assert profile["role"] == "新しい役割"
        assert profile["armament"]["max_tokens"] == 800

        # ファイルに反映され、未知キー・他セクションは保持される
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        assert data["ships"]["CVL"]["model_env"] == "BEDROCK_LITE_MODEL_ID"
        assert data["ships"]["CVL"]["custom_key"] == "must survive"
        assert data["ships"]["CVL"]["constraints"] == ["a", "b"]
        assert data["local"]["root"] == "./local"
        assert data["version"] == 1

        # バックアップが作られる
        assert p.with_suffix(".yaml.bak").exists()


def test_update_partial_armament():
    with TemporaryDirectory() as td:
        p = _write_sample(Path(td))
        profile = ships_config.update_ship("CVL", {"armament": {"max_tokens": 999}}, p)
        assert profile["armament"]["max_tokens"] == 999
        assert profile["armament"]["temperature"] == 0.1  # 既存値は保持


def test_validation_errors():
    with TemporaryDirectory() as td:
        p = _write_sample(Path(td))
        cases = [
            {"model_env": "NOT_A_MODEL_ENV"},
            {"armament": {"max_tokens": "abc"}},
            {"armament": {"temperature": 5.0}},
            {"armament": {"max_tokens": 0}},
            {"unknown_field": "x"},
            {"constraints": "not-a-list"},
            {"role": 123},
        ]
        for update in cases:
            try:
                ships_config.update_ship("CVL", update, p)
                assert False, f"should raise: {update}"
            except ShipsConfigError:
                pass
        # 検証エラー時はファイルが変更されない
        assert yaml.safe_load(p.read_text(encoding="utf-8"))["ships"]["CVL"]["role"] == "索敵・圧縮"


def test_unknown_ship():
    with TemporaryDirectory() as td:
        p = _write_sample(Path(td))
        try:
            ships_config.update_ship("ZZ", {"role": "x"}, p)
            assert False, "should raise"
        except ShipsConfigError:
            pass


def test_list_models():
    env = {"BEDROCK_SONNET_MODEL_ID": "arn:aws:bedrock:xxx"}
    models = ships_config.list_models(env)
    by_env = {m["model_env"]: m for m in models}
    assert by_env["BEDROCK_SONNET_MODEL_ID"]["configured"] == "true"
    assert by_env["BEDROCK_OPUS_MODEL_ID"]["configured"] == "false"
    assert len(models) == 4


def main() -> None:
    test_list_and_get()
    test_update_valid()
    test_update_partial_armament()
    test_validation_errors()
    test_unknown_ship()
    test_list_models()
    print("PASS: ships_config")


if __name__ == "__main__":
    main()

"""艦種ごとの AI エージェント設定（config.yaml の ships: セクション）の読み書き。

GUI から安全に設定変更できるよう、次の方針で実装する:
- 書き込み前に config.yaml.bak へバックアップを取る
- ships: セクション以外のキーは一切変更しない
- 未知のキーはプロファイル内に保持する（手書き設定を壊さない）
- 書き込み後は fleet_local のプロセス内キャッシュを無効化する
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

BASE_SHIP_CLASSES = ["CVL", "DD", "CL", "CVB", "CA", "BB"]

# GUI から編集を許可するキーと型
EDITABLE_FIELDS: Dict[str, type] = {
    "role": str,
    "voice": str,
    "objective": str,
    "thinking": str,
    "model_env": str,
    "model_tag": str,
}
EDITABLE_LIST_FIELDS = {"constraints", "checklist", "focus"}

MODEL_ENV_VARS = [
    "BEDROCK_SONNET_MODEL_ID",
    "BEDROCK_OPUS_MODEL_ID",
    "BEDROCK_MICRO_MODEL_ID",
    "BEDROCK_LITE_MODEL_ID",
]

MODEL_ENV_LABELS = {
    "BEDROCK_SONNET_MODEL_ID": "Claude Sonnet",
    "BEDROCK_OPUS_MODEL_ID": "Claude Opus",
    "BEDROCK_MICRO_MODEL_ID": "Nova Micro",
    "BEDROCK_LITE_MODEL_ID": "Nova Lite",
}


class ShipsConfigError(Exception):
    """設定の検証・書き込みに失敗。メッセージはユーザー表示可能。"""


def config_path(repo_root: Optional[Path] = None) -> Path:
    env_path = os.getenv("FLEET_LOCAL_CONFIG")
    if env_path:
        return Path(env_path)
    root = repo_root or Path(__file__).resolve().parents[2]
    return root / "config.yaml"


def _load_raw(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ShipsConfigError(f"config.yaml の読み込みに失敗しました: {exc}") from exc
    if not isinstance(data, dict):
        raise ShipsConfigError("config.yaml のルートはマッピングである必要があります")
    return data


def list_ships(path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """ships: セクション全体を返す（キー: 艦種、値: プロファイル）。"""
    p = path or config_path()
    data = _load_raw(p)
    ships = data.get("ships", {})
    if not isinstance(ships, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for name, profile in ships.items():
        if isinstance(profile, dict):
            out[str(name)] = profile
    return out


def get_ship(ship_class: str, path: Optional[Path] = None) -> Dict[str, Any]:
    ships = list_ships(path)
    if ship_class not in ships:
        raise ShipsConfigError(f"艦種が見つかりません: {ship_class}")
    return ships[ship_class]


def validate_profile_update(update: Dict[str, Any]) -> Dict[str, Any]:
    """GUI からの更新内容を検証し、適用可能な辞書を返す。"""
    cleaned: Dict[str, Any] = {}
    for key, value in update.items():
        if key in EDITABLE_FIELDS:
            if not isinstance(value, str):
                raise ShipsConfigError(f"{key} は文字列で指定してください")
            cleaned[key] = value
        elif key in EDITABLE_LIST_FIELDS:
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ShipsConfigError(f"{key} は文字列のリストで指定してください")
            cleaned[key] = value
        elif key == "armament":
            if not isinstance(value, dict):
                raise ShipsConfigError("armament はマッピングで指定してください")
            armament: Dict[str, Any] = {}
            if "max_tokens" in value:
                try:
                    mt = int(value["max_tokens"])
                except (TypeError, ValueError):
                    raise ShipsConfigError("armament.max_tokens は整数で指定してください")
                if not (1 <= mt <= 200_000):
                    raise ShipsConfigError("armament.max_tokens は 1〜200000 の範囲で指定してください")
                armament["max_tokens"] = mt
            if "temperature" in value:
                try:
                    temp = float(value["temperature"])
                except (TypeError, ValueError):
                    raise ShipsConfigError("armament.temperature は数値で指定してください")
                if not (0.0 <= temp <= 1.0):
                    raise ShipsConfigError("armament.temperature は 0.0〜1.0 の範囲で指定してください")
                armament["temperature"] = temp
            cleaned["armament"] = armament
        else:
            raise ShipsConfigError(f"編集できない項目です: {key}")
    if "model_env" in cleaned and cleaned["model_env"] not in MODEL_ENV_VARS:
        raise ShipsConfigError(
            f"model_env は {', '.join(MODEL_ENV_VARS)} のいずれかで指定してください"
        )
    return cleaned


def update_ship(
    ship_class: str,
    update: Dict[str, Any],
    path: Optional[Path] = None,
) -> Dict[str, Any]:
    """艦種プロファイルを検証付きで更新し、更新後のプロファイルを返す。"""
    p = path or config_path()
    cleaned = validate_profile_update(update)
    data = _load_raw(p)

    ships = data.get("ships")
    if not isinstance(ships, dict) or ship_class not in ships:
        raise ShipsConfigError(f"艦種が見つかりません: {ship_class}")
    profile = ships[ship_class]
    if not isinstance(profile, dict):
        raise ShipsConfigError(f"艦種プロファイルが不正です: {ship_class}")

    for key, value in cleaned.items():
        if key == "armament":
            current = profile.get("armament")
            if not isinstance(current, dict):
                current = {}
            current.update(value)
            profile["armament"] = current
        else:
            profile[key] = value

    # 書き込み前にバックアップ
    if p.exists():
        shutil.copy2(p, p.with_suffix(p.suffix + ".bak"))

    text = yaml.safe_dump(
        data,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    # 書き戻し前に round-trip 検証（壊れた YAML を書かない）
    reparsed = yaml.safe_load(text)
    if not isinstance(reparsed, dict) or "ships" not in reparsed:
        raise ShipsConfigError("設定の再構成に失敗したため保存を中止しました")
    p.write_text(text, encoding="utf-8")

    _invalidate_fleet_local_cache()
    return profile


def _invalidate_fleet_local_cache() -> None:
    try:
        import fleet_local

        fleet_local._CONFIG_CACHE = None
    except Exception:
        pass


def list_formations(path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """formations: セクションを返す（キー: 陣形名、値: description/stages）。"""
    p = path or config_path()
    data = _load_raw(p)
    formations = data.get("formations", {})
    if not isinstance(formations, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for name, entry in formations.items():
        if isinstance(entry, dict):
            out[str(name)] = {
                "description": str(entry.get("description", "")),
                "stages": [s for s in entry.get("stages", []) if isinstance(s, str)],
            }
        elif isinstance(entry, list):
            out[str(name)] = {"description": "", "stages": [s for s in entry if isinstance(s, str)]}
    return out


def list_models(env: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    """割当可能なモデル一覧（model_env 候補とその解決値）を返す。"""
    e = env if env is not None else dict(os.environ)
    models: List[Dict[str, str]] = []
    for var in MODEL_ENV_VARS:
        models.append(
            {
                "model_env": var,
                "label": MODEL_ENV_LABELS.get(var, var),
                "model_id": e.get(var, ""),
                "configured": "true" if e.get(var) else "false",
            }
        )
    return models

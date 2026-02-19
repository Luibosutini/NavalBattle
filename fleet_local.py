from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml


DEFAULT_CONFIG: Dict[str, Any] = {
    "version": 1,
    "local": {
        "root": "./local",
        "workspace_pattern": "missions/{mission_id}/{task_id}",
        "ship_pattern": "{workspace}/{ship_class}",
        "work_subdir": "work",
        "artifacts_subdir": "artifacts",
        "tmp_subdir": "tmp",
        "logs_subdir": "logs",
        "comms_db": "logs/naval_comm.db",
    },
    "ships": {},
}

_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_local_config(path: Optional[Path] = None) -> Dict[str, Any]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    cfg_path = path or Path(os.getenv("FLEET_LOCAL_CONFIG", "config.yaml"))
    data: Dict[str, Any] = {}
    if cfg_path.exists():
        try:
            data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}

    cfg = _deep_merge(DEFAULT_CONFIG, data)

    # Env overrides
    if os.getenv("FLEET_LOCAL_ROOT"):
        cfg["local"]["root"] = os.getenv("FLEET_LOCAL_ROOT")
    if os.getenv("FLEET_LOCAL_DB"):
        cfg["local"]["comms_db"] = os.getenv("FLEET_LOCAL_DB")

    _CONFIG_CACHE = cfg
    return cfg


def _base_ship_class(ship_class: str) -> str:
    if not ship_class:
        return ship_class
    if "_" in ship_class:
        base = ship_class.split("_", 1)[0]
        if base in {"CVL", "DD", "CL", "CVB", "CA", "BB"}:
            return base
    return ship_class


def get_ship_profile(ship_class: str) -> Dict[str, Any]:
    cfg = load_local_config()
    ships = cfg.get("ships", {})
    profile = ships.get(ship_class)
    if profile is None:
        base = _base_ship_class(ship_class)
        profile = ships.get(base, {})
    return profile or {}


def _format_pattern(pattern: str, **kwargs: str) -> str:
    try:
        return pattern.format(**kwargs)
    except Exception:
        return pattern


def _resolve_path(root: Path, path_str: str) -> Path:
    p = Path(path_str)
    if p.is_absolute():
        return p
    return root / p


def get_local_root() -> Path:
    cfg = load_local_config()
    return Path(cfg["local"]["root"]).resolve()


def get_local_db_path() -> Path:
    cfg = load_local_config()
    root = get_local_root()
    db_path = cfg["local"].get("comms_db", "logs/naval_comm.db")
    return _resolve_path(root, db_path)


def format_mental_model(ship_class: str, profile: Dict[str, Any]) -> str:
    cfg = load_local_config()
    lines = [f"# {ship_class} Mental Model", ""]
    if profile.get("role"):
        lines.append(f"- 役割: {profile['role']}")
    if profile.get("voice"):
        lines.append(f"- 口調: {profile['voice']}")
    if profile.get("objective"):
        lines.append(f"- 目的: {profile['objective']}")
    if profile.get("model_env"):
        lines.append(f"- model_env: {profile['model_env']}")
    if profile.get("model_tag"):
        lines.append(f"- model_tag: {profile['model_tag']}")
    if profile.get("thinking"):
        lines.append(f"- 思考様式: {profile['thinking']}")
    lines.append("")
    if profile.get("armament"):
        arm = profile["armament"]
        lines.append("## 兵装制限")
        if isinstance(arm, dict):
            if "max_tokens" in arm:
                lines.append(f"- max_tokens: {arm['max_tokens']}")
            if "temperature" in arm:
                lines.append(f"- temperature: {arm['temperature']}")
            if "top_p" in arm:
                lines.append(f"- top_p: {arm['top_p']}")
        lines.append("")
    if profile.get("constraints"):
        lines.append("## 制約")
        for c in profile["constraints"]:
            lines.append(f"- {c}")
        lines.append("")
    if profile.get("focus"):
        lines.append("## 重視点")
        for c in profile["focus"]:
            lines.append(f"- {c}")
        lines.append("")
    if profile.get("checklist"):
        lines.append("## チェックリスト")
        for c in profile["checklist"]:
            lines.append(f"- {c}")
        lines.append("")
    handoff = cfg.get("communication", {}).get("handoff", {}).get(ship_class)
    if not handoff and ship_class:
        handoff = cfg.get("communication", {}).get("handoff", {}).get(_base_ship_class(ship_class))
    if isinstance(handoff, dict):
        lines.append("## バトン規約")
        if handoff.get("to"):
            lines.append(f"- 次: {', '.join(handoff['to'])}")
        if handoff.get("must_include"):
            lines.append("- 必須: " + ", ".join(handoff["must_include"]))
        if handoff.get("optional"):
            lines.append("- 任意: " + ", ".join(handoff["optional"]))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def ensure_local_workspace(
    mission_id: str,
    task_id: str,
    ship_class: str,
    ship_id: Optional[str] = None,
) -> Dict[str, Path]:
    cfg = load_local_config()
    local_cfg = cfg["local"]
    root = get_local_root()

    workspace_rel = _format_pattern(
        local_cfg.get("workspace_pattern", "missions/{mission_id}/{task_id}"),
        mission_id=mission_id,
        task_id=task_id,
        ship_class=ship_class,
        ship_id=ship_id or "",
    )
    workspace = _resolve_path(root, workspace_rel)

    ship_pattern = local_cfg.get("ship_pattern", "{workspace}/{ship_class}")
    ship_str = _format_pattern(
        ship_pattern,
        workspace=str(workspace),
        mission_id=mission_id,
        task_id=task_id,
        ship_class=ship_class,
        ship_id=ship_id or "",
    )
    ship_dir = _resolve_path(root, ship_str)

    work_dir = ship_dir / local_cfg.get("work_subdir", "work")
    artifacts_dir = ship_dir / local_cfg.get("artifacts_subdir", "artifacts")
    tmp_dir = ship_dir / local_cfg.get("tmp_subdir", "tmp")
    logs_dir = ship_dir / local_cfg.get("logs_subdir", "logs")

    for d in [workspace, ship_dir, work_dir, artifacts_dir, tmp_dir, logs_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Write mental model once
    profile = get_ship_profile(ship_class)
    if profile:
        mm_path = ship_dir / "mental_model.md"
        if not mm_path.exists():
            mm_path.write_text(format_mental_model(ship_class, profile), encoding="utf-8")

    return {
        "root": root,
        "workspace": workspace,
        "ship_dir": ship_dir,
        "work_dir": work_dir,
        "artifacts_dir": artifacts_dir,
        "tmp_dir": tmp_dir,
        "logs_dir": logs_dir,
    }


def _init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS comms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                mission_id TEXT,
                task_id TEXT,
                ship_class TEXT,
                ship_id TEXT,
                from_role TEXT,
                to_role TEXT,
                comm_type TEXT,
                content TEXT,
                source TEXT,
                model_id TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comms_mission_ts ON comms(mission_id, ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comms_task_ts ON comms(task_id, ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comms_type_ts ON comms(comm_type, ts);")
        conn.commit()
    finally:
        conn.close()


def init_local_db() -> Path:
    db_path = get_local_db_path()
    _init_db(db_path)
    return db_path


def log_comm_local(
    mission_id: str,
    task_id: Optional[str],
    ship_class: Optional[str],
    ship_id: Optional[str],
    from_role: str,
    to_role: str,
    comm_type: str,
    content: str,
    source: str = "fleet_node",
    model_id: str = "",
) -> None:
    try:
        db_path = get_local_db_path()
        _init_db(db_path)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                """
                INSERT INTO comms
                (ts, mission_id, task_id, ship_class, ship_id, from_role, to_role, comm_type, content, source, model_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    mission_id,
                    task_id or "",
                    ship_class or "",
                    ship_id or "",
                    from_role,
                    to_role,
                    comm_type,
                    content,
                    source,
                    model_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # Best-effort only
        return


def search_comms(
    query: str = "",
    mission_id: str = "",
    task_id: str = "",
    ship_class: str = "",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    db_path = get_local_db_path()
    if not db_path.exists():
        return []

    where = ["1=1"]
    params: List[Any] = []
    if mission_id:
        where.append("mission_id = ?")
        params.append(mission_id)
    if task_id:
        where.append("task_id = ?")
        params.append(task_id)
    if ship_class:
        where.append("ship_class = ?")
        params.append(ship_class)
    if query:
        where.append("content LIKE ?")
        params.append(f"%{query}%")

    sql = (
        "SELECT ts, mission_id, task_id, ship_class, ship_id, from_role, to_role, "
        "comm_type, content, source, model_id FROM comms "
        f"WHERE {' AND '.join(where)} ORDER BY ts DESC LIMIT ?"
    )
    params.append(limit)

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for r in rows:
        results.append(
            {
                "ts": r[0],
                "mission_id": r[1],
                "task_id": r[2],
                "ship_class": r[3],
                "ship_id": r[4],
                "from_role": r[5],
                "to_role": r[6],
                "comm_type": r[7],
                "content": r[8],
                "source": r[9],
                "model_id": r[10],
            }
        )
    return results

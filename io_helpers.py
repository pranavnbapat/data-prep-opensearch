# io_helpers.py

from __future__ import annotations

import json
import os

from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

from utils import CustomJSONEncoder


def env_flag(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}

def run_stamp() -> str:
    return datetime.now().strftime("%d_%m-%Y_%H-%M-%S")


def output_dir(env_mode: str, root: str = "output") -> Path:
    now = datetime.now()
    return Path(root) / env_mode.upper() / now.strftime("%Y") / now.strftime("%m")


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    tmp.replace(path)


def find_latest_matching(env_mode: str, root: str, pattern: str) -> Optional[Path]:
    """
    Finds the newest file matching pattern under output/<ENV>/ recursively.
    Example pattern: "final_output_*.json"
    """
    base = Path(root) / env_mode.upper()
    if not base.exists():
        return None

    candidates = list(base.rglob(pattern))
    if not candidates:
        return None

    # Sort by mtime (newest first)
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def update_latest_pointer(env_mode: str, root: str, latest_name: str, target: Path) -> Path:
    """
    Writes a tiny JSON pointer file so other stages don't need to rglob.
    """
    latest_path = Path(root) / env_mode.upper() / latest_name
    payload = {
        "path": str(target),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    atomic_write_json(latest_path, payload)
    return latest_path


def resolve_latest_pointer(env_mode: str, root: str, latest_name: str) -> Optional[Path]:
    latest_path = Path(root) / env_mode.upper() / latest_name
    if not latest_path.exists():
        return None
    try:
        data = json.loads(latest_path.read_text(encoding="utf-8"))
        p = Path(str(data.get("path", "")))
        return p if p.exists() else None
    except Exception:
        return None

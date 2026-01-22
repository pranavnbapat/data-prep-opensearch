# truncate_final_output.py

from __future__ import annotations

import json
import os
import random
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    """Atomic JSON write: temp file in same dir then replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=str(path.parent), delete=False, encoding="utf-8") as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def _find_latest_final_output(root: Path) -> Path:
    """Find latest final_output_*.json under root by mtime."""
    candidates = list(root.rglob("final_output_*.json"))
    if not candidates:
        raise FileNotFoundError(f"No final_output_*.json found under: {root}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def truncate_latest_final_output(*, env_mode: str, output_root: str = "output", n_keep: int = 10) -> Tuple[Path, List[Dict[str, Any]]]:
    """
    Truncate the latest final_output_*.json to n_keep docs (random sample) and overwrite it.

    Returns:
      (path_to_truncated_file, truncated_docs)
    """
    env = env_mode.strip().upper()
    root = Path(output_root) / env

    latest_path = _find_latest_final_output(root)

    payload: Dict[str, Any] = json.loads(latest_path.read_text(encoding="utf-8"))
    docs = payload.get("docs") or []
    if not isinstance(docs, list):
        raise ValueError(f"Invalid shape: 'docs' is not a list in {latest_path}")

    k = min(n_keep, len(docs))
    sampled = random.sample(docs, k) if k > 0 else []

    payload["docs"] = sampled

    # Keep meta intact; adjust stats emitted to match the truncated docs.
    stats = payload.get("stats")
    if isinstance(stats, dict):
        stats["emitted"] = len(sampled)

    _atomic_write_json(latest_path, payload)
    return latest_path, sampled

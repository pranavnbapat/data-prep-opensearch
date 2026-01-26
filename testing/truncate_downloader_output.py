# truncate_downloader_output.py

"""
Randomly keep only 10 docs from the latest `final_output_*.json` under output/<ENV_MODE>/,
where ENV_MODE is loaded from .env (e.g., ENV_MODE=DEV).

- No CLI arguments.
- Finds the latest matching file by modification time.
- Overwrites the same file atomically (temp file + replace).
"""

from __future__ import annotations

import json
import os
import random
import tempfile

from pathlib import Path
from typing import Any, Dict, List


N_KEEP = 10  # hardcoded as requested


def load_env_mode() -> str:
    """Load ENV_MODE from .env (if available) or from environment variables."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()  # looks for .env in current working dir
    except Exception:
        # If python-dotenv isn't installed, we still allow ENV_MODE from actual env vars.
        pass

    env_mode = (os.getenv("ENV_MODE") or "").strip().upper()
    if not env_mode:
        raise SystemExit("ENV_MODE not found. Put ENV_MODE=DEV in .env (or export ENV_MODE).")
    return env_mode


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    """Write JSON atomically to avoid partial/corrupt writes."""
    dir_name = path.parent
    dir_name.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w",
        dir=str(dir_name),
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)

    # Atomic replace on POSIX
    tmp_path.replace(path)


def find_latest_final_output(root: Path) -> Path:
    """
    Find the latest `final_output_*.json` under root by modification time.
    Example root: output/DEV
    """
    candidates = list(root.rglob("final_output_*.json"))
    if not candidates:
        raise SystemExit(f"No files matching final_output_*.json found under: {root}")

    # Latest by mtime (modification time)
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def sample_docs_in_place(json_path: Path, n_keep: int) -> None:
    with json_path.open("r", encoding="utf-8") as f:
        payload: Dict[str, Any] = json.load(f)

    docs: List[Dict[str, Any]] = payload.get("docs") or []
    if not isinstance(docs, list):
        raise SystemExit(f"Invalid JSON structure: 'docs' is not a list in {json_path}")

    k = min(n_keep, len(docs))
    rng = random.Random()  # non-deterministic by default
    sampled = rng.sample(docs, k) if k > 0 else []

    payload["docs"] = sampled

    # Keep meta untouched. Keep stats mostly untouched, but emitted should reflect actual docs length.
    stats = payload.get("stats")
    if isinstance(stats, dict):
        stats["emitted"] = len(sampled)

    atomic_write_json(json_path, payload)


def main() -> int:
    env_mode = load_env_mode()

    root = Path("../output") / env_mode
    latest_file = find_latest_final_output(root)

    sample_docs_in_place(latest_file, N_KEEP)

    print(f"ENV_MODE={env_mode}")
    print(f"Sampled {N_KEEP} docs (or fewer if file had < {N_KEEP})")
    print(f"Overwrote: {latest_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

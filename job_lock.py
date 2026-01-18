# job_lock.py

from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from io_helpers import atomic_write_json


@dataclass(frozen=True)
class LockHandle:
    lock_dir: Path


def _pid_is_alive(pid: int) -> bool:
    """POSIX-friendly: checks if a pid exists (doesn't guarantee it's *your* process)."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def acquire_job_lock(*, env_mode: str, output_root: str, entrypoint: str, stale_after_sec: int = 24 * 3600) -> LockHandle:
    """
    Global mutual exclusion across pipeline/downloader/enricher/improver.
    Uses atomic mkdir as the lock.
    """
    lock_dir = Path(output_root) / env_mode.upper() / ".locks" / "pipeline.lock"
    status_path = lock_dir / "status.json"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now().isoformat(timespec="seconds")
    pid = os.getpid()

    try:
        lock_dir.mkdir()  # atomic if lock doesn't exist
    except FileExistsError:
        # lock exists -> check status to decide if stale
        try:
            status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
        except Exception:
            status = {}

        existing_pid = int(status.get("pid") or 0)
        started_at = status.get("started_at")
        started_ts = None
        try:
            if isinstance(started_at, str) and started_at:
                # coarse parse: fall back to mtime if parsing fails
                started_ts = lock_dir.stat().st_mtime
        except Exception:
            started_ts = None

        age = None
        try:
            age = time.time() - (started_ts or lock_dir.stat().st_mtime)
        except Exception:
            age = None

        if _pid_is_alive(existing_pid) and (age is None or age < stale_after_sec):
            who = status.get("entrypoint") or "unknown"
            raise RuntimeError(
                f"Job lock is held (status=running, entrypoint={who}, pid={existing_pid}). "
                f"Refusing to start {entrypoint}."
            )

        # stale lock: remove it and retry once
        try:
            if status_path.exists():
                status_path.unlink(missing_ok=True)  # py3.8+ ignore on older
        except Exception:
            pass
        try:
            # best-effort cleanup; directory may contain leftover files
            for p in lock_dir.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            lock_dir.rmdir()
        except Exception:
            # if we can't remove, fail loudly
            raise RuntimeError(f"Job lock exists but appears stale; could not remove {lock_dir} safely.")

        # retry acquire
        lock_dir.mkdir()

    status_payload: Dict[str, Any] = {
        "status": "running",
        "entrypoint": entrypoint,
        "env_mode": env_mode.upper(),
        "pid": pid,
        "host": socket.gethostname(),
        "started_at": now,
        "cmd": " ".join(os.getenv("_", "") for _ in []),  # keep simple; optional
    }
    atomic_write_json(status_path, status_payload)
    return LockHandle(lock_dir=lock_dir)


def release_job_lock(handle: Optional[LockHandle]) -> None:
    if not handle:
        return
    lock_dir = handle.lock_dir
    try:
        for p in lock_dir.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass
        lock_dir.rmdir()
    except Exception:
        # don't crash the main job just because we couldn't clean up lock
        pass

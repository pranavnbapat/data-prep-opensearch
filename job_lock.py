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

class JobLockHeldError(RuntimeError):
    """Raised when a job lock is held by another run (non-stale)."""

def _pid_is_alive(pid: int) -> bool:
    """POSIX-friendly: checks if a pid exists."""
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
        who = status.get("entrypoint") or "unknown"
        host = status.get("host") or "unknown"
        started_at = status.get("started_at") or "unknown"

        # Lock age: use directory mtime as a pragmatic proxy (works even if started_at parsing changes).
        age = None
        try:
            age = time.time() - lock_dir.stat().st_mtime
        except Exception:
            age = None

        alive = _pid_is_alive(existing_pid)
        not_stale = (age is None) or (age < stale_after_sec)

        if alive and not_stale:
            # Human-friendly message: clear next steps, no scary traceback (handled by caller).
            age_str = f"{int(age)}s" if isinstance(age, (int, float)) else "unknown"
            raise JobLockHeldError(
                "Another pipeline run is already in progress, so this run will not start.\n"
                f"  Env:        {env_mode.upper()}\n"
                f"  Entrypoint: {who}\n"
                f"  PID:        {existing_pid}\n"
                f"  Host:       {host}\n"
                f"  Started at: {started_at}\n"
                f"  Lock age:   {age_str}\n"
                f"  Lock dir:   {lock_dir}\n"
                "\n"
                "What to do next:\n"
                "  • If that process is still running, let it finish.\n"
                "  • If you are sure it crashed, remove the stale lock directory:\n"
                f"      rm -r {lock_dir}\n"
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

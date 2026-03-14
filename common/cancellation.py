from __future__ import annotations

from typing import Callable, Optional


class JobCancelled(RuntimeError):
    pass


def check_cancel(should_cancel: Optional[Callable[[], bool]], where: str) -> None:
    if should_cancel and should_cancel():
        raise JobCancelled(f"Job canceled during {where}")

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, field_validator


MAX_PAGE_SIZE = 100


class EnvMode(str, Enum):
    DEV = "DEV"
    PRD = "PRD"


class PipelineRunParams(BaseModel):
    page_size: Optional[int] = None
    env_mode: Optional[EnvMode] = None

    model_config = {"extra": "ignore"}

    @field_validator("page_size")
    @classmethod
    def clamp_page_size(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        if v <= 0:
            return None
        return min(v, MAX_PAGE_SIZE)


def effective_page_size(raw_page_size: int | None, *, default: int = 100, max_size: int = 100) -> int:
    try:
        ps = int(raw_page_size) if raw_page_size is not None else 0
    except (TypeError, ValueError):
        ps = 0

    if ps <= 0 or ps > max_size:
        return default
    return ps


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    canceled = "canceled"
    success = "success"
    error = "error"


class Job(BaseModel):
    id: str
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    env_mode: EnvMode
    page_size: int
    cancel_requested: bool = False
    emitted: Optional[int] = None
    dropped: Optional[int] = None
    output_file: Optional[str] = None
    final_path: Optional[str] = None
    report_path: Optional[str] = None
    latest_path: Optional[str] = None
    pipeline_stats: Optional[dict] = None
    error_trace: Optional[str] = None
    error: Optional[str] = None
    bucket_year: str
    bucket_month: str

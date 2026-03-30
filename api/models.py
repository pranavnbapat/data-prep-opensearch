from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


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


class BackendSyncParams(BaseModel):
    page_size: Optional[int] = Field(
        default=None,
        description="Downloader API page size for backend-core sync. Use 0 or omit to fall back to the default effective page size.",
    )
    env_mode: Optional[EnvMode] = Field(
        default=None,
        description="Target environment bucket to sync into, usually DEV or PRD.",
    )
    sort_criteria: int = Field(
        default=1,
        description="Backend-core API sort mode passed through to the downloader metadata endpoint.",
    )
    dl_workers: Optional[int] = Field(
        default=None,
        description="Downloader worker count for per-page processing during sync. Use 0 or omit to use the default configured worker count.",
    )

    model_config = {"extra": "ignore"}


class MysqlPipelineParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target environment bucket, usually DEV or PRD.")
    max_docs: Optional[int] = Field(
        default=None,
        description="Optional cap on how many MySQL-backed records to process in this run.",
    )
    llids: Optional[list[str]] = Field(
        default=None,
        description="Optional explicit logical-layer IDs to process instead of selecting by fast/deferred scope.",
    )

    model_config = {
        "extra": "ignore",
        "json_schema_extra": {
            "examples": [
                {"env_mode": "DEV"},
                {"env_mode": "DEV", "max_docs": 25},
                {"env_mode": "DEV", "llids": ["65cccdf7af785b8a565266dc"]},
            ]
        },
    }

    @field_validator("max_docs")
    @classmethod
    def normalize_max_docs(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        if v <= 0:
            return None
        return v

    @field_validator("llids")
    @classmethod
    def normalize_llids(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if not v:
            return None
        cleaned = []
        for item in v:
            if not isinstance(item, str):
                continue
            s = item.strip()
            if not s or s.lower() == "string":
                continue
            cleaned.append(s)
        return cleaned or None


class MysqlExportParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target environment bucket, usually DEV or PRD.")
    processed_only: bool = Field(
        default=False,
        description="If true, export only records that already have processed MySQL state. Otherwise export current known state for all synced records.",
    )
    eligible_only: bool = Field(
        default=True,
        description="If true, export only records that are processing-eligible. Set false to include blocked or ineligible records too.",
    )

    model_config = {"extra": "ignore"}


class MysqlSourceMetadataRepairParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target environment bucket, usually DEV or PRD.")
    max_docs: Optional[int] = Field(
        default=None,
        description="Optional cap on how many MySQL-backed records to repair in this run.",
    )
    llids: Optional[list[str]] = Field(
        default=None,
        description="Optional explicit logical-layer IDs to repair instead of scanning all source rows.",
    )

    model_config = {
        "extra": "ignore",
        "json_schema_extra": {
            "examples": [
                {"env_mode": "DEV"},
                {"env_mode": "DEV", "max_docs": 100},
                {"env_mode": "DEV", "llids": ["65cccdf7af785b8a565266dc"]},
            ]
        },
    }

    @field_validator("max_docs")
    @classmethod
    def normalize_max_docs(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        if v <= 0:
            return None
        return v

    @field_validator("llids")
    @classmethod
    def normalize_llids(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if not v:
            return None
        cleaned = []
        for item in v:
            if not isinstance(item, str):
                continue
            s = item.strip()
            if not s or s.lower() == "string":
                continue
            cleaned.append(s)
        return cleaned or None


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
    job_kind: str = "pipeline"
    scope: Optional[str] = None
    error_trace: Optional[str] = None
    error: Optional[str] = None
    bucket_year: str
    bucket_month: str

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

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


class TranslationsSyncParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target environment bucket, usually DEV or PRD.")
    only_missing: bool = Field(
        default=False,
        description="If true, skip records that already carry a metadata_translations block (no backend fetch for them). Set false to refresh all records, re-fetching and re-applying when the translation fingerprint changed.",
    )
    max_docs: Optional[int] = Field(
        default=None,
        description="Optional cap on how many MySQL-backed records to process in this run.",
    )
    llids: Optional[list[str]] = Field(
        default=None,
        description="Optional explicit logical-layer IDs to sync translations for instead of scanning all records.",
    )

    model_config = {
        "extra": "ignore",
        "json_schema_extra": {
            "examples": [
                {"env_mode": "DEV"},
                {"env_mode": "DEV", "only_missing": True},
                {"env_mode": "DEV", "max_docs": 100},
                {"env_mode": "DEV", "llids": ["65a52f44d7c24297492b48cb"]},
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


class ProjectsExportParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target backend-core environment bucket, usually DEV or PRD.")
    page_size: Optional[int] = Field(
        default=None,
        description="Backend-core projects API page size. Use 0 or omit to fall back to the default effective page size.",
    )

    model_config = {"extra": "ignore"}

    @field_validator("page_size")
    @classmethod
    def clamp_page_size(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return None
        if v <= 0:
            return None
        return min(v, 200)


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


class CurrentMetadataRepairField(str, Enum):
    date_of_completion = "date_of_completion"
    project_slug = "project_slug"


class MysqlCurrentMetadataRepairParams(BaseModel):
    env_mode: Optional[EnvMode] = Field(default=None, description="Target environment bucket, usually DEV or PRD.")
    max_docs: Optional[int] = Field(
        default=None,
        description="Optional cap on how many MySQL-backed records to repair in this run.",
    )
    llids: Optional[list[str]] = Field(
        default=None,
        description="Optional explicit logical-layer IDs to repair instead of scanning all rows.",
    )
    fields: list[CurrentMetadataRepairField] = Field(
        default_factory=lambda: [CurrentMetadataRepairField.date_of_completion, CurrentMetadataRepairField.project_slug],
        description="Which metadata fields to copy from source_doc_json into current_doc_json.",
    )

    model_config = {
        "extra": "ignore",
        "json_schema_extra": {
            "examples": [
                {"env_mode": "DEV", "fields": ["date_of_completion", "project_slug"]},
                {"env_mode": "DEV", "fields": ["date_of_completion"]},
                {"env_mode": "DEV", "fields": ["project_slug"], "llids": ["65cccdf7af785b8a565266dc"]},
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

    @field_validator("fields")
    @classmethod
    def normalize_fields(cls, v: list[CurrentMetadataRepairField]) -> list[CurrentMetadataRepairField]:
        return v or [CurrentMetadataRepairField.date_of_completion, CurrentMetadataRepairField.project_slug]


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


class BasicHealthResponse(BaseModel):
    ok: bool


class ScheduledJobResponse(BaseModel):
    status: str = Field(description="Scheduling result, typically `scheduled`.")
    job_id: str = Field(description="Queued job identifier.")


class ReprocessRecordResponse(ScheduledJobResponse):
    record_id: str = Field(description="Logical-layer record ID queued for reprocessing.")


class DependencyServiceStatus(BaseModel):
    service: str
    enabled: bool
    status: str = Field(description="One of `ok`, `error`, or `skipped`.")
    endpoint: Optional[str] = None
    status_code: Optional[int] = None
    latency_ms: Optional[int] = None
    error: Optional[str] = None
    details: Optional[dict[str, Any]] = None


class DependencyProbeResponse(BaseModel):
    ok: bool
    status: str
    env_mode: str
    services: list[DependencyServiceStatus]


class LatestExportResponse(BaseModel):
    job_id: str
    latest_path: str
    created_at: datetime


class JobLogsResponse(BaseModel):
    job_id: str
    lines: list[str]


class CancelJobResponse(BaseModel):
    job_id: str
    status: str
    cancel_requested: bool
    message: str


class MysqlRecordResponse(BaseModel):
    llid: str
    env_mode: str
    source_url: Optional[str] = None
    source_mimetype: Optional[str] = None
    source_kind: Optional[str] = None
    is_deferred: int | bool
    pdf_page_count: Optional[int] = None
    deferred_reason: Optional[str] = None
    processing_eligible: int | bool
    processing_ineligible_reason: Optional[str] = None
    security_status: Optional[str] = None
    security_scope: Optional[str] = None
    security_engine: Optional[str] = None
    security_checked_at: Optional[datetime] = None
    security_quarantined: int | bool
    security_escalated: int | bool
    security_reason: Optional[str] = None
    sync_status: str
    fast_pipeline_status: str
    deferred_pipeline_status: str
    synced_at: Optional[datetime] = None
    enriched_at: Optional[datetime] = None
    improved_at: Optional[datetime] = None
    background_completed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    source_doc: Optional[dict[str, Any]] = None
    current_doc: Optional[dict[str, Any]] = None

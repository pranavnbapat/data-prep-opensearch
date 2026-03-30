# app.py

import os
from contextlib import asynccontextmanager
from datetime import datetime
from time import tzset

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from uuid import uuid4

load_dotenv()

from api.control_plane import (get_mysql_record, run_backend_sync_job, run_mysql_export_job,
                               run_mysql_current_date_metadata_repair_job,
                               run_mysql_improver_fallback_job, run_mysql_pipeline_job,
                               run_mysql_source_metadata_repair_job)
from api.job_runner import run_pipeline_job
from api.job_store import (JOBS, JOB_LOGS, JOB_LOCK, bucket_parts_now, load_job_from_disk,
                           load_jobs_from_disk, log_file_path, recover_job_tmp_files, write_job_to_disk)
from api.mysql_store import ensure_schema, mysql_enabled
from api.job_store import request_job_cancel
from api.models import (BackendSyncParams, EnvMode, Job, JobStatus, MysqlExportParams,
                        MysqlSourceMetadataRepairParams,
                        MysqlPipelineParams, PipelineRunParams, effective_page_size)

@asynccontextmanager
async def lifespan(app: FastAPI):
    recover_job_tmp_files()
    load_jobs_from_disk()
    if mysql_enabled():
        ensure_schema()

    yield

    try:
        with JOB_LOCK:
            for j in JOBS.values():
                write_job_to_disk(j)
    except Exception:
        pass

app = FastAPI(title="Data Prep", lifespan=lifespan)

_tz = os.getenv("TZ")
if _tz:
    os.environ["TZ"] = _tz
    try:
        tzset()
    except Exception:
        pass

@app.get("/healthz")
def healthz():
    return {"ok": True}


def _ensure_no_active_job_locked() -> None:
    if any(j.status in {JobStatus.queued, JobStatus.running} for j in JOBS.values()):
        raise HTTPException(status_code=409, detail="Another job is already queued or running")

@app.post("/run-pipeline")
def trigger_pipeline_run(params: PipelineRunParams, bg: BackgroundTasks):
    with JOB_LOCK:
        _ensure_no_active_job_locked()

    env_mode_val = (params.env_mode.value if params.env_mode
                    else (os.getenv("ENV_MODE") or "DEV")).upper()
    try:
        resolved_env_mode = EnvMode(env_mode_val)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid ENV_MODE {env_mode_val!r}. Use DEV or PRD")

    byear, bmonth = bucket_parts_now()

    job_id = uuid4().hex
    job = Job(
        id=job_id,
        status=JobStatus.queued,
        created_at=datetime.utcnow(),
        env_mode=resolved_env_mode,
        page_size=effective_page_size(params.page_size),
        bucket_year=byear,
        bucket_month=bmonth,
    )

    with JOB_LOCK:
        _ensure_no_active_job_locked()
        JOBS[job_id] = job

    write_job_to_disk(job)

    bg.add_task(run_pipeline_job, job_id, job.page_size, job.env_mode)
    return {"status": "scheduled", "job_id": job_id}


def _create_job(*, env_mode: EnvMode, page_size: int, job_kind: str, scope: str | None = None) -> Job:
    byear, bmonth = bucket_parts_now()
    job = Job(
        id=uuid4().hex,
        status=JobStatus.queued,
        created_at=datetime.utcnow(),
        env_mode=env_mode,
        page_size=effective_page_size(page_size),
        bucket_year=byear,
        bucket_month=bmonth,
        job_kind=job_kind,
        scope=scope,
    )
    with JOB_LOCK:
        _ensure_no_active_job_locked()
        JOBS[job.id] = job
    write_job_to_disk(job)
    return job


@app.post(
    "/sync/backend-core",
    summary="Sync backend-core into MySQL",
    description=(
        "Runs the downloader against backend-core and stores normalized source records in MySQL. "
        "`page_size` controls backend paging, `env_mode` selects DEV/PRD output scope, "
        "`sort_criteria` is passed through to the backend metadata API, and `dl_workers` controls "
        "downloader per-page processing concurrency."
    ),
)
def trigger_backend_sync(params: BackendSyncParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(
        env_mode=resolved_env_mode,
        page_size=effective_page_size(params.page_size),
        job_kind="backend_sync",
        scope="mysql",
    )
    bg.add_task(
        run_backend_sync_job,
        job.id,
        env_mode=job.env_mode,
        page_size=job.page_size,
        sort_criteria=int(params.sort_criteria or 1),
        dl_workers=int(params.dl_workers or int(os.getenv("DL_MAX_WORKERS", "10"))),
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/sync/backend-core/status")
def backend_sync_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "backend_sync"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/fast",
    summary="Run fast MySQL-backed pipeline",
    description=(
        "Processes non-deferred MySQL records through the existing enricher and improver stages. "
        "`max_docs` caps the batch size, and `llids` can be used to target specific records."
    ),
)
def trigger_fast_pipeline(params: MysqlPipelineParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_pipeline", scope="fast")
    bg.add_task(
        run_mysql_pipeline_job,
        job.id,
        env_mode=job.env_mode,
        deferred_only=False,
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/pipeline/fast/status")
def fast_pipeline_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "fast"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/deferred",
    summary="Run deferred MySQL-backed pipeline",
    description=(
        "Processes deferred MySQL records, typically expensive PDFs above the fast-path threshold. "
        "`max_docs` caps the batch size, and `llids` can override selection for targeted processing."
    ),
)
def trigger_deferred_pipeline(params: MysqlPipelineParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_pipeline", scope="deferred")
    bg.add_task(
        run_mysql_pipeline_job,
        job.id,
        env_mode=job.env_mode,
        deferred_only=True,
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/pipeline/deferred/status")
def deferred_pipeline_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "deferred"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/improver-fallback",
    summary="Run improver-only fallback from MySQL source content",
    description=(
        "Processes records through improver without running enricher first. "
        "If `current_doc_json` already has `ko_content_flat_summarised`, the record is skipped. "
        "Otherwise the job builds the working record from MySQL state, falling back to "
        "`source_doc_json.ko_content_flat` when needed, and writes the improved result back to "
        "`current_doc_json`. By default this is intended for deferred records."
    ),
)
def trigger_improver_fallback(params: MysqlPipelineParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_pipeline", scope="improver_fallback")
    bg.add_task(
        run_mysql_improver_fallback_job,
        job.id,
        env_mode=job.env_mode,
        deferred_only=True,
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/pipeline/improver-fallback/status")
def improver_fallback_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "improver_fallback"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/source/repair-metadata",
    summary="Repair source MIME type and source kind in MySQL",
    description=(
        "Recomputes source-side metadata for existing MySQL rows without touching current_doc_json. "
        "This updates source_doc_json, source_fp, source_url, source_mimetype, and source_kind."
    ),
)
def trigger_source_metadata_repair(params: MysqlSourceMetadataRepairParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_source_metadata_repair", scope="source_metadata")
    bg.add_task(
        run_mysql_source_metadata_repair_job,
        job.id,
        env_mode=job.env_mode,
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/source/repair-metadata/status")
def source_metadata_repair_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_source_metadata_repair"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/current/repair-date-of-completion",
    summary="Repair date_of_completion in current_doc_json from source_doc_json",
    description=(
        "Copies `date_of_completion` and `date_of_completion_source` from source_doc_json into current_doc_json "
        "for rows that already have current_doc_json. Rows with empty current_doc_json are skipped."
    ),
)
def trigger_current_date_metadata_repair(params: MysqlSourceMetadataRepairParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_current_date_metadata_repair", scope="current_date_metadata")
    bg.add_task(
        run_mysql_current_date_metadata_repair_job,
        job.id,
        env_mode=job.env_mode,
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/current/repair-date-of-completion/status")
def current_date_metadata_repair_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_current_date_metadata_repair"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/records/{record_id}/reprocess",
    summary="Reprocess one MySQL-backed record",
    description="Queues one logical-layer record for targeted reprocessing through the fast-path pipeline.",
)
def reprocess_record(record_id: str, params: MysqlPipelineParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_pipeline", scope="reprocess")
    bg.add_task(
        run_mysql_pipeline_job,
        job.id,
        env_mode=job.env_mode,
        deferred_only=False,
        max_docs=1,
        llids=[record_id],
    )
    return {"status": "scheduled", "job_id": job.id, "record_id": record_id}


@app.get("/records/{record_id}")
def get_record(record_id: str, env_mode: EnvMode | None = None):
    resolved_env_mode = env_mode or EnvMode((os.getenv("ENV_MODE") or "DEV").upper())
    record = get_mysql_record(resolved_env_mode, record_id)
    if not record:
        raise HTTPException(status_code=404, detail="record not found")
    return record


@app.post(
    "/exports/final-improved",
    summary="Export final-improved snapshot from MySQL",
    description=(
        "Builds a fresh JSON export from MySQL-backed record state. "
        "By default only processing-eligible records are exported. "
        "If `processed_only=true`, only records with processed state are exported."
    ),
)
def export_final_improved(params: MysqlExportParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_export", scope="final_improved")
    bg.add_task(
        run_mysql_export_job,
        job.id,
        env_mode=job.env_mode,
        processed_only=bool(params.processed_only),
        eligible_only=bool(params.eligible_only),
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get("/exports/final-improved/latest")
def latest_final_improved_export():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_export" and j.latest_path]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        if not jobs:
            raise HTTPException(status_code=404, detail="no mysql export found")
        job = jobs[0]
        return {"job_id": job.id, "latest_path": job.latest_path, "created_at": job.created_at}

@app.get("/jobs")
def list_jobs(env_mode: EnvMode | None = None):
    with JOB_LOCK:
        jobs = JOBS.values()
        if env_mode is not None:
            jobs = [j for j in jobs if j.env_mode == env_mode]
        return [j.model_dump() for j in jobs]

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")

        data = job.model_dump(mode="json")
        data.pop("error_trace", None)
        return data


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    job = request_job_cancel(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job.status in {JobStatus.success, JobStatus.error, JobStatus.canceled}:
        return {
            "job_id": job_id,
            "status": job.status.value,
            "cancel_requested": bool(job.cancel_requested),
            "message": "Job is already finished",
        }

    write_job_to_disk(job)
    return {
        "job_id": job_id,
        "status": job.status.value,
        "cancel_requested": True,
        "message": "Cancellation requested",
    }

@app.get("/jobs/{job_id}/logs")
def get_job_logs(job_id: str, tail: int = 200):
    # Try in-memory buffer first
    buf = JOB_LOGS.get(job_id)
    if buf is not None:
        if tail < 1:
            tail = 1
        tail = min(tail, len(buf))
        return {"job_id": job_id, "lines": list(buf)[-tail:]}

    # Fallback to file (bucketed path)
    job = JOBS.get(job_id)
    if not job:
        job = load_job_from_disk(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        if not isinstance(job, Job):
            raise HTTPException(status_code=500, detail="could not load job metadata")

    log_fp = log_file_path(job)
    if not log_fp.exists():
        raise HTTPException(status_code=404, detail="log file not found")

    try:
        with log_fp.open("r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
        if tail < 1:
            tail = 1
        return {"job_id": job_id, "lines": [ln.rstrip("\n") for ln in lines[-tail:]]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"could not read log file: {e}")

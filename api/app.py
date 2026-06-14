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
                               run_projects_export_job,
                               run_mysql_current_date_metadata_repair_job,
                               run_mysql_improver_fallback_job, run_mysql_pipeline_job,
                               run_mysql_source_metadata_repair_job, run_translations_sync_job)
from api.dependency_probe import probe_dependencies
from api.job_runner import run_pipeline_job
from api.job_store import (JOBS, JOB_LOGS, JOB_LOCK, bucket_parts_now, load_job_from_disk,
                           load_jobs_from_disk, log_file_path, recover_job_tmp_files, write_job_to_disk)
from api.mysql_store import ensure_schema, mysql_enabled
from api.job_store import request_job_cancel
from api.models import (BackendSyncParams, BasicHealthResponse, CancelJobResponse, DependencyProbeResponse, EnvMode, Job,
                        JobLogsResponse, JobStatus, LatestExportResponse, MysqlCurrentMetadataRepairParams,
                        MysqlExportParams, MysqlRecordResponse, MysqlSourceMetadataRepairParams, ProjectsExportParams,
                        ReprocessRecordResponse,
                        ScheduledJobResponse, TranslationsSyncParams,
                        MysqlPipelineParams, PipelineRunParams, effective_page_size)

OPENAPI_TAGS = [
    {
        "name": "Health",
        "description": "Basic service health and external dependency probes.",
    },
    {
        "name": "Sync",
        "description": "Backend-core ingestion and MySQL source-state refresh operations.",
    },
    {
        "name": "Pipeline",
        "description": "Fast, deferred, and improver-fallback processing jobs over MySQL-backed records.",
    },
    {
        "name": "Metadata Repair",
        "description": "Targeted source and current metadata repair operations that avoid rerunning enrichment and LLM stages.",
    },
    {
        "name": "Records",
        "description": "Inspect or reprocess individual MySQL-backed records.",
    },
    {
        "name": "Exports",
        "description": "Generate or inspect final MySQL-backed export snapshots.",
    },
    {
        "name": "Jobs",
        "description": "Inspect queued/running/completed jobs, cancellation, and per-job logs.",
    },
    # {
    #     "name": "Legacy",
    #     "description": "Older combined entrypoints retained for compatibility.",
    # },
]

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

app = FastAPI(title="Data Prep", lifespan=lifespan, openapi_tags=OPENAPI_TAGS)

_tz = os.getenv("TZ")
if _tz:
    os.environ["TZ"] = _tz
    try:
        tzset()
    except Exception:
        pass

@app.get(
    "/healthz",
    tags=["Health"],
    summary="Basic API health check",
    description="Returns a minimal in-process health response for the FastAPI app itself.",
    response_description="Simple `{ok: true}` payload when the API process is up.",
    response_model=BasicHealthResponse,
)
def healthz():
    return {"ok": True}


@app.get(
    "/health/dependencies",
    tags=["Health"],
    summary="Probe external service dependencies",
    description=(
        "Checks the external services this app depends on, including backend-core and any "
        "currently configured enrichment/security/LLM services. Disabled or unconfigured "
        "services are reported as skipped. "
        "The optional `env_mode` query parameter chooses which backend-core environment to test."
    ),
    response_description="Overall dependency probe result plus one status object per configured external service.",
    response_model=DependencyProbeResponse,
)
def dependencies_health(env_mode: EnvMode | None = None):
    resolved_env_mode = env_mode or EnvMode((os.getenv("ENV_MODE") or "DEV").upper())
    return probe_dependencies(resolved_env_mode.value)


def _ensure_no_active_job_locked() -> None:
    if any(j.status in {JobStatus.queued, JobStatus.running} for j in JOBS.values()):
        raise HTTPException(status_code=409, detail="Another job is already queued or running")

# Legacy endpoint retained for compatibility, but hidden from OpenAPI docs.
@app.post(
    "/run-pipeline",
    tags=["Legacy"],
    summary="Run legacy combined pipeline",
    description=(
        "Queues the older combined file-output pipeline entrypoint. "
        "This is retained for compatibility; the MySQL-backed sync and pipeline endpoints are the primary flow."
    ),
    response_description="Scheduling confirmation with the queued job ID.",
    response_model=ScheduledJobResponse,
    include_in_schema=False,
)
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
    tags=["Sync"],
    summary="Sync backend-core into MySQL — scheduled daily 02:00 UTC",
    description=(
        "Runs the downloader against backend-core and stores normalized source records in MySQL. "
        "`page_size` controls backend paging, `env_mode` selects DEV/PRD output scope, "
        "`sort_criteria` is passed through to the backend metadata API, and `dl_workers` controls "
        "downloader per-page processing concurrency."
    ),
    response_description="Scheduling confirmation with the queued backend-sync job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/sync/backend-core/status",
    tags=["Sync"],
    summary="List recent backend-sync jobs",
    description="Returns the most recent backend-sync jobs with their current status and recorded progress.",
    response_description="Up to 10 recent backend-sync job objects.",
    response_model=list[Job],
)
def backend_sync_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "backend_sync"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/sync/translations",
    tags=["Sync"],
    summary="Sync KO metadata translations into MySQL — scheduled daily 04:00 UTC",
    description=(
        "Fetches KO metadata translations (title, subtitle, description, keywords) from backend-core "
        "and merges them into existing MySQL records as a `metadata_translations` block, keyed by language. "
        "This is fetch-only and incremental: it never runs the enricher or improver, and records whose "
        "translation fingerprint is unchanged are skipped without a write. The block is merged into both "
        "`source_doc_json` and `current_doc_json`, so the next `/exports/final-improved` includes it. "
        "Set `only_missing` to skip records that already have translations, `max_docs` to cap the batch, "
        "and `llids` to target specific records."
    ),
    response_description="Scheduling confirmation with the queued translations-sync job ID.",
    response_model=ScheduledJobResponse,
)
def trigger_translations_sync(params: TranslationsSyncParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="translations_sync", scope="mysql")
    bg.add_task(
        run_translations_sync_job,
        job.id,
        env_mode=job.env_mode,
        only_missing=bool(params.only_missing),
        max_docs=params.max_docs,
        llids=params.llids,
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get(
    "/sync/translations/status",
    tags=["Sync"],
    summary="List recent translations-sync jobs",
    description="Returns the most recent translations-sync jobs with their current status and recorded progress.",
    response_description="Up to 10 recent translations-sync job objects.",
    response_model=list[Job],
)
def translations_sync_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "translations_sync"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/fast",
    tags=["Pipeline"],
    summary="Run fast MySQL-backed pipeline — scheduled daily 05:30 UTC",
    description=(
        "Processes non-deferred MySQL records through the existing enricher and improver stages. "
        "`max_docs` caps the batch size, and `llids` can be used to target specific records."
    ),
    response_description="Scheduling confirmation with the queued fast-pipeline job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/pipeline/fast/status",
    tags=["Pipeline"],
    summary="List recent fast-pipeline jobs",
    description="Returns the most recent fast MySQL pipeline jobs and their current processing status.",
    response_description="Up to 10 recent fast-pipeline job objects.",
    response_model=list[Job],
)
def fast_pipeline_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "fast"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/deferred",
    tags=["Pipeline"],
    summary="Run deferred MySQL-backed pipeline — scheduled Fridays 22:00 UTC",
    description=(
        "Processes deferred MySQL records, typically expensive PDFs above the fast-path threshold. "
        "`max_docs` caps the batch size, and `llids` can override selection for targeted processing."
    ),
    response_description="Scheduling confirmation with the queued deferred-pipeline job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/pipeline/deferred/status",
    tags=["Pipeline"],
    summary="List recent deferred-pipeline jobs",
    description="Returns the most recent deferred MySQL pipeline jobs and their current processing status.",
    response_description="Up to 10 recent deferred-pipeline job objects.",
    response_model=list[Job],
)
def deferred_pipeline_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "deferred"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/pipeline/improver-fallback",
    tags=["Pipeline"],
    summary="Run improver-only fallback from MySQL source content",
    description=(
        "Processes records through improver without running enricher first. "
        "If `current_doc_json` already has `ko_content_flat_summarised`, the record is skipped. "
        "Otherwise the job builds the working record from MySQL state, falling back to "
        "`source_doc_json.ko_content_flat` when needed, and writes the improved result back to "
        "`current_doc_json`. By default this is intended for deferred records."
    ),
    response_description="Scheduling confirmation with the queued improver-fallback job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/pipeline/improver-fallback/status",
    tags=["Pipeline"],
    summary="List recent improver-fallback jobs",
    description="Returns the most recent improver-only fallback jobs and their current processing status.",
    response_description="Up to 10 recent improver-fallback job objects.",
    response_model=list[Job],
)
def improver_fallback_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_pipeline" and j.scope == "improver_fallback"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/source/repair-metadata",
    tags=["Metadata Repair"],
    summary="Repair source MIME type and source kind in MySQL",
    description=(
        "Recomputes source-side metadata for existing MySQL rows without touching current_doc_json. "
        "This updates source_doc_json, source_fp, source_url, source_mimetype, and source_kind."
    ),
    response_description="Scheduling confirmation with the queued source-metadata-repair job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/source/repair-metadata/status",
    tags=["Metadata Repair"],
    summary="List recent source metadata repair jobs",
    description="Returns the most recent source-side metadata repair jobs and their current status.",
    response_description="Up to 10 recent source-metadata-repair job objects.",
    response_model=list[Job],
)
def source_metadata_repair_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_source_metadata_repair"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/current/repair-metadata",
    tags=["Metadata Repair"],
    summary="Repair selected current_doc_json metadata from source_doc_json",
    description=(
        "Copies selected metadata fields from source_doc_json into current_doc_json "
        "for rows that already have current_doc_json. "
        "Supported fields are `date_of_completion` and `project_slug`. "
        "`date_of_completion_source` is updated together with `date_of_completion`. "
        "Rows with empty current_doc_json are skipped."
    ),
    response_description="Scheduling confirmation with the queued current-metadata-repair job ID.",
    response_model=ScheduledJobResponse,
)
@app.post(
    "/current/repair-date-of-completion",
    include_in_schema=False,
)
def trigger_current_metadata_repair(params: MysqlCurrentMetadataRepairParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    job = _create_job(env_mode=resolved_env_mode, page_size=100, job_kind="mysql_current_date_metadata_repair", scope="current_date_metadata")
    bg.add_task(
        run_mysql_current_date_metadata_repair_job,
        job.id,
        env_mode=job.env_mode,
        max_docs=params.max_docs,
        llids=params.llids,
        fields=[field.value for field in params.fields],
    )
    return {"status": "scheduled", "job_id": job.id}


@app.get(
    "/current/repair-metadata/status",
    tags=["Metadata Repair"],
    summary="List recent current metadata repair jobs",
    description="Returns the most recent current metadata repair jobs and their current status.",
    response_description="Up to 10 recent current-metadata-repair job objects.",
    response_model=list[Job],
)
@app.get("/current/repair-date-of-completion/status", include_in_schema=False)
def current_metadata_repair_status():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_current_date_metadata_repair"]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return [j.model_dump(mode="json") for j in jobs[:10]]


@app.post(
    "/records/{record_id}/reprocess",
    tags=["Records"],
    summary="Reprocess one MySQL-backed record",
    description="Queues one logical-layer record for targeted reprocessing through the fast-path pipeline.",
    response_description="Scheduling confirmation with the queued reprocess job ID and target record ID.",
    response_model=ReprocessRecordResponse,
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


@app.get(
    "/records/{record_id}",
    tags=["Records"],
    summary="Fetch one MySQL-backed record",
    description=(
        "Returns the stored MySQL row for one logical-layer record, including source and current JSON state "
        "for the selected environment."
    ),
    response_description="One MySQL-backed record payload for the requested logical-layer ID.",
    response_model=MysqlRecordResponse,
)
def get_record(record_id: str, env_mode: EnvMode | None = None):
    resolved_env_mode = env_mode or EnvMode((os.getenv("ENV_MODE") or "DEV").upper())
    record = get_mysql_record(resolved_env_mode, record_id)
    if not record:
        raise HTTPException(status_code=404, detail="record not found")
    return record


@app.post(
    "/exports/final-improved",
    tags=["Exports"],
    summary="Export final-improved snapshot from MySQL — scheduled daily 08:00 UTC",
    description=(
        "Builds a fresh JSON export from MySQL-backed record state. "
        "By default only processing-eligible records are exported. "
        "If `processed_only=true`, only records with processed state are exported."
    ),
    response_description="Scheduling confirmation with the queued export job ID.",
    response_model=ScheduledJobResponse,
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


@app.get(
    "/exports/final-improved/latest",
    tags=["Exports"],
    summary="Get the latest final-improved export path",
    description="Returns the latest completed MySQL export job that produced a final-improved snapshot file.",
    response_description="Latest export job ID, created time, and output path.",
    response_model=LatestExportResponse,
)
def latest_final_improved_export():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "mysql_export" and j.latest_path]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        if not jobs:
            raise HTTPException(status_code=404, detail="no mysql export found")
        job = jobs[0]
        return {"job_id": job.id, "latest_path": job.latest_path, "created_at": job.created_at}


@app.post(
    "/projects/export",
    tags=["Exports"],
    summary="Export backend-core projects snapshot — scheduled daily 12:00 UTC",
    description=(
        "Fetches all backend-core projects for the selected DEV or PRD environment via paginated "
        "`POST /api/logical_layer/projects/search`, keeps only the curated project fields, and writes "
        "a JSON snapshot under the standard output directory."
    ),
    response_description="Scheduling confirmation with the queued project-export job ID.",
    response_model=ScheduledJobResponse,
)
def export_projects(params: ProjectsExportParams, bg: BackgroundTasks):
    env_mode_val = (params.env_mode.value if params.env_mode else (os.getenv("ENV_MODE") or "DEV")).upper()
    resolved_env_mode = EnvMode(env_mode_val)
    page_size = params.page_size or 200
    job = _create_job(env_mode=resolved_env_mode, page_size=page_size, job_kind="projects_export", scope="projects")
    bg.add_task(run_projects_export_job, job.id, env_mode=job.env_mode, page_size=job.page_size)
    return {"status": "scheduled", "job_id": job.id}


@app.get(
    "/projects/export/latest",
    tags=["Exports"],
    summary="Get the latest backend-core projects export path",
    description="Returns the latest completed projects export job and the path to the generated JSON snapshot.",
    response_description="Latest project export job ID, created time, and output path.",
    response_model=LatestExportResponse,
)
def latest_projects_export():
    with JOB_LOCK:
        jobs = [j for j in JOBS.values() if j.job_kind == "projects_export" and j.latest_path]
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        if not jobs:
            raise HTTPException(status_code=404, detail="no projects export found")
        job = jobs[0]
        return {"job_id": job.id, "latest_path": job.latest_path, "created_at": job.created_at}

@app.get(
    "/jobs",
    tags=["Jobs"],
    summary="List known jobs",
    description="Returns queued, running, and completed jobs currently known to the API. Optionally filters by `env_mode`.",
    response_description="List of job objects currently tracked by the API.",
    response_model=list[Job],
)
def list_jobs(env_mode: EnvMode | None = None):
    with JOB_LOCK:
        jobs = JOBS.values()
        if env_mode is not None:
            jobs = [j for j in jobs if j.env_mode == env_mode]
        return [j.model_dump() for j in jobs]

@app.get(
    "/jobs/{job_id}",
    tags=["Jobs"],
    summary="Get one job",
    description="Returns the current status and metadata for one job ID.",
    response_description="One job object without the internal error trace field.",
    response_model=Job,
)
def get_job(job_id: str):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")

        data = job.model_dump(mode="json")
        data.pop("error_trace", None)
        return data


@app.post(
    "/jobs/{job_id}/cancel",
    tags=["Jobs"],
    summary="Request job cancellation",
    description="Marks a queued or running job for cancellation. If the job already finished, the current terminal state is returned.",
    response_description="Cancellation acknowledgement for the target job.",
    response_model=CancelJobResponse,
)
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

@app.get(
    "/jobs/{job_id}/logs",
    tags=["Jobs"],
    summary="Get recent job log lines",
    description=(
        "Returns recent log lines for one job. The optional `tail` query parameter controls how many lines "
        "to return, defaulting to 200."
    ),
    response_description="Job log payload with the requested tail of log lines.",
    response_model=JobLogsResponse,
)
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

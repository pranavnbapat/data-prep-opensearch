# app.py

import os
from contextlib import asynccontextmanager
from datetime import datetime
from time import tzset

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from uuid import uuid4

load_dotenv()

from api.job_runner import run_pipeline_job
from api.job_store import (JOBS, JOB_LOGS, JOB_LOCK, bucket_parts_now, load_job_from_disk,
                           load_jobs_from_disk, log_file_path, recover_job_tmp_files, write_job_to_disk)
from api.models import EnvMode, Job, JobStatus, PipelineRunParams, effective_page_size

@asynccontextmanager
async def lifespan(app: FastAPI):
    recover_job_tmp_files()
    load_jobs_from_disk()

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

@app.post("/run-pipeline")
def trigger_pipeline_run(params: PipelineRunParams, bg: BackgroundTasks):
    with JOB_LOCK:
        if any(j.status == JobStatus.running for j in JOBS.values()):
            raise HTTPException(status_code=409, detail="Another job is already running")

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
        JOBS[job_id] = job

    write_job_to_disk(job)

    bg.add_task(run_pipeline_job, job_id, job.page_size, job.env_mode)
    return {"status": "scheduled", "job_id": job_id}

@app.get("/jobs")
def list_jobs():
    with JOB_LOCK:
        return [j.model_dump() for j in JOBS.values()]

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")

        data = job.model_dump(mode="json")
        data.pop("error_trace", None)
        return data

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

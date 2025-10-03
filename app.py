# app.py

import logging
import os
import traceback

from collections import deque
from datetime import datetime
from enum import Enum
from threading import Lock
from time import tzset
from typing import Optional, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, field_validator
from uuid import uuid4

from download_mongodb_data import get_ko_metadata, select_environment
load_dotenv()
app = FastAPI(title="Data Prep")

MAX_PAGE_SIZE = 100

_tz = os.getenv("TZ")
if _tz:
    os.environ["TZ"] = _tz
    try:
        tzset()
    except Exception:
        pass

class EnvMode(str, Enum):
    DEV = "DEV"
    PRD = "PRD"

class RunParams(BaseModel):
    page_size: Optional[int] = None
    env_mode: Optional[EnvMode] = None

    model_config = {"extra": "ignore"}

    @field_validator("page_size")
    @classmethod
    def clamp_page_size(cls, v: Optional[int]) -> Optional[int]:
        # Let None mean "use default from env (or 100)"
        if v is None:
            return None
        # If user sends 0 or negative, treat as None so we fall back to default 100
        if v <= 0:
            return None
        # Cap to 100
        return min(v, MAX_PAGE_SIZE)

def _effective_page_size(val: Optional[int]) -> int:
    # Default from env (or 100), then cap to 100, min 1 (defensive)
    env_default = int(os.getenv("DL_PAGE_SIZE", str(MAX_PAGE_SIZE)))
    ps = env_default if val is None else val
    if ps < 1:
        ps = 1
    if ps > MAX_PAGE_SIZE:
        ps = MAX_PAGE_SIZE
    return ps


# ---------------- Job tracking ----------------

class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
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
    emitted: Optional[int] = None
    dropped: Optional[int] = None
    output_file: Optional[str] = None
    error: Optional[str] = None

# global registries
JOBS: Dict[str, Job] = {}
JOB_LOGS: Dict[str, deque[str]] = {}
JOB_LOCK = Lock()

class PerJobLogHandler(logging.Handler):
    def __init__(self, job_id: str, max_lines: int = 1000, persist_dir: Optional[str] = "output/job-logs"):
        super().__init__()
        self.job_id = job_id
        self.buf = JOB_LOGS.setdefault(job_id, deque(maxlen=max_lines))
        self.persist_fp = None
        self.log_path = None
        if persist_dir:
            try:
                os.makedirs(persist_dir, exist_ok=True)
                self.log_path = os.path.join(persist_dir, f"{job_id}.log")
                self.persist_fp = open(self.log_path, "a", encoding="utf-8")
            except Exception as e:
                self.persist_fp = None
                self.log_path = None
                logging.warning("PerJobLogHandler: file persistence disabled: %s", e)

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        line = f"{datetime.utcnow().isoformat()}Z {record.levelname} {msg}"
        self.buf.append(line)
        if self.persist_fp:
            try:
                self.persist_fp.write(line + "\n")
                self.persist_fp.flush()
            except Exception:
                pass

    def close(self):
        try:
            if self.persist_fp:
                self.persist_fp.close()
        finally:
            super().close()

@app.get("/healthz")
def healthz():
    return {"ok": True}

def _run_job(job_id: str, page_size: int, env_mode: EnvMode):
    with JOB_LOCK:
        job = JOBS[job_id]
        job.status = JobStatus.running
        job.started_at = datetime.utcnow()

    # attach per-job log capture
    handler = PerJobLogHandler(job_id)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        # switch backend credentials/host for this run
        select_environment(env_mode.value)

        # static knobs from env
        mw = int(os.getenv("DL_MAX_WORKERS", "10"))
        sc = int(os.getenv("DL_SORT_CRITERIA", "1"))
        ps = _effective_page_size(page_size)

        logging.info("Starting data-prep (env=%s, page_size=%s, workers=%s, sort=%s)", env_mode, ps, mw, sc)
        summary = get_ko_metadata(max_workers=mw, page_size=ps, sort_criteria=sc)

        with JOB_LOCK:
            job.emitted = summary.get("emitted")
            job.dropped = summary.get("dropped")
            job.output_file = summary.get("output_file")
            job.status = JobStatus.success
            job.finished_at = datetime.utcnow()
        logging.info("Data-prep finished (emitted=%s, dropped=%s, file=%r)",
                     job.emitted, job.dropped, job.output_file)
    except Exception as e:
        err_txt = f"{e.__class__.__name__}: {e}"
        tb = traceback.format_exc()
        with JOB_LOCK:
            job.status = JobStatus.error
            job.error = err_txt + "\n" + tb
            job.finished_at = datetime.utcnow()
        logging.exception("Data-prep failed: %s", err_txt)
    finally:
        root_logger.removeHandler(handler)
        handler.close()

@app.post("/run")
def trigger_run(params: RunParams, bg: BackgroundTasks):
    # prevent concurrent runs
    with JOB_LOCK:
        if any(j.status == JobStatus.running for j in JOBS.values()):
            raise HTTPException(status_code=409, detail="Another job is already running")

    env_mode_val = (params.env_mode.value if params.env_mode
                    else (os.getenv("ENV_MODE") or "DEV")).upper()
    try:
        resolved_env_mode = EnvMode(env_mode_val)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid ENV_MODE {env_mode_val!r}. Use DEV or PRD")

    job_id = uuid4().hex
    job = Job(
        id=job_id,
        status=JobStatus.queued,
        created_at=datetime.utcnow(),
        env_mode=resolved_env_mode,
        page_size=_effective_page_size(params.page_size)
    )
    with JOB_LOCK:
        JOBS[job_id] = job

    bg.add_task(_run_job, job_id, job.page_size, job.env_mode)
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
        return job.model_dump()

@app.get("/jobs/{job_id}/logs")
def get_job_logs(job_id: str, tail: int = 200):
    buf = JOB_LOGS.get(job_id)
    if buf is None:
        raise HTTPException(status_code=404, detail="job not found")
    if tail < 1:
        tail = 1
    tail = min(tail, len(buf))
    return {"job_id": job_id, "lines": list(buf)[-tail:]}

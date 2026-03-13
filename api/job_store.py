from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional

from api.models import Job, JobStatus


JOBS: dict[str, Job] = {}
JOB_LOGS: dict[str, deque[str]] = {}
JOB_LOCK = Lock()


def bucket_parts_now() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y"), now.strftime("%m")


def job_meta_path(job: Job) -> Path:
    return Path("output") / job.env_mode.value / job.bucket_year / job.bucket_month / "jobs" / f"{job.id}.json"


def log_file_path(job: Job) -> Path:
    return Path("output") / job.env_mode.value / job.bucket_year / job.bucket_month / "job-logs" / f"{job.id}.log"


def ensure_parent(fp: Path) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)


def write_job_to_disk(job: Job) -> None:
    try:
        fp = job_meta_path(job)
        ensure_parent(fp)
        tmp = fp.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(job.model_dump(mode="json"), fh, ensure_ascii=False, indent=2)
        tmp.replace(fp)
    except Exception as e:
        logging.warning("Could not persist job %s: %s", job.id, e)


def scan_all_job_files() -> list[Path]:
    return list(Path("output").glob("*/*/*/jobs/*.json"))


def recover_job_tmp_files() -> None:
    for tmp in Path("output").glob("*/*/*/jobs/*.json.tmp"):
        final = tmp.with_suffix(".json")
        try:
            with tmp.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict) or "id" not in data:
                tmp.unlink(missing_ok=True)
                continue
            tmp.replace(final)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass


def load_jobs_from_disk() -> None:
    files = scan_all_job_files()
    if not files:
        return

    now = datetime.utcnow()
    loaded: dict[str, Job] = {}
    for fp in files:
        try:
            with fp.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            job = Job.model_validate(data)
            if job.status == JobStatus.running:
                job.status = JobStatus.error
                job.error = (job.error or "") + "\nServer restarted while job was running."
                job.finished_at = now
            loaded[job.id] = job
        except Exception as e:
            logging.warning("Could not load job file %s: %s", fp, e)

    with JOB_LOCK:
        JOBS.clear()
        JOBS.update(loaded)


def load_job_from_disk(job_id: str) -> Optional[Job]:
    files = [p for p in scan_all_job_files() if p.name == f"{job_id}.json"]
    if not files:
        return None

    with files[0].open("r", encoding="utf-8") as fh:
        job = Job.model_validate(json.load(fh))

    with JOB_LOCK:
        JOBS[job_id] = job
    return job


class PerJobLogHandler(logging.Handler):
    def __init__(self, job_id: str, max_lines: int = 1000, persist_dir: Optional[str] = "output/job-logs"):
        super().__init__()
        self.job_id = job_id
        self.buf = JOB_LOGS.setdefault(job_id, deque(maxlen=max_lines))
        self.persist_fp = None
        if persist_dir:
            try:
                Path(persist_dir).mkdir(parents=True, exist_ok=True)
                self.persist_fp = open(Path(persist_dir) / f"{job_id}.log", "a", encoding="utf-8")
            except Exception as e:
                self.persist_fp = None
                logging.warning("PerJobLogHandler: file persistence disabled: %s", e)

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        for part in msg.splitlines() or [""]:
            line = f"{datetime.utcnow().isoformat()}Z {record.levelname} {part}"
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

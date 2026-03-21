from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Event, Lock
from typing import Optional

from api.models import Job, JobStatus


JOBS: dict[str, Job] = {}
JOB_LOGS: dict[str, deque[str]] = {}
JOB_CANCEL_FLAGS: dict[str, Event] = {}
JOB_LOCK = Lock()


def bucket_parts_now() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y"), now.strftime("%m")


def job_meta_path(job: Job) -> Path:
    return Path("output") / job.env_mode.value / job.bucket_year / job.bucket_month / "jobs" / f"{job.id}.json"


def log_file_path(job: Job) -> Path:
    return Path("output") / job.env_mode.value / job.bucket_year / job.bucket_month / "job-logs" / f"{job.id}.log"


def run_log_path(job: Job) -> Path:
    return Path("output") / job.env_mode.value / job.bucket_year / job.bucket_month / "logs.txt"


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
            if job.status in {JobStatus.running, JobStatus.queued}:
                previous = job.status
                job.status = JobStatus.error
                if previous == JobStatus.running:
                    msg = "Server restarted while job was running."
                else:
                    msg = "Server restarted while job was still queued."
                job.error = ((job.error or "").rstrip() + "\n" + msg).strip()
                job.finished_at = now
            loaded[job.id] = job
        except Exception as e:
            logging.warning("Could not load job file %s: %s", fp, e)

    with JOB_LOCK:
        JOBS.clear()
        JOBS.update(loaded)
        JOB_CANCEL_FLAGS.clear()


def load_job_from_disk(job_id: str) -> Optional[Job]:
    files = [p for p in scan_all_job_files() if p.name == f"{job_id}.json"]
    if not files:
        return None

    with files[0].open("r", encoding="utf-8") as fh:
        job = Job.model_validate(json.load(fh))

    with JOB_LOCK:
        JOBS[job_id] = job
    return job


def get_cancel_event(job_id: str) -> Event:
    with JOB_LOCK:
        return JOB_CANCEL_FLAGS.setdefault(job_id, Event())


def is_cancel_requested(job_id: str) -> bool:
    return get_cancel_event(job_id).is_set()


def request_job_cancel(job_id: str) -> Optional[Job]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            return None
        if job.status in {JobStatus.success, JobStatus.error, JobStatus.canceled}:
            return job
        JOB_CANCEL_FLAGS.setdefault(job_id, Event()).set()
        job.cancel_requested = True
        if job.status == JobStatus.queued:
            job.status = JobStatus.canceled
            job.finished_at = datetime.utcnow()
        return job


def clear_job_cancel(job_id: str) -> None:
    with JOB_LOCK:
        JOB_CANCEL_FLAGS.pop(job_id, None)


class PerJobLogHandler(logging.Handler):
    def __init__(
        self,
        job_id: str,
        max_lines: int = 1000,
        persist_dir: Optional[str] = "output/job-logs",
        mirror_path: Optional[Path] = None,
        mirror_mode: str = "a",
    ):
        super().__init__()
        self.job_id = job_id
        self.buf = JOB_LOGS.setdefault(job_id, deque(maxlen=max_lines))
        self.persist_fp = None
        self.mirror_fp = None
        if persist_dir:
            try:
                Path(persist_dir).mkdir(parents=True, exist_ok=True)
                self.persist_fp = open(Path(persist_dir) / f"{job_id}.log", "a", encoding="utf-8")
            except Exception as e:
                self.persist_fp = None
                logging.warning("PerJobLogHandler: file persistence disabled: %s", e)
        if mirror_path is not None:
            try:
                ensure_parent(mirror_path)
                self.mirror_fp = open(mirror_path, mirror_mode, encoding="utf-8")
            except Exception as e:
                self.mirror_fp = None
                logging.warning("PerJobLogHandler: mirror file persistence disabled: %s", e)

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
            if self.mirror_fp:
                try:
                    self.mirror_fp.write(line + "\n")
                    self.mirror_fp.flush()
                except Exception:
                    pass

    def close(self):
        try:
            if self.persist_fp:
                self.persist_fp.close()
            if self.mirror_fp:
                self.mirror_fp.close()
        finally:
            super().close()

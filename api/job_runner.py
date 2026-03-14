from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime

from common.cancellation import JobCancelled
from api.job_store import (JOBS, JOB_LOCK, PerJobLogHandler, clear_job_cancel, get_cancel_event,
                           log_file_path, write_job_to_disk)
from api.models import EnvMode, JobStatus
from pipeline.orchestrator import run_pipeline


def clean_error_message(e: Exception) -> str:
    txt = str(e).strip()
    if "Traceback (most recent call last):" in txt:
        txt = txt.split("Traceback (most recent call last):", 1)[0].rstrip()
    return txt


def run_pipeline_job(job_id: str, page_size: int, env_mode: EnvMode) -> None:
    cancel_event = get_cancel_event(job_id)
    with JOB_LOCK:
        job = JOBS[job_id]
        if cancel_event.is_set() or job.status == JobStatus.canceled:
            job.status = JobStatus.canceled
            job.finished_at = datetime.utcnow()
            write_job_to_disk(job)
            clear_job_cancel(job_id)
            return
        job.status = JobStatus.running
        job.started_at = datetime.utcnow()
    write_job_to_disk(job)

    log_dir = log_file_path(job).parent
    handler = PerJobLogHandler(job_id, persist_dir=str(log_dir))
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        dl_workers = int(os.getenv("DL_MAX_WORKERS", "10"))
        sort_criteria = int(os.getenv("DL_SORT_CRITERIA", "1"))
        extractor_workers = int(os.getenv("EXTRACTOR_MAX_WORKERS", "4"))
        transcribe_workers = int(os.getenv("TRANSCRIBE_MAX_WORKERS", "3"))
        max_chars_env = int(os.getenv("PIPELINE_MAX_CHARS", "0"))
        max_chars = max_chars_env if max_chars_env > 0 else None
        output_root = os.getenv("OUTPUT_ROOT", "output")

        logging.info("Starting pipeline (env=%s, page_size=%s)", env_mode.value, page_size)

        res = run_pipeline(
            env_mode=env_mode.value,
            page_size=page_size,
            sort_criteria=sort_criteria,
            dl_workers=dl_workers,
            extractor_workers=extractor_workers,
            transcribe_workers=transcribe_workers,
            max_chars=max_chars,
            output_root=output_root,
            should_cancel=cancel_event.is_set,
        )

        with JOB_LOCK:
            job.status = JobStatus.success
            job.finished_at = datetime.utcnow()
            job.final_path = res.final_path
            job.report_path = res.report_path
            job.latest_path = res.latest_path
            job.pipeline_stats = res.stats
            dl_stats = (res.stats or {}).get("downloader") or {}
            job.emitted = dl_stats.get("emitted")
            job.dropped = dl_stats.get("dropped")

        write_job_to_disk(job)
        logging.info("Pipeline finished (final=%r report=%r latest=%r)", res.final_path, res.report_path, res.latest_path)

    except JobCancelled as e:
        with JOB_LOCK:
            job.status = JobStatus.canceled
            job.error = str(e)
            job.finished_at = datetime.utcnow()
        write_job_to_disk(job)
        logging.warning("Pipeline canceled: %s", e)

    except Exception as e:
        clean = f"{e.__class__.__name__}: {clean_error_message(e)}"
        tb = traceback.format_exc()
        with JOB_LOCK:
            job.status = JobStatus.error
            job.error = clean
            job.error_trace = tb
            job.finished_at = datetime.utcnow()
        write_job_to_disk(job)
        logging.error("Pipeline failed: %s", clean)
        logging.debug("Pipeline traceback:\n%s", tb)

    finally:
        root_logger.removeHandler(handler)
        handler.close()
        clear_job_cancel(job_id)

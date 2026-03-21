from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.job_store import JOBS, JOB_LOCK, clear_job_cancel, get_cancel_event, write_job_to_disk
from api.models import EnvMode, JobStatus
from api.mysql_store import (ensure_schema, export_docs, fetch_record, fetch_source_docs,
                             summarize_status, upsert_current_docs, upsert_processed_docs, upsert_source_docs)
from common.cancellation import JobCancelled
from pipeline.io import atomic_write_json, output_dir, run_stamp
from stages.downloader.service import download_and_prepare
from stages.enricher.core import enrich_docs_via_routes
from stages.enricher.utils import compute_enrich_inputs_fp
from stages.improver.engine import improve_doc_in_place
from stages.improver.utils import carry_forward_previous_improvements, compute_improver_fp, should_skip_improve


logger = logging.getLogger(__name__)


def _has_summary(doc: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(doc, dict):
        return False
    text = doc.get("ko_content_flat_summarised")
    return isinstance(text, str) and bool(text.strip())


def _should_skip_mysql_record(source_doc: Dict[str, Any], prev_current: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(prev_current, dict):
        return False
    if int(prev_current.get("improved") or 0) != 1:
        return False
    if not _has_summary(prev_current):
        return False

    cur_enrich_fp = compute_enrich_inputs_fp(source_doc)
    prev_enrich_fp = prev_current.get("_enrich_inputs_fp")
    if not isinstance(prev_enrich_fp, str) or not prev_enrich_fp.strip():
        prev_enrich_fp = compute_enrich_inputs_fp(prev_current)
    if cur_enrich_fp != prev_enrich_fp:
        return False

    cur_improver_fp = compute_improver_fp(prev_current)
    prev_improver_fp = prev_current.get("_improver_fp")
    if not isinstance(prev_improver_fp, str) or not prev_improver_fp.strip():
        prev_improver_fp = cur_improver_fp
    return cur_improver_fp == prev_improver_fp


def _set_job_running(job_id: str) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job.status = JobStatus.running
        job.started_at = datetime.utcnow()
    write_job_to_disk(job)


def _set_job_done(job_id: str, *, details: Dict[str, Any], latest_path: Optional[str] = None) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job.status = JobStatus.success
        job.finished_at = datetime.utcnow()
        job.pipeline_stats = details
        job.latest_path = latest_path
    write_job_to_disk(job)


def _set_job_error(job_id: str, exc: Exception) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job.status = JobStatus.error
        job.finished_at = datetime.utcnow()
        job.error = f"{exc.__class__.__name__}: {exc}"
    write_job_to_disk(job)


def _set_job_canceled(job_id: str, exc: Exception) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job.status = JobStatus.canceled
        job.finished_at = datetime.utcnow()
        job.error = f"{exc.__class__.__name__}: {exc}"
    write_job_to_disk(job)


def run_backend_sync_job(job_id: str, *, env_mode: EnvMode, page_size: int, sort_criteria: int, dl_workers: int) -> None:
    _set_job_running(job_id)
    try:
        ensure_schema()
        sync_acc = {"synced": 0, "deferred": 0, "changed": 0, "unchanged": 0, "pages_written": 0}

        def on_page(docs_page: List[Dict[str, Any]], page_stats: Dict[str, Any]) -> None:
            if not docs_page:
                return
            page_res = upsert_source_docs(env_mode=env_mode.value, docs=docs_page)
            sync_acc["synced"] += int(page_res.get("synced") or 0)
            sync_acc["deferred"] += int(page_res.get("deferred") or 0)
            sync_acc["changed"] += int(page_res.get("changed") or 0)
            sync_acc["unchanged"] += int(page_res.get("unchanged") or 0)
            sync_acc["pages_written"] += 1
            with JOB_LOCK:
                job = JOBS[job_id]
                job.pipeline_stats = {
                    "downloader_page": page_stats,
                    "mysql_sync": dict(sync_acc),
                    "mysql_status": summarize_status(env_mode=env_mode.value),
                }
            write_job_to_disk(JOBS[job_id])

        dl = download_and_prepare(
            env_mode=env_mode.value,
            page_size=page_size,
            sort_criteria=sort_criteria,
            max_workers=dl_workers,
            prev_index={},
            page_callback=on_page,
            use_lock=False,
        )
        details = {
            "downloader": dl.stats,
            "mysql_sync": dict(sync_acc),
            "mysql_status": summarize_status(env_mode=env_mode.value),
        }
        _set_job_done(job_id, details=details)
    except Exception as e:
        _set_job_error(job_id, e)


def _load_docs_from_stage_output(out_path: Optional[str], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(out_path, str) or not out_path:
        return fallback
    try:
        payload = json.loads(Path(out_path).read_text(encoding="utf-8"))
        docs = payload.get("docs") if isinstance(payload, dict) else None
        return docs if isinstance(docs, list) else fallback
    except Exception:
        return fallback


def run_mysql_pipeline_job(
    job_id: str,
    *,
    env_mode: EnvMode,
    deferred_only: bool,
    max_docs: Optional[int],
    llids: Optional[List[str]] = None,
) -> None:
    cancel_event = get_cancel_event(job_id)
    _set_job_running(job_id)
    try:
        ensure_schema()
        mysql_progress = {"updated": 0, "unchanged": 0}
        docs = fetch_source_docs(
            env_mode=env_mode.value,
            deferred_only=deferred_only,
            llids=llids,
            max_docs=max_docs,
        )
        if not docs:
            _set_job_done(job_id, details={"selected_docs": 0, "notes": "no matching mysql records"})
            return

        output_root = os.getenv("OUTPUT_ROOT", "output")
        exw = int(os.getenv("EXTRACTOR_MAX_WORKERS", "4"))
        trw = int(os.getenv("TRANSCRIBE_MAX_WORKERS", "3"))
        max_chars = int(os.getenv("ENRICH_MAX_CHARS", "0")) or None
        total = len(docs)
        enrich_progress = {"patched": 0, "failed": 0, "skipped_prev_done": 0, "carry_forward_copied": 0}
        improve_progress = {
            "attempted": 0,
            "improved": 0,
            "failed": 0,
            "skipped_prev_done": 0,
            "carry_forward_copied": 0,
            "skipped_complete_current": 0,
            "failure_reasons": {},
        }

        def update_progress(*, current_index: int) -> None:
            with JOB_LOCK:
                job = JOBS[job_id]
                job.pipeline_stats = {
                    "selected_docs": total,
                    "processed_records": current_index,
                    "enricher": dict(enrich_progress),
                    "improver": dict(improve_progress),
                    "mysql_update": dict(mysql_progress),
                    "mysql_status": summarize_status(env_mode=env_mode.value),
                }
            write_job_to_disk(JOBS[job_id])

        for idx, source_doc in enumerate(docs, start=1):
            if cancel_event.is_set():
                raise JobCancelled("Job canceled during mysql pipeline execution")

            llid = source_doc.get("_orig_id") or source_doc.get("_id")
            if not isinstance(llid, str) or not llid:
                continue

            row = fetch_record(env_mode=env_mode.value, llid=llid) or {}
            prev_current = row.get("current_doc") if isinstance(row.get("current_doc"), dict) else None

            if _should_skip_mysql_record(source_doc, prev_current):
                enrich_progress["skipped_prev_done"] += 1
                improve_progress["skipped_prev_done"] += 1
                improve_progress["skipped_complete_current"] += 1
                update_progress(current_index=idx)
                continue

            doc = dict(source_doc)
            enrich_stats = enrich_docs_via_routes(
                [doc],
                prev_enriched_index={llid: prev_current} if prev_current else {},
                extractor_workers=exw,
                transcribe_workers=trw,
                max_chars=max_chars,
                checkpoint_cb=None,
                should_cancel=cancel_event.is_set,
            )
            enrich_progress["patched"] += int(enrich_stats.get("patched") or 0)
            enrich_progress["failed"] += int(enrich_stats.get("failed") or 0)
            enrich_progress["skipped_prev_done"] += int(enrich_stats.get("skipped_prev_done") or 0)
            enrich_progress["carry_forward_copied"] += int(enrich_stats.get("carry_forward_copied") or 0)

            res = upsert_current_docs(env_mode=env_mode.value, docs=[doc], background=deferred_only, stage="enriched")
            mysql_progress["updated"] += int(res.get("updated") or 0)
            mysql_progress["unchanged"] += int(res.get("unchanged") or 0)
            update_progress(current_index=idx)

            prev_for_improver = prev_current if isinstance(prev_current, dict) else None
            doc["_improver_fp"] = compute_improver_fp(doc)
            if isinstance(prev_for_improver, dict) and not isinstance(prev_for_improver.get("_improver_fp"), str):
                prev_for_improver["_improver_fp"] = compute_improver_fp(prev_for_improver)

            if carry_forward_previous_improvements(doc, prev_for_improver):
                improve_progress["carry_forward_copied"] += 1

            if should_skip_improve(doc, prev_for_improver):
                improve_progress["skipped_prev_done"] += 1
                res = upsert_current_docs(env_mode=env_mode.value, docs=[doc], background=deferred_only, stage="improved")
                mysql_progress["updated"] += int(res.get("updated") or 0)
                mysql_progress["unchanged"] += int(res.get("unchanged") or 0)
                update_progress(current_index=idx)
                continue

            improve_progress["attempted"] += 1
            ok, reason = improve_doc_in_place(doc)
            if ok:
                improve_progress["improved"] += 1
                res = upsert_current_docs(env_mode=env_mode.value, docs=[doc], background=deferred_only, stage="improved")
                mysql_progress["updated"] += int(res.get("updated") or 0)
                mysql_progress["unchanged"] += int(res.get("unchanged") or 0)
            else:
                improve_progress["failed"] += 1
                tag = "other_error" if not reason else reason
                improve_progress["failure_reasons"][tag] = improve_progress["failure_reasons"].get(tag, 0) + 1
            update_progress(current_index=idx)

        details = {
            "selected_docs": total,
            "processed_records": total,
            "enricher": dict(enrich_progress),
            "improver": dict(improve_progress),
            "mysql_update": dict(mysql_progress),
            "mysql_status": summarize_status(env_mode=env_mode.value),
        }
        _set_job_done(job_id, details=details, latest_path=None)
    except JobCancelled as e:
        _set_job_canceled(job_id, e)
    except Exception as e:
        _set_job_error(job_id, e)
    finally:
        clear_job_cancel(job_id)


def run_mysql_improver_fallback_job(
    job_id: str,
    *,
    env_mode: EnvMode,
    deferred_only: bool,
    max_docs: Optional[int],
    llids: Optional[List[str]] = None,
) -> None:
    cancel_event = get_cancel_event(job_id)
    _set_job_running(job_id)
    try:
        ensure_schema()
        docs = fetch_source_docs(
            env_mode=env_mode.value,
            deferred_only=deferred_only,
            llids=llids,
            max_docs=max_docs,
        )
        if not docs:
            _set_job_done(job_id, details={"selected_docs": 0, "notes": "no matching mysql records"})
            return

        total = len(docs)
        progress = {
            "selected_docs": total,
            "processed_records": 0,
            "skipped_has_summary": 0,
            "attempted": 0,
            "improved": 0,
            "failed": 0,
            "mysql_update": {"updated": 0, "unchanged": 0},
            "failure_reasons": {},
        }

        def update_progress() -> None:
            with JOB_LOCK:
                job = JOBS[job_id]
                job.pipeline_stats = {
                    **progress,
                    "mysql_status": summarize_status(env_mode=env_mode.value),
                }
            write_job_to_disk(JOBS[job_id])

        for idx, source_doc in enumerate(docs, start=1):
            if cancel_event.is_set():
                raise JobCancelled("Job canceled during improver fallback execution")

            llid = source_doc.get("_orig_id") or source_doc.get("_id")
            if not isinstance(llid, str) or not llid:
                continue

            row = fetch_record(env_mode=env_mode.value, llid=llid) or {}
            prev_current = row.get("current_doc") if isinstance(row.get("current_doc"), dict) else None

            current_summary = prev_current.get("ko_content_flat_summarised") if prev_current else None
            if isinstance(current_summary, str) and current_summary.strip():
                progress["skipped_has_summary"] += 1
                progress["processed_records"] = idx
                update_progress()
                continue

            doc = dict(source_doc)
            if prev_current:
                doc.update(prev_current)
                if not (isinstance(doc.get("ko_content_flat"), str) and doc.get("ko_content_flat").strip()):
                    source_flat = source_doc.get("ko_content_flat")
                    if isinstance(source_flat, str) and source_flat.strip():
                        doc["ko_content_flat"] = source_flat

            doc["_improver_fp"] = compute_improver_fp(doc)
            progress["attempted"] += 1
            ok, reason = improve_doc_in_place(doc)
            if ok:
                progress["improved"] += 1
                res = upsert_current_docs(env_mode=env_mode.value, docs=[doc], background=deferred_only, stage="improved")
                progress["mysql_update"]["updated"] += int(res.get("updated") or 0)
                progress["mysql_update"]["unchanged"] += int(res.get("unchanged") or 0)
            else:
                progress["failed"] += 1
                tag = "other_error" if not reason else reason
                progress["failure_reasons"][tag] = progress["failure_reasons"].get(tag, 0) + 1
            progress["processed_records"] = idx
            update_progress()

        _set_job_done(
            job_id,
            details={
                **progress,
                "mysql_status": summarize_status(env_mode=env_mode.value),
            },
            latest_path=None,
        )
    except JobCancelled as e:
        _set_job_canceled(job_id, e)
    except Exception as e:
        _set_job_error(job_id, e)
    finally:
        clear_job_cancel(job_id)


def run_mysql_export_job(job_id: str, *, env_mode: EnvMode, processed_only: bool) -> None:
    _set_job_running(job_id)
    try:
        ensure_schema()
        docs = export_docs(env_mode=env_mode.value, processed_only=processed_only)
        run_id = run_stamp()
        out_dir = output_dir(env_mode.value, root=os.getenv("OUTPUT_ROOT", "output"))
        out_path = out_dir / f"final_improved_mysql_export_{run_id}.json"
        payload = {
            "meta": {
                "env_mode": env_mode.value,
                "run_id": run_id,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
                "stage": "mysql_export",
                "processed_only": processed_only,
            },
            "counts": {"docs": len(docs)},
            "docs": docs,
        }
        atomic_write_json(out_path, payload)
        _set_job_done(
            job_id,
            details={"exported_docs": len(docs), "mysql_status": summarize_status(env_mode=env_mode.value)},
            latest_path=str(out_path),
        )
    except Exception as e:
        _set_job_error(job_id, e)


def get_mysql_record(env_mode: EnvMode, llid: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    return fetch_record(env_mode=env_mode.value, llid=llid)

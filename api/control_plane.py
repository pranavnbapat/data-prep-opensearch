from __future__ import annotations

import hashlib
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.job_store import (JOBS, JOB_LOCK, PerJobLogHandler, clear_job_cancel, get_cancel_event,
                           log_file_path, run_log_path, write_job_to_disk)
from api.models import EnvMode, JobStatus
from api.mysql_store import (apply_translations_patch, ensure_schema, export_docs, fetch_record,
                             fetch_source_docs, list_translation_targets, repair_current_date_metadata,
                             repair_source_metadata, summarize_status, upsert_current_docs,
                             upsert_processed_docs, upsert_source_docs)
from common.cancellation import JobCancelled
from pipeline.io import atomic_write_json, output_dir, run_stamp
from stages.downloader.service import download_and_prepare
from stages.downloader.translations import (build_translations_block, compute_translations_fp,
                                            fetch_metadata_translations_api)
from stages.downloader.utils import get_session, load_backend_cfg
from stages.enricher.core import enrich_docs_via_routes
from stages.enricher.utils import compute_enrich_inputs_fp
from stages.improver.engine import improve_doc_in_place
from stages.improver.utils import carry_forward_previous_improvements, compute_improver_fp, should_skip_improve


logger = logging.getLogger(__name__)

PROJECT_EXPORT_FIELDS = [
    "created_by",
    "created_ts",
    "updated_ts",
    "updated_by",
    "version",
    "status",
    "id",
    "project_type",
    "display_name",
    "title",
    "acronym",
    "URL",
    "description",
    "teaser",
    "startDate",
    "endDate",
    "duration",
    "country",
    "db_id",
    "cordis_url",
    "slug",
    "_id",
]


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


@contextmanager
def _job_logging(job_id: str):
    with JOB_LOCK:
        job = JOBS[job_id]
    log_dir = log_file_path(job).parent
    monthly_log_path = run_log_path(job)
    handler = PerJobLogHandler(
        job_id,
        persist_dir=str(log_dir),
        mirror_path=monthly_log_path,
        mirror_mode="a",
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        yield
    finally:
        root_logger.removeHandler(handler)
        handler.close()


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


def _project_export_doc(raw: Dict[str, Any]) -> Dict[str, Any]:
    doc: Dict[str, Any] = {}
    for field in PROJECT_EXPORT_FIELDS:
        doc[field] = raw.get(field)
    return doc


def _fetch_projects_export_docs(*, env_mode: EnvMode, page_size: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    backend_cfg = load_backend_cfg(env_mode.value)
    timeout = int(os.getenv("DL_HTTP_TIMEOUT", "30"))
    session = get_session(backend_cfg, timeout=timeout)
    url = f"{backend_cfg['host']}/api/logical_layer/projects/search"
    base_body = {"project_type": [], "project_country": [], "project_status": []}
    # Backend reads `limit`/`page` from the request BODY; passing them as query
    # params is silently ignored (it then always serves page 1 with next_page=2,
    # which previously sent this loop spinning forever).
    effective_limit = page_size if isinstance(page_size, int) and page_size > 0 else 200

    docs: list[dict[str, Any]] = []
    page = 1
    total_records: Optional[int] = None
    total_pages: Optional[int] = None

    while True:
        resp = session.post(
            url,
            json={**base_body, "limit": effective_limit, "page": page},
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        data = payload.get("data") or []
        pagination = payload.get("pagination") or {}

        if total_records is None:
            try:
                total_records = int(pagination.get("total_records"))
            except Exception:
                total_records = None
        if total_pages is None:
            try:
                total_pages = int(pagination.get("total_pages"))
            except Exception:
                total_pages = None

        if not data:
            break

        for raw in data:
            if isinstance(raw, dict):
                docs.append(_project_export_doc(raw))

        logger.info(
            "projects/search page=%s got=%s collected=%s total_records=%s total_pages=%s",
            page, len(data), len(docs), total_records, total_pages,
        )

        # Guard: if the backend ignores paging (current_page never advances), stop
        # instead of looping forever on the same page.
        current_page = pagination.get("current_page")
        try:
            if current_page is not None and int(current_page) != page:
                logger.warning(
                    "projects/search ignored page=%s (returned current_page=%s); stopping to avoid an infinite loop",
                    page, current_page,
                )
                break
        except Exception:
            pass

        next_page = pagination.get("next_page")
        if not next_page:
            break
        next_page = int(next_page)
        if next_page <= page:
            logger.warning("projects/search next_page=%s did not advance past %s; stopping", next_page, page)
            break
        page = next_page

    meta = {
        "backend_total_records": total_records,
        "backend_total_pages": total_pages,
        "selected_fields": PROJECT_EXPORT_FIELDS,
    }
    return docs, meta


def run_backend_sync_job(job_id: str, *, env_mode: EnvMode, page_size: int, sort_criteria: int, dl_workers: int) -> None:
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting backend sync (env=%s, page_size=%s, sort_criteria=%s, dl_workers=%s)",
                        env_mode.value, page_size, sort_criteria, dl_workers)
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
                logger.info("Sync page written: page=%s synced=%s changed=%s unchanged=%s deferred=%s",
                            page_stats.get("page"), sync_acc["synced"], sync_acc["changed"],
                            sync_acc["unchanged"], sync_acc["deferred"])
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
            logger.info("Backend sync finished: %s", details)
            _set_job_done(job_id, details=details)
        except Exception as e:
            logger.exception("Backend sync failed")
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
    with _job_logging(job_id):
        try:
            logger.info("Starting mysql pipeline (env=%s, scope=%s, max_docs=%s, llids=%s)",
                        env_mode.value, "deferred" if deferred_only else "fast", max_docs, llids)
            ensure_schema()
            mysql_progress = {"updated": 0, "unchanged": 0}
            docs = fetch_source_docs(
                env_mode=env_mode.value,
                deferred_only=deferred_only,
                llids=llids,
                max_docs=max_docs,
            )
            if not docs:
                logger.info("No matching mysql records found")
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
                logger.info("Processing mysql record %s/%s llid=%s", idx, total, llid)

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
            logger.info("Mysql pipeline finished: %s", details)
            _set_job_done(job_id, details=details, latest_path=None)
        except JobCancelled as e:
            logger.warning("Mysql pipeline canceled: %s", e)
            _set_job_canceled(job_id, e)
        except Exception as e:
            logger.exception("Mysql pipeline failed")
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
    with _job_logging(job_id):
        try:
            logger.info("Starting improver fallback (env=%s, deferred_only=%s, max_docs=%s, llids=%s)",
                        env_mode.value, deferred_only, max_docs, llids)
            ensure_schema()
            docs = fetch_source_docs(
                env_mode=env_mode.value,
                deferred_only=deferred_only,
                llids=llids,
                max_docs=max_docs,
            )
            if not docs:
                logger.info("No matching mysql records found")
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

            logger.info("Improver fallback finished: %s", progress)
            _set_job_done(
                job_id,
                details={
                    **progress,
                    "mysql_status": summarize_status(env_mode=env_mode.value),
                },
                latest_path=None,
            )
        except JobCancelled as e:
            logger.warning("Improver fallback canceled: %s", e)
            _set_job_canceled(job_id, e)
        except Exception as e:
            logger.exception("Improver fallback failed")
            _set_job_error(job_id, e)
        finally:
            clear_job_cancel(job_id)


def run_translations_sync_job(
    job_id: str,
    *,
    env_mode: EnvMode,
    only_missing: bool,
    max_docs: Optional[int],
    llids: Optional[List[str]] = None,
) -> None:
    """
    Fetch KO metadata translations from backend-core and merge them into existing
    MySQL records. Fetch-only and incremental: never runs enricher/improver, and a
    record whose translation fingerprint is unchanged is skipped without a write.
    """
    cancel_event = get_cancel_event(job_id)
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting translations sync (env=%s, only_missing=%s, max_docs=%s, llids=%s)",
                        env_mode.value, only_missing, max_docs, llids)
            ensure_schema()
            backend_cfg = load_backend_cfg(env_mode.value)
            targets = list_translation_targets(
                env_mode=env_mode.value,
                llids=llids,
                max_docs=max_docs,
                only_missing=only_missing,
            )
            total = len(targets)
            progress: Dict[str, Any] = {
                "selected_records": total,
                "processed_records": 0,
                "fetched": 0,
                "updated": 0,
                "unchanged": 0,
                "no_translations": 0,
                "failed": 0,
                "langs_seen": {},
            }

            def update_progress(current_index: int) -> None:
                with JOB_LOCK:
                    job = JOBS[job_id]
                    job.pipeline_stats = {
                        **progress,
                        "processed_records": current_index,
                        "mysql_status": summarize_status(env_mode=env_mode.value),
                    }
                write_job_to_disk(JOBS[job_id])

            if not targets:
                logger.info("No matching translation targets found")
                _set_job_done(job_id, details={**progress, "notes": "no matching records"})
                return

            for idx, target in enumerate(targets, start=1):
                if cancel_event.is_set():
                    raise JobCancelled("Job canceled during translations sync")

                llid = target.get("llid")
                if not isinstance(llid, str) or not llid:
                    continue

                try:
                    raw = fetch_metadata_translations_api(backend_cfg, llid)
                except Exception:
                    logger.exception("Translations fetch failed for llid=%s", llid)
                    progress["failed"] += 1
                    update_progress(idx)
                    continue

                progress["fetched"] += 1
                block, langs = build_translations_block(raw)
                if not block:
                    progress["no_translations"] += 1
                    update_progress(idx)
                    continue

                fp = compute_translations_fp(block)
                if target.get("existing_fp") and target.get("existing_fp") == fp:
                    progress["unchanged"] += 1
                    update_progress(idx)
                    continue

                result = apply_translations_patch(
                    env_mode=env_mode.value,
                    llid=llid,
                    block=block,
                    langs=langs,
                    translations_fp=fp,
                )
                if result == "updated":
                    progress["updated"] += 1
                    for lang in langs:
                        progress["langs_seen"][lang] = progress["langs_seen"].get(lang, 0) + 1
                elif result == "unchanged":
                    progress["unchanged"] += 1
                else:
                    progress["failed"] += 1
                update_progress(idx)

            details = {
                **progress,
                "processed_records": total,
                "mysql_status": summarize_status(env_mode=env_mode.value),
            }
            logger.info("Translations sync finished: %s", details)
            _set_job_done(job_id, details=details, latest_path=None)
        except JobCancelled as e:
            logger.warning("Translations sync canceled: %s", e)
            _set_job_canceled(job_id, e)
        except Exception as e:
            logger.exception("Translations sync failed")
            _set_job_error(job_id, e)
        finally:
            clear_job_cancel(job_id)


def run_mysql_export_job(job_id: str, *, env_mode: EnvMode, processed_only: bool, eligible_only: bool) -> None:
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting mysql export (env=%s, processed_only=%s, eligible_only=%s)", env_mode.value, processed_only, eligible_only)
            ensure_schema()
            docs = export_docs(env_mode=env_mode.value, processed_only=processed_only, eligible_only=eligible_only)
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
                    "eligible_only": eligible_only,
                },
                "counts": {"docs": len(docs)},
                "docs": docs,
            }
            atomic_write_json(out_path, payload)
            logger.info("Mysql export finished: exported_docs=%s path=%s", len(docs), out_path)
            _set_job_done(
                job_id,
                details={"exported_docs": len(docs), "mysql_status": summarize_status(env_mode=env_mode.value)},
                latest_path=str(out_path),
            )
        except Exception as e:
            logger.exception("Mysql export failed")
            _set_job_error(job_id, e)


def _projects_fingerprint(docs: List[Dict[str, Any]]) -> str:
    """Stable content hash of the exported projects, order-independent. Two runs
    with identical project content produce the same fingerprint regardless of
    backend pagination order."""
    ordered = sorted(docs, key=lambda d: (str(d.get("id")), str(d.get("_id"))))
    blob = json.dumps(ordered, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _latest_projects_export_path(env_mode: str) -> Optional[Path]:
    """Most recent projects_export_*.json for the env, across all year/month dirs."""
    root = Path(os.getenv("OUTPUT_ROOT", "output")) / env_mode.upper()
    if not root.exists():
        return None
    candidates = list(root.rglob("projects_export_*.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _previous_projects_fingerprint(prev_path: Optional[Path]) -> Optional[str]:
    if prev_path is None:
        return None
    try:
        prev_payload = json.loads(prev_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Could not read previous projects export %s for dedup", prev_path)
        return None
    fp = (prev_payload.get("meta") or {}).get("projects_fp")
    if fp:
        return fp
    # Older exports predate the stored fp: recompute from their projects list.
    return _projects_fingerprint(prev_payload.get("projects") or [])


def run_projects_export_job(job_id: str, *, env_mode: EnvMode, page_size: int) -> None:
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting projects export (env=%s, page_size=%s)", env_mode.value, page_size)
            docs, backend_meta = _fetch_projects_export_docs(env_mode=env_mode, page_size=page_size)
            new_fp = _projects_fingerprint(docs)

            # Idempotent: if the projects are identical to the last export, don't
            # write a new file — reuse the existing one.
            prev_path = _latest_projects_export_path(env_mode.value)
            prev_fp = _previous_projects_fingerprint(prev_path)
            if prev_fp is not None and prev_fp == new_fp:
                logger.info(
                    "Projects export unchanged (fp=%s, %s projects); reusing %s",
                    new_fp, len(docs), prev_path,
                )
                _set_job_done(
                    job_id,
                    details={
                        "exported_projects": len(docs),
                        "unchanged": True,
                        "projects_fp": new_fp,
                        "reused_path": str(prev_path),
                        **backend_meta,
                    },
                    latest_path=str(prev_path),
                )
                return

            run_id = run_stamp()
            out_dir = output_dir(env_mode.value, root=os.getenv("OUTPUT_ROOT", "output"))
            out_path = out_dir / f"projects_export_{run_id}.json"
            payload = {
                "meta": {
                    "env_mode": env_mode.value,
                    "run_id": run_id,
                    "created_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "stage": "projects_export",
                    "page_size": page_size,
                    "projects_fp": new_fp,
                    **backend_meta,
                },
                "counts": {"projects": len(docs)},
                "projects": docs,
            }
            atomic_write_json(out_path, payload)
            logger.info("Projects export finished: projects=%s path=%s", len(docs), out_path)
            _set_job_done(
                job_id,
                details={"exported_projects": len(docs), "unchanged": False, "projects_fp": new_fp, **backend_meta},
                latest_path=str(out_path),
            )
        except Exception as e:
            logger.exception("Projects export failed")
            _set_job_error(job_id, e)


def run_mysql_source_metadata_repair_job(
    job_id: str,
    *,
    env_mode: EnvMode,
    max_docs: Optional[int],
    llids: Optional[List[str]] = None,
) -> None:
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting source metadata repair (env=%s, max_docs=%s, llids=%s)", env_mode.value, max_docs, llids)
            ensure_schema()
            stats = repair_source_metadata(env_mode=env_mode.value, max_docs=max_docs, llids=llids)
            details = {**stats, "mysql_status": summarize_status(env_mode=env_mode.value)}
            logger.info("Source metadata repair finished: %s", details)
            _set_job_done(job_id, details=details)
        except Exception as e:
            logger.exception("Source metadata repair failed")
            _set_job_error(job_id, e)


def run_mysql_current_date_metadata_repair_job(
    job_id: str,
    *,
    env_mode: EnvMode,
    max_docs: Optional[int],
    llids: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
) -> None:
    _set_job_running(job_id)
    with _job_logging(job_id):
        try:
            logger.info("Starting current metadata repair (env=%s, max_docs=%s, llids=%s, fields=%s)", env_mode.value, max_docs, llids, fields)
            ensure_schema()
            stats = repair_current_date_metadata(env_mode=env_mode.value, max_docs=max_docs, llids=llids, fields=fields)
            details = {**stats, "mysql_status": summarize_status(env_mode=env_mode.value)}
            logger.info("Current metadata repair finished: %s", details)
            _set_job_done(job_id, details=details)
        except Exception as e:
            logger.exception("Current metadata repair failed")
            _set_job_error(job_id, e)


def get_mysql_record(env_mode: EnvMode, llid: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    return fetch_record(env_mode=env_mode.value, llid=llid)

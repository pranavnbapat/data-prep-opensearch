import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from common.cancellation import JobCancelled
from common.utils import CustomJSONEncoder
from pipeline.io import run_stamp, output_dir, atomic_write_json, update_latest_pointer, resolve_latest_pointer
from pipeline.locks import acquire_job_lock, release_job_lock, JobLockHeldError
from stages.downloader.prepare import fetch_ko_metadata_api, process_page, upsert_dropped_kos
from stages.downloader.utils import DownloadResult, load_backend_cfg

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)


def download_and_prepare(
    *,
    env_mode: str,
    page_size: int,
    sort_criteria: int = 1,
    max_workers: int = 10,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
    page_callback: Optional[Callable[[List[Dict[str, Any]], Dict[str, Any]], None]] = None,
    use_lock: bool = True,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> DownloadResult:
    output_root = os.getenv("OUTPUT_ROOT", "output")
    lock = None
    if use_lock:
        lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="downloader")

    try:
        backend_cfg = load_backend_cfg(env_mode)
        run_id = run_stamp()
        should_write_output = os.getenv("DL_WRITE_OUTPUT", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        out_dir = output_dir(env_mode, root=output_root)
        out_path = out_dir / f"final_output_{run_id}.json"
        page = 1
        pages_seen = set()
        emitted_total = 0
        dropped_total = 0
        unchanged_total = 0
        new_total = 0
        updated_total = 0
        changed_ids_all: List[str] = []
        emitted_ids_all: List[str] = []
        source_seen_ids_all: List[str] = []
        docs_all: List[Dict[str, Any]] = []
        t0 = time.perf_counter()
        source_seen_total = 0

        def write_checkpoint(*, complete: bool) -> None:
            if not should_write_output:
                return
            elapsed = time.perf_counter() - t0
            removed_from_source = 0
            if prev_index:
                prev_ids = set(prev_index.keys())
                current_ids = set(source_seen_ids_all)
                removed_from_source = len(prev_ids - current_ids)
            stats = {
                "env_mode": env_mode.upper(),
                "source_seen": source_seen_total,
                "emitted": emitted_total,
                "dropped": dropped_total,
                "changed": new_total + updated_total,
                "new_added": new_total,
                "updated": updated_total,
                "unchanged_reused": unchanged_total,
                "removed_from_source": removed_from_source,
                "elapsed_sec": round(elapsed, 2),
            }
            payload = {
                "meta": {
                    "env_mode": env_mode.upper(),
                    "run_id": run_id,
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "stage": "downloader",
                    "checkpoint_complete": complete,
                    "checkpoint_page": page,
                },
                "stats": stats,
                "docs": docs_all,
            }
            atomic_write_json(out_path, payload)
            update_latest_pointer(env_mode, output_root, "latest_downloaded.json", out_path)

        logging.info(
            "[DownloaderStart] env=%s page_size=%s workers=%s sort=%s backend=%s prev_docs=%s",
            env_mode.upper(),
            page_size,
            max_workers,
            sort_criteria,
            backend_cfg["host"],
            (len(prev_index) if prev_index is not None else 0),
        )

        while True:
            if should_cancel and should_cancel():
                raise JobCancelled("Job canceled during downloader page fetch loop")
            if page in pages_seen:
                logging.warning("Breaking due to repeated page indicator: %s", page)
                break
            pages_seen.add(page)

            t_page = time.perf_counter()
            t_fetch = time.perf_counter()
            logging.info(
                "[PageFetchStart] env=%s page=%s limit=%s sort=%s",
                env_mode.upper(),
                page,
                page_size,
                sort_criteria,
            )
            payload = fetch_ko_metadata_api(
                backend_cfg,
                limit=page_size,
                page=page,
                sort_criteria=sort_criteria,
            )
            dt_fetch = time.perf_counter() - t_fetch
            logging.info("[PageFetch] env=%s Page=%s dt=%.2fs", env_mode, page, dt_fetch)

            warn_thr = float(os.getenv("PAGE_FETCH_WARN_SEC", "5.0"))
            if dt_fetch > warn_thr:
                logging.warning("[PageFetchSlow] env=%s Page=%s dt=%.2fs thr=%.2fs", env_mode, page, dt_fetch, warn_thr)

            kos_page = payload.get("data", []) or payload.get("results", []) or []
            if not kos_page:
                break
            source_seen_total += len(kos_page)
            for ko in kos_page:
                llid = ko.get("_id")
                if isinstance(llid, str) and llid.strip():
                    source_seen_ids_all.append(llid.strip())

            pagination = payload.get("pagination") or {}
            next_page = pagination.get("next_page")
            logging.info("[KO API] env=%s Page=%s fetched=%s next=%r", env_mode, page, len(kos_page), next_page)

            t_process = time.perf_counter()
            docs_p, emitted_p, dropped_p, drop_reasons, dropped_items, unchanged_p, changed_ids_p, emitted_ids_p, new_p, updated_p = process_page(
                backend_cfg,
                kos_page,
                workers=max_workers,
                prev_index=prev_index,
            )
            if should_cancel and should_cancel():
                raise JobCancelled("Job canceled during downloader page processing")
            dt_process = time.perf_counter() - t_process
            dt_page = time.perf_counter() - t_page

            logging.info(
                "[PageDone] env=%s page=%s fetched=%s emitted=%s dropped=%s unchanged=%s new=%s updated=%s fetch_dt=%.2fs process_dt=%.2fs page_dt=%.2fs",
                env_mode,
                page,
                len(kos_page),
                emitted_p,
                dropped_p,
                unchanged_p,
                new_p,
                updated_p,
                dt_fetch,
                dt_process,
                dt_page,
            )

            process_warn_thr = float(os.getenv("PAGE_PROCESS_WARN_SEC", "10.0"))
            if dt_process > process_warn_thr:
                logging.warning(
                    "[PageProcessSlow] env=%s page=%s process_dt=%.2fs thr=%.2fs fetched=%s emitted=%s dropped=%s",
                    env_mode,
                    page,
                    dt_process,
                    process_warn_thr,
                    len(kos_page),
                    emitted_p,
                    dropped_p,
                )

            page_warn_thr = float(os.getenv("PAGE_TOTAL_WARN_SEC", "15.0"))
            if dt_page > page_warn_thr:
                logging.warning(
                    "[PageTotalSlow] env=%s page=%s page_dt=%.2fs thr=%.2fs fetch_dt=%.2fs process_dt=%.2fs",
                    env_mode,
                    page,
                    dt_page,
                    page_warn_thr,
                    dt_fetch,
                    dt_process,
                )

            if drop_reasons:
                logging.info("[PageDrops] env=%s Page=%s Reasons=%s", env_mode, page, drop_reasons)

            max_show = int(os.getenv("DROP_LOG_MAX", "100"))
            if dropped_items:
                total = len(dropped_items)
                show_n = min(max_show, total)
                for i, item in enumerate(dropped_items[:show_n], start=1):
                    doc = item.get("doc") if isinstance(item, dict) else None
                    doc = doc if isinstance(doc, dict) else {}
                    details = doc.get("_drop_details") if isinstance(doc.get("_drop_details"), dict) else {}
                    missing_pre = details.get("missing_pre_fields")
                    logging.info(
                        "[DroppedKOs] env=%s Page=%s showing=%s/%s Reason=%r missing_pre_fields=%r logical_layer_id=%r title=%r",
                        env_mode, page, i, total, doc.get("_drop_reason"), missing_pre, item.get("logical_layer_id"), doc.get("title"),
                    )
                if show_n < total:
                    logging.info("[DroppedKOs] env=%s Page=%s truncated (showing %s/%s). Increase DROP_LOG_MAX to see more.",
                                 env_mode, page, show_n, total)

                drops_path = upsert_dropped_kos(
                    env_mode=env_mode,
                    output_root=output_root,
                    run_id=run_id,
                    page=page,
                    dropped_records=dropped_items,
                )
                if drops_path:
                    logging.info("[DroppedKOsFile] Updated=%s Count Added or Updated=%s", drops_path, len(dropped_items))

            docs_all.extend(docs_p)
            emitted_total += emitted_p
            dropped_total += dropped_p
            unchanged_total += unchanged_p
            new_total += new_p
            updated_total += updated_p
            changed_ids_all.extend(changed_ids_p)
            emitted_ids_all.extend(emitted_ids_p)

            logging.info(
                "[DownloaderProgress] env=%s page=%s source_seen_total=%s emitted_total=%s new_total=%s updated_total=%s unchanged_total=%s dropped_total=%s",
                env_mode.upper(),
                page,
                source_seen_total,
                emitted_total,
                new_total,
                updated_total,
                unchanged_total,
                dropped_total,
            )

            if page_callback is not None:
                page_stats = {
                    "page": page,
                    "fetched": len(kos_page),
                    "emitted": emitted_p,
                    "dropped": dropped_p,
                    "unchanged": unchanged_p,
                    "new": new_p,
                    "updated": updated_p,
                    "source_seen_total": source_seen_total,
                    "emitted_total": emitted_total,
                    "dropped_total": dropped_total,
                }
                page_callback(docs_p, page_stats)

            write_checkpoint(complete=False)

            if not next_page:
                break
            try:
                next_page_int = int(next_page)
            except Exception:
                break
            if next_page_int == page:
                break
            page = next_page_int

        changed_total = new_total + updated_total
        prev_ids = set(prev_index.keys()) if prev_index else set()
        current_ids = set(source_seen_ids_all)
        removed_from_source = len(prev_ids - current_ids) if prev_ids else 0

        elapsed = time.perf_counter() - t0
        stats = {
            "env_mode": env_mode.upper(),
            "source_seen": source_seen_total,
            "emitted": emitted_total,
            "dropped": dropped_total,
            "changed": changed_total,
            "new_added": new_total,
            "updated": updated_total,
            "unchanged_reused": unchanged_total,
            "removed_from_source": removed_from_source,
            "elapsed_sec": round(elapsed, 2),
        }

        logging.warning("[Downloader] env=%s source_seen=%s emitted=%s new_added=%s updated=%s unchanged=%s dropped=%s removed=%s elapsed=%.2fs",
                        stats["env_mode"], source_seen_total, emitted_total, new_total, updated_total, unchanged_total,
                        dropped_total, removed_from_source, elapsed)

        if changed_total > 0:
            seen = set()
            uniq = []
            for x in changed_ids_all:
                if x not in seen:
                    uniq.append(x)
                    seen.add(x)
            max_show = int(os.getenv("DL_CHANGED_IDS_MAX", "25"))
            show = uniq[:max_show]
            logging.warning("[DownloaderSummary] New=%s Updated=%s Unchanged=%s Dropped=%s Removed=%s",
                            new_total, updated_total, unchanged_total, dropped_total, removed_from_source)
            logging.warning("[DownloaderSummary] Changed IDs=%s", ", ".join(show))
            if len(uniq) > max_show:
                logging.warning("[DownloaderSummary] Changed IDs Truncated Total=%s Shown=%s", len(uniq), max_show)
        else:
            logging.warning("[DownloaderSummary] No new or updated documents. Emitted=%s Unchanged=%s Dropped=%s Removed=%s",
                            emitted_total, unchanged_total, dropped_total, removed_from_source)

        if should_write_output:
            write_checkpoint(complete=True)

        return DownloadResult(
            docs=docs_all,
            stats=stats,
        )
    finally:
        if lock is not None:
            release_job_lock(lock)


if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()
    ps = int(os.getenv("DL_PAGE_SIZE", "100"))
    ps = max(1, min(ps, 100))
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))

    output_root = os.getenv("OUTPUT_ROOT", "output")
    prev_path = resolve_latest_pointer(env_mode, output_root, "latest_downloaded.json")
    prev_index = {}
    if prev_path:
        try:
            payload = json.loads(prev_path.read_text(encoding="utf-8"))
            docs = payload.get("docs", []) if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])
            if isinstance(docs, list):
                tmp: Dict[str, Dict[str, Any]] = {}
                for d in docs:
                    if not isinstance(d, dict):
                        continue
                    k = d.get("_orig_id") or d.get("_id")
                    if isinstance(k, str) and k.strip():
                        tmp[k.strip()] = d
                prev_index = tmp
        except Exception:
            prev_index = {}

    try:
        res = download_and_prepare(
            env_mode=env_mode,
            page_size=ps,
            sort_criteria=sc,
            max_workers=mw,
            prev_index=prev_index,
        )
    except JobLockHeldError as e:
        print(str(e))
        raise SystemExit(2)

    print(json.dumps(res.stats, indent=2, cls=CustomJSONEncoder))

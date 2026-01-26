# pipeline.py

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from downloader import download_and_prepare, DownloadResult
from enricher import run_enricher_stage
from improver import run_improver_stage
from io_helpers import run_stamp, output_dir, atomic_write_json, env_flag
from job_lock import acquire_job_lock, release_job_lock
from utils import CustomJSONEncoder

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)

# ---------------- Previous snapshot reuse ----------------

def _index_by_llid(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        llid = d.get("_orig_id") or d.get("_id")
        if isinstance(llid, str) and llid:
            idx[llid] = d
    return idx

def load_previous_index(env_mode: str, root: str = "output") -> Dict[str, Dict[str, Any]]:
    """
    Loads the most recent 'latest.json' if present.
    Expected shape: {"docs":[...]} or direct list of docs.
    """
    latest_path = Path(root) / env_mode.upper() / "latest.json"
    if not latest_path.exists():
        logger.info("[Pipeline] No previous snapshot found at %s", latest_path)
        return {}

    try:
        with latest_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as e:
        logger.warning("[Pipeline] Failed to read previous snapshot (%s): %s", latest_path, e)
        return {}

    if isinstance(payload, dict) and isinstance(payload.get("docs"), list):
        docs = payload["docs"]
    elif isinstance(payload, list):
        docs = payload
    else:
        logger.warning("[Pipeline] Previous snapshot has unexpected shape: %s", type(payload))
        return {}

    idx = _index_by_llid(docs)
    logger.info("[Pipeline] Loaded previous snapshot index: %d docs", len(idx))
    return idx

def _pipeline_has_changes(dl_stats: Dict[str, Any], enricher_stats: Dict[str, Any], improver_stats: Dict[str, Any]) -> bool:
    dl_changed = int(dl_stats.get("changed", 0) or 0)
    enr_patched = int(enricher_stats.get("patched", 0) or 0)

    imp_improved = int(improver_stats.get("improved", 0) or 0)
    imp_carried = int(improver_stats.get("carry_forward_copied", 0) or 0)

    return (dl_changed > 0) or (enr_patched > 0) or (imp_improved > 0) or (imp_carried > 0)


# ---------------- Improver hook ----------------

def improve_docs_stub(docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Placeholder for future metadata improvement stage.
    For now does nothing; returns stats only.
    """
    # Later: call LLM improver and patch docs in place.
    return {"improved": 0, "notes": "stub"}


# ---------------- Orchestration ----------------

@dataclass(frozen=True)
class PipelineResult:
    env_mode: str
    run_id: str
    stats: Dict[str, Any]
    final_path: str
    report_path: str
    latest_path: str


def run_pipeline(
    *,
    env_mode: str,
    page_size: int,
    sort_criteria: int,
    dl_workers: int,
    extractor_workers: int,
    transcribe_workers: int,
    max_chars: Optional[int],
    output_root: str = "output",
) -> PipelineResult:

    lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="pipeline")

    try:
        run_id = run_stamp()

        enable_downloader = env_flag("ENABLE_DOWNLOADER", True)
        enable_enricher = env_flag("ENABLE_ENRICHER", False)
        enable_improver = env_flag("ENABLE_IMPROVER", False)

        # Step 1: download
        prev_index = load_previous_index(env_mode, root=output_root)

        dl: Optional[DownloadResult] = None
        if enable_downloader:
            logger.warning("--------------- START OF DOWNLOADER ---------------")
            dl = download_and_prepare(
                env_mode=env_mode,
                page_size=page_size,
                sort_criteria=sort_criteria,
                max_workers=dl_workers,
                prev_index=prev_index,
                use_lock=False
            )
        else:
            logger.warning("[Pipeline] Downloader disabled; downstream stages will resolve latest inputs themselves.")

        logger.warning("--------------- END OF DOWNLOADER ---------------")

        # Build a "working set" from downloader outputs (do NOT mutate dl, it may be frozen)
        dl_docs: List[Dict[str, Any]] = dl.docs if dl else []
        dl_url_tasks: List[Dict[str, Any]] = dl.url_tasks if dl else []
        dl_media_tasks: List[Dict[str, Any]] = dl.media_tasks if dl else []

        # Optional: truncate latest downloader final_output_* to a small random sample (DEV convenience)
        # truncate_final = env_flag("TRUNCATE_FINAL_OUTPUT", False)
        # if truncate_final and dl:
        #     try:
        #         truncated_path, truncated_docs = truncate_latest_final_output(
        #             env_mode=env_mode,
        #             output_root=output_root,
        #             n_keep=10,
        #         )
        #
        #         # Use truncated docs as the working set for downstream stages
        #         dl_docs = truncated_docs
        #
        #         logger.warning(
        #             "[Pipeline] TRUNCATE_FINAL_OUTPUT=1 → truncated %s to %d docs (downstream will use truncated set)",
        #             str(truncated_path),
        #             len(truncated_docs),
        #         )
        #     except Exception as e:
        #         logger.warning("[Pipeline] TRUNCATE_FINAL_OUTPUT requested but truncation failed: %s", e)

        # Step 2: enrich (cascading: only runs if downloader ran and enable_enricher=True)
        enrich_res: Dict[str, Any] = {}
        enrich_stats: Dict[str, Any] = {"patched": 0, "notes": "skipped_or_disabled"}

        # Default to downloader outputs (only valid if downloader ran)
        # enriched_docs: List[Dict[str, Any]] = dl.docs if dl else []
        # enriched_url_tasks: List[Dict[str, Any]] = dl.url_tasks if dl else []
        # enriched_media_tasks: List[Dict[str, Any]] = dl.media_tasks if dl else []
        # Default to downloader outputs (or truncated working set)
        enriched_docs: List[Dict[str, Any]] = dl_docs
        enriched_url_tasks: List[Dict[str, Any]] = dl_url_tasks
        enriched_media_tasks: List[Dict[str, Any]] = dl_media_tasks


        logger.warning("--------------- START OF ENRICHER ---------------")
        if enable_enricher:
            if not dl:
                # Should never happen due to cascading flags, but keep it defensive.
                logger.warning("[Pipeline] Enricher enabled but downloader did not run; skipping enricher.")
            else:
                enrich_res = run_enricher_stage(
                    env_mode=env_mode,
                    output_root=output_root,
                    extractor_workers=extractor_workers,
                    transcribe_workers=transcribe_workers,
                    max_chars=max_chars,
                    use_lock=False,
                )
                enrich_stats = enrich_res.get("stats") or {"patched": 0, "notes": "missing_stats"}

                # Enricher output contains only docs; tasks remain from downloader
                try:
                    with Path(enrich_res["out"]).open("r", encoding="utf-8") as fh:
                        enriched_payload = json.load(fh)

                    enriched_docs = enriched_payload.get("docs", enriched_docs)
                    if not isinstance(enriched_docs, list):
                        raise ValueError("enriched docs is not a list")
                except Exception as e:
                    logger.warning("[Pipeline] Failed to load enriched output (%s): %s", enrich_res.get("out"), e)
                    # Keep downloader docs/tasks as-is
        else:
            logger.warning("[Pipeline] Enricher disabled (or cascaded off); skipping.")

        enrich_res = enrich_res or {}
        logger.warning("--------------- END OF ENRICHER ---------------")

        # Step 3: improver
        improve_res: Dict[str, Any] = {}
        improve_stats: Dict[str, Any] = {"improved": 0, "notes": "skipped_or_disabled"}

        # Default to whatever we currently have in memory (downloader/enricher output)
        improved_docs: List[Dict[str, Any]] = enriched_docs


        logger.warning("--------------- START OF IMPROVER ---------------")
        if enable_improver:
            # The improver stage resolves its own latest_enriched input via pointer,
            # writes final_improved_*.json, and updates latest_improved.json
            improve_res = run_improver_stage(
                env_mode=env_mode,
                output_root=output_root,
                max_docs=(int(os.getenv("IMPROVER_MAX_DOCS", "0")) or None),
                use_lock=False,
            ) or {}

            improve_stats = improve_res.get("stats") or {"improved": 0, "notes": "missing_stats"}

            # Load the improver output docs so pipeline’s final payload becomes "improved" docs.
            try:
                out_path = improve_res.get("out")
                if isinstance(out_path, str) and out_path:
                    with Path(out_path).open("r", encoding="utf-8") as fh:
                        improved_payload = json.load(fh)

                    improved_docs = improved_payload.get("docs", improved_docs)
                    if not isinstance(improved_docs, list):
                        raise ValueError("improved docs is not a list")
                else:
                    logger.warning("[Pipeline] Improver returned no out path; keeping prior docs.")
            except Exception as e:
                logger.warning("[Pipeline] Failed to load improved output (%s): %s", improve_res.get("out"), e)
                # Keep enriched_docs as-is
        else:
            logger.warning("[Pipeline] Improver disabled; skipping.")

        logger.warning("--------------- END OF IMPROVER ---------------")

        downloader_stats = dl.stats if dl else {"changed": 0, "emitted": 0, "dropped": 0, "notes": "disabled"}
        # if truncate_final and dl:
        #     # avoid mutating dl.stats; just override in the stats we report
        #     downloader_stats = dict(downloader_stats)
        #     downloader_stats["emitted"] = len(dl_docs)

        # If nothing changed since previous snapshot, don't create new final_* files.
        # Still refresh latest.json (optional) so reuse continues to work.
        has_changes = _pipeline_has_changes(downloader_stats, enrich_stats, improve_stats)

        if not has_changes:
            latest_path = Path(output_root) / env_mode.upper() / "latest.json"

            # --- IMPORTANT SAFETY: never overwrite latest.json with empty docs/tasks ---
            prev_payload: Dict[str, Any] = {}
            if latest_path.exists():
                try:
                    prev_payload = json.loads(latest_path.read_text(encoding="utf-8"))
                except Exception as e:
                    logger.warning("[Pipeline] Failed to read existing latest.json (%s): %s", latest_path, e)
                    prev_payload = {}

            # If current run didn't produce docs/tasks in memory (e.g. downloader disabled),
            # preserve what was already in latest.json.
            docs_out = improved_docs if improved_docs else (prev_payload.get("docs") or [])
            url_tasks_out = enriched_url_tasks if enriched_url_tasks else (prev_payload.get("url_tasks") or [])
            media_tasks_out = enriched_media_tasks if enriched_media_tasks else (prev_payload.get("media_tasks") or [])

            latest_payload = {
                "meta": {
                    "env_mode": env_mode.upper(),
                    "run_id": run_id,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "no_changes": True,
                },
                "stats": {
                    "downloader": downloader_stats,
                    "enricher": enrich_stats,
                    "improver": improve_stats,
                },
                "docs": docs_out,
                "url_tasks": url_tasks_out,
                "media_tasks": media_tasks_out,
            }
            atomic_write_json(latest_path, latest_payload)

            logger.warning(
                "[Pipeline] env=%s run_id=%s no_changes=True (skipping final_* outputs)",
                env_mode.upper(), run_id
            )

            return PipelineResult(
                env_mode=env_mode.upper(),
                run_id=run_id,
                stats=latest_payload["stats"],
                final_path="",
                report_path="",
                latest_path=str(latest_path),
            )

        out_dir = output_dir(env_mode, root=output_root)

        final_report_path = out_dir / f"final_report_{run_id}.json"

        latest_path = Path(output_root) / env_mode.upper() / "latest.json"

        # 2) Save report (meta + stats + counts)
        report = {
            "meta": {
                "env_mode": env_mode.upper(),
                "run_id": run_id,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            },
            "stats": {
                "downloader": downloader_stats,
                "enricher": enrich_stats,
                "improver": improve_stats,
            },
            "counts": {
                "docs": len(improved_docs),
                "url_tasks": len(enriched_url_tasks),
                "media_tasks": len(enriched_media_tasks),
            },
            "paths": {
                "downloader_out": enrich_res.get("in", ""),
                "enriched_out": enrich_res.get("out", ""),
                "final_report": str(final_report_path),
                "latest": str(latest_path),
            },
        }
        atomic_write_json(final_report_path, report)

        # 3) Keep latest.json as the full payload for reuse
        payload = {
            "meta": report["meta"],
            "stats": report["stats"],
            "docs": improved_docs,
            "url_tasks": enriched_url_tasks,
            "media_tasks": enriched_media_tasks,
        }
        atomic_write_json(latest_path, payload)

        logger.warning(
            "[Pipeline] env=%s run_id=%s docs=%d enriched_out=%s report=%s",
            env_mode.upper(), run_id, len(improved_docs), enrich_res.get("out", ""), str(final_report_path)
        )

        return PipelineResult(
            env_mode=env_mode.upper(),
            run_id=run_id,
            stats=report["stats"],
            final_path=(
                str(improve_res["out"]) if (enable_improver and improve_res.get("out"))
                else (str(enrich_res["out"]) if (enable_enricher and enrich_res.get("out")) else "")
            ),
            report_path=str(final_report_path),
            latest_path=str(latest_path),
        )
    finally:
        release_job_lock(lock)


# ---------------- CLI ----------------

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode_raw = (os.getenv("ENV_MODE") or "").strip().upper()
    env_mode = env_mode_raw if env_mode_raw in {"DEV", "PRD"} else "DEV"

    page_size = int(os.getenv("DL_PAGE_SIZE", "100"))
    page_size = max(1, min(page_size, 100))

    dl_workers = int(os.getenv("DL_MAX_WORKERS", "10"))
    sort_criteria = int(os.getenv("DL_SORT_CRITERIA", "1"))

    extractor_workers = int(os.getenv("EXTRACTOR_MAX_WORKERS", "4"))
    transcribe_workers = int(os.getenv("TRANSCRIBE_MAX_WORKERS", "3"))

    max_chars_env = int(os.getenv("PIPELINE_MAX_CHARS", "0"))
    max_chars = max_chars_env if max_chars_env > 0 else None

    res = run_pipeline(
        env_mode=env_mode,
        page_size=page_size,
        sort_criteria=sort_criteria,
        dl_workers=dl_workers,
        extractor_workers=extractor_workers,
        transcribe_workers=transcribe_workers,
        max_chars=max_chars,
        output_root=os.getenv("OUTPUT_ROOT", "output"),
    )

    print(json.dumps({
        "env_mode": res.env_mode,
        "run_id": res.run_id,
        "final_path": res.final_path,
        "report_path": res.report_path,
        "latest_path": res.latest_path,
        "stats": res.stats,
    }, indent=4, cls=CustomJSONEncoder))

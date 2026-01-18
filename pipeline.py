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
from enricher import enrich_external_content
from utils import CustomJSONEncoder

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)


# ---------------- Output paths ----------------

def _run_stamp() -> str:
    return datetime.now().strftime("%d_%m-%Y_%H-%M-%S")

def _output_dir(env_mode: str, root: str = "output") -> Path:
    now = datetime.now()
    return Path(root) / env_mode.upper() / now.strftime("%Y") / now.strftime("%m")

def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    tmp.replace(path)


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
    return (dl_changed > 0) or (enr_patched > 0) or (imp_improved > 0)


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
    run_id = _run_stamp()

    prev_index = load_previous_index(env_mode, root=output_root)

    # Step 1: download
    dl: DownloadResult = download_and_prepare(
        env_mode=env_mode,
        page_size=page_size,
        sort_criteria=sort_criteria,
        max_workers=dl_workers,
        prev_index=prev_index,
    )

    cur_index = {d.get("_orig_id"): d for d in dl.docs if isinstance(d.get("_orig_id"), str)}

    filtered_url_tasks = []
    for t in dl.url_tasks:
        llid = t.get("logical_layer_id")
        cur_doc = cur_index.get(llid)
        prev_doc = prev_index.get(llid) if prev_index else None
        if not cur_doc:
            continue
        if (prev_doc or {}).get("_enricher_fp") != cur_doc.get("_enricher_fp"):
            filtered_url_tasks.append(t)

    filtered_media_tasks = []
    for t in dl.media_tasks:
        llid = t.get("logical_layer_id")
        cur_doc = cur_index.get(llid)
        prev_doc = prev_index.get(llid) if prev_index else None
        if not cur_doc:
            continue
        if (prev_doc or {}).get("_enricher_fp") != cur_doc.get("_enricher_fp"):
            filtered_media_tasks.append(t)

    # Step 2: enrich
    enrich_stats = enrich_external_content(
        dl.docs,
        filtered_url_tasks,
        filtered_media_tasks,
        previous_index=prev_index,
        extractor_workers=extractor_workers,
        transcribe_workers=transcribe_workers,
        max_chars=max_chars,
    )

    # Step 3: improver (stub)
    improve_stats = improve_docs_stub(dl.docs)

    # If nothing changed since previous snapshot, don't create new final_* files.
    # Still refresh latest.json (optional) so reuse continues to work.
    has_changes = _pipeline_has_changes(dl.stats, enrich_stats, improve_stats)

    if not has_changes:
        latest_path = Path(output_root) / env_mode.upper() / "latest.json"
        latest_payload = {
            "meta": {
                "env_mode": env_mode.upper(),
                "run_id": run_id,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            },
            "stats": {
                "downloader": dl.stats,
                "enricher": enrich_stats,
                "improver": improve_stats,
            },
            "docs": dl.docs,
            "url_tasks": dl.url_tasks,
            "media_tasks": dl.media_tasks,
        }
        _atomic_write_json(latest_path, latest_payload)

        logger.warning(
            "[Pipeline] env=%s run_id=%s no_changes=True (skipping final_* outputs)",
            env_mode.upper(), run_id
        )

        # Return paths pointing to latest only
        return PipelineResult(
            env_mode=env_mode.upper(),
            run_id=run_id,
            stats=latest_payload["stats"],
            final_path="",
            report_path="",
            latest_path=str(latest_path),
        )

    # Build output payload
    payload = {
        "meta": {
            "env_mode": env_mode.upper(),
            "run_id": run_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        },
        "stats": {
            "downloader": dl.stats,
            "enricher": enrich_stats,
            "improver": improve_stats,
        },
        "docs": dl.docs,
        "url_tasks": dl.url_tasks,
        "media_tasks": dl.media_tasks,
    }

    out_dir = _output_dir(env_mode, root=output_root)
    final_docs_path = out_dir / f"final_output_{run_id}.json"
    final_report_path = out_dir / f"final_report_{run_id}.json"

    latest_path = Path(output_root) / env_mode.upper() / "latest.json"

    # Save full output
    _atomic_write_json(final_docs_path, dl.docs)

    # 2) Save report (meta + stats + counts)
    report = {
        "meta": {
            "env_mode": env_mode.upper(),
            "run_id": run_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        },
        "stats": {
            "downloader": dl.stats,
            "enricher": enrich_stats,
            "improver": improve_stats,
        },
        "counts": {
            "docs": len(dl.docs),
            "url_tasks": len(dl.url_tasks),
            "media_tasks": len(dl.media_tasks),
        },
        "paths": {
            "final_docs": str(final_docs_path),
            "final_report": str(final_report_path),
            "latest": str(latest_path),
        },
    }
    _atomic_write_json(final_report_path, report)

    # 3) Keep latest.json as the full payload for reuse
    payload = {
        "meta": report["meta"],
        "stats": report["stats"],
        "docs": dl.docs,
        "url_tasks": dl.url_tasks,
        "media_tasks": dl.media_tasks,
    }
    _atomic_write_json(latest_path, payload)

    logger.warning(
        "[Pipeline] env=%s run_id=%s docs=%d saved_docs=%s",
        env_mode.upper(), run_id, len(dl.docs), str(final_docs_path)
    )

    return PipelineResult(
        env_mode=env_mode.upper(),
        run_id=run_id,
        stats=report["stats"],
        final_path=str(final_docs_path),
        report_path=str(final_report_path),
        latest_path=str(latest_path),
    )


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

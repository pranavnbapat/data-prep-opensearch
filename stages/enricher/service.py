from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from pipeline.io import atomic_write_json, update_latest_pointer, run_stamp, output_dir
from pipeline.locks import acquire_job_lock, release_job_lock
from stages.enricher.core import enrich_docs_via_routes, index_docs_by_llid
from stages.enricher.utils import load_latest_downloader_output, load_latest_enricher_output


logger = logging.getLogger(__name__)


def run_enricher_stage(
    *,
    env_mode: str,
    output_root: str,
    extractor_workers: int,
    transcribe_workers: int,
    max_chars: Optional[int],
    use_lock: bool = True,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    lock = None
    if use_lock:
        lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="enricher")

    try:
        in_path, docs = load_latest_downloader_output(env_mode, output_root)
        prev_path, prev_docs = load_latest_enricher_output(env_mode, output_root)
        prev_index = index_docs_by_llid(prev_docs)

        stats = enrich_docs_via_routes(
            docs,
            prev_enriched_index=prev_index,
            extractor_workers=extractor_workers,
            transcribe_workers=transcribe_workers,
            max_chars=max_chars,
            should_cancel=should_cancel,
        )

        patched = int(stats.get("patched") or 0)
        carried = int(stats.get("carry_forward_copied") or 0)
        if patched == 0 and carried == 0 and prev_path is not None:
            logger.warning("[EnricherStage] no_changes; keeping_prev=%s", str(prev_path))
            return {"in": str(in_path), "out": str(prev_path), "stats": stats, "no_changes": True}

        run_id = run_stamp()
        out_dir = output_dir(env_mode, root=output_root)
        out_path = out_dir / f"final_enriched_{run_id}.json"
        payload = {
            "meta": {
                "env_mode": env_mode.upper(),
                "run_id": run_id,
                "created_from": str(in_path),
                "created_from_prev_enriched": (str(prev_path) if prev_path else None),
                "stage": "enricher",
            },
            "stats": stats,
            "docs": docs,
        }

        atomic_write_json(out_path, payload)
        update_latest_pointer(env_mode, output_root, "latest_enriched.json", out_path)
        logger.warning("[EnricherStage] in=%s out=%s", str(in_path), str(out_path))
        return {"in": str(in_path), "out": str(out_path), "stats": stats}

    finally:
        if lock is not None:
            release_job_lock(lock)


if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()
    output_root = os.getenv("OUTPUT_ROOT", "output")
    exw = int(os.getenv("EXTRACTOR_MAX_WORKERS", "4"))
    trw = int(os.getenv("TRANSCRIBE_MAX_WORKERS", "3"))
    max_chars = int(os.getenv("ENRICH_MAX_CHARS", "0")) or None

    res = run_enricher_stage(
        env_mode=env_mode,
        output_root=output_root,
        extractor_workers=exw,
        transcribe_workers=trw,
        max_chars=max_chars,
    )
    print(json.dumps(res, indent=2))

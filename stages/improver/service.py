# improver.py

from __future__ import annotations

import json
import logging
import os
import time

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from common.cancellation import JobCancelled
from pipeline.io import atomic_write_json, update_latest_pointer, run_stamp, output_dir
from pipeline.locks import acquire_job_lock, release_job_lock
from stages.improver.config import BASE_VLLM_HOST
from stages.improver.engine import improve_doc_in_place
from stages.improver.utils import (load_latest_enricher_output, load_latest_improver_output, should_skip_improve,
                                   classify_failure, carry_forward_previous_improvements, compute_improver_fp)


logger = logging.getLogger(__name__)
DOC_DELAY_SEC = float(os.getenv("IMPROVER_DOC_DELAY_SEC", "0"))


def _index_docs_by_llid(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        llid = d.get("_orig_id") or d.get("_id")
        if isinstance(llid, str) and llid:
            idx[llid] = d
    return idx


# ---------------- LLM hook (placeholder) ----------------

def improve_one_doc_via_llm(doc: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    # Delegate to the real engine (mutates doc in-place)
    return improve_doc_in_place(doc)


# ---------------- Core stage ----------------

def run_improver_stage(
    *,
    env_mode: str,
    output_root: str,
    max_docs: Optional[int] = None,
    input_path: Optional[str] = None,
    input_docs: Optional[List[Dict[str, Any]]] = None,
    use_lock: bool = True,
    should_cancel: Optional[Callable[[], bool]] = None,
    processed_callback: Optional[Callable[[Dict[str, Any], Dict[str, Any]], None]] = None,
) -> Optional[Dict[str, Any]]:
    lock = None
    if use_lock:
        lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="improver")

    try:
        if isinstance(input_docs, list):
            docs = input_docs
            in_path = Path(input_path) if input_path else None
        else:
            try:
                in_path, docs = load_latest_enricher_output(env_mode, output_root)
            except RuntimeError as e:
                msg = str(e)
                if "No enricher output found" in msg:
                    logger.warning("[ImproverStage] no input found: %s", str(e))
                    return None
                raise

        logger.warning("[ImproverStart] env=%s input=%s total_docs=%d", env_mode.upper(), str(in_path), len(docs))
        logger.warning("[ImproverStart] vllm_host=%s model=%s", (BASE_VLLM_HOST or "").rstrip("/"),
                       os.getenv("VLLM_MODEL", ""))

        prev_path, prev_docs = load_latest_improver_output(env_mode, output_root)
        prev_index = _index_docs_by_llid(prev_docs)
        run_id = run_stamp()
        out_dir = output_dir(env_mode, root=output_root)
        out_path = out_dir / f"final_improved_{run_id}.json"

        total = len(docs)
        if isinstance(max_docs, int) and max_docs > 0:
            # limit deterministically by original doc order (not index order)
            docs = docs[:max_docs]

        skipped_prev_done = 0
        attempted = 0
        improved = 0
        failed = 0
        failure_reasons: Dict[str, int] = {}
        carry_forward_copied = 0

        t0 = time.perf_counter()

        def current_stats() -> Dict[str, Any]:
            elapsed = time.perf_counter() - t0
            return {
                "total_docs_in_input": total,
                "docs_processed": len(docs),
                "attempted": attempted,
                "improved": improved,
                "failed": failed,
                "skipped_prev_done": skipped_prev_done,
                "carry_forward_copied": carry_forward_copied,
                "failure_reasons": failure_reasons,
                "elapsed_sec": round(elapsed, 2),
            }

        def write_checkpoint(*, complete: bool) -> None:
            payload = {
                "meta": {
                    "env_mode": env_mode.upper(),
                    "run_id": run_id,
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "created_from": (str(in_path) if in_path else None),
                    "created_from_prev_improved": (str(prev_path) if prev_path else None),
                    "stage": "improver",
                    "checkpoint_complete": complete,
                },
                "stats": current_stats(),
                "docs": docs,
            }
            atomic_write_json(out_path, payload)
            update_latest_pointer(env_mode, output_root, "latest_improved.json", out_path)

        def persist_processed_doc(doc: Dict[str, Any]) -> None:
            if processed_callback is None:
                return
            try:
                processed_callback(doc, current_stats())
            except Exception:
                logger.exception("[ImproverStage] processed_callback failed")

        for i, d in enumerate(docs, start=1):
            if should_cancel and should_cancel():
                raise JobCancelled("Job canceled during improver execution")
            llid = d.get("_orig_id") or d.get("_id")
            if not isinstance(llid, str) or not llid:
                failed += 1
                failure_reasons["missing_llid"] = failure_reasons.get("missing_llid", 0) + 1
                write_checkpoint(complete=False)
                continue

            prev = prev_index.get(llid)
            d["_improver_fp"] = compute_improver_fp(d)
            if isinstance(prev, dict) and not isinstance(prev.get("_improver_fp"), str):
                prev["_improver_fp"] = compute_improver_fp(prev)

            # Always carry forward previous improvements if they exist
            if carry_forward_previous_improvements(d, prev):
                carry_forward_copied += 1

            # Now decide whether to skip
            if should_skip_improve(d, prev):
                skipped_prev_done += 1
                write_checkpoint(complete=False)
                persist_processed_doc(d)
                continue

            title = d.get("title") or ""
            title_snip = (title[:50] + "…") if isinstance(title, str) and len(title) > 50 else (
                title if isinstance(title, str) else "")

            # attempted is "LLM attempts", i/total is "position in input list"
            logger.warning('[ImproverDoc] %d/%d title="%s"',
                           i, len(docs), title_snip)

            attempted += 1

            if attempted % 25 == 0:
                logger.warning("[ImproverProgress] attempted=%d improved=%d failed=%d skipped_prev=%d",
                               attempted, improved, failed, skipped_prev_done)

            ok, reason = improve_one_doc_via_llm(d)

            if ok:
                d["improved"] = 1
                improved += 1
            else:
                failed += 1
                tag = classify_failure(reason)
                failure_reasons[tag] = failure_reasons.get(tag, 0) + 1

            write_checkpoint(complete=False)
            persist_processed_doc(d)

            # Throttle between documents to avoid overloading vLLM / proxy
            if DOC_DELAY_SEC > 0:
                time.sleep(DOC_DELAY_SEC)

        stats: Dict[str, Any] = current_stats()

        # If nothing changed and we have a previous snapshot, don't create a new file
        if improved == 0 and carry_forward_copied == 0 and prev_path is not None:
            logger.warning("[ImproverStage] no_changes; keeping_prev=%s", str(prev_path))
            return {"in": str(in_path), "out": str(prev_path), "stats": stats, "no_changes": True}

        write_checkpoint(complete=True)

        logger.warning("[ImproverStage] in=%s out=%s stats=%s", str(in_path), str(out_path), stats)
        return {"in": str(in_path), "out": str(out_path), "stats": stats}

    finally:
        if lock is not None:
            release_job_lock(lock)


# ---------------- CLI ----------------
if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()
    output_root = os.getenv("OUTPUT_ROOT", "output")

    max_docs_env = int(os.getenv("IMPROVER_MAX_DOCS", "0")) or None

    try:
        res = run_improver_stage(
            env_mode=env_mode,
            output_root=output_root,
            max_docs=max_docs_env,
            use_lock=True,
        )
    except RuntimeError as e:
        # Final fallback
        logger.warning("[ImproverCLI] aborted: %s", e)
        res = {"status": "error", "reason": str(e)}

    # Only print structured output if there is a result
    if res is not None:
        print(json.dumps(res, indent=2))

# enricher.py

from __future__ import annotations

import json
import logging
import os
import random
import time

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from deapi_transcribe import transcribe_video, DeapiError
from job_lock import acquire_job_lock, release_job_lock
from io_helpers import (resolve_latest_pointer, find_latest_matching, atomic_write_json, update_latest_pointer,
                        run_stamp, output_dir)
from enricher_utils import (pagesense_one, custom_transcribe_one, load_stage_payload, classify_url_for_enrichment,
                            target_url_for_enrichment, has_enrich_via, should_skip, is_placeholder_content,
                            is_deapi_platform_url)


logger = logging.getLogger(__name__)

def load_latest_downloader_output(env_mode: str, output_root: str) -> Tuple[Path, List[Dict[str, Any]]]:
    # Prefer pointer file first
    p = resolve_latest_pointer(env_mode, output_root, "latest_downloaded.json")
    if p is None:
        p = find_latest_matching(env_mode, output_root, "final_output_*.json")
    if p is None:
        raise RuntimeError(f"No downloader output found under {output_root}/{env_mode.upper()}/")

    payload = load_stage_payload(p)

    # Support both shapes: full payload dict OR raw docs list
    if isinstance(payload, dict):
        docs = payload.get("docs") or []
    elif isinstance(payload, list):
        docs = payload
    else:
        raise RuntimeError(f"Unexpected payload shape in {p}: {type(payload)}")

    if not isinstance(docs, list):
        raise RuntimeError(f"Invalid docs in {p}")

    return p, docs

def load_latest_enricher_output(env_mode: str, output_root: str) -> Tuple[Optional[Path], List[Dict[str, Any]]]:
    # Prefer pointer file first
    p = resolve_latest_pointer(env_mode, output_root, "latest_enriched.json")
    if p is None:
        p = find_latest_matching(env_mode, output_root, "final_enriched_*.json")
    if p is None:
        return None, []

    payload = load_stage_payload(p)
    if isinstance(payload, dict):
        docs = payload.get("docs") or []
    elif isinstance(payload, list):
        docs = payload
    else:
        return p, []

    if not isinstance(docs, list):
        return p, []

    return p, docs

def _index_docs_by_llid(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        llid = d.get("_orig_id") or d.get("_id")
        if isinstance(llid, str) and llid:
            idx[llid] = d
    return idx

def _now_perf() -> float:
    return time.perf_counter()

def _inc(counter: Dict[str, int], key: str, n: int = 1) -> None:
    counter[key] = counter.get(key, 0) + n

def _env_bool(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _norm_mimetype(m: Any) -> str:
    if not isinstance(m, str):
        return ""
    return m.strip().lower()

def _norm_ext(e: Any) -> str:
    if not isinstance(e, str):
        return ""
    e = e.strip().lower()
    if e.startswith("."):
        e = e[1:]
    return e

# ---------------- Small utilities ----------------


# ---------------- deAPI transcription ----------------
def transcribe_with_deapi(*, video_url: Optional[str] = None, video_path: Optional[Path] = None) -> Optional[str]:
    """
    Uses deAPI to transcribe a video URL and returns plain text or None.
    """
    api_key = (os.getenv("DEAPI_API_KEY") or "").strip()
    if not api_key:
        logger.error("[deAPI] Missing DEAPI_API_KEY env var")
        return None

    model = (os.getenv("DEAPI_TRANSCRIBE_MODEL") or "WhisperLargeV3").strip() or "WhisperLargeV3"
    include_ts = _env_bool("DEAPI_INCLUDE_TIMESTAMPS", False)

    # Polling knobs (transcription can take time)
    timeout_s = int(os.getenv("DEAPI_HTTP_TIMEOUT", "60"))
    poll_interval_s = float(os.getenv("DEAPI_POLL_INTERVAL_SEC", "2.0"))
    max_wait_s = int(os.getenv("DEAPI_MAX_WAIT_SEC", str(12 * 60)))  # 12 min default

    try:
        if video_url is not None and isinstance(video_url, (bytes, bytearray)):
            video_url = video_url.decode("utf-8", "ignore")

        video_arg: Optional[str] = None
        if isinstance(video_url, str) and video_url.strip():
            video_arg = video_url.strip()
        elif video_path is not None:
            video_arg = str(video_path)

        if not video_arg:
            logger.error("[deAPI] Missing video source (both url and path empty)")
            return None

        result = transcribe_video(
            api_key=api_key,
            video=video_arg,
            model=model,
            include_timestamps=include_ts,
            return_result_in_response=True,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            max_wait_s=max_wait_s,
        )

        text = (result.transcript_text or "").strip()
        if not text:
            logger.warning(
                "[deAPI] Empty transcript. status=%s request_id=%s url_host=%s",
                result.status, result.request_id, video_url
            )
            return None
        return text

    except DeapiError as e:
        logger.error("[deAPI] Transcription failed url_host=%s err=%s", video_url, e)
        return None
    except Exception as e:
        logger.exception("[deAPI] Unexpected error url_host=%s err=%s", video_url, e)
        return None
# ---------------- deAPI transcription ----------------


def carry_forward_enrichment(doc: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> bool:
    """
    If prev has valid enrichment and current doc doesn't, copy it over.
    Returns True if we copied anything.
    """
    if not prev:
        return False

    prev_text = prev.get("ko_content_flat")
    prev_enriched = int(prev.get("enriched") or 0) == 1

    # Only carry forward if prev looks enriched and current isn't
    if not prev_enriched or not isinstance(prev_text, str) or is_placeholder_content(prev_text):
        return False

    cur_text = doc.get("ko_content_flat")
    cur_enriched = int(doc.get("enriched") or 0) == 1

    if cur_enriched and isinstance(cur_text, str) and not is_placeholder_content(cur_text):
        return False  # already has content

    # Copy the enrichment payload
    doc["ko_content_flat"] = prev_text
    doc["enriched"] = 1

    # Preserve provenance if present
    if isinstance(prev.get("ko_content_source"), str):
        doc["ko_content_source"] = prev["ko_content_source"]
    if isinstance(prev.get("ko_content_url"), str):
        doc["ko_content_url"] = prev["ko_content_url"]

    logger.info(
        "[EnrichCarryForward] llid=%s source=%s",
        doc.get("_orig_id") or doc.get("_id"),
        prev.get("ko_content_source")
    )

    return True

def enrich_docs_via_routes(
    docs: List[Dict[str, Any]],
    *,
    prev_enriched_index: Dict[str, Dict[str, Any]],
    extractor_workers: int,
    transcribe_workers: int,
    max_chars: Optional[int],
) -> Dict[str, Any]:
    """
    Mutates docs in-place.

    Only enriches docs that have enrich_via in {"pagesense", "api_transcribe", "custom_transcribe"}.
    Skips if already enriched in previous enricher snapshot AND @id unchanged.
    Writes enriched text into ko_content_flat.
    """

    def _retry_n(*, fn, attempts: int, base_sleep_s: float, jitter_s: float, label: str):
        """
        Retry wrapper for per-record calls.
        Adds logs so long-running calls don't look stuck.
        """
        last = None
        for i in range(attempts):
            attempt_no = i + 1
            t0 = time.perf_counter()
            logger.info("[EnrichTry] %s attempt=%d/%d", label, attempt_no, attempts)

            last = fn()

            dt = time.perf_counter() - t0
            if last is not None:
                logger.info("[EnrichTryOK] %s attempt=%d/%d dt=%.2fs", label, attempt_no, attempts, dt)
                return last

            logger.warning("[EnrichTryFail] %s attempt=%d/%d dt=%.2fs", label, attempt_no, attempts, dt)

            if i < attempts - 1:
                delay = (base_sleep_s ** i) + random.uniform(0.0, jitter_s)
                logger.info("[EnrichBackoff] %s sleep=%.2fs", label, delay)
                time.sleep(delay)

        return None

    def _pagesense_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        """
        Returns (text_or_none, tag)
        """
        def _call():
            # pagesense_one returns (llid, url, text, tag)
            res = pagesense_one(llid=llid, url=url, http_timeout=0)
            # Support both 4-tuple and 5-tuple returns
            if isinstance(res, tuple) and len(res) >= 4:
                _llid, _url, text, tag = res[0], res[1], res[2], res[3]
            else:
                text, tag = None, "pagesense:failed"

            if text and not is_placeholder_content(text):
                return (text, tag)
            return None

        out = _retry_n(
            fn=_call,
            attempts=int(os.getenv("ENRICH_PAGESENSE_TRIES", "3")),
            base_sleep_s=float(os.getenv("ENRICH_PAGESENSE_BACKOFF", "1.6")),
            jitter_s=float(os.getenv("ENRICH_PAGESENSE_JITTER", "0.8")),
            label=f"pagesense llid={llid} host={url}",
        )

        if out is None:
            return None, "pagesense:empty"
        return out[0], "pagesense"

    def _custom_transcribe_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        """
        Returns (text_or_none, tag)
        """

        def _call():
            # custom_transcribe_one returns (llid, url, text, tag)
            _llid, _url, text, tag = custom_transcribe_one(llid=llid, url=url)
            return text.strip() if isinstance(text, str) and text.strip() else None

        text = _retry_n(
            fn=_call,
            attempts=int(os.getenv("ENRICH_TRANSCRIBE_TRIES", "3")),
            base_sleep_s=float(os.getenv("ENRICH_TRANSCRIBE_BACKOFF", "1.7")),
            jitter_s=float(os.getenv("ENRICH_TRANSCRIBE_JITTER", "0.8")),
            label=f"custom_transcribe llid={llid} host={url}",
        )

        if not text:
            return None, "custom_transcribe:failed"
        return text, "custom_transcribe"

    def _api_transcribe_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        """
        Returns (text_or_none, tag)
        """
        if not is_deapi_platform_url(url):
            return None, "api_transcribe:unsupported_url"

        def _call():
            text = transcribe_with_deapi(video_url=url)
            return text.strip() if isinstance(text, str) and text.strip() else None

        text = _retry_n(
            fn=_call,
            attempts=int(os.getenv("ENRICH_TRANSCRIBE_TRIES", "3")),
            base_sleep_s=float(os.getenv("ENRICH_TRANSCRIBE_BACKOFF", "1.7")),
            jitter_s=float(os.getenv("ENRICH_TRANSCRIBE_JITTER", "0.8")),
            label=f"api_transcribe llid={llid} host={url}",
        )

        if not text:
            return None, "api_transcribe:failed"
        return text, "api_transcribe"


    if not docs:
        return {"patched": 0, "skipped": 0, "failed": 0}

    enable_pagesense = _env_bool("ENRICH_ENABLE_URL_EXTRACT", True)
    enable_api_transcribe = _env_bool("ENRICH_ENABLE_API_TRANSCRIBE", True)
    enable_custom_transcribe = _env_bool("ENRICH_ENABLE_CUSTOM_TRANSCRIBE", True)

    idx = _index_docs_by_llid(docs)

    # ---- build candidates ----
    candidates: List[Tuple[str, str, str]] = []  # (llid, route, target_url)
    skipped_no_route = 0
    skipped_prev_done = 0
    skipped_disabled = 0
    skipped_bad_target = 0
    skipped_missing_target = 0
    carry_forward_copied = 0

    for llid, doc in idx.items():
        if not has_enrich_via(doc):
            logger.debug("[EnrichSkip] id=%s reason=no_route", llid)
            skipped_no_route += 1
            continue

        prev = prev_enriched_index.get(llid)

        # Carry forward previous enrichment so we don't overwrite good content with placeholders
        if carry_forward_enrichment(doc, prev):
            carry_forward_copied += 1

        if should_skip(doc, prev):
            logger.debug("[EnrichSkip] id=%s reason=prev_done", llid)
            skipped_prev_done += 1
            continue

        route = doc.get("enrich_via")
        if route == "pagesense" and not enable_pagesense:
            skipped_disabled += 1
            continue
        if route == "api_transcribe" and not enable_api_transcribe:
            skipped_disabled += 1
            continue
        if route == "custom_transcribe" and not enable_custom_transcribe:
            skipped_disabled += 1
            continue

        target = target_url_for_enrichment(doc)
        if not target:
            logger.warning("[EnrichSkip] id=%s route=%s reason=missing_target", llid, route)
            skipped_missing_target += 1
            continue

        ok, reason = classify_url_for_enrichment(target)
        if not ok:
            logger.warning(
                "[EnrichSkip] id=%s route=%s reason=%s target=%r",
                llid, route, reason, target
            )
            skipped_bad_target += 1
            continue

        logger.info(
            "[EnrichQueue] ID=%s route=%s target_host=%s",
            llid, route, target
        )

        candidates.append((llid, route, target))

    if not candidates:
        return {
            "patched": 0,
            "skipped": len(docs),
            "failed": 0,
            "skipped_no_route": skipped_no_route,
            "skipped_prev_done": skipped_prev_done,
            "skipped_disabled": skipped_disabled,
            "skipped_missing_target": skipped_missing_target,
            "carry_forward_copied": carry_forward_copied,
            "candidates": 0,
        }

    patched_pagesense = 0
    patched_transcribe = 0
    failed_pagesense = 0
    failed_transcribe = 0

    failure_reasons: Dict[str, int] = {}

    # Apply patch helper
    def _apply_success(doc: Dict[str, Any], *, text: str, source: str, url: str) -> None:
        if isinstance(max_chars, int) and max_chars > 0:
            text = text[:max_chars]
        doc["ko_content_flat"] = text
        doc["ko_content_source"] = source  # "pagesense" or "api_transcribe" or "custom_transcribe"
        doc["ko_content_url"] = url
        doc["enriched"] = 1

    # ---- sequential runner: one KO at a time, with per-record retries ----
    per_record_delay = float(os.getenv("ENRICH_PER_RECORD_DELAY_SEC", "0.25"))

    total = len(candidates)

    for n, (llid, route, target) in enumerate(candidates, start=1):
        doc = idx.get(llid)
        if not doc:
            continue

        # Per-record header
        t0 = time.perf_counter()
        logger.info(
            "[EnrichRun] %d/%d id=%s route=%s target=%s",
            n, total, llid, route, target
        )

        # Small jitter/delay to avoid hammering even in serial mode
        if per_record_delay > 0:
            time.sleep(per_record_delay + random.uniform(0.0, 0.15))

        if route == "pagesense":
            text, tag = _pagesense_try(llid, target)
            if not text:
                failed_pagesense += 1
                _inc(failure_reasons, tag)
                logger.warning(
                    "[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs",
                    n, total, llid, route, tag, time.perf_counter() - t0
                )
                continue

            _apply_success(doc, text=text, source="pagesense", url=target)
            patched_pagesense += 1
            logger.info(
                "[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs",
                n, total, llid, route, len(text), time.perf_counter() - t0
            )
            continue

        if route == "api_transcribe":
            text, tag = _api_transcribe_try(llid, target)
            if not text:
                failed_transcribe += 1
                _inc(failure_reasons, tag)
                logger.warning(
                    "[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs",
                    n, total, llid, route, tag, time.perf_counter() - t0
                )
                continue

            _apply_success(doc, text=text, source="api_transcribe", url=target)
            patched_transcribe += 1
            logger.info(
                "[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs",
                n, total, llid, route, len(text), time.perf_counter() - t0
            )
            continue

        if route == "custom_transcribe":
            text, tag = _custom_transcribe_try(llid, target)
            if not text:
                failed_transcribe += 1
                _inc(failure_reasons, tag)
                logger.warning(
                    "[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs",
                    n, total, llid, route, tag, time.perf_counter() - t0
                )
                continue

            _apply_success(doc, text=text, source="custom_transcribe", url=target)
            patched_transcribe += 1
            logger.info(
                "[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs",
                n, total, llid, route, len(text), time.perf_counter() - t0
            )
            continue

    patched_total = patched_pagesense + patched_transcribe
    failed_total = failed_pagesense + failed_transcribe

    logger.warning(
        "[EnricherDone] candidates=%d patched=%d failed=%d skipped_prev=%d",
        len(candidates), patched_total, failed_total, skipped_prev_done,
    )

    return {
        "total_docs": len(docs),
        "candidates": len(candidates),
        "patched": patched_total,
        "patched_pagesense": patched_pagesense,
        "patched_transcribe": patched_transcribe,
        "failed": failed_total,
        "failed_pagesense": failed_pagesense,
        "failed_transcribe": failed_transcribe,
        "skipped_no_route": skipped_no_route,
        "skipped_prev_done": skipped_prev_done,
        "skipped_disabled": skipped_disabled,
        "skipped_missing_target": skipped_missing_target,
        "carry_forward_copied": carry_forward_copied,
        "failure_reasons": failure_reasons,
    }


# ---------------- Routing decision ----------------
def run_enricher_stage(
    *,
    env_mode: str,
    output_root: str,
    extractor_workers: int,
    transcribe_workers: int,
    max_chars: Optional[int],
    use_lock: bool = True,
) -> Dict[str, Any]:
    lock = None
    if use_lock:
        lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="enricher")

    try:
        in_path, docs = load_latest_downloader_output(env_mode, output_root)

        prev_path, prev_docs = load_latest_enricher_output(env_mode, output_root)
        prev_index = _index_docs_by_llid(prev_docs)

        stats = enrich_docs_via_routes(
            docs,
            prev_enriched_index=prev_index,
            extractor_workers=extractor_workers,
            transcribe_workers=transcribe_workers,
            max_chars=max_chars,
        )

        # If nothing changed and we have a previous snapshot, don't create a new file
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


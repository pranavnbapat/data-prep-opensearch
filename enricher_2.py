# enricher.py

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from deapi_transcribe import transcribe_video, DeapiError
from job_lock import acquire_job_lock, release_job_lock
from io_helpers import (resolve_latest_pointer, find_latest_matching, atomic_write_json, update_latest_pointer,
                        run_stamp, output_dir)
from utils import (
    fetch_url_text_with_extractor,
    flatten_ko_content
)


logger = logging.getLogger(__name__)
_tls = threading.local()

# ---------------- HTTP session helpers ----------------
def get_public_session(timeout: int = 30) -> requests.Session:
    """
    Thread-local session for calling external services (PageSense, sniffing, etc).
    Does NOT include backend basic auth.
    """
    key = f"public|{timeout}"
    sess = getattr(_tls, "session", None)
    sess_key = getattr(_tls, "session_key", None)

    if sess is None or sess_key != key:
        try:
            if sess is not None:
                sess.close()
        except Exception:
            pass

        s = requests.Session()
        s.headers.update({"accept": "application/json"})

        # Default timeout wrapper (like downloader)
        original = s.request

        def _with_timeout(method, url, **kw):
            kw.setdefault("timeout", timeout)
            return original(method, url, **kw)

        s.request = _with_timeout  # type: ignore[attr-defined]

        _tls.session = s
        _tls.session_key = key
        sess = s

    return sess
# ---------------- HTTP session helpers ----------------


# ---------------- Small utilities ----------------
def _load_stage_payload(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def load_latest_downloader_output(env_mode: str, output_root: str) -> Tuple[Path, List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Prefer pointer file first
    p = resolve_latest_pointer(env_mode, output_root, "latest_downloaded.json")
    if p is None:
        p = find_latest_matching(env_mode, output_root, "final_output_*.json")
    if p is None:
        raise RuntimeError(f"No downloader output found under {output_root}/{env_mode.upper()}/")

    payload = _load_stage_payload(p)

    # Support both shapes: full payload dict OR raw docs list
    if isinstance(payload, dict):
        docs = payload.get("docs") or []
        url_tasks = payload.get("url_tasks") or []
        media_tasks = payload.get("media_tasks") or []
    elif isinstance(payload, list):
        docs = payload
        url_tasks = []
        media_tasks = []
    else:
        raise RuntimeError(f"Unexpected payload shape in {p}: {type(payload)}")

    if not isinstance(docs, list):
        raise RuntimeError(f"Invalid docs in {p}")

    return p, docs, url_tasks, media_tasks

def _is_placeholder_content(val: Any) -> bool:
    if not isinstance(val, str):
        return True
    return val.strip().lower() in ("", "no content present")

def _needs_url_extract(doc: Dict[str, Any]) -> bool:
    """Return True if doc is a URL-only KO that needs external extraction."""
    return (
        bool(doc.get("is_url_only")) is True
        and bool(doc.get("ko_is_hosted")) is False
        and _is_placeholder_content(doc.get("ko_content_flat"))
        and isinstance(doc.get("first_url"), str)
        and doc["first_url"].strip() != ""
    )

def _index_docs_by_llid(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        llid = d.get("_orig_id") or d.get("_id")
        if isinstance(llid, str) and llid:
            idx[llid] = d
    return idx

def _now_perf() -> float:
    return time.perf_counter()

def _safe_host(url: str) -> str:
    # Keep logs readable; avoids dumping full URLs with long querystrings.
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
        return f"{u.scheme}://{u.netloc}"
    except Exception:
        return "unknown://"

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

def _is_youtube_url(u: str) -> bool:
    if not isinstance(u, str):
        return False
    v = u.lower()
    return (
        "youtube.com/watch" in v or
        "youtube.com/live" in v or
        "youtube.com/shorts" in v or
        "youtu.be/" in v
    )
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
        result = transcribe_video(
            api_key=api_key,
            video=video_url,                   # URL in -> /vid2txt
            video_path=str(video_path),
            model=model,
            include_timestamps=include_ts,
            return_result_in_response=True,    # best chance to get text inline
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            max_wait_s=max_wait_s,
        )
        text = (result.transcript_text or "").strip()
        if not text:
            logger.warning(
                "[deAPI] Empty transcript. status=%s request_id=%s url_host=%s",
                result.status, result.request_id, _safe_host(video_url)
            )
            return None
        return text

    except DeapiError as e:
        logger.error("[deAPI] Transcription failed url_host=%s err=%s", _safe_host(video_url), e)
        return None
    except Exception as e:
        logger.exception("[deAPI] Unexpected error url_host=%s err=%s", _safe_host(video_url), e)
        return None
# ---------------- deAPI transcription ----------------



def enrich_url_only_with_extractor(
    docs: List[Dict[str, Any]],
    *,
    extractor_workers: int,
    max_chars: Optional[int],
) -> Dict[str, Any]:
    """
    Mutates docs in-place.
    For each doc where:
      - is_url_only == True
      - ko_is_hosted == False
      - ko_content_flat is placeholder
    -> call fetch_url_text_with_extractor(first_url) and store into ko_content_flat.
    """
    if not docs:
        return {"patched": 0, "skipped": 0, "failed": 0}

    if not _env_bool("ENRICH_ENABLE_URL_EXTRACT", True):
        logger.info("[Enricher] URL extract disabled by ENRICH_ENABLE_URL_EXTRACT")
        return {"patched": 0, "skipped": len(docs), "failed": 0}

    idx = _index_docs_by_llid(docs)

    # Build candidate list
    candidates: List[Tuple[str, str]] = []
    for llid, doc in idx.items():
        if _needs_url_extract(doc):
            candidates.append((llid, doc["first_url"]))

    if not candidates:
        logger.info("[EnricherURL] No URL-only candidates found")
        return {"patched": 0, "skipped": len(docs), "failed": 0}

    # Concurrency controls
    max_workers = int(os.getenv("EXTRACTOR_MAX_WORKERS", str(extractor_workers)))
    sema = threading.BoundedSemaphore(int(os.getenv("EXTRACTOR_MAX_CONCURRENCY", str(max_workers))))

    patched = 0
    skipped = 0
    failed = 0
    failure_reasons: Dict[str, int] = {}

    def _extract_one(llid: str, url: str) -> Tuple[str, str, Optional[str], str]:
        time.sleep(random.uniform(0.15, 0.5))  # stagger load

        t_wait = _now_perf()
        sema.acquire()
        wait_dt = _now_perf() - t_wait
        if wait_dt > float(os.getenv("ENRICH_SEMA_WAIT_WARN_SEC", "1.0")):
            logger.warning("[EnrichWait] route=external_url_extractor llid=%s wait=%.2fs", llid, wait_dt)

        try:
            sess = get_public_session(timeout=int(os.getenv("EXTRACTOR_HTTP_TIMEOUT", "35")))
            t_call = _now_perf()
            text = fetch_url_text_with_extractor(
                url,
                timeout=int(os.getenv("EXTRACTOR_TIMEOUT", "150")),
                retries=int(os.getenv("EXTRACTOR_RETRIES", "3")),
                backoff=float(os.getenv("EXTRACTOR_BACKOFF", "1.6")),
                min_chars=int(os.getenv("EXTRACTOR_MIN_CHARS", "100")),
                session=sess,
            )
            dt = _now_perf() - t_call
            return llid, url, (text if isinstance(text, str) and text.strip() else None), "external_url_extractor"
        except Exception as e:
            logger.error("[EnrichCallFail] route=external_url_extractor llid=%s host=%s err=%s", llid, _safe_host(url), e)
            return llid, url, None, "external_url_extractor:failed"
        finally:
            sema.release()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_extract_one, llid, url) for llid, url in candidates]
        for fut in as_completed(futures):
            llid, url, text, tag = fut.result()
            doc = idx.get(llid)
            if not doc:
                skipped += 1
                continue

            # Might have been filled already
            if not _is_placeholder_content(doc.get("ko_content_flat")):
                skipped += 1
                continue

            if not text:
                failed += 1
                _inc(failure_reasons, tag)
                continue

            if isinstance(max_chars, int) and max_chars > 0:
                text = text[:max_chars]

            doc["ko_content_flat"] = text
            doc["ko_content_source"] = "external_url_extractor"
            doc["ko_content_url"] = url
            patched += 1

    logger.warning(
        "[EnricherURLDone] patched=%d failed=%d skipped=%d failure_reasons=%s",
        patched, failed, skipped, failure_reasons
    )
    return {"patched": patched, "failed": failed, "skipped": skipped, "failure_reasons": failure_reasons}


# ---------------- Routing decision ----------------
@dataclass(frozen=True)
class RouteDecision:
    action: str  # "extract" | "transcribe" | "ocr" | "skip"
    tag: str             # used in ko_content_source and logging
    reason: str = ""     # extra human-friendly reason for debug


_MEDIA_EXTS = {"mp4", "mov", "m4a", "mp3", "wav", "aac", "ogg", "webm"}
_PDF_EXTS = {"pdf"}
_IMAGE_EXTS = {"jpg", "jpeg", "png", "svg", "webp", "tif", "tiff"}


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
        in_path, docs, url_tasks, media_tasks = load_latest_downloader_output(env_mode, output_root)

        # ✅ Step 1 only: URL-only extraction
        stats = enrich_url_only_with_extractor(
            docs,
            extractor_workers=extractor_workers,
            max_chars=max_chars,
        )

        run_id = run_stamp()
        out_dir = output_dir(env_mode, root=output_root)
        out_path = out_dir / f"final_enriched_{run_id}.json"

        payload = {
            "meta": {
                "env_mode": env_mode.upper(),
                "run_id": run_id,
                "created_from": str(in_path),
                "stage": "enricher",
            },
            "stats": stats,
            "docs": docs,
            "url_tasks": url_tasks,
            "media_tasks": media_tasks,
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


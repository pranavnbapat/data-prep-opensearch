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


# ---------------- Routing decision ----------------
@dataclass(frozen=True)
class RouteDecision:
    action: str          # "extract" | "transcribe" | "skip"
    tag: str             # used in ko_content_source and logging
    reason: str = ""     # extra human-friendly reason for debug


_MEDIA_EXTS = {"mp4", "mov", "m4a", "mp3", "wav", "aac", "ogg", "webm"}
_PDF_EXTS = {"pdf"}
_IMAGE_EXTS = {"jpg", "jpeg", "png", "svg", "webp", "tif", "tiff"}


def _guess_ext_from_headers(content_type: str, content_disp: str, url: str) -> str:
    """
    Best-effort extension inference from HTTP headers + URL path.
    This is intentionally conservative; it only helps disambiguate octet-stream URL-only items.
    """
    ct = (content_type or "").split(";")[0].strip().lower()
    cd = (content_disp or "").lower()

    # Try filename= from Content-Disposition
    try:
        import re
        m = re.search(r'filename\*?=(?:utf-8\'\')?"?([^";]+)"?', cd)
        if m:
            fn = m.group(1).strip().strip('"')
            if "." in fn:
                return _norm_ext(fn.rsplit(".", 1)[-1])
    except Exception:
        pass

    # Try common content-types
    if ct == "application/pdf":
        return "pdf"
    if ct.startswith("video/"):
        # video/mp4 -> mp4, etc.
        return ct.split("/", 1)[-1].strip() or ""
    if ct.startswith("audio/"):
        return ct.split("/", 1)[-1].strip() or ""
    if ct.startswith("image/"):
        return ct.split("/", 1)[-1].strip() or ""

    # Lastly, try URL path suffix
    try:
        from urllib.parse import urlparse
        path = urlparse(url).path or ""
        if "." in path:
            return _norm_ext(path.rsplit(".", 1)[-1])
    except Exception:
        pass

    return ""


def _sniff_url_headers(url: str, session: requests.Session) -> Tuple[str, str]:
    """
    HEAD sniff to improve routing when mimetype is missing/unknown (e.g., application/octet-stream).
    Returns (content_type, content_disposition). Empty strings on failure.

    NOTE: This is for routing only, not for downloading content.
    """
    try:
        # Some hosts reject HEAD; allow fallback to GET with Range if enabled.
        r = session.head(url, allow_redirects=True, timeout=int(os.getenv("HEAD_SNIFF_TIMEOUT", "15")))
        ct = r.headers.get("Content-Type", "") or ""
        cd = r.headers.get("Content-Disposition", "") or ""
        if ct or cd:
            return ct, cd
    except Exception:
        pass

    if _env_bool("HEAD_SNIFF_ALLOW_GET_FALLBACK", False):
        try:
            # Tiny GET: ask for first byte only (many servers honour it).
            headers = {"Range": "bytes=0-0"}
            r = session.get(url, headers=headers, stream=True, allow_redirects=True, timeout=int(os.getenv("HEAD_SNIFF_TIMEOUT", "15")))
            ct = r.headers.get("Content-Type", "") or ""
            cd = r.headers.get("Content-Disposition", "") or ""
            return ct, cd
        except Exception:
            pass

    return "", ""

def _download_media_to_temp(url: str, llid: str, session: requests.Session) -> Optional[Path]:
    """
    Download hosted media to a temp file for deAPI upload.
    """
    tmp_dir = Path(os.getenv("ENRICH_MEDIA_TMP_DIR", "output/_tmp_media"))
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_path = tmp_dir / f"{llid}.bin"

    try:
        with session.get(url, stream=True, allow_redirects=True, timeout=int(os.getenv("MEDIA_DL_TIMEOUT", "120"))) as r:
            r.raise_for_status()
            with out_path.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        return out_path
    except Exception as e:
        logger.error("[MediaDownloadFail] llid=%s url_host=%s err=%s", llid, _safe_host(url), e)
        return None

def decide_route(
    *,
    kind: str,
    url: str,
    is_url_only: bool,
    ko_is_hosted: bool,
    mimetype: str,
    ext: str,
    enable_pdf: bool,
    enable_images: bool,
    enable_head_sniff: bool,
    head_session: Optional[requests.Session],
) -> RouteDecision:
    """
    Decide what to do with this task. This is where your KO distribution logic lives.

    Important: we *never* send PDFs/images to deAPI vid2txt. For now we skip them (or you can add extractors later).
    """
    mt = _norm_mimetype(mimetype)
    ex = _norm_ext(ext)

    # 1) YouTube: always treat as transcription target (URL-only or otherwise)
    if mt == "external/youtube" or _is_youtube_url(url):
        return RouteDecision(action="transcribe", tag="deapi_vid2txt", reason="youtube")

    # 2) Clear media mimetypes
    if mt.startswith("video/") or mt.startswith("audio/"):
        return RouteDecision(action="transcribe", tag="deapi_vid2txt", reason=f"mimetype={mt}")

    # 3) Clear PDF
    if mt == "application/pdf":
        if enable_pdf:
            # Placeholder: wire your PDF extractor here later.
            return RouteDecision(action="skip", tag="skip:pdf:not_implemented", reason="pdf extractor not implemented")
        return RouteDecision(action="skip", tag="skip:pdf:disabled", reason="pdf disabled")

    # 4) Clear images
    if mt.startswith("image/"):
        if enable_images:
            # Placeholder: wire OCR (deAPI img2txt) here later.
            return RouteDecision(action="skip", tag="skip:image:not_implemented", reason="image OCR not implemented")
        return RouteDecision(action="skip", tag="skip:image:disabled", reason="image disabled")

    # 5) Octet-stream / missing / unreliable mimetype: route via extension or sniff
    if mt in ("application/octet-stream", ""):
        # Extension-based decision first (cheap and reliable when present)
        if ex in _MEDIA_EXTS:
            return RouteDecision(action="transcribe", tag="deapi_vid2txt", reason=f"ext={ex}")
        if ex in _PDF_EXTS:
            if enable_pdf:
                return RouteDecision(action="skip", tag="skip:pdf:not_implemented", reason="pdf extractor not implemented")
            return RouteDecision(action="skip", tag="skip:pdf:disabled", reason="pdf disabled")
        if ex in _IMAGE_EXTS:
            if enable_images:
                return RouteDecision(action="skip", tag="skip:image:not_implemented", reason="image OCR not implemented")
            return RouteDecision(action="skip", tag="skip:image:disabled", reason="image disabled")

        # For URL tasks only, we can optionally HEAD-sniff to disambiguate
        if kind == "url" and enable_head_sniff and head_session is not None:
            ct, cd = _sniff_url_headers(url, head_session)
            sniff_ext = _guess_ext_from_headers(ct, cd, url)
            sniff_ext = _norm_ext(sniff_ext)

            if sniff_ext in _MEDIA_EXTS:
                return RouteDecision(action="transcribe", tag="deapi_vid2txt", reason=f"sniff_ext={sniff_ext}")
            if sniff_ext in _PDF_EXTS:
                if enable_pdf:
                    return RouteDecision(action="skip", tag="skip:pdf:not_implemented", reason="pdf extractor not implemented")
                return RouteDecision(action="skip", tag="skip:pdf:disabled", reason="pdf disabled")
            if sniff_ext in _IMAGE_EXTS:
                if enable_images:
                    return RouteDecision(action="skip", tag="skip:image:not_implemented", reason="image OCR not implemented")
                return RouteDecision(action="skip", tag="skip:image:disabled", reason="image disabled")

        # If still unknown:
        # - If it's a URL task, try URL extractor (might be a normal webpage mislabelled as octet-stream).
        # - If it's a media task with unknown type, avoid calling deAPI blindly: skip for now.
        if kind == "url":
            return RouteDecision(action="extract", tag="external_url_extractor", reason="octet-stream url -> try extractor")
        return RouteDecision(action="skip", tag="skip:unknown_media", reason="octet-stream media with unknown ext")

    # 6) Everything else: treat as webpage URL extraction for url tasks, otherwise skip
    if kind == "url":
        return RouteDecision(action="extract", tag="external_url_extractor", reason=f"default url mt={mt or 'none'}")
    return RouteDecision(action="skip", tag="skip:unhandled", reason=f"default media mt={mt or 'none'}")
# ---------------- Routing decision ----------------


# ---------------- Core enrichment entrypoint ----------------
def enrich_external_content(
    docs: List[Dict[str, Any]],
    url_tasks: List[Dict[str, Any]],
    media_tasks: List[Dict[str, Any]],
    *,
    ko_content_mode: str = None,
    previous_index: Optional[Dict[str, Dict[str, Any]]] = None,
    extractor_workers: int = 4,
    transcribe_workers: int = 3,
    max_chars: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Patches docs in-place using a routing logic driven by mimetype/extension/URL pattern.

    Notes:
    - deAPI is used for video/audio transcription only.
    - PDFs/images are currently skipped (by design) unless you later add extractors.
    """
    if not docs:
        return {"patched": 0, "reused": 0, "failed": 0, "skipped": 0}

    ko_content_mode = (ko_content_mode or os.getenv("KO_CONTENT_MODE", "flat_only")).strip() or "flat_only"
    prev_index = previous_index or {}

    # Feature flags
    enable_url_extract = _env_bool("ENRICH_ENABLE_URL_EXTRACT", True)
    enable_transcribe = _env_bool("ENRICH_ENABLE_TRANSCRIBE", True)
    enable_deapi_transcribe = _env_bool("ENRICH_ENABLE_DEAPI_TRANSCRIBE", True)

    # These are routing toggles (future work):
    enable_pdf = _env_bool("ENRICH_ENABLE_PDF_EXTRACT", False)
    enable_images = _env_bool("ENRICH_ENABLE_IMAGE_OCR", False)

    # Sniffing is specifically for octet-stream URL-only ambiguity
    enable_head_sniff = _env_bool("ENRICH_ENABLE_HEAD_SNIFF", True)

    logger.info(
        "[EnricherFlags] url_extract=%s transcribe=%s deapi_transcribe=%s pdf=%s images=%s head_sniff=%s",
        enable_url_extract, enable_transcribe, enable_deapi_transcribe, enable_pdf, enable_images, enable_head_sniff
    )

    idx = _index_docs_by_llid(docs)

    t0 = _now_perf()
    logger.warning(
        "[EnricherStart] docs=%d url_tasks=%d media_tasks=%d ko_content_mode=%s extractor_workers=%d transcribe_workers=%d",
        len(docs), len(url_tasks or []), len(media_tasks or []), ko_content_mode, extractor_workers, transcribe_workers
    )

    # --- build a unified worklist ---
    # Each item: (llid, url, kind, is_url_only, ko_is_hosted, mimetype, ext)
    work: List[Tuple[str, str, str, bool, bool, Optional[str], Optional[str]]] = []

    seen = set()

    for row in url_tasks or []:
        llid = row.get("logical_layer_id")
        url = row.get("first_url") or row.get("url")
        if not llid or not url:
            continue
        if llid in seen:
            continue
        seen.add(llid)
        doc = idx.get(str(llid), {})
        is_url_only = bool(doc.get("is_url_only"))
        ko_is_hosted = bool(doc.get("ko_is_hosted"))
        mt = doc.get("ko_object_mimetype")
        ext = doc.get("ko_object_extension")
        work.append((str(llid), str(url), "url", is_url_only, ko_is_hosted, mt, ext))

    for row in media_tasks or []:
        llid = row.get("logical_layer_id")
        url = row.get("media_url")
        mt = row.get("mimetype")
        if not llid or not url:
            continue
        if llid in seen:
            continue
        seen.add(llid)
        doc = idx.get(str(llid), {})
        is_url_only = bool(doc.get("is_url_only"))  # usually False / missing for hosted media, but safe
        ko_is_hosted = bool(doc.get("ko_is_hosted"))
        ext = doc.get("ko_object_extension")
        work.append((str(llid), str(url), "media", is_url_only, ko_is_hosted, mt, ext))

    if not work:
        return {"patched": 0, "reused": 0, "failed": 0, "skipped": 0}

    kind_counts: Dict[str, int] = {}
    host_counts: Dict[str, int] = {}
    for _llid, url, kind, _mt, _ext in work:
        _inc(kind_counts, kind)
        _inc(host_counts, _safe_host(url))

    logger.info(
        "[EnricherWorklist] total=%d kinds=%s unique_hosts=%d top_host=%s",
        len(work),
        kind_counts,
        len(host_counts),
        (sorted(host_counts.items(), key=lambda kv: kv[1], reverse=True)[:1] or [("n/a", 0)])[0],
    )

    # Concurrency limits (be nice to external services)
    extractor_max = int(os.getenv("EXTRACTOR_MAX_CONCURRENCY", str(extractor_workers)))
    transcribe_max = int(os.getenv("TRANSCRIBE_MAX_CONCURRENCY", str(transcribe_workers)))
    extractor_sema = threading.BoundedSemaphore(extractor_max)
    transcribe_sema = threading.BoundedSemaphore(transcribe_max)

    patched = 0
    reused = 0
    skipped = 0
    failed = 0

    failures: List[Dict[str, Any]] = []
    reused_items: List[Dict[str, Any]] = []

    reused_reasons: Dict[str, int] = {}
    failure_reasons: Dict[str, int] = {}
    source_failures: Dict[str, int] = {}
    skipped_reasons: Dict[str, int] = {}

    # --- reuse from previous snapshot when possible ---
    work2: List[Tuple[str, str, str, Optional[str], Optional[str]]] = []
    for llid, url, kind, mimetype, ext in work:
        doc = idx.get(llid)
        if not doc:
            skipped += 1
            _inc(skipped_reasons, "skipped:missing_doc")
            continue

        cur_flat = doc.get("ko_content_flat")
        if not _is_placeholder_content(cur_flat):
            skipped += 1
            _inc(skipped_reasons, "skipped:already_has_content")
            continue

        prev_doc = prev_index.get(llid)
        if prev_doc:
            prev_flat = prev_doc.get("ko_content_flat")
            prev_url = prev_doc.get("ko_content_url") or prev_doc.get("first_url")
            if (
                    isinstance(prev_flat, str)
                    and prev_flat.strip()
                    and (not _is_placeholder_content(prev_flat))
                    and (not prev_url or prev_url == url)
            ):

                doc["ko_content_flat"] = prev_flat
                if prev_doc.get("ko_content_source"):
                    doc["ko_content_source"] = prev_doc["ko_content_source"]
                doc["ko_content_url"] = url
                reused += 1
                reused_items.append({"llid": llid, "url": url, "source": doc.get("ko_content_source")})
                _inc(reused_reasons, "reuse:prev_flat_same_url")
                continue

        work2.append((llid, url, kind, mimetype, ext))

    if reused:
        max_show = int(os.getenv("REUSE_LOG_MAX", "10"))
        logger.info("[EnricherReuse] reused=%d reasons=%s", reused, reused_reasons)
        logger.info(
            "[ReusedKOs] showing=%d/%d items=%s",
            min(max_show, len(reused_items)),
            len(reused_items),
            reused_items[:max_show],
        )

    if not work2:
        return {
            "patched": 0,
            "reused": reused,
            "failed": 0,
            "skipped": skipped,
            "failures": failures,
            "skipped_reasons": skipped_reasons,
        }

    # --- router worker ---
    def _route_and_fetch(item: Tuple[str, str, str, Optional[str], Optional[str]]) -> Tuple[str, str, Optional[str], str]:
        llid, url, kind, mimetype, ext = item

        t_item = _now_perf()
        logger.debug(
            "[EnrichItemStart] llid=%s kind=%s url_host=%s mimetype_in=%r ext_in=%r",
            llid, kind, _safe_host(url), mimetype, ext
        )

        def _slow_warn(tag: str, dt: float, thr_env: str, default_thr: float) -> None:
            thr = float(os.getenv(thr_env, str(default_thr)))
            if dt > thr:
                logger.warning("[SlowEnrichCall] tag=%s llid=%s dt=%.2fs url_host=%s", tag, llid, dt, _safe_host(url))

        # stagger to avoid thundering herd
        time.sleep(random.uniform(0.15, 0.5))

        # For HEAD sniff we use the public session with shorter timeout
        head_sess = None
        if enable_head_sniff:
            head_sess = get_public_session(timeout=int(os.getenv("HEAD_SNIFF_HTTP_TIMEOUT", "20")))

        decision = decide_route(
            kind=kind,
            url=url,
            is_url_only=is_url_only,
            ko_is_hosted=ko_is_hosted,
            mimetype=mimetype or "",
            ext=ext or "",
            enable_pdf=enable_pdf,
            enable_images=enable_images,
            enable_head_sniff=enable_head_sniff,
            head_session=head_sess,
        )

        logger.debug(
            "[RouteDecision] llid=%s kind=%s tag=%s action=%s reason=%s",
            llid, kind, decision.tag, decision.action, decision.reason
        )

        # --- SKIP routes ---
        if decision.action == "skip":
            # We mark source as :skipped so the caller counts it as skipped, not failed.
            return llid, url, None, f"{decision.tag}:skipped"

        # --- EXTRACT routes ---
        if decision.action == "extract":
            if not enable_url_extract:
                logger.info("[EnrichSkip] route=%s llid=%s disabled_by_flag", decision.tag, llid)
                return llid, url, None, f"{decision.tag}:disabled"

            t_wait = _now_perf()
            extractor_sema.acquire()
            wait_dt = _now_perf() - t_wait
            if wait_dt > float(os.getenv("ENRICH_SEMA_WAIT_WARN_SEC", "1.0")):
                logger.warning("[EnrichWait] route=%s llid=%s wait=%.2fs", decision.tag, llid, wait_dt)

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
                call_dt = _now_perf() - t_call
                _slow_warn(decision.tag, call_dt, "SLOW_EXTRACTOR_SEC", 8.0)
                logger.info(
                    "[EnrichCallDone] route=%s llid=%s ok=%s chars=%s dt=%.2fs",
                    decision.tag, llid, bool(text), (len(text) if isinstance(text, str) else None), call_dt
                )
                return llid, url, text, decision.tag
            except Exception as e:
                call_dt = _now_perf() - t_item
                logger.error("[EnrichCallFail] route=%s llid=%s dt=%.2fs err=%s", decision.tag, llid, call_dt, e)
                return llid, url, None, decision.tag
            finally:
                extractor_sema.release()

        # --- TRANSCRIBE routes ---
        if decision.action == "transcribe":
            if not enable_transcribe or not enable_deapi_transcribe:
                logger.info("[EnrichSkip] route=%s llid=%s disabled_by_flag", decision.tag, llid)
                return llid, url, None, f"{decision.tag}:disabled"

            t_wait = _now_perf()
            transcribe_sema.acquire()
            wait_dt = _now_perf() - t_wait
            if wait_dt > float(os.getenv("ENRICH_SEMA_WAIT_WARN_SEC", "1.0")):
                logger.warning("[EnrichWait] route=%s llid=%s wait=%.2fs", decision.tag, llid, wait_dt)

            try:
                t_call = _now_perf()
                sess = get_public_session(timeout=int(os.getenv("ENRICH_DL_HTTP_TIMEOUT", "60")))
                local_path = _download_media_to_temp(url, llid, sess)
                if not local_path:
                    return llid, url, None, f"{decision.tag}:download_failed"

                text = transcribe_with_deapi(video_path=local_path)

                call_dt = _now_perf() - t_call
                _slow_warn(decision.tag, call_dt, "SLOW_TRANSCRIBE_SEC", 20.0)
                logger.info(
                    "[EnrichCallDone] route=%s llid=%s ok=%s chars=%s dt=%.2fs",
                    decision.tag, llid, bool(text), (len(text) if isinstance(text, str) else None), call_dt
                )
                return llid, url, text, decision.tag
            except Exception as e:
                call_dt = _now_perf() - t_item
                logger.error("[EnrichCallFail] route=%s llid=%s dt=%.2fs err=%s", decision.tag, llid, call_dt, e)
                return llid, url, None, decision.tag
            finally:
                transcribe_sema.release()

        # Defensive fallback: should never happen
        logger.warning("[EnrichNoRoute] llid=%s kind=%s url_host=%s mt=%r ext=%r", llid, kind, _safe_host(url), mimetype, ext)
        return llid, url, None, "no_route"

    # Worker pool size: can be the max of both because semaphores throttle real concurrency.
    max_workers = int(os.getenv("ENRICH_MAX_WORKERS", str(max(extractor_workers, transcribe_workers, 4))))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_route_and_fetch, item) for item in work2]
        for fut in as_completed(futures):
            try:
                llid, url, text, source = fut.result()
            except Exception as e:
                failed += 1
                _inc(failure_reasons, "worker_exception")
                failures.append({"reason": "worker_exception", "error": str(e)})
                continue

            doc = idx.get(llid)
            if not doc:
                skipped += 1
                _inc(skipped_reasons, "skipped:missing_doc_postfetch")
                continue

            if not text:
                if isinstance(source, str) and source.endswith(":disabled"):
                    skipped += 1
                    _inc(skipped_reasons, "skipped:disabled_by_flag")
                    continue

                if isinstance(source, str) and source.endswith(":skipped"):
                    skipped += 1
                    _inc(skipped_reasons, source)
                    continue

                failed += 1
                _inc(failure_reasons, "no_text")
                _inc(source_failures, f"source:{source}")
                failures.append({"llid": llid, "url": url, "source": source, "reason": "no_text"})
                continue

            if isinstance(max_chars, int) and max_chars > 0:
                text = text[:max_chars]

            doc["ko_content_flat"] = text
            doc["ko_content_source"] = source
            doc["ko_content_url"] = url

            flattened = flatten_ko_content(doc, mode=ko_content_mode)
            doc.clear()
            doc.update(flattened)

            patched += 1

    elapsed = _now_perf() - t0

    max_fail_show = int(os.getenv("FAIL_LOG_MAX", "10"))
    if failed:
        logger.info("[EnricherFailures] failed=%d reasons=%s by_source=%s", failed, failure_reasons, source_failures)
        logger.info(
            "[FailedKOs] showing=%d/%d items=%s",
            min(max_fail_show, len(failures)),
            len(failures),
            failures[:max_fail_show],
        )

    logger.warning(
        "[EnricherDone] patched=%d reused=%d failed=%d skipped=%d elapsed=%.2fs",
        patched, reused, failed, skipped, elapsed
    )

    return {
        "patched": patched,
        "reused": reused,
        "failed": failed,
        "skipped": skipped,
        "elapsed_sec": round(elapsed, 2),
        "failures": failures,
        "failure_reasons": failure_reasons,
        "skipped_reasons": skipped_reasons,
    }

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

        stats = enrich_external_content(
            docs,
            url_tasks,
            media_tasks,
            previous_index=None,
            extractor_workers=extractor_workers,
            transcribe_workers=transcribe_workers,
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

# ---------------- Core enrichment entrypoint ----------------


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


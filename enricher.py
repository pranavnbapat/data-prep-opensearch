# enricher.py

from __future__ import annotations

import logging
import os
import random
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from deapi_transcribe import transcribe_video, DeapiError
from utils import (
    fetch_url_text_with_extractor,
    flatten_ko_content
)


logger = logging.getLogger(__name__)

_tls = threading.local()

def get_public_session(timeout: int = 30) -> requests.Session:
    """
    Thread-local session for calling external services (PageSense, transcriber, YouTube captions).
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


# ---------------- Routing helpers ----------------

def _is_media_mimetype(m: Any) -> bool:
    if not isinstance(m, str):
        return False
    m = m.lower().strip()
    return m == "video/mp4" or m.startswith("audio/")

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

def _is_ugent_url(u: str) -> bool:
    if not isinstance(u, str):
        return False
    return "ugent" in u.lower()

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


# ---------------- Stubs / provisions ----------------

# def fetch_youtube_text_stub(video_url: str, session: requests.Session) -> Optional[str]:
#     """
#     Provision for YouTube handling.
#     Uses existing parse_youtube_value + fetch_transcript_segments if available.
#     Returns text or None.
#     """
#     vid, _ = parse_youtube_value(video_url)
#     if not vid:
#         return None
#
#     pref = [s.strip() for s in os.getenv("YT_CAP_PREF_LANGS", "en").split(",") if s.strip()]
#     translate_to = os.getenv("YT_CAP_TRANSLATE_TO") or None
#
#     segs, _lang_code = fetch_transcript_segments(
#         vid,
#         preferred_langs=pref,
#         translate_to=translate_to,
#         http_client=session,
#     )
#     text = "\n".join(s.get("text", "") for s in segs if isinstance(s, dict) and s.get("text"))
#     text = text.strip()
#     return text or None

# def fetch_other_video_text_stub(video_url: str, session: requests.Session) -> Optional[str]:
#     _ = session
#     _ = video_url
#     return None

def transcribe_with_deapi(video_url: str) -> Optional[str]:
    """
    Uses deAPI to transcribe a video URL and returns plain text or None.
    """
    api_key = (os.getenv("DEAPI_API_KEY") or "").strip()
    if not api_key:
        logger.error("[deAPI] Missing DEAPI_API_KEY env var")
        return None

    model = (os.getenv("DEAPI_TRANSCRIBE_MODEL") or "WhisperLargeV3").strip() or "WhisperLargeV3"
    include_ts = _env_bool("DEAPI_INCLUDE_TIMESTAMPS", False)

    # deAPI polling knobs (these are intentionally long for transcription)
    timeout_s = int(os.getenv("DEAPI_HTTP_TIMEOUT", "60"))
    poll_interval_s = float(os.getenv("DEAPI_POLL_INTERVAL_SEC", "2.0"))
    max_wait_s = int(os.getenv("DEAPI_MAX_WAIT_SEC", str(12 * 60)))  # 12 min default

    try:
        result = transcribe_video(
            api_key=api_key,
            video=video_url,                   # URL in -> /vid2txt
            model=model,
            include_timestamps=include_ts,
            return_result_in_response=True,    # best chance to get text inline
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            max_wait_s=max_wait_s,
        )
        text = (result.transcript_text or "").strip()
        if not text:
            logger.warning("[deAPI] Empty transcript. status=%s request_id=%s url_host=%s",
                           result.status, result.request_id, _safe_host(video_url))
            return None
        return text

    except DeapiError as e:
        logger.error("[deAPI] Transcription failed url_host=%s err=%s", _safe_host(video_url), e)
        return None
    except Exception as e:
        logger.exception("[deAPI] Unexpected error url_host=%s err=%s", _safe_host(video_url), e)
        return None


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
    Patches docs in-place using a single routing logic.

    Inputs:
      - docs: output from downloader (already flattened once)
      - url_tasks: from downloader (logical_layer_id, first_url, ...)
      - media_tasks: from downloader (logical_layer_id, media_url, mimetype, ...)
      - previous_index: snapshot index for reuse (llid -> previous doc)

    Outputs: stats dict + failure lists.
    """
    if not docs:
        return {"patched": 0, "reused": 0, "failed": 0, "skipped": 0}

    ko_content_mode = (ko_content_mode or os.getenv("KO_CONTENT_MODE", "flat_only")).strip() or "flat_only"
    prev_index = previous_index or {}

    enable_url_extract = _env_bool("ENRICH_ENABLE_URL_EXTRACT", True)
    enable_transcribe = _env_bool("ENRICH_ENABLE_TRANSCRIBE", True)
    enable_deapi_transcribe = _env_bool("ENRICH_ENABLE_DEAPI_TRANSCRIBE", True)
    # enable_youtube_caps = _env_bool("ENRICH_ENABLE_YOUTUBE_CAPTIONS", False)
    # enable_other_video = _env_bool("ENRICH_ENABLE_OTHER_VIDEO", False)

    logger.info(
        "[EnricherFlags] url_extract=%s transcribe=%s deapi_transcribe=%s",
        enable_url_extract, enable_transcribe, enable_deapi_transcribe
    )

    idx = _index_docs_by_llid(docs)

    t0 = _now_perf()
    logger.warning(
        "[EnricherStart] docs=%d url_tasks=%d media_tasks=%d ko_content_mode=%s extractor_workers=%d transcribe_workers=%d",
        len(docs), len(url_tasks or []), len(media_tasks or []), ko_content_mode, extractor_workers, transcribe_workers
    )

    # --- build a unified worklist ---
    # Each item: (llid, url, kind, mimetype)
    # kind: "url" or "media"
    work: List[Tuple[str, str, str, Optional[str]]] = []
    seen = set()

    # url_tasks use first_url
    for row in url_tasks or []:
        llid = row.get("logical_layer_id")
        url = row.get("first_url")
        if not llid or not url:
            continue
        if llid in seen:
            continue
        seen.add(llid)
        work.append((str(llid), str(url), "url", None))

    # media_tasks use media_url + mimetype
    for row in media_tasks or []:
        llid = row.get("logical_layer_id")
        url = row.get("media_url")
        mimetype = row.get("mimetype")
        if not llid or not url:
            continue
        if llid in seen:
            continue
        seen.add(llid)
        work.append((str(llid), str(url), "media", mimetype))

    if not work:
        return {"patched": 0, "reused": 0, "failed": 0, "skipped": 0}

    # quick worklist summary
    kind_counts: Dict[str, int] = {}
    host_counts: Dict[str, int] = {}
    for llid, url, kind, _mt in work:
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

    # --- reuse from previous snapshot when possible ---
    # If URL unchanged AND prev_flat exists, reuse without any calls.
    # NOTE: we only reuse if current doc's content is empty/placeholder.
    work2: List[Tuple[str, str, str, Optional[str]]] = []
    for llid, url, kind, mimetype in work:
        doc = idx.get(llid)
        if not doc:
            skipped += 1
            _inc(failure_reasons, "skipped:missing_doc")
            continue

        cur_flat = doc.get("ko_content_flat")
        if not _is_placeholder_content(cur_flat):
            # already has content; don't overwrite
            skipped += 1
            _inc(failure_reasons, "skipped:already_has_content")
            continue

        prev_doc = prev_index.get(llid)
        if prev_doc:
            prev_flat = prev_doc.get("ko_content_flat")
            prev_url = prev_doc.get("ko_content_url") or prev_doc.get("first_url")
            if isinstance(prev_flat, str) and prev_flat.strip() and (not prev_url or prev_url == url):
                doc["ko_content_flat"] = prev_flat
                if prev_doc.get("ko_content_source"):
                    doc["ko_content_source"] = prev_doc["ko_content_source"]
                doc["ko_content_url"] = url
                reused += 1
                reused_items.append({"llid": llid, "url": url, "source": doc.get("ko_content_source")})

                _inc(reused_reasons, "reuse:prev_flat_same_url")

                continue

        work2.append((llid, url, kind, mimetype))

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
        }

    # --- router ---
    # Returns (llid, url, text, source_tag) or (llid, url, None, source_tag) on failure
    def _route_and_fetch(item: Tuple[str, str, str, Optional[str]]) -> Tuple[str, str, Optional[str], str]:
        llid, url, kind, mimetype = item

        t_item = _now_perf()
        logger.debug(
            "[EnrichItemStart] llid=%s kind=%s url_host=%s mimetype_in=%r",
            llid, kind, _safe_host(url), mimetype
        )

        def _slow_warn(tag: str, dt: float, thr_env: str, default_thr: float) -> None:
            thr = float(os.getenv(thr_env, str(default_thr)))
            if dt > thr:
                logger.warning("[SlowEnrichCall] tag=%s llid=%s dt=%.2fs url_host=%s", tag, llid, dt, _safe_host(url))

        # stagger to avoid thundering herd
        time.sleep(random.uniform(0.15, 0.5))

        # Determine mimetype from doc if missing
        doc = idx.get(llid, {})
        mt = mimetype or doc.get("ko_object_mimetype")

        # Core simplified routing:
        if not _is_media_mimetype(mt):
            route = "external_url_extractor"
            if not enable_url_extract:
                logger.info("[EnrichSkip] route=%s llid=%s disabled_by_flag", route, llid)
                return llid, url, None, f"{route}:disabled"

            t_wait = _now_perf()
            extractor_sema.acquire()
            wait_dt = _now_perf() - t_wait
            if wait_dt > float(os.getenv("ENRICH_SEMA_WAIT_WARN_SEC", "1.0")):
                logger.warning("[EnrichWait] route=%s llid=%s wait=%.2fs", route, llid, wait_dt)

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
                _slow_warn(route, call_dt, "SLOW_EXTRACTOR_SEC", 8.0)
                logger.info(
                    "[EnrichCallDone] route=%s llid=%s ok=%s chars=%s dt=%.2fs",
                    route, llid, bool(text), (len(text) if isinstance(text, str) else None), call_dt
                )
                return llid, url, text, route
            except Exception as e:
                call_dt = _now_perf() - t_item
                logger.error("[EnrichCallFail] route=%s llid=%s dt=%.2fs err=%s", route, llid, call_dt, e)
                return llid, url, None, route
            finally:
                extractor_sema.release()

        # Media mimetype branch:
        if kind == "media":
            route = "deapi_vid2txt"
            if not enable_transcribe or not enable_deapi_transcribe:
                logger.info("[EnrichSkip] route=%s llid=%s disabled_by_flag", route, llid)
                return llid, url, None, f"{route}:disabled"

            t_wait = _now_perf()
            transcribe_sema.acquire()
            wait_dt = _now_perf() - t_wait
            if wait_dt > float(os.getenv("ENRICH_SEMA_WAIT_WARN_SEC", "1.0")):
                logger.warning("[EnrichWait] route=%s llid=%s wait=%.2fs", route, llid, wait_dt)

            try:
                t_call = _now_perf()
                text = transcribe_with_deapi(url)
                call_dt = _now_perf() - t_call
                _slow_warn(route, call_dt, "SLOW_TRANSCRIBE_SEC", 20.0)
                logger.info(
                    "[EnrichCallDone] route=%s llid=%s ok=%s chars=%s dt=%.2fs",
                    route, llid, bool(text), (len(text) if isinstance(text, str) else None), call_dt
                )
                return llid, url, text, route
            except Exception as e:
                call_dt = _now_perf() - t_item
                logger.error("[EnrichCallFail] route=%s llid=%s dt=%.2fs err=%s", route, llid, call_dt, e)
                return llid, url, None, route
            finally:
                transcribe_sema.release()

        logger.warning("[EnrichNoRoute] llid=%s kind=%s url_host=%s mt=%r", llid, kind, _safe_host(url), mt)
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
                _inc(failure_reasons, "skipped:missing_doc_postfetch")
                continue

            if not text:
                if isinstance(source, str) and source.endswith(":disabled"):
                    skipped += 1
                    _inc(failure_reasons, "skipped:disabled_by_flag")
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
    }


if __name__ == "__main__":
    import json
    import logging
    import os

    from downloader import download_and_prepare

    # Make INFO logs show even if utils configured logging earlier
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()

    # Downloader knobs
    ps = int(os.getenv("DL_PAGE_SIZE", "100"))
    ps = max(1, min(ps, 100))
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))

    # Enricher knobs
    exw = int(os.getenv("EXTRACTOR_MAX_WORKERS", "4"))
    trw = int(os.getenv("TRANSCRIBE_MAX_WORKERS", "3"))
    max_chars = int(os.getenv("ENRICH_MAX_CHARS", "0")) or None

    # Step 1: download
    dl = download_and_prepare(env_mode=env_mode, page_size=ps, sort_criteria=sc, max_workers=mw)

    # Step 2: enrich
    stats = enrich_external_content(
        dl.docs,
        dl.url_tasks,
        dl.media_tasks,
        previous_index=None,
        extractor_workers=exw,
        transcribe_workers=trw,
        max_chars=max_chars,
    )

    logging.warning("[Enricher] stats=%s", stats)
    print(json.dumps(stats, indent=2))

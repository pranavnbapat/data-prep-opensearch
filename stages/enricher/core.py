from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

from stages.enricher.transcription import transcribe_with_deapi
from stages.enricher.vision import describe_visual_url, extract_visual_target_url, vision_enabled
from stages.enricher.utils import (pagesense_one, classify_url_for_enrichment,
                                   target_url_for_enrichment, has_enrich_via, should_skip,
                                   is_placeholder_content, is_deapi_platform_url,
                                   env_bool, compute_enrich_inputs_fp, probe_target_url,
                                   transcribe_with_custom_api_detailed)


logger = logging.getLogger(__name__)


def index_docs_by_llid(docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        llid = d.get("_orig_id") or d.get("_id")
        if isinstance(llid, str) and llid:
            idx[llid] = d
    return idx


def inc(counter: Dict[str, int], key: str, n: int = 1) -> None:
    counter[key] = counter.get(key, 0) + n


def carry_forward_enrichment(doc: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> bool:
    if not prev:
        return False

    prev_text = prev.get("ko_content_flat")
    prev_enriched = int(prev.get("enriched") or 0) == 1
    if not prev_enriched or not isinstance(prev_text, str) or is_placeholder_content(prev_text):
        return False

    cur_text = doc.get("ko_content_flat")
    cur_enriched = int(doc.get("enriched") or 0) == 1
    if cur_enriched and isinstance(cur_text, str) and not is_placeholder_content(cur_text):
        return False

    if (
        doc.get("ko_content_flat") == prev_text
        and int(doc.get("enriched") or 0) == 1
        and doc.get("ko_content_source") == prev.get("ko_content_source")
        and doc.get("ko_content_url") == prev.get("ko_content_url")
    ):
        return False

    doc["ko_content_flat"] = prev_text
    doc["enriched"] = 1
    if isinstance(prev.get("ko_content_source"), str):
        doc["ko_content_source"] = prev["ko_content_source"]
    if isinstance(prev.get("ko_content_url"), str):
        doc["ko_content_url"] = prev["ko_content_url"]

    if (
        doc.get("ko_content_flat") == prev.get("ko_content_flat")
        and int(doc.get("enriched") or 0) == int(prev.get("enriched") or 0)
        and doc.get("ko_content_source") == prev.get("ko_content_source")
        and doc.get("ko_content_url") == prev.get("ko_content_url")
    ):
        return False

    logger.info("[EnrichCarryForward] id=%s source=%s", doc.get("_orig_id") or doc.get("_id"), prev.get("ko_content_source"))
    return True


def enrich_docs_via_routes(
    docs: List[Dict[str, Any]],
    *,
    prev_enriched_index: Dict[str, Dict[str, Any]],
    extractor_workers: int,
    transcribe_workers: int,
    max_chars: Optional[int],
) -> Dict[str, Any]:
    def retry_n(*, fn, attempts: int, base_sleep_s: float, jitter_s: float, label: str):
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

    def pagesense_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        vision_probe_timeout = int(os.getenv("ENRICH_VISION_PROBE_TIMEOUT", "12"))
        prefer_vision_reasons = {
            "content_type_image",
            "content_type_pdf",
            "get_content_type_image",
            "get_content_type_pdf",
            "embedded_pdf",
        }

        if env_bool("ENRICH_ENABLE_VISION_FALLBACK", True) and vision_enabled():
            visual_target, visual_reason = extract_visual_target_url(url, timeout=vision_probe_timeout)
            if visual_target and visual_reason in prefer_vision_reasons:
                logger.info(
                    "[VisionPreferred] id=%s page=%s target=%s reason=%s",
                    llid,
                    url,
                    visual_target,
                    visual_reason,
                )
                vision_text = describe_visual_url(visual_target, source_page_url=url)
                if vision_text and not is_placeholder_content(vision_text):
                    return vision_text, "vision_fallback"
                logger.warning(
                    "[VisionPreferredFail] id=%s page=%s target=%s reason=%s",
                    llid,
                    url,
                    visual_target,
                    visual_reason,
                )
                return None, f"vision_preferred:{visual_reason}"

        def call():
            res = pagesense_one(llid=llid, url=url, http_timeout=0)
            if isinstance(res, tuple) and len(res) >= 4:
                _, _, text, tag = res[0], res[1], res[2], res[3]
            else:
                text, tag = None, "pagesense:failed"
            if text and not is_placeholder_content(text):
                return text, tag
            return None

        out = retry_n(
            fn=call,
            attempts=int(os.getenv("ENRICH_PAGESENSE_TRIES", "3")),
            base_sleep_s=float(os.getenv("ENRICH_PAGESENSE_BACKOFF", "1.6")),
            jitter_s=float(os.getenv("ENRICH_PAGESENSE_JITTER", "0.8")),
            label=f"pagesense id={llid} host={url}",
        )
        if out is None:
            if env_bool("ENRICH_ENABLE_VISION_FALLBACK", True) and vision_enabled():
                visual_target, visual_reason = extract_visual_target_url(
                    url,
                    timeout=vision_probe_timeout,
                )
                if visual_target:
                    logger.info("[VisionFallbackTry] id=%s page=%s target=%s reason=%s",
                                llid, url, visual_target, visual_reason)
                    vision_text = describe_visual_url(visual_target, source_page_url=url)
                    if vision_text and not is_placeholder_content(vision_text):
                        return vision_text, "vision_fallback"
            return None, "pagesense:empty"
        return out[0], "pagesense"

    def custom_transcribe_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        attempts = int(os.getenv("ENRICH_TRANSCRIBE_TRIES", "3"))
        base_sleep_s = float(os.getenv("ENRICH_TRANSCRIBE_BACKOFF", "1.7"))
        jitter_s = float(os.getenv("ENRICH_TRANSCRIBE_JITTER", "0.8"))
        terminal_tags = {
            "custom_transcribe:proxy_timeout",
            "custom_transcribe:http_404",
            "custom_transcribe:http_403",
            "custom_transcribe:too_long",
        }

        for i in range(attempts):
            attempt_no = i + 1
            t0 = time.perf_counter()
            logger.info("[EnrichTry] custom_transcribe id=%s host=%s attempt=%d/%d", llid, url, attempt_no, attempts)
            text, tag = transcribe_with_custom_api_detailed(media_url=url)
            dt = time.perf_counter() - t0
            if text:
                logger.info("[EnrichTryOK] custom_transcribe id=%s host=%s attempt=%d/%d dt=%.2fs", llid, url, attempt_no, attempts, dt)
                return text, "custom_transcribe"

            logger.warning("[EnrichTryFail] custom_transcribe id=%s host=%s attempt=%d/%d dt=%.2fs tag=%s",
                           llid, url, attempt_no, attempts, dt, tag)
            if tag in terminal_tags:
                return None, tag
            if i < attempts - 1:
                delay = (base_sleep_s ** i) + random.uniform(0.0, jitter_s)
                logger.info("[EnrichBackoff] custom_transcribe id=%s host=%s sleep=%.2fs", llid, url, delay)
                time.sleep(delay)

        return None, "custom_transcribe:failed"

    def api_transcribe_try(llid: str, url: str) -> Tuple[Optional[str], str]:
        if not is_deapi_platform_url(url):
            return None, "api_transcribe:unsupported_url"

        def call():
            text = transcribe_with_deapi(video_url=url)
            return text.strip() if isinstance(text, str) and text.strip() else None

        text = retry_n(
            fn=call,
            attempts=int(os.getenv("ENRICH_TRANSCRIBE_TRIES", "3")),
            base_sleep_s=float(os.getenv("ENRICH_TRANSCRIBE_BACKOFF", "1.7")),
            jitter_s=float(os.getenv("ENRICH_TRANSCRIBE_JITTER", "0.8")),
            label=f"api_transcribe id={llid} host={url}",
        )
        if not text:
            return None, "api_transcribe:failed"
        return text, "api_transcribe"

    if not docs:
        return {"patched": 0, "skipped": 0, "failed": 0}

    enable_pagesense = env_bool("ENRICH_ENABLE_URL_EXTRACT", True)
    enable_api_transcribe = env_bool("ENRICH_ENABLE_API_TRANSCRIBE", True)
    enable_custom_transcribe = env_bool("ENRICH_ENABLE_CUSTOM_TRANSCRIBE", True)

    idx = index_docs_by_llid(docs)
    candidates: List[Tuple[str, str, str]] = []
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

        doc["_enrich_inputs_fp"] = compute_enrich_inputs_fp(doc)
        prev = prev_enriched_index.get(llid)
        if prev is not None and not isinstance(prev.get("_enrich_inputs_fp"), str):
            prev["_enrich_inputs_fp"] = compute_enrich_inputs_fp(prev)
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
            logger.info("[EnrichSkip] id=%s route=%s reason=missing_target", llid, route)
            skipped_missing_target += 1
            continue

        ok, reason = classify_url_for_enrichment(target)
        if not ok:
            logger.info("[EnrichSkip] id=%s route=%s reason=%s target=%r", llid, route, reason, target)
            skipped_bad_target += 1
            continue

        should_probe = (
            (route == "custom_transcribe" and env_bool("ENRICH_PROBE_MEDIA_URLS", True))
            or (route == "pagesense" and env_bool("ENRICH_PROBE_PAGE_URLS", False))
        )
        if should_probe:
            probe_ok, probe_reason = probe_target_url(target, timeout=int(os.getenv("ENRICH_PROBE_TIMEOUT", "8")))
            if not probe_ok:
                logger.info("[EnrichSkip] id=%s route=%s reason=probe_%s target=%r", llid, route, probe_reason, target)
                skipped_bad_target += 1
                continue

        logger.info("[EnrichQueue] ID=%s route=%s target_host=%s", llid, route, target)
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
    per_record_delay = float(os.getenv("ENRICH_PER_RECORD_DELAY_SEC", "0.25"))
    total = len(candidates)

    def apply_success(doc: Dict[str, Any], *, text: str, source: str, url: str) -> None:
        if isinstance(max_chars, int) and max_chars > 0:
            text = text[:max_chars]
        doc["ko_content_flat"] = text
        doc["ko_content_source"] = source
        doc["ko_content_url"] = url
        doc["enriched"] = 1
        doc["_enrich_inputs_fp"] = compute_enrich_inputs_fp(doc)

    for n, (llid, route, target) in enumerate(candidates, start=1):
        doc = idx.get(llid)
        if not doc:
            continue
        t0 = time.perf_counter()
        logger.info("[EnrichRun] %d/%d id=%s route=%s target=%s", n, total, llid, route, target)
        if per_record_delay > 0:
            time.sleep(per_record_delay + random.uniform(0.0, 0.15))

        if route == "pagesense":
            text, tag = pagesense_try(llid, target)
            if not text:
                failed_pagesense += 1
                inc(failure_reasons, tag)
                logger.warning("[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs", n, total, llid, route, tag, time.perf_counter() - t0)
                continue
            source = "vision_fallback" if tag == "vision_fallback" else "pagesense"
            apply_success(doc, text=text, source=source, url=target)
            patched_pagesense += 1
            logger.info("[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs", n, total, llid, route, len(text), time.perf_counter() - t0)
            continue

        if route == "api_transcribe":
            text, tag = api_transcribe_try(llid, target)
            if not text:
                failed_transcribe += 1
                inc(failure_reasons, tag)
                logger.warning("[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs", n, total, llid, route, tag, time.perf_counter() - t0)
                continue
            apply_success(doc, text=text, source="api_transcribe", url=target)
            patched_transcribe += 1
            logger.info("[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs", n, total, llid, route, len(text), time.perf_counter() - t0)
            continue

        if route == "custom_transcribe":
            text, tag = custom_transcribe_try(llid, target)
            if not text:
                failed_transcribe += 1
                inc(failure_reasons, tag)
                logger.warning("[EnrichFail] %d/%d id=%s route=%s reason=%s dt=%.2fs", n, total, llid, route, tag, time.perf_counter() - t0)
                continue
            apply_success(doc, text=text, source="custom_transcribe", url=target)
            patched_transcribe += 1
            logger.info("[EnrichOK] %d/%d id=%s route=%s chars=%d dt=%.2fs", n, total, llid, route, len(text), time.perf_counter() - t0)

    patched_total = patched_pagesense + patched_transcribe
    failed_total = failed_pagesense + failed_transcribe
    logger.warning("[EnricherDone] candidates=%d patched=%d failed=%d skipped_prev=%d",
                   len(candidates), patched_total, failed_total, skipped_prev_done)
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

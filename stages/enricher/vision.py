from __future__ import annotations

import logging
import os
import re
import base64
import subprocess
import tempfile
import time
import threading
from html import unescape
from typing import Any, Optional, Tuple
from urllib.parse import urljoin, unquote_to_bytes

import requests

from stages.enricher.utils import get_public_session, safe_host


logger = logging.getLogger(__name__)
_VISION_CALL_LOCK = threading.Lock()
_VISION_LAST_CALL_AT = 0.0

_META_IMAGE_PATTERNS = [
    re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
]
_IMG_SRC_PATTERN = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)
_PDF_URL_PATTERNS = [
    re.compile(r'["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', re.I),
    re.compile(r'(?:file|src|href)\s*[:=]\s*["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', re.I),
]

VISION_PROMPT = (
    "Extract useful plain text from this visual agricultural resource. "
    "If the image contains readable text, transcribe the important text. "
    "Then add a concise factual description of the visual content for search and retrieval. "
    "Return plain text only."
)

VISION_REDUCE_PROMPT = (
    "You are combining summaries from a multi-page agricultural document. "
    "Produce one factual, coherent plain-text summary that preserves important entities, numbers, "
    "procedures, recommendations, and context. Avoid repetition and unsupported claims. "
    "Return plain text only."
)


def vision_enabled() -> bool:
    return bool(
        (os.getenv("EUF_VISION_URL") or "").strip()
        and (os.getenv("EUF_VISION_MODEL") or "").strip()
        and (os.getenv("EUF_VISION_API_KEY") or "").strip()
    )


def _vision_chat_url() -> str:
    raw = (os.getenv("EUF_VISION_URL") or "").strip()
    if not raw:
        return ""
    normalized = raw.rstrip("/")
    if normalized.endswith("/v1/chat/completions"):
        return normalized
    return f"{normalized}/v1/chat/completions"


def probe_content_type(url: str, *, timeout: int = 10) -> Tuple[Optional[str], Optional[str]]:
    if url.startswith("data:"):
        m = re.match(r"^data:([^;,]+)", url, flags=re.I)
        return ((m.group(1).strip().lower() if m else None), url)
    sess = get_public_session(timeout=timeout)
    try:
        r = sess.head(url, allow_redirects=True)
        if r.status_code == 405 or r.status_code >= 400:
            r = sess.get(url, allow_redirects=True, stream=True)
        content_type = (r.headers.get("content-type") or "").split(";", 1)[0].strip().lower() or None
        final_url = r.url
        try:
            r.close()
        except Exception:
            pass
        return content_type, final_url
    except Exception:
        return None, None


def _load_data_url_bytes(data_url: str) -> Tuple[Optional[bytes], Optional[str]]:
    m = re.match(r"^data:([^;,]+)(;base64)?,(.*)$", data_url, flags=re.I | re.S)
    if not m:
        return None, None
    mime = (m.group(1) or "").strip().lower() or None
    is_b64 = bool(m.group(2))
    payload = m.group(3) or ""
    try:
        if is_b64:
            return base64.b64decode(payload), mime
        return unquote_to_bytes(payload), mime
    except Exception:
        return None, mime


def _png_data_url_from_bytes(raw: bytes) -> str:
    return f"data:image/png;base64,{base64.b64encode(raw).decode('ascii')}"


def _render_svg_bytes_to_png_data_url(svg_bytes: bytes, *, timeout: int) -> Optional[str]:
    try:
        with tempfile.TemporaryDirectory(prefix="vision_svg_") as tmpdir:
            svg_path = os.path.join(tmpdir, "input.svg")
            png_path = os.path.join(tmpdir, "output.png")
            with open(svg_path, "wb") as f:
                f.write(svg_bytes)
            cmd = ["/usr/bin/convert", svg_path, png_path]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 30))
            if proc.returncode != 0:
                logger.error(
                    "[VisionSvgRenderFail] rc=%s stderr=%s",
                    proc.returncode,
                    (proc.stderr or "")[:500],
                )
                return None
            with open(png_path, "rb") as f:
                return _png_data_url_from_bytes(f.read())
    except Exception as e:
        logger.error("[VisionSvgRenderFail] err=%r", e)
        return None


def _render_pdf_first_page_data_url(pdf_url: str, *, timeout: int) -> Optional[str]:
    pdf_bytes = _download_pdf_bytes(pdf_url, timeout=timeout)
    if pdf_bytes is None:
        return None
    return _render_pdf_page_data_url(pdf_bytes, page_num=1, timeout=timeout)


def _download_pdf_bytes(pdf_url: str, *, timeout: int) -> Optional[bytes]:
    sess = get_public_session(timeout=timeout)
    retries = max(1, int(os.getenv("EUF_VISION_PDF_FETCH_RETRIES", "3")))
    base_sleep = float(os.getenv("EUF_VISION_PDF_FETCH_BACKOFF_SEC", "1.0"))
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(pdf_url, allow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            logger.error(
                "[VisionPdfFetchFail] target=%s attempt=%s/%s err=%r",
                pdf_url,
                attempt,
                retries,
                e,
            )
            if attempt < retries:
                time.sleep(base_sleep * attempt)
    return None


def _render_pdf_page_data_url(pdf_bytes: bytes, *, page_num: int, timeout: int) -> Optional[str]:
    try:
        with tempfile.TemporaryDirectory(prefix="vision_pdf_") as tmpdir:
            pdf_path = os.path.join(tmpdir, "input.pdf")
            out_prefix = os.path.join(tmpdir, "page")
            with open(pdf_path, "wb") as f:
                f.write(pdf_bytes)

            cmd = [
                "/usr/bin/pdftoppm",
                "-png",
                "-f",
                str(page_num),
                "-l",
                str(page_num),
                "-singlefile",
                pdf_path,
                out_prefix,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 30))
            if proc.returncode != 0:
                logger.error(
                    "[VisionPdfRenderFail] page=%s rc=%s stderr=%s",
                    page_num,
                    proc.returncode,
                    (proc.stderr or "")[:500],
                )
                return None

            png_path = f"{out_prefix}.png"
            with open(png_path, "rb") as f:
                return _png_data_url_from_bytes(f.read())
    except Exception as e:
        logger.error("[VisionPdfRenderFail] page=%s err=%r", page_num, e)
        return None


def _pdf_page_count(pdf_bytes: bytes, *, timeout: int) -> Optional[int]:
    try:
        with tempfile.TemporaryDirectory(prefix="vision_pdfinfo_") as tmpdir:
            pdf_path = os.path.join(tmpdir, "input.pdf")
            with open(pdf_path, "wb") as f:
                f.write(pdf_bytes)
            cmd = ["/usr/bin/pdfinfo", pdf_path]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 30))
            if proc.returncode != 0:
                logger.error("[VisionPdfInfoFail] rc=%s stderr=%s", proc.returncode, (proc.stderr or "")[:500])
                return None
            m = re.search(r"^Pages:\s+(\d+)\s*$", proc.stdout or "", flags=re.M)
            if not m:
                return None
            return int(m.group(1))
    except Exception as e:
        logger.error("[VisionPdfInfoFail] err=%r", e)
        return None


# --- Long-document page selection -------------------------------------------
# Mirrors the AI Uploader strategy (django_euf_admin pod1_runner) so data-prep
# and the uploader bound long-PDF cost the same way:
#   <= 30 pages      -> render/describe every page (short docs are cheap)
#   31 .. max pages   -> map-reduce a representative SUBSET (head + stratified
#                       text-rich middle + tail). Subset size is ADAPTIVE:
#                       round(RATE * N) clamped to [MIN, MAX] (defaults 30% in
#                       [18, 30]) so it tracks document length.
#   > max pages       -> handled by the caller: returns pdf_too_many_pages and the
#                       enricher keeps the upstream ko_content_flat text.
def _pdf_full_process_max_pages() -> int:
    return max(1, int(os.getenv("EUF_VISION_PDF_FULL_PROCESS_MAX_PAGES", "30")))


def _pdf_sample_rate() -> float:
    return min(1.0, max(0.0, float(os.getenv("EUF_VISION_PDF_SAMPLE_RATE", "0.30"))))


def _pdf_sample_min_pages() -> int:
    return max(1, int(os.getenv("EUF_VISION_PDF_SAMPLE_MIN_PAGES", "18")))


def _pdf_sample_max_pages() -> int:
    return max(_pdf_sample_min_pages(), int(os.getenv("EUF_VISION_PDF_SAMPLE_MAX_PAGES", "30")))


def _pdf_sample_head_pages() -> int:
    return max(0, int(os.getenv("EUF_VISION_PDF_SAMPLE_HEAD_PAGES", "5")))


def _pdf_sample_tail_pages() -> int:
    return max(0, int(os.getenv("EUF_VISION_PDF_SAMPLE_TAIL_PAGES", "2")))


def _adaptive_sample_target(n: int) -> int:
    """Subset size for an n-page doc: round(RATE * n) clamped to [MIN, MAX] and n."""
    target = int(round(_pdf_sample_rate() * n))
    target = max(_pdf_sample_min_pages(), min(target, _pdf_sample_max_pages()))
    return min(target, n)


def _pdf_page_texts(pdf_bytes: bytes, *, timeout: int) -> Optional[list[str]]:
    """Extract per-page plain text in one pdftotext call (pages split on form-feed).

    Returns a list of length page_count, or None if extraction failed. Used only to
    rank pages by text richness for stratified sampling; rendering still happens per
    selected page.
    """
    try:
        with tempfile.TemporaryDirectory(prefix="vision_pdftext_") as tmpdir:
            pdf_path = os.path.join(tmpdir, "input.pdf")
            with open(pdf_path, "wb") as f:
                f.write(pdf_bytes)
            cmd = ["/usr/bin/pdftotext", "-enc", "UTF-8", pdf_path, "-"]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 30))
            if proc.returncode != 0:
                logger.warning("[VisionPdfTextFail] rc=%s stderr=%s", proc.returncode, (proc.stderr or "")[:300])
                return None
            # pdftotext separates pages with the form-feed (\x0c) character.
            return (proc.stdout or "").split("\f")
    except Exception as e:
        logger.warning("[VisionPdfTextFail] err=%r", e)
        return None


def _usable_pdf_text_layer(page_texts: Optional[list[str]], page_count: int) -> Optional[str]:
    """Return the PDF's embedded text layer as one string when it is substantial
    enough to use as the body verbatim (born-digital PDFs), else None.

    Text-first: when a PDF carries a real text layer we index it directly — lossless,
    cheap, and full-coverage even for long/over-limit documents — instead of rendering
    and OCR-ing pages. Returns None for scanned/sparse PDFs so the caller falls back to
    vision (or, when too long to OCR, to metadata-only).
    """
    if not page_texts:
        return None
    min_total = int(os.getenv("EUF_VISION_PDF_TEXT_LAYER_MIN_CHARS", "200"))
    min_page_chars = int(os.getenv("EUF_VISION_PDF_TEXT_LAYER_MIN_PAGE_CHARS", "50"))
    min_page_ratio = float(os.getenv("EUF_VISION_PDF_TEXT_LAYER_MIN_PAGE_RATIO", "0.5"))
    cleaned = [((t or "").strip()) for t in page_texts]
    total_chars = sum(len(t) for t in cleaned)
    if total_chars < min_total:
        return None
    pages_with_text = sum(1 for t in cleaned if len(t) >= min_page_chars)
    if pages_with_text / max(1, page_count) < min_page_ratio:
        return None
    full = "\n\n".join(t for t in cleaned if t)
    # Cap to the same limit storage applies to ko_content_flat, so the value the
    # improver fingerprints in-memory equals the value persisted — otherwise a text
    # layer larger than the cap would mismatch every run and re-summarise endlessly.
    cap = int(os.getenv("STORAGE_MAX_KO_CONTENT_FLAT_CHARS", os.getenv("PROCESSING_MAX_TEXT_CHARS", "500000")))
    full = full[:cap]
    return full or None


def _select_pdf_pages_for_processing(
    page_count: int, *, page_texts: Optional[list[str]] = None
) -> Tuple[list[int], dict]:
    """Pick the 1-based page numbers to render for a PDF.

    For documents above the full-process threshold, choose a representative subset
    instead of every page: always keep the first HEAD and last TAIL pages, then fill
    the remaining budget from the middle by stratified sampling — the most text-rich
    page in each evenly-spaced window (more metadata signal). When per-page text is
    unavailable, the window midpoint is used. Returns (page_nums, info).
    """
    n = max(0, int(page_count))
    if n <= _pdf_full_process_max_pages():
        return list(range(1, n + 1)), {"strategy": "full", "total_pages": n, "processed_pages": n}

    head = min(_pdf_sample_head_pages(), n)
    tail = min(_pdf_sample_tail_pages(), max(0, n - head))
    target = _adaptive_sample_target(n)
    target = min(max(target, head + tail), n)

    head_idx = list(range(0, head))
    tail_idx = list(range(n - tail, n)) if tail else []
    middle = list(range(head, n - tail))

    def _page_text_len(idx: int) -> int:
        if page_texts is not None and 0 <= idx < len(page_texts):
            return len((page_texts[idx] or "").strip())
        return 0

    remaining = max(0, target - len(head_idx) - len(tail_idx))
    chosen_middle: list[int] = []
    if remaining > 0 and middle:
        step = len(middle) / remaining
        for k in range(remaining):
            w_start = int(k * step)
            w_end = int((k + 1) * step) if k < remaining - 1 else len(middle)
            window = middle[w_start:w_end]
            if not window:
                continue
            if page_texts is not None:
                best = max(window, key=_page_text_len)
            else:
                best = window[len(window) // 2]
            chosen_middle.append(best)

    sel_idx = sorted(set(head_idx + chosen_middle + tail_idx))
    page_nums = [i + 1 for i in sel_idx]
    return page_nums, {
        "strategy": "sampled",
        "total_pages": n,
        "target_pages": target,
        "processed_pages": len(page_nums),
        "head_pages": head,
        "tail_pages": tail,
        "processed_page_nums": page_nums,
    }


def _join_chunk_texts(chunks: list[str]) -> str:
    cleaned = []
    seen = set()
    for chunk in chunks:
        text = (chunk or "").strip()
        if not text:
            continue
        key = text[:500]
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return "\n\n".join(cleaned).strip()


def _call_vision_chat(payload: dict[str, Any], *, target_url: str, source_page_url: Optional[str], timeout: float) -> Optional[str]:
    base_url = (os.getenv("EUF_VISION_URL") or "").rstrip("/")
    api_key = (os.getenv("EUF_VISION_API_KEY") or "").strip()
    url = _vision_chat_url()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    retries = max(1, int(os.getenv("EUF_VISION_RETRIES", "3")))
    base_sleep = max(0.1, float(os.getenv("EUF_VISION_RETRY_BASE_SEC", "2.0")))
    min_interval = max(0.0, float(os.getenv("EUF_VISION_MIN_INTERVAL_SEC", "1.5")))

    global _VISION_LAST_CALL_AT
    for attempt in range(1, retries + 1):
        try:
            with _VISION_CALL_LOCK:
                now = time.monotonic()
                wait_for = (_VISION_LAST_CALL_AT + min_interval) - now
                if wait_for > 0:
                    logger.info("[VisionThrottle] target=%s sleep=%.2fs", target_url, wait_for)
                    time.sleep(wait_for)
                resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
                _VISION_LAST_CALL_AT = time.monotonic()

            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                retry_after = (resp.headers.get("Retry-After") or "").strip()
                sleep_s = max(0.1, float(retry_after)) if retry_after.isdigit() else (base_sleep * attempt)
                logger.warning(
                    "[VisionRetry] target=%s page=%s status=%s attempt=%s/%s sleep=%.2fs",
                    target_url,
                    source_page_url,
                    resp.status_code,
                    attempt,
                    retries,
                    sleep_s,
                )
                if attempt < retries:
                    time.sleep(sleep_s)
                    continue

            if resp.status_code >= 400:
                logger.error(
                    "[VisionFail] target=%s page=%s status=%s body=%s",
                    target_url,
                    source_page_url,
                    resp.status_code,
                    (resp.text or "")[:500],
                )
                return None
            data = resp.json()
            text = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = text.strip() if isinstance(text, str) else ""
            if not text:
                logger.info("[VisionSkip] empty response target=%s page=%s", target_url, source_page_url)
                return None
            return text
        except Exception as e:
            logger.error("[VisionFail] target=%s page=%s host=%s err=%r",
                         target_url, source_page_url, safe_host(target_url), e)
            return None
    return None


def _describe_visual_payload(resolved_target: str, *, target_url: str, source_page_url: Optional[str], timeout: float) -> Optional[str]:
    model = (os.getenv("EUF_VISION_MODEL") or "").strip()
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_PROMPT},
                    {"type": "image_url", "image_url": {"url": resolved_target}},
                ],
            }
        ],
        "temperature": 0.1,
        "max_tokens": int(os.getenv("EUF_VISION_MAX_TOKENS", "900")),
    }
    return _call_vision_chat(payload, target_url=target_url, source_page_url=source_page_url, timeout=timeout)


def _reduce_text_parts(parts: list[str], *, target_url: str, timeout: float) -> Optional[str]:
    cleaned = [(part or "").strip() for part in parts if (part or "").strip()]
    if not cleaned:
        return None

    model = (os.getenv("EUF_VISION_MODEL") or "").strip()
    parts_per_pass = max(2, int(os.getenv("EUF_VISION_REDUCE_PARTS_PER_PASS", "8")))
    current = cleaned
    pass_no = 1

    while len(current) > 1:
        logger.info("[VisionReduce] target=%s pass=%s parts=%s parts_per_pass=%s", target_url, pass_no, len(current), parts_per_pass)
        nxt: list[str] = []
        for start in range(0, len(current), parts_per_pass):
            batch = current[start:start + parts_per_pass]
            if len(batch) == 1:
                nxt.append(batch[0])
                continue
            content = "\n\n".join(f"[PART {i + 1}]\n{part}" for i, part in enumerate(batch))
            payload: dict[str, Any] = {
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": VISION_REDUCE_PROMPT + "\n\nChunk summaries:\n\n" + content,
                    }
                ],
                "temperature": 0.1,
                "max_tokens": int(os.getenv("EUF_VISION_MAX_TOKENS", "900")),
            }
            reduced = _call_vision_chat(
                payload,
                target_url=target_url,
                source_page_url=f"{target_url}#reduce_pass={pass_no}",
                timeout=timeout,
            )
            if reduced:
                nxt.append(reduced.strip())
        if not nxt:
            return _join_chunk_texts(current)
        current = nxt
        pass_no += 1

    return current[0].strip() if current else None


def _fetch_binary_target(target_url: str, *, timeout: int) -> Tuple[Optional[bytes], Optional[str]]:
    if target_url.startswith("data:"):
        return _load_data_url_bytes(target_url)
    sess = get_public_session(timeout=timeout)
    try:
        resp = sess.get(target_url, allow_redirects=True)
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "").split(";", 1)[0].strip().lower() or None
        return resp.content, content_type
    except Exception as e:
        logger.error("[VisionFetchFail] target=%s err=%r", target_url, e)
        return None, None


def extract_visual_target_url(url: str, *, timeout: int = 12) -> Tuple[Optional[str], str]:
    content_type, final_url = probe_content_type(url, timeout=timeout)
    candidate = final_url or url

    if content_type:
        if content_type.startswith("image/"):
            return candidate, "content_type_image"
        if content_type == "application/pdf":
            return candidate, "content_type_pdf"

    sess = get_public_session(timeout=timeout)
    try:
        r = sess.get(candidate, allow_redirects=True)
        ctype = (r.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
        if ctype.startswith("image/"):
            return r.url, "get_content_type_image"
        if ctype == "application/pdf":
            return r.url, "get_content_type_pdf"
        if "html" not in ctype:
            return None, "not_visual_content_type"

        body = r.text or ""
        for pattern in _PDF_URL_PATTERNS:
            for match in pattern.findall(body):
                pdf_url = urljoin(r.url, unescape(match.strip()))
                pdf_content_type, pdf_final_url = probe_content_type(pdf_url, timeout=timeout)
                if pdf_content_type == "application/pdf":
                    return pdf_final_url or pdf_url, "embedded_pdf"

        for pattern in _META_IMAGE_PATTERNS:
            m = pattern.search(body)
            if m:
                return urljoin(r.url, unescape(m.group(1).strip())), "meta_image"

        img_matches = _IMG_SRC_PATTERN.findall(body)
        if len(img_matches) == 1:
            return urljoin(r.url, unescape(img_matches[0].strip())), "single_img"
        return None, "html_no_visual_target"
    except Exception:
        return None, "visual_probe_failed"


def describe_visual_url(target_url: str, *, source_page_url: Optional[str] = None) -> Optional[str]:
    text, _ = describe_visual_url_detailed(target_url, source_page_url=source_page_url)
    return text


def describe_visual_url_detailed(target_url: str, *, source_page_url: Optional[str] = None) -> Tuple[Optional[str], str]:
    base_url = (os.getenv("EUF_VISION_URL") or "").rstrip("/")
    model = (os.getenv("EUF_VISION_MODEL") or "").strip()
    api_key = (os.getenv("EUF_VISION_API_KEY") or "").strip()
    timeout = float(os.getenv("EUF_VISION_TIMEOUT", "90"))
    if not (base_url and model and api_key):
        logger.info("[VisionSkip] missing vision configuration")
        return None, "vision_not_configured"

    resolved_target = target_url
    target_content_type = probe_content_type(target_url, timeout=10)[0]
    if target_url.lower().endswith(".pdf") or target_content_type == "application/pdf":
        pdf_bytes = _download_pdf_bytes(target_url, timeout=int(timeout))
        if pdf_bytes is None:
            return None, "pdf_fetch_failed"
        page_count = _pdf_page_count(pdf_bytes, timeout=int(timeout)) or 1
        map_reduce_threshold = max(1, int(os.getenv("EUF_VISION_PDF_MAP_REDUCE_THRESHOLD", "3")))
        chunk_size = max(1, int(os.getenv("EUF_VISION_PDF_CHUNK_PAGES", "3")))
        max_pages = max(1, int(os.getenv("EUF_VISION_PDF_MAX_PAGES", "100")))

        # Text-first: a born-digital PDF carries a full text layer — index it verbatim
        # (lossless, cheap, full coverage even beyond max_pages) instead of OCR-ing
        # pages. Only scanned/sparse PDFs fall through to vision below.
        page_texts = _pdf_page_texts(pdf_bytes, timeout=int(timeout))
        text_layer = _usable_pdf_text_layer(page_texts, page_count)
        if text_layer is not None:
            logger.info("[VisionPdfTextLayer] target=%s page_count=%s chars=%s", target_url, page_count, len(text_layer))
            return text_layer, "pdf_text_layer"

        if page_count > max_pages:
            # Scanned/sparse AND too long to OCR every page → no lossless body. Caller
            # keeps upstream ko_content_flat (often empty) and indexes metadata only.
            logger.warning("[VisionPdfSkip] target=%s page_count=%s max_pages=%s reason=no_text_layer", target_url, page_count, max_pages)
            return None, "pdf_too_many_pages"

        if page_count > map_reduce_threshold:
            # Scanned/sparse long doc: render a representative subset of pages instead
            # of every page (head + stratified text-rich middle + tail) to bound OCR.
            selected_pages, sel_info = _select_pdf_pages_for_processing(page_count, page_texts=page_texts)
            logger.info(
                "[VisionPdfMapReduce] target=%s page_count=%s chunk_pages=%s threshold=%s max_pages=%s selection=%s",
                target_url,
                page_count,
                chunk_size,
                map_reduce_threshold,
                max_pages,
                sel_info,
            )
            chunk_summaries: list[str] = []
            for start in range(0, len(selected_pages), chunk_size):
                chunk_pages = selected_pages[start:start + chunk_size]
                page_texts: list[str] = []
                for page_num in chunk_pages:
                    rendered = _render_pdf_page_data_url(pdf_bytes, page_num=page_num, timeout=int(timeout))
                    if not rendered:
                        continue
                    text = _describe_visual_payload(
                        rendered,
                        target_url=target_url,
                        source_page_url=f"{source_page_url or target_url}#page={page_num}",
                        timeout=timeout,
                    )
                    if text:
                        page_texts.append(text)
                if page_texts:
                    chunk_summary = _reduce_text_parts(
                        page_texts,
                        target_url=f"{target_url}#pages={chunk_pages[0]}-{chunk_pages[-1]}",
                        timeout=timeout,
                    )
                    if chunk_summary:
                        chunk_summaries.append(chunk_summary)

            combined = _reduce_text_parts(chunk_summaries, target_url=target_url, timeout=timeout)
            if combined:
                return combined, "vision_pdf_map_reduce"
            return None, "vision_pdf_map_reduce_failed"

        rendered = _render_pdf_page_data_url(pdf_bytes, page_num=1, timeout=int(timeout))
        if not rendered:
            return None, "pdf_render_failed"
        resolved_target = rendered
    elif target_content_type == "image/svg+xml":
        raw, _ = _fetch_binary_target(target_url, timeout=int(timeout))
        if not raw:
            return None, "svg_fetch_failed"
        rendered = _render_svg_bytes_to_png_data_url(raw, timeout=int(timeout))
        if not rendered:
            return None, "svg_render_failed"
        resolved_target = rendered
    text = _describe_visual_payload(
        resolved_target,
        target_url=target_url,
        source_page_url=source_page_url,
        timeout=timeout,
    )
    if text:
        return text, "vision"
    return None, "vision_failed"

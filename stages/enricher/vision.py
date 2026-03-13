from __future__ import annotations

import logging
import os
import re
import base64
import subprocess
import tempfile
from html import unescape
from typing import Any, Optional, Tuple
from urllib.parse import urljoin, unquote_to_bytes

import requests

from stages.enricher.utils import get_public_session, safe_host


logger = logging.getLogger(__name__)

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


def vision_enabled() -> bool:
    return bool(
        (os.getenv("EUF_VISION_URL") or "").strip()
        and (os.getenv("EUF_VISION_MODEL") or "").strip()
        and (os.getenv("EUF_VISION_API_KEY") or "").strip()
    )


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
    sess = get_public_session(timeout=timeout)
    try:
        resp = sess.get(pdf_url, allow_redirects=True)
        resp.raise_for_status()
        pdf_bytes = resp.content
    except Exception as e:
        logger.error("[VisionPdfFetchFail] target=%s err=%r", pdf_url, e)
        return None

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
                "1",
                "-singlefile",
                pdf_path,
                out_prefix,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 30))
            if proc.returncode != 0:
                logger.error(
                    "[VisionPdfRenderFail] target=%s rc=%s stderr=%s",
                    pdf_url,
                    proc.returncode,
                    (proc.stderr or "")[:500],
                )
                return None

            png_path = f"{out_prefix}.png"
            with open(png_path, "rb") as f:
                return _png_data_url_from_bytes(f.read())
    except Exception as e:
        logger.error("[VisionPdfRenderFail] target=%s err=%r", pdf_url, e)
        return None


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
    base_url = (os.getenv("EUF_VISION_URL") or "").rstrip("/")
    model = (os.getenv("EUF_VISION_MODEL") or "").strip()
    api_key = (os.getenv("EUF_VISION_API_KEY") or "").strip()
    timeout = float(os.getenv("EUF_VISION_TIMEOUT", "90"))
    if not (base_url and model and api_key):
        logger.info("[VisionSkip] missing vision configuration")
        return None

    url = f"{base_url}/v1/chat/completions"
    resolved_target = target_url
    target_content_type = probe_content_type(target_url, timeout=10)[0]
    if target_url.lower().endswith(".pdf") or target_content_type == "application/pdf":
        rendered = _render_pdf_first_page_data_url(target_url, timeout=int(timeout))
        if not rendered:
            return None
        resolved_target = rendered
    elif target_content_type == "image/svg+xml":
        raw, _ = _fetch_binary_target(target_url, timeout=int(timeout))
        if not raw:
            return None
        rendered = _render_svg_bytes_to_png_data_url(raw, timeout=int(timeout))
        if not rendered:
            return None
        resolved_target = rendered

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
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
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

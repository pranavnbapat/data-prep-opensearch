# enricher_utils.py

import json
import logging
import os
import random
import threading
import time

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from requests.auth import HTTPBasicAuth


_tls = threading.local()
logger = logging.getLogger(__name__)


# ---------------- Small utilities ----------------
def load_stage_payload(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def safe_host(url: Any) -> str:
    try:
        from urllib.parse import urlparse
        if isinstance(url, (bytes, bytearray)):
            url = url.decode("utf-8", "ignore")
        u = urlparse(str(url))
        return f"{u.scheme}://{u.netloc}"
    except Exception:
        return "unknown://"

# ------------------------ HTTP session helpers ------------------------
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


# ------------------------ pagesense runner ------------------------
def fetch_url_text_with_extractor(
    url: str,
    timeout: Optional[int] = None,
    retries: int = None,
    backoff: float = None,
    min_chars: int = None,
    session: Optional[requests.Session] = None,
) -> Optional[str]:
    """
    Calls text-extractor:
      GET {EXTRACTOR_BASE}/api/extract?url=<encoded>
    Returns raw extracted text (str) or None on failure.
    """
    base = os.getenv("URL_CONTENT_EXTRACTOR_BASE", "https://pagesense.nexavion.com").rstrip("/")
    endpoint = f"{base}/api/extract"
    timeout = int(os.getenv("EXTRACTOR_TIMEOUT", str(timeout or 720)))
    retries = int(os.getenv("EXTRACTOR_RETRIES", str(retries or 3)))
    backoff = float(os.getenv("EXTRACTOR_BACKOFF", str(backoff or 1.6)))
    min_chars = int(os.getenv("EXTRACTOR_MIN_CHARS", str(min_chars or 100)))

    sess = session or requests.Session()

    last_err = None
    for attempt in range(retries + 1):
        try:
            # small pre-call jitter to avoid thundering herd
            time.sleep(random.uniform(0.15, 0.45))

            r = sess.get(endpoint, params={"url": url}, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            if r.status_code != 200:
                return None

            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if not body or not body.get("ok"):
                return None

            text = (body.get("text") or "").strip()
            if len(text) < min_chars:
                return None
            return text

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_err = e
            if attempt < retries:
                # exponential backoff + jitter
                delay = (backoff ** attempt) + random.uniform(0.2, 0.6)
                time.sleep(delay)
                continue
            return None
        except Exception as e:
            last_err = e
            return None

# def pagesense_one(llid: str, url: str) -> Tuple[str, str, Optional[str], str]:
def pagesense_one(*, llid: str, url: str, http_timeout: int) -> Tuple[str, str, Optional[str], str, float]:
    time.sleep(random.uniform(0.05, 0.25))
    try:
        sess = get_public_session(timeout=int(os.getenv("EXTRACTOR_HTTP_TIMEOUT", "35")))
        text = fetch_url_text_with_extractor(
            url,
            timeout=int(os.getenv("EXTRACTOR_TIMEOUT", "150")),
            retries=int(os.getenv("EXTRACTOR_RETRIES", "3")),
            backoff=float(os.getenv("EXTRACTOR_BACKOFF", "1.6")),
            min_chars=int(os.getenv("EXTRACTOR_MIN_CHARS", "100")),
            session=sess,
        )
        return llid, url, (text.strip() if isinstance(text, str) else None), "pagesense"
    except Exception as e:
        logger.error("[PageSenseFail] llid=%s host=%s err=%s", llid, safe_host(url), e)
        return llid, url, None, "pagesense:failed"


# ------------------------ transcribe with runpod custom runner ------------------------
def transcribe_with_custom_api(*, media_url: str) -> Optional[str]:
    """
    Calls the RunPod /transcribe endpoint with a direct media URL (S3 hosted audio/video).
    Returns transcript text or None.
    """
    user = (os.getenv("CUSTOM_TRANSCRIBE_USER") or "").strip()
    pwd = (os.getenv("CUSTOM_TRANSCRIBE_PASSWORD") or "").strip()

    auth = None
    if user and pwd:
        auth = HTTPBasicAuth(user, pwd)

    retries = int(os.getenv("CUSTOM_TRANSCRIBE_RETRIES", "2"))
    backoff = float(os.getenv("CUSTOM_TRANSCRIBE_BACKOFF", "1.5"))
    endpoint = (os.getenv("CUSTOM_TRANSCRIBE_ENDPOINT") or "").strip()

    if not endpoint:
        logger.error("[CustomTranscribe] Missing CUSTOM_TRANSCRIBE_ENDPOINT env var")
        return None

    whisper_model = (os.getenv("CUSTOM_TRANSCRIBE_MODEL") or "large-v1").strip() or "large-v1"
    timeout_s = float(os.getenv("CUSTOM_TRANSCRIBE_HTTP_TIMEOUT", "180"))  # transcription can take a while

    for attempt in range(retries + 1):
        try:
            sess = get_public_session(timeout=int(timeout_s))
            r = sess.post(
                endpoint.rstrip("/") + "/transcribe",
                json={"url": media_url, "whisper_model": whisper_model},
                headers={"accept": "application/json", "content-type": "application/json"},
                auth=auth,
            )

            if r.status_code >= 400:
                logger.error(
                    "[CustomTranscribeFail] media_host=%s status=%s body=%s",
                    media_url,
                    r.status_code,
                    (r.text or "")[:500],  # cap to avoid huge logs
                )
                return None

            r.raise_for_status()
            data = r.json()

            text = (((data or {}).get("whisper") or {}).get("text") or "")
            text = text.strip() if isinstance(text, str) else ""
            return text or None
        except Exception as e:
            if attempt >= retries:
                logger.error("[CustomTranscribeFail] host=%s err=%s", safe_host(media_url), e)
                return None
            time.sleep(backoff ** attempt)

def custom_transcribe_one(llid: str, url: str) -> Tuple[str, str, Optional[str], str]:
    """
    custom_transcribe: send direct hosted media URL to the custom endpoint.
    """
    time.sleep(random.uniform(0.05, 0.25))

    text = transcribe_with_custom_api(media_url=url)
    if text:
        return llid, url, text, "custom_transcribe"
    return llid, url, None, "custom_transcribe:failed"

def _normalise_netloc(netloc: str) -> str:
    n = (netloc or "").strip().lower()
    # Drop common prefix for checks
    if n.startswith("www."):
        n = n[4:]
    return n

def classify_url_for_enrichment(url: Any) -> Tuple[bool, str]:
    """
    Returns (ok, reason).
    ok=True  -> safe to send to pagesense/transcribe
    ok=False -> skip with 'reason'
    """
    if isinstance(url, (bytes, bytearray)):
        url = url.decode("utf-8", "ignore")

    if not isinstance(url, str):
        return False, "not_a_string"

    u = url.strip()
    if not u:
        return False, "empty"

    try:
        p = urlparse(u)
    except Exception:
        return False, "parse_error"

    scheme = (p.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return False, f"unsupported_scheme:{scheme or 'none'}"

    netloc = _normalise_netloc(p.netloc)
    if not netloc:
        return False, "missing_host"

    # Reject obviously broken hosts like "www.google" (no TLD)
    # Heuristic: must contain at least one dot and end segment length >= 2
    if "." not in netloc:
        return False, "host_missing_tld"
    tld = netloc.rsplit(".", 1)[-1]
    if len(tld) < 2:
        return False, "host_bad_tld"

    # Homepage / bare domain: no real path (or "/") AND no query params
    path = p.path or ""
    has_query = bool(p.query and p.query.strip())
    if (path == "" or path == "/") and not has_query:
        return False, "homepage_or_bare_domain"

    return True, "ok"
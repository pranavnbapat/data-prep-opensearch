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

from downloader_utils import sha256_obj, stable_str
from io_helpers import resolve_latest_pointer, find_latest_matching
from requests.auth import HTTPBasicAuth


_tls = threading.local()
logger = logging.getLogger(__name__)


# ---------------- Small utilities ----------------
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

def env_bool(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

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
def pagesense_one(*, llid: str, url: str, http_timeout: int) -> Tuple[str, str, Optional[str], str]:
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
        logger.error("[PageSenseFail] id=%s host=%s err=%s", llid, safe_host(url), e)
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

def target_url_for_enrichment(doc: Dict[str, Any]) -> Optional[str]:
    """
    Decide what URL we enrich from.

    - pagesense: always doc["@id"]
    - transcribe:
        - if hosted media: use ko_file_id (S3 hosted URL)
        - else: use @id (youtube/vimeo/etc.)
    """
    via = doc.get("enrich_via")
    at_id = doc.get("@id") if isinstance(doc.get("@id"), str) else None
    ko_file_id = doc.get("ko_file_id") if isinstance(doc.get("ko_file_id"), str) else None

    if via == "pagesense":
        return at_id.strip() if at_id and at_id.strip() else None

    ko_is_hosted = bool(doc.get("ko_is_hosted"))
    mimetype = (doc.get("ko_object_mimetype") or "").strip().lower()

    if via == "api_transcribe":
        # hosted media (video/audio/image) -> use hosted file url
        if (not ko_is_hosted) and any(mimetype.startswith(x) for x in ("video/", "audio/")):
            return ko_file_id.strip() if ko_file_id and ko_file_id.strip() else None

        # non-hosted media platforms -> use @id
        return at_id.strip() if at_id and at_id.strip() else None

    if via == "custom_transcribe":
        # hosted media (video/audio/image) -> use hosted file url
        if ko_is_hosted and any(mimetype.startswith(x) for x in ("video/", "audio/")):
            return ko_file_id.strip() if ko_file_id and ko_file_id.strip() else None

        # non-hosted media platforms -> use @id
        return at_id.strip() if at_id and at_id.strip() else None

    return None

def is_media_mimetype(m: Any) -> bool:
    """
    Treat image/*, audio/*, video/* as media requiring transcription.
    """
    if not isinstance(m, str):
        return False
    mm = m.strip().lower()
    # mm.startswith("image/") or
    return mm.startswith("audio/") or mm.startswith("video/")


def is_video_platform_url(u: Any) -> bool:
    """
    True if URL is a known video/audio platform where "transcribe" makes sense.
    Keep it conservative and domain-based (less false positives).
    """
    if not isinstance(u, str):
        return False
    s = u.strip()
    if not s:
        return False

    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return False

    # normalise common "www."
    if host.startswith("www."):
        host = host[4:]

    video_hosts = {
        "youtube.com",
        "youtu.be",
        "m.youtube.com",
        "dailymotion.com",
        "dai.ly",
        "vimeo.com",
        "player.vimeo.com",
        "tiktok.com",
        "twitter.com",
        "x.com",
        "facebook.com",
        "fb.watch",
        "instagram.com",
        "twitch.tv",
    }

    if host in video_hosts:
        return True

    # A few platforms use subdomains heavily (e.g. *.youtube.com)
    if host.endswith(".youtube.com"):
        return True
    if host.endswith(".dailymotion.com"):
        return True
    if host.endswith(".tiktok.com"):
        return True
    if host.endswith(".twitch.tv"):
        return True

    # Optional: treat direct media file URLs as transcribe-worthy even if not hosted
    if any(path.endswith(ext) for ext in (".mp4", ".mp3", ".wav", ".m4a", ".webm", ".mov", ".mkv", ".aac", ".ogg")):
        return True

    return False

def as_bool(v: Any) -> bool:
    """Normalise common truthy/falsey representations."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off", ""}:
            return False
    # last resort: Python truthiness
    return bool(v)

def set_enrich_via(d: Dict[str, Any]) -> None:
    ko_is_hosted = as_bool(d.get("ko_is_hosted"))
    mimetype = d.get("ko_object_mimetype")
    at_id = d.get("@id")

    if ko_is_hosted and is_media_mimetype(mimetype):
        d["enrich_via"] = "custom_transcribe"
        return

    if (not ko_is_hosted) and is_video_platform_url(at_id):
        d["enrich_via"] = "api_transcribe"
        return

    if not ko_is_hosted:
        d["enrich_via"] = "pagesense"
        return

    # Hosted but non-media: no external enrichment route needed (keep it absent)
    d.pop("enrich_via", None)

def has_enrich_via(doc: Dict[str, Any]) -> bool:
    return doc.get("enrich_via") in {"pagesense", "api_transcribe", "custom_transcribe"}

def is_placeholder_content(val: Any) -> bool:
    if not isinstance(val, str):
        return True
    return val.strip().lower() in ("", "no content present")

def already_enriched(prev_doc: Dict[str, Any]) -> bool:
    return prev_doc.get("enriched") == 1 and not is_placeholder_content(prev_doc.get("ko_content_flat"))

def should_skip(current: Dict[str, Any], prev_doc: Optional[Dict[str, Any]]) -> bool:
    if not prev_doc:
        return False
    # skip only if target didn't change
    if (prev_doc.get("@id") or "") != (current.get("@id") or ""):
        return False
    # and previous enrichment exists
    return already_enriched(prev_doc)

def is_deapi_platform_url(u: str) -> bool:
    """
    deAPI /vid2txt only accepts a few platforms (per error message).
    Keep this strict to avoid 422 spam.
    """
    if not isinstance(u, str):
        return False
    v = u.lower()
    return any(host in v for host in (
        "youtube.com/", "youtu.be/",
        "twitter.com/", "x.com/",
        "twitch.tv/",
        "kick.com/",
    ))

def compute_enrich_inputs_fp(doc: Dict[str, Any]) -> str:
    """
    Compute enricher-input fingerprint according to the 3 regimes we agreed.

    A) URL-only (ko_is_hosted False) -> @id only
    B) Hosted doc-like (hosted, non media, no enrich_via) -> @id + ko_file_id + ko_content_flat
       (even if enricher does nothing, this FP is still useful for consistency)
    C) Hosted media (video/audio/image) -> @id + ko_file_id
    """
    ko_is_hosted = as_bool(doc.get("ko_is_hosted"))
    mimetype = (doc.get("ko_object_mimetype") or "").strip().lower()
    at_id = stable_str(doc.get("@id"))
    ko_file_id = stable_str(doc.get("ko_file_id"))
    ko_content_flat = stable_str(doc.get("ko_content_flat"))

    is_media = any(mimetype.startswith(x) for x in ("video/", "audio/", "image/"))

    if not ko_is_hosted:
        obj = {"mode": "url_only", "@id": at_id}
        return sha256_obj(obj)

    if is_media:
        obj = {"mode": "hosted_media", "@id": at_id, "ko_file_id": ko_file_id}
        return sha256_obj(obj)

    # hosted, non-media
    obj = {"mode": "hosted_doc", "@id": at_id, "ko_file_id": ko_file_id, "ko_content_flat": ko_content_flat}
    return sha256_obj(obj)


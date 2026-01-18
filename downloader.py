# downloader.py

import hashlib
import json
import logging
import os
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TypedDict, Optional, Dict, Any, List, Tuple
from urllib3.util.retry import Retry

import requests

from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

from utils import (CustomJSONEncoder, normalize_date_to_yyyy_mm_dd, strip_html_light, clean_list, get_ko_id,
                   extract_location_names, is_url_based_ko, flatten_ko_content,)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


ENV_CHOICES = {"DEV", "PRD"}
KO_CONTENT_MODE = os.getenv("KO_CONTENT_MODE", "flat_only").strip() or "flat_only"

logger = logging.getLogger(__name__)

class BackendCfg(TypedDict):
    host: str
    user: str
    pwd: str

@dataclass(frozen=True)
class DownloadResult:
    docs: List[Dict[str, Any]]
    url_tasks: List[Dict[str, Any]]
    media_tasks: List[Dict[str, Any]]
    stats: Dict[str, Any]


# ---------------- Thread-local session ----------------
_tls = threading.local()

def _session_cache_key(backend_cfg: BackendCfg, timeout: int) -> str:
    # Key includes host + username + timeout; password changes should trigger new container redeploy anyway.
    return f"{backend_cfg['host']}|{backend_cfg['user']}|{timeout}"

def get_session(backend_cfg: BackendCfg, timeout: int = 15) -> requests.Session:
    """
    Return a thread-local pooled session. Recreates session when backend/timeout changes.
    Safe to call from threadpool workers.
    """
    key = _session_cache_key(backend_cfg, timeout)
    sess = getattr(_tls, "session", None)
    sess_key = getattr(_tls, "session_key", None)
    if sess is None or sess_key != key:
        # Close old session if present
        try:
            if sess is not None:
                sess.close()
        except Exception:
            pass
        sess = _build_session(backend_cfg, timeout=timeout)
        _tls.session = sess
        _tls.session_key = key
    return sess

def _build_session(backend_cfg: BackendCfg, timeout: int = 15) -> requests.Session:
    """
    Create a pooled, retrying Requests Session with HTTP Basic Auth.
    Adds default timeout to all requests.
    """
    s = requests.Session()
    s.auth = HTTPBasicAuth(backend_cfg["user"], backend_cfg["pwd"])
    s.headers.update({
        "accept": "application/json",
        "Content-Type": "application/json",
    })

    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # Enforce a default timeout transparently
    original = s.request
    def _with_timeout(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return original(method, url, **kw)
    s.request = _with_timeout  # type: ignore[attr-defined]

    return s


# ---------------- Backend config ----------------
def load_backend_cfg(env_mode: str) -> BackendCfg:
    """
    Loads backend config from environment variables.
    No globals; safe per-job.
    """
    mode = (env_mode or "").strip().upper()
    if mode not in ENV_CHOICES:
        logging.warning("Invalid ENV_MODE=%r; falling back to DEV", env_mode)
        mode = "DEV"

    if mode == "DEV":
        host = os.getenv("BACKEND_CORE_HOST_DEV", "")
        user = os.getenv("BACKEND_CORE_DEV_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_DEV_API_PASSWORD", "")
    else:
        host = os.getenv("BACKEND_CORE_HOST_PRD", "")
        user = os.getenv("BACKEND_CORE_PRD_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_PRD_API_PASSWORD", "")

    if not host or not user or not pwd:
        raise RuntimeError(
            f"Missing required env vars for {mode}. "
            f"Expected BACKEND_CORE_HOST_{mode}, BACKEND_CORE_{mode}_API_USERNAME, BACKEND_CORE_{mode}_API_PASSWORD"
        )

    return {"host": host.rstrip("/"), "user": user, "pwd": pwd}


def api_base(backend_cfg: BackendCfg) -> str:
    base = backend_cfg["host"]
    if not base:
        raise RuntimeError("Backend host is empty.")
    return base


# ---------------- Small helpers ----------------

def _stable_list(v: Any) -> List[str]:
    """Normalise list-like fields to a deterministic list of strings."""
    if v is None:
        return []
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if isinstance(x, str):
                s = x.strip()
                if s:
                    out.append(s)
        return sorted(out, key=str.casefold)
    return []

def _stable_str(v: Any) -> str:
    return (v or "").strip() if isinstance(v, str) else ("" if v is None else str(v))

def _sha256_obj(obj: Any) -> str:
    """Hash a python object deterministically using JSON canonicalisation."""
    # IMPORTANT: ensure_ascii=False keeps unicode stable; sort_keys stabilises key order
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _is_blank(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str):
        return not val.strip()
    if isinstance(val, list):
        return len(val) == 0 or all((not isinstance(x, str)) or (not x.strip()) for x in val)
    if isinstance(val, dict):
        return len(val) == 0
    return False

def _norm_ts(v: Any) -> Optional[str]:
    """Normalise timestamps to comparable strings; keep None if missing."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    return str(v)

def _is_unchanged_against_prev(
    *,
    llid: str,
    cleaned: Dict[str, Any],
    prev_index: Optional[Dict[str, Dict[str, Any]]],
    compare_project_ts: bool = True,
) -> bool:
    """
    True when ko_updated_at (and optionally proj_updated_at) matches previous snapshot.
    """
    if not prev_index:
        return False
    prev = prev_index.get(llid)
    if not prev:
        return False

    cur_ko = _norm_ts(cleaned.get("ko_updated_at"))
    prev_ko = _norm_ts(prev.get("ko_updated_at"))

    if not cur_ko or not prev_ko:
        return False
    if cur_ko != prev_ko:
        return False

    if compare_project_ts:
        cur_p = _norm_ts(cleaned.get("proj_updated_at"))
        prev_p = _norm_ts(prev.get("proj_updated_at"))
        # If either missing, don't let project timestamp block reuse
        if cur_p and prev_p and cur_p != prev_p:
            return False

    return True

def _valid_year(yyyy: str, min_year: int = 1000, max_year: int = 2100) -> bool:
    try:
        y = int(yyyy)
        return min_year <= y <= max_year
    except Exception:
        return False

def dedup_case_insensitive(items: Optional[List[str]]) -> List[str]:
    if not items:
        return []
    seen = set()
    out: List[str] = []
    for x in items:
        if not isinstance(x, str):
            continue
        k = x.casefold()
        if k not in seen:
            out.append(x)
            seen.add(k)
    return out

def _is_image_mimetype(m: Any) -> bool:
    if not isinstance(m, str):
        return False
    return m.lower().strip().startswith("image/")

def _is_video_mimetype(m: Any) -> bool:
    if not isinstance(m, str):
        return False
    return m.lower().strip().startswith("video/")

def _is_audio_mimetype(m: Any) -> bool:
    if not isinstance(m, str):
        return False
    return m.lower().strip().startswith("audio/")

def _is_media_any(m: Any) -> bool:
    return _is_video_mimetype(m) or _is_audio_mimetype(m) or _is_image_mimetype(m)

def _looks_like_url(s: Any) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))

def _as_bool(v: Any) -> bool:
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

def extract_first_resource_file_info(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull file metadata from doc['knowledge_object_resources'][0].
    """
    kor = doc.get("knowledge_object_resources")
    if not isinstance(kor, list) or not kor:
        return {}

    first = kor[0]
    if not isinstance(first, dict):
        return {}

    dm = first.get("display_metadata") or {}
    kfd = first.get("ko_file_details") or {}

    out: Dict[str, Any] = {}

    if isinstance(kfd, dict):
        out["ko_object_name"] = kfd.get("object_name")
        out["ko_object_extension"] = kfd.get("object_extension")
        out["ko_object_size"] = kfd.get("object_size")
        out["ko_object_mimetype"] = kfd.get("object_mimetype")
        out["ko_upload_source"] = kfd.get("upload_source")
        out["ko_file_id"] = kfd.get("@id")

    if isinstance(dm, dict):
        out["ko_is_hosted"] = dm.get("is_hosted")
        out["ko_hosted_mime_type"] = dm.get("hosted_mime_type")
        out["ko_external_content_type"] = dm.get("external_content_type")

    out["ko_resource_language"] = first.get("language")

    # drop blanks
    return {k: v for k, v in out.items() if not _is_blank(v)}


def extract_first_resource_url(doc: Dict[str, Any]) -> Optional[str]:
    """
    Best-effort URL from first resource.
    """
    kor = doc.get("knowledge_object_resources")
    if not isinstance(kor, list) or not kor:
        return None

    first = kor[0]
    if not isinstance(first, dict):
        return None

    dm = first.get("display_metadata") or {}
    if not isinstance(dm, dict):
        dm = {}

    candidates = [
        first.get("url"),
        first.get("download_url"),
        first.get("href"),
        first.get("link"),
        dm.get("hosted_url"),
        dm.get("external_url"),
        dm.get("resource_url"),
        first.get("@id"),
    ]
    for u in candidates:
        if isinstance(u, str) and u.strip():
            return u.strip()
    return None

def is_youtube_url(u: str) -> bool:
    if not isinstance(u, str):
        return False
    v = u.lower()
    return (
        "youtube.com/watch" in v or
        "youtube.com/live" in v or
        "youtube.com/shorts" in v or
        "youtu.be/" in v
    )


# ---------------- API calls ----------------

def fetch_ko_metadata_api(
    backend_cfg: BackendCfg,
    *,
    limit: Optional[int] = None,
    page: int = 1,
    sort_criteria: int = 1,
) -> Dict[str, Any]:
    """
    POST {base}/api/logical_layer/documents?limit={limit}&page={page}&sort_criteria={sort_criteria}
    Body: {}
    Returns: { "data": [...], "pagination": {...} }
    """
    base = api_base(backend_cfg)
    url = f"{base}/api/logical_layer/documents"
    params: Dict[str, Any] = {"page": page, "sort_criteria": sort_criteria}
    if limit is not None:
        params["limit"] = limit

    s = get_session(backend_cfg, timeout=int(os.getenv("DL_HTTP_TIMEOUT", "30")))
    r = s.post(url, params=params, json={})
    r.raise_for_status()
    return r.json()

def fetch_projects_api(backend_cfg: BackendCfg, project_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    POST {base}/api/logical_layer/projects/
    Body: ["id1","id2",...]
    Returns: list of project objects; indexed by _id string.
    """
    if not project_ids:
        return {}

    base = api_base(backend_cfg)
    url = f"{base}/api/logical_layer/projects/"

    s = get_session(backend_cfg, timeout=int(os.getenv("DL_HTTP_TIMEOUT", "30")))
    r = s.post(url, json=project_ids)
    r.raise_for_status()
    items = r.json() or []
    return {str(p.get("_id")): p for p in items if isinstance(p, dict)}

def get_ko_content(backend_cfg: BackendCfg, document_id: str) -> List[dict]:
    """
    GET {base}/api/nlp/ko_content_document?document_id=<id>
    Returns list[dict] normalised.
    404 -> [] (benign)
    """

    t0 = time.perf_counter()

    base = api_base(backend_cfg)
    url = f"{base}/api/nlp/ko_content_document"
    params = {"document_id": document_id}

    s = get_session(backend_cfg, timeout=int(os.getenv("CONTENT_HTTP_TIMEOUT", "30")))

    try:
        r = s.get(url, params=params)
        if r.status_code == 404:
            dt = time.perf_counter() - t0
            thr = float(os.getenv("SLOW_CONTENT_SEC", "2.0"))
            if dt > thr:
                logging.warning("[SlowContent] id=%s dt=%.2fs", document_id, dt)

            # no extracted content available
            return []
        r.raise_for_status()
        body = r.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        logging.error("Content API error for %s: HTTP %s %s", document_id, status, e)

        dt = time.perf_counter() - t0
        thr = float(os.getenv("SLOW_CONTENT_SEC", "2.0"))
        if dt > thr:
            logging.warning("[SlowContent] id=%s dt=%.2fs", document_id, dt)

        return []
    except Exception as e:
        logging.error("Content API error for %s: %s", document_id, e)

        dt = time.perf_counter() - t0
        thr = float(os.getenv("SLOW_CONTENT_SEC", "2.0"))
        if dt > thr:
            logging.warning("[SlowContent] id=%s dt=%.2fs", document_id, dt)

        return []

    # normalise to list
    if isinstance(body, dict):
        if "data" in body and isinstance(body["data"], list):
            items = body["data"]
        elif "results" in body and isinstance(body["results"], list):
            items = body["results"]
        elif "content" in body and isinstance(body["content"], list):
            items = body["content"]
        else:
            items = [body]
    elif isinstance(body, list):
        items = body
    elif isinstance(body, str):
        items = [body]
    else:
        items = []

    normalised: List[dict] = []
    for el in items:
        if isinstance(el, dict):
            # drop heavy/unwanted keys here if needed; keep as-is for flatten_ko_content()
            normalised.append(el)
        elif isinstance(el, str):
            normalised.append({"content_text": el})
        else:
            continue

    dt = time.perf_counter() - t0
    thr = float(os.getenv("SLOW_CONTENT_SEC", "2.0"))
    if dt > thr:
        logging.warning("[SlowContent] id=%s dt=%.2fs", document_id, dt)

    return normalised


# ---------------- Cleaning + enrichment ----------------

def enrich_with_project(ko_doc: Dict[str, Any], projects_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Projects are already fetched via API; projects_index maps project_id/_id -> project_doc.
    """
    project_id = ko_doc.get("project_id")
    if not project_id:
        return ko_doc

    p = projects_index.get(str(project_id))
    if not p:
        return ko_doc

    ko_doc["project_name"] = p.get("title") or p.get("projectName") or p.get("name")
    ko_doc["project_acronym"] = p.get("acronym") or p.get("projectAcronym")
    ko_doc["project_url"] = p.get("URL") or p.get("projectURL") or p.get("website")
    ko_doc["project_doi"] = (p.get("identifiers") or {}).get("grantDoi") or p.get("projectDoi")
    ko_doc["project_type"] = p.get("project_type") or p.get("type")

    if p.get("created_ts"):
        ko_doc["proj_created_at"] = p["created_ts"]
    if p.get("updated_ts"):
        ko_doc["proj_updated_at"] = p["updated_ts"]
    return ko_doc

def clean_ko_metadata(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cleans metadata fields while preserving meaning.
    Also normalises date_of_completion into YYYY-MM-DD and drops invalid dates.
    """
    fields_to_exclude = {
        "schema_version", "@context", "_tags", "object_hash", "uploaded_by", "knowledge_object_version",
        "created_by", "updated_by", "version", "contributor_custom_metadata", "physical_layer_ko_metadata_id",
        "status", "object_metadata", "language_versions", "otherFields", "knowledge_object_resources", "collection",
    }

    cleaned = {k: v for k, v in doc.items() if k not in fields_to_exclude}

    # attach file/resource info from resource[0]
    cleaned.update(extract_first_resource_file_info(doc))

    raw_date = doc.get("date_of_completion")
    parsed = None
    try:
        parsed = normalize_date_to_yyyy_mm_dd(raw_date) if raw_date else None
    except Exception:
        parsed = None

    if parsed and _valid_year(parsed[:4], min_year=1000, max_year=2100):
        cleaned["date_of_completion"] = parsed
    else:
        cleaned.pop("date_of_completion", None)

    # topics normalisation (string OR list)
    topics_raw = doc.get("topics")
    topics_norm: List[str] = []
    if isinstance(topics_raw, str):
        s = topics_raw.strip()
        topics_norm = [s] if s else []
    elif isinstance(topics_raw, list):
        seen = set()
        for x in topics_raw:
            if isinstance(x, str):
                s = x.strip()
                if s and s not in seen:
                    topics_norm.append(s)
                    seen.add(s)

    if topics_norm:
        cleaned["topics"] = topics_norm
    else:
        cleaned.pop("topics", None)

    # rename created/updated timestamps (avoid clashing with project ones)
    if "created_ts" in cleaned:
        cleaned["ko_created_at"] = cleaned.pop("created_ts")
    if "updated_ts" in cleaned:
        cleaned["ko_updated_at"] = cleaned.pop("updated_ts")

    # drop nested project_details if present; we enrich separately
    cleaned.pop("project_details", None)

    return cleaned

def combine_metadata_and_content(metadata: Dict[str, Any], content_list: List[dict]) -> Dict[str, Any]:
    d = dict(metadata)
    d["ko_content"] = content_list
    return d


# ---------------- Core per-doc preparation ----------------

def prepare_one_doc(
    backend_cfg: BackendCfg,
    doc: Dict[str, Any],
    projects_index: Dict[str, Dict[str, Any]],
    *,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
    skip_content_if_unchanged: bool = True,
) -> Tuple[
    Optional[Dict[str, Any]],
    Optional[Dict[str, Any]],
    Optional[Dict[str, Any]],
    str,
    Optional[Dict[str, Any]],
    bool,
    Optional[str],  # dl_fp
    Optional[str],  # enricher_fp
    Optional[str],  # improver_fp
]:

    """
    Returns:
      (combined_doc_or_none, url_task_or_none, media_task_or_none, status)

    status is one of:
      - "emitted"
      - "dropped"
    """

    def _drop(reason: str, cleaned_doc: Optional[Dict[str, Any]] = None) -> Tuple[
        None, None, None, str, Dict[str, Any], bool, None, None, None
    ]:
        return (
            None, None, None,
            f"drop:{reason}",
            {
                "reason": f"drop:{reason}",
                "logical_layer_id": str(doc.get("_id", "")).strip() or None,
                "at_id": (cleaned_doc or {}).get("@id") or get_ko_id(doc) or None,
                "title": (cleaned_doc or {}).get("title") or doc.get("title") or None,
                "project_id": (cleaned_doc or {}).get("project_id") or doc.get("project_id") or None,
            },
            True,
            None, None, None
        )

    try:
        # hard stop: published only
        if str(doc.get("status", "")).strip().lower() != "published":
            return _drop("not_published", None)

        cleaned = clean_ko_metadata(doc)

        # canonical KO @id
        ko_id = get_ko_id(doc) or get_ko_id(cleaned)  # prefer original
        if ko_id:
            cleaned["@id"] = ko_id

        # locations -> locations_flat
        loc_flat = extract_location_names(doc.get("locations", []))
        if loc_flat:
            cleaned["locations_flat"] = loc_flat

        # Clean list-ish fields (light HTML strip, whitespace normalisation)
        cleaned["keywords"] = clean_list(cleaned.get("keywords"), item_cleaner=strip_html_light)
        cleaned["languages"] = clean_list(cleaned.get("languages"), item_cleaner=strip_html_light)
        cleaned["locations_flat"] = clean_list(cleaned.get("locations_flat"), item_cleaner=strip_html_light)

        subcats = cleaned.get("subcategories", cleaned.get("subcategory"))
        cleaned["subcategories"] = clean_list(subcats, item_cleaner=strip_html_light)
        cleaned.pop("subcategory", None)

        cleaned["topics"] = clean_list(cleaned.get("topics"), item_cleaner=strip_html_light)
        cleaned["themes"] = clean_list(cleaned.get("themes"), item_cleaner=strip_html_light)
        cleaned["creators"] = clean_list(cleaned.get("creators"), item_cleaner=strip_html_light)

        # dedupe case-insensitively
        cleaned["keywords"] = dedup_case_insensitive(cleaned.get("keywords"))
        cleaned["languages"] = dedup_case_insensitive(cleaned.get("languages"))
        cleaned["locations_flat"] = dedup_case_insensitive(cleaned.get("locations_flat"))
        cleaned["subcategories"] = dedup_case_insensitive(cleaned.get("subcategories"))
        cleaned["topics"] = dedup_case_insensitive(cleaned.get("topics"))
        cleaned["themes"] = dedup_case_insensitive(cleaned.get("themes"))
        cleaned["creators"] = dedup_case_insensitive(cleaned.get("creators"))

        # enforce date_of_completion
        if _is_blank(cleaned.get("date_of_completion")):
            return _drop("missing_date_of_completion", cleaned)

        # pre-enrichment required fields
        required_pre = ["project_id", "title", "@id", "topics", "themes", "date_of_completion"]
        if any(_is_blank(cleaned.get(f)) for f in required_pre):
            return _drop("missing_required_pre", cleaned)

        # enrich with project
        cleaned = enrich_with_project(cleaned, projects_index)

        # post-enrichment required fields
        required_post = ["project_name", "project_acronym"]
        if any(_is_blank(cleaned.get(f)) for f in required_post):
            return _drop("missing_project_fields", cleaned)

        # logical layer id
        logical_layer_id = str(doc.get("_id", "")).strip()
        if not logical_layer_id:
            return _drop("missing_logical_layer_id", cleaned)

        cleaned["_orig_id"] = logical_layer_id

        # ---------------- Classification for enrichment ----------------
        ko_id_url = cleaned.get("@id")  # canonical KO id
        ko_is_hosted = _as_bool(cleaned.get("ko_is_hosted"))
        mimetype = cleaned.get("ko_object_mimetype")
        hosted_url = extract_first_resource_url(doc)  # best effort for hosted media/file access

        # Default flags
        cleaned.pop("is_url_only", None)
        cleaned.pop("is_media", None)
        cleaned.pop("first_url", None)
        cleaned.pop("media_url", None)

        def _compute_fps(cleaned_doc: Dict[str, Any]) -> Tuple[str, str, str]:
            # ---- Improver inputs ----
            improver_obj = {
                "title": _stable_str(cleaned_doc.get("title")),
                "subtitle": _stable_str(cleaned_doc.get("subtitle")),
                "description": _stable_str(cleaned_doc.get("description")),
                "keywords": _stable_list(cleaned_doc.get("keywords")),
                "ko_content_flat": _stable_str(cleaned_doc.get("ko_content_flat")),
            }
            improver_fp = _sha256_obj(improver_obj)

            # ---- Enricher inputs (only identity of external targets) ----
            enricher_obj = {
                "is_url_only": bool(cleaned_doc.get("is_url_only")),
                "is_media": bool(cleaned_doc.get("is_media")),
                "first_url": _stable_str(cleaned_doc.get("first_url")),  # we'll set this below when relevant
                "media_url": _stable_str(cleaned_doc.get("media_url")),  # we'll set this below when relevant
                "media_mimetype": _stable_str(cleaned_doc.get("media_mimetype")),
            }
            enricher_fp = _sha256_obj(enricher_obj)

            # ---- Downloader/source fingerprint (bigger picture) ----
            dl_obj = {
                "@id": _stable_str(cleaned_doc.get("@id")),
                "project_id": _stable_str(cleaned_doc.get("project_id")),
                "project_acronym": _stable_str(cleaned_doc.get("project_acronym")),
                "project_name": _stable_str(cleaned_doc.get("project_name")),
                "date_of_completion": _stable_str(cleaned_doc.get("date_of_completion")),
                "topics": _stable_list(cleaned_doc.get("topics")),
                "themes": _stable_list(cleaned_doc.get("themes")),
                "license": _stable_str(cleaned_doc.get("license")),
                "ko_file_id": _stable_str(cleaned_doc.get("ko_file_id")),
                "ko_object_mimetype": _stable_str(cleaned_doc.get("ko_object_mimetype")),
                "ko_object_size": cleaned_doc.get("ko_object_size"),
                "ko_updated_at": _stable_str(cleaned_doc.get("ko_updated_at")),
                "proj_updated_at": _stable_str(cleaned_doc.get("proj_updated_at")),
                # include content signal that you actually use downstream
                "ko_content_flat": _stable_str(cleaned_doc.get("ko_content_flat")),
                # include external identity as part of source truth too
                "first_url": _stable_str(cleaned_doc.get("first_url")),
                "media_url": _stable_str(cleaned_doc.get("media_url")),
                "media_mimetype": _stable_str(cleaned_doc.get("media_mimetype")),
            }
            dl_fp = _sha256_obj(dl_obj)

            return dl_fp, enricher_fp, improver_fp

        # --- incremental reuse: if unchanged, reuse previous snapshot doc and skip content API ---
        compare_proj = os.getenv("DL_COMPARE_PROJECT_TS", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        is_unchanged = _is_unchanged_against_prev(
            llid=logical_layer_id,
            cleaned=cleaned,
            prev_index=prev_index,
            compare_project_ts=compare_proj,
        )

        if is_unchanged and prev_index and skip_content_if_unchanged:
            prev_doc = prev_index.get(logical_layer_id)
            if isinstance(prev_doc, dict):
                # Keep the latest cleaned metadata for fields that might be re-normalised,
                # but preserve heavy/derived fields from previous (content/enrich/llm).
                merged = dict(prev_doc)  # start from previous snapshot (has ko_content_flat etc.)
                merged.update(cleaned)   # ensure current core metadata is up-to-date
                merged["_orig_id"] = logical_layer_id

                if os.getenv("LOG_LEVEL", "").upper() == "DEBUG":
                    logging.debug("[ReuseUnchanged] llid=%s ko_updated_at=%s", logical_layer_id, cleaned.get("ko_updated_at"))

                dl_fp, enr_fp, imp_fp = _compute_fps(merged)
                merged["_dl_fp"] = dl_fp
                merged["_enricher_fp"] = enr_fp
                merged["_improver_fp"] = imp_fp

                prev_dl_fp = None
                if prev_index and isinstance(prev_index.get(logical_layer_id), dict):
                    prev_dl_fp = prev_index[logical_layer_id].get("_dl_fp")
                source_changed = True if prev_dl_fp is None else (prev_dl_fp != dl_fp)

                return merged, None, None, "emitted", None, source_changed, dl_fp, enr_fp, imp_fp

        # --- classify URL-only first (skip content API; create URL task) ---
        url_only, url_list = is_url_based_ko(doc)

        # Fallback: if @id is a URL and not hosted
        if not url_only and not ko_is_hosted and _looks_like_url(ko_id_url):
            url_only = True
            url_list = [ko_id_url]

        if url_only:
            target_url = (url_list[0] if url_list else None) or ko_id_url

            cleaned["first_url"] = target_url
            cleaned["is_url_only"] = True

            combined = combine_metadata_and_content(cleaned, [])
            combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)
            combined["_orig_id"] = logical_layer_id

            if target_url and is_youtube_url(target_url):
                url_task = {
                    "kind": "youtube",
                    "logical_layer_id": logical_layer_id,
                    "at_id": combined.get("@id"),
                    "title": combined.get("title"),
                    "url": target_url,
                    "route": "youtube:disabled",
                }

                dl_fp, enr_fp, imp_fp = _compute_fps(combined)
                combined["_dl_fp"] = dl_fp
                combined["_enricher_fp"] = enr_fp
                combined["_improver_fp"] = imp_fp

                prev_dl_fp = None
                if prev_index and isinstance(prev_index.get(logical_layer_id), dict):
                    prev_dl_fp = prev_index[logical_layer_id].get("_dl_fp")
                source_changed = True if prev_dl_fp is None else (prev_dl_fp != dl_fp)

                return combined, url_task, None, "emitted", None, source_changed, dl_fp, enr_fp, imp_fp

            # Normal URL extraction branch (PageSense)
            url_task = {
                "kind": "url",
                "logical_layer_id": logical_layer_id,
                "at_id": cleaned.get("@id"),
                "title": cleaned.get("title"),
                "url": target_url,
                "route": "pagesense",
            }

            dl_fp, enr_fp, imp_fp = _compute_fps(combined)
            combined["_dl_fp"] = dl_fp
            combined["_enricher_fp"] = enr_fp
            combined["_improver_fp"] = imp_fp

            prev_dl_fp = None
            if prev_index and isinstance(prev_index.get(logical_layer_id), dict):
                prev_dl_fp = prev_index[logical_layer_id].get("_dl_fp")

            source_changed = True if prev_dl_fp is None else (prev_dl_fp != dl_fp)

            return combined, url_task, None, "emitted", None, source_changed, dl_fp, enr_fp, imp_fp

        # 2) Hosted file KO: decide media transcribe vs content API
        if ko_is_hosted:
            # If hosted file is media, create media task and STOP here
            if _is_media_any(mimetype):
                cleaned["is_media"] = True
                cleaned["media_mimetype"] = mimetype
                cleaned["media_url"] = hosted_url  # best effort
                # Optional: keep first_url aligned for downstream logs/debug
                cleaned["first_url"] = hosted_url

                combined = combine_metadata_and_content(cleaned, [])
                combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)
                combined["_orig_id"] = logical_layer_id

                media_task = {
                    "kind": "media",
                    "logical_layer_id": logical_layer_id,
                    "at_id": combined.get("@id"),
                    "title": combined.get("title"),
                    "media_url": hosted_url,
                    "mimetype": mimetype,
                    "route": "transcribe",
                }

                dl_fp, enr_fp, imp_fp = _compute_fps(combined)
                combined["_dl_fp"] = dl_fp
                combined["_enricher_fp"] = enr_fp
                combined["_improver_fp"] = imp_fp

                prev_dl_fp = None
                if prev_index and isinstance(prev_index.get(logical_layer_id), dict):
                    prev_dl_fp = prev_index[logical_layer_id].get("_dl_fp")
                source_changed = True if prev_dl_fp is None else (prev_dl_fp != dl_fp)

                return combined, None, media_task, "emitted", None, source_changed, dl_fp, enr_fp, imp_fp


        # --- hosted content fetch via content API ---
        content_list = get_ko_content(backend_cfg, logical_layer_id)
        combined = combine_metadata_and_content(cleaned, content_list)
        combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)
        combined["_orig_id"] = logical_layer_id

        dl_fp, enr_fp, imp_fp = _compute_fps(combined)
        combined["_dl_fp"] = dl_fp
        combined["_enricher_fp"] = enr_fp
        combined["_improver_fp"] = imp_fp

        prev_dl_fp = None
        if prev_index and isinstance(prev_index.get(logical_layer_id), dict):
            prev_dl_fp = prev_index[logical_layer_id].get("_dl_fp")

        source_changed = True if prev_dl_fp is None else (prev_dl_fp != dl_fp)

        return combined, None, None, "emitted", None, source_changed, dl_fp, enr_fp, imp_fp

    except Exception as e:
        logging.exception("prepare_one_doc failed for _id=%r: %s", doc.get("_id"), e)
        return _drop("exception", cleaned if "cleaned" in locals() else None)


# ---------------- Page processing ----------------

def _process_page(
    backend_cfg: BackendCfg,
    kos_page: List[Dict[str, Any]],
    *,
    workers: int,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    int,
    int,
    Dict[str, int],
    List[Dict[str, Any]],
    int,  # changed_count
    int,  # unchanged_count
]:

    """
    Process one page:
      - filter to published
      - batch-fetch projects for this page
      - parallel prepare_one_doc (includes hosted content fetch)
    Returns:
      (docs, url_tasks, media_tasks, emitted_count, dropped_count)
    """
    if not kos_page:
        return [], [], [], 0, 0, {}, [], 0, 0

    # filter published
    before = len(kos_page)
    kos_page = [d for d in kos_page if str(d.get("status", "")).strip().lower() == "published"]
    skipped_non_published = before - len(kos_page)

    # collect unique project ids (order-preserving)
    proj_ids: List[str] = []
    for ko in kos_page:
        pid = ko.get("project_id")
        if pid:
            proj_ids.append(str(pid))
    seen = set()
    unique_proj_ids = [x for x in proj_ids if not (x in seen or seen.add(x))]

    projects_index = fetch_projects_api(backend_cfg, unique_proj_ids)

    docs_out: List[Dict[str, Any]] = []
    url_tasks: List[Dict[str, Any]] = []
    media_tasks: List[Dict[str, Any]] = []
    emitted = 0
    dropped = 0
    changed_count = 0
    unchanged_count = 0

    drop_reasons: Dict[str, int] = {}
    dropped_items: List[Dict[str, Any]] = []

    max_workers = workers if workers > 0 else int(os.getenv("DL_MAX_WORKERS", "10"))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        skip_unchanged = os.getenv("DL_SKIP_UNCHANGED_CONTENT", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        futs = [
            ex.submit(
                prepare_one_doc,
                backend_cfg,
                doc,
                projects_index,
                prev_index=prev_index,
                skip_content_if_unchanged=skip_unchanged,
            )
            for doc in kos_page
        ]

        for fut in as_completed(futs):
            doc_out, url_task, media_task, status, drop_info, changed, dl_fp, enr_fp, imp_fp = fut.result()
            if status != "emitted" or not doc_out:
                if drop_info:
                    dropped_items.append(drop_info)

                dropped += 1
                # count reason
                if isinstance(status, str) and status.startswith("drop:"):
                    drop_reasons[status] = drop_reasons.get(status, 0) + 1
                else:
                    drop_reasons["drop:unknown"] = drop_reasons.get("drop:unknown", 0) + 1
                continue
            emitted += 1
            docs_out.append(doc_out)

            if changed:
                changed_count += 1
            else:
                unchanged_count += 1

            if url_task:
                url_tasks.append(url_task)
            if media_task:
                media_tasks.append(media_task)

    # Count non-published as dropped as well (they were removed before threading)
    dropped += skipped_non_published

    if skipped_non_published:
        drop_reasons["drop:not_published_prefilter"] = drop_reasons.get("drop:not_published_prefilter",
                                                                        0) + skipped_non_published

    # return docs_out, url_tasks, media_tasks, emitted, dropped
    return (
        docs_out,
        url_tasks,
        media_tasks,
        emitted,
        dropped,
        drop_reasons,
        dropped_items,
        changed_count,
        unchanged_count,
    )


# ---------------- Public API ----------------

def download_and_prepare(
    *,
    env_mode: str,
    page_size: int,
    sort_criteria: int = 1,
    max_workers: int = 10,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> DownloadResult:
    """
    End-to-end Step 1:
      - fetch all pages from KO documents endpoint
      - clean + enrich + classify
      - fetch hosted content (internal API) for hosted docs
    """
    backend_cfg = load_backend_cfg(env_mode)

    page = 1
    pages_seen = set()

    emitted_total = 0
    dropped_total = 0
    url_task_total = 0
    media_task_total = 0
    changed_total = 0
    unchanged_total = 0

    docs_all: List[Dict[str, Any]] = []
    url_tasks_all: List[Dict[str, Any]] = []
    media_tasks_all: List[Dict[str, Any]] = []

    t0 = time.perf_counter()

    while True:
        if page in pages_seen:
            logging.warning("Breaking due to repeated page indicator: %s", page)
            break
        pages_seen.add(page)

        t_page = time.perf_counter()

        t_fetch = time.perf_counter()

        payload = fetch_ko_metadata_api(
            backend_cfg,
            limit=page_size,
            page=page,
            sort_criteria=sort_criteria,
        )

        dt_fetch = time.perf_counter() - t_fetch
        if dt_fetch > float(os.getenv("SLOW_PAGE_FETCH_SEC", "1.0")):
            logging.warning("[SlowPageFetch] env=%s page=%s dt=%.2fs", env_mode, page, dt_fetch)

        kos_page = payload.get("data", []) or payload.get("results", []) or []
        if not kos_page:
            break

        pagination = payload.get("pagination") or {}
        next_page = pagination.get("next_page")

        logging.info("[KO API] env=%s page=%s fetched=%s next=%r", env_mode, page, len(kos_page), next_page)

        docs_p, url_p, media_p, emitted_p, dropped_p, drop_reasons, dropped_items, changed_p, unchanged_p = _process_page(
            backend_cfg,
            kos_page,
            workers=max_workers,
            prev_index=prev_index,
        )

        logging.info(
            "[PageDone] env=%s page=%s emitted=%s dropped=%s url_tasks=%s media_tasks=%s dt=%.2fs",
            env_mode, page, emitted_p, dropped_p, len(url_p), len(media_p),
            time.perf_counter() - t_page
        )

        if drop_reasons:
            logging.info("[PageDrops] env=%s page=%s reasons=%s", env_mode, page, drop_reasons)

        max_show = int(os.getenv("DROP_LOG_MAX", "100"))
        if dropped_items:
            total = len(dropped_items)
            show_n = min(max_show, total)

            # One KO per line for readability in logs
            for i, item in enumerate(dropped_items[:show_n], start=1):
                logging.info(
                    "[DroppedKOs] env=%s page=%s showing=%s/%s reason=%r logical_layer_id=%r title=%r",
                    env_mode,
                    page,
                    i,
                    total,
                    item.get("reason"),
                    item.get("logical_layer_id"),
                    item.get("title"),
                )

            # Optional: make it explicit if we truncated the list
            if show_n < total:
                logging.info(
                    "[DroppedKOs] env=%s page=%s truncated (showing %s/%s). Increase DROP_LOG_MAX to see more.",
                    env_mode,
                    page,
                    show_n,
                    total,
                )

        docs_all.extend(docs_p)
        url_tasks_all.extend(url_p)
        media_tasks_all.extend(media_p)

        emitted_total += emitted_p
        dropped_total += dropped_p

        changed_total += changed_p
        unchanged_total += unchanged_p

        url_task_total += len(url_p)
        media_task_total += len(media_p)

        if not next_page:
            break
        try:
            next_page_int = int(next_page)
        except Exception:
            break
        if next_page_int == page:
            break
        page = next_page_int

    elapsed = time.perf_counter() - t0

    stats = {
        "env_mode": env_mode.upper(),
        "emitted": emitted_total,
        "dropped": dropped_total,
        "url_tasks": url_task_total,
        "media_tasks": media_task_total,
        "changed": changed_total,
        "unchanged_reused": unchanged_total,
        "elapsed_sec": round(elapsed, 2),
    }

    logging.warning(
        "[Downloader] env=%s emitted=%s dropped=%s changed=%s unchanged=%s url_tasks=%s media_tasks=%s elapsed=%.2fs",
        stats["env_mode"],
        emitted_total,
        dropped_total,
        changed_total,
        unchanged_total,
        url_task_total,
        media_task_total,
        elapsed,
    )

    return DownloadResult(
        docs=docs_all,
        url_tasks=url_tasks_all,
        media_tasks=media_tasks_all,
        stats=stats,
    )


# ---------------- CLI (optional) ----------------

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()
    ps = int(os.getenv("DL_PAGE_SIZE", "100"))
    ps = max(1, min(ps, 100))
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))

    res = download_and_prepare(
        env_mode=env_mode,
        page_size=ps,
        sort_criteria=sc,
        max_workers=mw,
    )

    # quick local smoke output (don’t do this in production)
    print(json.dumps(res.stats, indent=2, cls=CustomJSONEncoder))

# downloader.py

import json
import logging
import os
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List, Tuple

import requests

from io_helpers import run_stamp, output_dir, atomic_write_json, update_latest_pointer, resolve_latest_pointer
from job_lock import acquire_job_lock, release_job_lock, JobLockHeldError
from downloader_utils import (BackendCfg, DownloadResult, get_session, api_base, load_backend_cfg, KO_CONTENT_MODE,
                              valid_year, is_blank, sha256_obj, stable_str)
from enricher_utils import set_enrich_via
from utils import (CustomJSONEncoder, normalize_date_to_yyyy_mm_dd, strip_html_light, clean_list, get_ko_id,
                   extract_location_names, flatten_ko_content)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


SOURCE_FP_FIELDS = [
    "@id",
    "title",
    "subtitle",
    "description",
    "keywords",
    "creators",
    "languages",
    "date_of_completion",
    "intended_purposes",
    "locations_flat",
    "category",
    "topics",
    "themes",
    "license",
    "project_id",
    "subcategories",
    "project_name",
    "project_acronym",
    "project_url",
    "project_type",
    "_orig_id",
]


# ---------------- Small helpers ----------------
def _stable_value(v: Any) -> Any:
    """
    Convert values into a deterministic, JSON-serialisable form so hashes are stable across runs.

    - str: stripped
    - list: recursively stable + sorted (order-insensitive)
    - dict: recursively stable with sorted keys (via json.dumps in sha256_obj)
    - numbers/bool/None: kept
    - anything else: stringified
    """
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s
    if isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, list):
        # make list order-insensitive for hashing (important for keywords/topics/etc.)
        stable_items = [_stable_value(x) for x in v]
        # remove empty strings / Nones to avoid noise
        stable_items = [x for x in stable_items if x not in (None, "")]
        # sort deterministically (string representation is the safest cross-type key)
        return sorted(stable_items, key=lambda x: str(x).casefold())
    if isinstance(v, dict):
        # keep structure; keys sorted later by sha256_obj via json.dumps(sort_keys=True)
        return {str(k): _stable_value(val) for k, val in v.items()}
    return str(v)

def compute_source_fp(doc: Dict[str, Any]) -> str:
    """
    Fingerprint only the fields we care about for 'did the KO change?'.
    Excludes ko_content_flat so content/extraction drift doesn't masquerade as metadata changes.
    """
    obj = {f: _stable_value(doc.get(f)) for f in SOURCE_FP_FIELDS}
    return sha256_obj(obj)

def compute_content_fp(doc: Dict[str, Any]) -> str:
    """Fingerprint only the flattened content."""
    return sha256_obj(stable_str(doc.get("ko_content_flat")))

def compute_field_hashes(doc: Dict[str, Any]) -> Dict[str, str]:
    """
    Hash a curated set of KO fields (metadata + derived text), per-field.
    """
    fields = [
        "@id",
        "title",
        "subtitle",
        "description",
        "keywords",
        "creators",
        "languages",
        "date_of_completion",
        "intended_purposes",
        "locations_flat",
        "category",
        "topics",
        "themes",
        "license",
        "project_id",
        "subcategories",
        "project_name",
        "project_acronym",
        "project_url",
        "project_type",
        "_orig_id",
        "ko_content_flat",
    ]

    out: Dict[str, str] = {}
    for f in fields:
        v = _stable_value(doc.get(f))
        out[f] = sha256_obj(v)
    return out

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
        # return sorted(out, key=lambda s: s.casefold())
        # OR
        return sorted(dedup_case_insensitive(out), key=lambda s: s.casefold())
    return []

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
        out["ko_external_content_type"] = dm.get("external_content_type")

    out["ko_resource_language"] = first.get("language")

    # drop blanks
    return {k: v for k, v in out.items() if not is_blank(v)}


# def _write_drops_report(
#     *,
#     env_mode: str,
#     output_root: str,
#     run_id: str,
#     page: int,
#     dropped_items: List[Dict[str, Any]],
# ) -> Optional[str]:
#     """
#     Append dropped KO details to a per-run JSONL file.
# 
#     JSONL is ideal for logs: 1 JSON object per line, easy to grep, stream, and parse.
#     Returns the file path as a string (or None if nothing written).
#     """
#     if not dropped_items:
#         return None
# 
#     out_dir = output_dir(env_mode, root=output_root)
#     drops_path = out_dir / f"dropped_kos_{run_id}.jsonl"
# 
#     # Keep lines small-ish but detailed.
#     # We also add page/env/run_id for traceability.
#     lines = []
#     for item in dropped_items:
#         if not isinstance(item, dict):
#             continue
#         rec = dict(item)
#         rec["env_mode"] = env_mode.upper()
#         rec["run_id"] = run_id
#         rec["page"] = page
#         lines.append(json.dumps(rec, ensure_ascii=False, cls=CustomJSONEncoder))
# 
#     # Append mode so each page adds to the same file.
#     drops_path.parent.mkdir(parents=True, exist_ok=True)
#     with drops_path.open("a", encoding="utf-8") as f:
#         for ln in lines:
#             f.write(ln + "\n")
# 
#     return str(drops_path)


def upsert_dropped_kos(
    *,
    env_mode: str,
    output_root: str,
    run_id: str,
    page: int,
    dropped_records: List[Dict[str, Any]],
) -> Optional[str]:
    """
    Maintain ONE JSON file of dropped KOs (dedup by logical_layer_id).
    If a KO already exists, update/overwrite it.
    """
    if not dropped_records:
        return None

    out_dir = output_dir(env_mode, root=output_root)
    path = out_dir / "dropped_kos.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing
    existing: Dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}

    dropped_by_id = existing.get("dropped_by_id")
    if not isinstance(dropped_by_id, dict):
        dropped_by_id = {}

    # Update meta (keep it lightweight and current)
    existing["meta"] = {
        "env_mode": env_mode.upper(),
        "last_run_id": run_id,
        "last_updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stage": "downloader",
    }

    # Upsert each record
    for rec in dropped_records:
        if not isinstance(rec, dict):
            continue

        logical_layer_id = rec.get("logical_layer_id") or rec.get("_orig_id") or rec.get("_id")
        if not isinstance(logical_layer_id, str) or not logical_layer_id.strip():
            continue
        logical_layer_id = logical_layer_id.strip()

        doc_block = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
        if not doc_block:
            continue

        # Inject run/page metadata into the doc itself
        inject = {
            "_drop_run_id": run_id,
            "_drop_page": page,
            "_drop_updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "_drop_env_mode": env_mode.upper(),
        }

        # Rebuild in order so injected fields appear first, then the existing doc fields
        new_doc = dict(inject)
        new_doc.update(doc_block)

        dropped_by_id[logical_layer_id] = new_doc

    existing["dropped_by_id"] = dropped_by_id

    # Atomic write
    atomic_write_json(path, existing)
    return str(path)


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

    if parsed and valid_year(parsed[:4], min_year=1000, max_year=2100):
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

    def _drop(reason: str, cleaned_doc: Optional[Dict[str, Any]] = None, *, details: Optional[Dict[str, Any]] = None) -> \
    Tuple[
        None, None, None, str, Dict[str, Any], bool, None, None, None
    ]:
        logical_layer_id = str(doc.get("_id", "")).strip() or None

        full_doc = dict(cleaned_doc) if isinstance(cleaned_doc, dict) else dict(doc)
        if logical_layer_id and isinstance(full_doc, dict):
            full_doc["_orig_id"] = logical_layer_id

        # Add drop metadata *into* the doc (in this order)
        drop_meta: Dict[str, Any] = {
            "_drop_reason": f"drop:{reason}",
        }
        if isinstance(details, dict):
            drop_meta["_drop_details"] = details

        # Put drop_meta "just above _id" by rebuilding dict in order
        ordered_doc = dict(drop_meta)
        ordered_doc.update(full_doc)

        payload = {
            "logical_layer_id": logical_layer_id,
            "doc": ordered_doc,
        }

        return (
            None, None, None,
            f"drop:{reason}",
            payload,
            True,
            None, None, None
        )

    cleaned: Optional[Dict[str, Any]] = None

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
        # if is_blank(cleaned.get("date_of_completion")):
        #     return _drop(
        #         "missing_date_of_completion",
        #         cleaned,
        #         details={"message": "Missing required field: date_of_completion"},
        #     )

        # pre-enrichment required fields
        required_pre = ["project_id", "@id", "date_of_completion"]
        missing_pre = [f for f in required_pre if is_blank(cleaned.get(f))]
        if missing_pre:
            pretty = ", ".join(missing_pre)

            return _drop(
                "missing_required_pre",
                cleaned,
                details={
                    "missing_pre_fields": missing_pre,
                    "message": f"Missing required fields before enrichment: {pretty}",
                },
            )

        # enrich with project
        cleaned = enrich_with_project(cleaned, projects_index)

        # post-enrichment required fields
        required_post = ["project_name", "project_acronym"]
        missing_post = [f for f in required_post if is_blank(cleaned.get(f))]
        if missing_post:
            return _drop(
                "missing_project_fields",
                cleaned,
                details={
                    "missing_post_fields": missing_post,
                    "message": f"Missing required fields after enrichment: {', '.join(missing_post)}",
                },
            )

        # logical layer id
        logical_layer_id = str(doc.get("_id", "")).strip()
        if not logical_layer_id:
            return _drop("missing_logical_layer_id", cleaned)

        cleaned["_orig_id"] = logical_layer_id

        def _ensure_stage_flags(d: Dict[str, Any]) -> None:
            d.setdefault("enriched", 0)
            d.setdefault("improved", 0)

        def _compute_fps(cleaned_doc: Dict[str, Any]) -> Tuple[str, str, str]:
            # ---- Improver inputs ----
            improver_obj = {
                "title": stable_str(cleaned_doc.get("title")),
                "subtitle": stable_str(cleaned_doc.get("subtitle")),
                "description": stable_str(cleaned_doc.get("description")),
                "keywords": _stable_list(cleaned_doc.get("keywords")),
                "ko_content_flat": stable_str(cleaned_doc.get("ko_content_flat")),
            }
            improver_fp = sha256_obj(improver_obj)

            # ---- Enricher inputs (only identity of external targets) ----
            enricher_obj = {
                "policy": {
                    "router_version": stable_str(os.getenv("ENRICHER_ROUTER_VERSION", "1")),
                    "enable_url_extract": stable_str(os.getenv("ENRICH_ENABLE_URL_EXTRACT", "1")),
                    "enable_transcribe": stable_str(os.getenv("ENRICH_ENABLE_TRANSCRIBE", "0")),
                    "enable_deapi_transcribe": stable_str(os.getenv("ENRICH_ENABLE_DEAPI_TRANSCRIBE", "0")),
                    "deapi_model": stable_str(os.getenv("DEAPI_TRANSCRIBE_MODEL", "WhisperLargeV3")),
                }
            }
            enricher_fp = sha256_obj(enricher_obj)

            # ---- Downloader/source fingerprint (bigger picture) ----
            dl_obj = {
                "@id": stable_str(cleaned_doc.get("@id")),
                "project_id": stable_str(cleaned_doc.get("project_id")),
                "project_acronym": stable_str(cleaned_doc.get("project_acronym")),
                "project_name": stable_str(cleaned_doc.get("project_name")),
                "date_of_completion": stable_str(cleaned_doc.get("date_of_completion")),
                "topics": _stable_list(cleaned_doc.get("topics")),
                "themes": _stable_list(cleaned_doc.get("themes")),
                "license": stable_str(cleaned_doc.get("license")),
                "ko_file_id": stable_str(cleaned_doc.get("ko_file_id")),
                "ko_object_mimetype": stable_str(cleaned_doc.get("ko_object_mimetype")),
                "ko_object_size": cleaned_doc.get("ko_object_size"),
                "ko_updated_at": stable_str(cleaned_doc.get("ko_updated_at")),
                "proj_updated_at": stable_str(cleaned_doc.get("proj_updated_at")),
                "ko_content_flat": stable_str(cleaned_doc.get("ko_content_flat")),
            }
            dl_fp = sha256_obj(dl_obj)

            return dl_fp, enricher_fp, improver_fp

        # ---------------- Source fingerprint gate (primary truth) ----------------
        # Compute a fingerprint of exactly the "fields we care about".
        cur_source_fp = compute_source_fp(cleaned)

        prev_doc = prev_index.get(logical_layer_id) if prev_index else None
        prev_source_fp = prev_doc.get("_source_fp") if isinstance(prev_doc, dict) else None

        source_changed = True if prev_source_fp is None else (prev_source_fp != cur_source_fp)

        # If the KO is unchanged by our definition, reuse previous snapshot (including heavy fields),
        # and skip the content API call.
        if (not source_changed) and isinstance(prev_doc, dict) and skip_content_if_unchanged:
            merged = dict(prev_doc)  # keep previous ko_content_flat + enriched/improved outputs
            merged.update(cleaned)   # refresh normalised metadata fields
            merged["_orig_id"] = logical_layer_id

            set_enrich_via(merged)
            _ensure_stage_flags(merged)

            # Store fingerprints on the document
            merged["_source_fp"] = cur_source_fp
            merged["_content_fp"] = compute_content_fp(merged)

            # --- per-field hashes (for "which fields changed") ---
            merged["_field_hashes"] = compute_field_hashes(merged)

            prev_hashes = prev_doc.get("_field_hashes") if isinstance(prev_doc, dict) else None
            cur_hashes = merged["_field_hashes"]

            changed_fields: List[str] = []
            if isinstance(prev_hashes, dict):
                for k, v in cur_hashes.items():
                    if prev_hashes.get(k) != v:
                        changed_fields.append(k)
            else:
                # no previous hashes available -> treat all as changed
                changed_fields = list(cur_hashes.keys())

            merged["_changed_fields"] = changed_fields
            merged["_changed_field_count"] = len(changed_fields)

            return merged, None, None, "emitted", None, False, None, None, None

        # --- hosted content fetch via content API ---
        content_list = get_ko_content(backend_cfg, logical_layer_id)
        combined = combine_metadata_and_content(cleaned, content_list)
        combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)
        combined["_orig_id"] = logical_layer_id

        set_enrich_via(combined)

        _ensure_stage_flags(combined)

        # --- fingerprints + per-field hashes ---
        combined["_source_fp"] = cur_source_fp
        combined["_content_fp"] = compute_content_fp(combined)

        combined["_field_hashes"] = compute_field_hashes(combined)

        prev_hashes = prev_doc.get("_field_hashes") if isinstance(prev_doc, dict) else None
        cur_hashes = combined["_field_hashes"]

        changed_fields: List[str] = []
        if isinstance(prev_hashes, dict):
            for k, v in cur_hashes.items():
                if prev_hashes.get(k) != v:
                    changed_fields.append(k)
        else:
            changed_fields = list(cur_hashes.keys())

        combined["_changed_fields"] = changed_fields
        combined["_changed_field_count"] = len(changed_fields)

        return combined, None, None, "emitted", None, source_changed, None, None, None

    except Exception as e:
        logging.exception("prepare_one_doc failed for _id=%r: %s", doc.get("_id"), e)
        return _drop("exception", cleaned)


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
    List[str],  # changed_ids
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
        return [], [], [], 0, 0, {}, [], 0, 0, []

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
    changed_ids: List[str] = []

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
            doc_out, url_task, media_task, status, drop_info, changed, _, _, _ = fut.result()
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
                # Prefer _orig_id for stability, fall back to @id
                cid = doc_out.get("_orig_id") or doc_out.get("@id")
                if isinstance(cid, str) and cid.strip():
                    changed_ids.append(cid.strip())
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
        changed_ids,
    )


# ---------------- Public API ----------------

def download_and_prepare(
    *,
    env_mode: str,
    page_size: int,
    sort_criteria: int = 1,
    max_workers: int = 10,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
    use_lock: bool = True
) -> DownloadResult:
    """
    End-to-end Step 1:
      - fetch all pages from KO documents endpoint
      - clean + enrich + classify
      - fetch hosted content (internal API) for hosted docs
    """
    output_root = os.getenv("OUTPUT_ROOT", "output")
    lock = None
    if use_lock:
        lock = acquire_job_lock(env_mode=env_mode, output_root=output_root, entrypoint="downloader")

    try:
        backend_cfg = load_backend_cfg(env_mode)

        run_id = run_stamp()

        page = 1
        pages_seen = set()

        emitted_total = 0
        dropped_total = 0
        url_task_total = 0
        media_task_total = 0
        changed_total = 0
        unchanged_total = 0
        changed_ids_all: List[str] = []

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

            # Always log as a metric at INFO (keeps WARN meaningful)
            logging.info("[PageFetch] env=%s Page=%s dt=%.2fs", env_mode, page, dt_fetch)

            # Only WARN if it’s *actually* slow
            warn_thr = float(os.getenv("PAGE_FETCH_WARN_SEC", "5.0"))
            if dt_fetch > warn_thr:
                logging.warning("[PageFetchSlow] env=%s Page=%s dt=%.2fs thr=%.2fs", env_mode, page, dt_fetch, warn_thr)

            kos_page = payload.get("data", []) or payload.get("results", []) or []
            if not kos_page:
                break

            pagination = payload.get("pagination") or {}
            next_page = pagination.get("next_page")

            logging.info("[KO API] env=%s Page=%s fetched=%s next=%r", env_mode, page, len(kos_page), next_page)

            docs_p, url_p, media_p, emitted_p, dropped_p, drop_reasons, dropped_items, changed_p, unchanged_p, changed_ids_p = _process_page(
                backend_cfg,
                kos_page,
                workers=max_workers,
                prev_index=prev_index,
            )

            logging.info(
                "[PageDone] env=%s Page=%s Emitted=%s Dropped=%s url_tasks=%s media_tasks=%s dt=%.2fs",
                env_mode, page, emitted_p, dropped_p, len(url_p), len(media_p),
                time.perf_counter() - t_page
            )

            if drop_reasons:
                logging.info("[PageDrops] env=%s Page=%s Reasons=%s", env_mode, page, drop_reasons)

            max_show = int(os.getenv("DROP_LOG_MAX", "100"))
            if dropped_items:
                total = len(dropped_items)
                show_n = min(max_show, total)

                # One KO per line for readability in logs
                for i, item in enumerate(dropped_items[:show_n], start=1):
                    doc = item.get("doc") if isinstance(item, dict) else None
                    doc = doc if isinstance(doc, dict) else {}

                    details = doc.get("_drop_details") if isinstance(doc.get("_drop_details"), dict) else {}
                    missing_pre = details.get("missing_pre_fields")

                    logging.info(
                        "[DroppedKOs] env=%s Page=%s showing=%s/%s Reason=%r missing_pre_fields=%r logical_layer_id=%r title=%r",
                        env_mode,
                        page,
                        i,
                        total,
                        doc.get("_drop_reason"),
                        missing_pre,
                        item.get("logical_layer_id"),
                        doc.get("title"),
                    )

                # Optional: make it explicit if we truncated the list
                if show_n < total:
                    logging.info(
                        "[DroppedKOs] env=%s Page=%s truncated (showing %s/%s). Increase DROP_LOG_MAX to see more.",
                        env_mode,
                        page,
                        show_n,
                        total,
                    )

                drops_path = upsert_dropped_kos(
                    env_mode=env_mode,
                    output_root=output_root,
                    run_id=run_id,
                    page=page,
                    dropped_records=dropped_items,
                )
                if drops_path:
                    logging.info("[DroppedKOsFile] Updated=%s Count Added or Updated=%s", drops_path,
                                 len(dropped_items))

            docs_all.extend(docs_p)
            url_tasks_all.extend(url_p)
            media_tasks_all.extend(media_p)

            emitted_total += emitted_p
            dropped_total += dropped_p

            changed_total += changed_p
            unchanged_total += unchanged_p

            changed_ids_all.extend(changed_ids_p)

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
            "[Downloader] env=%s Emitted=%s Dropped=%s Changed=%s unChanged=%s url_tasks=%s media_tasks=%s elapsed=%.2fs",
            stats["env_mode"],
            emitted_total,
            dropped_total,
            changed_total,
            unchanged_total,
            url_task_total,
            media_task_total,
            elapsed,
        )

        # ---------------- End-of-run change summary ----------------
        if changed_total > 0:
            # de-dup but preserve order
            seen = set()
            uniq = []
            for x in changed_ids_all:
                if x not in seen:
                    uniq.append(x)
                    seen.add(x)

            max_show = int(os.getenv("DL_CHANGED_IDS_MAX", "25"))
            show = uniq[:max_show]

            logging.warning("[DownloaderSummary] Changed=%s Unchanged Reused=%s", changed_total, unchanged_total)
            logging.warning("[DownloaderSummary] Changed IDs=%s", ", ".join(show))
            if len(uniq) > max_show:
                logging.warning("[DownloaderSummary] Changed IDs Truncated Total=%s Shown=%s", len(uniq), max_show)
        else:
            logging.warning("[DownloaderSummary] No changes detected. Emitted=%s Dropped=%s", emitted_total, dropped_total)
            logging.warning("[DownloaderSummary] Unchanged Reused=%s", unchanged_total)

        # --- Optional: persist downloader output for independent stage runs ---
        should_write_output = os.getenv("DL_WRITE_OUTPUT", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        prev_count = len(prev_index) if prev_index is not None else None
        has_meaningful_changes = (changed_total > 0) or (prev_count is not None and len(docs_all) != prev_count)

        if should_write_output and has_meaningful_changes:
            run_id = run_stamp()

            out_dir = output_dir(env_mode, root=output_root)
            out_path = out_dir / f"final_output_{run_id}.json"

            payload = {
                "meta": {
                    "env_mode": env_mode.upper(),
                    "run_id": run_id,
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "stage": "downloader",
                },
                "stats": stats,
                "docs": docs_all,
                "url_tasks": url_tasks_all,
                "media_tasks": media_tasks_all,
            }

            atomic_write_json(out_path, payload)
            update_latest_pointer(env_mode, output_root, "latest_downloaded.json", out_path)

        elif should_write_output and not has_meaningful_changes:
            logging.warning("[Downloader] No changes detected (skipping writing of final_output)")


        return DownloadResult(
            docs=docs_all,
            url_tasks=url_tasks_all,
            media_tasks=media_tasks_all,
            stats=stats,
        )
    finally:
        if lock is not None:
            release_job_lock(lock)


# ---------------- CLI (optional) ----------------

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))

    env_mode = (os.getenv("ENV_MODE") or "DEV").upper()
    ps = int(os.getenv("DL_PAGE_SIZE", "100"))
    ps = max(1, min(ps, 100))
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))

    output_root = os.getenv("OUTPUT_ROOT", "output")
    prev_path = resolve_latest_pointer(env_mode, output_root, "latest_downloaded.json")
    prev_index = {}
    if prev_path:
        try:
            payload = json.loads(prev_path.read_text(encoding="utf-8"))
            docs = payload.get("docs", []) if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])

            prev_docs_len = len(docs) if isinstance(docs, list) else 0

            if isinstance(docs, list):
                tmp: Dict[str, Dict[str, Any]] = {}
                for d in docs:
                    if not isinstance(d, dict):
                        continue
                    k = d.get("_orig_id") or d.get("_id")
                    if isinstance(k, str) and k.strip():
                        tmp[k.strip()] = d
                prev_index = tmp

        except Exception:
            prev_index = {}

    try:
        res = download_and_prepare(
            env_mode=env_mode,
            page_size=ps,
            sort_criteria=sc,
            max_workers=mw,
            prev_index=prev_index,
        )
    except JobLockHeldError as e:
        # Expected case: show friendly message, no traceback.
        print(str(e))
        raise SystemExit(2)

    # quick local smoke output (don’t do this in production)
    print(json.dumps(res.stats, indent=2, cls=CustomJSONEncoder))

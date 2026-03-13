from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from common.utils import (normalize_date_to_yyyy_mm_dd, strip_html_light, clean_list, get_ko_id,
                          extract_location_names, flatten_ko_content)
from pipeline.io import atomic_write_json, output_dir
from stages.downloader.fingerprints import (compute_content_fp, compute_field_hashes, compute_source_fp,
                                            dedup_case_insensitive, stable_list)
from stages.downloader.utils import (BackendCfg, KO_CONTENT_MODE, api_base, get_session, is_blank, load_backend_cfg,
                                     sha256_obj, stable_str, valid_year)
from stages.enricher.utils import set_enrich_via


logger = logging.getLogger(__name__)


def extract_first_resource_file_info(doc: Dict[str, Any]) -> Dict[str, Any]:
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
    return {k: v for k, v in out.items() if not is_blank(v)}


def upsert_dropped_kos(
    *,
    env_mode: str,
    output_root: str,
    run_id: str,
    page: int,
    dropped_records: List[Dict[str, Any]],
) -> Optional[str]:
    if not dropped_records:
        return None

    out_dir = output_dir(env_mode, root=output_root)
    path = out_dir / "dropped_kos.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    existing: Dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}

    dropped_by_id = existing.get("dropped_by_id")
    if not isinstance(dropped_by_id, dict):
        dropped_by_id = {}

    existing["meta"] = {
        "env_mode": env_mode.upper(),
        "last_run_id": run_id,
        "last_updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stage": "downloader",
    }

    for rec in dropped_records:
        if not isinstance(rec, dict):
            continue
        logical_layer_id = rec.get("logical_layer_id") or rec.get("_orig_id") or rec.get("_id")
        if not isinstance(logical_layer_id, str) or not logical_layer_id.strip():
            continue

        doc_block = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
        if not doc_block:
            continue

        inject = {
            "_drop_run_id": run_id,
            "_drop_page": page,
            "_drop_updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "_drop_env_mode": env_mode.upper(),
        }
        new_doc = dict(inject)
        new_doc.update(doc_block)
        dropped_by_id[logical_layer_id.strip()] = new_doc

    existing["dropped_by_id"] = dropped_by_id
    atomic_write_json(path, existing)
    return str(path)


def fetch_ko_metadata_api(
    backend_cfg: BackendCfg,
    *,
    limit: Optional[int] = None,
    page: int = 1,
    sort_criteria: int = 1,
) -> Dict[str, Any]:
    url = f"{api_base(backend_cfg)}/api/logical_layer/documents"
    params: Dict[str, Any] = {"page": page, "sort_criteria": sort_criteria}
    if limit is not None:
        params["limit"] = limit
    timeout = int(os.getenv("DL_HTTP_TIMEOUT", "30"))
    s = get_session(backend_cfg, timeout=timeout)
    try:
        r = s.post(url, params=params, json={})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(
            "[KOFetchError] url=%s page=%s limit=%s sort=%s timeout=%ss err=%r",
            url,
            page,
            limit,
            sort_criteria,
            timeout,
            e,
        )
        raise


def fetch_projects_api(backend_cfg: BackendCfg, project_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not project_ids:
        return {}
    url = f"{api_base(backend_cfg)}/api/logical_layer/projects/"
    timeout = int(os.getenv("DL_HTTP_TIMEOUT", "30"))
    s = get_session(backend_cfg, timeout=timeout)
    try:
        r = s.post(url, json=project_ids)
        r.raise_for_status()
        items = r.json() or []
        return {str(p.get("_id")): p for p in items if isinstance(p, dict)}
    except Exception as e:
        logger.error(
            "[ProjectFetchError] url=%s project_count=%s timeout=%ss err=%r",
            url,
            len(project_ids),
            timeout,
            e,
        )
        raise


def get_ko_content(backend_cfg: BackendCfg, document_id: str) -> List[dict]:
    t0 = time.perf_counter()
    url = f"{api_base(backend_cfg)}/api/nlp/ko_content_document"
    s = get_session(backend_cfg, timeout=int(os.getenv("CONTENT_HTTP_TIMEOUT", "30")))

    try:
        r = s.get(url, params={"document_id": document_id})
        if r.status_code == 404:
            _log_slow_content(document_id, t0)
            return []
        r.raise_for_status()
        body = r.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        logger.error("Content API error for %s: HTTP %s %s", document_id, status, e)
        _log_slow_content(document_id, t0)
        return []
    except Exception as e:
        logger.error("Content API error for %s: %s", document_id, e)
        _log_slow_content(document_id, t0)
        return []

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
            normalised.append(el)
        elif isinstance(el, str):
            normalised.append({"content_text": el})

    _log_slow_content(document_id, t0)
    return normalised


def _log_slow_content(document_id: str, started_at: float) -> None:
    dt = time.perf_counter() - started_at
    thr = float(os.getenv("SLOW_CONTENT_SEC", "2.0"))
    if dt > thr:
        logger.warning("[SlowContent] id=%s dt=%.2fs", document_id, dt)


def enrich_with_project(ko_doc: Dict[str, Any], projects_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
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
    fields_to_exclude = {
        "schema_version", "@context", "_tags", "object_hash", "uploaded_by", "knowledge_object_version",
        "created_by", "updated_by", "version", "contributor_custom_metadata", "physical_layer_ko_metadata_id",
        "status", "object_metadata", "language_versions", "otherFields", "knowledge_object_resources", "collection",
    }
    cleaned = {k: v for k, v in doc.items() if k not in fields_to_exclude}
    cleaned.update(extract_first_resource_file_info(doc))

    date_candidates = [
        ("date_of_completion", doc.get("date_of_completion")),
        ("dateCreated", doc.get("dateCreated")),
        ("date", doc.get("date")),
        ("created_ts", doc.get("created_ts")),
        ("updated_ts", doc.get("updated_ts")),
    ]
    parsed = None
    parsed_source = None
    for source_name, raw_date in date_candidates:
        if not raw_date:
            continue
        try:
            candidate = normalize_date_to_yyyy_mm_dd(raw_date)
        except Exception:
            candidate = None
        if candidate and valid_year(candidate[:4], min_year=1000, max_year=2100):
            parsed = candidate
            parsed_source = source_name
            break
    if parsed:
        cleaned["date_of_completion"] = parsed
        cleaned["date_of_completion_source"] = parsed_source
        cleaned.pop("date_of_completion_missing", None)
    else:
        cleaned.pop("date_of_completion", None)
        cleaned["date_of_completion_missing"] = True
        cleaned["date_of_completion_source"] = "missing"

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

    if "created_ts" in cleaned:
        cleaned["ko_created_at"] = cleaned.pop("created_ts")
    if "updated_ts" in cleaned:
        cleaned["ko_updated_at"] = cleaned.pop("updated_ts")
    cleaned.pop("project_details", None)
    return cleaned


def combine_metadata_and_content(metadata: Dict[str, Any], content_list: List[dict]) -> Dict[str, Any]:
    d = dict(metadata)
    d["ko_content"] = content_list
    return d


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
    str,
    Optional[str],
    Optional[str],
    Optional[str],
]:
    def _drop(reason: str, cleaned_doc: Optional[Dict[str, Any]] = None, *, details: Optional[Dict[str, Any]] = None):
        logical_layer_id = str(doc.get("_id", "")).strip() or None
        full_doc = dict(cleaned_doc) if isinstance(cleaned_doc, dict) else dict(doc)
        if logical_layer_id and isinstance(full_doc, dict):
            full_doc["_orig_id"] = logical_layer_id

        drop_meta: Dict[str, Any] = {"_drop_reason": f"drop:{reason}"}
        if isinstance(details, dict):
            drop_meta["_drop_details"] = details
        ordered_doc = dict(drop_meta)
        ordered_doc.update(full_doc)
        payload = {"logical_layer_id": logical_layer_id, "doc": ordered_doc}
        return None, None, None, f"drop:{reason}", payload, "dropped", None, None, None

    cleaned: Optional[Dict[str, Any]] = None

    try:
        if str(doc.get("status", "")).strip().lower() != "published":
            return _drop("not_published", None)

        cleaned = clean_ko_metadata(doc)
        ko_id = get_ko_id(doc) or get_ko_id(cleaned)
        if ko_id:
            cleaned["@id"] = ko_id

        loc_flat = extract_location_names(doc.get("locations", []))
        if loc_flat:
            cleaned["locations_flat"] = loc_flat

        cleaned["keywords"] = clean_list(cleaned.get("keywords"), item_cleaner=strip_html_light)
        cleaned["languages"] = clean_list(cleaned.get("languages"), item_cleaner=strip_html_light)
        cleaned["locations_flat"] = clean_list(cleaned.get("locations_flat"), item_cleaner=strip_html_light)
        subcats = cleaned.get("subcategories", cleaned.get("subcategory"))
        cleaned["subcategories"] = clean_list(subcats, item_cleaner=strip_html_light)
        cleaned.pop("subcategory", None)
        cleaned["topics"] = clean_list(cleaned.get("topics"), item_cleaner=strip_html_light)
        cleaned["themes"] = clean_list(cleaned.get("themes"), item_cleaner=strip_html_light)
        cleaned["creators"] = clean_list(cleaned.get("creators"), item_cleaner=strip_html_light)

        cleaned["keywords"] = dedup_case_insensitive(cleaned.get("keywords"))
        cleaned["languages"] = dedup_case_insensitive(cleaned.get("languages"))
        cleaned["locations_flat"] = dedup_case_insensitive(cleaned.get("locations_flat"))
        cleaned["subcategories"] = dedup_case_insensitive(cleaned.get("subcategories"))
        cleaned["topics"] = dedup_case_insensitive(cleaned.get("topics"))
        cleaned["themes"] = dedup_case_insensitive(cleaned.get("themes"))
        cleaned["creators"] = dedup_case_insensitive(cleaned.get("creators"))

        required_pre = ["project_id", "@id"]
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

        cleaned = enrich_with_project(cleaned, projects_index)

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

        logical_layer_id = str(doc.get("_id", "")).strip()
        if not logical_layer_id:
            return _drop("missing_logical_layer_id", cleaned)

        cleaned["_orig_id"] = logical_layer_id
        prev_doc = prev_index.get(logical_layer_id) if prev_index else None
        cur_source_fp = compute_source_fp(cleaned)
        prev_source_fp = prev_doc.get("_source_fp") if isinstance(prev_doc, dict) else None
        source_changed = True if prev_source_fp is None else (prev_source_fp != cur_source_fp)

        if (not source_changed) and isinstance(prev_doc, dict) and skip_content_if_unchanged:
            merged = dict(prev_doc)
            merged.update(cleaned)
            merged["_orig_id"] = logical_layer_id
            set_enrich_via(merged)
            _ensure_stage_flags(merged)
            merged["_source_fp"] = cur_source_fp
            merged["_content_fp"] = compute_content_fp(merged)
            merged["_field_hashes"] = compute_field_hashes(merged)
            _apply_changed_fields(merged, prev_doc)
            return merged, None, None, "emitted", None, "unchanged", None, None, None

        content_list = get_ko_content(backend_cfg, logical_layer_id)
        combined = combine_metadata_and_content(cleaned, content_list)
        combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)
        combined["_orig_id"] = logical_layer_id
        set_enrich_via(combined)
        _ensure_stage_flags(combined)
        combined["_source_fp"] = cur_source_fp
        combined["_content_fp"] = compute_content_fp(combined)
        combined["_field_hashes"] = compute_field_hashes(combined)
        _apply_changed_fields(combined, prev_doc)
        change_kind = "new" if prev_doc is None else "updated"
        return combined, None, None, "emitted", None, change_kind, None, None, None

    except Exception as e:
        logger.exception("prepare_one_doc failed for _id=%r: %s", doc.get("_id"), e)
        return _drop("exception", cleaned)


def _ensure_stage_flags(d: Dict[str, Any]) -> None:
    d.setdefault("enriched", 0)
    d.setdefault("improved", 0)


def _apply_changed_fields(doc: Dict[str, Any], prev_doc: Optional[Dict[str, Any]]) -> None:
    prev_hashes = prev_doc.get("_field_hashes") if isinstance(prev_doc, dict) else None
    cur_hashes = doc["_field_hashes"]
    changed_fields: List[str] = []
    if isinstance(prev_hashes, dict):
        for k, v in cur_hashes.items():
            if prev_hashes.get(k) != v:
                changed_fields.append(k)
    else:
        changed_fields = list(cur_hashes.keys())
    doc["_changed_fields"] = changed_fields
    doc["_changed_field_count"] = len(changed_fields)


def process_page(
    backend_cfg: BackendCfg,
    kos_page: List[Dict[str, Any]],
    *,
    workers: int,
    prev_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[
    List[Dict[str, Any]],
    int,
    int,
    Dict[str, int],
    List[Dict[str, Any]],
    int,
    List[str],
    List[str],
    int,
    int,
]:
    if not kos_page:
        return [], 0, 0, {}, [], 0, [], [], 0, 0

    before = len(kos_page)
    kos_page = [d for d in kos_page if str(d.get("status", "")).strip().lower() == "published"]
    skipped_non_published = before - len(kos_page)

    proj_ids: List[str] = []
    for ko in kos_page:
        pid = ko.get("project_id")
        if pid:
            proj_ids.append(str(pid))
    seen = set()
    unique_proj_ids = [x for x in proj_ids if not (x in seen or seen.add(x))]
    projects_index = fetch_projects_api(backend_cfg, unique_proj_ids)

    docs_out: List[Dict[str, Any]] = []
    emitted = 0
    dropped = 0
    unchanged_count = 0
    changed_ids: List[str] = []
    emitted_ids: List[str] = []
    new_count = 0
    updated_count = 0
    drop_reasons: Dict[str, int] = {}
    dropped_items: List[Dict[str, Any]] = []

    max_workers = workers if workers > 0 else int(os.getenv("DL_MAX_WORKERS", "10"))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        skip_unchanged = os.getenv("DL_SKIP_UNCHANGED_CONTENT", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
        futs = {
            ex.submit(
                prepare_one_doc,
                backend_cfg,
                doc,
                projects_index,
                prev_index=prev_index,
                skip_content_if_unchanged=skip_unchanged,
            ): idx
            for idx, doc in enumerate(kos_page)
        }
        ordered_results: List[Optional[Tuple[
            Optional[Dict[str, Any]],
            Optional[Dict[str, Any]],
            Optional[Dict[str, Any]],
            str,
            Optional[Dict[str, Any]],
            str,
            Optional[str],
            Optional[str],
            Optional[str],
        ]]] = [None] * len(futs)

        for fut in as_completed(futs):
            ordered_results[futs[fut]] = fut.result()

        for result in ordered_results:
            if result is None:
                continue
            doc_out, _, _, status, drop_info, change_kind, _, _, _ = result
            if status != "emitted" or not doc_out:
                if drop_info:
                    dropped_items.append(drop_info)
                dropped += 1
                if isinstance(status, str) and status.startswith("drop:"):
                    drop_reasons[status] = drop_reasons.get(status, 0) + 1
                else:
                    drop_reasons["drop:unknown"] = drop_reasons.get("drop:unknown", 0) + 1
                continue

            emitted += 1
            docs_out.append(doc_out)
            cid = doc_out.get("_orig_id") or doc_out.get("@id")
            if isinstance(cid, str) and cid.strip():
                emitted_ids.append(cid.strip())
                if change_kind in {"new", "updated"}:
                    changed_ids.append(cid.strip())

            if change_kind == "new":
                new_count += 1
            elif change_kind == "updated":
                updated_count += 1
            else:
                unchanged_count += 1

    dropped += skipped_non_published
    if skipped_non_published:
        drop_reasons["drop:not_published_prefilter"] = drop_reasons.get("drop:not_published_prefilter", 0) + skipped_non_published

    return (
        docs_out,
        emitted,
        dropped,
        drop_reasons,
        dropped_items,
        unchanged_count,
        changed_ids,
        emitted_ids,
        new_count,
        updated_count,
    )

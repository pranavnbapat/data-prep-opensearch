from __future__ import annotations

from typing import Any, Dict, List, Optional

from stages.downloader.utils import sha256_obj, stable_str


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


def stable_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, list):
        stable_items = [stable_value(x) for x in v]
        stable_items = [x for x in stable_items if x not in (None, "")]
        return sorted(stable_items, key=lambda x: str(x).casefold())
    if isinstance(v, dict):
        return {str(k): stable_value(val) for k, val in v.items()}
    return str(v)


def compute_source_fp(doc: Dict[str, Any]) -> str:
    obj = {f: stable_value(doc.get(f)) for f in SOURCE_FP_FIELDS}
    return sha256_obj(obj)


def compute_content_fp(doc: Dict[str, Any]) -> str:
    return sha256_obj(stable_str(doc.get("ko_content_flat")))


def compute_field_hashes(doc: Dict[str, Any]) -> Dict[str, str]:
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
    return {f: sha256_obj(stable_value(doc.get(f))) for f in fields}


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


def stable_list(v: Any) -> List[str]:
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
        return sorted(dedup_case_insensitive(out), key=lambda s: s.casefold())
    return []

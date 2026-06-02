from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from stages.downloader.utils import BackendCfg, api_base, get_session, sha256_obj


logger = logging.getLogger(__name__)

# Only these canonical KO metadata fields are mirrored from ko_metadata_translations.
# These translate the ORIGINAL metadata (source.version), not the improver `*_llm` output.
TRANSLATION_FIELDS = ("title", "subtitle", "description", "keywords")


def _allowed_langs() -> Optional[set]:
    """Optional allowlist (e.g. the 24 EU langs) via TRANSLATION_ALLOWED_LANGS=de,fr,..."""
    raw = os.getenv("TRANSLATION_ALLOWED_LANGS", "").strip()
    if not raw:
        return None
    langs = {x.strip().lower() for x in raw.split(",") if x.strip()}
    return langs or None


def _version_num(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return -1


def fetch_metadata_translations_api(
    backend_cfg: BackendCfg,
    document_id: str,
    *,
    lang: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch KO metadata translations from backend-core (api-core gateway):
      GET /api/logical_layer/metadata_translations/{document_id}[?lang=xx]

    Returns the list of per-(ko, lang) translation docs. A missing KO (404)
    yields an empty list rather than raising.
    """
    url = f"{api_base(backend_cfg)}/api/logical_layer/metadata_translations/{document_id}"
    params: Dict[str, Any] = {}
    if lang:
        params["lang"] = lang
    timeout = int(os.getenv("TRANSLATIONS_HTTP_TIMEOUT", os.getenv("DL_HTTP_TIMEOUT", "30")))
    s = get_session(backend_cfg, timeout=timeout)
    try:
        r = s.get(url, params=params or None)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        body = r.json()
    except Exception as e:
        logger.error(
            "[TranslationsFetchError] url=%s id=%s lang=%s timeout=%ss err=%r",
            url, document_id, lang, timeout, e,
        )
        raise

    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("data", "results", "translations"):
            val = body.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        if body.get("lang") and isinstance(body.get("fields"), dict):
            return [body]
    return []


def build_translations_block(raw_list: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Reshape the per-(ko, lang) translation docs into a single lang-keyed block:

        {
          "de": {"title": ..., "subtitle": ..., "description": ..., "keywords": [...],
                 "_status": "draft", "_source_version": 7, "_updated_ts": "..."},
          "fr": {...}
        }

    Provenance (_status / _source_version / _updated_ts) travels per language so the
    ingest side can filter by status or detect staleness against the KO version.
    When the backend returns more than one doc for a language, the highest
    _source_version wins.
    """
    allowed = _allowed_langs()
    block: Dict[str, Any] = {}
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        lang = item.get("lang")
        if not isinstance(lang, str) or not lang.strip():
            continue
        lang = lang.strip().lower()
        if allowed is not None and lang not in allowed:
            continue

        fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}

        entry: Dict[str, Any] = {}
        for f in TRANSLATION_FIELDS:
            if f in fields:
                entry[f] = fields.get(f)
        if not entry:
            continue
        entry["_status"] = meta.get("status")
        entry["_source_version"] = source.get("version")
        entry["_updated_ts"] = meta.get("updated_ts") or source.get("updated_ts")

        prev = block.get(lang)
        if isinstance(prev, dict) and _version_num(prev.get("_source_version")) > _version_num(entry.get("_source_version")):
            continue
        block[lang] = entry

    return block, sorted(block.keys())


def compute_translations_fp(block: Dict[str, Any]) -> str:
    """
    Stable fingerprint over the translation content + status + KO version, used to
    skip records whose translations have not changed. `_updated_ts` is excluded so
    a pure regeneration timestamp bump does not churn unchanged content.
    """
    projection = {
        lang: {k: v for k, v in entry.items() if k != "_updated_ts"}
        for lang, entry in block.items()
        if isinstance(entry, dict)
    }
    return sha256_obj(projection)

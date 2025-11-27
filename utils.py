# utils.py

import glob
import json
import logging
import os
import re
import sys
import time
import unicodedata

from bson import ObjectId
from datetime import datetime, timezone
from dateutil import parser as du_parser
from html import unescape
from types import SimpleNamespace
from typing import Iterable, List, Optional, Callable, Dict, Any, TypedDict
from urllib.parse import urlparse, urlunparse
from urllib3.util.retry import Retry

import requests

from requests.adapters import HTTPAdapter


# Logging
log_level = getattr(logging, os.getenv("LOG_LEVEL", "WARNING").upper(), logging.WARNING)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


_ZW     = re.compile(r'[\u200B-\u200D\uFEFF]')                 # zero-width
_CTRL   = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')      # control chars + DEL
_TAGS   = re.compile(r'<[^>]+>')                               # any tag

# Thin/nbsp variants frequently appear in copy/paste text
_SPACE_MAP = {
    "\u00A0": " ",  # NBSP
    "\u202F": " ",  # NNBSP (narrow no-break space)
    "\u2009": " ",  # thin space
    "\u200A": " ",  # hair space
}

# Custom JSON Encoder for ObjectId and datetime
class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for ObjectId and datetime objects."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)     # Convert ObjectId to string
        if isinstance(obj, datetime):
            return obj.isoformat()      # Convert datetime to ISO 8601 format
        return super().default(obj)

def canonical_url(u: Optional[str]) -> Optional[str]:
    """
    Return a canonical http(s) URL or None.
    - Ensures scheme is http/https (defaults to https if missing)
    - Lowercases scheme and host
    - Strips fragment
    - Leaves path and query intact
    """
    if not u:
        return None
    u = u.strip()
    # add scheme if missing
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    try:
        p = urlparse(u)
        if p.scheme.lower() not in ("http", "https") or not p.netloc:
            return None
        p = p._replace(
            scheme=p.scheme.lower(),
            netloc=p.netloc.lower(),
            fragment=""
        )
        return urlunparse(p)
    except Exception:
        return None

def get_ko_id(doc: dict):
    """
    Return the KO canonical ID/URL.
    Prefer top-level '@id'. If missing, fall back to knowledge_object_resources[0]['@id'] when present.
    """
    if not doc:
        return None
    candidate = None
    if "@id" in doc and doc["@id"]:
        candidate = doc["@id"]
    else:
        try:
            kors = doc.get("knowledge_object_resources") or []
            if kors and isinstance(kors, list) and kors[0].get("@id"):
                return kors[0]["@id"]
        except Exception:
            pass
    return canonical_url(candidate)

def extract_location_names(locations_raw):
    """
    Turn `locations` (list of dicts or strings) into a flat, case-insensitively
    de-duplicated list of names. Optionally apply a cleaner function to each.
    """
    if not isinstance(locations_raw, list):
        return []

    # 1) collect names (prefer dict["name"]; fall back to nuts_id if name blank)
    collected = []
    for item in locations_raw:
        if isinstance(item, dict):
            name = (item.get("name") or "").strip()
            if not name:
                # conservative fallback: use nuts_id only if present
                nuts = (item.get("nuts_id") or "").strip()
                if nuts:
                    name = nuts
            if name:
                collected.append(name)
        elif isinstance(item, str):
            s = item.strip()
            if s:
                collected.append(s)

    # 2) de-duplicate case-insensitively but preserve original casing of first occurrence
    seen = set()
    dedup = []
    for n in collected:
        key = n.lower()
        if key not in seen:
            seen.add(key)
            dedup.append(n)

    return dedup

ORDINAL_SUFFIX_RE = re.compile(r'(\b\d{1,2})(st|nd|rd|th)\b', re.IGNORECASE)

def normalize_date_to_yyyy_mm_dd(value: str) -> str:
    """
    Normalise many date/date-time strings to 'YYYY-MM-DD'.

    Behaviour & assumptions:
    - UK/EU interpretation for numeric dates (day-first). Example: '03/04/2005' -> 2005-04-03.
    - Ordinal suffixes allowed: '13th Dec 1988' -> 1988-12-13.
    - ISO8601 with 'T' and 'Z' supported; when timezone is present or implied, convert to UTC,
      then take the UTC calendar date (e.g. '2024-03-01T23:30:00-02:00' -> '2024-03-02').
    - Ignores extra punctuation/commas; trims whitespace.
    - Raises ValueError with a helpful message when parsing fails.

    Examples:
      '13-12-1988'        -> '1988-12-13'
      '1988-12-13'        -> '1988-12-13'
      '13 Dec 1988'       -> '1988-12-13'
      '13th December 1988'-> '1988-12-13'
      '13/12/1988'        -> '1988-12-13'
      '1988/12/13'        -> '1988-12-13'
      '2020-05-17T10:20:30Z' -> '2020-05-17'
      '2020-05-17T23:30:00+02:00' -> '2020-05-17' (UTC date)
    """
    if value is None:
        raise ValueError("Date value is None")

    s = str(value).strip()
    if not s:
        raise ValueError("Empty date string")

    # 1) Remove ordinal suffixes like 1st/2nd/3rd/4th, keep the number.
    s = ORDINAL_SUFFIX_RE.sub(r"\1", s)

    # 2) Remove trailing commas around tokens (e.g., '13, Dec, 1988' -> '13 Dec 1988')
    s = re.sub(r'\s*,\s*', ' ', s)

    # 3) Normalise a common ISO quirk: 'Z' -> '+00:00' for fromisoformat compatibility
    s_iso = s.replace('Z', '+00:00').replace('z', '+00:00')

    # Strategy A: dateutil is very tolerant; prefer it with day-first semantics.
    try:
        dt = du_parser.parse(s, dayfirst=True, yearfirst=False, fuzzy=True)
    except (du_parser.ParserError, ValueError):
        # Strategy B: try Python's ISO parser for strictly ISO inputs after Z fix.
        try:
            dt = datetime.fromisoformat(s_iso)
        except ValueError as e:
            raise ValueError(f"Could not parse date: {value!r}") from e

    # If it's a date-only (no time), dateutil gives a datetime at 00:00 with no tzinfo.
    # If it has tzinfo or came from ISO with offset, convert to UTC before taking the date.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    # Else it’s naive; we treat it as given local calendar date and keep it unchanged.

    return dt.date().isoformat()

def _extract_content_pages(ko_content):
    """
    Safely extract a list of strings from a nested ko_content structure.
    - Accepts the current structure: [{"content": {"content_pages": [...]}}]
    - Ignores non-list / non-string entries
    - Strips whitespace-only pages
    """
    flat = []
    if isinstance(ko_content, list):
        for item in ko_content:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, dict):
                continue
            pages = content.get("content_pages") or []
            if isinstance(pages, list):
                for p in pages:
                    if isinstance(p, str) and p.strip():
                        flat.append(p)
    return flat


def flatten_ko_content(doc: dict, mode: str = "flat_only", empty_sentinel: str = "No content present"):
    """
    Normalise KO content according to `mode`.

    Modes:
      - "flat_only"     -> keep ONLY ko_content_flat; drop ko_content
      - "both"          -> keep ko_content AND ko_content_flat
      - "original_only" -> keep ONLY ko_content; drop ko_content_flat

    When no pages are found:
      - ko_content_flat becomes the string `empty_sentinel`
      - ko_content is preserved/removed per mode
    """
    ko_content = doc.get("ko_content")

    # Extract pages (list of non-empty strings)
    flat_pages = _extract_content_pages(ko_content)

    # Build ko_content_flat
    if flat_pages:
        doc["ko_content_flat"] = flat_pages
        doc["ko_content_flat"] = clean_ko_content(flat_pages) if flat_pages else empty_sentinel
    else:
        # As requested, use a string sentinel instead of an empty list
        doc["ko_content_flat"] = empty_sentinel

    # Apply mode
    if mode == "flat_only":
        # Drop the heavy original structure
        doc.pop("ko_content", None)
    elif mode == "both":
        # Keep both as-is
        pass
    elif mode == "original_only":
        # Keep original; remove the flat field
        doc.pop("ko_content_flat", None)
    else:
        # Defensive default: behave like flat_only
        doc.pop("ko_content", None)

    return doc

def clean_str(s: str | None) -> str | None:
    """
    Normalise and de-gunk short strings:
    - NFC normalisation, remove zero-width + control chars
    - normalise CRLF/CR to LF, collapse runs of whitespace
    - convert NBSP/thin spaces to regular space
    """
    if not s or not isinstance(s, str):
        return None
    s = unicodedata.normalize("NFC", s)
    for k, v in _SPACE_MAP.items():
        s = s.replace(k, v)
    s = _ZW.sub("", s)
    s = _CTRL.sub("", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r'\s+', ' ', s)  # collapse runs
    s = s.strip()
    return s or None

def strip_html_light(s: str | None) -> str | None:
    """
    Light HTML stripper:
    - unescape entities
    - strip tags inline
    - collapse whitespace
    Best for labels/one-line fields (keywords, languages, names).
    """
    s = clean_str(s)
    if not s:
        return None
    s = unescape(s)
    s = _TAGS.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None

def clean_list(values: Optional[Iterable[str] | str],
               *,
               dedupe_casefold: bool = True,
               item_cleaner: Optional[Callable[[str | None], str | None]] = None
               ) -> List[str]:
    """
    Generic cleaner for list-like fields (labels):
    - Accepts None | str | Iterable[str]
    - Cleans each item (default: clean_str; pass strip_html_light if needed)
    - Dedupe case-insensitively via .casefold() while preserving first casing
    """
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]

    cleaner = item_cleaner or clean_str
    out: List[str] = []
    seen: set[str] = set()

    for v in values:
        if v is None:
            continue
        t = cleaner(v)
        if not t:
            continue
        key = t.casefold() if dedupe_casefold else t
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out

def session_with_retries(total: int = 5, backoff: float = 0.5) -> requests.Session:
    """
    Pooled session with retries for 429/5xx, safe for POST pagination.
    """
    s = requests.Session()
    r = Retry(
        total=total, read=total, connect=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=r, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch_ko_metadata(
    *,
    base_host: Optional[str] = None,
    page_size: int = 100,
    limit: Optional[int] = None,
    timeout: int = 30,
    bearer_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch KO metadata via POST /api/logical_layer/search.
    - Paginates until no next_page (or until `limit` items collected).
    - Sets `logical_layer_id` = str(_id)
    - Normalises date into `date_of_completion` from any of: date_of_completion | dateCreated | date
      using normalize_date_to_yyyy_mm_dd (best-effort; leaves None if unparsable).
    - Cleans text-ish fields (title/subtitle/description).
    - Normalises list-ish fields via clean_list (dedup + strip HTML lightly).
    Returns a list of dicts (shallow copies of server items with light normalisation).
    """
    host = (base_host or os.getenv("BACKEND_CORE_HOST_DEV", "https://backend-core.dev.farmbook.ugent.be")).rstrip("/")
    url = f"{host}/api/logical_layer/search"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    sess = session_with_retries()
    out: List[Dict[str, Any]] = []

    remaining = limit if limit is not None else float("inf")
    page = 1

    while remaining > 0:
        params = {"limit": page_size, "page": page, "sort_criteria": 1}
        resp = sess.post(url, params=params, json={}, headers=headers, timeout=timeout)
        resp.raise_for_status()

        payload = resp.json() if resp.content else {}
        data = payload.get("data") or []
        if not data:
            break

        for raw in data:
            if remaining <= 0:
                break

            ko = dict(raw)  # shallow copy so we don't mutate the original payload

            # Stable logical-layer id
            ko["logical_layer_id"] = str(ko.get("_id", ""))

            # Date → canonical date_of_completion (YYYY-MM-DD) if possible
            raw_date = ko.get("date_of_completion") or ko.get("dateCreated") or ko.get("date")
            try:
                parsed = normalize_date_to_yyyy_mm_dd(raw_date) if raw_date else None
            except Exception:
                parsed = None
            ko["date_of_completion"] = parsed

            # Clean key text fields (preserve case, strip tags/controls, tidy spacing)
            for k in ("title", "subtitle", "description"):
                if k in ko:
                    ko[k] = strip_html_light(ko[k])

            # Normalise list-ish fields (dedupe, strip html lightly)
            for k in ("topics", "themes", "keywords", "languages", "locations", "subcategory", "creators"):
                if k in ko:
                    ko[k] = clean_list(ko[k], item_cleaner=strip_html_light)

            out.append(ko)
            remaining -= 1

        # pagination
        nxt = (payload.get("pagination") or {}).get("next_page")
        if not nxt:
            break
        page = nxt

    return out


def fetch_projects(
    *,
    base_host: Optional[str] = None,
    page_size: int = 200,
    timeout: int = 30,
    bearer_token: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch Projects via POST /api/logical_layer/projects/search.
    - Paginates until no next_page.
    - Returns a dict indexed by BOTH str(project_id) and str(_id) for easy lookup.
    - Normalises common display fields when present:
        projectName, projectAcronym, projectURL, projectDoi, project_type
    Leaves original keys intact.
    """
    host = (base_host or os.getenv("BACKEND_CORE_HOST_DEV", "https://backend-core.dev.farmbook.ugent.be")).rstrip("/")
    url = f"{host}/api/logical_layer/projects/search"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    sess = session_with_retries()
    index: Dict[str, Dict[str, Any]] = {}

    page = 1
    body = {"project_type": [], "project_country": [], "project_status": []}

    def _get(d: Dict[str, Any], *keys, default=None):
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return default

    def _get_nested(d: Dict[str, Any], path: List[str], default=None):
        cur: Any = d
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur if cur not in (None, "") else default

    while True:
        params = {"limit": page_size, "page": page}
        resp = sess.post(url, params=params, json=body, headers=headers, timeout=timeout)
        resp.raise_for_status()

        payload = resp.json() if resp.content else {}
        data = payload.get("data") or []
        if not data:
            break

        for raw in data:
            proj = dict(raw)

            # Ensure ids are strings for indexing
            pid = str(proj.get("project_id") or "")
            oid = str(proj.get("_id") or "")

            # Gentle normalisation of common fields (if present)
            proj["projectName"] = _get(proj, "projectName", "title", "name")
            proj["projectAcronym"] = _get(proj, "projectAcronym", "acronym")
            proj["projectURL"] = _get(proj, "projectURL", "URL", "website", "homepage")
            proj["projectDoi"] = _get_nested(proj, ["identifiers", "grantDoi"], default=_get(proj, "projectDoi"))
            proj["project_type"] = _get(proj, "project_type", "type", "programme")

            # Index by both ids when available
            if pid:
                index[pid] = proj
            if oid:
                index[oid] = proj

        nxt = (payload.get("pagination") or {}).get("next_page")
        if not nxt:
            break
        page = nxt

    return index


def clean_ko_content(chunks: list[str]) -> str:
    """
    Clean a list of text fragments extracted from PDFs/JSON for search/embedding.
    Keeps paragraphs; removes page furniture and common PDF artefacts.
    """
    # 1) Join and normalise Unicode (NFKC flattens compatibility forms)
    s = " ".join(chunks)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = unicodedata.normalize("NFKC", s)

    # 2) Whitespace: convert NBSP & friends to regular space; remove zero-widths
    #   \u00A0 NBSP; \u2000–\u200A various spaces; \u202F NNBSP; \u205F MMSP
    s = re.sub(r"[\u00A0\u2000-\u200A\u202F\u205F]", " ", s)
    #   \u200B ZWSP, \u200C ZWNJ, \u200D ZWJ, \uFEFF BOM
    s = re.sub(r"[\u200B\u200C\u200D\uFEFF]", "", s)
    #   U+00AD SOFT HYPHEN: remove entirely (PyCharm shows it as 'SHY')
    s = s.replace("\u00AD", "")
    #   HTML entity form sometimes appears in scraped text
    s = s.replace("&shy;", "")

    # 3) Remove page headers/footers like "7 / 31" at line starts
    s = re.sub(r"(?m)^\s*\d+\s*/\s*\d+\s+", "", s)

    # 4) Table-of-contents dot leaders → single space
    s = re.sub(r"\.{2,}", " ", s)

    # 5) Normalise bullets and dash spacing
    #    lines that start with a loose "-" become bullets
    s = re.sub(r"(?m)^\s*-\s+", "• ", s)
    #    collapse weird spaced hyphens/dashes to " - "
    s = re.sub(r"\s*[-–—]\s*", " - ", s)

    # Normalise special hyphen/minus to ASCII hyphen so later rules behave consistently
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2212", "-")

    # [NEW] Preserve true hyphenated compounds (collapse spaces around hyphen when both sides are word chars)
    # Examples: "EIP - AGRI" -> "EIP-AGRI", "multi - actor" -> "multi-actor"
    s = re.sub(r'(?<=\w)\s*-\s*(?=\w)', '-', s)

    # [NEW] Fix occasional split at word-start like "T hese" -> "These"
    # (Capital letter + single space + 2+ lowercase letters)
    s = re.sub(r'\b([A-Z])\s([a-z]{2,})\b', r'\1\2', s)

    # 6) Map curly quotes/ellipsis to ASCII; drop ©/®/™ clutter
    trans = {
        ord("“"): '"', ord("”"): '"', ord("„"): '"', ord("‟"): '"',
        ord("‘"): "'", ord("’"): "'", ord("‚"): "'", ord("‛"): "'",
        ord("…"): "...", ord("©"): " ", ord("®"): " ", ord("™"): " ",
    }
    s = s.translate(trans)

    # 7) Remove control characters (except \n and \t)
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s)

    # 8) Light de-noising of obvious repeated headers (exact matches only, safe)
    #    Here: drop duplicate standalone "Brassica Fact Sheet" lines
    s = re.sub(r"(?m)^\s*Brassica\s+Fact\s+Sheet\s*$", "", s)

    # [NEW] Deduplicate exact lines (helps when PDFs repeat headers/URLs verbatim)
    _lines, _seen = [], set()
    for _line in s.splitlines():
        _key = _line.strip()
        if _key and _key not in _seen:
            _seen.add(_key)
            _lines.append(_line)
    s = "\n".join(_lines)

    # De-hyphenate words split across lines: "nutricio-\nnal" -> "nutricional"
    s = re.sub(r'(?<=\w)-\n(?=\w)', '', s)

    # Belt-and-braces: if a soft hyphen survived with a newline, drop both
    s = re.sub(r'\u00AD\n?', '', s)

    # [NEW] Join intra-sentence hard wraps: replace a single newline between word chars with a space
    # e.g., "Increase\nproductivity" -> "Increase productivity"
    s = re.sub(r'(?<=\w)\n(?=\w)', ' ', s)

    # ensure a space when lowercase is followed by Uppercase (productivityOptimize -> productivity Optimize)
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)

    # ensure a space after a colon (to:Developing -> to: Developing)
    s = re.sub(r':(?!\s)', ': ', s)

    # space between compact number+suffix and a 4-digit year (6,99M2018 -> 6,99M 2018)
    s = re.sub(r'(\d[\d.,]*\s*[kKmMbB])(?=\d{4}\b)', r'\1 ', s)

    # collapse duplicate "n° NN" tokens (n°19 n°19 -> n°19)
    s = re.sub(r'\b(n°\s*\d+)\s+\1\b', r'\1', s, flags=re.IGNORECASE)

    # Normalise "Nº"/"N°"/"No." variants to a single form "n°"
    s = re.sub(r'\b[Nn][oO][\.\s]?(?=\d)', 'n° ', s)  # "No 5", "No.5" -> "n° 5"
    s = re.sub(r'\b[Nn][º°]\s*(?=\d)', 'n° ', s)  # "Nº5", "N° 5" -> "n° 5"

    # Remove spaces before punctuation (e.g., "palabra :" -> "palabra:")
    s = re.sub(r'\s+([,.;:!?])', r'\1', s)

    # 9) Trim spaces around newlines; collapse excessive blank lines and spaces
    s = re.sub(r"[ \t]+\n", "\n", s)           # strip trailing spaces before NL
    s = re.sub(r"\n{3,}", "\n\n", s)           # max two newlines
    s = re.sub(r"[ \t]{2,}", " ", s)           # collapse runs of spaces/tabs
    s = re.sub(r"\s{2,}", " ", s)              # extra safety
    s = s.strip()

    return s


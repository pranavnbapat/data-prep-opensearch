# download_mongodb_data.py

import random
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth

from utils import *

# Choose ONE of: "flat_only", "both", "original_only"
KO_CONTENT_MODE = "flat_only"

_tls = threading.local()

class BackendCfg(TypedDict):
    host: str
    user: str
    pwd: str

ENV_CHOICES = {"DEV", "PRD"}

# ---- Report for KOs that are URL-based but have no ko_content_document ----
MISSING_URL_CONTENT = []          # list of dict rows for CSV/JSON report
_MISSING_LOCK = threading.Lock()  # protect appends across worker threads

def _load_backend_cfg(env_mode: str) -> BackendCfg:
    env_mode = (env_mode or "DEV").upper()
    if env_mode not in ENV_CHOICES:
        raise ValueError(f"Invalid env_mode {env_mode!r}. Choose one of {sorted(ENV_CHOICES)}")

    if env_mode == "DEV":
        host = os.getenv("BACKEND_CORE_HOST_DEV", "")
        user = os.getenv("BACKEND_CORE_DEV_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_DEV_API_PASSWORD", "")
    else:  # PRD
        host = os.getenv("BACKEND_CORE_HOST_PRD", "")
        user = os.getenv("BACKEND_CORE_PRD_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_PRD_API_PASSWORD", "")

    if not host or not user or pwd is None:
        raise RuntimeError(
            f"Missing required env vars for {env_mode}. "
            f"Expected BACKEND_CORE_HOST_{env_mode}, BACKEND_CORE_{env_mode}_API_USERNAME, BACKEND_CORE_{env_mode}_API_PASSWORD"
        )

    return {"host": host.rstrip("/"), "user": user, "pwd": pwd}

# --- lazy-selected backend (no env required at import) ---
CURRENT_ENV_MODE: Optional[str] = None
CURRENT_BACKEND: Optional[BackendCfg] = None

def _ensure_backend(env_hint: Optional[str] = None) -> None:
    """Initialise CURRENT_BACKEND the first time it's needed."""
    global CURRENT_ENV_MODE, CURRENT_BACKEND
    if CURRENT_BACKEND is not None:
        return
    # Prefer explicit hint, then ENV_MODE, else default to DEV
    mode = (env_hint or os.getenv("ENV_MODE") or "DEV").upper()
    CURRENT_BACKEND = _load_backend_cfg(mode)
    CURRENT_ENV_MODE = mode

def select_environment(env_mode: str) -> None:
    """Switch active environment at runtime and clear the thread-local session."""
    global CURRENT_ENV_MODE, CURRENT_BACKEND
    mode = (env_mode or "DEV").upper()
    if CURRENT_ENV_MODE == mode and CURRENT_BACKEND is not None:
        return
    CURRENT_BACKEND = _load_backend_cfg(mode)
    CURRENT_ENV_MODE = mode
    try:
        if getattr(_tls, "session", None) is not None:
            try:
                _tls.session.close()
            except Exception:
                pass
            _tls.session = None
    except Exception:
        pass
    logging.info("Switched environment to %s", CURRENT_ENV_MODE)

def dedup_case_insensitive(items: Optional[List[str]]) -> List[str]:
    if not items:
        return []
    seen = set()
    out = []
    for x in items:
        if not isinstance(x, str):
            continue
        k = x.casefold()
        if k not in seen:
            out.append(x)
            seen.add(k)
    return out

def max_workers_global() -> int:
    try:
        return int(os.getenv("DL_MAX_WORKERS", "10"))
    except Exception:
        return 10

def get_session(timeout: int = 15) -> requests.Session:
    """
    Return a thread-local Session. The first call in a thread creates it via requests_session().
    Subsequent calls in the same thread reuse the same pooled session.
    """
    sess = getattr(_tls, "session", None)
    if sess is None:
        _tls.session = requests_session(timeout=timeout)
    return _tls.session

def requests_session(timeout: int = 15) -> requests.Session:
    """
    Create a pooled, retrying Requests Session with HTTP Basic Auth taken from Django settings.
    - Reuses TCP connections (HTTP keep-alive) across calls
    - Adds exponential backoff on 429/5xx
    - Applies a default timeout to every request unless overridden
    """
    _ensure_backend()
    user = CURRENT_BACKEND["user"]
    pwd = CURRENT_BACKEND["pwd"]

    if not user or pwd is None:
        raise RuntimeError(
            "Missing credentials in CURRENT_BACKEND (user/pwd)."
        )

    s = requests.Session()
    s.auth = HTTPBasicAuth(user, pwd)
    s.headers.update({
        "accept": "application/json",
        "Content-Type": "application/json",
    })

    # ---- Retries + connection pooling ----
    retry = Retry(
        total=5,                    # up to 5 total attempts
        backoff_factor=0.5,         # 0.5s, 1s, 2s, 4s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=100,       # connection pools per host
        pool_maxsize=100,           # max concurrent connections per host
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # ---- Enforce a default timeout transparently ----
    original = s.request
    def _with_timeout(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return original(method, url, **kw)
    s.request = _with_timeout

    return s

def api_base() -> str:
    _ensure_backend()
    base = CURRENT_BACKEND["host"]
    if not base:
        raise RuntimeError("BACKEND_CORE_HOST is empty in the active environment.")
    return base

def fetch_ko_metadata_api(limit: Optional[int] = None, page: int = 1, sort_criteria: int = 1) -> Dict[str, Any]:
    """
    Calls:
    POST {base}/api/logical_layer/documents?limit={limit}&page={page}&sort_criteria={sort_criteria}
    Body: {}
    Returns: { "data": [ ...KOs... ], "pagination": {...} }
    """
    base = api_base()

    url = f"{base}/api/logical_layer/documents"

    params = {"page": page, "sort_criteria": sort_criteria}

    if limit is not None:
        params["limit"] = limit

    s = get_session()
    r = s.post(url, params=params, json={})

    r.raise_for_status()

    data = r.json()

    return data

def fetch_projects_api(project_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Calls:
    POST {base}/api/logical_layer/projects/
    Body: ["id1","id2",...]
    Returns: list of project objects. We index by _id for quick lookup.
    """
    if not project_ids:
        return {}
    base = api_base()
    url = f"{base}/api/logical_layer/projects/"

    s = get_session()

    r = s.post(url, json=project_ids)
    r.raise_for_status()
    items = r.json() or []
    # Index by _id
    return {str(p.get("_id")): p for p in items if isinstance(p, dict)}


def clean_text(text):
    """
    Cleans input text by:
    - Normalizing Unicode (keeping accents)
    - Removing special characters except letters, numbers, hyphens, and spaces
    - Replacing multiple spaces with a single space
    - Converting text to lowercase
    """
    if not isinstance(text, str):
        return ""

    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[^\w\s\-.@()]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def _valid_year(yyyy: str, min_year=1000, max_year=2100) -> bool:
    """
    Guard against pathological years that business logic shouldn't index.
    Adjust bounds (e.g., min_year=1900).
    """
    try:
        y = int(yyyy)
        return min_year <= y <= max_year
    except Exception:
        return False


def clean_ko_metadata(doc):
    """
    Cleans metadata fields while preserving original case.
    - Safely standardises 'dateCreated' to ISO 8601 (YYYY-MM-DD) if possible.
    - Silently skips malformed dates without raising exceptions.
    """
    fields_to_exclude = {
        'schema_version', '@context', '_tags', 'object_hash', 'uploaded_by', 'knowledge_object_version',
        'created_by', 'updated_by', 'version', 'contributor_custom_metadata', 'physical_layer_ko_metadata_id',
        'status', 'object_metadata', 'language_versions', 'otherFields', 'knowledge_object_resources', 'collection'
    }

    cleaned = {key: value for key, value in doc.items() if key not in fields_to_exclude}

    raw_date = doc.get("date_of_completion")
    parsed = normalize_date_to_yyyy_mm_dd(raw_date)

    if parsed and _valid_year(parsed[:4], min_year=1000, max_year=2100):
        cleaned["date_of_completion"] = parsed
    else:
        # Remove if invalid / out of range
        cleaned.pop("date_of_completion", None)

    topics_raw = doc.get("topics")
    topics_norm: list[str] = []
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
    # write back or drop
    if topics_norm:
        cleaned["topics"] = topics_norm
    else:
        cleaned.pop("topics", None)

    if "created_ts" in cleaned:
        cleaned["ko_created_at"] = cleaned.pop("created_ts")
    if "updated_ts" in cleaned:
        cleaned["ko_updated_at"] = cleaned.pop("updated_ts")

    pd = doc.get("project_details")
    if isinstance(pd, dict):
        disp = pd.get("display_name")
        if disp and not cleaned.get("project_display_name"):
            cleaned["project_display_name"] = disp
    # ensure no nested copy remains
    cleaned.pop("project_details", None)

    # Avoid duplicate project_type (prefer the one from enrich_with_project)
    if "project_type" not in cleaned and isinstance(pd, dict) and pd.get("project_type"):
        cleaned["project_type"] = pd["project_type"]

    return cleaned


def clean_ko_content(doc):
    """
    Cleans and filters content fields before saving.
    """

    if isinstance(doc, str):
        return {"content_text": doc}

    if not isinstance(doc, dict):
        return {}

    fields_to_exclude = {
        'object_hash', 'uploaded_by', 'created_ts', 'created_by',
        'updated_ts', 'updated_by', 'version', 'status', 'object_metadata',
        'language_versions', 'otherFields'
    }

    return {key: value for key, value in doc.items() if key not in fields_to_exclude}

def combine_metadata_and_content(metadata, content_list):
    metadata_copy = metadata.copy()
    metadata_copy['ko_content'] = content_list
    return metadata_copy

def _env_and_date_paths(output_root: str = "output") -> tuple[str, str, str, str]:
    """
    Returns (env_dir, write_dir, year, month)
    - env_dir:   output/<ENV_MODE>
    - write_dir: output/<ENV_MODE>/<YYYY>/<MM>
    """
    _ensure_backend()  # ensures CURRENT_ENV_MODE is available
    env = (CURRENT_ENV_MODE or os.getenv("ENV_MODE") or "DEV").upper()

    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")

    env_dir = os.path.join(output_root, env)
    write_dir = os.path.join(env_dir, year, month)
    return env_dir, write_dir, year, month

def save_results(data, output_root: str = "output"):
    """
    Save data to a timestamped JSON snapshot if it differs from the latest snapshot.

    Directory layout:
      output/<ENV_MODE>/<YYYY>/<MM>/final_output_<dd_mm-YYYY>_<HH-MM-SS>.json

    - Canonicalises both "existing" and "new" docs the same way before comparing:
        * move top-level "_id" -> "_orig_id" (non-destructive: done on copies)
    - Order-insensitive: sorts by (@id || _orig_id) before comparing/writing
    - Atomic write: write to .tmp then os.replace() to the final path
    """
    if not data:
        logging.warning("No documents found to save.")
        return None

    # -------- helpers --------
    def _canonicalise_docs(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Make a shallow-copied, comparable list:
           - map top-level _id -> _orig_id (if present)
           - return a NEW list so caller's 'data' is not mutated
        """
        out: List[Dict[str, Any]] = []
        for d in docs:
            if not isinstance(d, dict):
                # ignore non-dicts defensively
                continue
            c = dict(d)  # shallow copy
            if "_id" in c:
                # rename once to maintain a consistent field for equality checks/snapshots
                c["_orig_id"] = c.pop("_id")
            out.append(c)
        return out

    def _sort_key(doc: Dict[str, Any]) -> str:
        return (doc.get("@id") or doc.get("_orig_id") or "")

    # -------- resolve target dirs --------
    env_dir, write_dir, year, month = _env_and_date_paths(output_root)
    os.makedirs(write_dir, exist_ok=True)

    # -------- find latest snapshot (if any) --------
    existing_files = sorted(
        glob.glob(os.path.join(env_dir, "**", "final_output_*.json"), recursive=True),
        reverse=True
    )
    latest_file = existing_files[0] if existing_files else None

    existing_data_canon: List[Dict[str, Any]] = []
    if latest_file:
        try:
            with open(latest_file, "r", encoding="utf-8") as fh:
                existing_raw = json.load(fh)
            # Canonicalise and sort for a fair, order-insensitive comparison
            existing_data_canon = _canonicalise_docs(existing_raw)
            existing_data_canon.sort(key=_sort_key)
        except Exception as e:
            logging.warning("Error reading %s: %s. Proceeding as if no prior snapshot.", latest_file, e)
            existing_data_canon = []

    # -------- canonicalise and compare new data --------
    new_data_canon = _canonicalise_docs(data)
    new_data_canon.sort(key=_sort_key)

    if latest_file and existing_data_canon == new_data_canon:
        logging.info("No changes detected. Skipping file save: %s", latest_file)
        return latest_file

    # -------- write new snapshot (atomic) --------
    timestamp = datetime.now().strftime("%d_%m-%Y_%H-%M-%S")
    json_filename = os.path.join(write_dir, f"final_output_{timestamp}.json")
    tmp_filename = json_filename + ".tmp"

    logging.warning("[Save] about to write %d documents -> %s", len(new_data_canon), json_filename)

    try:
        # Use the canonicalised, sorted data for writing so snapshots are consistent
        with open(tmp_filename, "w", encoding="utf-8") as fh:
            json.dump(new_data_canon, fh, cls=CustomJSONEncoder, indent=4, ensure_ascii=False)
        os.replace(tmp_filename, json_filename)  # atomic on POSIX
        logging.info("Data saved: %s", json_filename)
        return json_filename
    except Exception as e:
        # Clean up tmp if something went wrong
        try:
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
        except Exception:
            pass
        logging.error("Error saving file %s: %s", json_filename, e)
        return None


def _is_blank(val) -> bool:
    """
    Treat None, empty strings (after strip), and empty/blank-only lists/dicts as missing.
    """
    if val is None:
        return True
    if isinstance(val, str):
        return not val.strip()
    if isinstance(val, list):
        # Blank if it's empty OR all items are non-strings or blank strings
        return len(val) == 0 or all((not isinstance(x, str)) or (not x.strip()) for x in val)
    if isinstance(val, dict):
        return len(val) == 0
    return False


def process_document(doc, projects_collection):
    try:
        # Hard stop if not Published (defensive; in case other callers reuse this)
        if str(doc.get("status", "")).strip().lower() != "published":
            _id = doc.get("_id")
            # normalise _id for logs: handle {"$oid": "..."} or plain values
            if isinstance(_id, dict) and "$oid" in _id:
                _id = _id["$oid"]
            logging.debug("Dropping KO %r due to status=%r", _id, doc.get("status"))
            return None

        cleaned_doc = clean_ko_metadata(doc)  # Remove unwanted fields

        # Canonicalise the KO ID
        ko_id = get_ko_id(cleaned_doc)
        if ko_id:
            cleaned_doc["@id"] = ko_id

        locations_raw = doc.get("locations", [])
        loc_flat = extract_location_names(locations_raw)

        if loc_flat:
            cleaned_doc["locations_flat"] = loc_flat

        cleaned_doc["keywords"] = clean_list(cleaned_doc.get("keywords"), item_cleaner=strip_html_light)
        cleaned_doc["languages"] = clean_list(cleaned_doc.get("languages"), item_cleaner=strip_html_light)
        cleaned_doc["locations_flat"] = clean_list(cleaned_doc.get("locations_flat"), item_cleaner=strip_html_light)

        subcats = cleaned_doc.get("subcategories", cleaned_doc.get("subcategory"))
        cleaned_doc["subcategories"] = clean_list(subcats, item_cleaner=strip_html_light)
        cleaned_doc.pop("subcategory", None)

        cleaned_doc["topics"] = clean_list(cleaned_doc.get("topics"), item_cleaner=strip_html_light)
        cleaned_doc["themes"] = clean_list(cleaned_doc.get("themes"), item_cleaner=strip_html_light)
        cleaned_doc["creators"] = clean_list(cleaned_doc.get("creators"), item_cleaner=strip_html_light)

        cleaned_doc["keywords"] = dedup_case_insensitive(cleaned_doc.get("keywords"))
        cleaned_doc["languages"] = dedup_case_insensitive(cleaned_doc.get("languages"))
        cleaned_doc["locations_flat"] = dedup_case_insensitive(cleaned_doc.get("locations_flat"))
        cleaned_doc["subcategories"] = dedup_case_insensitive(cleaned_doc.get("subcategories"))
        cleaned_doc["topics"] = dedup_case_insensitive(cleaned_doc.get("topics"))
        cleaned_doc["themes"] = dedup_case_insensitive(cleaned_doc.get("themes"))
        cleaned_doc["creators"] = dedup_case_insensitive(cleaned_doc.get("creators"))

        if _is_blank(cleaned_doc.get("date_of_completion")):
            doc_id = doc.get("_id")
            logging.warning(
                f"Dropping document {doc_id} due to invalid/missing date_of_completion"
            )
            return None

        required_pre = ["project_id", "title", "@id", "topics", "themes", "date_of_completion"]
        missing_pre = [f for f in required_pre if _is_blank(cleaned_doc.get(f))]
        if missing_pre:
            doc_id = doc.get("_id")
            logging.warning(
                "Dropping document %s due to missing pre-enrichment fields: %s "
                "(title=%r, at_id=%r, project_id=%r, topics=%r, themes=%r)",
                doc_id, missing_pre,
                cleaned_doc.get("title"),
                cleaned_doc.get("@id"),
                cleaned_doc.get("project_id"),
                cleaned_doc.get("topics"),
                cleaned_doc.get("themes"),
            )
            return None

        cleaned_doc = enrich_with_project(cleaned_doc, projects_collection)

        required_post = ["project_name", "project_acronym"]
        missing_post = [f for f in required_post if _is_blank(cleaned_doc.get(f))]
        if missing_post:
            doc_id = doc.get("_id")
            logging.warning(
                f"Dropping document {doc_id} due to missing post-enrichment fields: {missing_post} "
                f"(title={cleaned_doc.get('title')!r}, at_id={cleaned_doc.get('@id')!r}, "
                f"project_name={cleaned_doc.get('project_name')!r}, project_acronym={cleaned_doc.get('project_acronym')!r})"
            )
            return None

        logical_layer_id = str(doc.get('_id', ''))

        cleaned_doc["_orig_id"] = logical_layer_id

        # ---- Skip ko_content_document for URL-only KOs (prevents 404 noise) ----
        url_only, url_list = is_url_based_ko(doc)  # MUST use original doc
        if url_only:
            row = {
                "logical_layer_id": str(doc.get("_id", "")),
                "at_id": cleaned_doc.get("@id"),
                "title": cleaned_doc.get("title"),
                "first_url": (url_list[0] if url_list else None),
                "url_count": len(url_list),
            }
            with _MISSING_LOCK:
                MISSING_URL_CONTENT.append(row)

            logging.warning(
                "[URL-only KO] Skipping ko_content_document fetch; id=%r title=%r first_url=%s",
                row["logical_layer_id"], row["title"], row["first_url"]
            )

            # Mark provenance for downstream consumers and debugging
            cleaned_doc["is_url_only"] = True

            combined = combine_metadata_and_content(cleaned_doc, [])
            combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)

            combined["_orig_id"] = logical_layer_id

            return combined

        if not logical_layer_id:
            logging.warning("Skipping content fetch: missing _id for doc title=%r", cleaned_doc.get("title"))
            return None

        content_data = get_ko_content(logical_layer_id)
        combined = combine_metadata_and_content(cleaned_doc, content_data)
        combined = flatten_ko_content(combined, mode=KO_CONTENT_MODE)

        return combined

    except Exception as e:
        logging.error("Error processing document %r: %s", (doc.get("_id") if isinstance(doc, dict) else None), e)
        return None


def enrich_with_project(ko_doc, projects_collection):
    project_id = ko_doc.get("project_id")
    if not project_id:
        return ko_doc  # nothing to enrich

    project_doc = projects_collection.find_one({"_id": project_id})
    if not project_doc:
        return ko_doc  # no matching project found

    # Map fields from project_doc into KO
    ko_doc["project_name"] = project_doc.get("title")
    ko_doc["project_acronym"] = project_doc.get("acronym")
    ko_doc["project_url"] = project_doc.get("URL")
    ko_doc["project_doi"] = project_doc.get("identifiers", {}).get("grantDoi")
    ko_doc["project_type"] = project_doc.get("project_type")
    if project_doc.get("created_ts"):
        ko_doc["proj_created_at"] = project_doc["created_ts"]
    if project_doc.get("updated_ts"):
        ko_doc["proj_updated_at"] = project_doc["updated_ts"]
    return ko_doc

def _process_page(kos_page: List[Dict[str, Any]], workers: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Given one page of KO metadata:
      - batch-fetch projects for those items
      - enrich, fetch content, flatten in parallel
    Returns the list of emitted combined docs for this page.
    """
    if not kos_page:
        return []

    before = len(kos_page)
    kos_page = [
        d for d in kos_page
        if str(d.get("status", "")).strip().lower() == "published"
    ]
    skipped = before - len(kos_page)
    if skipped:
        logging.info("Skipped %d non-published KOs on this page (kept=%d).", skipped, len(kos_page))

    # Collect project ids for this page
    proj_ids = []
    for ko in kos_page:
        pid = ko.get("project_id")
        if pid:
            proj_ids.append(str(pid))
    # unique order-preserving
    seen = set()
    unique_proj_ids = [x for x in proj_ids if not (x in seen or seen.add(x))]

    projects_index = fetch_projects_api(unique_proj_ids)

    def _find_one(query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(query, dict):
            return None
        key = query.get("_id") or query.get("project_id") or query.get("id")
        if key is None:
            return None
        return projects_index.get(str(key))

    projects_collection = SimpleNamespace(find_one=_find_one)

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=(workers or max_workers_global())) as executor:
        futures = [executor.submit(process_document, doc, projects_collection) for doc in kos_page]
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as e:
                logging.error("Unhandled worker exception: %s", e)
                continue
            if res:
                results.append(res)
    return results


def load_latest_snapshot_index(output_root: str = "output") -> Dict[str, Dict[str, Any]]:
    """
    Load the latest final_output_* snapshot (if any) and return an index by _orig_id.
    Used as a cache so we can reuse previously extracted ko_content_flat for URL-only KOs.

    Returns: { "<logical_layer_id>": doc, ... }
    """
    env_dir, _, _, _ = _env_and_date_paths(output_root)

    existing_files = sorted(
        glob.glob(os.path.join(env_dir, "**", "final_output_*.json"), recursive=True),
        reverse=True,
    )
    if not existing_files:
        return {}

    latest_file = existing_files[0]
    try:
        with open(latest_file, "r", encoding="utf-8") as fh:
            docs = json.load(fh) or []
    except Exception as e:
        logging.warning("Could not load latest snapshot %s for cache reuse: %s", latest_file, e)
        return {}

    index: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        if not isinstance(d, dict):
            continue
        key = d.get("_orig_id") or d.get("_id")
        if isinstance(key, str) and key:
            index[key] = d

    logging.info("Loaded %d docs from latest snapshot cache %s", len(index), latest_file)
    return index


def write_missing_url_content_report(output_root: str = "output") -> Optional[Dict[str, Any]]:
    """Write JSON+CSV once per run for URL-only KOs (those we skipped fetching).

    Files are saved alongside final_output_* in:
      output/<ENV_MODE>/<YYYY>/<MM>/

    Returns a small dict with:
      - timestamp
      - json_path
      - csv_path
      - count
    or None if nothing to write.
    """
    try:
        count = len(MISSING_URL_CONTENT)
        if count == 0:
            logging.info("[Report] No URL-only KOs to report; skipping report files.")
            return None

        # Use the same directory scheme as final_output_* files
        env_dir, write_dir, year, month = _env_and_date_paths(output_root)
        os.makedirs(write_dir, exist_ok=True)

        ts = datetime.now().strftime("%d_%m-%Y_%H-%M-%S")

        miss_json = os.path.join(write_dir, f"missing_url_content_{ts}.json")
        miss_csv  = os.path.join(write_dir, f"missing_url_content_{ts}.csv")

        logging.info(
            "[Report] Preparing URL-only report for ENV=%s year=%s month=%s "
            "(count=%s, dir=%s)",
            (CURRENT_ENV_MODE or os.getenv("ENV_MODE") or "DEV"),
            year,
            month,
            count,
            write_dir,
        )

        with open(miss_json, "w", encoding="utf-8") as fh:
            json.dump(MISSING_URL_CONTENT, fh, cls=CustomJSONEncoder, indent=2, ensure_ascii=False)

        import csv
        with open(miss_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["logical_layer_id", "at_id", "title", "first_url", "url_count"]
            )
            writer.writeheader()
            writer.writerows(MISSING_URL_CONTENT)

        logging.warning(
            "[Report] URL-only rows: %d → json=%s ; csv=%s",
            count, miss_json, miss_csv,
        )

        return {
            "timestamp": ts,
            "json_path": miss_json,
            "csv_path": miss_csv,
            "count": count,
        }
    except Exception:
        logging.exception("Failed to write missing URL-content report")
        return None


def _is_video_url(u: str) -> bool:
    if not isinstance(u, str):
        return False
    u = u.lower()
    return any(host in u for host in (
        "youtube.com", "youtu.be", "vimeo.com", "dailymotion.com"
    ))

def is_youtube_url(u: str) -> bool:
    """Return True for YouTube/YouTu.be watch/shorts/live urls."""
    if not isinstance(u, str):
        return False
    v = u.lower()
    return (
        "youtube.com/watch" in v or
        "youtube.com/live" in v or
        "youtube.com/shorts" in v or
        "youtu.be/" in v
    )

def patch_url_only_docs_with_extracted_text(
    docs: List[Dict[str, Any]],
    url_rows: List[Dict[str, Any]],
    max_workers: int = 5,
    max_chars: Optional[int] = None,
    previous_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> int:
    """
    For each URL-only KO (from MISSING_URL_CONTENT), fetch text and set ko_content_flat.
    - docs: the already-emitted combined (and flattened) docs list
    - url_rows: rows from MISSING_URL_CONTENT (logical_layer_id, first_url, ...)
    - max_workers: parallelism for extractor calls (be nice to the service)
    - max_chars: if set, truncate ko_content_flat to this many chars
    Returns number of docs patched.
    """
    if not docs or not url_rows:
        return 0

    max_conc = int(os.getenv("EXTRACTOR_MAX_CONCURRENCY", "4"))
    _sema = threading.BoundedSemaphore(max_conc)

    # index docs by _orig_id for fast patching
    # index docs by _orig_id for fast patching
    index: Dict[str, Dict[str, Any]] = {}
    for d in docs:
        key = d.get("_orig_id") or d.get("_id")  # prefer _orig_id
        if isinstance(key, str) and key:
            index[key] = d

    prev_index = previous_index or {}

    # build tasks (skip duplicates by logical_layer_id)
    tasks = []
    seen = set()
    reused = 0

    for row in url_rows:
        llid = row.get("logical_layer_id")
        first_url = row.get("first_url")
        if not llid or not first_url or llid in seen:
            continue

        cur_doc = index.get(llid)
        if not cur_doc:
            logging.info("[Patch] No matching doc in memory for _orig_id=%s (title=%r)", llid, row.get("title"))
            continue

        # ---- Reuse cached content if we already have text for the same URL ----
        prev_doc = prev_index.get(str(llid))
        if prev_doc:
            prev_flat = prev_doc.get("ko_content_flat")
            prev_url = prev_doc.get("ko_content_url") or prev_doc.get("first_url")

            # Reuse only when:
            #   - we have non-empty ko_content_flat, and
            #   - URL did not change
            if prev_flat and (not prev_url or prev_url == first_url):
                cur_doc["ko_content_flat"] = prev_flat
                if prev_doc.get("ko_content_source"):
                    cur_doc["ko_content_source"] = prev_doc["ko_content_source"]
                if prev_url:
                    cur_doc["ko_content_url"] = prev_url
                reused += 1
                seen.add(llid)
                continue  # no PageSense hit for this KO

        seen.add(llid)
        tasks.append((llid, first_url))

    if reused:
        logging.info("[Patch] Reused cached extracted text for %d URL-only KOs (no extractor call).", reused)

    if not tasks:
        # Nothing left that actually needs extraction
        return reused

    patched = 0

    def _work(item):
        llid, url = item
        # stagger starts slightly to avoid burst
        time.sleep(random.uniform(0.2, 0.8))
        _sema.acquire()
        try:
            # You can reuse the global session you already have if desired:
            sess = get_session(timeout=int(os.getenv("EXTRACTOR_HTTP_TIMEOUT", "35")))
            # --- 1) YouTube: try captions first (fast, no download) ---
            if is_youtube_url(url):
                vid, _ = parse_youtube_value(url)
                if vid:
                    try:
                        # env overrides are optional; commas allowed
                        pref = [s.strip() for s in os.getenv("YT_CAP_PREF_LANGS", "en").split(",") if s.strip()]
                        # if you prefer server-side EN translation first, set YT_CAP_TRANSLATE_TO=en
                        translate_to = os.getenv("YT_CAP_TRANSLATE_TO") or None

                        segs, lang_code = fetch_transcript_segments(
                            vid,
                            preferred_langs=pref,
                            translate_to=translate_to,
                            http_client=sess,  # shares cookies/proxies if you set them on the session
                        )
                        # Convert to plain text (your STT format uses start/end; mirror that)
                        text_from_caps = "\n".join(s["text"] for s in segs if s.get("text"))
                        # Return a tagged payload so the main loop can set source fields
                        return (llid, url, text_from_caps, "youtube_captions")
                    except Exception as cap_err:
                        # Fall back to HTML extractor for YT pages if captions blocked/missing
                        logging.info("[Patch] Captions unavailable for %s (%s): %s", llid, url, cap_err)

            # --- 2) Generic HTML/article extraction path (your existing service) ---
            text = fetch_url_text_with_extractor(
                url,
                timeout=int(os.getenv("EXTRACTOR_TIMEOUT", "150")),
                retries=int(os.getenv("EXTRACTOR_RETRIES", "3")),
                backoff=float(os.getenv("EXTRACTOR_BACKOFF", "1.6")),
                min_chars=int(os.getenv("EXTRACTOR_MIN_CHARS", "100")),
                session=sess,
            )
            return (llid, url, text, "external_url_extractor")
        finally:
            _sema.release()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_work, t) for t in tasks]
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if not isinstance(res, tuple):
                    logging.error("[Patch] Unexpected worker result type: %r", type(res))
                    continue

                if len(res) == 3:
                    llid, url, text = res
                    source_tag = "external_url_extractor"  # sensible default
                elif len(res) == 4:
                    llid, url, text, source_tag = res
                else:
                    logging.error("[Patch] Unexpected worker result length: %d (res=%r)", len(res), res)
                    continue
            except Exception as e:
                logging.error("[Patch] Worker error: %s", e)
                continue

            doc = index.get(llid)
            if not doc:
                continue

            if not text:
                # Mark well-known video hosts explicitly so downstream can treat them specially
                if _is_video_url(url):
                    doc["is_url_only"] = True
                    doc["ko_content_source"] = "video_redirect"
                    doc["ko_content_url"] = url

                    # overwrite only if empty or the default placeholder from flattening
                    existing = doc.get("ko_content_flat")
                    if not existing or (isinstance(existing, str) and existing.strip().lower() == "no content present"):
                        doc["ko_content_flat"] = "External video (no extractable page text)"

                logging.info("[Patch] No text extracted for %s (%s)", llid, url)
                continue

            if isinstance(max_chars, int) and max_chars > 0:
                text = text[:max_chars]

            doc["ko_content_flat"] = text
            doc["ko_content_source"] = "external_url_extractor"
            doc["ko_content_url"] = url
            patched += 1

    return patched

def get_ko_metadata(max_workers: int = 10, page_size: Optional[int] = None, sort_criteria: int = 1):
    """
    Fetch KO metadata from HTTP API (not Mongo), enrich with projects (HTTP API),
    then fetch content from Mongo physical_layer (as before), flatten and save.
    - page_size: API 'limit'
    - max_pages: None -> walk all pages; else stop after N pages
    """
    try:
        # Load latest snapshot as a cache to avoid re-hitting the extractor
        previous_index = load_latest_snapshot_index()

        page = 1
        pages_seen = set()
        total_emitted = 0
        total_dropped = 0
        t0 = time.perf_counter()
        workers = max_workers or max_workers_global()

        combined_results_all: List[Dict[str, Any]] = []

        while True:
            if page in pages_seen:
                logging.warning("Breaking due to repeated page indicator: %s", page)
                break
            pages_seen.add(page)

            payload = fetch_ko_metadata_api(limit=page_size, page=page, sort_criteria=sort_criteria)
            kos_page = payload.get("data", []) or payload.get("results", []) or []
            if not kos_page:
                break

            pagination = payload.get("pagination") or {}
            next_page = pagination.get("next_page")
            logging.info("[KO API] page=%s fetched=%s next=%r", page, len(kos_page), next_page)

            # Process this page now (low memory)
            emitted_page = _process_page(kos_page, workers=workers)
            emitted_count = len(emitted_page)
            dropped_count = len(kos_page) - emitted_count
            total_emitted += emitted_count
            total_dropped += dropped_count

            combined_results_all.extend(emitted_page)

            if not next_page:
                break
            try:
                next_page_int = int(next_page)
            except Exception:
                break
            if next_page_int == page:
                break
            page = next_page_int

        logging.warning("[Process] emitted=%s, dropped=%s, elapsed=%.2fs",
                        total_emitted, total_dropped, time.perf_counter() - t0)

        patched_count = patch_url_only_docs_with_extracted_text(
            combined_results_all,
            MISSING_URL_CONTENT,
            max_workers=int(os.getenv("EXTRACTOR_WORKERS", "5")),
            max_chars=None,
            previous_index=previous_index,
        )

        logging.info(
            "[Patch] URL-only docs patched=%s; remaining_after_patch=%s",
            patched_count, len(MISSING_URL_CONTENT),
        )

        # Keep only those that STILL lack ko_content_flat
        remaining = []
        by_id = {d.get("_orig_id") or d.get("_id"): d for d in combined_results_all}
        for row in MISSING_URL_CONTENT:
            doc = by_id.get(row.get("logical_layer_id"))
            if not doc:
                remaining.append(row)
                continue
            kcf = doc.get("ko_content_flat")
            if (not kcf) or (isinstance(kcf, str) and kcf.strip().lower() == "no content present"):
                remaining.append(row)

        with _MISSING_LOCK:
            MISSING_URL_CONTENT[:] = remaining

        # Save once at the end
        write_missing_url_content_report()

        if total_emitted == 0:
            saved_path = save_results([])
        else:
            saved_path = save_results(combined_results_all)

        summary = {
            "emitted": total_emitted,
            "dropped": total_dropped,
            "url_only_remaining": len(MISSING_URL_CONTENT),
            "saved_path": saved_path,
        }
        return summary

    except Exception:
        logging.exception("Error in get_ko_metadata run")
        return None


def get_ko_content(document_id: str) -> List[dict]:
    """
    Fetch KO content via HTTP API instead of Mongo.
    API expects the original KO _id as 'document_id' (same as our pre-save _id).
    Returns a list of content docs; we then clean them as before.
    """
    base = api_base()
    url = f"{base}/api/nlp/ko_content_document"
    params = {"document_id": document_id}

    try:
        s = get_session()
        r = s.get(url, params=params)
        if r.status_code == 404:
            # Benign: no extracted text available for this KO
            if os.getenv("LOG_CONTENT_404", "0") == "1":
                logging.info("[Content API] %s → 404 Not Found (no content)", document_id)
            else:
                logging.debug("[Content API] %s → 404 Not Found (no content)", document_id)
            return []
        r.raise_for_status()
        body = r.json()
    except requests.HTTPError as e:
        # Non-404 HTTP errors—keep them visible but don't explode the run
        status = getattr(e.response, "status_code", None)
        logging.error("Error fetching content via API for %s: HTTP %s %s",
                      document_id, status, e)
        return []
    except Exception as e:
        logging.error("Error fetching content via API for %s: %s", document_id, e)
        return []

    # Normalise to list
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

    # Coerce strings → dicts with a single text field
    normalised: List[dict] = []
    for el in items:
        if isinstance(el, dict):
            normalised.append(clean_ko_content(el))
        elif isinstance(el, str):
            normalised.append({"content_text": el})
        else:
            continue

    if not normalised:
        logging.warning("[Content API] %s → 0 item(s) after normalisation", document_id)

    return normalised

# Run the function
if __name__ == "__main__":
    _ensure_backend()
    ps = int(os.getenv("DL_PAGE_SIZE", "100"))
    if ps < 1:
        ps = 1
    if ps > 100:
        ps = 100
    mw = int(os.getenv("DL_MAX_WORKERS", "10"))
    sc = int(os.getenv("DL_SORT_CRITERIA", "1"))

    backend_host = None
    try:
        backend_host = (CURRENT_BACKEND or {}).get("host")
    except Exception:
        backend_host = "<unknown>"

    logging.info("Active ENV_MODE=%s; backend=%s", CURRENT_ENV_MODE, backend_host)

    get_ko_metadata(
        max_workers=mw,
        page_size=ps,
        sort_criteria=sc,
    )


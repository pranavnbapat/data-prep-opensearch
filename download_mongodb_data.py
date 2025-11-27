# download_mongodb_data.py

import glob

from types import SimpleNamespace

from utils import *
from utils import  _ensure_backend, _load_backend_cfg, _tls, _MISSING_LOCK

# Choose ONE of: "flat_only", "both", "original_only"
KO_CONTENT_MODE = "flat_only"

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

def write_missing_url_content_report(output_root: str = "output") -> Optional[str]:
    """Write JSON+CSV once per run for URL-only KOs (those we skipped fetching)."""

    if not MISSING_URL_CONTENT:
        logging.warning("[Report] URL-only rows: 0 → nothing to write")
        return None

    try:
        env_dir, write_dir, year, month = _env_and_date_paths(output_root)

        report_dir = os.path.join(write_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)

        ts = datetime.now().strftime("%d_%m-%Y_%H-%M-%S")

        miss_json = os.path.join(report_dir, f"missing_url_content_{ts}.json")
        with open(miss_json, "w", encoding="utf-8") as fh:
            json.dump(
                MISSING_URL_CONTENT,
                fh,
                cls=CustomJSONEncoder,
                indent=2,
                ensure_ascii=False,
            )

        logging.warning(
            "[Report] URL-only rows: %d → %s (env=%s, year=%s, month=%s)",
            len(MISSING_URL_CONTENT),
            miss_json,
            os.getenv("ENV_MODE", "DEV"),
            year,
            month,
        )

        return miss_json
    except Exception as e:
        logging.error("Failed to write missing URL-content report: %s", e)
        return None

def get_ko_metadata(max_workers: int = 10, page_size: Optional[int] = None, sort_criteria: int = 1):
    """
    Fetch KO metadata from HTTP API (not Mongo), enrich with projects (HTTP API),
    then fetch content from Mongo physical_layer (as before), flatten and save.
    - page_size: API 'limit'
    - max_pages: None -> walk all pages; else stop after N pages
    """
    try:
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

        _ = patch_url_only_docs_with_extracted_text(
            combined_results_all,
            MISSING_URL_CONTENT,
            max_workers=int(os.getenv("EXTRACTOR_WORKERS", "5")),
            max_chars=None
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
        miss_json = write_missing_url_content_report()

        if total_emitted == 0:
            output_file = save_results([])
        else:
            output_file = save_results(combined_results_all)

        return {
            "emitted": total_emitted,
            "dropped": total_dropped,
            "output_file": output_file,
            "missing_url_json": miss_json,
        }

    except Exception as e:
        logging.error(f"Error fetching metadata via API: {e}")
        raise


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

    backend_host = CURRENT_BACKEND["host"] if CURRENT_BACKEND else "<unset>"
    logging.info("Active ENV_MODE=%s; backend=%s", CURRENT_ENV_MODE, backend_host)

    get_ko_metadata(
        max_workers=mw,
        page_size=ps,
        sort_criteria=sc,
    )

